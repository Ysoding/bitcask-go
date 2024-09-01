package bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
	"github.com/ysoding/bitcask/data"
	"github.com/ysoding/bitcask/fio"
	"github.com/ysoding/bitcask/index"
	"github.com/ysoding/bitcask/utils"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

type DB struct {
	option
	indexer         index.Indexer
	mu              *sync.RWMutex
	activeFile      *data.DataFile            // 当前活跃文件，可以写入
	oldFiles        map[uint32]*data.DataFile // 旧的文件，只用于读 fileid->datafile
	reclaimSize     int64                     // 表示有多少数据是无效的
	bytesWrite      uint64                    //总计写的节字数
	isInitial       bool                      // 是否是第一次初始化此数据目录
	fileLock        *flock.Flock
	fileIDs         []int
	seqNo           uint64 // 事务序列号，全局递增
	isMerging       bool
	seqNoFileExists bool // 存储事务序列号的文件是否存在
}

func Open(opts ...DBOption) (*DB, error) {
	db := &DB{
		option:   DefaultOption,
		oldFiles: make(map[uint32]*data.DataFile),
		mu:       new(sync.RWMutex),
	}

	for _, opt := range opts {
		opt(&db.option)
	}

	if err := db.checkConfiguration(); err != nil {
		return nil, err
	}

	db.indexer = index.NewIndexer(index.IndexerType(db.indexerType), db.dirPath, db.syncWrite)

	isInitial, err := db.initDirectory()
	if err != nil {
		return nil, err
	}

	if err := db.checkDatabaseIsUsing(); err != nil {
		return nil, err
	}

	hasData, err := db.checkDatabaseHasData()
	if err != nil {
		return nil, err
	}

	if !hasData {
		isInitial = true
	}

	db.isInitial = isInitial

	// 加载 merge 数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	if db.indexerType != BPlusTree {
		// 从 hint 索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}

		// 重置 IO 类型为标准文件 IO
		if db.mmapAtStartUp {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	} else { // BPlusTree
		// B+树索引不需要从数据文件中加载索引
		// 取出当前事务序列号
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}

	return db, nil
}

// 将数据文件的 IO 类型设置为标准文件 IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.dirPath, fio.StandardFileIO); err != nil {
		return err
	}

	for _, dataFile := range db.oldFiles {
		if err := dataFile.SetIOManager(db.dirPath, fio.StandardFileIO); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.dirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.dirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true

	return os.Remove(fileName)
}

func (db *DB) loadIndexFromHintFile() error {
	// 查看 hint 索引文件是否存在
	hintFileName := filepath.Join(db.dirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//	打开 hint 索引文件
	hintFile, err := data.OpenHintFile(db.dirPath)
	if err != nil {
		return err
	}

	offset := int64(0)
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.indexer.Put(logRecord.Key, pos)
		offset += size
	}

	return nil
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// merge 目录不存在的话直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识 merge 完成的文件，判断 merge 是否处理完了
	var mergeFinished bool
	var mergeFileNames []string

	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}

		if entry.Name() == data.SeqNoFileName || entry.Name() == fileLockName {
			continue
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// 没有 merge 完成则直接返回
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return nil
	}

	// 删除旧的数据文件
	fileID := uint32(0)
	for ; fileID < nonMergeFileId; fileID++ {
		filename := data.GetDataFileName(db.dirPath, fileID)
		if _, err := os.Stat(filename); err == nil {
			if err := os.Remove(filename); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动到数据目录中
	for _, filename := range mergeFileNames {
		srcPath := filepath.Join(mergePath, filename)
		destPath := filepath.Join(db.dirPath, filename)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) initDirectory() (bool, error) {
	if _, err := os.Stat(db.dirPath); os.IsNotExist(err) {
		if err := os.Mkdir(db.dirPath, os.ModePerm); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

// 判断基于dirPath目录是否被使用
func (db *DB) checkDatabaseIsUsing() error {
	fileLock := flock.New(filepath.Join(db.dirPath, fileLockName))
	locked, err := fileLock.TryLock()
	if err != nil {
		return err
	}

	if !locked {
		return ErrDatabaseIsUsing
	}

	db.fileLock = fileLock

	return nil
}

// 检测dirPath目录是否有.data文件
func (db *DB) checkDatabaseHasData() (bool, error) {
	entries, err := os.ReadDir(db.dirPath)
	if err != nil {
		return false, err
	}

	hasData := false

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			hasData = true
			break
		}
	}

	return hasData, nil
}

func (db *DB) loadDataFiles() error {
	entries, err := os.ReadDir(db.dirPath)
	if err != nil {
		return err
	}

	var fileIDs []int
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			tmps := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(tmps[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIDs = append(fileIDs, fileID)
		}
	}

	sort.Ints(fileIDs)
	db.fileIDs = fileIDs

	for i, fileID := range fileIDs {
		ioType := fio.StandardFileIO
		if db.mmapAtStartUp {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.dirPath, uint32(fileID), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIDs)-1 {
			db.activeFile = dataFile
		} else {
			db.oldFiles[uint32(fileID)] = dataFile
		}
	}

	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIDs) == 0 {
		return nil
	}

	// 查看是否发生过 merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.dirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileID(db.dirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.indexer.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.indexer.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	currentSeqNo := nonTransactionSeqNo

	for i, fileID := range db.fileIDs {
		fileID := uint32(fileID)

		// 如果比最近未参与 merge 的文件 id 更小，则说明已经从 Hint 文件中加载索引了
		if hasMerge && fileID < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFiles[fileID]
		}

		offset := int64(0)
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &data.LogRecordPos{FileID: fileID, Offset: offset, Size: uint32(size)}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordTxnFinished {
					// 事务完成，对应的 seq no 的数据可以更新到内存索引中
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	db.seqNo = currentSeqNo
	return nil
}

func (db *DB) Close() error {
	defer func() {
		// 释放文件锁
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}

		// 关闭索引
		if err := db.indexer.Close(); err != nil {
			panic("failed to close index")
		}
	}()

	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 保存当前事务序列号
	if err := db.saveCurrentSeqNo(); err != nil {
		return err
	}

	//	关闭当前活跃文件g
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, file := range db.oldFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) saveCurrentSeqNo() error {
	seqNoFile, err := data.OpenSeqNoFile(db.dirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encBuf, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encBuf); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) Backup(dir string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return utils.CopyDir(db.dirPath, dir, []string{fileLockName})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	info := db.indexer.Get(key)
	if info == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByIndexInfo(info)
}

func (db *DB) Put(key []byte, val []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Value: val,
		Type:  data.LogRecordNormal,
	}

	info, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if oldInfo := db.indexer.Put(key, info); oldInfo != nil {
		db.reclaimSize += int64(oldInfo.Size)
	}

	return nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if info := db.indexer.Get(key); info == nil {
		return nil
	}

	logRecord := &data.LogRecord{Key: logRecordKeyWithSeqNo(key, nonTransactionSeqNo), Type: data.LogRecordDeleted}
	info, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(info.Size)

	oldInfo, ok := db.indexer.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldInfo != nil {
		db.reclaimSize += int64(oldInfo.Size)
	}

	return nil
}

func (db *DB) ListKeys() ([][]byte, error) {
	iterator := db.indexer.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.indexer.Size())

	idx := 0
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}

	return keys, nil
}

// Fold 获取所有的数据，并执行用户指定的操作，函数返回 false 时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.indexer.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByIndexInfo(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) checkConfiguration() error {
	if db.dirPath == "" {
		return errors.New("error: database direcotry path is empty")
	}
	if db.dataFileSize <= 0 {
		return errors.New("error: database data file size must be greater than 0")
	}
	return nil
}

func (db *DB) getValueByIndexInfo(info *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile

	if db.activeFile.FileID == info.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFiles[info.FileID]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(info.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 向activeFile追加写入数据
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	if db.activeFile == nil {
		if err := db.updateActiveDataFile(); err != nil {
			return nil, err
		}
	}

	encodedRecord, size := data.EncodeLogRecord(logRecord)
	if db.activeFile.WriteOffset+size > db.dataFileSize {
		// 当前file大小不够，刷新到disk，创建新的文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.oldFiles[db.activeFile.FileID] = db.activeFile

		if err := db.updateActiveDataFile(); err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}
	db.bytesWrite += uint64(size)

	if db.needSync() {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		db.bytesWrite = 0
	}

	return &data.LogRecordPos{FileID: db.activeFile.FileID, Offset: offset, Size: uint32(size)}, nil
}

func (db *DB) needSync() bool {
	if db.syncWrite {
		return true
	}
	return db.bytesPerSync > 0 && db.bytesWrite >= uint64(db.bytesPerSync)
}

// updateActiveDataFile 更新设置ActiveDataFile 需要持有db的锁
func (db *DB) updateActiveDataFile() error {
	fileID := uint32(0)

	if db.activeFile != nil {
		fileID = db.activeFile.FileID + 1
	}

	dataFile, err := data.OpenDataFile(db.dirPath, fileID, fio.StandardFileIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}
