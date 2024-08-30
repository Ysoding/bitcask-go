package bitcask

import (
	"errors"
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
)

const (
	fileLockName = "flock"
)

type DB struct {
	option
	indexer     index.Indexer
	mu          *sync.RWMutex
	activeFile  *data.DataFile            // 当前活跃文件，可以写入
	oldFiles    map[uint32]*data.DataFile // 旧的文件，只用于读 fileid->datafile
	reclaimSize int64                     // 表示有多少数据是无效的
	bytesWrite  uint64                    //总计写的节字数
	isInitial   bool                      // 是否是第一次初始化此数据目录
	fileLock    *flock.Flock
	fileIDs     []int
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

	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	return db, nil
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
		dataFile, err := data.OpenDataFile(db.dirPath, uint32(fileID), fio.StandardFileIO)
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

func (db *DB) Close() error {
	return nil
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

// Put
func (db *DB) Put(key []byte, val []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
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

// Delete
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if info := db.indexer.Get(key); info == nil {
		return nil
	}

	logRecord := &data.LogRecord{Key: key, Type: data.LogRecodDeleted}
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
	return nil, nil
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

	logRecord, err := dataFile.ReadLogRecord(info.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecodDeleted {
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
	if _, err := db.activeFile.Write(encodedRecord); err != nil {
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
