package bitcask

import (
	"errors"
	"sync"

	"github.com/ysoding/bitcask/data"
	"github.com/ysoding/bitcask/index"
)

type DB struct {
	option
	indexer    index.Indexer
	mu         sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件，可以写入
	oldFiles   map[uint32]*data.DataFile // 旧的文件，只用于读 fileid->datafile
}

func Open(opts ...DBOption) (*DB, error) {

	db := &DB{
		option: DefaultOption,
	}

	for _, opt := range opts {
		opt(&db.option)
	}

	err := db.checkConfiguration()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Close() error {
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	info := db.indexer.Get(key)
	if info == nil {
		return nil, ErrKeyNotFound

	}

	return db.getValueByIndexInfo(info)
}

// Put 返回旧的值
func (db *DB) Put(key []byte, val []byte) ([]byte, error) {
	return nil, nil
}

// Delete 返回旧的值
func (db *DB) Delete() ([]byte, error) {
	return nil, nil
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
