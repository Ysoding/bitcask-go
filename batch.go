package bitcask

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/ysoding/bitcask/data"
)

const nonTransactionSeqNo uint64 = 0

var txnFinishedKey = []byte("txn-finished")

type WriteBatch struct {
	writeBatchOption
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts ...WriteBatchOption) *WriteBatch {
	b := &WriteBatch{
		writeBatchOption: DefaultWriteBatchOption,
		db:               db,
		mu:               new(sync.Mutex),
		pendingWrites:    make(map[string]*data.LogRecord),
	}

	for _, opt := range opts {
		opt(&b.writeBatchOption)
	}

	return b
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{Key: key, Value: value, Type: data.LogRecordNormal}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.indexer.Get(key)
	if logRecordPos == nil {
		tmp := string(key)
		if wb.pendingWrites[tmp] != nil {
			delete(wb.pendingWrites, tmp)
		}
		return nil
	}

	// 数据存在，已经被保存，则需要append delete log
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if len(wb.pendingWrites) > wb.maxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	positions := make(map[string]*data.LogRecordPos)

	// 开始写数据到数据文件当中
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeqNo(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}

		positions[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinishedKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.syncWrite && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]

		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.indexer.Put(record.Key, pos)
		}

		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.indexer.Delete(record.Key)
		}

		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], buf[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析 LogRecord 的 key，获取实际的 key 和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
