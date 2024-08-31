package bitcask

import (
	"bytes"

	"github.com/ysoding/bitcask/index"
)

type Iterator struct {
	iteratorOption
	indexerIter index.Iterator
	db          *DB
}

func (db *DB) NewIterator(opts ...IteratorOption) *Iterator {

	iter := &Iterator{
		db:             db,
		iteratorOption: DefaultIteratorOption,
	}

	for _, opt := range opts {
		opt(&iter.iteratorOption)
	}

	indexerIter := db.indexer.Iterator(iter.Reverse)
	iter.indexerIter = indexerIter

	return iter
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexerIter.Rewind()
	it.skipToNext()
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexerIter.Seek(key)
	it.skipToNext()
}

// Next 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexerIter.Next()
	it.skipToNext()
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexerIter.Valid()
}

// Key 当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexerIter.Key()
}

// Value 当前遍历位置的 Value 数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexerIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByIndexInfo(logRecordPos)
}

// Close 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexerIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexerIter.Valid(); it.indexerIter.Next() {
		key := it.indexerIter.Key()

		if prefixLen <= len(key) && bytes.Compare(it.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
