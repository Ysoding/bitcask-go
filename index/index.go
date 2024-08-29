package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/ysoding/bitcask/data"
)

type Indexer interface {
	Get(key []byte) *data.LogRecordPos
	Put(key []byte, data *data.LogRecordPos) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)
	Size() int
}

type IndexerType byte

const (
	Btree IndexerType = iota
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	default:
		panic("unsupported indexer type")
	}
}

type Item struct {
	key  []byte
	data *data.LogRecordPos
}

func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}
