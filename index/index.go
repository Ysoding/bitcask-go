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
	Close() error
	Iterator(reverse bool) Iterator
}

type IndexerType byte

const (
	Btree IndexerType = iota
	// ART 自适应基数树索引
	ART
	BPTree
)

func NewIndexer(typ IndexerType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
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

type Iterator interface {
	// Rewind 重新回到迭代器的起点，即第一个数据
	Rewind()

	// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// Next 跳转到下一个 key
	Next()

	// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// Key 当前遍历位置的 Key 数据
	Key() []byte

	// Value 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// Close 关闭迭代器，释放相应资源
	Close()
}
