package index

import (
	"bytes"
	"sort"
	"sync"

	"github.com/google/btree"
	"github.com/ysoding/bitcask/data"
)

type BTree struct {
	tree *btree.BTree
	mu   *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   new(sync.RWMutex),
	}
}

func (b *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}

	bitem := b.tree.Get(item)
	if bitem == nil {
		return nil
	}

	return bitem.(*Item).data
}

func (b *BTree) Put(key []byte, data *data.LogRecordPos) *data.LogRecordPos {

	item := &Item{key: key, data: data}

	b.mu.Lock()
	bitem := b.tree.ReplaceOrInsert(item)
	b.mu.Unlock()

	if bitem == nil {
		return nil
	}

	return bitem.(*Item).data
}

func (b *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	item := &Item{key: key}

	b.mu.Lock()
	bitem := b.tree.Delete(item)
	b.mu.Unlock()

	if bitem == nil {
		return nil, false
	}

	return bitem.(*Item).data, true
}

func (b *BTree) Size() int {
	return b.tree.Len()
}

func (b *BTree) Close() error {
	return nil
}

func (b *BTree) Iterator(reverse bool) Iterator {
	if b.tree == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return newBtreeIterator(b.tree, reverse)
}

type btreeIterator struct {
	currIdx int
	reverse bool
	values  []*Item
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	idx := 0
	values := make([]*Item, tree.Len())

	iter := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(iter)
	} else {
		tree.Ascend(iter)
	}

	return &btreeIterator{
		currIdx: 0,
		reverse: reverse,
		values:  values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (b *btreeIterator) Rewind() {
	b.currIdx = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (b *btreeIterator) Seek(key []byte) {
	if b.reverse {
		b.currIdx = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) <= 0
		})
	} else {
		b.currIdx = sort.Search(len(b.values), func(i int) bool {
			return bytes.Compare(b.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (b *btreeIterator) Next() {
	b.currIdx++
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (b *btreeIterator) Valid() bool {
	return b.currIdx < len(b.values)
}

// Key 当前遍历位置的 Key 数据
func (b *btreeIterator) Key() []byte {
	return b.values[b.currIdx].key
}

// Value 当前遍历位置的 Value 数据
func (b *btreeIterator) Value() *data.LogRecordPos {
	return b.values[b.currIdx].data
}

// Close 关闭迭代器，释放相应资源
func (b *btreeIterator) Close() {
	b.values = nil
}
