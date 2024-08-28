package index

import (
	"sync"

	"github.com/google/btree"
	"github.com/ysoding/bitcask/data"
)

type BTree struct {
	tree *btree.BTree
	mu   *sync.Mutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		mu:   &sync.Mutex{},
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
