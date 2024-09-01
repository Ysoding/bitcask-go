package index

import (
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree"
	"github.com/ysoding/bitcask/data"
)

// AdaptiveRadixTree 自适应基数树索引
// 主要封装了 https://github.com/plar/go-adaptive-radix-tree 库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{tree: goart.New(), lock: new(sync.RWMutex)}
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Put(key []byte, val *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, val)
	art.lock.Unlock()
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	if oldValue == nil {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

type artIterator struct {
	currIndex int
	reverse   bool
	values    []*Item
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	index := 0
	if reverse {
		index = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())

	tree.ForEach(func(node goart.Node) (cont bool) {
		values[index] = &Item{
			key:  node.Key(),
			data: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			index--
		} else {
			index++
		}
		return true
	})

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (it *artIterator) Rewind() {
	it.currIndex = 0
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *artIterator) Seek(key []byte) {
	if it.reverse {
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) <= 0
		})
	} else {
		it.currIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) >= 0
		})
	}
}

// Next 跳转到下一个 key
func (it *artIterator) Next() {
	it.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *artIterator) Valid() bool {
	return it.currIndex < len(it.values)
}

// Key 当前遍历位置的 Key 数据
func (it *artIterator) Key() []byte {
	return it.values[it.currIndex].key
}

// Value 当前遍历位置的 Value 数据
func (it *artIterator) Value() *data.LogRecordPos {
	return it.values[it.currIndex].data
}

// Close 关闭迭代器，释放相应资源
func (it *artIterator) Close() {
	it.values = nil
}
