package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ysoding/bitcask/data"
)

func TestBTree_delete(t *testing.T) {
	bt := NewBTree()
	assert.Equal(t, 0, bt.Size())

	bt.Put(nil, &data.LogRecordPos{FileID: 1, Offset: 1})
	assert.Equal(t, 1, bt.Size())

	result, ok := bt.Delete(nil)
	assert.True(t, ok)
	assert.Equal(t, uint32(1), result.FileID)
	assert.Equal(t, int64(1), result.Offset)

	assert.Equal(t, 0, bt.Size())

	result, ok = bt.Delete([]byte("a"))
	assert.False(t, ok)
	assert.Nil(t, result)
}

func TestBTree_get(t *testing.T) {
	bt := NewBTree()

	bt.Put(nil, &data.LogRecordPos{FileID: 1, Offset: 1})

	result := bt.Get(nil)
	assert.Equal(t, uint32(1), result.FileID)
	assert.Equal(t, int64(1), result.Offset)

	assert.Nil(t, bt.Get([]byte("a")))
	assert.Equal(t, 1, bt.Size())
}

func TestBTree_put(t *testing.T) {
	bt := NewBTree()

	result := bt.Put(nil, &data.LogRecordPos{FileID: 1})
	assert.Nil(t, result)

	result = bt.Put([]byte("a"), &data.LogRecordPos{FileID: 1, Offset: 1})
	assert.Nil(t, result)

	result = bt.Put([]byte("a"), &data.LogRecordPos{FileID: 1, Offset: 2})
	assert.Equal(t, uint32(1), result.FileID)
	assert.Equal(t, int64(1), result.Offset)

	assert.Equal(t, 2, bt.Size())
}
