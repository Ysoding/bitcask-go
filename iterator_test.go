package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-1")
	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator()
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-2")
	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(getTestKey(10), getTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator()
	defer iterator.Close()
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, getTestKey(10), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, getTestKey(10), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator-3")
	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annde"), randomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("cnedc"), randomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aeeue"), randomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("esnue"), randomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("bnede"), randomValue(10))
	assert.Nil(t, err)

	// 正向迭代
	iter1 := db.NewIterator()
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// 反向迭代
	iter2 := db.NewIterator(WithIteratorReverse(true))
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// 指定了 prefix
	iter3 := db.NewIterator(WithIteratorPrefix([]byte("aee")))
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
