package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_WriteBatch1(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	db, err := Open(WithDBDirPath(dir))
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 写数据之后并不提交
	wb := db.NewWriteBatch()
	err = wb.Put(getTestKey(1), randomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(getTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(getTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(getTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 删除有效的数据
	wb2 := db.NewWriteBatch()
	err = wb2.Delete(getTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(getTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-2")

	db, err := Open(WithDBDirPath(dir))
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(getTestKey(1), randomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch()
	err = wb.Put(getTestKey(2), randomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(getTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(getTestKey(11), randomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(WithDBDirPath(dir))
	assert.Nil(t, err)

	_, err = db2.Get(getTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// 校验序列号
	assert.Equal(t, uint64(2), db.seqNo)
}

//func TestDB_WriteBatch3(t *testing.T) {
//	opts := DefaultOptions
//	//dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
//	dir := "/tmp/bitcask-go-batch-3"
//	opts.DirPath = dir
//	db, err := Open(opts)
//	//defer destroyDB(db)
//	assert.Nil(t, err)
//	assert.NotNil(t, db)
//
//	keys := db.ListKeys()
//	t.Log(len(keys))
//	//
//	//wbOpts := DefaultWriteBatchOptions
//	//wbOpts.MaxBatchNum = 10000000
//	//wb := db.NewWriteBatch(wbOpts)
//	//for i := 0; i < 500000; i++ {
//	//	err := wb.Put(getTestKey(i), randomValue(1024))
//	//	assert.Nil(t, err)
//	//}
//	//err = wb.Commit()
//	//assert.Nil(t, err)
//}
