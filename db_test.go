package bitcask

import (
	"fmt"
	"os"
	"testing"
	"time"

	"math/rand"

	"github.com/stretchr/testify/assert"
)

func removeDB(db *DB) {
	if db == nil {
		return
	}

	if db.activeFile != nil {
		db.Close()
	}

	err := os.RemoveAll(db.dirPath)
	if err != nil {
		panic(err)
	}

}

var (
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func getTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-go-key-%09d", i))
}

func randomValue(n int) []byte {
	b := make([]byte, n)

	for i := range b {
		b[i] = letters[randStr.Intn(len(letters))]
	}
	return []byte("bitcask-go-value-" + string(b))
}

func TestOpen(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go")
	db, err := Open(WithDirPath(dir))
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-put")

	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	key := getTestKey(1)
	val := randomValue(24)
	err = db.Put(key, val)
	assert.Nil(t, err)

	res, err := db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	// 2.重复 Put key 相同的数据
	err = db.Put(key, val)
	assert.Nil(t, err)
	res, err = db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

	// 3.key 为空
	err = db.Put(nil, val)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	key = getTestKey(2)
	err = db.Put(key, nil)
	assert.Nil(t, err)

	res, err = db.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(res))

	// 5.写到数据文件进行了转换

	val = randomValue(128)
	for i := 0; i < 1000000; i++ {
		err := db.Put(getTestKey(i), val)
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.oldFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts...)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	key = getTestKey(55)
	err = db2.Put(key, val)
	assert.Nil(t, err)

	res, err = db2.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, val, res)

}

func TestDB_Get(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-get")
	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(getTestKey(11), randomValue(24))
	assert.Nil(t, err)

	val1, err := db.Get(getTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(getTestKey(22), randomValue(24))
	assert.Nil(t, err)

	err = db.Put(getTestKey(22), randomValue(24))
	assert.Nil(t, err)

	val3, err := db.Get(getTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(getTestKey(33), randomValue(24))
	assert.Nil(t, err)

	err = db.Delete(getTestKey(33))
	assert.Nil(t, err)

	val4, err := db.Get(getTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; i < 1000000; i++ {
		err := db.Put(getTestKey(i), randomValue(128))
		assert.Nil(t, err)
	}

	assert.Equal(t, 2, len(db.oldFiles))

	val5, err := db.Get(getTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts...)
	assert.Nil(t, err)

	val6, err := db2.Get(getTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(getTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(getTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "bitcask-go-delete")
	opts := []DBOption{WithDirPath(dir), WithDataFileSize(64 * 1024 * 1024)}

	db, err := Open(opts...)
	defer removeDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(getTestKey(11), randomValue(128))
	assert.Nil(t, err)

	err = db.Delete(getTestKey(11))
	assert.Nil(t, err)

	_, err = db.Get(getTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(getTestKey(22), randomValue(128))
	assert.Nil(t, err)

	err = db.Delete(getTestKey(22))
	assert.Nil(t, err)

	err = db.Put(getTestKey(22), randomValue(128))
	assert.Nil(t, err)

	val1, err := db.Get(getTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts...)
	assert.Nil(t, err)

	_, err = db2.Get(getTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(getTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}
