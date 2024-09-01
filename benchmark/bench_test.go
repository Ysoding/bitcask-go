package benchmark

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ysoding/bitcask"
)

var db *bitcask.DB

func init() {
	// 初始化用于基准测试的存储引擎
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")

	var err error
	db, err = bitcask.Open(bitcask.WithDBDirPath(dir))
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

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(getTestKey(i), randomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(getTestKey(i), randomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(getTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(getTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
