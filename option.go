package bitcask

import "os"

type DBOption func(opt *option)
type IteratorOption func(opt *iteratorOption)
type WriteBatchOption func(opt *writeBatchOption)

type option struct {
	indexerType  IndexerType
	dirPath      string // 存储目录
	syncWrite    bool   // 每次写是否持久化
	bytesPerSync uint32
	dataFileSize int64 // 存储文件大小
}

type iteratorOption struct {
	// 遍历前缀为指定值的 Key，默认为空
	prefix []byte
	// 是否反向遍历，默认 false 是正向
	reverse bool
}

type writeBatchOption struct {
	maxBatchNum int  // 一个批次当中最大的数据量
	syncWrites  bool //	 提交时是否 sync 持久化
}

type IndexerType = byte

const (
	BTree IndexerType = iota
)

var DefaultOption = option{
	indexerType:  BTree,
	dirPath:      os.TempDir(),
	dataFileSize: 256 * 1024 * 1024, // 256MB
	syncWrite:    false,
	bytesPerSync: 0,
}

var DefaultIteratorOption = iteratorOption{
	prefix:  nil,
	reverse: false,
}

var DefaultWriteBatchOption = writeBatchOption{
	maxBatchNum: 10_000,
	syncWrites:  true,
}

func WithWriteSyncWrites(val bool) WriteBatchOption {
	return func(opt *writeBatchOption) {
		opt.syncWrites = val
	}
}

func WithWriteBatchMaxBatchNum(val int) WriteBatchOption {
	return func(opt *writeBatchOption) {
		opt.maxBatchNum = val
	}
}

func WithIteratorPrefix(val []byte) IteratorOption {
	return func(opt *iteratorOption) {
		opt.prefix = val
	}
}

func WithIteratorReverse(val bool) IteratorOption {
	return func(opt *iteratorOption) {
		opt.reverse = val
	}
}

func WithDBIndexerType(val IndexerType) DBOption {
	return func(opt *option) {
		opt.indexerType = val
	}
}

func WithDBDirPath(val string) DBOption {
	return func(opt *option) {
		opt.dirPath = val
	}
}

func WithDBDataFileSize(val int64) DBOption {
	return func(opt *option) {
		opt.dataFileSize = val
	}
}

func WithDBSyncWrite(val bool) DBOption {
	return func(opt *option) {
		opt.syncWrite = val
	}
}

func WithDBBytesPerWrite(val uint32) DBOption {
	return func(opt *option) {
		opt.bytesPerSync = val
	}
}
