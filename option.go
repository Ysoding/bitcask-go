package bitcask

import "os"

type DBOption func(opt *option)
type IteratorOption func(opt *iteratorOption)

type option struct {
	indexerType  IndexerType
	dirPath      string // 存储目录
	syncWrite    bool   // 每次写是否持久化
	bytesPerSync uint32
	dataFileSize int64 // 存储文件大小
}

type iteratorOption struct {
	// 遍历前缀为指定值的 Key，默认为空
	Prefix []byte
	// 是否反向遍历，默认 false 是正向
	Reverse bool
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
}

var DefaultIteratorOption = iteratorOption{
	Prefix:  nil,
	Reverse: false,
}

func WithIteratorPrefix(val []byte) IteratorOption {
	return func(opt *iteratorOption) {
		opt.Prefix = val
	}
}

func WithIteratorReverse(val bool) IteratorOption {
	return func(opt *iteratorOption) {
		opt.Reverse = val
	}
}

func WithIndexerType(val IndexerType) DBOption {
	return func(opt *option) {
		opt.indexerType = val
	}
}

func WithDirPath(val string) DBOption {
	return func(opt *option) {
		opt.dirPath = val
	}
}

func WithDataFileSize(val int64) DBOption {
	return func(opt *option) {
		opt.dataFileSize = val
	}
}

func WithSyncWrite(val bool) DBOption {
	return func(opt *option) {
		opt.syncWrite = val
	}
}

func WithBytesPerWrite(val uint32) DBOption {
	return func(opt *option) {
		opt.bytesPerSync = val
	}
}
