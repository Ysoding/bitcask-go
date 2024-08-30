package bitcask

import "os"

type DBOption func(opt *option)

type option struct {
	indexerType  IndexerType
	dirPath      string // 存储目录
	syncWrite    bool   // 每次写是否持久化
	bytesPerSync uint32
	dataFileSize int64 // 存储文件大小
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
