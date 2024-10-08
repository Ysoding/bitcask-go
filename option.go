package bitcask

import "os"

type DBOption func(opt *option)
type IteratorOption func(opt *iteratorOption)
type WriteBatchOption func(opt *writeBatchOption)

type option struct {
	indexerType        IndexerType
	dirPath            string // 存储目录
	syncWrite          bool   // 每次写是否持久化
	bytesPerSync       uint32
	dataFileSize       int64   // 存储文件大小
	mmapAtStartUp      bool    // 启动时是否使用 MMap 加载数据
	dataFileMergeRatio float32 //	数据文件合并的阈值
}

type iteratorOption struct {
	prefix  []byte // 遍历前缀为指定值的 Key，默认为空
	reverse bool   // 是否反向遍历，默认 false 是正向
}

type writeBatchOption struct {
	maxBatchNum int  // 一个批次当中最大的数据量
	syncWrite   bool //	 提交时是否 sync 持久化
}

type IndexerType = byte

const (
	BTree IndexerType = iota

	// ART Adpative Radix Tree 自适应基数树索引
	ART

	// BPlusTree B+ 树索引，将索引存储到磁盘上
	BPlusTree
)

var DefaultOption = option{
	indexerType:        BTree,
	dirPath:            os.TempDir(),
	dataFileSize:       256 * 1024 * 1024, // 256MB
	syncWrite:          false,
	bytesPerSync:       0,
	mmapAtStartUp:      true,
	dataFileMergeRatio: 0.5,
}

var DefaultIteratorOption = iteratorOption{
	prefix:  nil,
	reverse: false,
}

var DefaultWriteBatchOption = writeBatchOption{
	maxBatchNum: 10_000,
	syncWrite:   true,
}

func WithWriteSyncWrites(val bool) WriteBatchOption {
	return func(opt *writeBatchOption) {
		opt.syncWrite = val
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

func WithDBDataFileMergeRatio(val float32) DBOption {
	return func(opt *option) {
		opt.dataFileMergeRatio = val
	}
}

func WithDBMmapAtStartUp(val bool) DBOption {
	return func(opt *option) {
		opt.mmapAtStartUp = val
	}
}
