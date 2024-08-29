package data

type LogRecord struct {
	Key   []byte
	Value []byte
}

type LogRecordPos struct {
	FileID uint32
	Size   uint32
	Offset int64
}
