package data

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecodDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordPos struct {
	FileID uint32
	Size   uint32
	Offset int64
}
