package data

type LogRecord struct {
	Key   []byte
	Value []byte
}

type LogRecordPos struct {
	Fid    uint32 // 文件fid
	Size   uint32 // logrecord大小
	Offset int64  // 在文件中的offset
}
