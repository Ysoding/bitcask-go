package data

import "github.com/ysoding/bitcask/fio"

type DataFile struct {
	FileID      uint32
	WriteOffset int64
	fio.IOManager
}

func (d *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
