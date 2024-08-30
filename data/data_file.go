package data

import (
	"fmt"
	"path/filepath"

	"github.com/ysoding/bitcask/fio"
)

const (
	DataFileNameSuffix = ".data"
)

type DataFile struct {
	FileID      uint32
	WriteOffset int64
	fio.IOManager
}

func OpenDataFile(dbPath string, fileID uint32, ioType fio.IOType) (*DataFile, error) {
	return newDataFile(GetDataFileName(dbPath, fileID), fileID, ioType)
}

func (d *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func GetDataFileName(dbPath string, fileID uint32) string {
	return filepath.Join(dbPath, fmt.Sprintf("%09d%s", fileID, DataFileNameSuffix))
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

func newDataFile(fileName string, fileID uint32, ioType fio.IOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(ioType, fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileID:      fileID,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}
