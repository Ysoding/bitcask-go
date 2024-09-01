package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"github.com/ysoding/bitcask/fio"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

type DataFile struct {
	FileID      uint32
	WriteOffset int64
	IoManager   fio.IOManager
}

func OpenDataFile(dbPath string, fileID uint32, ioType fio.IOType) (*DataFile, error) {
	return newDataFile(GetDataFileName(dbPath, fileID), fileID, ioType)
}

// OpenSeqNoFile 存储事务序列号的文件
func OpenSeqNoFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StandardFileIO)
}

// OpenHintFile 打开 Hint 索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.StandardFileIO)
}

// OpenMergeFinishedFile 打开标识 merge 完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StandardFileIO)
}

func newDataFile(fileName string, fileID uint32, ioType fio.IOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(ioType, fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileID:      fileID,
		WriteOffset: 0,
		IoManager:   ioManager,
	}, nil
}

// ReadLogRecord 从数据文件中读取offset的LogRecord
func (d *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := d.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// 如果读取的最大 header 长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	headerBytes := int64(maxLogRecordHeaderSize)
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// read header
	headerBuf, err := d.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 下面的两个条件表示读取到了文件末尾，直接返回 EOF 错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize := int64(header.keySize)
	valueSize := int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.recordType}
	if keySize > 0 || valueSize > 0 {
		keyBuf, err := d.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = keyBuf[:keySize]
		logRecord.Value = keyBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil

}

func (d *DataFile) readNBytes(n int64, offset int64) ([]byte, error) {
	buf := make([]byte, n)
	_, err := d.IoManager.ReadAt(buf, offset)
	return buf, err
}

func (d *DataFile) Sync() error {
	return d.IoManager.Sync()
}

func (d *DataFile) Close() error {
	return d.IoManager.Close()
}

func GetDataFileName(dbPath string, fileID uint32) string {
	return filepath.Join(dbPath, fmt.Sprintf("%09d%s", fileID, DataFileNameSuffix))
}

func (d *DataFile) Write(buf []byte) error {
	n, err := d.IoManager.Write(buf)
	if err != nil {
		return err
	}
	d.WriteOffset += int64(n)
	return nil
}

// WriteHintRecord 写入索引信息到 hint 文件中
func (d *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{Key: key, Value: EncodeLogRecordPos(pos)}
	encRecord, _ := EncodeLogRecord(record)
	return d.Write(encRecord)
}

func (d *DataFile) SetIOManager(dirPath string, ioType fio.IOType) error {
	if err := d.IoManager.Close(); err != nil {
		return err
	}

	ioManager, err := fio.NewIOManager(ioType, GetDataFileName(dirPath, d.FileID))
	if err != nil {
		return err
	}

	d.IoManager = ioManager
	return nil
}
