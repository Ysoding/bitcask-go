package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// crc 	type 	keySize valueSize
// 4   +  1  + 	5     + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

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

// TransactionRecord 暂存的事务相关的数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

type logRecordHeader struct {
	crc        uint32 // crc检验值
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	index := 0

	fileID, n := binary.Varint(buf[index:])
	index += n

	offset, n := binary.Varint(buf[index:])
	index += n

	size, _ := binary.Varint(buf[index:])
	return &LogRecordPos{FileID: uint32(fileID), Offset: offset, Size: uint32(size)}
}

// EncodeLogRecordPos 对位置信息进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.FileID))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度
//
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	| crc 校验值  |  type 类型   |    key size |   value size |      key    |      value   |
//	+-------------+-------------+-------------+--------------+-------------+--------------+
//	    4字节          1字节        变长（最大5）   变长（最大5）     变长           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	headerBuf := make([]byte, maxLogRecordHeaderSize)

	headerBuf[4] = byte(logRecord.Type)

	index := 5
	index += binary.PutVarint(headerBuf[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(headerBuf[index:], int64(len(logRecord.Value)))

	size := index + len(logRecord.Key) + len(logRecord.Value)
	encBuf := make([]byte, size)

	// copy header
	copy(encBuf[:index], headerBuf[:index])
	// copy key
	copy(encBuf[index:], logRecord.Key)
	// copy value
	copy(encBuf[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBuf[crc32.Size:])
	binary.LittleEndian.PutUint32(encBuf[:crc32.Size], crc)

	return encBuf, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: LogRecordType(buf[4]),
	}

	index := 5

	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(logRecord *LogRecord, curBuf []byte) uint32 {
	crc := crc32.ChecksumIEEE(curBuf)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key)
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)
	return crc
}
