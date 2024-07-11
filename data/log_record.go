/*
 * Copyright (c) 2024. Shuojiang Liu.
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

// "crc" "type" "keySize" "valueSize"
//
//	4  +  1   + (max)5  +  (max)5   bytes
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord is a record written to a data file consisting Key, Value and Type
// It's called a log because the data in the data file is written in an append format, similar to a log
type LogRecord struct {
	Key   []byte
	Value []byte
	// Type indicates the type of the log record
	// it may be a normal record, a deleted record (tombstone value), or a transaction finished record
	Type LogRecordType
}

// logRecordHeader defines the header information before LogRecord
type logRecordHeader struct {
	// crc is the CRC checksum
	crc uint32
	// recordType is the Type field of LogRecord
	recordType LogRecordType
	// keySize is the length of key
	keySize uint32
	// valueSize is the length of value
	valueSize uint32
}

// LogRecordPos defines the data index information consisting Fid, Offset and Size
// It describes the data position in disks (a.k.a, each entry within "keydir")
type LogRecordPos struct {
	// Fid is File id, indicates the file to which the data is stored
	Fid uint32
	// Offset indicates where in the data file the data is stored
	Offset int64
	// Size indicates the size of the file on disk
	Size uint32
}

// TransactionRecord temporarily stores transaction-related data
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord encodes the LogRecord (easier for file writing)
// and returns the byte array and length
//
// +--------------------+----------------+-----------------------+-----------------------+------------+--------------+
// | crc checksum value | type of record |       key size        |      value size       | actual key | actual value |
// +--------------------+----------------+-----------------------+-----------------------+------------+--------------+
//
//	4 bytes            1 byte        variable(max 5 bytes)   variable(max 5 bytes)    variable      variable
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// initialize a byte array representing the header part
	header := make([]byte, maxLogRecordHeaderSize)

	// the 5th byte stores type info
	header[4] = logRecord.Type
	var index = 5

	// we store the length of key and value after the 5th byte
	// using variable length types to save space
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encodeBytes := make([]byte, size)

	// copy the header info to the to-be-returned array
	copy(encodeBytes[:index], header[:index])

	// copy the actual key and value to the to-be-returned array directly
	copy(encodeBytes[index:], logRecord.Key)
	copy(encodeBytes[index+len(logRecord.Key):], logRecord.Value)

	// finally, perform crc checksums on the entire LogRecord
	crc := crc32.ChecksumIEEE(encodeBytes[4:])
	binary.LittleEndian.PutUint32(encodeBytes[:4], crc)

	// fmt.Printf("header length: %d, crc: %d\n", index, crc)

	return encodeBytes, int64(size)
}

// EncodeLogRecordPos encodes the LogRecordPos position information
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buffer := make([]byte, binary.MaxVarintLen32*2+binary.MaxVarintLen64)
	var index = 0

	index += binary.PutVarint(buffer[index:], int64(pos.Fid))
	index += binary.PutVarint(buffer[index:], pos.Offset)
	index += binary.PutVarint(buffer[index:], int64(pos.Size))

	return buffer[:index]
}

// DecodeLogRecordPos decodes the byte array into LogRecordPos
func DecodeLogRecordPos(buffer []byte) *LogRecordPos {
	var index = 0

	fileID, numBytes := binary.Varint(buffer[index:])
	index += numBytes

	offset, numBytes := binary.Varint(buffer[index:])
	index += numBytes

	size, _ := binary.Varint(buffer[index:])

	return &LogRecordPos{
		Fid:    uint32(fileID),
		Offset: offset,
		Size:   uint32(size),
	}
}

// decodeLogRecordHeader decodes the header information from the byte array
// also returns the length of header
func decodeLogRecordHeader(buffer []byte) (*logRecordHeader, int64) {
	if len(buffer) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buffer[:4]),
		recordType: buffer[4],
	}

	var index = 5 // not start from the 6-th byte

	// get the key size
	keySize, n := binary.Varint(buffer[index:])
	header.keySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buffer[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// getLogRecordCRC returns the CRC code from LogRecord
func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])

	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}
