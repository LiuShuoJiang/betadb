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
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// test the normal type of data
	record1 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb"),
		Type:  LogRecordNormal,
	}
	result1, len1 := EncodeLogRecord(record1)
	// t.Log(result1)
	// t.Log(len1)
	assert.NotNil(t, result1)
	assert.Greater(t, len1, int64(5))

	// test when the value is empty
	record2 := &LogRecord{
		Key:  []byte("engine"),
		Type: LogRecordNormal,
	}
	result2, len2 := EncodeLogRecord(record2)
	// t.Log(result2)
	// t.Log(len2)
	assert.NotNil(t, result2)
	assert.Greater(t, len2, int64(5))

	// test when the type is deleted
	record3 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb"),
		Type:  LogRecordDeleted,
	}
	result3, len3 := EncodeLogRecord(record3)
	// t.Log(result3)
	// t.Log(len3)
	assert.NotNil(t, result3)
	assert.Greater(t, len3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	// test the normal type of data
	headerBuffer1 := []byte{77, 26, 80, 17, 0, 12, 12}
	header1, size1 := decodeLogRecordHeader(headerBuffer1)
	assert.NotNil(t, header1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(290462285), header1.crc)
	assert.Equal(t, LogRecordNormal, header1.recordType)
	assert.Equal(t, uint32(6), header1.keySize)
	assert.Equal(t, uint32(6), header1.valueSize)

	// test when the value is empty
	headerBuffer2 := []byte{207, 186, 204, 232, 0, 12, 0}
	header2, size2 := decodeLogRecordHeader(headerBuffer2)
	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(3905731279), header2.crc)
	assert.Equal(t, LogRecordNormal, header2.recordType)
	assert.Equal(t, uint32(6), header2.keySize)
	assert.Equal(t, uint32(0), header2.valueSize)

	// test when the type is deleted
	headerBuffer3 := []byte{165, 193, 171, 168, 1, 12, 12}
	header3, size3 := decodeLogRecordHeader(headerBuffer3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(2829828517), header3.crc)
	assert.Equal(t, LogRecordDeleted, header3.recordType)
	assert.Equal(t, uint32(6), header3.keySize)
	assert.Equal(t, uint32(6), header3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	// test the normal type of data
	record1 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb"),
		Type:  LogRecordNormal,
	}
	headerBuffer1 := []byte{77, 26, 80, 17, 0, 12, 12}
	crc1 := getLogRecordCRC(record1, headerBuffer1[crc32.Size:])
	assert.Equal(t, uint32(290462285), crc1)

	// test when the value is empty
	record2 := &LogRecord{
		Key:  []byte("engine"),
		Type: LogRecordNormal,
	}
	headerBuffer2 := []byte{207, 186, 204, 232, 0, 12, 0}
	crc2 := getLogRecordCRC(record2, headerBuffer2[crc32.Size:])
	assert.Equal(t, uint32(3905731279), crc2)

	// test when the type is deleted
	record3 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb"),
		Type:  LogRecordDeleted,
	}
	headerBuffer3 := []byte{165, 193, 171, 168, 1, 12, 12}
	crc3 := getLogRecordCRC(record3, headerBuffer3[crc32.Size:])
	assert.Equal(t, uint32(2829828517), crc3)
}
