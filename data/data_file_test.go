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
	"github.com/LiuShuoJiang/betadb/fileio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	// t.Log(os.TempDir())

	dataFile2, err := OpenDataFile(os.TempDir(), 114, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(os.TempDir(), 114, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("xyzabc"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("defghi"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("jklmno"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 115, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("xyz"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 116, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("mnopqrst"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1145, fileio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// test for only one LogRecord
	record1 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb"),
	}
	result1, size1 := EncodeLogRecord(record1)
	err = dataFile.Write(result1)
	assert.Nil(t, err)

	readRecord1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, record1, readRecord1)
	assert.Equal(t, size1, readSize1)

	// test for multiple LogRecord, reading from different positions
	record2 := &LogRecord{
		Key:   []byte("engine"),
		Value: []byte("betadb new"),
	}
	result2, size2 := EncodeLogRecord(record2)
	err = dataFile.Write(result2)
	assert.Nil(t, err)

	// remember to read from the new offset
	readRecord2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, record2, readRecord2)
	assert.Equal(t, size2, readSize2)

	// test the special case where the deleted data is at the end of the data file
	record3 := &LogRecord{
		Key:   []byte("2"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	result3, size3 := EncodeLogRecord(record3)
	err = dataFile.Write(result3)
	assert.Nil(t, err)

	readRecord3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, record3, readRecord3)
	assert.Equal(t, size3, readSize3)
}
