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
	"errors"
	"fmt"
	"github.com/LiuShuoJiang/betadb/fileio"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC value, log record might be corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// DataFile defines the IO format of data file
type DataFile struct {
	// FileID is the unique identifier of the data file
	FileID uint32

	// WriteOffset indicates the current writing offset of the data file
	WriteOffset int64

	// FileIOManager is the file IO manager
	IoManager fileio.IOManager
}

// newDataFile creates a new data file
func newDataFile(fileName string, fileID uint32, ioType fileio.FileIOType) (*DataFile, error) {
	// initialize IOManager interface
	ioManager, err := fileio.NewIOManager(fileName, ioType)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileID:      fileID,
		WriteOffset: 0,
		IoManager:   ioManager,
	}, nil
}

// GetDataFileName is a utility function to return the data file name
func GetDataFileName(directoryPath string, fileID uint32) string {
	return filepath.Join(directoryPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)
}

// OpenDataFile opens a new data file
func OpenDataFile(directoryPath string, fileID uint32, ioType fileio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(directoryPath, fileID)
	return newDataFile(fileName, fileID, ioType)
}

// OpenHintFile opens the hint index file
func OpenHintFile(directoryPath string) (*DataFile, error) {
	fileName := filepath.Join(directoryPath, HintFileName)
	return newDataFile(fileName, 0, fileio.StandardFileIO)
}

// OpenMergeFinishedFile opens the file that indicates the merge process has finished
func OpenMergeFinishedFile(directoryPath string) (*DataFile, error) {
	fileName := filepath.Join(directoryPath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fileio.StandardFileIO)
}

// OpenSeqNoFile opens the file that stores the transaction sequence number
func OpenSeqNoFile(directoryPath string) (*DataFile, error) {
	fileName := filepath.Join(directoryPath, SeqNoFileName)
	return newDataFile(fileName, 0, fileio.StandardFileIO)
}

// ReadLogRecord reads LogRecord from the data file according to offset
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	// Special case: if the maximum header length to be read already exceeds the length of the file
	// just read to the end of the file
	// otherwise we will get EOF error!
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// read header information
	headerBuffer, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := decodeLogRecordHeader(headerBuffer)
	// if we are reading towards the end of file, return EOF error
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// get the corresponding key length and value length
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	// get the LogRecord length
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	// start reading the key/value data actually stored by the user
	if keySize > 0 || valueSize > 0 {
		kvBuffer, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// fetch key and value directly
		logRecord.Key = kvBuffer[:keySize]
		logRecord.Value = kvBuffer[keySize:]
	}

	// verify the validity of data
	crc := getLogRecordCRC(logRecord, headerBuffer[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, recordSize, nil
}

// Write writes the given byte array to the data file
func (df *DataFile) Write(buffer []byte) error {
	numBytes, err := df.IoManager.Write(buffer)
	if err != nil {
		return err
	}
	// note that the offset is updated after the write operation
	df.WriteOffset += int64(numBytes)

	return nil
}

// WriteHintRecord writes the hint record to the hint index file
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}

	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

// Sync forces any writes to sync to disk
func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

// Close closes the data file
func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

// SetIOManager sets the IO manager for the data file
func (df *DataFile) SetIOManager(directoryPath string, ioType fileio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}

	ioManager, err := fileio.NewIOManager(GetDataFileName(directoryPath, df.FileID), ioType)
	if err != nil {
		return err
	}

	df.IoManager = ioManager
	return nil
}

// readNBytes is a utility function that reads n bytes from the data file
func (df *DataFile) readNBytes(numBytes int64, offset int64) (b []byte, err error) {
	b = make([]byte, numBytes)
	_, err = df.IoManager.Read(b, offset)
	return
}
