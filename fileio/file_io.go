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

package fileio

import "os"

// FileIO is a wrapper for the standard file IO descriptor
type FileIO struct {
	// fd is the system file descriptor
	fd *os.File
}

// NewFileIOManager creates a new FileIO instance
func NewFileIOManager(fileName string) (*FileIO, error) {
	// Open the file in read-write mode
	fd, err := os.OpenFile(
		fileName,
		// O_CREATE: create the file if it does not exist; O_RDWR: read-write mode; O_APPEND: append mode
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		DataFilePermission,
	)

	if err != nil {
		return nil, err
	}

	return &FileIO{fd: fd}, nil
}

// Read reads the corresponding data from a given location in a file
func (f *FileIO) Read(b []byte, offset int64) (int, error) {
	return f.fd.ReadAt(b, offset)
}

// Write writes the given byte array to file
func (f *FileIO) Write(b []byte) (int, error) {
	return f.fd.Write(b)
}

// Sync forces any writes to sync to disk
func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

// Close closes the file
func (f *FileIO) Close() error {
	return f.fd.Close()
}

// Size gets the size of file
func (f *FileIO) Size() (int64, error) {
	fileInfo, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}

	return fileInfo.Size(), nil
}
