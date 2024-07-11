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

// DataFilePermission is the permission for the data file
// 0644: user can read and write, group can read, others can read
const DataFilePermission = 0644

type FileIOType = byte

const (
	// StandardFileIO is the common standard file IO option
	StandardFileIO FileIOType = iota

	// MemoryMap is the MMAP file IO option
	MemoryMap
)

// IOManager is an abstract IO management interface that can integrate different IO types
// currently supports standard file IO
type IOManager interface {
	// Read reads the corresponding data from a given location in a file
	Read([]byte, int64) (int, error)

	// Write writes the given byte array to file
	Write([]byte) (int, error)

	// Sync forces any writes to sync to disk
	Sync() error

	// Close closes the file
	Close() error

	// Size gets the size of file
	Size() (int64, error)
}

// NewIOManager initializes IOManager, currently only supports standard FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFileIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported IO type, use standard IO or mmap")
	}
}
