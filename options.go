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

package betadb

import "os"

type Options struct {
	// DataDirectoryPath is the path to the data directory
	DirectoryPath string

	// DataFileSize is the size of the data file
	DataFileSize int64

	// SyncWrites indicates whether to sync for every write to disk
	SyncWrites bool

	// BytesPerSync indicates the cumulative number of bytes written before syncing to disk
	BytesPerSync uint

	// IndexType defines the type for index
	IndexType IndexerType

	// MMapAtStartUp indicates whether to use mmap to load the data file at startup
	MMapAtStartUp bool

	// DataFileMergeRatio indicates the threshold of the data file size to the merge size
	DataFileMergeRatio float32
}

// IteratorOptions defines the index iterator configuration options
type IteratorOptions struct {
	// Prefix denotes the iteration for the key with given prefix, default null
	Prefix []byte

	// Reverse indicates whether to traverse in reverse direction
	// the default value is false, which means forward traversal
	Reverse bool
}

// WriteBatchOptions defines batch writing configuration options
type WriteBatchOptions struct {
	// MaxBatchNum denotes the max data size within a batch
	MaxBatchNum uint

	// SyncWrites denotes whether to sync the disk when commiting
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree indicates btree index
	BTree IndexerType = iota + 1

	// ART indicates Adaptive Radix Tree index
	ART

	// BPlusTree indicates b+tree index
	BPlusTree
)

var DefaultOptions = Options{
	DirectoryPath:      os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexType:          BTree,
	MMapAtStartUp:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
