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

package index

import (
	"bytes"
	"github.com/LiuShuoJiang/betadb/data"
	"github.com/google/btree"
)

// Indexer is the abstract index interface
// If there are other data structures that require integration, implement this interface directly
type Indexer interface {
	// Put stores information about the location of the data corresponding to the key in the index
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get Fetches the index position information according to the key
	Get(key []byte) *data.LogRecordPos

	// Delete deletes the index position information according to the key
	Delete(key []byte) (*data.LogRecordPos, bool)

	// Size defines the size of index
	Size() int

	// Iterator defines an iterator to iterator over the index
	Iterator(reverse bool) Iterator

	// Close closes the index
	Close() error
}

type IndexType = int8

const (
	// Btree indicates btree index
	Btree IndexType = iota + 1

	// ART indicates Adaptive Radix Tree index
	ART

	// BPTree indicates b+tree index
	BPTree
)

// NewIndexer initializes the index according to the data structure type
func NewIndexer(tp IndexType, directoryPath string, sync bool) Indexer {
	switch tp {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(directoryPath, sync)
	default:
		panic("unsupported index type!")
	}
}

// Item defines each item to be inserted into the BTree structure
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less compares the current item with the right-hand side item
// it can be used to determine the order of the item in the BTree
func (i *Item) Less(rhs btree.Item) bool {
	return bytes.Compare(i.key, rhs.(*Item).key) == -1
}

// Iterator defines a generic index iterator
type Iterator interface {
	// Rewind returns to the start (first item) of the iterator
	Rewind()

	// Seek finds the first target key that is greater than (or less than) or equal to the key passed in
	// and starts traversing from this key
	Seek(key []byte)

	// Next jumps to the next key
	Next()

	// Valid checks the validity
	// by checking whether all keys have been traversed, which can be used to exit traversal
	Valid() bool

	// Key returns the current iterating Key data
	Key() []byte

	// Value returns the current iterating Value data
	Value() *data.LogRecordPos

	// Close closes the iterator, freeing the resources
	Close()
}
