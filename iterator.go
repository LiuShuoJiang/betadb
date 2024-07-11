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

import (
	"bytes"
	"github.com/LiuShuoJiang/betadb/index"
)

// Iterator defines the iterator for user to operate
type Iterator struct {
	indexIter index.Iterator
	db        *Database
	options   IteratorOptions
}

// NewIterator initializes the Iterator struct
func (db *Database) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
}

// Rewind returns to the starting point of the iterator, that is, the first data
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// Seek finds the first target key that is greater than (or less than) or equal to the key passed in
// and starts traversing from this key
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// Next jumps to the next key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// Valid checks if all the key has been iterated, used for exiting the iteration
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// Key gets the current iterating key data
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// Value gets the current iterating value data by byte array
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()

	return it.db.getValueByPosition(logRecordPos)
}

// Close closes the iterator to free resources
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
