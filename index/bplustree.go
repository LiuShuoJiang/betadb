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
	"github.com/LiuShuoJiang/betadb/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bPlusTreeIndexFileName = "bptree-index"

var indexBucketName = []byte("betadb-index")

// BPlusTree defines a B+ tree index
//
// refer to [https://github.com/etcd-io/bbolt]
type BPlusTree struct {
	tree *bbolt.DB
}

// NewBPlusTree initialize a new BPlusTree index
func NewBPlusTree(directoryPath string, syncWrites bool) *BPlusTree {
	options := bbolt.DefaultOptions
	options.NoSync = !syncWrites

	bPTree, err := bbolt.Open(filepath.Join(directoryPath, bPlusTreeIndexFileName), 0644, options)
	if err != nil {
		panic("failed to open BPlusTree!")
	}

	// create new bucket
	if err := bPTree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create buckets in BPlusTree!")
	}

	return &BPlusTree{tree: bPTree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	// no need to add lock here, since BPlusTree has used lock for us
	var oldValue []byte

	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	})
	if err != nil {
		panic("failed to put the value in BPlusTree!")
	}

	if len(oldValue) == 0 {
		return nil
	}

	return data.DecodeLogRecordPos(oldValue)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos

	err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	})
	if err != nil {
		panic("failed to get the value int BPlusTree!")
	}

	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte

	err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldValue = bucket.Get(key); len(oldValue) != 0 {
			return bucket.Delete(key)
		}

		return nil
	})
	if err != nil {
		panic("failed to delete the value in BPlusTree")
	}

	if len(oldValue) == 0 {
		return nil, false
	}

	return data.DecodeLogRecordPos(oldValue), true
}

func (bpt *BPlusTree) Size() int {
	var size int

	err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	})
	if err != nil {
		panic("failed to get the size of BPlusTree")
	}

	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPlusTreeIterator(bpt.tree, reverse)
}

// bPlusTreeIterator wraps a BPlusTree iterator
type bPlusTreeIterator struct {
	tx           *bbolt.Tx
	cursor       *bbolt.Cursor
	reverse      bool
	currentKey   []byte
	currentValue []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *bPlusTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction!")
	}

	bPlusIt := &bPlusTreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}

	bPlusIt.Rewind() // initialize key and value first

	return bPlusIt
}

func (bpti *bPlusTreeIterator) Rewind() {
	if bpti.reverse {
		bpti.currentKey, bpti.currentValue = bpti.cursor.Last()
	} else {
		bpti.currentKey, bpti.currentValue = bpti.cursor.First()
	}
}

func (bpti *bPlusTreeIterator) Seek(key []byte) {
	bpti.currentKey, bpti.currentValue = bpti.cursor.Seek(key)
}

func (bpti *bPlusTreeIterator) Next() {
	if bpti.reverse {
		bpti.currentKey, bpti.currentValue = bpti.cursor.Prev()
	} else {
		bpti.currentKey, bpti.currentValue = bpti.cursor.Next()
	}
}

func (bpti *bPlusTreeIterator) Valid() bool {
	return len(bpti.currentKey) != 0
}

func (bpti *bPlusTreeIterator) Key() []byte {
	return bpti.currentKey
}

func (bpti *bPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currentValue)
}

func (bpti *bPlusTreeIterator) Close() {
	_ = bpti.tx.Rollback()
}
