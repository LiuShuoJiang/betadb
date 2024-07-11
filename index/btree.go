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
	"sort"
	"sync"
)

// BTree defines the BTree index
//
// it mainly encapsulates Google's btree library: [https://github.com/google/btree]
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree constructor creates a new BTree index structure
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}

	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()

	if oldItem == nil {
		return nil
	}

	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}

	bTreeItem := bt.tree.Get(it)
	if bTreeItem == nil {
		return nil
	}

	return bTreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}

	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()

	if oldItem == nil {
		return nil, false
	}

	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return newBTreeIterator(bt.tree, reverse)
}

type bTreeIterator struct {
	// currentIndex defines the current iterating index position
	currentIndex int

	// reverse determines whether we are iterating backwards
	reverse bool

	// values stores the key and positional indexing information
	values []*Item
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// put all the data into the array
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &bTreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (bti *bTreeIterator) Rewind() {
	bti.currentIndex = 0
}

func (bti *bTreeIterator) Seek(key []byte) {
	if bti.reverse {
		// use binary search
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currentIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

func (bti *bTreeIterator) Next() {
	bti.currentIndex += 1
}

func (bti *bTreeIterator) Valid() bool {
	return bti.currentIndex < len(bti.values)
}

func (bti *bTreeIterator) Key() []byte {
	return bti.values[bti.currentIndex].key
}

func (bti *bTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currentIndex].pos
}

func (bti *bTreeIterator) Close() {
	bti.values = nil
}
