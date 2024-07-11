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
	goART "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadixTree defines an ART index
//
// refer to [https://github.com/plar/go-adaptive-radix-tree]
type AdaptiveRadixTree struct {
	tree goART.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goART.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()

	if oldValue == nil {
		return nil
	}

	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()

	value, found := art.tree.Search(key)
	if !found {
		return nil
	}

	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()

	if oldValue == nil {
		return nil, false
	}

	return oldValue.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()

	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()

	return newARTIterator(art.tree, reverse)
}

// artIterator defines an ART index iterator
type artIterator struct {
	// currentIndex defines the current iterating position
	currentIndex int

	// reverse indicates iterating backwards or not
	reverse bool

	// values contains the key and position information
	values []*Item
}

func newARTIterator(tree goART.Tree, reverse bool) *artIterator {
	var index int
	if reverse {
		index = tree.Size() - 1
	}

	values := make([]*Item, tree.Size())
	saveValues := func(node goART.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}

		values[index] = item

		if reverse {
			index--
		} else {
			index++
		}
		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (arti *artIterator) Rewind() {
	arti.currentIndex = 0
}

func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.currentIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.currentIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.currentIndex += 1
}

func (arti *artIterator) Valid() bool {
	return arti.currentIndex < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.currentIndex].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.currentIndex].pos
}

func (arti *artIterator) Close() {
	arti.values = nil
}
