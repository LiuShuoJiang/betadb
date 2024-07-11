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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	// Put a nil key
	result1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, result1)

	// Get the nil key
	result2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, result2)

	// Put the same key
	result3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 11, Offset: 12})
	assert.Equal(t, result3.Fid, uint32(1))
	assert.Equal(t, result3.Offset, int64(2))
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	// Put a nil key
	result1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, result1)

	// Get the nil key
	pos1 := bt.Get(nil)
	assert.Equal(t, pos1.Fid, uint32(1))
	assert.Equal(t, pos1.Offset, int64(100))

	// Put the same key
	result2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, result2)

	// Put the same key
	result3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.Equal(t, result3.Fid, uint32(1))
	assert.Equal(t, result3.Offset, int64(2))

	// Get the same key
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, pos2.Fid, uint32(1))
	assert.Equal(t, pos2.Offset, int64(3))
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	// Put a nil key
	result1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, result1)

	// Get the nil key
	result2, ok := bt.Delete(nil)
	assert.True(t, ok)
	assert.Equal(t, result2.Fid, uint32(1))
	assert.Equal(t, result2.Offset, int64(100))

	// Put the same key
	result3 := bt.Put([]byte("some"), &data.LogRecordPos{Fid: 42, Offset: 35})
	assert.Nil(t, result3)

	// Put the same key
	result4, ok := bt.Delete([]byte("some"))
	assert.True(t, ok)
	assert.Equal(t, result4.Fid, uint32(42))
	assert.Equal(t, result4.Offset, int64(35))
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	// (1) test for a null BTree
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// (2) test for bTree with value
	bt1.Put([]byte("golang"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// (3) test for multiple data entries
	bt1.Put([]byte("awsl"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("java"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("dart"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// (4) test for seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("bxt")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// (5) test for reversing seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zzz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
