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

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()

	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 24})
	assert.Nil(t, res1)

	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 24})
	assert.Nil(t, res2)

	res3 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 24})
	assert.Nil(t, res3)

	res4 := art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 114, Offset: 514})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(24), res4.Offset)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("key does not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("key does not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 24})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(24), res2.Offset)

	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 114})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 114})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 114})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 24})
	art.Put([]byte("java"), &data.LogRecordPos{Fid: 1, Offset: 24})
	art.Put([]byte("golang"), &data.LogRecordPos{Fid: 1, Offset: 24})
	art.Put([]byte("python"), &data.LogRecordPos{Fid: 1, Offset: 24})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
