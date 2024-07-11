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
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	result1 := tree.Put([]byte("java"), &data.LogRecordPos{
		Fid:    114,
		Offset: 514,
	})
	assert.Nil(t, result1)

	result2 := tree.Put([]byte("python"), &data.LogRecordPos{
		Fid:    114,
		Offset: 514,
	})
	assert.Nil(t, result2)

	result3 := tree.Put([]byte("golang"), &data.LogRecordPos{
		Fid:    114,
		Offset: 514,
	})
	assert.Nil(t, result3)

	result4 := tree.Put([]byte("golang"), &data.LogRecordPos{
		Fid:    1919,
		Offset: 810,
	})
	assert.Equal(t, uint32(114), result4.Fid)
	assert.Equal(t, int64(514), result4.Offset)
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("something that does not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("golang"), &data.LogRecordPos{Fid: 114, Offset: 514})
	pos1 := tree.Get([]byte("golang"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("golang"), &data.LogRecordPos{Fid: 1919, Offset: 810})
	pos2 := tree.Get([]byte("golang"))
	assert.NotNil(t, pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	result1, ok1 := tree.Delete([]byte("something that does not exist"))
	assert.False(t, ok1)
	assert.Nil(t, result1)

	tree.Put([]byte("java"), &data.LogRecordPos{Fid: 114, Offset: 514})
	result2, ok2 := tree.Delete([]byte("java"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(114), result2.Fid)
	assert.Equal(t, int64(514), result2.Offset)

	pos1 := tree.Get([]byte("java"))
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	assert.Equal(t, 0, tree.Size())

	tree.Put([]byte("ansi-c"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("cpp20"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("cpp23"), &data.LogRecordPos{Fid: 114, Offset: 514})

	assert.Equal(t, 3, tree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	tree.Put([]byte("cpp"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("java"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("python"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("golang"), &data.LogRecordPos{Fid: 114, Offset: 514})
	tree.Put([]byte("javascript"), &data.LogRecordPos{Fid: 114, Offset: 514})

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		// t.Log(string(iter.Key()))
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
