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
	"github.com/LiuShuoJiang/betadb/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDatabase_NewIterator(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory
	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestIterator_OneValue(t *testing.T) {
	options := DefaultOptions
	dir, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = dir
	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(114), utils.GetTestKey(114))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()

	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(114), iterator.Key())
	val, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(114), val)
}

func TestIterator_MultipleValues(t *testing.T) {
	options := DefaultOptions
	dir, _ := os.MkdirTemp("", "betadb-iterator")
	options.DirectoryPath = dir
	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("annex"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("crowd"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("average"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("ensure"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("break"), utils.RandomValue(10))
	assert.Nil(t, err)

	// iterate forwards
	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}
	iter1.Close()

	// iterate backwards
	iteratorOptions := DefaultIteratorOptions
	iteratorOptions.Reverse = true
	iter2 := db.NewIterator(iteratorOptions)

	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Rewind()

	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}
	iter2.Close()

	// designate prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("aee")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
}
