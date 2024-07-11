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
	"sync"
	"testing"
)

// TestDatabase_MergeNull tests for merging without any data
func TestDatabase_MergeNull(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Merge()
	assert.Nil(t, err)
}

// TestDatabase_MergeValid tests for merging valid data
func TestDatabase_MergeValid(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DataFileSize = 32 * 1024 * 1024
	options.DataFileMergeRatio = 0
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart database
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	defer func() {
		_ = db2.Close()
	}()

	assert.Nil(t, err)
	keys := db2.ListKeys()
	assert.Equal(t, 50000, len(keys))

	for i := 0; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// TestDatabase_MergeInvalidAndMultiplePuts test for merging data that is valid or being put for multiple times
func TestDatabase_MergeInvalidAndMultiplePuts(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DataFileSize = 32 * 1024 * 1024
	options.DataFileMergeRatio = 0
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 10000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 40000; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), []byte("some new value to merge"))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart database
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	defer func() {
		_ = db2.Close()
	}()

	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 40000, len(keys))

	for i := 0; i < 10000; i++ {
		_, err := db2.Get(utils.GetTestKey(i))
		assert.Equal(t, ErrKeyNotFound, err)
	}

	for i := 40000; i < 50000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("some new value to merge"), val)
	}
}

// TestDatabase_MergeInvalid tests for merging all invalid data
func TestDatabase_MergeInvalid(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DataFileSize = 32 * 1024 * 1024
	options.DataFileMergeRatio = 0
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	for i := 0; i < 50000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	err = db.Merge()
	assert.Nil(t, err)

	// restart database
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	defer func() {
		_ = db2.Close()
	}()

	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 0, len(keys))
}

// TestDatabase_MergeWhenWrite tests for merging when some new data are being written or deleted
func TestDatabase_MergeWhenWrite(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DataFileSize = 32 * 1024 * 1024
	options.DataFileMergeRatio = 0
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	// wait group for the goroutine
	waitGroup := new(sync.WaitGroup)
	waitGroup.Add(1) // add one goroutine

	go func() {
		defer waitGroup.Done() // done the goroutine

		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}

		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()

	err = db.Merge()
	assert.Nil(t, err)

	// wait for the goroutine to finish
	waitGroup.Wait()

	// restart database
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	defer func() {
		_ = db2.Close()
	}()

	assert.Nil(t, err)

	keys := db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}
