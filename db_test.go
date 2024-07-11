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

func destroyDB(db *Database) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}

		for _, off := range db.olderFiles {
			if off != nil {
				_ = off.Close()
			}
		}

		err := os.RemoveAll(db.options.DirectoryPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDatabase_Put(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory
	options.DataFileSize = 1024 * 1024 * 64

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// (1) test for the normal put
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(42))
	assert.Nil(t, err)
	value1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value1)

	// (2) test for putting the data with the same key
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(42))
	assert.Nil(t, err)
	value2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value2)

	// (3) test for putting empty key
	err = db.Put(nil, utils.RandomValue(42))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// (4) test for putting empty value
	err = db.Put(utils.GetTestKey(24), nil)
	assert.Nil(t, err)
	value3, err := db.Get(utils.GetTestKey(24))
	assert.Equal(t, 0, len(value3))
	assert.Nil(t, err)

	// test for writing when switching among data files
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// (6) test for putting after restarting the database
	if db.activeFile != nil {
		_ = db.Close()
	}
	for _, off := range db.olderFiles {
		if off != nil {
			_ = off.Close()
		}
	}
	// restart the database
	db2, err := Open(options)
	defer destroyDB(db2)

	assert.Nil(t, err)
	assert.NotNil(t, db2)
	value4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(1919), value4)
	assert.Nil(t, err)
	value5, err := db2.Get(utils.GetTestKey(1919))
	assert.Nil(t, err)
	assert.Equal(t, value4, value5)
}

func TestDatabase_Get(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory
	options.DataFileSize = 1024 * 1024 * 64

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// (1) test for reading a piece of data normally
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(514))
	assert.Nil(t, err)
	value1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, value1)

	// (2) test for reading a nonexistent key
	value2, err := db.Get([]byte("never inserted"))
	assert.Nil(t, value2)
	assert.Equal(t, ErrKeyNotFound, err)

	// (3) test for reading after putting several values
	err = db.Put(utils.GetTestKey(21), utils.RandomValue(14))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(21), utils.RandomValue(14))
	value3, err := db.Get(utils.GetTestKey(21))
	assert.Nil(t, err)
	assert.NotNil(t, value3)

	// (4) test for getting after deleting the key
	err = db.Put(utils.GetTestKey(40), utils.RandomValue(1919))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(40))
	assert.Nil(t, err)
	value4, err := db.Get(utils.GetTestKey(40))
	assert.Equal(t, 0, len(value4))
	assert.Equal(t, ErrKeyNotFound, err)

	// test for switching to the old data files and getting value from them
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	value5, err := db.Get(utils.GetTestKey(124))
	assert.Nil(t, err)
	assert.NotNil(t, value5)

	// (6) test for restarting the database and make sure the data can be obtained
	if db.activeFile != nil {
		_ = db.Close()
	}
	for _, off := range db.olderFiles {
		if off != nil {
			_ = off.Close()
		}
	}
	// restart the database
	db2, err := Open(options)
	defer destroyDB(db2)

	assert.Nil(t, err)
	assert.NotNil(t, db2)

	value6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, value6)
	assert.Equal(t, value1, value6)

	value7, err := db2.Get(utils.GetTestKey(21))
	assert.Nil(t, err)
	assert.NotNil(t, value7)
	assert.Equal(t, value3, value7)

	value8, err := db2.Get(utils.GetTestKey(40))
	assert.Equal(t, 0, len(value8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDatabase_Delete(t *testing.T) {
	options := DefaultOptions
	dir, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = dir
	options.DataFileSize = 1024 * 1024 * 64

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// (1) test for deleting an existing key normally
	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(114))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(114))
	assert.Equal(t, ErrKeyNotFound, err)

	// (2) test for deleting a nonexistent key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// (3) test for deleting a null key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// (5) test for putting after deletion
	err = db.Put(utils.GetTestKey(1145), utils.RandomValue(1919))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(1145))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(1145), utils.RandomValue(1919))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1145))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// (6) test for deleting after restart
	if db.activeFile != nil {
		_ = db.Close()
	}
	for _, of := range db.olderFiles {
		if of != nil {
			_ = of.Close()
		}
	}

	// restart the database
	db2, err := Open(options)
	defer destroyDB(db2)

	assert.Nil(t, err)
	assert.NotNil(t, db2)

	_, err = db2.Get(utils.GetTestKey(114))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(1145))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDatabase_Close(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)
}

func TestDatabase_Sync(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDatabase_ListKeys(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// (1) test for empty database
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// (2) test for only one data
	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// (3) test for multiple data entries
	err = db.Put(utils.GetTestKey(115), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(116), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(117), utils.RandomValue(514))
	assert.Nil(t, err)

	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))
	for _, k := range keys3 {
		assert.NotNil(t, k)
	}
}

func TestDatabase_Fold(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(115), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(116), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(117), utils.RandomValue(514))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(118), utils.RandomValue(514))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDatabase_FileLock(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(options)
	assert.Equal(t, ErrDatabaseIsUsing, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(options)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	err = db2.Close()
	assert.Nil(t, err)
}

func TestDatabase_Stat(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	for i := 100; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	// t.Log(stat)
	assert.NotNil(t, stat)
}

func TestDatabase_Backup(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 1; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	backupDir, _ := os.MkdirTemp("", "betadb-backup")

	err = db.Backup(backupDir)
	assert.Nil(t, err)

	options2 := DefaultOptions
	options2.DirectoryPath = backupDir

	db2, err := Open(options2)
	defer destroyDB(db2)

	assert.Nil(t, err)
	assert.NotNil(t, db2)
}
