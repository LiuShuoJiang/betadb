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

func TestDatabase_WriteBatch1(t *testing.T) {
	options := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-batch")
	options.DirectoryPath = directory

	db, err := Open(options)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	// test for no commit after writing data
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(115))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(114))
	assert.Equal(t, ErrKeyNotFound, err)

	// test for normal writing data
	err = wb.Commit()
	assert.Nil(t, err)

	value1, err := db.Get(utils.GetTestKey(114))
	// t.Log(value1)
	// t.Log(err)
	assert.NotNil(t, value1)
	assert.Nil(t, err)

	// test for deleting valid data
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(114))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(114))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDatabase_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-batch")
	opts.DirectoryPath = directory

	db, err := Open(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(114), utils.RandomValue(514))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(115), utils.RandomValue(514))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(114))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(116), utils.RandomValue(514))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// restart database
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(114))
	assert.Equal(t, ErrKeyNotFound, err)

	// verify the sequence number
	assert.Equal(t, uint64(2), db2.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "betadb-batch")
	opts.DirectoryPath = dir

	db, err := Open(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	wbOpts := DefaultWriteBatchOptions
	wbOpts.MaxBatchNum = 10000000

	wb := db.NewWriteBatch(wbOpts)
	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	err = wb.Commit()
	assert.Nil(t, err)
}
