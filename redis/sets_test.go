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

package redis

import (
	"github.com/LiuShuoJiang/betadb"
	"github.com/LiuShuoJiang/betadb/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRedisDataStructure_SIsMember(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// add some data
	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	// retrieve data
	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("value1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("value-does-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// add some data
	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	// delete data
	ok, err = rds.SRem(utils.GetTestKey(2), []byte("value1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}
