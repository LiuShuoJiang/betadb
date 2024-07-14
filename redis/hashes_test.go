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

func TestRedisDataStructure_HGet(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok1)

	value1 := utils.RandomValue(128)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), value1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	value2 := utils.RandomValue(128)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), value2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	number1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, value1, number1)

	number2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, value2, number2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("random-field"))
	assert.Equal(t, betadb.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	existBefore, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, existBefore)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(128))
	assert.Nil(t, err)
	assert.True(t, ok1)

	value1 := utils.RandomValue(128)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), value1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	value2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), value2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	existBefore2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, existBefore2)
}
