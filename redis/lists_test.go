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

func TestRedisDataStructure_LPop(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// push data
	result, err := rds.LPush(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), result)

	result, err = rds.LPush(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), result)

	result, err = rds.LPush(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), result)

	// pop data
	value, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)

	value, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)

	value, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// push data
	result, err := rds.RPush(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), result)

	result, err = rds.RPush(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), result)

	result, err = rds.RPush(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), result)

	// pop data
	value, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)

	value, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)

	value, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value)
}
