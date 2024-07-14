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
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(128))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(128))
	assert.Nil(t, err)

	value1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, value1)

	value2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, value2)

	_, err = rds.Get(utils.GetTestKey(3))
	assert.Equal(t, betadb.ErrKeyNotFound, err)
}

func TestRedisDataStructure_Del(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// delete
	err = rds.Del(utils.GetTestKey(12))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(24), 0, utils.RandomValue(128))
	assert.Nil(t, err)

	// type
	tp, err := rds.Type(utils.GetTestKey(24))
	assert.Nil(t, err)
	assert.Equal(t, String, tp)

	err = rds.Del(utils.GetTestKey(24))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(24))
	assert.Equal(t, betadb.ErrKeyNotFound, err)
}
