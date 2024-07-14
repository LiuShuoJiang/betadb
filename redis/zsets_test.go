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

func TestRedisDataStructure_ZScore(t *testing.T) {
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-redis")
	options.DirectoryPath = directory

	rds, err := NewRedisDataStructure(options)
	assert.Nil(t, err)

	// add data
	ok, err := rds.ZAdd(utils.GetTestKey(1), 115, []byte("value1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.ZAdd(utils.GetTestKey(1), 514, []byte("value1"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.ZAdd(utils.GetTestKey(1), 24, []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	// get score
	score, err := rds.ZScore(utils.GetTestKey(1), []byte("value1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(514), score)

	score, err = rds.ZScore(utils.GetTestKey(1), []byte("value2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(24), score)
}
