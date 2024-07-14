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
	"errors"
	"github.com/LiuShuoJiang/betadb"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure defines the redis data structure service
type RedisDataStructure struct {
	db *betadb.Database
}

// NewRedisDataStructure initializes the new redis data structure service
func NewRedisDataStructure(options betadb.Options) (*RedisDataStructure, error) {
	db, err := betadb.Open(options)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

func (r *RedisDataStructure) Close() error {
	return r.db.Close()
}

func (r *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaBuffer, err := r.db.Get(key)
	if err != nil && !errors.Is(err, betadb.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	var exist = true

	if errors.Is(err, betadb.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetadata(metaBuffer)

		// check the data type
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		// check the expired time
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}

		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}
