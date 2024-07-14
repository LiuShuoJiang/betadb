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
	"encoding/binary"
	"errors"
	"github.com/LiuShuoJiang/betadb"
)

// ========================================= Hash =========================================

// metadata:
//         +----------+------------+-----------+-----------+
// key =>  |   type   |  expire    |  version  |  size     |
//         | (1 byte) | (N bytes)  | (8 bytes) | (M bytes) |
//         +----------+------------+-----------+-----------+

// actual data:
//                            +---------------+
// [key | version | field] => |     value     |
//                            +---------------+

// hashInternalKey defines the format of Key for hash data structure
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

// encode encodes the hashInternalKey into a byte slice
func (hik *hashInternalKey) encode() []byte {
	buffer := make([]byte, len(hik.key)+len(hik.field)+8)

	// key
	var index = 0
	copy(buffer[index:index+len(hik.key)], hik.key)
	index += len(hik.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(hik.version))
	index += 8

	// field
	copy(buffer[index:], hik.field)

	return buffer
}

// HSet implements the set command for Hash data type
// return true if the field is a new field in the hash and value was set
func (r *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// find metadata first
	meta, err := r.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// create the key part of Hash data
	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encodeKey := hik.encode()

	// check if key exists first
	var exist = true
	if _, err = r.db.Get(encodeKey); errors.Is(err, betadb.ErrKeyNotFound) {
		exist = false
	}

	writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)

	// if the key does not exist, update metadata first
	if !exist {
		meta.size++
		_ = writeBatch.Put(key, meta.encode())
	}

	// put the actual key and value
	_ = writeBatch.Put(encodeKey, value)
	if err = writeBatch.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// HGet implements the get command for Hash data type
func (r *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := r.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return r.db.Get(hik.encode())
}

// HDel implements the del command for Hash data type
// return true if the field was present in the hash and is deleted
func (r *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := r.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	hik := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encodeKey := hik.encode()

	// check if key exists first
	var exist = true
	if _, err := r.db.Get(encodeKey); errors.Is(err, betadb.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)
		meta.size-- // reduce the size by 1

		_ = writeBatch.Put(key, meta.encode())
		_ = writeBatch.Delete(encodeKey)

		if err := writeBatch.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
