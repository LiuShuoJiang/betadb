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
	"github.com/LiuShuoJiang/betadb"
)

// ========================================= List =========================================

// metadata:
//         +----------+------------+-----------+-----------+-----------+-----------+
// key =>  |   type   |  expire    |  version  |   size    |   head    |   tail    |
//         | (1 byte) | (N bytes)  | (8 bytes) | (M bytes) | (8 bytes) | (b bytes) |
//         +----------+------------+-----------+-----------+-----------+-----------+

// actual data:
//                            +---------------+
// [key | version | index] => |     value     |
//                            +---------------+

// listInternalKey defines the format of Key for list data structure
type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

// encode encodes the listInternalKey into a byte slice
func (lik *listInternalKey) encode() []byte {
	buffer := make([]byte, len(lik.key)+8+8)

	// key
	var index = 0
	copy(buffer[index:index+len(lik.key)], lik.key)
	index += len(lik.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(lik.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buffer[index:], lik.index)

	return buffer
}

// LPush inserts an element at the head of the List stored at key
func (r *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return r.innerPush(key, element, true)
}

// RPush inserts an element at the tail of the List stored at key
func (r *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return r.innerPush(key, element, false)
}

// LPop removes and returns the first element of the List stored at key
func (r *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return r.innerPop(key, true)
}

// RPop removes and returns the last element of the List stored at key
func (r *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return r.innerPop(key, false)
}

// innerPush inserts an element at the head or tail of the List stored at key
// returns the length of the list after the push operation
func (r *RedisDataStructure) innerPush(key, element []byte, isPushLeft bool) (uint32, error) {
	// retrieve metadata
	meta, err := r.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// construct a key for the data part
	lik := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isPushLeft {
		lik.index = meta.head - 1
	} else {
		lik.index = meta.tail
	}

	// update data
	writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)
	meta.size++

	if isPushLeft {
		meta.head--
	} else {
		meta.tail++
	}

	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Put(lik.encode(), element)

	if err = writeBatch.Commit(); err != nil {
		return 0, err
	}

	return meta.size, nil
}

// innerPop removes and returns the first or last element of the List stored at key
func (r *RedisDataStructure) innerPop(key []byte, isPopLeft bool) ([]byte, error) {
	// retrieve metadata
	meta, err := r.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	// construct a key for the data part
	lik := &listInternalKey{
		key:     key,
		version: meta.version,
	}

	if isPopLeft {
		lik.index = meta.head
	} else {
		lik.index = meta.tail - 1
	}

	element, err := r.db.Get(lik.encode())
	if err != nil {
		return nil, err
	}

	// update metadata
	writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)
	meta.size--

	if isPopLeft {
		meta.head++
	} else {
		meta.tail--
	}

	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Delete(lik.encode())

	if err = writeBatch.Commit(); err != nil {
		return nil, err
	}

	return element, nil
}
