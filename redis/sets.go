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

// ========================================= Set =========================================

// metadata:
//         +----------+------------+-----------+-----------+
// key =>  |   type   |  expire    |  version  |  size     |
//         | (1 byte) | (N bytes)  | (8 bytes) | (M bytes) |
//         +----------+------------+-----------+-----------+

// actual data:
//                                          +---------------+
// [key | version | member | memberSize] => |     NULL      |
//                                          +---------------+

// setInternalKey defines the format of Key for set data structure
type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

// encode encodes the setInternalKey into a byte slice
func (sik *setInternalKey) encode() []byte {
	buffer := make([]byte, len(sik.key)+len(sik.member)+8+4)

	// key
	var index = 0
	copy(buffer[index:index+len(sik.key)], sik.key)
	index += len(sik.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(sik.version))
	index += 8

	// member
	copy(buffer[index:index+len(sik.member)], sik.member)
	index += len(sik.member)

	// member size
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(sik.member)))

	return buffer
}

// SAdd adds the member to the set stored at key for the Set data structure
// returns true if the member was added to the set, false if the member was already a member of the set
func (r *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	// retrieve metadata first
	meta, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// construct a key for the data part
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err := r.db.Get(sik.encode()); errors.Is(err, betadb.ErrKeyNotFound) {
		// if the key does not exist, do the update
		writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)
		meta.size++

		_ = writeBatch.Put(key, meta.encode())
		_ = writeBatch.Put(sik.encode(), nil)

		if err := writeBatch.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMember implements the SISMEMBER command for the Set data structure
func (r *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, err
	}

	// construct a key for the data part
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = r.db.Get(sik.encode())
	if err != nil && !errors.Is(err, betadb.ErrKeyNotFound) {
		return false, err
	}

	if errors.Is(err, betadb.ErrKeyNotFound) {
		return false, nil
	}

	return true, nil
}

// SRem removes the specified members from the set stored at key for the Set data structure
func (r *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, err
	}

	// construct a key for the data part
	sik := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = r.db.Get(sik.encode()); errors.Is(err, betadb.ErrKeyNotFound) {
		return false, err
	}

	// do the update
	writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)
	meta.size--

	_ = writeBatch.Put(key, meta.encode())
	_ = writeBatch.Delete(sik.encode())

	if err = writeBatch.Commit(); err != nil {
		return false, err
	}

	return true, nil
}
