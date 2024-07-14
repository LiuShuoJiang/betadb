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
	"github.com/LiuShuoJiang/betadb/utils"
)

// ========================================= Sorted Set =========================================

// metadata:
//         +----------+------------+-----------+-----------+
// key =>  |   type   |  expire    |  version  |  size     |
//         | (1 byte) | (N bytes)  | (8 bytes) | (M bytes) |
//         +----------+------------+-----------+-----------+

// actual data pert 1:
//                             +---------------+
// [key | version | member] => |     score     |
//                             +---------------+
// actual data part 2:
//                                                  +---------------+
// [key | version | score | member | memberSize] => |     NULL      |
//                                                  +---------------+

// sortedSetInternalKey defines the format of Key for ZSet data structure
type sortedSetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

// encodeWithMember encodes the sortedSetInternalKey into a byte slice with member
func (ssk *sortedSetInternalKey) encodeWithMember() []byte {
	buffer := make([]byte, len(ssk.key)+len(ssk.member)+8)

	// key
	var index = 0
	copy(buffer[index:index+len(ssk.key)], ssk.key)
	index += len(ssk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(ssk.version))
	index += 8

	// member
	copy(buffer[index:], ssk.member)

	return buffer
}

// encodeWithScore encodes the sortedSetInternalKey into a byte slice with score
func (ssk *sortedSetInternalKey) encodeWithScore() []byte {
	scoreBuffer := utils.Float64ToBytes(ssk.score)
	buffer := make([]byte, len(ssk.key)+len(ssk.member)+len(scoreBuffer)+8+4)

	// key
	var index = 0
	copy(buffer[index:index+len(ssk.key)], ssk.key)
	index += len(ssk.key)

	// version
	binary.LittleEndian.PutUint64(buffer[index:index+8], uint64(ssk.version))
	index += 8

	// score
	copy(buffer[index:index+len(scoreBuffer)], scoreBuffer)
	index += len(scoreBuffer)

	// member
	copy(buffer[index:index+len(ssk.member)], ssk.member)
	index += len(ssk.member)

	// member size
	binary.LittleEndian.PutUint32(buffer[index:], uint32(len(ssk.member)))

	return buffer
}

// ZAdd adds all the specified members with the specified scores to the sorted set stored at key
// return true if the member is added, false if the member is updated
// currently does not support the score which is less than zero
func (r *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	// retrieve metadata
	meta, err := r.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// construct a key for the data part
	ssk := &sortedSetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	var exist = true
	// check if the member already exists
	value, err := r.db.Get(ssk.encodeWithMember())
	if err != nil && !errors.Is(err, betadb.ErrKeyNotFound) {
		return false, err
	}

	if errors.Is(err, betadb.ErrKeyNotFound) {
		exist = false
	}

	// if the original score is equal to the user-input score, return directly
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// update metadata and actual data
	writeBatch := r.db.NewWriteBatch(betadb.DefaultWriteBatchOptions)

	if !exist {
		meta.size++
		_ = writeBatch.Put(key, meta.encode())
	} else {
		// delete the old key
		oldKey := &sortedSetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = writeBatch.Delete(oldKey.encodeWithScore())
	}

	_ = writeBatch.Put(ssk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = writeBatch.Put(ssk.encodeWithScore(), nil)

	if err = writeBatch.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

// ZScore returns the score of member in the sorted set at key
// currently does not support the score which is less than zero
func (r *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	// retrieve metadata
	meta, err := r.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}

	if meta.size == 0 {
		return -1, err
	}

	// construct a key for the data part
	ssk := &sortedSetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := r.db.Get(ssk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
