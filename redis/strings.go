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
	"time"
)

// ========================================= String =========================================

//         +----------+------------+--------------------+
// key =>  |   type   |  expire    |       payload      |
//         | (1 byte) | (X bytes)  |       (N bytes)    |
//         +----------+------------+--------------------+

// Set implements the set command for String data type
func (r *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// encode value: type + expire + actual payload
	buffer := make([]byte, binary.MaxVarintLen64+1)
	buffer[0] = String

	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buffer[index:], expire)

	encodeValue := make([]byte, index+len(value))
	copy(encodeValue[:index], buffer[:index])
	copy(encodeValue[index:], value)

	return r.db.Put(key, encodeValue)
}

// Get implements the get command for String data type
func (r *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encodeValue, err := r.db.Get(key)
	if err != nil {
		return nil, err
	}

	// decode
	dataType := encodeValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, numBytes := binary.Varint(encodeValue[index:])
	index += numBytes

	// check if the data has expired
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encodeValue[index:], nil
}
