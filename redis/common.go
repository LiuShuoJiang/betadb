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

import "errors"

func (r *RedisDataStructure) Del(key []byte) error {
	return r.db.Delete(key)
}

func (r *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	encodeValue, err := r.db.Get(key)
	if err != nil {
		return 0, err
	}

	if len(encodeValue) == 0 {
		return 0, errors.New("value is NULL")
	}

	// the first byte is type info
	return encodeValue[0], nil
}
