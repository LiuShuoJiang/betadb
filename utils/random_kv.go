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

package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randomStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters   = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey returns a key for testing
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("betadb-key-%09d", i))
}

// RandomValue returns a random value for different lengths
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randomStr.Intn(len(letters))]
	}
	return []byte("betadb-value-" + string(b))
}
