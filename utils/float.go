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

import "strconv"

func FloatFromBytes(value []byte) float64 {
	f, _ := strconv.ParseFloat(string(value), 64)
	return f
}

func Float64ToBytes(value float64) []byte {
	return []byte(strconv.FormatFloat(value, 'f', -1, 64))
}
