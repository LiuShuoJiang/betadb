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
	"math"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

// metadata defines the meta data information for common redis data structures
type metadata struct {
	dataType byte   // data type
	expire   int64  // expired time
	version  int64  // number used for improving efficiency of deletion
	size     uint32 // number of keys in data

	head uint64 // used exclusively for List
	tail uint64 // used exclusively for List
}

func (m *metadata) encode() []byte {
	var size = maxMetadataSize
	if m.dataType == List {
		size += extraListMetaSize
	}

	buffer := make([]byte, size)
	buffer[0] = m.dataType

	var index = 1
	index += binary.PutVarint(buffer[index:], m.expire)
	index += binary.PutVarint(buffer[index:], m.version)
	index += binary.PutVarint(buffer[index:], int64(m.size))

	if m.dataType == List {
		index += binary.PutUvarint(buffer[index:], m.head)
		index += binary.PutUvarint(buffer[index:], m.tail)
	}

	return buffer[:index]
}

func decodeMetadata(buffer []byte) *metadata {
	dataType := buffer[0]

	var index = 1
	expire, numBytes := binary.Varint(buffer[index:])
	index += numBytes

	version, numBytes := binary.Varint(buffer[index:])
	index += numBytes

	size, numBytes := binary.Varint(buffer[index:])
	index += numBytes

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, numBytes = binary.Uvarint(buffer[index:])
		index += numBytes

		tail, _ = binary.Uvarint(buffer[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}
