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

package fileio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap defines the mapping for memory and file
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePermission)
	if err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)
}

func (m *MMap) Write([]byte) (int, error) {
	// TODO
	panic("not implemented")
}

func (m *MMap) Sync() error {
	// TODO
	panic("not implemented")
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
