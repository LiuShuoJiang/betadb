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
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join(os.TempDir(), "mmap-data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	// test for empty file
	value1 := make([]byte, 24)
	numBytes1, err := mmapIO.Read(value1, 0)
	assert.Equal(t, 0, numBytes1)
	assert.Equal(t, io.EOF, err)

	// test for non-empty file
	fileIO, err := NewFileIOManager(path)
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("cpp"))
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("java"))
	assert.Nil(t, err)
	_, err = fileIO.Write([]byte("golang"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)

	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(13), size)

	value2 := make([]byte, 2)
	numBytes2, err := mmapIO2.Read(value2, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, numBytes2)
}
