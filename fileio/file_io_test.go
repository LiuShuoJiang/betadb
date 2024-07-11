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
	"os"
	"path/filepath"
	"testing"
)

// destroyFile removes the file
func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "some.data")
	fIO, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fIO)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "some.data")
	fIO, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fIO)

	numBytes, err := fIO.Write([]byte(""))
	assert.Equal(t, 0, numBytes)
	assert.Nil(t, err)

	numBytes, err = fIO.Write([]byte("some string"))
	assert.Equal(t, 11, numBytes)
	assert.Nil(t, err)

	numBytes, err = fIO.Write([]byte("Hello, 🌞"))
	assert.Equal(t, 11, numBytes)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "some.data")
	fIO, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fIO)

	_, err = fIO.Write([]byte("something"))
	assert.Nil(t, err)

	_, err = fIO.Write([]byte("Hello, world🤖"))
	assert.Nil(t, err)

	receiveByte1 := make([]byte, 9)
	numBytes, err := fIO.Read(receiveByte1, 0)
	assert.Equal(t, 9, numBytes)
	assert.Equal(t, []byte("something"), receiveByte1)

	receiveByte2 := make([]byte, 16)
	numBytes, err = fIO.Read(receiveByte2, 9)
	assert.Equal(t, 16, numBytes)
	assert.Equal(t, []byte("Hello, world🤖"), receiveByte2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "some.data")
	fIO, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fIO)

	err = fIO.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "some.data")
	fIO, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fIO)

	err = fIO.Close()
	assert.Nil(t, err)
}
