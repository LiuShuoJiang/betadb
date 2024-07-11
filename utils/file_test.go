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
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirectorySize(t *testing.T) {
	directory, _ := os.Getwd()
	dirSize, err := DirectorySize(directory)

	assert.Nil(t, err)
	// t.Log(dirSize)
	assert.True(t, dirSize > 0)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()

	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024) // show in GiB
	assert.True(t, size > 0)
}
