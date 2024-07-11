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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirectorySize returns the size of the directory
func DirectorySize(directoryPath string) (int64, error) {
	var size int64
	err := filepath.Walk(directoryPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}

// AvailableDiskSize returns the available disk size in bytes
func AvailableDiskSize() (uint64, error) {
	// get the current working directory
	workingDirectory, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t // syscall.Statfs_t is the struct for statfs system call
	// get the file system statistics by the working directory
	if err = syscall.Statfs(workingDirectory, &stat); err != nil {
		return 0, err
	}

	// calculate the available disk size using: available size = block size * available blocks
	return stat.Bavail * uint64(stat.Bsize), nil
}

// CopyDirectory copies the src directory to dst for backup
func CopyDirectory(src, dst string, exclude []string) error {
	// if the destination directory does not exist, create directly
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err := os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	// walk through the source directory
	err := filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		// replace the source directory with empty string to get the relative path
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		// check if the file should be excluded
		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}

			if matched {
				return nil
			}
		}

		// if the file is a directory, create the directory in the destination
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, fileName), info.Mode())
		}

		// read the file data and write to the destination
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
	})

	return err
}
