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

package betadb

import (
	"github.com/LiuShuoJiang/betadb/data"
	"github.com/LiuShuoJiang/betadb/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirectoryName = "-merge"
	mergeFinishedKey   = "merge.finished"
)

// Merge cleans the invalid data, and generate hint file
func (db *Database) Merge() error {
	// if the database is null, return directly
	if db.activeFile == nil {
		return nil
	}

	// ========== hold the lock
	db.mu.Lock()

	// if it is currently merging, return directly
	if db.isMerging {
		// ========= release the lock
		db.mu.Unlock()
		return ErrMergeIsInProgress
	}

	// check whether the data size that can be merged has reached to threshold
	totalSize, err := utils.DirectorySize(db.options.DirectoryPath)
	if err != nil {
		// ========= release the lock
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		// ========= release the lock
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// check whether the remaining space can accommodate the amount of data after the merge
	availableDiskSpace, err := utils.AvailableDiskSize()
	if err != nil {
		// ========= release the lock
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSpace {
		// ========= release the lock
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// sync current active file
	if err := db.activeFile.Sync(); err != nil {
		// ========= release the lock
		db.mu.Unlock()
		return err
	}
	// convert the current active file to the old data file
	db.olderFiles[db.activeFile.FileID] = db.activeFile

	// open a new active file
	if err := db.setActiveDataFile(); err != nil {
		// ========= release the lock
		db.mu.Unlock()
		return err
	}
	// record the file ID that have not participated in the merge recently
	nonMergeFileID := db.activeFile.FileID

	// get every file that needs to merge
	var filesToBeMerged []*data.DataFile
	for _, file := range db.olderFiles {
		filesToBeMerged = append(filesToBeMerged, file)
	}

	// ========= release the lock
	db.mu.Unlock()

	// sort the files to be merged in ascending order, merging them one by one
	sort.Slice(filesToBeMerged, func(i, j int) bool {
		return filesToBeMerged[i].FileID < filesToBeMerged[j].FileID
	})

	mergePath := db.getMergePath()

	// if the directory exists, it means that a merge has happened, delete it
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// create a new merge path directory
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// construct a new temporary Database instance
	mergeOptions := db.options
	mergeOptions.DirectoryPath = mergePath
	// set SyncWrites to false to improve efficiency
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// open hint file
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// iterate and process every data file
	for _, dataFile := range filesToBeMerged {
		var offset int64 = 0

		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// parse the actual key
			readKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(readKey)

			// compare with the index position in memory
			// and overwrite if valid
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileID && logRecordPos.Offset == offset {
				// clear the transaction marking
				logRecord.Key = logRecordKeyWithSeq(readKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// write the current positional index to hint file
				if err := hintFile.WriteHintRecord(readKey, pos); err != nil {
					return err
				}
			}

			// add offset
			offset += size
		}
	}

	// sync the data
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// write the file indicating merge has finished
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	// construct the merge-finished record
	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileID))),
	}

	encodeRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishedFile.Write(encodeRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *Database) getMergePath() string {
	directory := path.Dir(path.Clean(db.options.DirectoryPath))
	base := path.Base(db.options.DirectoryPath)
	return filepath.Join(directory, base+mergeDirectoryName)
}

// loadMergeFiles loads the merge data directory
func (db *Database) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// if the merge directory does not exist, return directly
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	directoryEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// find the file indicating merge has finished
	// in order to check whether merge has been processed
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range directoryEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}

		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// if merge has not finished, return directly
	if !mergeFinished {
		return nil
	}

	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// delete old data files
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(db.options.DirectoryPath, fileID)

		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// move the new data files into the data directory
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirectoryPath, fileName)

		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

func (db *Database) getNonMergeFileID(directoryPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(directoryPath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileID), nil
}

// loadIndexFromHintFile loads the indices from hint file
func (db *Database) loadIndexFromHintFile() error {
	// check if the hint file exists
	hintFileName := filepath.Join(db.options.DirectoryPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// open hint index file
	hintFile, err := data.OpenHintFile(db.options.DirectoryPath)
	if err != nil {
		return err
	}

	// read index from file
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		// decode to get the actual positional index
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}

	return nil
}
