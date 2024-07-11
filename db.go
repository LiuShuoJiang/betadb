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
	"errors"
	"fmt"
	"github.com/LiuShuoJiang/betadb/data"
	"github.com/LiuShuoJiang/betadb/fileio"
	"github.com/LiuShuoJiang/betadb/index"
	"github.com/LiuShuoJiang/betadb/utils"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "fLock"
)

// Database defines a storage engine instance
type Database struct {
	// options defines the user defined configurations
	options Options

	// mu defines the mutex for database
	mu *sync.RWMutex

	// fileIDs are the file ids which can only be used when loading the indices first
	// they cannot be updated or used elsewhere
	fileIDs []int

	// activeFile is the current active file that can be written
	activeFile *data.DataFile

	// olderFiles are the old data files that are read only
	olderFiles map[uint32]*data.DataFile

	// index defines the memory indexing information
	index index.Indexer

	// seqNo is the transaction sequence number, globally incremented
	seqNo uint64

	// isMerging tells whether we are executing the merging process or not
	isMerging bool

	// seqNoFilesExists indicates whether the file storing the transaction sequence number exists
	seqNoFilesExists bool

	// isInitial indicates whether this is the first time to initialize this data directory
	isInitial bool

	// fileLock is a file lock that ensures mutual exclusion between multiple processes
	// refer to [https://github.com/gofrs/flock]
	fileLock *flock.Flock

	// bytesWrite indicates how many bytes have been written
	bytesWrite uint

	// reclaimSize indicates how many bytes of data are invalid
	reclaimSize int64
}

// Stat stores engine statistics
type Stat struct {
	// KeyNum is the number of keys in the database
	KeyNum uint
	// DataFileNum is the number of data files
	DataFileNum uint
	// RecycleFileNum is the number of bytes of data that can be merged
	ReclaimableSize int64
	// DiskSize is the size of the data directory on disk
	DiskSize int64
}

// Open opens a BetaDB storage engine instance
func Open(options Options) (*Database, error) {
	// check the user options first
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// determine whether the data directory exists
	// if not, create the directory
	if _, err := os.Stat(options.DirectoryPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirectoryPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// determine whether the current data directory is in use
	fileLock := flock.New(filepath.Join(options.DirectoryPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// check if the directory entry is empty
	entries, err := os.ReadDir(options.DirectoryPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	// initialize Database instance struct
	db := &Database{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirectoryPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// load merge data directory first
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// then load data files
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+ tree indices do not require loading indexes from data files
	if options.IndexType != BPlusTree {
		// load index from hint index file first
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// then load index from data file
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	// load the current transaction sequence number
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOffset = size
		}
	}

	// reset IO type to standard file IO
	if db.options.MMapAtStartUp {
		if err := db.resetIOType(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// Close closes the database instance
func (db *Database) Close() error {
	defer func() {
		// release the file lock
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory: %v", err))
		}

		// close the index
		if err := db.index.Close(); err != nil {
			panic(fmt.Sprintf("failed to close index!"))
		}
	}()

	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// save the current transaction sequence number
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirectoryPath)
	if err != nil {
		return err
	}

	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}

	encodeRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encodeRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// close the current active file
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// close the old data files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Sync persistent data files
func (db *Database) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Stat gets the statistics of the database
func (db *Database) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	// get directory size
	dirSize, err := utils.DirectorySize(db.options.DirectoryPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get the directory size: %v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// Backup backs up the database and copies the data files to a new directory
func (db *Database) Backup(directory string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// note that we cannot copy the fileLock file
	return utils.CopyDirectory(db.options.DirectoryPath, directory, []string{fileLockName})
}

// Put writes Key/Value data, where the key cannot be empty
func (db *Database) Put(key []byte, value []byte) error {
	// is key valid or not
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// create a LogRecord struct
	logRecord := &data.LogRecord{
		// use nonTransactionSeqNo to indicate the non-transaction data
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// append writes to the currently active data file
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// update memory index
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Delete deletes the corresponding data according to the key
func (db *Database) Delete(key []byte) error {
	// determine the validity of the key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// first check if key exists, return directly if key does not exist
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// construct LogRecord, marking it as deleted
	logRecord := &data.LogRecord{
		// use nonTransactionSeqNo to indicate the non-transaction data
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	// write into the data file for the deleted record itself
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	// delete the corresponding key from the indices in memory
	// since the lock is maintained by BTree internals, there is no need to lock here
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}

	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

// Get obtains data by the key
func (db *Database) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// determine the validity of the key
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// get index information corresponding to the key from the memory data structure
	logRecordPos := db.index.Get(key)
	// if the key is not in the memory index, it means that the key does not exist
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// get the actual value from data file
	return db.getValueByPosition(logRecordPos)
}

// ListKeys lists all the keys within the database
func (db *Database) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	keys := make([][]byte, db.index.Size())

	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}

	return keys
}

// Fold obtains all data and performs the operations specified by the user
// the traversal is terminated when the function returns false
func (db *Database) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close() // remember to close the iterator

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}

	return nil
}

// getValueByPosition gets the corresponding value according to the indexing information
func (db *Database) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// find the corresponding data file according to the file id
	var dataFile *data.DataFile
	if db.activeFile.FileID == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	// if datafile is null
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// get the corresponding data according to offset
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecordWithLock is a wrapper for appendLogRecord with locks
func (db *Database) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// appendLogRecord appends data to the active file
//
//  1. Initialize active file if there are no active file present
//  2. When the active file is written to the threshold size, close the active file and open a new data file
//  3. Write (append) the content to the data file
//  4. Synchronize if needed
//
// Return the indexing position
func (db *Database) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	// determine if the currently active datafile exists
	// since no file is generated when the database has not been written to
	// initialize the datafile if it is empty
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// write the encoded data (we need encoding here!)
	encRecord, size := data.EncodeLogRecord(logRecord)

	// If the data written has reached the active file threshold
	// then the active file is closed and a new file is opened
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// first sync the data file to ensure that the existing data is persisted to disk
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// convert currently active file to old data file
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		// open a new data file
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// execute the actual data writing process
	writeOffset := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	db.bytesWrite += uint(size)

	// determine synchronization based on user configurations
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// clear cumulative values
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	// construct memory index information
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOffset,
		Size:   uint32(size),
	}

	return pos, nil
}

// setActiveDataFile sets the current active data file
// must hold a mutex lock before accessing this method
func (db *Database) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}

	// open new data file
	dataFile, err := data.OpenDataFile(db.options.DirectoryPath, initialFileID, fileio.StandardFileIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile

	return nil
}

// loadDataFiles loads the data files from disk
func (db *Database) loadDataFiles() error {
	directoryEntries, err := os.ReadDir(db.options.DirectoryPath)
	if err != nil {
		return err
	}

	var fileIDs []int

	// loop through all files in the directory
	// and find all files ending with .data
	for _, entry := range directoryEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(splitNames[0])

			// check if the data directory has been damaged or not
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIDs = append(fileIDs, fileID)
		}
	}

	// we must sort the file ids and load them in ascending order
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs

	// traverse each file id and open the corresponding data file
	for i, fid := range fileIDs {
		ioType := fileio.StandardFileIO
		if db.options.MMapAtStartUp {
			ioType = fileio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.options.DirectoryPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		// the last one has the largest id
		// indicating that it is the currently active file
		if i == len(fileIDs)-1 {
			db.activeFile = dataFile
		} else { // the else are older data files
			db.olderFiles[uint32(fid)] = dataFile
		}
	}

	return nil
}

// loadIndexFromDataFiles loads the indexing from data files
// it iterates over all records in the file and updates them into the in-memory indices
func (db *Database) loadIndexFromDataFiles() error {
	// if the database is empty
	if len(db.fileIDs) == 0 {
		return nil
	}

	// check if merge has happened
	hasMerge, nonMergeFileID := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirectoryPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileID(db.options.DirectoryPath)
		if err != nil {
			return err
		}

		hasMerge = true
		nonMergeFileID = fid
	}

	updateIndex := func(key []byte, tp data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos

		if tp == data.LogRecordDeleted {
			// if it is a deleted index
			// we need to process the deleted indices when starting the database engine
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}

		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// temporarily store transaction data
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// traverse all file IDs and process the records in the files
	for i, fid := range db.fileIDs {
		var fileID = uint32(fid)
		// If the id is smaller than the file id that has not been merged recently
		// it means that the index has been loaded from the Hint file
		if hasMerge && fileID < nonMergeFileID {
			continue
		}

		var dataFile *data.DataFile
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileID]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// construct the index in memory and save
			logRecordPos := &data.LogRecordPos{
				Fid:    fileID,
				Offset: offset,
				Size:   uint32(size),
			}

			// parse the key and get the transaction sequence number
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// non-transactional operation, directly update the memory index
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// if the transaction is completed
				// the corresponding seqNo data can be updated to the memory index
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else { // if the transaction has not been completed, temporarily store data
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// update transaction sequence number
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// increment offset and start reading from the new position next time
			offset += size
		}

		// if it is the current active file, update the WriteOffset of this file
		if i == len(db.fileIDs)-1 {
			db.activeFile.WriteOffset = offset
		}
	}

	// update transaction sequence number
	db.seqNo = currentSeqNo

	return nil
}

// checkOptions checks the validity of the used-defined options
func checkOptions(options Options) error {
	if options.DirectoryPath == "" {
		return errors.New("database directory path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("the data file size of database must be greater than zero")
	}

	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must be between 0 and 1 inclusive")
	}

	return nil
}

func (db *Database) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirectoryPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirectoryPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFilesExists = true

	return os.Remove(fileName)
}

// resetIOType sets the IO type of the data files into standard file IO
func (db *Database) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirectoryPath, fileio.StandardFileIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirectoryPath, fileio.StandardFileIO); err != nil {
			return err
		}
	}

	return nil
}
