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
	"encoding/binary"
	"github.com/LiuShuoJiang/betadb/data"
	"sync"
	"sync/atomic"
)

// nonTransactionSeqNo is the sequence number for normal, non-transaction data
const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// WriteBatch is a batch writing struct to ensure atomic transaction
type WriteBatch struct {
	options WriteBatchOptions
	mu      *sync.Mutex
	db      *Database

	// pendingWrites temporarily stores the user-written data
	pendingWrites map[string]*data.LogRecord
}

// NewWriteBatch initialize a new WriteBatch
func (db *Database) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPlusTree && !db.seqNoFilesExists && !db.isInitial {
		panic("cannot use write batch, seqNo file does not exist")
	}

	return &WriteBatch{
		options:       options,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put writes the data in batch
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// temporarily store LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Delete deletes the data in batch
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// if the data does not exist, return directly
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// temporarily store LogRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Commit commits the transaction
// writing the temporary data to data file and update memory index
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// locking ensures transaction serialization
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// get the current newest transaction sequence number
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// start writing data to the data file
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		// no need to add lock for appendLogRecord since we already have it
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})

		if err != nil {
			return err
		}

		positions[string(record.Key)] = logRecordPos
	}

	// write a data indicating transaction has completed
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished, // special type representing transaction finished
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// determine whether to sync based on user configuration
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// update memory index
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]

		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}

		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}

		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// clear the temporary data
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeq concatenates and encodes the key and seqNo
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))

	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// parseLogRecordKey parses the key of LogRecord to obtain the actual key and transaction sequence number
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
