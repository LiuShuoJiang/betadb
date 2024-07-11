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

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key is not found in the database")
	ErrDataFileNotFound       = errors.New("data file is not found")
	ErrDataDirectoryCorrupted = errors.New("database directory might be corrupted")
	ErrExceedMaxBatchNum      = errors.New("maximum batch numbers has been exceeded")
	ErrMergeIsInProgress      = errors.New("merging is in progress, please try again later")
	ErrDatabaseIsUsing        = errors.New("database directory is being used by another process")
	ErrMergeRatioUnreached    = errors.New("merge ratio does not reach the option")
	ErrNoEnoughSpaceForMerge  = errors.New("no enough space on disk for merging")
)
