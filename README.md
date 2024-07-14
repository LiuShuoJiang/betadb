# BetaDB

[![Go workflow](https://github.com/LiuShuoJiang/betadb/actions/workflows/go.yml/badge.svg)](https://github.com/LiuShuoJiang/betadb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Report Card](https://goreportcard.com/badge/github.com/LiuShuoJiang/betadb)](https://goreportcard.com/report/github.com/LiuShuoJiang/betadb)

**BetaDB** is a Key-Value single-machine database storage engine based on the [Bitcask](https://github.com/basho/bitcask) model.
It also supports some basic Redis commands, and is compatible with the Redis serialization protocol (RESP).

## General Features

### Simple API

- **Opening Datastore:**
    - Functions to open a new or existing BetaDB instance.
- **CRUD Operations:**
    - `Get` function retrieves values by key.
    - `Put` function stores key-value pairs.
    - `Delete` function removes keys from the datastore.
- **Utility Functions:**
    - `ListKeys` lists all keys in the datastore.
    - `Fold` allows iteration over all key-value pairs.
    - `Merge` compacts data files and generates hint files.
    - `Sync` ensures any writes are synced to disk.
    - `Close` flushes pending writes and closes the datastore.

### Log-Structured Storage

BetaDB utilizes a log-structured storage system that ***appends*** data sequentially to an active file in the directory.
This approach minimizes the need for disk seeks, enhancing write performance.
Once the active file reaches a certain size, it is closed, marked as immutable, and a new active file is created.

These immutable files ensure data integrity and simplify backup and recovery processes.
Deletions are managed by writing a tombstone value for the key, which is subsequently removed during the merge process.

### In-Memory Key Directory

BetaDB utilizes the in-memory key directory, or **Keydir** in the original Bitcask paper.
This structure is an in-memory hash table that maps each key to a fixed-size structure containing the file ID, offset,
and size of the most recent entry for that key.
The Keydir is updated atomically with each new entry, ensuring that reads always fetch the latest data.

This design allows for fast lookups, requiring only a single disk seek to read a value,
which significantly improves read performance.

### Compaction and Merging

To manage space efficiently, BetaDB employs a compaction and merging process.
Over time, the storage system may accumulate multiple versions of keys, increasing space usage.
The **merge** process iterates over all immutable files, compacting them and retaining only the latest version of each key.

This process produces new compacted data files and associated **hint files**,
which store metadata about the values in the corresponding data files.
These hint files speed up the startup process by providing quick access to the metadata.

### Crash Recovery

BetaDB implements a simple **transaction** feature. The integration ensures no data loss and simplifies recovery,
eliminating the need for log replay.
Recovery is further expedited by the use of hint files, which allow for faster scanning during startup.

## Benchmarking

Please refer to [benchmark](./benchmark) directory to use the benchmarking scripts.

The benchmarking results highlight BetaDB's ability to provide high throughput and low latency
for read and write operations, along with efficient memory utilization.

## Disclaimer

This project is not intended for industrial application, and is for academic or research use only.
