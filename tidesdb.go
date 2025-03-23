// Package tidesdb_go
// Copyright (C) TidesDB
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package tidesdb_go

/*
#cgo LDFLAGS: -L${SRCDIR}/lib -ltidesdb
#include <tidesdb/tidesdb.h>
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

// TidesDBCompressionAlgo represents the compression algorithm type.
type TidesDBCompressionAlgo int

const (
	TDB_NO_COMPRESSION TidesDBCompressionAlgo = iota
	TDB_COMPRESS_SNAPPY
	TDB_COMPRESS_LZ4
	TDB_COMPRESS_ZSTD
)

// TidesDB represents a TidesDB instance.
type TidesDB struct {
	tdb *C.tidesdb_t
}

// Cursor represents a TidesDB cursor.
type Cursor struct {
	cursor *C.tidesdb_cursor_t
}

// Transaction represents a TidesDB transaction.
type Transaction struct {
	txn *C.tidesdb_txn_t
}

// ColumnFamilyConfig represents the configuration for a column family.
type ColumnFamilyConfig struct {
	Name           string
	FlushThreshold int32
	MaxLevel       int32
	Probability    float32
	Compressed     bool
	CompressAlgo   TidesDBCompressionAlgo
	BloomFilter    bool
}

// ColumnFamilySSTableStat represents statistics about an SSTable in a column family.
type ColumnFamilySSTableStat struct {
	Path      string
	Size      int64
	NumBlocks int64
}

// ColumnFamilyStat represents statistics about a column family.
type ColumnFamilyStat struct {
	Config             ColumnFamilyConfig
	Name               string
	NumSSTables        int
	MemtableSize       int64
	MemtableEntryCount int64
	IncrementalMerging bool
	SSTableStats       []*ColumnFamilySSTableStat
}

// Open opens a TidesDB instance.
func Open(directory string) (*TidesDB, error) {
	cDir := C.CString(directory)
	defer C.free(unsafe.Pointer(cDir))

	var tdb *C.tidesdb_t
	err := C.tidesdb_open(cDir, &tdb)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	return &TidesDB{tdb: tdb}, nil
}

// Close closes a TidesDB instance.
func (db *TidesDB) Close() error {
	err := C.tidesdb_close(db.tdb)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// CreateColumnFamily creates a new column family.
func (db *TidesDB) CreateColumnFamily(name string, flushThreshold, maxLevel int, probability float32, compressed bool, compressAlgo int, bloomFilter bool) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	err := C.tidesdb_create_column_family(db.tdb, cName, C.int(flushThreshold), C.int(maxLevel), C.float(probability), C.bool(compressed), C.tidesdb_compression_algo_t(compressAlgo), C.bool(bloomFilter))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// DropColumnFamily drops a column family and all associated data.
func (db *TidesDB) DropColumnFamily(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	err := C.tidesdb_drop_column_family(db.tdb, cName)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// CompactSSTables pairs and merges SSTables in a column family.
func (db *TidesDB) CompactSSTables(columnFamilyName string, maxThreads int) error {
	cName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cName))

	err := C.tidesdb_compact_sstables(db.tdb, cName, C.int(maxThreads))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Put puts a key-value pair into TidesDB.
func (db *TidesDB) Put(columnFamilyName string, key, value []byte, ttl int64) error {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	cValue := (*C.uint8_t)(unsafe.Pointer(&value[0]))

	err := C.tidesdb_put(db.tdb, cfName, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), C.time_t(ttl))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Get gets a value from TidesDB.
func (db *TidesDB) Get(columnFamilyName string, key []byte) ([]byte, error) {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))

	var cValue *C.uint8_t
	var cValueSize C.size_t

	err := C.tidesdb_get(db.tdb, cfName, cKey, C.size_t(len(key)), &cValue, &cValueSize)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	return value, nil
}

// Delete deletes a key-value pair from TidesDB.
func (db *TidesDB) Delete(columnFamilyName string, key []byte) error {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))

	err := C.tidesdb_delete(db.tdb, cfName, cKey, C.size_t(len(key)))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// ListColumnFamilies lists the column families in TidesDB.
func (db *TidesDB) ListColumnFamilies() (string, error) {
	var cfList *C.char
	err := C.tidesdb_list_column_families(db.tdb, &cfList)
	if err != nil {
		return "", errors.New(C.GoString(err.message))
	}

	return C.GoString(cfList), nil
}

// CursorInit initializes a new TidesDB cursor.
func (db *TidesDB) CursorInit(columnFamily string) (*Cursor, error) {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var cursor *C.tidesdb_cursor_t
	err := C.tidesdb_cursor_init(db.tdb, cfName, &cursor)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	return &Cursor{cursor: cursor}, nil
}

// Next moves the cursor to the next key-value pair.
func (c *Cursor) Next() error {
	err := C.tidesdb_cursor_next(c.cursor)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Prev moves the cursor to the previous key-value pair.
func (c *Cursor) Prev() error {
	err := C.tidesdb_cursor_prev(c.cursor)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Get gets the current key-value pair from the cursor.
func (c *Cursor) Get() ([]byte, []byte, error) {
	var cKey *C.uint8_t
	var cKeySize C.size_t
	var cValue *C.uint8_t
	var cValueSize C.size_t

	err := C.tidesdb_cursor_get(c.cursor, &cKey, &cKeySize, &cValue, &cValueSize)
	if err != nil {
		return nil, nil, errors.New(C.GoString(err.message))
	}

	key := C.GoBytes(unsafe.Pointer(cKey), C.int(cKeySize))
	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))

	return key, value, nil
}

// Free frees the memory for the cursor.
func (c *Cursor) Free() error {
	err := C.tidesdb_cursor_free(c.cursor)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// BeginTxn begins a transaction.
func (db *TidesDB) BeginTxn(columnFamily string) (*Transaction, error) {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var txn *C.tidesdb_txn_t
	err := C.tidesdb_txn_begin(db.tdb, &txn, cfName)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	return &Transaction{txn: txn}, nil
}

// Put adds a key-value pair to the transaction.
func (txn *Transaction) Put(key, value []byte, ttl int64) error {
	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	cValue := (*C.uint8_t)(unsafe.Pointer(&value[0]))

	err := C.tidesdb_txn_put(txn.txn, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), C.time_t(ttl))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Get gets a value in the transaction.
func (txn *Transaction) Get(key []byte) ([]byte, error) {
	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))

	var cValue *C.uint8_t
	var cValueSize C.size_t

	err := C.tidesdb_txn_get(txn.txn, cKey, C.size_t(len(key)), &cValue, &cValueSize)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	return value, nil
}

// Delete removes a key-value pair from the transaction.
func (txn *Transaction) Delete(key []byte) error {
	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))

	err := C.tidesdb_txn_delete(txn.txn, cKey, C.size_t(len(key)))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Commit commits the transaction.
func (txn *Transaction) Commit() error {
	err := C.tidesdb_txn_commit(txn.txn)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Rollback rolls back the transaction.
func (txn *Transaction) Rollback() error {
	err := C.tidesdb_txn_rollback(txn.txn)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Free frees the transaction and its operations.
func (txn *Transaction) Free() error {
	err := C.tidesdb_txn_free(txn.txn)
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// StartIncrementalMerge starts a background incremental merges for a column family.  Will run in background until db closure.  Will merge pairs incrementally and only once min sstables are has been reached.
func (db *TidesDB) StartIncrementalMerge(columnFamilyName string, seconds, minSSTables int) error {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	err := C.tidesdb_start_incremental_merge(db.tdb, cfName, C.int(seconds), C.int(minSSTables))
	if err != nil {
		return errors.New(C.GoString(err.message))
	}
	return nil
}

// Range retrieves all key-value pairs within a specified range.
func (db *TidesDB) Range(columnFamilyName string, startKey, endKey []byte) ([][2][]byte, error) {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	var cStartKey, cEndKey *C.uint8_t
	var cStartKeySize, cEndKeySize C.size_t

	if len(startKey) > 0 {
		cStartKey = (*C.uint8_t)(unsafe.Pointer(&startKey[0]))
		cStartKeySize = C.size_t(len(startKey))
	}

	if len(endKey) > 0 {
		cEndKey = (*C.uint8_t)(unsafe.Pointer(&endKey[0]))
		cEndKeySize = C.size_t(len(endKey))
	}

	var result **C.tidesdb_key_value_pair_t
	var resultSize C.size_t

	err := C.tidesdb_range(db.tdb, cfName, cStartKey, cStartKeySize,
		cEndKey, cEndKeySize, &result, &resultSize)

	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	// Convert C result to Go
	size := int(resultSize)
	pairs := make([][2][]byte, size)

	resultSlice := (*[1 << 30]*C.tidesdb_key_value_pair_t)(unsafe.Pointer(result))[:size:size]

	for i := 0; i < size; i++ {
		kvPair := resultSlice[i]

		// Copy key and value to Go slices
		pairs[i][0] = C.GoBytes(unsafe.Pointer(kvPair.key), C.int(kvPair.key_size))
		pairs[i][1] = C.GoBytes(unsafe.Pointer(kvPair.value), C.int(kvPair.value_size))
	}

	for i := 0; i < size; i++ {
		// Free individual key-value pairs
		C.free(unsafe.Pointer(resultSlice[i]))
	}

	// Free the array itself
	C.free(unsafe.Pointer(result))

	return pairs, nil
}

// DeleteByRange deletes all key-value pairs within a specified range atomically.
func (db *TidesDB) DeleteByRange(columnFamilyName string, startKey, endKey []byte) error {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	var cStartKey, cEndKey *C.uint8_t
	var cStartKeySize, cEndKeySize C.size_t

	if len(startKey) > 0 {
		cStartKey = (*C.uint8_t)(unsafe.Pointer(&startKey[0]))
		cStartKeySize = C.size_t(len(startKey))
	}

	if len(endKey) > 0 {
		cEndKey = (*C.uint8_t)(unsafe.Pointer(&endKey[0]))
		cEndKeySize = C.size_t(len(endKey))
	}

	err := C.tidesdb_delete_by_range(db.tdb, cfName, cStartKey, cStartKeySize,
		cEndKey, cEndKeySize)

	if err != nil {
		return errors.New(C.GoString(err.message))
	}

	return nil
}

// GetColumnFamilyStat retrieves statistics about a column family.
func (db *TidesDB) GetColumnFamilyStat(columnFamilyName string) (*ColumnFamilyStat, error) {
	cfName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cfName))

	var cStat *C.tidesdb_column_family_stat_t
	err := C.tidesdb_get_column_family_stat(db.tdb, cfName, &cStat)
	if err != nil {
		return nil, errors.New(C.GoString(err.message))
	}

	var stat *ColumnFamilyStat

	defer func() {
		if cStat != nil {
			C.tidesdb_free_column_family_stat(cStat)
		}
	}()

	configName := C.GoString(cStat.config.name)
	cfNameVal := C.GoString(cStat.cf_name)

	var ssTablePaths []string
	var cSSTableStatSlice []*C.tidesdb_column_family_sstable_stat_t
	if cStat.sstable_stats != nil && cStat.num_sstables > 0 {
		cSSTableStatSlice = (*[1 << 30]*C.tidesdb_column_family_sstable_stat_t)(unsafe.Pointer(cStat.sstable_stats))[:int(cStat.num_sstables):int(cStat.num_sstables)]
		ssTablePaths = make([]string, int(cStat.num_sstables))
		for i := 0; i < int(cStat.num_sstables); i++ {
			ssTablePaths[i] = C.GoString(cSSTableStatSlice[i].sstable_path)
		}
	}

	stat = &ColumnFamilyStat{
		Config: ColumnFamilyConfig{
			Name:           configName,
			FlushThreshold: int32(cStat.config.flush_threshold),
			MaxLevel:       int32(cStat.config.max_level),
			Probability:    float32(cStat.config.probability),
			Compressed:     bool(cStat.config.compressed),
			CompressAlgo:   TidesDBCompressionAlgo(cStat.config.compress_algo),
			BloomFilter:    bool(cStat.config.bloom_filter),
		},
		Name:               cfNameVal,
		NumSSTables:        int(cStat.num_sstables),
		MemtableSize:       int64(cStat.memtable_size),
		MemtableEntryCount: int64(cStat.memtable_entries_count),
		IncrementalMerging: bool(cStat.incremental_merging),
	}

	if cStat.sstable_stats != nil && cStat.num_sstables > 0 {
		stat.SSTableStats = make([]*ColumnFamilySSTableStat, int(cStat.num_sstables))
		for i := 0; i < int(cStat.num_sstables); i++ {
			stat.SSTableStats[i] = &ColumnFamilySSTableStat{
				Path:      ssTablePaths[i],
				Size:      int64(cSSTableStatSlice[i].size),
				NumBlocks: int64(cSSTableStatSlice[i].num_blocks),
			}
		}
	}

	return stat, nil
}
