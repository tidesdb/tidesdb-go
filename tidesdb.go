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
#cgo darwin CFLAGS: -I/opt/homebrew/include
#cgo darwin LDFLAGS: -L/opt/homebrew/lib
#cgo linux LDFLAGS: -lpthread -lm
#include <tidesdb/tidesdb.h>
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// TidesDBCompressionAlgo represents the compression algorithm type.
type TidesDBCompressionAlgo int

const (
	TDB_COMPRESS_SNAPPY TidesDBCompressionAlgo = C.COMPRESS_SNAPPY
	TDB_COMPRESS_LZ4    TidesDBCompressionAlgo = C.COMPRESS_LZ4
	TDB_COMPRESS_ZSTD   TidesDBCompressionAlgo = C.COMPRESS_ZSTD
)

// TidesDBSyncMode represents the sync mode for durability.
type TidesDBSyncMode int

const (
	TDB_SYNC_NONE       TidesDBSyncMode = C.TDB_SYNC_NONE
	TDB_SYNC_BACKGROUND TidesDBSyncMode = C.TDB_SYNC_BACKGROUND
	TDB_SYNC_FULL       TidesDBSyncMode = C.TDB_SYNC_FULL
)

// Error codes from TidesDB
const (
	TDB_SUCCESS                  = C.TDB_SUCCESS
	TDB_ERROR                    = C.TDB_ERROR
	TDB_ERR_MEMORY               = C.TDB_ERR_MEMORY
	TDB_ERR_INVALID_ARGS         = C.TDB_ERR_INVALID_ARGS
	TDB_ERR_IO                   = C.TDB_ERR_IO
	TDB_ERR_NOT_FOUND            = C.TDB_ERR_NOT_FOUND
	TDB_ERR_EXISTS               = C.TDB_ERR_EXISTS
	TDB_ERR_CORRUPT              = C.TDB_ERR_CORRUPT
	TDB_ERR_LOCK                 = C.TDB_ERR_LOCK
	TDB_ERR_TXN_COMMITTED        = C.TDB_ERR_TXN_COMMITTED
	TDB_ERR_TXN_ABORTED          = C.TDB_ERR_TXN_ABORTED
	TDB_ERR_READONLY             = C.TDB_ERR_READONLY
	TDB_ERR_FULL                 = C.TDB_ERR_FULL
	TDB_ERR_INVALID_NAME         = C.TDB_ERR_INVALID_NAME
	TDB_ERR_COMPARATOR_NOT_FOUND = C.TDB_ERR_COMPARATOR_NOT_FOUND
	TDB_ERR_MAX_COMPARATORS      = C.TDB_ERR_MAX_COMPARATORS
	TDB_ERR_INVALID_CF           = C.TDB_ERR_INVALID_CF
	TDB_ERR_THREAD               = C.TDB_ERR_THREAD
	TDB_ERR_CHECKSUM             = C.TDB_ERR_CHECKSUM
)

// TidesDB represents a TidesDB instance.
type TidesDB struct {
	tdb *C.tidesdb_t
}

// Iterator represents a TidesDB iterator.
type Iterator struct {
	iter *C.tidesdb_iter_t
}

// Transaction represents a TidesDB transaction.
type Transaction struct {
	txn *C.tidesdb_txn_t
}

// ColumnFamily represents a TidesDB column family.
type ColumnFamily struct {
	cf *C.tidesdb_column_family_t
}

// Config represents the configuration for opening a TidesDB instance.
type Config struct {
	DBPath             string
	EnableDebugLogging bool
}

// ColumnFamilyConfig represents the configuration for a column family.
type ColumnFamilyConfig struct {
	MemtableFlushSize            int
	MaxSSTablesBeforeCompaction  int
	CompactionThreads            int
	MaxLevel                     int
	Probability                  float32
	Compressed                   bool
	CompressAlgo                 TidesDBCompressionAlgo
	BloomFilterFPRate            float64
	EnableBackgroundCompaction   bool
	BackgroundCompactionInterval int
	UseSBHA                      bool
	SyncMode                     TidesDBSyncMode
	SyncInterval                 int
	ComparatorName               *string
}

// ColumnFamilySSTableStat represents statistics about an SSTable in a column family.
type ColumnFamilySSTableStat struct {
	Path      string
	Size      int64
	NumBlocks int64
}

// ColumnFamilyStat represents statistics about a column family.
type ColumnFamilyStat struct {
	Name             string
	ComparatorName   string
	NumSSTables      int
	TotalSSTableSize int64
	MemtableSize     int64
	MemtableEntries  int
	Config           ColumnFamilyConfig
}

// errorFromCode converts a C error code to a Go error.
func errorFromCode(code C.int, context string) error {
	if code == C.TDB_SUCCESS {
		return nil
	}

	var errMsg string
	switch code {
	case C.TDB_ERR_MEMORY:
		errMsg = "memory allocation failed"
	case C.TDB_ERR_INVALID_ARGS:
		errMsg = "invalid arguments"
	case C.TDB_ERR_IO:
		errMsg = "I/O error"
	case C.TDB_ERR_NOT_FOUND:
		errMsg = "not found"
	case C.TDB_ERR_EXISTS:
		errMsg = "already exists"
	case C.TDB_ERR_CORRUPT:
		errMsg = "data corruption"
	case C.TDB_ERR_LOCK:
		errMsg = "lock acquisition failed"
	case C.TDB_ERR_TXN_COMMITTED:
		errMsg = "transaction already committed"
	case C.TDB_ERR_TXN_ABORTED:
		errMsg = "transaction aborted"
	case C.TDB_ERR_READONLY:
		errMsg = "read-only transaction"
	case C.TDB_ERR_FULL:
		errMsg = "database full"
	case C.TDB_ERR_INVALID_NAME:
		errMsg = "invalid name"
	case C.TDB_ERR_COMPARATOR_NOT_FOUND:
		errMsg = "comparator not found"
	case C.TDB_ERR_MAX_COMPARATORS:
		errMsg = "max comparators reached"
	case C.TDB_ERR_INVALID_CF:
		errMsg = "invalid column family"
	case C.TDB_ERR_THREAD:
		errMsg = "thread operation failed"
	case C.TDB_ERR_CHECKSUM:
		errMsg = "checksum verification failed"
	default:
		errMsg = "unknown error"
	}

	if context != "" {
		return fmt.Errorf("%s: %s (code: %d)", context, errMsg, code)
	}
	return fmt.Errorf("%s (code: %d)", errMsg, code)
}

// DefaultColumnFamilyConfig returns a default column family configuration.
func DefaultColumnFamilyConfig() ColumnFamilyConfig {
	cConfig := C.tidesdb_default_column_family_config()
	return ColumnFamilyConfig{
		MemtableFlushSize:           int(cConfig.memtable_flush_size),
		MaxSSTablesBeforeCompaction: int(cConfig.max_sstables_before_compaction),
		CompactionThreads:           int(cConfig.compaction_threads),
		MaxLevel:                    int(cConfig.max_level),
		Probability:                 float32(cConfig.probability),
		Compressed:                  cConfig.compressed != 0,
		CompressAlgo:                TidesDBCompressionAlgo(cConfig.compress_algo),
		BloomFilterFPRate:           float64(cConfig.bloom_filter_fp_rate),
		EnableBackgroundCompaction:  cConfig.enable_background_compaction != 0,
		UseSBHA:                     cConfig.use_sbha != 0,
		SyncMode:                    TidesDBSyncMode(cConfig.sync_mode),
		SyncInterval:                int(cConfig.sync_interval),
		ComparatorName:              nil,
	}
}

// Open opens a TidesDB instance with the given configuration.
func Open(config Config) (*TidesDB, error) {
	cConfig := C.tidesdb_config_t{
		enable_debug_logging:  C.int(0),
		max_open_file_handles: C.int(0), // 0 = disabled (no caching)
	}
	if config.EnableDebugLogging {
		cConfig.enable_debug_logging = C.int(1)
	}

	cPath := C.CString(config.DBPath)
	defer C.free(unsafe.Pointer(cPath))
	// Copy path to fixed-size array
	for i := 0; i < len(config.DBPath) && i < 1024; i++ {
		cConfig.db_path[i] = C.char(config.DBPath[i])
	}
	cConfig.db_path[len(config.DBPath)] = 0

	var tdb *C.tidesdb_t
	result := C.tidesdb_open(&cConfig, &tdb)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to open database")
	}

	return &TidesDB{tdb: tdb}, nil
}

// Close closes a TidesDB instance.
func (db *TidesDB) Close() error {
	if db == nil || db.tdb == nil {
		return nil
	}
	result := C.tidesdb_close(db.tdb)
	db.tdb = nil
	return errorFromCode(result, "failed to close database")
}

// CreateColumnFamily creates a new column family with the given configuration.
func (db *TidesDB) CreateColumnFamily(name string, config ColumnFamilyConfig) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cConfig := C.tidesdb_column_family_config_t{
		memtable_flush_size:            C.size_t(config.MemtableFlushSize),
		max_sstables_before_compaction: C.int(config.MaxSSTablesBeforeCompaction),
		compaction_threads:             C.int(config.CompactionThreads),
		max_level:                      C.int(config.MaxLevel),
		probability:                    C.float(config.Probability),
		compressed:                     C.int(0),
		compress_algo:                  C.compress_type(config.CompressAlgo),
		bloom_filter_fp_rate:           C.double(config.BloomFilterFPRate),
		enable_background_compaction:   C.int(0),
		background_compaction_interval: C.int(config.BackgroundCompactionInterval),
		use_sbha:                       C.int(0),
		sync_mode:                      C.tidesdb_sync_mode_t(config.SyncMode),
		sync_interval:                  C.int(config.SyncInterval),
		comparator_name:                nil,
	}

	if config.Compressed {
		cConfig.compressed = C.int(1)
	}
	if config.EnableBackgroundCompaction {
		cConfig.enable_background_compaction = C.int(1)
	}
	if config.UseSBHA {
		cConfig.use_sbha = C.int(1)
	}
	if config.ComparatorName != nil {
		cCompName := C.CString(*config.ComparatorName)
		defer C.free(unsafe.Pointer(cCompName))
		cConfig.comparator_name = cCompName
	}

	result := C.tidesdb_create_column_family(db.tdb, cName, &cConfig)
	return errorFromCode(result, "failed to create column family")
}

// DropColumnFamily drops a column family and all associated data.
func (db *TidesDB) DropColumnFamily(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tidesdb_drop_column_family(db.tdb, cName)
	return errorFromCode(result, "failed to drop column family")
}

// GetColumnFamily retrieves a column family by name.
func (db *TidesDB) GetColumnFamily(name string) (*ColumnFamily, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cf := C.tidesdb_get_column_family(db.tdb, cName)
	if cf == nil {
		return nil, fmt.Errorf("column family not found: %s", name)
	}

	return &ColumnFamily{cf: cf}, nil
}

// ListColumnFamilies lists all column families in the database.
func (db *TidesDB) ListColumnFamilies() ([]string, error) {
	var names **C.char
	var count C.int

	result := C.tidesdb_list_column_families(db.tdb, &names, &count)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to list column families")
	}

	if count == 0 {
		return []string{}, nil
	}

	// Convert C array to Go slice
	namesSlice := (*[1 << 30]*C.char)(unsafe.Pointer(names))[:count:count]
	result_names := make([]string, count)

	for i := 0; i < int(count); i++ {
		result_names[i] = C.GoString(namesSlice[i])
		C.free(unsafe.Pointer(namesSlice[i]))
	}
	C.free(unsafe.Pointer(names))

	return result_names, nil
}

// GetColumnFamilyStats retrieves statistics about a column family.
func (db *TidesDB) GetColumnFamilyStats(name string) (*ColumnFamilyStat, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cStats *C.tidesdb_column_family_stat_t
	result := C.tidesdb_get_column_family_stats(db.tdb, cName, &cStats)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get column family stats")
	}
	defer C.free(unsafe.Pointer(cStats))

	stats := &ColumnFamilyStat{
		Name:             C.GoString(&cStats.name[0]),
		ComparatorName:   C.GoString(&cStats.comparator_name[0]),
		NumSSTables:      int(cStats.num_sstables),
		TotalSSTableSize: int64(cStats.total_sstable_size),
		MemtableSize:     int64(cStats.memtable_size),
		MemtableEntries:  int(cStats.memtable_entries),
		Config: ColumnFamilyConfig{
			MemtableFlushSize:           int(cStats.config.memtable_flush_size),
			MaxSSTablesBeforeCompaction: int(cStats.config.max_sstables_before_compaction),
			CompactionThreads:           int(cStats.config.compaction_threads),
			MaxLevel:                    int(cStats.config.max_level),
			Probability:                 float32(cStats.config.probability),
			Compressed:                  bool(cStats.config.compressed != 0),
			CompressAlgo:                TidesDBCompressionAlgo(cStats.config.compress_algo),
			BloomFilterFPRate:           float64(cStats.config.bloom_filter_fp_rate),
			EnableBackgroundCompaction:  bool(cStats.config.enable_background_compaction != 0),
			UseSBHA:                     bool(cStats.config.use_sbha != 0),
			SyncMode:                    TidesDBSyncMode(cStats.config.sync_mode),
			SyncInterval:                int(cStats.config.sync_interval),
		},
	}

	return stats, nil
}

// Compact manually triggers compaction for a column family.
func (cf *ColumnFamily) Compact() error {
	result := C.tidesdb_compact(cf.cf)
	return errorFromCode(result, "failed to compact column family")
}

// BeginTxn begins a new write transaction.
func (db *TidesDB) BeginTxn() (*Transaction, error) {
	var txn *C.tidesdb_txn_t
	result := C.tidesdb_txn_begin(db.tdb, &txn)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to begin transaction")
	}
	return &Transaction{txn: txn}, nil
}

// BeginReadTxn begins a new read-only transaction.
func (db *TidesDB) BeginReadTxn() (*Transaction, error) {
	var txn *C.tidesdb_txn_t
	result := C.tidesdb_txn_begin_read(db.tdb, &txn)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to begin read transaction")
	}
	return &Transaction{txn: txn}, nil
}

// Put adds a key-value pair to the transaction.
func (txn *Transaction) Put(columnFamily string, key, value []byte, ttl int64) error {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var cKey, cValue *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		cValue = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.tidesdb_txn_put(txn.txn, cfName, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), C.time_t(ttl))
	return errorFromCode(result, "failed to put key-value pair")
}

// Get retrieves a value from the transaction.
func (txn *Transaction) Get(columnFamily string, key []byte) ([]byte, error) {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	var cValue *C.uint8_t
	var cValueSize C.size_t

	result := C.tidesdb_txn_get(txn.txn, cfName, cKey, C.size_t(len(key)), &cValue, &cValueSize)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get value")
	}

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	C.free(unsafe.Pointer(cValue))
	return value, nil
}

// Delete removes a key-value pair from the transaction.
func (txn *Transaction) Delete(columnFamily string, key []byte) error {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.tidesdb_txn_delete(txn.txn, cfName, cKey, C.size_t(len(key)))
	return errorFromCode(result, "failed to delete key")
}

// Commit commits the transaction.
func (txn *Transaction) Commit() error {
	result := C.tidesdb_txn_commit(txn.txn)
	return errorFromCode(result, "failed to commit transaction")
}

// Rollback rolls back the transaction.
func (txn *Transaction) Rollback() error {
	result := C.tidesdb_txn_rollback(txn.txn)
	return errorFromCode(result, "failed to rollback transaction")
}

// Free frees the transaction resources.
func (txn *Transaction) Free() {
	C.tidesdb_txn_free(txn.txn)
}

// NewIterator creates a new iterator for a column family within a transaction.
func (txn *Transaction) NewIterator(columnFamily string) (*Iterator, error) {
	cfName := C.CString(columnFamily)
	defer C.free(unsafe.Pointer(cfName))

	var iter *C.tidesdb_iter_t
	result := C.tidesdb_iter_new(txn.txn, cfName, &iter)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to create iterator")
	}
	return &Iterator{iter: iter}, nil
}

// SeekToFirst positions the iterator at the first key.
// Note: This automatically advances to the first valid entry.
func (iter *Iterator) SeekToFirst() error {
	result := C.tidesdb_iter_seek_to_first(iter.iter)
	if result != C.TDB_SUCCESS {
		return errorFromCode(result, "failed to seek to first")
	}
	return nil
}

// SeekToLast positions the iterator at the last key.
// Note: This automatically moves to the last valid entry.
func (iter *Iterator) SeekToLast() error {
	result := C.tidesdb_iter_seek_to_last(iter.iter)
	if result != C.TDB_SUCCESS {
		return errorFromCode(result, "failed to seek to last")
	}
	return nil
}

// Valid returns true if the iterator is positioned at a valid entry.
func (iter *Iterator) Valid() bool {
	return C.tidesdb_iter_valid(iter.iter) != 0
}

// Next moves the iterator to the next entry.
func (iter *Iterator) Next() {
	C.tidesdb_iter_next(iter.iter)
}

// Prev moves the iterator to the previous entry.
func (iter *Iterator) Prev() {
	C.tidesdb_iter_prev(iter.iter)
}

// Key retrieves the current key from the iterator.
func (iter *Iterator) Key() ([]byte, error) {
	var cKey *C.uint8_t
	var cKeySize C.size_t

	result := C.tidesdb_iter_key(iter.iter, &cKey, &cKeySize)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get key")
	}

	// Note: Don't free cKey - it's managed by the iterator and freed with tidesdb_iter_free
	key := C.GoBytes(unsafe.Pointer(cKey), C.int(cKeySize))
	return key, nil
}

// Value retrieves the current value from the iterator.
func (iter *Iterator) Value() ([]byte, error) {
	var cValue *C.uint8_t
	var cValueSize C.size_t

	result := C.tidesdb_iter_value(iter.iter, &cValue, &cValueSize)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get value")
	}

	// Note: Don't free cValue - it's managed by the iterator and freed with tidesdb_iter_free
	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	return value, nil
}

// Free frees the iterator resources.
func (iter *Iterator) Free() {
	C.tidesdb_iter_free(iter.iter)
}
