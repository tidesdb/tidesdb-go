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
#cgo LDFLAGS: -ltidesdb
#cgo darwin CFLAGS: -I/opt/homebrew/include
#cgo darwin LDFLAGS: -L/opt/homebrew/lib -L/usr/local/lib -Wl,-rpath,/usr/local/lib
#cgo linux LDFLAGS: -lpthread -lm
#cgo windows CFLAGS: -I/mingw64/include
#cgo windows LDFLAGS: -L/mingw64/lib -lws2_32 -lbcrypt -lzstd -llz4 -lsnappy
#include <tidesdb/db.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// CompressionAlgorithm the compression algorithm type.
type CompressionAlgorithm int

const (
	NoCompression       CompressionAlgorithm = C.NO_COMPRESSION
	LZ4Compression      CompressionAlgorithm = C.LZ4_COMPRESSION
	ZstdCompression     CompressionAlgorithm = C.ZSTD_COMPRESSION
	LZ4FastCompression  CompressionAlgorithm = C.LZ4_FAST_COMRESSION
)

// SyncMode the sync mode for durability.
type SyncMode int

const (
	SyncNone     SyncMode = C.TDB_SYNC_NONE
	SyncFull     SyncMode = C.TDB_SYNC_FULL
	SyncInterval SyncMode = C.TDB_SYNC_INTERVAL
)

// LogLevel the logging level.
type LogLevel int

const (
	LogDebug LogLevel = C.TDB_LOG_DEBUG
	LogInfo  LogLevel = C.TDB_LOG_INFO
	LogWarn  LogLevel = C.TDB_LOG_WARN
	LogError LogLevel = C.TDB_LOG_ERROR
	LogFatal LogLevel = C.TDB_LOG_FATAL
	LogNone  LogLevel = C.TDB_LOG_NONE
)

// IsolationLevel the transaction isolation level.
type IsolationLevel int

const (
	IsolationReadUncommitted IsolationLevel = C.TDB_ISOLATION_READ_UNCOMMITTED
	IsolationReadCommitted   IsolationLevel = C.TDB_ISOLATION_READ_COMMITTED
	IsolationRepeatableRead  IsolationLevel = C.TDB_ISOLATION_REPEATABLE_READ
	IsolationSnapshot        IsolationLevel = C.TDB_ISOLATION_SNAPSHOT
	IsolationSerializable    IsolationLevel = C.TDB_ISOLATION_SERIALIZABLE
)

// Error codes from TidesDB
const (
	ErrSuccess     = C.TDB_SUCCESS
	ErrMemory      = C.TDB_ERR_MEMORY
	ErrInvalidArgs = C.TDB_ERR_INVALID_ARGS
	ErrNotFound    = C.TDB_ERR_NOT_FOUND
	ErrIO          = C.TDB_ERR_IO
	ErrCorruption  = C.TDB_ERR_CORRUPTION
	ErrExists      = C.TDB_ERR_EXISTS
	ErrConflict    = C.TDB_ERR_CONFLICT
	ErrTooLarge    = C.TDB_ERR_TOO_LARGE
	ErrMemoryLimit = C.TDB_ERR_MEMORY_LIMIT
	ErrInvalidDB   = C.TDB_ERR_INVALID_DB
	ErrUnknown     = C.TDB_ERR_UNKNOWN
	ErrLocked      = C.TDB_ERR_LOCKED
)

// TidesDB is a TidesDB instance.
type TidesDB struct {
	db *C.tidesdb_t
}

// Iterator is a TidesDB iterator.
type Iterator struct {
	iter *C.tidesdb_iter_t
}

// Transaction is a TidesDB transaction.
type Transaction struct {
	txn *C.tidesdb_txn_t
}

// ColumnFamily is a TidesDB column family.
type ColumnFamily struct {
	cf *C.tidesdb_column_family_t
}

// Config is the configuration for opening a TidesDB instance.
type Config struct {
	DBPath               string
	NumFlushThreads      int
	NumCompactionThreads int
	LogLevel             LogLevel
	BlockCacheSize       uint64
	MaxOpenSSTables      uint64
}

// ColumnFamilyConfig is the configuration for a column family.
type ColumnFamilyConfig struct {
	WriteBufferSize       uint64
	LevelSizeRatio        uint64
	MinLevels             int
	DividingLevelOffset   int
	KlogValueThreshold    uint64
	CompressionAlgorithm  CompressionAlgorithm
	EnableBloomFilter     bool
	BloomFPR              float64
	EnableBlockIndexes    bool
	IndexSampleRatio      int
	BlockIndexPrefixLen   int
	SyncMode              SyncMode
	SyncIntervalUs        uint64
	ComparatorName        string
	SkipListMaxLevel      int
	SkipListProbability   float32
	DefaultIsolationLevel IsolationLevel
	MinDiskSpace          uint64
	L1FileCountTrigger    int
	L0QueueStallThreshold int
}

// Stats is statistics about a column family.
type Stats struct {
	NumLevels        int
	MemtableSize     uint64
	LevelSizes       []uint64
	LevelNumSSTables []int
	Config           *ColumnFamilyConfig
}

// CacheStats is statistics about the block cache.
type CacheStats struct {
	Enabled       bool
	TotalEntries  uint64
	TotalBytes    uint64
	Hits          uint64
	Misses        uint64
	HitRate       float64
	NumPartitions uint64
}

// errorFromCode converts a C error code to a GO error.
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
	case C.TDB_ERR_NOT_FOUND:
		errMsg = "not found"
	case C.TDB_ERR_IO:
		errMsg = "I/O error"
	case C.TDB_ERR_CORRUPTION:
		errMsg = "data corruption"
	case C.TDB_ERR_EXISTS:
		errMsg = "already exists"
	case C.TDB_ERR_CONFLICT:
		errMsg = "transaction conflict"
	case C.TDB_ERR_TOO_LARGE:
		errMsg = "key or value too large"
	case C.TDB_ERR_MEMORY_LIMIT:
		errMsg = "memory limit exceeded"
	case C.TDB_ERR_INVALID_DB:
		errMsg = "invalid database handle"
	case C.TDB_ERR_UNKNOWN:
		errMsg = "unknown error"
	case C.TDB_ERR_LOCKED:
		errMsg = "database is locked"
	default:
		errMsg = "unknown error"
	}

	if context != "" {
		return fmt.Errorf("%s: %s (code: %d)", context, errMsg, code)
	}
	return fmt.Errorf("%s (code: %d)", errMsg, code)
}

// DefaultConfig returns a default database configuration.
func DefaultConfig() Config {
	return Config{
		DBPath:               "",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024, 
		MaxOpenSSTables:      256,
	}
}

// DefaultColumnFamilyConfig returns a default column family configuration.
func DefaultColumnFamilyConfig() ColumnFamilyConfig {
	cConfig := C.tidesdb_default_column_family_config()
	return ColumnFamilyConfig{
		WriteBufferSize:       uint64(cConfig.write_buffer_size),
		LevelSizeRatio:        uint64(cConfig.level_size_ratio),
		MinLevels:             int(cConfig.min_levels),
		DividingLevelOffset:   int(cConfig.dividing_level_offset),
		KlogValueThreshold:    uint64(cConfig.klog_value_threshold),
		CompressionAlgorithm:  CompressionAlgorithm(cConfig.compression_algo),
		EnableBloomFilter:     cConfig.enable_bloom_filter != 0,
		BloomFPR:              float64(cConfig.bloom_fpr),
		EnableBlockIndexes:    cConfig.enable_block_indexes != 0,
		IndexSampleRatio:      int(cConfig.index_sample_ratio),
		BlockIndexPrefixLen:   int(cConfig.block_index_prefix_len),
		SyncMode:              SyncMode(cConfig.sync_mode),
		SyncIntervalUs:        uint64(cConfig.sync_interval_us),
		ComparatorName:        C.GoString(&cConfig.comparator_name[0]),
		SkipListMaxLevel:      int(cConfig.skip_list_max_level),
		SkipListProbability:   float32(cConfig.skip_list_probability),
		DefaultIsolationLevel: IsolationLevel(cConfig.default_isolation_level),
		MinDiskSpace:          uint64(cConfig.min_disk_space),
		L1FileCountTrigger:    int(cConfig.l1_file_count_trigger),
		L0QueueStallThreshold: int(cConfig.l0_queue_stall_threshold),
	}
}

// Open opens a TidesDB instance with the given configuration.
func Open(config Config) (*TidesDB, error) {
	cPath := C.CString(config.DBPath)
	defer C.free(unsafe.Pointer(cPath))

	cConfig := C.tidesdb_config_t{
		db_path:                cPath,
		num_flush_threads:      C.int(config.NumFlushThreads),
		num_compaction_threads: C.int(config.NumCompactionThreads),
		log_level:              C.tidesdb_log_level_t(config.LogLevel),
		block_cache_size:       C.size_t(config.BlockCacheSize),
		max_open_sstables:      C.size_t(config.MaxOpenSSTables),
	}

	var db *C.tidesdb_t
	result := C.tidesdb_open(&cConfig, &db)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to open database")
	}

	return &TidesDB{db: db}, nil
}

// Close closes a TidesDB instance.
func (db *TidesDB) Close() error {
	if db == nil || db.db == nil {
		return nil
	}
	result := C.tidesdb_close(db.db)
	db.db = nil
	return errorFromCode(result, "failed to close database")
}

// CreateColumnFamily creates a new column family with the given configuration.
func (db *TidesDB) CreateColumnFamily(name string, config ColumnFamilyConfig) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cConfig := C.tidesdb_column_family_config_t{
		write_buffer_size:       C.size_t(config.WriteBufferSize),
		level_size_ratio:        C.size_t(config.LevelSizeRatio),
		min_levels:              C.int(config.MinLevels),
		dividing_level_offset:   C.int(config.DividingLevelOffset),
		klog_value_threshold:    C.size_t(config.KlogValueThreshold),
		compression_algo:        C.compression_algorithm(config.CompressionAlgorithm),
		enable_bloom_filter:     C.int(0),
		bloom_fpr:               C.double(config.BloomFPR),
		enable_block_indexes:    C.int(0),
		index_sample_ratio:      C.int(config.IndexSampleRatio),
		block_index_prefix_len:  C.int(config.BlockIndexPrefixLen),
		sync_mode:               C.int(config.SyncMode),
		sync_interval_us:        C.uint64_t(config.SyncIntervalUs),
		skip_list_max_level:     C.int(config.SkipListMaxLevel),
		skip_list_probability:   C.float(config.SkipListProbability),
		default_isolation_level: C.tidesdb_isolation_level_t(config.DefaultIsolationLevel),
		min_disk_space:          C.uint64_t(config.MinDiskSpace),
		l1_file_count_trigger:   C.int(config.L1FileCountTrigger),
		l0_queue_stall_threshold: C.int(config.L0QueueStallThreshold),
	}

	if config.EnableBloomFilter {
		cConfig.enable_bloom_filter = C.int(1)
	}
	if config.EnableBlockIndexes {
		cConfig.enable_block_indexes = C.int(1)
	}

	if config.ComparatorName != "" {
		cCompName := C.CString(config.ComparatorName)
		defer C.free(unsafe.Pointer(cCompName))
		C.strncpy(&cConfig.comparator_name[0], cCompName, C.TDB_MAX_COMPARATOR_NAME-1)
		cConfig.comparator_name[C.TDB_MAX_COMPARATOR_NAME-1] = 0
	}

	result := C.tidesdb_create_column_family(db.db, cName, &cConfig)
	return errorFromCode(result, "failed to create column family")
}

// DropColumnFamily drops a column family and all associated data.
func (db *TidesDB) DropColumnFamily(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tidesdb_drop_column_family(db.db, cName)
	return errorFromCode(result, "failed to drop column family")
}

// GetColumnFamily retrieves a column family by name.
func (db *TidesDB) GetColumnFamily(name string) (*ColumnFamily, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cf := C.tidesdb_get_column_family(db.db, cName)
	if cf == nil {
		return nil, fmt.Errorf("column family not found: %s", name)
	}

	return &ColumnFamily{cf: cf}, nil
}

// ListColumnFamilies lists all column families in the database.
func (db *TidesDB) ListColumnFamilies() ([]string, error) {
	var names **C.char
	var count C.int

	result := C.tidesdb_list_column_families(db.db, &names, &count)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to list column families")
	}

	if count == 0 {
		return []string{}, nil
	}

	// Convert C array to Go slice
	namesSlice := (*[1 << 30]*C.char)(unsafe.Pointer(names))[:count:count]
	resultNames := make([]string, count)

	for i := 0; i < int(count); i++ {
		resultNames[i] = C.GoString(namesSlice[i])
		C.free(unsafe.Pointer(namesSlice[i]))
	}
	C.free(unsafe.Pointer(names))

	return resultNames, nil
}

// GetStats retrieves statistics about a column family.
func (cf *ColumnFamily) GetStats() (*Stats, error) {
	var cStats *C.tidesdb_stats_t
	result := C.tidesdb_get_stats(cf.cf, &cStats)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get stats")
	}
	defer C.tidesdb_free_stats(cStats)

	stats := &Stats{
		NumLevels:    int(cStats.num_levels),
		MemtableSize: uint64(cStats.memtable_size),
	}

	if cStats.num_levels > 0 && cStats.level_sizes != nil {
		levelSizes := (*[1 << 30]C.size_t)(unsafe.Pointer(cStats.level_sizes))[:cStats.num_levels:cStats.num_levels]
		stats.LevelSizes = make([]uint64, cStats.num_levels)
		for i := 0; i < int(cStats.num_levels); i++ {
			stats.LevelSizes[i] = uint64(levelSizes[i])
		}
	}

	if cStats.num_levels > 0 && cStats.level_num_sstables != nil {
		levelNumSSTables := (*[1 << 30]C.int)(unsafe.Pointer(cStats.level_num_sstables))[:cStats.num_levels:cStats.num_levels]
		stats.LevelNumSSTables = make([]int, cStats.num_levels)
		for i := 0; i < int(cStats.num_levels); i++ {
			stats.LevelNumSSTables[i] = int(levelNumSSTables[i])
		}
	}

	if cStats.config != nil {
		stats.Config = &ColumnFamilyConfig{
			WriteBufferSize:       uint64(cStats.config.write_buffer_size),
			LevelSizeRatio:        uint64(cStats.config.level_size_ratio),
			MinLevels:             int(cStats.config.min_levels),
			DividingLevelOffset:   int(cStats.config.dividing_level_offset),
			KlogValueThreshold:    uint64(cStats.config.klog_value_threshold),
			CompressionAlgorithm:  CompressionAlgorithm(cStats.config.compression_algo),
			EnableBloomFilter:     cStats.config.enable_bloom_filter != 0,
			BloomFPR:              float64(cStats.config.bloom_fpr),
			EnableBlockIndexes:    cStats.config.enable_block_indexes != 0,
			IndexSampleRatio:      int(cStats.config.index_sample_ratio),
			BlockIndexPrefixLen:   int(cStats.config.block_index_prefix_len),
			SyncMode:              SyncMode(cStats.config.sync_mode),
			SyncIntervalUs:        uint64(cStats.config.sync_interval_us),
			ComparatorName:        C.GoString(&cStats.config.comparator_name[0]),
			SkipListMaxLevel:      int(cStats.config.skip_list_max_level),
			SkipListProbability:   float32(cStats.config.skip_list_probability),
			DefaultIsolationLevel: IsolationLevel(cStats.config.default_isolation_level),
			MinDiskSpace:          uint64(cStats.config.min_disk_space),
			L1FileCountTrigger:    int(cStats.config.l1_file_count_trigger),
			L0QueueStallThreshold: int(cStats.config.l0_queue_stall_threshold),
		}
	}

	return stats, nil
}

// GetCacheStats retrieves statistics about the block cache.
func (db *TidesDB) GetCacheStats() (*CacheStats, error) {
	var cStats C.tidesdb_cache_stats_t
	result := C.tidesdb_get_cache_stats(db.db, &cStats)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get cache stats")
	}

	return &CacheStats{
		Enabled:       cStats.enabled != 0,
		TotalEntries:  uint64(cStats.total_entries),
		TotalBytes:    uint64(cStats.total_bytes),
		Hits:          uint64(cStats.hits),
		Misses:        uint64(cStats.misses),
		HitRate:       float64(cStats.hit_rate),
		NumPartitions: uint64(cStats.num_partitions),
	}, nil
}

// Compact manually triggers compaction for a column family.
func (cf *ColumnFamily) Compact() error {
	result := C.tidesdb_compact(cf.cf)
	return errorFromCode(result, "failed to compact column family")
}

// FlushMemtable manually triggers memtable flush for a column family.
func (cf *ColumnFamily) FlushMemtable() error {
	result := C.tidesdb_flush_memtable(cf.cf)
	return errorFromCode(result, "failed to flush memtable")
}

// RegisterComparator registers a custom comparator with the database.
func (db *TidesDB) RegisterComparator(name string, ctxStr string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cCtxStr *C.char
	if ctxStr != "" {
		cCtxStr = C.CString(ctxStr)
		defer C.free(unsafe.Pointer(cCtxStr))
	}

	result := C.tidesdb_register_comparator(db.db, cName, nil, cCtxStr, nil)
	return errorFromCode(result, "failed to register comparator")
}

// BeginTxn begins a new transaction with default isolation level.
func (db *TidesDB) BeginTxn() (*Transaction, error) {
	var txn *C.tidesdb_txn_t
	result := C.tidesdb_txn_begin(db.db, &txn)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to begin transaction")
	}
	return &Transaction{txn: txn}, nil
}

// BeginTxnWithIsolation begins a new transaction with the specified isolation level.
func (db *TidesDB) BeginTxnWithIsolation(isolation IsolationLevel) (*Transaction, error) {
	var txn *C.tidesdb_txn_t
	result := C.tidesdb_txn_begin_with_isolation(db.db, C.tidesdb_isolation_level_t(isolation), &txn)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to begin transaction with isolation")
	}
	return &Transaction{txn: txn}, nil
}

// Put adds a key-value pair to the transaction.
// TTL is Unix timestamp (seconds since epoch) for expiration, or -1 for no expiration.
func (txn *Transaction) Put(cf *ColumnFamily, key, value []byte, ttl int64) error {
	var cKey, cValue *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}
	if len(value) > 0 {
		cValue = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	result := C.tidesdb_txn_put(txn.txn, cf.cf, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), C.time_t(ttl))
	return errorFromCode(result, "failed to put key-value pair")
}

// Get retrieves a value from the transaction.
func (txn *Transaction) Get(cf *ColumnFamily, key []byte) ([]byte, error) {
	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	var cValue *C.uint8_t
	var cValueSize C.size_t

	result := C.tidesdb_txn_get(txn.txn, cf.cf, cKey, C.size_t(len(key)), &cValue, &cValueSize)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get value")
	}

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	C.free(unsafe.Pointer(cValue))
	return value, nil
}

// Delete removes a key-value pair from the transaction.
func (txn *Transaction) Delete(cf *ColumnFamily, key []byte) error {
	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.tidesdb_txn_delete(txn.txn, cf.cf, cKey, C.size_t(len(key)))
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
	if txn != nil && txn.txn != nil {
		C.tidesdb_txn_free(txn.txn)
		txn.txn = nil
	}
}

// Savepoint creates a savepoint within the transaction.
func (txn *Transaction) Savepoint(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tidesdb_txn_savepoint(txn.txn, cName)
	return errorFromCode(result, "failed to create savepoint")
}

// RollbackToSavepoint rolls back the transaction to a savepoint.
func (txn *Transaction) RollbackToSavepoint(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tidesdb_txn_rollback_to_savepoint(txn.txn, cName)
	return errorFromCode(result, "failed to rollback to savepoint")
}

// ReleaseSavepoint releases a savepoint without rolling back.
func (txn *Transaction) ReleaseSavepoint(name string) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	result := C.tidesdb_txn_release_savepoint(txn.txn, cName)
	return errorFromCode(result, "failed to release savepoint")
}

// NewIterator creates a new iterator for a column family within a transaction.
func (txn *Transaction) NewIterator(cf *ColumnFamily) (*Iterator, error) {
	var iter *C.tidesdb_iter_t
	result := C.tidesdb_iter_new(txn.txn, cf.cf, &iter)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to create iterator")
	}
	return &Iterator{iter: iter}, nil
}

// SeekToFirst positions the iterator at the first key.
func (iter *Iterator) SeekToFirst() error {
	result := C.tidesdb_iter_seek_to_first(iter.iter)
	return errorFromCode(result, "failed to seek to first")
}

// SeekToLast positions the iterator at the last key.
func (iter *Iterator) SeekToLast() error {
	result := C.tidesdb_iter_seek_to_last(iter.iter)
	return errorFromCode(result, "failed to seek to last")
}

// Seek positions the iterator at the first key >= target key.
func (iter *Iterator) Seek(key []byte) error {
	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.tidesdb_iter_seek(iter.iter, cKey, C.size_t(len(key)))
	return errorFromCode(result, "failed to seek")
}

// SeekForPrev positions the iterator at the last key <= target key.
func (iter *Iterator) SeekForPrev(key []byte) error {
	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.tidesdb_iter_seek_for_prev(iter.iter, cKey, C.size_t(len(key)))
	return errorFromCode(result, "failed to seek for prev")
}

// Valid returns true if the iterator is positioned at a valid entry.
func (iter *Iterator) Valid() bool {
	return C.tidesdb_iter_valid(iter.iter) != 0
}

// Next moves the iterator to the next entry.
func (iter *Iterator) Next() error {
	result := C.tidesdb_iter_next(iter.iter)
	return errorFromCode(result, "failed to move to next")
}

// Prev moves the iterator to the previous entry.
func (iter *Iterator) Prev() error {
	result := C.tidesdb_iter_prev(iter.iter)
	return errorFromCode(result, "failed to move to prev")
}

// Key retrieves the current key from the iterator.
func (iter *Iterator) Key() ([]byte, error) {
	var cKey *C.uint8_t
	var cKeySize C.size_t

	result := C.tidesdb_iter_key(iter.iter, &cKey, &cKeySize)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get key")
	}

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

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	return value, nil
}

// Free frees the iterator resources.
func (iter *Iterator) Free() {
	if iter != nil && iter.iter != nil {
		C.tidesdb_iter_free(iter.iter)
		iter.iter = nil
	}
}
