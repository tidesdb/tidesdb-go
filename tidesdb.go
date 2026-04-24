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
	NoCompression      CompressionAlgorithm = C.TDB_COMPRESS_NONE
	SnappyCompression  CompressionAlgorithm = C.TDB_COMPRESS_SNAPPY
	LZ4Compression     CompressionAlgorithm = C.TDB_COMPRESS_LZ4
	ZstdCompression    CompressionAlgorithm = C.TDB_COMPRESS_ZSTD
	LZ4FastCompression CompressionAlgorithm = C.TDB_COMPRESS_LZ4_FAST
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
	ErrReadonly    = C.TDB_ERR_READONLY
)

// ObjStoreBackend identifies the object store backend in use.
type ObjStoreBackend int

const (
	BackendFS      ObjStoreBackend = C.TDB_BACKEND_FS
	BackendS3      ObjStoreBackend = C.TDB_BACKEND_S3
	BackendUnknown ObjStoreBackend = C.TDB_BACKEND_UNKNOWN
)

// ObjStore is an opaque object store connector handle.
type ObjStore struct {
	store *C.tidesdb_objstore_t
}

// ObjStoreConfig configures object store mode behavior.
type ObjStoreConfig struct {
	LocalCachePath         string
	LocalCacheMaxBytes     uint64
	CacheOnRead            bool
	CacheOnWrite           bool
	MaxConcurrentUploads   int
	MaxConcurrentDownloads int
	MultipartThreshold     uint64
	MultipartPartSize      uint64
	SyncManifestToObject   bool
	ReplicateWal           bool
	WalUploadSync          bool
	WalSyncThresholdBytes  uint64
	WalSyncOnCommit        bool
	ReplicaMode            bool
	ReplicaSyncIntervalUs  uint64
	ReplicaReplayWal       bool
}

// ObjStoreDefaultConfig returns the default object store configuration.
func ObjStoreDefaultConfig() ObjStoreConfig {
	cConfig := C.tidesdb_objstore_default_config()
	return ObjStoreConfig{
		LocalCacheMaxBytes:     uint64(cConfig.local_cache_max_bytes),
		CacheOnRead:            cConfig.cache_on_read != 0,
		CacheOnWrite:           cConfig.cache_on_write != 0,
		MaxConcurrentUploads:   int(cConfig.max_concurrent_uploads),
		MaxConcurrentDownloads: int(cConfig.max_concurrent_downloads),
		MultipartThreshold:     uint64(cConfig.multipart_threshold),
		MultipartPartSize:      uint64(cConfig.multipart_part_size),
		SyncManifestToObject:   cConfig.sync_manifest_to_object != 0,
		ReplicateWal:           cConfig.replicate_wal != 0,
		WalUploadSync:          cConfig.wal_upload_sync != 0,
		WalSyncThresholdBytes:  uint64(cConfig.wal_sync_threshold_bytes),
		WalSyncOnCommit:        cConfig.wal_sync_on_commit != 0,
		ReplicaMode:            cConfig.replica_mode != 0,
		ReplicaSyncIntervalUs:  uint64(cConfig.replica_sync_interval_us),
		ReplicaReplayWal:       cConfig.replica_replay_wal != 0,
	}
}

// ObjStoreFsCreate creates a filesystem-backed object store connector for testing and local replication.
// Objects are stored as files under rootDir mirroring the key path structure.
func ObjStoreFsCreate(rootDir string) (*ObjStore, error) {
	cRootDir := C.CString(rootDir)
	defer C.free(unsafe.Pointer(cRootDir))

	store := C.tidesdb_objstore_fs_create(cRootDir)
	if store == nil {
		return nil, fmt.Errorf("failed to create filesystem object store at %s", rootDir)
	}

	return &ObjStore{store: store}, nil
}

// Init initializes TidesDB with the system allocator.
// Must be called exactly once before any other TidesDB function.
// If not called, TidesDB auto-initializes on the first Open.
func Init() error {
	result := C.tidesdb_init(nil, nil, nil, nil)
	if result != 0 {
		return fmt.Errorf("tidesdb_init failed: already initialized (code: %d)", result)
	}
	return nil
}

// Finalize finalizes TidesDB and resets the allocator.
// After calling this, Init can be called again.
func Finalize() {
	C.tidesdb_finalize()
}

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
	DBPath                             string
	NumFlushThreads                    int
	NumCompactionThreads               int
	LogLevel                           LogLevel
	BlockCacheSize                     uint64
	MaxOpenSSTables                    uint64
	MaxMemoryUsage                     uint64
	LogToFile                          bool
	LogTruncationAt                    uint64
	UnifiedMemtable                    bool
	UnifiedMemtableWriteBufferSize     uint64
	UnifiedMemtableSkipListMaxLevel    int
	UnifiedMemtableSkipListProbability float64
	UnifiedMemtableSyncMode            SyncMode
	UnifiedMemtableSyncInterval        uint64
	ObjectStore                        *ObjStore
	ObjectStoreConfig                  *ObjStoreConfig
}

// ColumnFamilyConfig is the configuration for a column family.
type ColumnFamilyConfig struct {
	Name                     string
	WriteBufferSize          uint64
	LevelSizeRatio           uint64
	MinLevels                int
	DividingLevelOffset      int
	KlogValueThreshold       uint64
	CompressionAlgorithm     CompressionAlgorithm
	EnableBloomFilter        bool
	BloomFPR                 float64
	EnableBlockIndexes       bool
	IndexSampleRatio         int
	BlockIndexPrefixLen      int
	SyncMode                 SyncMode
	SyncIntervalUs           uint64
	ComparatorName           string
	SkipListMaxLevel         int
	SkipListProbability      float32
	DefaultIsolationLevel    IsolationLevel
	MinDiskSpace             uint64
	L1FileCountTrigger       int
	L0QueueStallThreshold    int
	UseBtree                 int
	ObjectLazyCompaction     int
	ObjectPrefetchCompaction int
}

// Stats is statistics about a column family.
type Stats struct {
	NumLevels        int
	MemtableSize     uint64
	LevelSizes       []uint64
	LevelNumSSTables []int
	Config           *ColumnFamilyConfig
	TotalKeys        uint64
	TotalDataSize    uint64
	AvgKeySize       float64
	AvgValueSize     float64
	LevelKeyCounts   []uint64
	ReadAmp          float64
	HitRate          float64
	UseBtree         bool
	BtreeTotalNodes  uint64
	BtreeMaxHeight   uint32
	BtreeAvgHeight   float64
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

// DbStats is aggregate statistics across the entire database instance.
type DbStats struct {
	NumColumnFamilies       int
	TotalMemory             uint64
	AvailableMemory         uint64
	ResolvedMemoryLimit     uint64
	MemoryPressureLevel     int
	FlushPendingCount       int
	TotalMemtableBytes      int64
	TotalImmutableCount     int
	TotalSstableCount       int
	TotalDataSizeBytes      uint64
	NumOpenSstables         int
	GlobalSeq               uint64
	TxnMemoryBytes          int64
	CompactionQueueSize     uint64
	FlushQueueSize          uint64
	UnifiedMemtableEnabled  bool
	UnifiedMemtableBytes    int64
	UnifiedImmutableCount   int
	UnifiedIsFlushing       bool
	UnifiedNextCFIndex      uint32
	UnifiedWalGeneration    uint64
	ObjectStoreEnabled      bool
	ObjectStoreConnector    string
	LocalCacheBytesUsed     uint64
	LocalCacheBytesMax      uint64
	LocalCacheNumFiles      int
	LastUploadedGeneration  uint64
	UploadQueueDepth        uint64
	TotalUploads            uint64
	TotalUploadFailures     uint64
	ReplicaMode             bool
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
	case C.TDB_ERR_READONLY:
		errMsg = "database is read-only"
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
		DBPath:                             "",
		NumFlushThreads:                    2,
		NumCompactionThreads:               2,
		LogLevel:                           LogInfo,
		BlockCacheSize:                     64 * 1024 * 1024,
		MaxOpenSSTables:                    256,
		MaxMemoryUsage:                     0,
		LogToFile:                          false,
		LogTruncationAt:                    24 * (1024 * 1024),
		UnifiedMemtable:                    false,
		UnifiedMemtableWriteBufferSize:     0,
		UnifiedMemtableSkipListMaxLevel:    0,
		UnifiedMemtableSkipListProbability: 0,
		UnifiedMemtableSyncMode:            SyncNone,
		UnifiedMemtableSyncInterval:        0,
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
		CompressionAlgorithm:  CompressionAlgorithm(cConfig.compression_algorithm),
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
		L0QueueStallThreshold:    int(cConfig.l0_queue_stall_threshold),
		UseBtree:                 int(cConfig.use_btree),
		ObjectLazyCompaction:     int(cConfig.object_lazy_compaction),
		ObjectPrefetchCompaction: int(cConfig.object_prefetch_compaction),
	}
}

// boolToInt converts a bool to C.int (0 or 1).
func boolToInt(b bool) C.int {
	if b {
		return C.int(1)
	}
	return C.int(0)
}

// Open opens a TidesDB instance with the given configuration.
func Open(config Config) (*TidesDB, error) {
	cPath := C.CString(config.DBPath)
	defer C.free(unsafe.Pointer(cPath))

	cConfig := C.tidesdb_config_t{
		db_path:                                cPath,
		num_flush_threads:                      C.int(config.NumFlushThreads),
		num_compaction_threads:                 C.int(config.NumCompactionThreads),
		log_level:                              C.tidesdb_log_level_t(config.LogLevel),
		block_cache_size:                       C.size_t(config.BlockCacheSize),
		max_open_sstables:                      C.size_t(config.MaxOpenSSTables),
		max_memory_usage:                       C.size_t(config.MaxMemoryUsage),
		log_to_file:                            C.int(0),
		log_truncation_at:                      C.size_t(config.LogTruncationAt),
		unified_memtable:                       C.int(0),
		unified_memtable_write_buffer_size:     C.size_t(config.UnifiedMemtableWriteBufferSize),
		unified_memtable_skip_list_max_level:   C.int(config.UnifiedMemtableSkipListMaxLevel),
		unified_memtable_skip_list_probability: C.float(config.UnifiedMemtableSkipListProbability),
		unified_memtable_sync_mode:             C.int(config.UnifiedMemtableSyncMode),
		unified_memtable_sync_interval_us:      C.uint64_t(config.UnifiedMemtableSyncInterval),
	}

	if config.LogToFile {
		cConfig.log_to_file = C.int(1)
	}

	if config.UnifiedMemtable {
		cConfig.unified_memtable = C.int(1)
	}

	if config.ObjectStore != nil {
		cConfig.object_store = config.ObjectStore.store
	}

	var cObjStoreConfig C.tidesdb_objstore_config_t
	if config.ObjectStoreConfig != nil {
		osc := config.ObjectStoreConfig
		if osc.LocalCachePath != "" {
			cObjStoreConfig.local_cache_path = C.CString(osc.LocalCachePath)
			defer C.free(unsafe.Pointer(cObjStoreConfig.local_cache_path))
		}
		cObjStoreConfig.local_cache_max_bytes = C.size_t(osc.LocalCacheMaxBytes)
		cObjStoreConfig.cache_on_read = boolToInt(osc.CacheOnRead)
		cObjStoreConfig.cache_on_write = boolToInt(osc.CacheOnWrite)
		cObjStoreConfig.max_concurrent_uploads = C.int(osc.MaxConcurrentUploads)
		cObjStoreConfig.max_concurrent_downloads = C.int(osc.MaxConcurrentDownloads)
		cObjStoreConfig.multipart_threshold = C.size_t(osc.MultipartThreshold)
		cObjStoreConfig.multipart_part_size = C.size_t(osc.MultipartPartSize)
		cObjStoreConfig.sync_manifest_to_object = boolToInt(osc.SyncManifestToObject)
		cObjStoreConfig.replicate_wal = boolToInt(osc.ReplicateWal)
		cObjStoreConfig.wal_upload_sync = boolToInt(osc.WalUploadSync)
		cObjStoreConfig.wal_sync_threshold_bytes = C.size_t(osc.WalSyncThresholdBytes)
		cObjStoreConfig.wal_sync_on_commit = boolToInt(osc.WalSyncOnCommit)
		cObjStoreConfig.replica_mode = boolToInt(osc.ReplicaMode)
		cObjStoreConfig.replica_sync_interval_us = C.uint64_t(osc.ReplicaSyncIntervalUs)
		cObjStoreConfig.replica_replay_wal = boolToInt(osc.ReplicaReplayWal)
		cConfig.object_store_config = &cObjStoreConfig
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

// Backup creates an on-disk snapshot of an open database without blocking normal reads/writes.
// The dir parameter must be a non-existent directory or an empty directory.
func (db *TidesDB) Backup(dir string) error {
	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	result := C.tidesdb_backup(db.db, cDir)
	return errorFromCode(result, "failed to backup database")
}

// Checkpoint creates a lightweight, near-instant snapshot of an open database using hard links
// instead of copying SSTable data. The checkpointDir parameter must be a non-existent directory
// or an empty directory. The checkpoint can be opened as a normal TidesDB database.
func (db *TidesDB) Checkpoint(checkpointDir string) error {
	cDir := C.CString(checkpointDir)
	defer C.free(unsafe.Pointer(cDir))

	result := C.tidesdb_checkpoint(db.db, cDir)
	return errorFromCode(result, "failed to checkpoint database")
}

// CreateColumnFamily creates a new column family with the given configuration.
func (db *TidesDB) CreateColumnFamily(name string, config ColumnFamilyConfig) error {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var cConfig C.tidesdb_column_family_config_t

	// Copy name into the fixed-size array
	nameBytes := []byte(name)
	for i := 0; i < len(nameBytes) && i < 127; i++ {
		cConfig.name[i] = C.char(nameBytes[i])
	}

	cConfig = C.tidesdb_column_family_config_t{
		name:                     cConfig.name,
		write_buffer_size:        C.size_t(config.WriteBufferSize),
		level_size_ratio:         C.size_t(config.LevelSizeRatio),
		min_levels:               C.int(config.MinLevels),
		dividing_level_offset:    C.int(config.DividingLevelOffset),
		klog_value_threshold:     C.size_t(config.KlogValueThreshold),
		compression_algorithm:    C.compression_algorithm(config.CompressionAlgorithm),
		enable_bloom_filter:      C.int(0),
		bloom_fpr:                C.double(config.BloomFPR),
		enable_block_indexes:     C.int(0),
		index_sample_ratio:       C.int(config.IndexSampleRatio),
		block_index_prefix_len:   C.int(config.BlockIndexPrefixLen),
		sync_mode:                C.int(config.SyncMode),
		sync_interval_us:         C.uint64_t(config.SyncIntervalUs),
		skip_list_max_level:      C.int(config.SkipListMaxLevel),
		skip_list_probability:    C.float(config.SkipListProbability),
		default_isolation_level:  C.tidesdb_isolation_level_t(config.DefaultIsolationLevel),
		min_disk_space:           C.uint64_t(config.MinDiskSpace),
		l1_file_count_trigger:    C.int(config.L1FileCountTrigger),
		l0_queue_stall_threshold:     C.int(config.L0QueueStallThreshold),
		use_btree:                    C.int(config.UseBtree),
		object_lazy_compaction:       C.int(config.ObjectLazyCompaction),
		object_prefetch_compaction:   C.int(config.ObjectPrefetchCompaction),
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

// RenameColumnFamily atomically renames a column family and its underlying directory.
// Waits for any in-progress flush/compaction to complete before renaming.
func (db *TidesDB) RenameColumnFamily(oldName, newName string) error {
	cOldName := C.CString(oldName)
	defer C.free(unsafe.Pointer(cOldName))
	cNewName := C.CString(newName)
	defer C.free(unsafe.Pointer(cNewName))

	result := C.tidesdb_rename_column_family(db.db, cOldName, cNewName)
	return errorFromCode(result, "failed to rename column family")
}

// CloneColumnFamily creates a complete copy of an existing column family with a new name.
// The clone contains all the data from the source at the time of cloning.
// Both column families exist independently after cloning.
func (db *TidesDB) CloneColumnFamily(sourceName, destName string) error {
	cSourceName := C.CString(sourceName)
	defer C.free(unsafe.Pointer(cSourceName))
	cDestName := C.CString(destName)
	defer C.free(unsafe.Pointer(cDestName))

	result := C.tidesdb_clone_column_family(db.db, cSourceName, cDestName)
	return errorFromCode(result, "failed to clone column family")
}

// DeleteColumnFamily drops a column family by pointer.
// This is faster than DropColumnFamily when you already hold the ColumnFamily pointer,
// as it skips the internal name lookup.
func (db *TidesDB) DeleteColumnFamily(cf *ColumnFamily) error {
	result := C.tidesdb_delete_column_family(db.db, cf.cf)
	return errorFromCode(result, "failed to delete column family")
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
		NumLevels:       int(cStats.num_levels),
		MemtableSize:    uint64(cStats.memtable_size),
		TotalKeys:       uint64(cStats.total_keys),
		TotalDataSize:   uint64(cStats.total_data_size),
		AvgKeySize:      float64(cStats.avg_key_size),
		AvgValueSize:    float64(cStats.avg_value_size),
		ReadAmp:         float64(cStats.read_amp),
		HitRate:         float64(cStats.hit_rate),
		UseBtree:        cStats.use_btree != 0,
		BtreeTotalNodes: uint64(cStats.btree_total_nodes),
		BtreeMaxHeight:  uint32(cStats.btree_max_height),
		BtreeAvgHeight:  float64(cStats.btree_avg_height),
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

	if cStats.num_levels > 0 && cStats.level_key_counts != nil {
		levelKeyCounts := (*[1 << 30]C.uint64_t)(unsafe.Pointer(cStats.level_key_counts))[:cStats.num_levels:cStats.num_levels]
		stats.LevelKeyCounts = make([]uint64, cStats.num_levels)
		for i := 0; i < int(cStats.num_levels); i++ {
			stats.LevelKeyCounts[i] = uint64(levelKeyCounts[i])
		}
	}

	if cStats.config != nil {
		stats.Config = &ColumnFamilyConfig{
			WriteBufferSize:       uint64(cStats.config.write_buffer_size),
			LevelSizeRatio:        uint64(cStats.config.level_size_ratio),
			MinLevels:             int(cStats.config.min_levels),
			DividingLevelOffset:   int(cStats.config.dividing_level_offset),
			KlogValueThreshold:    uint64(cStats.config.klog_value_threshold),
			CompressionAlgorithm:  CompressionAlgorithm(cStats.config.compression_algorithm),
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
			L0QueueStallThreshold:    int(cStats.config.l0_queue_stall_threshold),
			UseBtree:                 int(cStats.config.use_btree),
			ObjectLazyCompaction:     int(cStats.config.object_lazy_compaction),
			ObjectPrefetchCompaction: int(cStats.config.object_prefetch_compaction),
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

// RangeCost estimates the computational cost of iterating between two keys in a column family.
// The returned value is an opaque double - meaningful only for comparison with other values
// from the same function. It uses only in-memory metadata and performs no disk I/O.
// Key order does not matter - the function normalizes the range internally.
func (cf *ColumnFamily) RangeCost(keyA, keyB []byte) (float64, error) {
	var cKeyA, cKeyB *C.uint8_t
	if len(keyA) > 0 {
		cKeyA = (*C.uint8_t)(unsafe.Pointer(&keyA[0]))
	}
	if len(keyB) > 0 {
		cKeyB = (*C.uint8_t)(unsafe.Pointer(&keyB[0]))
	}

	var cost C.double
	result := C.tidesdb_range_cost(cf.cf, cKeyA, C.size_t(len(keyA)), cKeyB, C.size_t(len(keyB)), &cost)
	if result != C.TDB_SUCCESS {
		return 0, errorFromCode(result, "failed to estimate range cost")
	}

	return float64(cost), nil
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

// IsFlushing checks if a column family has a flush operation in progress.
func (cf *ColumnFamily) IsFlushing() bool {
	return C.tidesdb_is_flushing(cf.cf) != 0
}

// IsCompacting checks if a column family has a compaction operation in progress.
func (cf *ColumnFamily) IsCompacting() bool {
	return C.tidesdb_is_compacting(cf.cf) != 0
}

// SyncWal forces an immediate fsync of the active write-ahead log for a column family.
// This is useful for explicit durability control when using SyncNone or SyncInterval modes.
func (cf *ColumnFamily) SyncWal() error {
	result := C.tidesdb_sync_wal(cf.cf)
	return errorFromCode(result, "failed to sync WAL")
}

// PurgeCF forces a synchronous flush and aggressive compaction for a single column family.
// Unlike FlushMemtable and Compact (which are non-blocking), PurgeCF blocks until all
// flush and compaction I/O is complete.
func (cf *ColumnFamily) PurgeCF() error {
	result := C.tidesdb_purge_cf(cf.cf)
	return errorFromCode(result, "failed to purge column family")
}

// Purge forces a synchronous flush and aggressive compaction for all column families,
// then drains both the global flush and compaction queues. This blocks until all work
// is complete.
func (db *TidesDB) Purge() error {
	result := C.tidesdb_purge(db.db)
	return errorFromCode(result, "failed to purge database")
}

// GetDbStats retrieves aggregate statistics across the entire database instance.
// Unlike GetStats (which heap-allocates), GetDbStats fills a caller-provided struct
// on the stack. No free is needed.
func (db *TidesDB) GetDbStats() (*DbStats, error) {
	var cStats C.tidesdb_db_stats_t
	result := C.tidesdb_get_db_stats(db.db, &cStats)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to get database stats")
	}

	return &DbStats{
		NumColumnFamilies:      int(cStats.num_column_families),
		TotalMemory:            uint64(cStats.total_memory),
		AvailableMemory:        uint64(cStats.available_memory),
		ResolvedMemoryLimit:    uint64(cStats.resolved_memory_limit),
		MemoryPressureLevel:    int(cStats.memory_pressure_level),
		FlushPendingCount:      int(cStats.flush_pending_count),
		TotalMemtableBytes:     int64(cStats.total_memtable_bytes),
		TotalImmutableCount:    int(cStats.total_immutable_count),
		TotalSstableCount:      int(cStats.total_sstable_count),
		TotalDataSizeBytes:     uint64(cStats.total_data_size_bytes),
		NumOpenSstables:        int(cStats.num_open_sstables),
		GlobalSeq:              uint64(cStats.global_seq),
		TxnMemoryBytes:         int64(cStats.txn_memory_bytes),
		CompactionQueueSize:    uint64(cStats.compaction_queue_size),
		FlushQueueSize:         uint64(cStats.flush_queue_size),
		UnifiedMemtableEnabled: cStats.unified_memtable_enabled != 0,
		UnifiedMemtableBytes:   int64(cStats.unified_memtable_bytes),
		UnifiedImmutableCount:  int(cStats.unified_immutable_count),
		UnifiedIsFlushing:      cStats.unified_is_flushing != 0,
		UnifiedNextCFIndex:     uint32(cStats.unified_next_cf_index),
		UnifiedWalGeneration:   uint64(cStats.unified_wal_generation),
		ObjectStoreEnabled: cStats.object_store_enabled != 0,
		ObjectStoreConnector: func() string {
			if cStats.object_store_connector != nil {
				return C.GoString(cStats.object_store_connector)
			}
			return ""
		}(),
		LocalCacheBytesUsed: uint64(cStats.local_cache_bytes_used),
		LocalCacheBytesMax:     uint64(cStats.local_cache_bytes_max),
		LocalCacheNumFiles:     int(cStats.local_cache_num_files),
		LastUploadedGeneration: uint64(cStats.last_uploaded_generation),
		UploadQueueDepth:       uint64(cStats.upload_queue_depth),
		TotalUploads:           uint64(cStats.total_uploads),
		TotalUploadFailures:    uint64(cStats.total_upload_failures),
		ReplicaMode:            cStats.replica_mode != 0,
	}, nil
}

// UpdateRuntimeConfig updates runtime-safe configuration settings for a column family.
// If persistToDisk is true, changes are saved to config.ini in the column family directory.
func (cf *ColumnFamily) UpdateRuntimeConfig(config ColumnFamilyConfig, persistToDisk bool) error {
	cConfig := C.tidesdb_column_family_config_t{
		write_buffer_size:        C.size_t(config.WriteBufferSize),
		level_size_ratio:         C.size_t(config.LevelSizeRatio),
		min_levels:               C.int(config.MinLevels),
		dividing_level_offset:    C.int(config.DividingLevelOffset),
		klog_value_threshold:     C.size_t(config.KlogValueThreshold),
		compression_algorithm:    C.compression_algorithm(config.CompressionAlgorithm),
		enable_bloom_filter:      C.int(0),
		bloom_fpr:                C.double(config.BloomFPR),
		enable_block_indexes:     C.int(0),
		index_sample_ratio:       C.int(config.IndexSampleRatio),
		block_index_prefix_len:   C.int(config.BlockIndexPrefixLen),
		sync_mode:                C.int(config.SyncMode),
		sync_interval_us:         C.uint64_t(config.SyncIntervalUs),
		skip_list_max_level:      C.int(config.SkipListMaxLevel),
		skip_list_probability:    C.float(config.SkipListProbability),
		default_isolation_level:  C.tidesdb_isolation_level_t(config.DefaultIsolationLevel),
		min_disk_space:           C.uint64_t(config.MinDiskSpace),
		l1_file_count_trigger:    C.int(config.L1FileCountTrigger),
		l0_queue_stall_threshold:     C.int(config.L0QueueStallThreshold),
		use_btree:                    C.int(config.UseBtree),
		object_lazy_compaction:       C.int(config.ObjectLazyCompaction),
		object_prefetch_compaction:   C.int(config.ObjectPrefetchCompaction),
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

	persist := C.int(0)
	if persistToDisk {
		persist = C.int(1)
	}

	result := C.tidesdb_cf_update_runtime_config(cf.cf, &cConfig, persist)
	return errorFromCode(result, "failed to update runtime config")
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

// GetComparator retrieves a registered comparator by name.
// Returns true if the comparator is registered, false otherwise.
func (db *TidesDB) GetComparator(name string) (bool, error) {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var fn C.tidesdb_comparator_fn
	var ctx unsafe.Pointer

	result := C.tidesdb_get_comparator(db.db, cName, &fn, &ctx)
	if result != C.TDB_SUCCESS {
		return false, errorFromCode(result, "failed to get comparator")
	}

	return true, nil
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

// SingleDelete writes a tombstone carrying a caller-provided promise that the key
// has been put at most once since its previous single-delete (or since the start
// of history). This lets compaction drop the put and tombstone together as soon
// as both appear in the same merge input, rather than carrying the tombstone
// forward to the largest active level. Read semantics match Delete.
//
// The engine does not verify the promise at runtime; violating it can leave
// older puts visible and is a bug in the caller. Use only for insert-once-then-
// delete patterns (classic insert benchmarks, secondary indexes on never-updated
// columns, log tables with scheduled purges). Not safe for repeated updates to
// the same key - when in doubt, prefer Delete.
func (txn *Transaction) SingleDelete(cf *ColumnFamily, key []byte) error {
	var cKey *C.uint8_t
	if len(key) > 0 {
		cKey = (*C.uint8_t)(unsafe.Pointer(&key[0]))
	}

	result := C.tidesdb_txn_single_delete(txn.txn, cf.cf, cKey, C.size_t(len(key)))
	return errorFromCode(result, "failed to single-delete key")
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

// Reset resets a committed or aborted transaction for reuse with a new isolation level.
// This avoids the overhead of freeing and reallocating transaction resources in hot loops.
// The transaction must be committed or aborted before reset; resetting an active transaction returns an error.
func (txn *Transaction) Reset(isolation IsolationLevel) error {
	result := C.tidesdb_txn_reset(txn.txn, C.tidesdb_isolation_level_t(isolation))
	return errorFromCode(result, "failed to reset transaction")
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

// KeyValue retrieves both the current key and value from the iterator in a single call.
// This is more efficient than calling Key() and Value() separately.
func (iter *Iterator) KeyValue() ([]byte, []byte, error) {
	var cKey *C.uint8_t
	var cKeySize C.size_t
	var cValue *C.uint8_t
	var cValueSize C.size_t

	result := C.tidesdb_iter_key_value(iter.iter, &cKey, &cKeySize, &cValue, &cValueSize)
	if result != C.TDB_SUCCESS {
		return nil, nil, errorFromCode(result, "failed to get key-value")
	}

	key := C.GoBytes(unsafe.Pointer(cKey), C.int(cKeySize))
	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	return key, value, nil
}

// Free frees the iterator resources.
func (iter *Iterator) Free() {
	if iter != nil && iter.iter != nil {
		C.tidesdb_iter_free(iter.iter)
		iter.iter = nil
	}
}

// PromoteToPrimary switches a read-only replica database to primary mode.
func (db *TidesDB) PromoteToPrimary() error {
	result := C.tidesdb_promote_to_primary(db.db)
	return errorFromCode(result, "failed to promote to primary")
}

// CfConfigLoadFromIni loads a column family configuration from an INI file.
func CfConfigLoadFromIni(iniFile, sectionName string) (*ColumnFamilyConfig, error) {
	cIniFile := C.CString(iniFile)
	defer C.free(unsafe.Pointer(cIniFile))
	cSectionName := C.CString(sectionName)
	defer C.free(unsafe.Pointer(cSectionName))

	var cConfig C.tidesdb_column_family_config_t
	result := C.tidesdb_cf_config_load_from_ini(cIniFile, cSectionName, &cConfig)
	if result != C.TDB_SUCCESS {
		return nil, errorFromCode(result, "failed to load config from INI")
	}

	return &ColumnFamilyConfig{
		Name:                     C.GoString(&cConfig.name[0]),
		WriteBufferSize:          uint64(cConfig.write_buffer_size),
		LevelSizeRatio:           uint64(cConfig.level_size_ratio),
		MinLevels:                int(cConfig.min_levels),
		DividingLevelOffset:      int(cConfig.dividing_level_offset),
		KlogValueThreshold:       uint64(cConfig.klog_value_threshold),
		CompressionAlgorithm:     CompressionAlgorithm(cConfig.compression_algorithm),
		EnableBloomFilter:        cConfig.enable_bloom_filter != 0,
		BloomFPR:                 float64(cConfig.bloom_fpr),
		EnableBlockIndexes:       cConfig.enable_block_indexes != 0,
		IndexSampleRatio:         int(cConfig.index_sample_ratio),
		BlockIndexPrefixLen:      int(cConfig.block_index_prefix_len),
		SyncMode:                 SyncMode(cConfig.sync_mode),
		SyncIntervalUs:           uint64(cConfig.sync_interval_us),
		ComparatorName:           C.GoString(&cConfig.comparator_name[0]),
		SkipListMaxLevel:         int(cConfig.skip_list_max_level),
		SkipListProbability:      float32(cConfig.skip_list_probability),
		DefaultIsolationLevel:    IsolationLevel(cConfig.default_isolation_level),
		MinDiskSpace:             uint64(cConfig.min_disk_space),
		L1FileCountTrigger:       int(cConfig.l1_file_count_trigger),
		L0QueueStallThreshold:    int(cConfig.l0_queue_stall_threshold),
		UseBtree:                 int(cConfig.use_btree),
		ObjectLazyCompaction:     int(cConfig.object_lazy_compaction),
		ObjectPrefetchCompaction: int(cConfig.object_prefetch_compaction),
	}, nil
}

// CfConfigSaveToIni saves a column family configuration to an INI file.
func CfConfigSaveToIni(iniFile, sectionName string, config ColumnFamilyConfig) error {
	cIniFile := C.CString(iniFile)
	defer C.free(unsafe.Pointer(cIniFile))
	cSectionName := C.CString(sectionName)
	defer C.free(unsafe.Pointer(cSectionName))

	cConfig := C.tidesdb_column_family_config_t{
		write_buffer_size:          C.size_t(config.WriteBufferSize),
		level_size_ratio:           C.size_t(config.LevelSizeRatio),
		min_levels:                 C.int(config.MinLevels),
		dividing_level_offset:      C.int(config.DividingLevelOffset),
		klog_value_threshold:       C.size_t(config.KlogValueThreshold),
		compression_algorithm:      C.compression_algorithm(config.CompressionAlgorithm),
		enable_bloom_filter:        boolToInt(config.EnableBloomFilter),
		bloom_fpr:                  C.double(config.BloomFPR),
		enable_block_indexes:       boolToInt(config.EnableBlockIndexes),
		index_sample_ratio:         C.int(config.IndexSampleRatio),
		block_index_prefix_len:     C.int(config.BlockIndexPrefixLen),
		sync_mode:                  C.int(config.SyncMode),
		sync_interval_us:           C.uint64_t(config.SyncIntervalUs),
		skip_list_max_level:        C.int(config.SkipListMaxLevel),
		skip_list_probability:      C.float(config.SkipListProbability),
		default_isolation_level:    C.tidesdb_isolation_level_t(config.DefaultIsolationLevel),
		min_disk_space:             C.uint64_t(config.MinDiskSpace),
		l1_file_count_trigger:      C.int(config.L1FileCountTrigger),
		l0_queue_stall_threshold:   C.int(config.L0QueueStallThreshold),
		use_btree:                  C.int(config.UseBtree),
		object_lazy_compaction:     C.int(config.ObjectLazyCompaction),
		object_prefetch_compaction: C.int(config.ObjectPrefetchCompaction),
	}

	if config.Name != "" {
		nameBytes := []byte(config.Name)
		for i := 0; i < len(nameBytes) && i < 127; i++ {
			cConfig.name[i] = C.char(nameBytes[i])
		}
	}

	if config.ComparatorName != "" {
		cCompName := C.CString(config.ComparatorName)
		defer C.free(unsafe.Pointer(cCompName))
		C.strncpy(&cConfig.comparator_name[0], cCompName, C.TDB_MAX_COMPARATOR_NAME-1)
		cConfig.comparator_name[C.TDB_MAX_COMPARATOR_NAME-1] = 0
	}

	result := C.tidesdb_cf_config_save_to_ini(cIniFile, cSectionName, &cConfig)
	return errorFromCode(result, "failed to save config to INI")
}
