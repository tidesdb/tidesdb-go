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

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOpenClose(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestCreateDropColumnFamily(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.MemtableFlushSize = 64 * 1024 * 1024
	cfConfig.CompressAlgo = TDB_COMPRESS_SNAPPY
	
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	
	err = db.DropColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to drop column family: %v", err)
	}
}

func TestListColumnFamilies(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	
	// Create multiple column families
	cfNames := []string{"cf1", "cf2", "cf3"}
	for _, name := range cfNames {
		err = db.CreateColumnFamily(name, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family %s: %v", name, err)
		}
	}
	
	// List column families
	list, err := db.ListColumnFamilies()
	if err != nil {
		t.Fatalf("Failed to list column families: %v", err)
	}
	
	if len(list) != len(cfNames) {
		t.Fatalf("Expected %d column families, got %d", len(cfNames), len(list))
	}
	
	// Verify all names are present
	nameMap := make(map[string]bool)
	for _, name := range list {
		nameMap[name] = true
	}
	
	for _, expectedName := range cfNames {
		if !nameMap[expectedName] {
			t.Fatalf("Expected column family %s not found in list", expectedName)
		}
	}
}

type TestStruct struct {
	Name string
	Age  int
}

func TestTransactionPutGetDelete(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	// Test Put
	s := &TestStruct{
		Name: "John Doe",
		Age:  30,
	}
	
	var buf bytes.Buffer
	err = gob.NewEncoder(&buf).Encode(s)
	if err != nil {
		t.Fatalf("Failed to encode struct: %v", err)
	}
	
	key := []byte("key")
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	err = txn.Put("test_cf", key, buf.Bytes(), -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Test Get
	readTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	
	gotValue, err := readTxn.Get("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	readTxn.Free()
	
	// Decode the value
	var ts TestStruct
	err = gob.NewDecoder(bytes.NewBuffer(gotValue)).Decode(&ts)
	if err != nil {
		t.Fatalf("Failed to decode value: %v", err)
	}
	
	if ts.Name != s.Name || ts.Age != s.Age {
		t.Fatalf("Expected value %v, got %v", s, ts)
	}
	
	// Test Delete
	deleteTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}
	
	err = deleteTxn.Delete("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	
	err = deleteTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
	deleteTxn.Free()
	
	// Verify deletion
	verifyTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()
	
	_, err = verifyTxn.Get("test_cf", key)
	if err == nil {
		t.Fatalf("Expected key to be deleted but it still exists")
	}
}

func TestTransactionWithTTL(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	key := []byte("temp_key")
	value := []byte("temp_value")
	
	// Set TTL to 2 seconds from now
	ttl := time.Now().Add(2 * time.Second).Unix()
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	err = txn.Put("test_cf", key, value, ttl)
	if err != nil {
		t.Fatalf("Failed to put key-value pair with TTL: %v", err)
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Verify key exists before expiration
	readTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	
	gotValue, err := readTxn.Get("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to get value before expiration: %v", err)
	}
	
	if !bytes.Equal(gotValue, value) {
		t.Fatalf("Expected value %s, got %s", value, gotValue)
	}
	readTxn.Free()
	
	// Wait for expiration
	time.Sleep(3 * time.Second)
	
	// Verify key is expired
	expiredTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction after expiration: %v", err)
	}
	defer expiredTxn.Free()
	
	_, err = expiredTxn.Get("test_cf", key)
	if err == nil {
		t.Fatalf("Expected key to be expired but it still exists")
	}
}

func TestMultiOperationTransaction(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Multiple operations in one transaction
	err = txn.Put("test_cf", []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	
	err = txn.Put("test_cf", []byte("key2"), []byte("value2"), -1)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	
	err = txn.Put("test_cf", []byte("key3"), []byte("value3"), -1)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Verify all keys exist
	verifyTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()
	
	for i := 1; i <= 3; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		
		value, err := verifyTxn.Get("test_cf", key)
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}
		
		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Expected value %s for key%d, got %s", expectedValue, i, value)
		}
	}
}

func TestTransactionRollback(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	key := []byte("rollback_key")
	value := []byte("rollback_value")
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	err = txn.Put("test_cf", key, value, -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}
	
	// Rollback instead of commit
	err = txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	txn.Free()
	
	// Verify key does not exist
	verifyTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()
	
	_, err = verifyTxn.Get("test_cf", key)
	if err == nil {
		t.Fatalf("Expected key to not exist after rollback, but it does")
	}
}

func TestIteratorForward(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	for k, v := range testData {
		err = txn.Put("test_cf", []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Create iterator
	iterTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer iterTxn.Free()
	
	iter, err := iterTxn.NewIterator("test_cf")
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()
	
	// Iterate forward
	iter.SeekToFirst()
	
	count := 0
	for iter.Valid() {
		key, err := iter.Key()
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
		
		value, err := iter.Value()
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		
		expectedValue, exists := testData[string(key)]
		if !exists {
			t.Fatalf("Unexpected key: %s", key)
		}
		
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}
		
		count++
		iter.Next()
	}
	
	if count != len(testData) {
		t.Fatalf("Expected to iterate over %d entries, got %d", len(testData), count)
	}
}

func TestIteratorBackward(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	for k, v := range testData {
		err = txn.Put("test_cf", []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Create iterator
	iterTxn, err := db.BeginReadTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer iterTxn.Free()
	
	iter, err := iterTxn.NewIterator("test_cf")
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()
	
	// Iterate backward
	iter.SeekToLast()
	
	count := 0
	for iter.Valid() {
		key, err := iter.Key()
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
		
		value, err := iter.Value()
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
		
		expectedValue, exists := testData[string(key)]
		if !exists {
			t.Fatalf("Unexpected key: %s", key)
		}
		
		if string(value) != expectedValue {
			t.Fatalf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}
		
		count++
		iter.Prev()
	}
	
	if count != len(testData) {
		t.Fatalf("Expected to iterate over %d entries, got %d", len(testData), count)
	}
}

func TestGetColumnFamilyStats(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	// Create column family with specific settings
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.MemtableFlushSize = 2 * 1024 * 1024 // 2MB
	cfConfig.MaxLevel = 12
	cfConfig.Compressed = true
	cfConfig.CompressAlgo = TDB_COMPRESS_SNAPPY
	cfConfig.BloomFilterFPRate = 0.01
	
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	// Add some data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		
		err = txn.Put("test_cf", key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}
	
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()
	
	// Get statistics
	stats, err := db.GetColumnFamilyStats("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family statistics: %v", err)
	}
	
	// Verify statistics
	if stats.Name != "test_cf" {
		t.Errorf("Expected column family name 'test_cf', got '%s'", stats.Name)
	}
	
	if stats.Config.MemtableFlushSize != cfConfig.MemtableFlushSize {
		t.Errorf("Expected flush threshold %d, got %d", cfConfig.MemtableFlushSize, stats.Config.MemtableFlushSize)
	}
	
	if stats.Config.MaxLevel != cfConfig.MaxLevel {
		t.Errorf("Expected max level %d, got %d", cfConfig.MaxLevel, stats.Config.MaxLevel)
	}
	
	if !stats.Config.Compressed {
		t.Errorf("Expected compressed to be true, got false")
	}
	
	if stats.Config.CompressAlgo != cfConfig.CompressAlgo {
		t.Errorf("Expected compression algorithm %d, got %d", cfConfig.CompressAlgo, stats.Config.CompressAlgo)
	}
	
	t.Logf("Column family stats:")
	t.Logf("  Name: %s", stats.Name)
	t.Logf("  Comparator: %s", stats.ComparatorName)
	t.Logf("  Number of SSTables: %d", stats.NumSSTables)
	t.Logf("  Total SSTable Size: %d bytes", stats.TotalSSTableSize)
	t.Logf("  Memtable Size: %d bytes", stats.MemtableSize)
	t.Logf("  Memtable Entries: %d", stats.MemtableEntries)
}

func TestCompaction(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	// Create column family with small flush threshold to force SSTables
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.MemtableFlushSize = 1024 // 1KB to force frequent flushes
	cfConfig.EnableBackgroundCompaction = false // Disable auto compaction for testing
	cfConfig.CompactionThreads = 2
	
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")
	
	// Add data to create multiple SSTables
	for batch := 0; batch < 5; batch++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}
		
		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("key%d_%d", batch, i))
			value := make([]byte, 512) // 512 bytes
			for j := range value {
				value[j] = byte(i % 256)
			}
			
			err = txn.Put("test_cf", key, value, -1)
			if err != nil {
				t.Fatalf("Failed to put key-value pair: %v", err)
			}
		}
		
		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		txn.Free()
	}
	
	// Get column family for compaction
	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}
	
	// Check stats before compaction
	statsBefore, err := db.GetColumnFamilyStats("test_cf")
	if err != nil {
		t.Fatalf("Failed to get stats before compaction: %v", err)
	}
	
	t.Logf("Before compaction: %d SSTables", statsBefore.NumSSTables)
	
	// Perform manual compaction (requires at least 2 SSTables)
	if statsBefore.NumSSTables >= 2 {
		err = cf.Compact()
		if err != nil {
			t.Logf("Compaction note: %v", err)
		}
		
		// Check stats after compaction
		statsAfter, err := db.GetColumnFamilyStats("test_cf")
		if err != nil {
			t.Fatalf("Failed to get stats after compaction: %v", err)
		}
		
		t.Logf("After compaction: %d SSTables", statsAfter.NumSSTables)
	} else {
		t.Logf("Skipping compaction test: need at least 2 SSTables, got %d", statsBefore.NumSSTables)
	}
}

func TestNonExistentColumnFamily(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	// Try to get stats for non-existent column family
	_, err = db.GetColumnFamilyStats("nonexistent_cf")
	if err == nil {
		t.Fatalf("Expected error when getting stats for non-existent column family")
	}
	
	if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "invalid") {
		t.Logf("Got error (acceptable): %v", err)
	}
	
	// Try to drop non-existent column family
	err = db.DropColumnFamily("nonexistent_cf")
	if err == nil {
		t.Fatalf("Expected error when dropping non-existent column family")
	}
}

func TestSyncModes(t *testing.T) {
	defer os.RemoveAll("testdb")
	
	config := Config{
		DBPath:             "testdb",
		EnableDebugLogging: false,
	}
	
	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	
	// Test each sync mode
	syncModes := []struct {
		name     string
		mode     TidesDBSyncMode
		interval int
	}{
		{"none", TDB_SYNC_NONE, 0},
		{"background", TDB_SYNC_BACKGROUND, 1000},
		{"full", TDB_SYNC_FULL, 0},
	}
	
	for _, sm := range syncModes {
		cfName := fmt.Sprintf("cf_%s", sm.name)
		
		cfConfig := DefaultColumnFamilyConfig()
		cfConfig.SyncMode = sm.mode
		cfConfig.SyncInterval = sm.interval
		
		err = db.CreateColumnFamily(cfName, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family with %s sync mode: %v", sm.name, err)
		}
		
		// Verify sync mode in stats
		stats, err := db.GetColumnFamilyStats(cfName)
		if err != nil {
			t.Fatalf("Failed to get stats for %s: %v", cfName, err)
		}
		
		if stats.Config.SyncMode != sm.mode {
			t.Errorf("Expected sync mode %d, got %d", sm.mode, stats.Config.SyncMode)
		}
		
		t.Logf("Created column family '%s' with sync mode: %s", cfName, sm.name)
	}
}