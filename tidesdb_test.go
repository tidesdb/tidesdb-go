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
)

func TestOpenClose(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestCreateDropColumnFamily(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	err = db.DropColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to drop column family: %v", err)
	}
}

type TestStruct struct {
	Name string
	Age  int
}

func TestPutGetDelete(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer func(db *TidesDB, name string) {
		err := db.DropColumnFamily(name)
		if err != nil {

		}
	}(db, "test_cf")

	s := &TestStruct{
		Name: "John Doe",
		Age:  30,
	}

	b := make([]byte, 0)
	buff := bytes.NewBuffer(b)

	err = gob.NewEncoder(buff).Encode(s)
	if err != nil {
		return
	}

	key := []byte("key")

	err = db.Put("test_cf", key, buff.Bytes(), -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	gotValue, err := db.Get("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	// decode the value
	var ts TestStruct
	err = gob.NewDecoder(bytes.NewBuffer(gotValue)).Decode(&ts)

	if ts.Name != s.Name || ts.Age != s.Age {
		t.Fatalf("Expected value %v, got %v", s, ts)

	}

	err = db.Delete("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
}

func TestCompactSSTables(t *testing.T) {
	// @TODO
}

func TestTransaction(t *testing.T) {
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer func(db *TidesDB, name string) {
		err := db.DropColumnFamily(name)
		if err != nil {
			t.Fatalf("Failed to drop column family: %v", err)
		}
	}(db, "test_cf")

	txn, err := db.BeginTxn("test_cf")
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn.Free()

	key := []byte("key")
	value := []byte("value")

	err = txn.Put(key, value, -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair in transaction: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Check if the key-value pair was added to the database
	gotValue, err := db.Get("test_cf", key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if string(gotValue) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, gotValue)
	}
}

func TestRange(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")

	// Insert several key-value pairs with clear min/max boundaries
	pairs := map[string]string{
		"a_key1": "value1",
		"b_key2": "value2",
		"c_key3": "value3",
		"d_key4": "value4",
		"e_key5": "value5",
	}

	// Insert in sorted order to ensure predictable range results
	keys := []string{"a_key1", "b_key2", "c_key3", "d_key4", "e_key5"}
	for _, key := range keys {
		err = db.Put("test_cf", []byte(key), []byte(pairs[key]), -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Test specific range query (b_key2 to d_key4)
	rangePairs, err := db.Range("test_cf", []byte("b_key2"), []byte("d_key4"))
	if err != nil {
		t.Fatalf("Failed to get range: %v", err)
	}

	// We expect b_key2, c_key3, d_key4 to be returned
	expectedCount := 3
	if len(rangePairs) != expectedCount {
		t.Fatalf("Expected %d pairs, got %d", expectedCount, len(rangePairs))
	}

	// Verify the returned key-value pairs
	for i, pair := range rangePairs {
		key := string(pair[0])
		value := string(pair[1])
		expectedKey := keys[i+1] // Start from b_key2
		expectedValue := pairs[expectedKey]

		if key != expectedKey || value != expectedValue {
			t.Fatalf("Expected (%s, %s), got (%s, %s)", expectedKey, expectedValue, key, value)
		}
	}

	// Test range query for a prefix (all keys starting with "c_")
	rangePairs, err = db.Range("test_cf", []byte("c_"), []byte("c`")) // '`' is just after '_' in ASCII
	if err != nil {
		t.Fatalf("Failed to get prefix range: %v", err)
	}

	// We expect only c_key3 to be returned
	expectedCount = 1
	if len(rangePairs) != expectedCount {
		t.Fatalf("Expected %d pairs for prefix query, got %d", expectedCount, len(rangePairs))
	}

	// Test range query with minimum key to a specific key
	rangePairs, err = db.Range("test_cf", []byte("a_key1"), []byte("c_key3"))
	if err != nil {
		t.Fatalf("Failed to get range from minimum: %v", err)
	}

	// We expect a_key1, b_key2, c_key3 to be returned
	expectedCount = 3
	if len(rangePairs) != expectedCount {
		t.Fatalf("Expected %d pairs for min range, got %d", expectedCount, len(rangePairs))
	}

	// Test range query with a specific key to maximum key
	rangePairs, err = db.Range("test_cf", []byte("d_key4"), []byte("e_key5"))
	if err != nil {
		t.Fatalf("Failed to get range to maximum: %v", err)
	}

	// We expect d_key4, e_key5 to be returned
	expectedCount = 2
	if len(rangePairs) != expectedCount {
		t.Fatalf("Expected %d pairs for max range, got %d", expectedCount, len(rangePairs))
	}

	// Test full range query (first to last key)
	rangePairs, err = db.Range("test_cf", []byte("a_key1"), []byte("e_key5"))
	if err != nil {
		t.Fatalf("Failed to get full range: %v", err)
	}

	// We expect all 5 keys to be returned
	expectedCount = 5
	if len(rangePairs) != expectedCount {
		t.Fatalf("Expected %d pairs for full range, got %d", expectedCount, len(rangePairs))
	}
}

func TestDeleteByRange(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")

	// Insert several key-value pairs with clear min/max boundaries
	pairs := map[string]string{
		"a_key1": "value1",
		"b_key2": "value2",
		"c_key3": "value3",
		"d_key4": "value4",
		"e_key5": "value5",
	}

	// Insert in sorted order
	keys := []string{"a_key1", "b_key2", "c_key3", "d_key4", "e_key5"}
	for _, key := range keys {
		err = db.Put("test_cf", []byte(key), []byte(pairs[key]), -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Delete a range of keys (b_key2 to d_key4)
	err = db.DeleteByRange("test_cf", []byte("b_key2"), []byte("d_key4"))
	if err != nil {
		t.Fatalf("Failed to delete range: %v", err)
	}

	// Verify that the keys in the range were deleted
	for _, key := range []string{"b_key2", "c_key3", "d_key4"} {
		_, err := db.Get("test_cf", []byte(key))
		if err == nil {
			t.Fatalf("Key %s should be deleted but still exists", key)
		}

		if !strings.Contains(err.Error(), "not found") {
			t.Fatalf("Unexpected error when getting deleted key: %v", err)
		}
	}

	// Verify that keys outside the range still exist
	for _, key := range []string{"a_key1", "e_key5"} {
		val, err := db.Get("test_cf", []byte(key))
		if err != nil {
			t.Fatalf("Key %s should exist but got error: %v", key, err)
		}
		if string(val) != pairs[key] {
			t.Fatalf("Expected value %s for key %s, got %s", pairs[key], key, string(val))
		}
	}

	// Test deleting a non-existent range (should succeed without error)
	err = db.DeleteByRange("test_cf", []byte("x_key"), []byte("z_key"))
	if err != nil {
		t.Fatalf("DeleteByRange for non-existent range should succeed, got error: %v", err)
	}

	// Test deleting a range in a non-existent column family (should fail)
	err = db.DeleteByRange("nonexistent_cf", []byte("a_key"), []byte("b_key"))
	if err == nil {
		t.Fatalf("DeleteByRange in non-existent column family should fail")
	}
}

func TestGetColumnFamilyStat(t *testing.T) {
	defer os.RemoveAll("testdb")
	db, err := Open("testdb")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a column family with specific settings
	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	defer db.DropColumnFamily("test_cf")

	// Add some data to create SSTables
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = db.Put("test_cf", key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	// Get column family statistics
	stat, err := db.GetColumnFamilyStat("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family statistics: %v", err)
	}

	// Verify column family config
	if stat.Config.Name != "test_cf" {
		t.Errorf("Expected column family name 'test_cf', got '%s'", stat.Config.Name)
	}
	if stat.Config.FlushThreshold != 1024*1024*64 {
		t.Errorf("Expected flush threshold %d, got %d", 1024*1024*64, stat.Config.FlushThreshold)
	}
	if stat.Config.MaxLevel != 12 {
		t.Errorf("Expected max level 12, got %d", stat.Config.MaxLevel)
	}
	if !stat.Config.Compressed {
		t.Errorf("Expected compressed to be true, got false")
	}
	if stat.Config.CompressAlgo != TDB_COMPRESS_SNAPPY {
		t.Errorf("Expected compression algorithm %d, got %d", TDB_COMPRESS_SNAPPY, stat.Config.CompressAlgo)
	}
	if !stat.Config.BloomFilter {
		t.Errorf("Expected bloom filter to be true, got false")
	}

	// Verify column family name
	if stat.Name != "test_cf" {
		t.Errorf("Expected column family name 'test_cf', got '%s'", stat.Name)
	}

	// Verify memtable stats (values will vary, just check existence)
	if stat.MemtableSize <= 0 {
		t.Logf("Memtable size: %d", stat.MemtableSize)
	}
	if stat.MemtableEntryCount <= 0 {
		t.Logf("Memtable entries count: %d", stat.MemtableEntryCount)
	}

	// Check if there are SSTables (may not be any if memtable hasn't been flushed)
	t.Logf("Number of SSTables: %d", stat.NumSSTables)

	// Force a flush to create SSTables (if available in the API)
	// This is optional and depends on your implementation

	// If there are SSTables, verify their stats
	if stat.NumSSTables > 0 {
		for i, sstStat := range stat.SSTableStats {
			if sstStat.Path == "" {
				t.Errorf("SSTable %d has empty path", i)
			}
			if sstStat.Size <= 0 {
				t.Errorf("SSTable %d has invalid size: %d", i, sstStat.Size)
			}
			if sstStat.NumBlocks <= 0 {
				t.Errorf("SSTable %d has invalid block count: %d", i, sstStat.NumBlocks)
			}
			t.Logf("SSTable %d: Path=%s, Size=%d, NumBlocks=%d",
				i, sstStat.Path, sstStat.Size, sstStat.NumBlocks)
		}
	}

	// Test non-existent column family
	_, err = db.GetColumnFamilyStat("nonexistent_cf")
	if err == nil {
		t.Fatalf("Expected error when getting stats for non-existent column family")
	}
}

// More tests to be added...
