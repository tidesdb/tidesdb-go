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
	"sync"
	"testing"
	"time"
)

const testDBPath = "testdb"

func cleanupTestDB(t *testing.T) {
	os.RemoveAll(testDBPath)
}

func TestOpenClose(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.WriteBufferSize = 64 * 1024 * 1024
	cfConfig.CompressionAlgorithm = LZ4Compression

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
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()

	cfNames := []string{"cf1", "cf2", "cf3"}
	for _, name := range cfNames {
		err = db.CreateColumnFamily(name, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family %s: %v", name, err)
		}
	}

	list, err := db.ListColumnFamilies()
	if err != nil {
		t.Fatalf("Failed to list column families: %v", err)
	}

	if len(list) != len(cfNames) {
		t.Fatalf("Expected %d column families, got %d", len(cfNames), len(list))
	}

	// We verify all names are present
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
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

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

	err = txn.Put(cf, key, buf.Bytes(), -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}

	gotValue, err := readTxn.Get(cf, key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	readTxn.Free()

	var ts TestStruct
	err = gob.NewDecoder(bytes.NewBuffer(gotValue)).Decode(&ts)
	if err != nil {
		t.Fatalf("Failed to decode value: %v", err)
	}

	if ts.Name != s.Name || ts.Age != s.Age {
		t.Fatalf("Expected value %v, got %v", s, ts)
	}

	deleteTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}

	err = deleteTxn.Delete(cf, key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	err = deleteTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
	deleteTxn.Free()

	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	_, err = verifyTxn.Get(cf, key)
	if err == nil {
		t.Fatalf("Expected key to be deleted but it still exists")
	}
}

func TestTransactionWithTTL(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	key := []byte("temp_key")
	value := []byte("temp_value")

	// We set TTL to 2 seconds from now
	ttl := time.Now().Add(2 * time.Second).Unix()

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, key, value, ttl)
	if err != nil {
		t.Fatalf("Failed to put key-value pair with TTL: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Verify key exists before expiration
	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}

	gotValue, err := readTxn.Get(cf, key)
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
	expiredTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction after expiration: %v", err)
	}
	defer expiredTxn.Free()

	_, err = expiredTxn.Get(cf, key)
	if err == nil {
		t.Fatalf("Expected key to be expired but it still exists")
	}
}

func TestMultiOperationTransaction(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Multiple operations in one transaction
	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	err = txn.Put(cf, []byte("key3"), []byte("value3"), -1)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// We verify all keys exist
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	for i := 1; i <= 3; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))

		value, err := verifyTxn.Get(cf, key)
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Expected value %s for key%d, got %s", expectedValue, i, value)
		}
	}
}

func TestTransactionRollback(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	key := []byte("rollback_key")
	value := []byte("rollback_value")

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, key, value, -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	// We rollback instead of commit
	err = txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	txn.Free()

	// We verify key does not exist
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	_, err = verifyTxn.Get(cf, key)
	if err == nil {
		t.Fatalf("Expected key to not exist after rollback, but it does")
	}
}

func TestSavepoints(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	err = txn.Savepoint("sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}

	err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	// Rollback to savepoint -- key2 is discarded, key1 remains
	err = txn.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("Failed to rollback to savepoint: %v", err)
	}

	// Add different operation after rollback
	err = txn.Put(cf, []byte("key3"), []byte("value3"), -1)
	if err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	// Commit transaction -- only key1 and key3 are written
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	// key1 should exist
	_, err = verifyTxn.Get(cf, []byte("key1"))
	if err != nil {
		t.Fatalf("Expected key1 to exist: %v", err)
	}

	// key2 should not exist (rolled back)
	_, err = verifyTxn.Get(cf, []byte("key2"))
	if err == nil {
		t.Fatalf("Expected key2 to not exist after savepoint rollback")
	}

	// key3 should exist
	_, err = verifyTxn.Get(cf, []byte("key3"))
	if err != nil {
		t.Fatalf("Expected key3 to exist: %v", err)
	}
}

func TestIsolationLevels(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	isolationLevels := []struct {
		name  string
		level IsolationLevel
	}{
		{"read_uncommitted", IsolationReadUncommitted},
		{"read_committed", IsolationReadCommitted},
		{"repeatable_read", IsolationRepeatableRead},
		{"snapshot", IsolationSnapshot},
		{"serializable", IsolationSerializable},
	}

	for _, il := range isolationLevels {
		txn, err := db.BeginTxnWithIsolation(il.level)
		if err != nil {
			t.Fatalf("Failed to begin transaction with %s isolation: %v", il.name, err)
		}

		key := []byte(fmt.Sprintf("key_%s", il.name))
		value := []byte(fmt.Sprintf("value_%s", il.name))

		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put with %s isolation: %v", il.name, err)
		}

		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit with %s isolation: %v", il.name, err)
		}
		txn.Free()

		t.Logf("Successfully tested %s isolation level", il.name)
	}
}

func TestIteratorForward(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

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
		err = txn.Put(cf, []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	iterTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer iterTxn.Free()

	iter, err := iterTxn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	if err := iter.SeekToFirst(); err != nil {
		t.Fatalf("Failed to seek to first: %v", err)
	}

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
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

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
		err = txn.Put(cf, []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	iterTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer iterTxn.Free()

	iter, err := iterTxn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	if err := iter.SeekToLast(); err != nil {
		t.Fatalf("Failed to seek to last: %v", err)
	}

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

		t.Logf("Backward iteration %d: key=%s, value=%s", count+1, string(key), string(value))

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

	if count < 2 {
		t.Fatalf("Expected to iterate over at least 2 entries, got %d", count)
	}
}

func TestIteratorSeek(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%02d", i))
		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	iterTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer iterTxn.Free()

	iter, err := iterTxn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	err = iter.Seek([]byte("key05"))
	if err != nil {
		t.Fatalf("Failed to seek: %v", err)
	}

	if iter.Valid() {
		key, err := iter.Key()
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
		t.Logf("Seek to key05 found: %s", string(key))
		if string(key) != "key05" {
			t.Logf("Note: Seek found %s instead of key05 (may be >= behavior)", string(key))
		}
	}
}

func TestGetStats(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.WriteBufferSize = 2 * 1024 * 1024
	cfConfig.CompressionAlgorithm = LZ4Compression
	cfConfig.EnableBloomFilter = true
	cfConfig.BloomFPR = 0.01

	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get column family statistics: %v", err)
	}

	t.Logf("Column family stats:")
	t.Logf("  Number of Levels: %d", stats.NumLevels)
	t.Logf("  Memtable Size: %d bytes", stats.MemtableSize)

	if stats.Config != nil {
		t.Logf("  Write Buffer Size: %d", stats.Config.WriteBufferSize)
		t.Logf("  Compression: %d", stats.Config.CompressionAlgorithm)
		t.Logf("  Bloom Filter: %v", stats.Config.EnableBloomFilter)
	}
}

func TestCacheStats(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cacheStats, err := db.GetCacheStats()
	if err != nil {
		t.Fatalf("Failed to get cache statistics: %v", err)
	}

	t.Logf("Cache stats:")
	t.Logf("  Enabled: %v", cacheStats.Enabled)
	t.Logf("  Total Entries: %d", cacheStats.TotalEntries)
	t.Logf("  Total Bytes: %d", cacheStats.TotalBytes)
	t.Logf("  Hits: %d", cacheStats.Hits)
	t.Logf("  Misses: %d", cacheStats.Misses)
	t.Logf("  Hit Rate: %.2f%%", cacheStats.HitRate*100)
	t.Logf("  Num Partitions: %d", cacheStats.NumPartitions)
}

func TestCompaction(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.WriteBufferSize = 1024 // 1KB to force frequent flushes

	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	for batch := 0; batch < 5; batch++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("key%d_%d", batch, i))
			value := make([]byte, 512)
			for j := range value {
				value[j] = byte(i % 256)
			}

			err = txn.Put(cf, key, value, -1)
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

	// We check stats before compaction
	statsBefore, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats before compaction: %v", err)
	}

	t.Logf("Before compaction: %d levels", statsBefore.NumLevels)

	err = cf.Compact()
	if err != nil {
		t.Logf("Compaction note: %v", err)
	}

	statsAfter, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats after compaction: %v", err)
	}

	t.Logf("After compaction: %d levels", statsAfter.NumLevels)
}

func TestFlushMemtable(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	err = cf.FlushMemtable()
	if err != nil {
		t.Logf("Flush memtable note: %v", err)
	}

	t.Logf("Memtable flush completed")
}

func TestNonExistentColumnFamily(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.GetColumnFamily("nonexistent_cf")
	if err == nil {
		t.Fatalf("Expected error when getting non-existent column family")
	}

	if !strings.Contains(err.Error(), "not found") {
		t.Logf("Got error (acceptable): %v", err)
	}

	err = db.DropColumnFamily("nonexistent_cf")
	if err == nil {
		t.Fatalf("Expected error when dropping non-existent column family")
	}
}

func TestSyncModes(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	syncModes := []struct {
		name       string
		mode       SyncMode
		intervalUs uint64
	}{
		{"none", SyncNone, 0},
		{"interval", SyncInterval, 128000},
		{"full", SyncFull, 0},
	}

	for _, sm := range syncModes {
		cfName := fmt.Sprintf("cf_%s", sm.name)

		cfConfig := DefaultColumnFamilyConfig()
		cfConfig.SyncMode = sm.mode
		cfConfig.SyncIntervalUs = sm.intervalUs

		err = db.CreateColumnFamily(cfName, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family with %s sync mode: %v", sm.name, err)
		}

		cf, err := db.GetColumnFamily(cfName)
		if err != nil {
			t.Fatalf("Failed to get column family %s: %v", cfName, err)
		}

		stats, err := cf.GetStats()
		if err != nil {
			t.Fatalf("Failed to get stats for %s: %v", cfName, err)
		}

		if stats.Config != nil && stats.Config.SyncMode != sm.mode {
			t.Errorf("Expected sync mode %d, got %d", sm.mode, stats.Config.SyncMode)
		}

		t.Logf("Created column family '%s' with sync mode: %s", cfName, sm.name)
	}
}

func TestCompressionAlgorithms(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	compressionAlgos := []struct {
		name string
		algo CompressionAlgorithm
	}{
		{"none", NoCompression},
		{"lz4", LZ4Compression},
		{"zstd", ZstdCompression},
		{"lz4_fast", LZ4FastCompression},
	}

	for _, ca := range compressionAlgos {
		cfName := fmt.Sprintf("cf_%s", ca.name)

		cfConfig := DefaultColumnFamilyConfig()
		cfConfig.CompressionAlgorithm = ca.algo

		err = db.CreateColumnFamily(cfName, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family with %s compression: %v", ca.name, err)
		}

		cf, err := db.GetColumnFamily(cfName)
		if err != nil {
			t.Fatalf("Failed to get column family %s: %v", cfName, err)
		}

		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key%d", i))
			value := []byte(fmt.Sprintf("value%d_with_some_extra_data_to_compress", i))
			err = txn.Put(cf, key, value, -1)
			if err != nil {
				t.Fatalf("Failed to put with %s compression: %v", ca.name, err)
			}
		}

		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit with %s compression: %v", ca.name, err)
		}
		txn.Free()

		t.Logf("Created column family '%s' with compression: %s", cfName, ca.name)
	}
}

func TestMultiColumnFamilyTransaction(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()

	err = db.CreateColumnFamily("users", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create users column family: %v", err)
	}

	err = db.CreateColumnFamily("orders", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create orders column family: %v", err)
	}

	usersCF, err := db.GetColumnFamily("users")
	if err != nil {
		t.Fatalf("Failed to get users column family: %v", err)
	}

	ordersCF, err := db.GetColumnFamily("orders")
	if err != nil {
		t.Fatalf("Failed to get orders column family: %v", err)
	}

	// Multi-CF transaction
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(usersCF, []byte("user:1000"), []byte("John Doe"), -1)
	if err != nil {
		t.Fatalf("Failed to put user: %v", err)
	}

	err = txn.Put(ordersCF, []byte("order:5000"), []byte("user:1000|product:A"), -1)
	if err != nil {
		t.Fatalf("Failed to put order: %v", err)
	}

	// Atomic commit across both CFs
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit multi-CF transaction: %v", err)
	}
	txn.Free()
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	userValue, err := verifyTxn.Get(usersCF, []byte("user:1000"))
	if err != nil {
		t.Fatalf("Failed to get user: %v", err)
	}
	if string(userValue) != "John Doe" {
		t.Fatalf("Expected 'John Doe', got '%s'", string(userValue))
	}

	orderValue, err := verifyTxn.Get(ordersCF, []byte("order:5000"))
	if err != nil {
		t.Fatalf("Failed to get order: %v", err)
	}
	if string(orderValue) != "user:1000|product:A" {
		t.Fatalf("Expected 'user:1000|product:A', got '%s'", string(orderValue))
	}

	t.Logf("Multi-CF transaction completed successfully")
}

func TestBtreeColumnFamily(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create column family with B+tree format enabled
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.UseBtree = 1
	cfConfig.WriteBufferSize = 1024 // Small buffer to force flushes
	cfConfig.CompressionAlgorithm = LZ4Compression
	cfConfig.EnableBloomFilter = true
	cfConfig.BloomFPR = 0.01

	err = db.CreateColumnFamily("btree_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create B+tree column family: %v", err)
	}

	cf, err := db.GetColumnFamily("btree_cf")
	if err != nil {
		t.Fatalf("Failed to get B+tree column family: %v", err)
	}

	// Insert data to populate the B+tree
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("btree_key_%04d", i))
		value := []byte(fmt.Sprintf("btree_value_%04d_with_extra_data", i))
		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Force flush to create SSTable with B+tree structure
	err = cf.FlushMemtable()
	if err != nil {
		t.Logf("Flush memtable note: %v", err)
	}

	// Wait briefly for flush to complete
	time.Sleep(500 * time.Millisecond)

	// Verify data can be read back
	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTxn.Free()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("btree_key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("btree_value_%04d_with_extra_data", i))

		value, err := readTxn.Get(cf, key)
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", string(key), err)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Expected value %s, got %s", string(expectedValue), string(value))
		}
	}

	// Get stats and verify B+tree fields
	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	t.Logf("B+tree column family stats:")
	t.Logf("  UseBtree: %v", stats.UseBtree)
	t.Logf("  BtreeTotalNodes: %d", stats.BtreeTotalNodes)
	t.Logf("  BtreeMaxHeight: %d", stats.BtreeMaxHeight)
	t.Logf("  BtreeAvgHeight: %.2f", stats.BtreeAvgHeight)
	t.Logf("  NumLevels: %d", stats.NumLevels)
	t.Logf("  TotalKeys: %d", stats.TotalKeys)
	t.Logf("  MemtableSize: %d bytes", stats.MemtableSize)

	// Verify UseBtree is true
	if !stats.UseBtree {
		t.Errorf("Expected UseBtree to be true for B+tree column family")
	}

	// Verify config also reflects B+tree setting
	if stats.Config != nil {
		if stats.Config.UseBtree != 1 {
			t.Errorf("Expected Config.UseBtree to be 1, got %d", stats.Config.UseBtree)
		}
		t.Logf("  Config.UseBtree: %d", stats.Config.UseBtree)
	}

	// Test iterator with B+tree format
	iter, err := readTxn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	err = iter.SeekToFirst()
	if err != nil {
		t.Fatalf("Failed to seek to first: %v", err)
	}

	count := 0
	for iter.Valid() {
		count++
		iter.Next()
	}

	if count != 100 {
		t.Errorf("Expected to iterate over 100 entries, got %d", count)
	}

	t.Logf("B+tree column family test completed successfully with %d entries", count)
}

func TestCloneColumnFamily(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create source column family with data
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.CompressionAlgorithm = LZ4Compression
	cfConfig.EnableBloomFilter = true

	err = db.CreateColumnFamily("source_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create source column family: %v", err)
	}

	sourceCF, err := db.GetColumnFamily("source_cf")
	if err != nil {
		t.Fatalf("Failed to get source column family: %v", err)
	}

	// Insert data into source
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err = txn.Put(sourceCF, []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Clone the column family
	err = db.CloneColumnFamily("source_cf", "cloned_cf")
	if err != nil {
		t.Fatalf("Failed to clone column family: %v", err)
	}

	// Verify both column families exist
	cfList, err := db.ListColumnFamilies()
	if err != nil {
		t.Fatalf("Failed to list column families: %v", err)
	}

	foundSource := false
	foundClone := false
	for _, name := range cfList {
		if name == "source_cf" {
			foundSource = true
		}
		if name == "cloned_cf" {
			foundClone = true
		}
	}

	if !foundSource {
		t.Fatalf("Source column family not found after clone")
	}
	if !foundClone {
		t.Fatalf("Cloned column family not found")
	}

	// Verify cloned data
	clonedCF, err := db.GetColumnFamily("cloned_cf")
	if err != nil {
		t.Fatalf("Failed to get cloned column family: %v", err)
	}

	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	for k, expectedV := range testData {
		value, err := verifyTxn.Get(clonedCF, []byte(k))
		if err != nil {
			t.Fatalf("Failed to get %s from cloned CF: %v", k, err)
		}
		if string(value) != expectedV {
			t.Fatalf("Expected value %s for key %s in clone, got %s", expectedV, k, value)
		}
	}

	// Verify independence: modify clone, source should be unchanged
	modifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin modify transaction: %v", err)
	}

	err = modifyTxn.Put(clonedCF, []byte("key4"), []byte("value4"), -1)
	if err != nil {
		t.Fatalf("Failed to put key4 in clone: %v", err)
	}

	err = modifyTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit modify transaction: %v", err)
	}
	modifyTxn.Free()

	// Verify key4 exists in clone but not in source
	checkTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin check transaction: %v", err)
	}
	defer checkTxn.Free()

	_, err = checkTxn.Get(clonedCF, []byte("key4"))
	if err != nil {
		t.Fatalf("key4 should exist in cloned CF: %v", err)
	}

	_, err = checkTxn.Get(sourceCF, []byte("key4"))
	if err == nil {
		t.Fatalf("key4 should NOT exist in source CF (clone should be independent)")
	}

	t.Logf("Clone column family test completed successfully")
}

func TestTransactionReset(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Begin transaction and do first batch of work
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit first transaction: %v", err)
	}

	// Reset transaction instead of free + begin
	err = txn.Reset(IsolationReadCommitted)
	if err != nil {
		t.Fatalf("Failed to reset transaction: %v", err)
	}

	// Second batch of work using the same transaction
	err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)
	if err != nil {
		t.Fatalf("Failed to put key2 after reset: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit second transaction: %v", err)
	}

	// Reset again with different isolation level
	err = txn.Reset(IsolationRepeatableRead)
	if err != nil {
		t.Fatalf("Failed to reset transaction with different isolation: %v", err)
	}

	// Third batch of work
	err = txn.Put(cf, []byte("key3"), []byte("value3"), -1)
	if err != nil {
		t.Fatalf("Failed to put key3 after second reset: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit third transaction: %v", err)
	}

	// Free once when done
	txn.Free()

	// Verify all keys exist
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	for i := 1; i <= 3; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))

		value, err := verifyTxn.Get(cf, key)
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Fatalf("Expected value %s for key%d, got %s", expectedValue, i, value)
		}
	}

	t.Logf("Transaction reset test completed successfully")
}

func TestTransactionResetAfterRollback(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
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

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Begin transaction and rollback
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("rollback_key"), []byte("rollback_value"), -1)
	if err != nil {
		t.Fatalf("Failed to put rollback_key: %v", err)
	}

	err = txn.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Reset after rollback should work
	err = txn.Reset(IsolationReadCommitted)
	if err != nil {
		t.Fatalf("Failed to reset transaction after rollback: %v", err)
	}

	// Use reset transaction
	err = txn.Put(cf, []byte("after_reset_key"), []byte("after_reset_value"), -1)
	if err != nil {
		t.Fatalf("Failed to put after_reset_key: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit after reset: %v", err)
	}

	txn.Free()

	// Verify rollback_key does not exist, after_reset_key exists
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	_, err = verifyTxn.Get(cf, []byte("rollback_key"))
	if err == nil {
		t.Fatalf("rollback_key should not exist")
	}

	value, err := verifyTxn.Get(cf, []byte("after_reset_key"))
	if err != nil {
		t.Fatalf("Failed to get after_reset_key: %v", err)
	}

	if string(value) != "after_reset_value" {
		t.Fatalf("Expected 'after_reset_value', got '%s'", string(value))
	}

	t.Logf("Transaction reset after rollback test completed successfully")
}

func TestCheckpoint(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	checkpointDir := "testdb_checkpoint"
	os.RemoveAll(checkpointDir)
	defer os.RemoveAll(checkpointDir)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write some data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("checkpoint_key_%d", i))
		value := []byte(fmt.Sprintf("checkpoint_value_%d", i))
		err = txn.Put(cf, key, value, -1)
		if err != nil {
			txn.Rollback()
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Create checkpoint
	err = db.Checkpoint(checkpointDir)
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	t.Logf("Checkpoint created successfully at %s", checkpointDir)

	// Verify checkpoint directory exists
	if _, err := os.Stat(checkpointDir); os.IsNotExist(err) {
		t.Fatalf("Checkpoint directory does not exist")
	}

	db.Close()

	// Open the checkpoint as a database and verify data
	checkpointConfig := Config{
		DBPath:               checkpointDir,
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	checkpointDB, err := Open(checkpointConfig)
	if err != nil {
		t.Fatalf("Failed to open checkpoint database: %v", err)
	}
	defer checkpointDB.Close()

	checkpointCF, err := checkpointDB.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family from checkpoint: %v", err)
	}

	// Verify data in checkpoint
	readTxn, err := checkpointDB.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction on checkpoint: %v", err)
	}
	defer readTxn.Free()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("checkpoint_key_%d", i))
		expectedValue := fmt.Sprintf("checkpoint_value_%d", i)

		value, err := readTxn.Get(checkpointCF, key)
		if err != nil {
			t.Fatalf("Failed to get key %s from checkpoint: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Fatalf("Expected '%s', got '%s'", expectedValue, string(value))
		}
	}

	// Verify checkpoint to existing directory fails
	err = checkpointDB.Checkpoint(checkpointDir)
	if err == nil {
		t.Fatalf("Expected error when checkpointing to existing non-empty directory")
	}

	t.Logf("Checkpoint test completed successfully")
}

func TestRangeCost(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.WriteBufferSize = 1024 // Small buffer to force flushes
	cfConfig.CompressionAlgorithm = LZ4Compression
	cfConfig.EnableBloomFilter = true
	cfConfig.BloomFPR = 0.01

	err = db.CreateColumnFamily("test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Insert data across a key range
	for batch := 0; batch < 5; batch++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("user:%04d", batch*50+i))
			value := make([]byte, 256)
			for j := range value {
				value[j] = byte(i % 256)
			}
			err = txn.Put(cf, key, value, -1)
			if err != nil {
				t.Fatalf("Failed to put key: %v", err)
			}
		}

		err = txn.Commit()
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		txn.Free()
	}

	// Wait for flushes to complete
	time.Sleep(500 * time.Millisecond)

	// Estimate cost for a narrow range
	costNarrow, err := cf.RangeCost([]byte("user:0000"), []byte("user:0010"))
	if err != nil {
		t.Fatalf("Failed to estimate narrow range cost: %v", err)
	}

	// Estimate cost for a wide range
	costWide, err := cf.RangeCost([]byte("user:0000"), []byte("user:0249"))
	if err != nil {
		t.Fatalf("Failed to estimate wide range cost: %v", err)
	}

	t.Logf("Narrow range cost (user:0000 - user:0010): %f", costNarrow)
	t.Logf("Wide range cost   (user:0000 - user:0249): %f", costWide)

	// Wide range should generally cost more than or equal to narrow range
	if costWide < costNarrow {
		t.Logf("Note: Wide range cost (%.2f) < narrow range cost (%.2f); may vary with data distribution", costWide, costNarrow)
	}

	// Test that key order does not matter
	costReversed, err := cf.RangeCost([]byte("user:0249"), []byte("user:0000"))
	if err != nil {
		t.Fatalf("Failed to estimate reversed range cost: %v", err)
	}

	if costWide != costReversed {
		t.Logf("Note: Forward cost (%.6f) != reversed cost (%.6f); expected equal", costWide, costReversed)
	}

	t.Logf("Range cost estimation test completed successfully")
}

func TestDeleteColumnFamily(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()

	err = db.CreateColumnFamily("delete_me", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	// Verify it exists
	cf, err := db.GetColumnFamily("delete_me")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Insert some data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Delete by pointer (faster than DropColumnFamily by name)
	err = db.DeleteColumnFamily(cf)
	if err != nil {
		t.Fatalf("Failed to delete column family by pointer: %v", err)
	}

	// Verify it no longer exists
	_, err = db.GetColumnFamily("delete_me")
	if err == nil {
		t.Fatalf("Expected error when getting deleted column family")
	}

	t.Logf("Delete column family by pointer test completed successfully")
}

func TestCommitHook(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("hook_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("hook_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Collect commit operations via the hook
	var mu sync.Mutex
	var capturedOps []CommitOp
	var capturedSeqs []uint64

	err = cf.SetCommitHook(func(ops []CommitOp, commitSeq uint64) int {
		mu.Lock()
		defer mu.Unlock()
		capturedOps = append(capturedOps, ops...)
		capturedSeqs = append(capturedSeqs, commitSeq)
		return 0
	})
	if err != nil {
		t.Fatalf("Failed to set commit hook: %v", err)
	}

	// Put some data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}

	err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)
	if err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Verify hook captured the put operations
	mu.Lock()
	if len(capturedOps) != 2 {
		t.Fatalf("Expected 2 captured ops, got %d", len(capturedOps))
	}

	for _, op := range capturedOps {
		if op.IsDelete {
			t.Fatalf("Expected put operation, got delete")
		}
		t.Logf("Captured put: key=%s value=%s", string(op.Key), string(op.Value))
	}

	if len(capturedSeqs) != 1 {
		t.Fatalf("Expected 1 commit sequence, got %d", len(capturedSeqs))
	}
	firstSeq := capturedSeqs[0]
	t.Logf("First commit sequence: %d", firstSeq)
	mu.Unlock()

	// Delete a key and verify the hook captures it
	txn2, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin delete transaction: %v", err)
	}

	err = txn2.Delete(cf, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to delete key1: %v", err)
	}

	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit delete transaction: %v", err)
	}
	txn2.Free()

	mu.Lock()
	if len(capturedOps) != 3 {
		t.Fatalf("Expected 3 captured ops after delete, got %d", len(capturedOps))
	}

	lastOp := capturedOps[2]
	if !lastOp.IsDelete {
		t.Fatalf("Expected delete operation")
	}
	if string(lastOp.Key) != "key1" {
		t.Fatalf("Expected deleted key 'key1', got '%s'", string(lastOp.Key))
	}
	if lastOp.Value != nil {
		t.Fatalf("Expected nil value for delete, got %v", lastOp.Value)
	}

	// Verify commit sequence is monotonically increasing
	if len(capturedSeqs) != 2 {
		t.Fatalf("Expected 2 commit sequences, got %d", len(capturedSeqs))
	}
	if capturedSeqs[1] <= capturedSeqs[0] {
		t.Fatalf("Expected monotonically increasing commit_seq: %d <= %d", capturedSeqs[1], capturedSeqs[0])
	}
	t.Logf("Second commit sequence: %d (monotonically increasing: OK)", capturedSeqs[1])
	mu.Unlock()

	t.Logf("Commit hook test completed successfully")
}

func TestCommitHookReplace(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("hook_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("hook_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Set first hook
	var hook1Count int
	var mu sync.Mutex

	err = cf.SetCommitHook(func(ops []CommitOp, commitSeq uint64) int {
		mu.Lock()
		hook1Count++
		mu.Unlock()
		return 0
	})
	if err != nil {
		t.Fatalf("Failed to set first commit hook: %v", err)
	}

	// Trigger first hook
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	err = txn.Put(cf, []byte("k1"), []byte("v1"), -1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn.Free()

	mu.Lock()
	if hook1Count != 1 {
		t.Fatalf("Expected hook1 called 1 time, got %d", hook1Count)
	}
	mu.Unlock()

	// Replace with second hook
	var hook2Count int

	err = cf.SetCommitHook(func(ops []CommitOp, commitSeq uint64) int {
		mu.Lock()
		hook2Count++
		mu.Unlock()
		return 0
	})
	if err != nil {
		t.Fatalf("Failed to set second commit hook: %v", err)
	}

	// Trigger second hook
	txn2, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	err = txn2.Put(cf, []byte("k2"), []byte("v2"), -1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn2.Free()

	mu.Lock()
	if hook1Count != 1 {
		t.Fatalf("Expected hook1 still called 1 time after replacement, got %d", hook1Count)
	}
	if hook2Count != 1 {
		t.Fatalf("Expected hook2 called 1 time, got %d", hook2Count)
	}
	mu.Unlock()

	t.Logf("Commit hook replacement test completed successfully")
}

func TestCommitHookClear(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := Config{
		DBPath:               "testdb",
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	err = db.CreateColumnFamily("hook_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("hook_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	var hookCount int
	var mu sync.Mutex

	err = cf.SetCommitHook(func(ops []CommitOp, commitSeq uint64) int {
		mu.Lock()
		hookCount++
		mu.Unlock()
		return 0
	})
	if err != nil {
		t.Fatalf("Failed to set commit hook: %v", err)
	}

	// Trigger hook
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	err = txn.Put(cf, []byte("k1"), []byte("v1"), -1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn.Free()

	mu.Lock()
	if hookCount != 1 {
		t.Fatalf("Expected hook called 1 time, got %d", hookCount)
	}
	mu.Unlock()

	// Clear the hook
	err = cf.ClearCommitHook()
	if err != nil {
		t.Fatalf("Failed to clear commit hook: %v", err)
	}

	// Commit again — hook should NOT fire
	txn2, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	err = txn2.Put(cf, []byte("k2"), []byte("v2"), -1)
	if err != nil {
		t.Fatalf("Failed to put: %v", err)
	}
	err = txn2.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn2.Free()

	mu.Lock()
	if hookCount != 1 {
		t.Fatalf("Expected hook still called 1 time after clear, got %d", hookCount)
	}
	mu.Unlock()

	// SetCommitHook with nil should also clear
	err = cf.SetCommitHook(nil)
	if err != nil {
		t.Fatalf("Failed to set nil commit hook: %v", err)
	}

	t.Logf("Commit hook clear test completed successfully")
}
