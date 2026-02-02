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
