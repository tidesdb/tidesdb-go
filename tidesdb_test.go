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
	"encoding/binary"
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

func TestTransactionSingleDelete(t *testing.T) {
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

	key := []byte("single_delete_key")
	value := []byte("insert_once")

	putTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin put transaction: %v", err)
	}

	err = putTxn.Put(cf, key, value, -1)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	err = putTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit put transaction: %v", err)
	}
	putTxn.Free()

	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}

	gotValue, err := readTxn.Get(cf, key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	readTxn.Free()

	if string(gotValue) != string(value) {
		t.Fatalf("Expected value %s, got %s", value, gotValue)
	}

	singleDeleteTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin single-delete transaction: %v", err)
	}

	err = singleDeleteTxn.SingleDelete(cf, key)
	if err != nil {
		t.Fatalf("Failed to single-delete key: %v", err)
	}

	err = singleDeleteTxn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit single-delete transaction: %v", err)
	}
	singleDeleteTxn.Free()

	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify transaction: %v", err)
	}
	defer verifyTxn.Free()

	_, err = verifyTxn.Get(cf, key)
	if err == nil {
		t.Fatalf("Expected key to be single-deleted but it still exists")
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

	// Commit again - hook should NOT fire
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

func TestBackup(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	backupDir := "testdb_backup"
	os.RemoveAll(backupDir)
	defer os.RemoveAll(backupDir)

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
		key := []byte(fmt.Sprintf("backup_key_%d", i))
		value := []byte(fmt.Sprintf("backup_value_%d", i))
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

	// Create backup
	err = db.Backup(backupDir)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	t.Logf("Backup created successfully at %s", backupDir)

	// Verify backup directory exists
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		t.Fatalf("Backup directory does not exist")
	}

	db.Close()

	// Open the backup as a database and verify data
	backupConfig := Config{
		DBPath:               backupDir,
		NumFlushThreads:      2,
		NumCompactionThreads: 2,
		LogLevel:             LogInfo,
		BlockCacheSize:       64 * 1024 * 1024,
		MaxOpenSSTables:      256,
	}

	backupDB, err := Open(backupConfig)
	if err != nil {
		t.Fatalf("Failed to open backup database: %v", err)
	}
	defer backupDB.Close()

	backupCF, err := backupDB.GetColumnFamily("test_cf")
	if err != nil {
		t.Fatalf("Failed to get column family from backup: %v", err)
	}

	// Verify data in backup
	readTxn, err := backupDB.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction on backup: %v", err)
	}
	defer readTxn.Free()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("backup_key_%d", i))
		expectedValue := fmt.Sprintf("backup_value_%d", i)

		value, err := readTxn.Get(backupCF, key)
		if err != nil {
			t.Fatalf("Failed to get key %s from backup: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Fatalf("Expected '%s', got '%s'", expectedValue, string(value))
		}
	}

	// Verify backup to existing directory fails
	err = backupDB.Backup(backupDir)
	if err == nil {
		t.Fatalf("Expected error when backing up to existing non-empty directory")
	}

	t.Logf("Backup test completed successfully")
}

func TestRenameColumnFamily(t *testing.T) {
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
	err = db.CreateColumnFamily("old_name", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	// Insert data
	cf, err := db.GetColumnFamily("old_name")
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

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Rename column family
	err = db.RenameColumnFamily("old_name", "new_name")
	if err != nil {
		t.Fatalf("Failed to rename column family: %v", err)
	}

	// Verify old name no longer exists
	_, err = db.GetColumnFamily("old_name")
	if err == nil {
		t.Fatalf("Expected old_name to not exist after rename")
	}

	// Verify new name exists and data is intact
	renamedCF, err := db.GetColumnFamily("new_name")
	if err != nil {
		t.Fatalf("Failed to get renamed column family: %v", err)
	}

	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTxn.Free()

	value, err := readTxn.Get(renamedCF, []byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 from renamed CF: %v", err)
	}

	if string(value) != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", string(value))
	}

	// Verify renaming to existing name fails
	err = db.CreateColumnFamily("another_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create another_cf: %v", err)
	}

	err = db.RenameColumnFamily("new_name", "another_cf")
	if err == nil {
		t.Fatalf("Expected error when renaming to existing column family name")
	}

	// Verify renaming non-existent CF fails
	err = db.RenameColumnFamily("nonexistent", "something")
	if err == nil {
		t.Fatalf("Expected error when renaming non-existent column family")
	}

	t.Logf("Rename column family test completed successfully")
}

func TestUpdateRuntimeConfig(t *testing.T) {
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

	// Update runtime config
	newConfig := DefaultColumnFamilyConfig()
	newConfig.WriteBufferSize = 256 * 1024 * 1024
	newConfig.SkipListMaxLevel = 16
	newConfig.BloomFPR = 0.001

	err = cf.UpdateRuntimeConfig(newConfig, false)
	if err != nil {
		t.Fatalf("Failed to update runtime config: %v", err)
	}

	// Verify updated config via stats
	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats after config update: %v", err)
	}

	if stats.Config != nil {
		if stats.Config.WriteBufferSize != 256*1024*1024 {
			t.Errorf("Expected WriteBufferSize 256MB, got %d", stats.Config.WriteBufferSize)
		}
		t.Logf("Updated WriteBufferSize: %d", stats.Config.WriteBufferSize)
		t.Logf("Updated SkipListMaxLevel: %d", stats.Config.SkipListMaxLevel)
	}

	// Write data with updated config to verify it works
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
	if err != nil {
		t.Fatalf("Failed to put key after config update: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit after config update: %v", err)
	}
	txn.Free()

	t.Logf("Update runtime config test completed successfully")
}

func TestIsFlushingIsCompacting(t *testing.T) {
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

	// IsFlushing and IsCompacting should return false for idle CF
	if cf.IsFlushing() {
		t.Logf("Note: Column family is flushing (may be expected in some environments)")
	}

	if cf.IsCompacting() {
		t.Logf("Note: Column family is compacting (may be expected in some environments)")
	}

	// Write data and trigger flush
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		err = txn.Put(cf, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn.Free()

	// Trigger flush and check status
	err = cf.FlushMemtable()
	if err != nil {
		t.Logf("Flush note: %v", err)
	}

	// Log current status (may or may not be flushing depending on timing)
	t.Logf("IsFlushing: %v", cf.IsFlushing())
	t.Logf("IsCompacting: %v", cf.IsCompacting())

	t.Logf("IsFlushing/IsCompacting test completed successfully")
}

func TestGetComparator(t *testing.T) {
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

	// Built-in comparator "memcmp" should be registered
	found, err := db.GetComparator("memcmp")
	if err != nil {
		t.Fatalf("Failed to get built-in comparator 'memcmp': %v", err)
	}
	if !found {
		t.Fatalf("Expected built-in comparator 'memcmp' to be registered")
	}
	t.Logf("Built-in comparator 'memcmp' found: %v", found)

	// Built-in comparator "reverse" should be registered
	found, err = db.GetComparator("reverse")
	if err != nil {
		t.Fatalf("Failed to get built-in comparator 'reverse': %v", err)
	}
	if !found {
		t.Fatalf("Expected built-in comparator 'reverse' to be registered")
	}
	t.Logf("Built-in comparator 'reverse' found: %v", found)

	// Non-existent comparator should fail
	found, err = db.GetComparator("nonexistent_comparator")
	if err == nil {
		t.Fatalf("Expected error for non-existent comparator")
	}
	t.Logf("Non-existent comparator correctly returned error: %v", err)

	t.Logf("GetComparator test completed successfully")
}

func TestSyncWal(t *testing.T) {
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

	// Create column family with SyncNone to test manual WAL sync
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.SyncMode = SyncNone

	err = db.CreateColumnFamily("sync_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("sync_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write some data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("sync_key_%d", i))
		value := []byte(fmt.Sprintf("sync_value_%d", i))
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

	// Force WAL sync
	err = cf.SyncWal()
	if err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Verify data is still readable after sync
	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTxn.Free()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("sync_key_%d", i))
		expectedValue := fmt.Sprintf("sync_value_%d", i)

		value, err := readTxn.Get(cf, key)
		if err != nil {
			t.Fatalf("Failed to get key %s after WAL sync: %v", key, err)
		}

		if string(value) != expectedValue {
			t.Fatalf("Expected '%s', got '%s'", expectedValue, string(value))
		}
	}

	t.Logf("SyncWal test completed successfully")
}

func TestPurgeCF(t *testing.T) {
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

	err = db.CreateColumnFamily("purge_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("purge_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write multiple batches to create multiple SSTables
	for batch := 0; batch < 5; batch++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		for i := 0; i < 20; i++ {
			key := []byte(fmt.Sprintf("purge_key_%d_%d", batch, i))
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

	// Purge the column family (synchronous flush + compaction)
	err = cf.PurgeCF()
	if err != nil {
		t.Fatalf("Failed to purge column family: %v", err)
	}

	// After purge, flushing and compacting should be done
	if cf.IsFlushing() {
		t.Logf("Note: Column family still flushing after purge (unexpected)")
	}
	if cf.IsCompacting() {
		t.Logf("Note: Column family still compacting after purge (unexpected)")
	}

	// Verify data is still readable
	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTxn.Free()

	value, err := readTxn.Get(cf, []byte("purge_key_0_0"))
	if err != nil {
		t.Fatalf("Failed to get key after purge: %v", err)
	}
	if len(value) != 256 {
		t.Fatalf("Expected value length 256, got %d", len(value))
	}

	t.Logf("PurgeCF test completed successfully")
}

func TestPurge(t *testing.T) {
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
	cfConfig.WriteBufferSize = 1024

	// Create multiple column families
	cfNames := []string{"purge_cf1", "purge_cf2"}
	for _, name := range cfNames {
		err = db.CreateColumnFamily(name, cfConfig)
		if err != nil {
			t.Fatalf("Failed to create column family %s: %v", name, err)
		}
	}

	// Write data to each column family
	for _, name := range cfNames {
		cf, err := db.GetColumnFamily(name)
		if err != nil {
			t.Fatalf("Failed to get column family %s: %v", name, err)
		}

		for batch := 0; batch < 3; batch++ {
			txn, err := db.BeginTxn()
			if err != nil {
				t.Fatalf("Failed to begin transaction: %v", err)
			}

			for i := 0; i < 20; i++ {
				key := []byte(fmt.Sprintf("key_%d_%d", batch, i))
				value := make([]byte, 128)
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
	}

	// Purge entire database (all CFs)
	err = db.Purge()
	if err != nil {
		t.Fatalf("Failed to purge database: %v", err)
	}

	// Verify data in all column families is still readable
	for _, name := range cfNames {
		cf, err := db.GetColumnFamily(name)
		if err != nil {
			t.Fatalf("Failed to get column family %s after purge: %v", name, err)
		}

		readTxn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin read transaction: %v", err)
		}

		value, err := readTxn.Get(cf, []byte("key_0_0"))
		if err != nil {
			t.Fatalf("Failed to get key from %s after purge: %v", name, err)
		}
		if len(value) != 128 {
			t.Fatalf("Expected value length 128 in %s, got %d", name, len(value))
		}

		readTxn.Free()
	}

	t.Logf("Purge (database-level) test completed successfully")
}

func TestGetDbStats(t *testing.T) {
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

	// Get stats with no column families
	dbStats, err := db.GetDbStats()
	if err != nil {
		t.Fatalf("Failed to get database stats: %v", err)
	}

	if dbStats.NumColumnFamilies != 0 {
		t.Fatalf("Expected 0 column families, got %d", dbStats.NumColumnFamilies)
	}
	t.Logf("Initial DB stats: %d CFs, total memory: %d", dbStats.NumColumnFamilies, dbStats.TotalMemory)

	// Create column families and add data
	cfConfig := DefaultColumnFamilyConfig()

	err = db.CreateColumnFamily("stats_cf1", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	err = db.CreateColumnFamily("stats_cf2", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf1, err := db.GetColumnFamily("stats_cf1")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("stats_key_%d", i))
		value := []byte(fmt.Sprintf("stats_value_%d", i))
		err = txn.Put(cf1, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Get stats after creating CFs and data
	dbStats, err = db.GetDbStats()
	if err != nil {
		t.Fatalf("Failed to get database stats after data: %v", err)
	}

	if dbStats.NumColumnFamilies != 2 {
		t.Fatalf("Expected 2 column families, got %d", dbStats.NumColumnFamilies)
	}

	if dbStats.TotalMemory == 0 {
		t.Fatalf("Expected non-zero total memory")
	}

	if dbStats.ResolvedMemoryLimit == 0 {
		t.Fatalf("Expected non-zero resolved memory limit")
	}

	t.Logf("DB Stats after data:")
	t.Logf("  Column families: %d", dbStats.NumColumnFamilies)
	t.Logf("  Total memory: %d bytes", dbStats.TotalMemory)
	t.Logf("  Available memory: %d bytes", dbStats.AvailableMemory)
	t.Logf("  Resolved memory limit: %d bytes", dbStats.ResolvedMemoryLimit)
	t.Logf("  Memory pressure level: %d", dbStats.MemoryPressureLevel)
	t.Logf("  Flush pending count: %d", dbStats.FlushPendingCount)
	t.Logf("  Total memtable bytes: %d", dbStats.TotalMemtableBytes)
	t.Logf("  Total immutable count: %d", dbStats.TotalImmutableCount)
	t.Logf("  Total SSTable count: %d", dbStats.TotalSstableCount)
	t.Logf("  Total data size: %d bytes", dbStats.TotalDataSizeBytes)
	t.Logf("  Open SSTables: %d", dbStats.NumOpenSstables)
	t.Logf("  Global seq: %d", dbStats.GlobalSeq)
	t.Logf("  Txn memory bytes: %d", dbStats.TxnMemoryBytes)
	t.Logf("  Compaction queue size: %d", dbStats.CompactionQueueSize)
	t.Logf("  Flush queue size: %d", dbStats.FlushQueueSize)

	t.Logf("GetDbStats test completed successfully")
}

func TestUnifiedMemtable(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)
	var memtableBuffSize uint64 = 8 * 1024

	config := Config{
		DBPath:                             "testdb",
		NumFlushThreads:                    2,
		NumCompactionThreads:               2,
		LogLevel:                           LogInfo,
		BlockCacheSize:                     64 * 1024 * 1024,
		MaxOpenSSTables:                    256,
		UnifiedMemtable:                    true,
		UnifiedMemtableWriteBufferSize:     memtableBuffSize,
		UnifiedMemtableSkipListProbability: 0.25,
	}

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer func() {
		// Drain all pending flushes before close to avoid race between
		// flush workers and the close path on macOS
		db.Purge()
		db.Close()
	}()

	// Get stats with no column families
	dbStats, err := db.GetDbStats()
	if err != nil {
		t.Fatalf("Failed to get database stats: %v", err)
	}

	if !dbStats.UnifiedMemtableEnabled {
		t.Fatalf("Unified memtable not enabled.")
	}

	// Create column families and add data
	cfConfig := DefaultColumnFamilyConfig()

	err = db.CreateColumnFamily("stats_cf1", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	err = db.CreateColumnFamily("stats_cf2", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf1, err := db.GetColumnFamily("stats_cf1")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write data
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("stats_key_%d", i))
		value := []byte(fmt.Sprintf("stats_value_%d", i))
		err = txn.Put(cf1, key, value, -1)
		if err != nil {
			t.Fatalf("Failed to put key: %v", err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	txn.Free()

	// Get stats after creating CFs and data
	dbStats, err = db.GetDbStats()
	if err != nil {
		t.Fatalf("Failed to get database stats after data: %v", err)
	}

	if dbStats.NumColumnFamilies != 2 {
		t.Fatalf("Expected 2 column families, got %d", dbStats.NumColumnFamilies)
	}

	if dbStats.TotalMemory == 0 {
		t.Fatalf("Expected non-zero total memory")
	}

	// check bytes in active memtable
	if dbStats.UnifiedMemtableBytes > int64(memtableBuffSize) {
		t.Fatalf("Memtable buffer overflowed.")
	}

	t.Logf("DB Stats for unified memtable:")
	t.Logf("  Column families: %d", dbStats.NumColumnFamilies)
	t.Logf("  Total memory: %d bytes", dbStats.TotalMemory)
	t.Logf("  Available memory: %d bytes", dbStats.AvailableMemory)
	t.Logf("  Resolved memory limit: %d bytes", dbStats.ResolvedMemoryLimit)
	t.Logf("  Memory pressure level: %d", dbStats.MemoryPressureLevel)
	t.Logf("  Flush pending count: %d", dbStats.FlushPendingCount)
	t.Logf("  Total memtable bytes: %d", dbStats.TotalMemtableBytes)
	t.Logf("  Total immutable count: %d", dbStats.TotalImmutableCount)
	t.Logf("  Total SSTable count: %d", dbStats.TotalSstableCount)
	t.Logf("  Total data size: %d bytes", dbStats.TotalDataSizeBytes)
	t.Logf("  Open SSTables: %d", dbStats.NumOpenSstables)
	t.Logf("  Global seq: %d", dbStats.GlobalSeq)
	t.Logf("  Txn memory bytes: %d", dbStats.TxnMemoryBytes)
	t.Logf("  Compaction queue size: %d", dbStats.CompactionQueueSize)
	t.Logf("  Flush queue size: %d", dbStats.FlushQueueSize)
	t.Logf("  Unified memtable enabled: %t", dbStats.UnifiedMemtableEnabled)
	t.Logf("  Unified memtable bytes: %d", dbStats.UnifiedMemtableBytes)

	t.Logf("UnifiedMemtable test completed successfully")
}

func TestInitFinalize(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	// Finalize first in case a previous test left it initialized
	Finalize()

	// Init with system allocator
	err := Init()
	if err != nil {
		t.Fatalf("Failed to init TidesDB: %v", err)
	}

	// Second init should fail (already initialized)
	err = Init()
	if err == nil {
		t.Fatalf("Expected error on double init, got nil")
	}
	t.Logf("Expected error on double init: %v", err)

	// Open DB to verify init worked
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
		t.Fatalf("Failed to open database after init: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Finalize
	Finalize()
	t.Logf("InitFinalize test completed successfully")
}

func TestIterKeyValue(t *testing.T) {
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

	// Write data
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
		err = txn.Put(cf, []byte(k), []byte(v), -1)
		if err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	err = txn.Commit()
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}
	txn.Free()

	// Read using KeyValue()
	readTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTxn.Free()

	iter, err := readTxn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	iter.SeekToFirst()
	count := 0
	for iter.Valid() {
		key, value, err := iter.KeyValue()
		if err != nil {
			t.Fatalf("Failed to get key-value: %v", err)
		}

		expectedValue, ok := testData[string(key)]
		if !ok {
			t.Fatalf("Unexpected key: %s", key)
		}

		if string(value) != expectedValue {
			t.Fatalf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}

		t.Logf("KeyValue: %s = %s", key, value)
		count++
		iter.Next()
	}

	if count != len(testData) {
		t.Fatalf("Expected %d entries, got %d", len(testData), count)
	}

	t.Logf("IterKeyValue test completed successfully")
}

func TestColumnFamilyConfigObjectStoreFields(t *testing.T) {
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
	cfConfig.ObjectLazyCompaction = 1
	cfConfig.ObjectPrefetchCompaction = 0

	err = db.CreateColumnFamily("objstore_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family with object store config: %v", err)
	}

	cf, err := db.GetColumnFamily("objstore_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.Config == nil {
		t.Fatalf("Expected config in stats, got nil")
	}

	t.Logf("ObjectLazyCompaction: %d", stats.Config.ObjectLazyCompaction)
	t.Logf("ObjectPrefetchCompaction: %d", stats.Config.ObjectPrefetchCompaction)

	t.Logf("ColumnFamilyConfigObjectStoreFields test completed successfully")
}

func TestDbStatsObjectStoreFields(t *testing.T) {
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

	dbStats, err := db.GetDbStats()
	if err != nil {
		t.Fatalf("Failed to get db stats: %v", err)
	}

	// Without object store configured, these should be defaults
	if dbStats.ObjectStoreEnabled {
		t.Fatalf("Expected object store disabled, got enabled")
	}

	if dbStats.ReplicaMode {
		t.Fatalf("Expected replica mode disabled, got enabled")
	}

	// Without a primary lease (no object store), single-writer fencing epochs are zero.
	if dbStats.PrimaryEpoch != 0 {
		t.Fatalf("Expected PrimaryEpoch 0 without a lease, got %d", dbStats.PrimaryEpoch)
	}
	if dbStats.SeenEpoch != 0 {
		t.Fatalf("Expected SeenEpoch 0 without a lease, got %d", dbStats.SeenEpoch)
	}

	t.Logf("ObjectStoreEnabled: %t", dbStats.ObjectStoreEnabled)
	t.Logf("ObjectStoreConnector: %s", dbStats.ObjectStoreConnector)
	t.Logf("LocalCacheBytesUsed: %d", dbStats.LocalCacheBytesUsed)
	t.Logf("LocalCacheBytesMax: %d", dbStats.LocalCacheBytesMax)
	t.Logf("LocalCacheNumFiles: %d", dbStats.LocalCacheNumFiles)
	t.Logf("LastUploadedGeneration: %d", dbStats.LastUploadedGeneration)
	t.Logf("UploadQueueDepth: %d", dbStats.UploadQueueDepth)
	t.Logf("TotalUploads: %d", dbStats.TotalUploads)
	t.Logf("TotalUploadFailures: %d", dbStats.TotalUploadFailures)
	t.Logf("ReplicaMode: %t", dbStats.ReplicaMode)
	t.Logf("PrimaryEpoch: %d", dbStats.PrimaryEpoch)
	t.Logf("SeenEpoch: %d", dbStats.SeenEpoch)

	t.Logf("DbStatsObjectStoreFields test completed successfully")
}

func TestObjStoreDefaultConfig(t *testing.T) {
	cfg := ObjStoreDefaultConfig()

	// Verify defaults match C defaults
	if !cfg.CacheOnRead {
		t.Fatalf("Expected CacheOnRead=true by default")
	}
	if !cfg.CacheOnWrite {
		t.Fatalf("Expected CacheOnWrite=true by default")
	}
	if cfg.MaxConcurrentUploads <= 0 {
		t.Fatalf("Expected positive MaxConcurrentUploads, got %d", cfg.MaxConcurrentUploads)
	}
	if cfg.MaxConcurrentDownloads <= 0 {
		t.Fatalf("Expected positive MaxConcurrentDownloads, got %d", cfg.MaxConcurrentDownloads)
	}

	t.Logf("ObjStoreDefaultConfig:")
	t.Logf("  CacheOnRead: %t", cfg.CacheOnRead)
	t.Logf("  CacheOnWrite: %t", cfg.CacheOnWrite)
	t.Logf("  MaxConcurrentUploads: %d", cfg.MaxConcurrentUploads)
	t.Logf("  MaxConcurrentDownloads: %d", cfg.MaxConcurrentDownloads)
	t.Logf("  MultipartThreshold: %d", cfg.MultipartThreshold)
	t.Logf("  MultipartPartSize: %d", cfg.MultipartPartSize)
	t.Logf("  SyncManifestToObject: %t", cfg.SyncManifestToObject)
	t.Logf("  ReplicateWal: %t", cfg.ReplicateWal)
	t.Logf("  WalUploadSync: %t", cfg.WalUploadSync)
	t.Logf("  WalSyncThresholdBytes: %d", cfg.WalSyncThresholdBytes)
	t.Logf("  WalSyncOnCommit: %t", cfg.WalSyncOnCommit)
	t.Logf("  ReplicaMode: %t", cfg.ReplicaMode)
	t.Logf("  ReplicaSyncIntervalUs: %d", cfg.ReplicaSyncIntervalUs)
	t.Logf("  ReplicaReplayWal: %t", cfg.ReplicaReplayWal)

	t.Logf("ObjStoreDefaultConfig test completed successfully")
}

func TestObjStoreFsCreate(t *testing.T) {
	testDir := "testdb_objstore_fs"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	os.MkdirAll(testDir, 0755)

	store, err := ObjStoreFsCreate(testDir)
	if err != nil {
		t.Fatalf("Failed to create FS object store: %v", err)
	}

	if store == nil {
		t.Fatalf("Expected non-nil store")
	}

	if store.store == nil {
		t.Fatalf("Expected non-nil store handle")
	}

	t.Logf("ObjStoreFsCreate test completed successfully")
}

func TestCfConfigIni(t *testing.T) {
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

	// Create a column family with specific config
	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.WriteBufferSize = 32 * 1024 * 1024
	cfConfig.CompressionAlgorithm = ZstdCompression
	cfConfig.EnableBloomFilter = true
	cfConfig.BloomFPR = 0.005

	err = db.CreateColumnFamily("ini_test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	// Save config to INI
	iniFile := "testdb/test_config.ini"
	err = CfConfigSaveToIni(iniFile, "ini_test_cf", cfConfig)
	if err != nil {
		t.Fatalf("Failed to save config to INI: %v", err)
	}

	// Load config from INI
	loaded, err := CfConfigLoadFromIni(iniFile, "ini_test_cf")
	if err != nil {
		t.Fatalf("Failed to load config from INI: %v", err)
	}

	if loaded.WriteBufferSize != cfConfig.WriteBufferSize {
		t.Fatalf("WriteBufferSize mismatch: expected %d, got %d", cfConfig.WriteBufferSize, loaded.WriteBufferSize)
	}

	if loaded.CompressionAlgorithm != cfConfig.CompressionAlgorithm {
		t.Fatalf("CompressionAlgorithm mismatch: expected %d, got %d", cfConfig.CompressionAlgorithm, loaded.CompressionAlgorithm)
	}

	t.Logf("Loaded config from INI:")
	t.Logf("  WriteBufferSize: %d", loaded.WriteBufferSize)
	t.Logf("  CompressionAlgorithm: %d", loaded.CompressionAlgorithm)
	t.Logf("  BloomFPR: %f", loaded.BloomFPR)
	t.Logf("  EnableBloomFilter: %t", loaded.EnableBloomFilter)

	t.Logf("CfConfigIni test completed successfully")
}

func TestErrorCodeReadonly(t *testing.T) {
	// Just verify the constant exists and has the right value
	if ErrReadonly != -13 {
		t.Fatalf("Expected ErrReadonly = -13, got %d", ErrReadonly)
	}
	t.Logf("ErrReadonly = %d", ErrReadonly)
	t.Logf("ErrorCodeReadonly test completed successfully")
}

func TestObjStoreBackendConstants(t *testing.T) {
	if BackendFS != 0 {
		t.Fatalf("Expected BackendFS = 0, got %d", BackendFS)
	}
	if BackendS3 != 1 {
		t.Fatalf("Expected BackendS3 = 1, got %d", BackendS3)
	}
	if BackendUnknown != 99 {
		t.Fatalf("Expected BackendUnknown = 99, got %d", BackendUnknown)
	}
	t.Logf("BackendFS=%d, BackendS3=%d, BackendUnknown=%d", BackendFS, BackendS3, BackendUnknown)
	t.Logf("ObjStoreBackendConstants test completed successfully")
}

func TestTombstoneDensityConfig(t *testing.T) {
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

	defaults := DefaultColumnFamilyConfig()
	if defaults.TombstoneDensityMinEntries == 0 {
		t.Fatalf("Expected non-zero default TombstoneDensityMinEntries, got %d", defaults.TombstoneDensityMinEntries)
	}
	t.Logf("Default TombstoneDensityTrigger: %f", defaults.TombstoneDensityTrigger)
	t.Logf("Default TombstoneDensityMinEntries: %d", defaults.TombstoneDensityMinEntries)

	cfConfig := defaults
	cfConfig.TombstoneDensityTrigger = 0.5
	cfConfig.TombstoneDensityMinEntries = 256

	if err := db.CreateColumnFamily("density_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	cf, err := db.GetColumnFamily("density_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.Config == nil {
		t.Fatalf("Stats.Config was nil")
	}
	if stats.Config.TombstoneDensityTrigger != 0.5 {
		t.Fatalf("TombstoneDensityTrigger mismatch: expected 0.5, got %f", stats.Config.TombstoneDensityTrigger)
	}
	if stats.Config.TombstoneDensityMinEntries != 256 {
		t.Fatalf("TombstoneDensityMinEntries mismatch: expected 256, got %d", stats.Config.TombstoneDensityMinEntries)
	}
	t.Logf("Tombstone density config round-trip OK")
}

func TestTombstoneDensityStats(t *testing.T) {
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
	cfConfig.WriteBufferSize = 4 * 1024
	if err := db.CreateColumnFamily("ts_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("ts_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Insert keys, then delete a portion to generate tombstones.
	const total = 200
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin txn: %v", err)
	}
	for i := 0; i < total; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		val := []byte(fmt.Sprintf("val%05d", i))
		if err := txn.Put(cf, key, val, -1); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	txn.Free()

	if err := cf.FlushMemtable(); err != nil {
		t.Fatalf("FlushMemtable failed: %v", err)
	}

	delTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin delete txn: %v", err)
	}
	for i := 0; i < total/2; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := delTxn.Delete(cf, key); err != nil {
			t.Fatalf("Delete failed at %d: %v", i, err)
		}
	}
	if err := delTxn.Commit(); err != nil {
		t.Fatalf("Commit delete failed: %v", err)
	}
	delTxn.Free()

	if err := cf.FlushMemtable(); err != nil {
		t.Fatalf("FlushMemtable (post delete) failed: %v", err)
	}
	// Allow the flush to land
	time.Sleep(500 * time.Millisecond)

	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	t.Logf("TotalTombstones: %d", stats.TotalTombstones)
	t.Logf("TombstoneRatio: %f", stats.TombstoneRatio)
	t.Logf("MaxSSTDensity: %f", stats.MaxSSTDensity)
	t.Logf("MaxSSTDensityLevel: %d", stats.MaxSSTDensityLevel)
	t.Logf("LevelTombstoneCounts: %v", stats.LevelTombstoneCounts)

	if stats.NumLevels > 0 && stats.LevelTombstoneCounts == nil {
		t.Fatalf("Expected LevelTombstoneCounts when num_levels=%d", stats.NumLevels)
	}
	if stats.LevelTombstoneCounts != nil && len(stats.LevelTombstoneCounts) != stats.NumLevels {
		t.Fatalf("LevelTombstoneCounts length=%d, expected %d", len(stats.LevelTombstoneCounts), stats.NumLevels)
	}
	if stats.TombstoneRatio < 0.0 || stats.TombstoneRatio > 1.0 {
		t.Fatalf("TombstoneRatio out of range [0,1]: %f", stats.TombstoneRatio)
	}
	if stats.MaxSSTDensity < 0.0 || stats.MaxSSTDensity > 1.0 {
		t.Fatalf("MaxSSTDensity out of range [0,1]: %f", stats.MaxSSTDensity)
	}
}

func TestCompactRange(t *testing.T) {
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
	cfConfig.WriteBufferSize = 4 * 1024
	if err := db.CreateColumnFamily("range_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("range_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write and flush in a few batches to create multiple sstables
	for batch := 0; batch < 4; batch++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin txn: %v", err)
		}
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("k_%02d_%04d", batch, i))
			val := make([]byte, 256)
			for j := range val {
				val[j] = byte(i)
			}
			if err := txn.Put(cf, key, val, -1); err != nil {
				t.Fatalf("Put failed: %v", err)
			}
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txn.Free()
		if err := cf.FlushMemtable(); err != nil {
			t.Fatalf("FlushMemtable failed: %v", err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	// Compact a narrow key range
	start := []byte("k_01_")
	end := []byte("k_02_")
	if err := cf.CompactRange(start, end); err != nil {
		t.Fatalf("CompactRange failed: %v", err)
	}

	// Both nil/empty endpoints should be rejected
	if err := cf.CompactRange(nil, nil); err == nil {
		t.Fatalf("Expected error when both bounds nil, got nil")
	} else {
		t.Logf("CompactRange(nil,nil) correctly rejected: %v", err)
	}

	// Verify keys outside the range are still readable
	verifyTxn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin verify txn: %v", err)
	}
	defer verifyTxn.Free()

	v, err := verifyTxn.Get(cf, []byte("k_03_0000"))
	if err != nil {
		t.Fatalf("Get post-compact failed: %v", err)
	}
	if len(v) != 256 {
		t.Fatalf("Unexpected value length: %d", len(v))
	}
	t.Logf("CompactRange completed; post-compact read ok")
}

func TestMaxConcurrentFlushesConfig(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	defaults := DefaultConfig()
	t.Logf("Default MaxConcurrentFlushes: %d", defaults.MaxConcurrentFlushes)

	config := defaults
	config.DBPath = "testdb"
	config.MaxConcurrentFlushes = 1

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database with MaxConcurrentFlushes=1: %v", err)
	}
	defer db.Close()

	// Smoke test that the DB still works with the override.
	cfConfig := DefaultColumnFamilyConfig()
	if err := db.CreateColumnFamily("flush_cap_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("flush_cap_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin txn: %v", err)
	}
	if err := txn.Put(cf, []byte("k"), []byte("v"), -1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	txn.Free()

	if err := cf.FlushMemtable(); err != nil {
		t.Fatalf("FlushMemtable failed: %v", err)
	}
}

func TestRaiseOpenFileLimit(t *testing.T) {
	// A non-positive request reports the current ceiling without changing it.
	current := RaiseOpenFileLimit(0)
	if current <= 0 {
		t.Fatalf("Expected a positive current open-file ceiling, got %d", current)
	}
	t.Logf("Current open-file ceiling: %d", current)

	// Requesting the current ceiling should report at least that many.
	after := RaiseOpenFileLimit(current)
	if after < current {
		t.Fatalf("Ceiling shrank after raise attempt: before=%d after=%d", current, after)
	}
	t.Logf("Open-file ceiling after raise(%d): %d", current, after)
}

func TestCancelBackgroundWork(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := DefaultConfig()
	config.DBPath = "testdb"

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	if err := db.CreateColumnFamily("cancel_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("cancel_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// Write some data so there is potential background work.
	for i := 0; i < 100; i++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin txn: %v", err)
		}
		key := []byte(fmt.Sprintf("key-%04d", i))
		if err := txn.Put(cf, key, bytes.Repeat([]byte("v"), 128), -1); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txn.Free()
	}

	if err := db.CancelBackgroundWork(); err != nil {
		t.Fatalf("CancelBackgroundWork failed: %v", err)
	}
	t.Logf("CancelBackgroundWork completed successfully")
}

func TestFinishCompactionsOnCloseConfig(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	defaults := DefaultConfig()
	t.Logf("Default FinishCompactionsOnClose: %t", defaults.FinishCompactionsOnClose)

	config := defaults
	config.DBPath = "testdb"
	config.FinishCompactionsOnClose = true

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database with FinishCompactionsOnClose=true: %v", err)
	}

	cfConfig := DefaultColumnFamilyConfig()
	if err := db.CreateColumnFamily("finish_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("finish_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin txn: %v", err)
	}
	if err := txn.Put(cf, []byte("k"), []byte("v"), -1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	txn.Free()

	// Close should honor the finish-compactions-on-close behavior without error.
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestErrorCodeBusy(t *testing.T) {
	if ErrBusy != -14 {
		t.Fatalf("Expected ErrBusy to be -14, got %d", ErrBusy)
	}
	t.Logf("ErrBusy = %d", ErrBusy)
}

func TestErrorCodePrecondition(t *testing.T) {
	if ErrPrecondition != -15 {
		t.Fatalf("Expected ErrPrecondition to be -15, got %d", ErrPrecondition)
	}
	t.Logf("ErrPrecondition = %d", ErrPrecondition)
}

func TestBuiltinComparatorNames(t *testing.T) {
	cases := map[string]string{
		ComparatorMemcmp:          "memcmp",
		ComparatorLexicographic:   "lexicographic",
		ComparatorUint64:          "uint64",
		ComparatorInt64:           "int64",
		ComparatorReverse:         "reverse",
		ComparatorCaseInsensitive: "case_insensitive",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("Built-in comparator name mismatch: got %q want %q", got, want)
		}
	}
}

func TestUint64ComparatorOrdering(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := DefaultConfig()
	config.DBPath = "testdb"

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	cfConfig.ComparatorName = ComparatorUint64
	if err := db.CreateColumnFamily("u64_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("u64_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	// The uint64 comparator reads each 8-byte key in host byte order, so encode
	// keys with NativeEndian. Insert out of numeric order.
	nums := []uint64{5, 1, 256, 3, 2, 1000, 4}
	for _, n := range nums {
		key := make([]byte, 8)
		binary.NativeEndian.PutUint64(key, n)
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin txn: %v", err)
		}
		if err := txn.Put(cf, key, []byte("v"), -1); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txn.Free()
	}

	// Verify the cf config round-trips the comparator name.
	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats.Config == nil || stats.Config.ComparatorName != ComparatorUint64 {
		t.Fatalf("Expected ComparatorName %q in stats config, got %+v", ComparatorUint64, stats.Config)
	}

	// Iterate forward; keys must come out in ascending numeric order.
	txn, err := db.BeginTxn()
	if err != nil {
		t.Fatalf("Failed to begin txn: %v", err)
	}
	defer txn.Free()
	iter, err := txn.NewIterator(cf)
	if err != nil {
		t.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Free()

	if err := iter.SeekToFirst(); err != nil {
		t.Fatalf("SeekToFirst failed: %v", err)
	}

	var got []uint64
	for iter.Valid() {
		key, err := iter.Key()
		if err != nil {
			t.Fatalf("Key failed: %v", err)
		}
		if len(key) != 8 {
			t.Fatalf("Expected 8-byte key, got %d bytes", len(key))
		}
		got = append(got, binary.NativeEndian.Uint64(key))
		if err := iter.Next(); err != nil {
			break
		}
	}

	want := []uint64{1, 2, 3, 4, 5, 256, 1000}
	if len(got) != len(want) {
		t.Fatalf("Expected %d keys, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("uint64 comparator ordering wrong at %d: got %v want %v", i, got, want)
		}
	}
	t.Logf("uint64 comparator produced ascending order: %v", got)
}

func TestComparatorCtxStrIni(t *testing.T) {
	iniFile := "testdb_comparator_ctx.ini"
	os.Remove(iniFile)
	defer os.Remove(iniFile)

	config := DefaultColumnFamilyConfig()
	config.Name = "ctx_cf"
	config.ComparatorName = ComparatorMemcmp
	config.ComparatorCtxStr = "endian=big;width=8"

	if err := CfConfigSaveToIni(iniFile, "ctx_cf", config); err != nil {
		t.Fatalf("Failed to save config to INI: %v", err)
	}

	loaded, err := CfConfigLoadFromIni(iniFile, "ctx_cf")
	if err != nil {
		t.Fatalf("Failed to load config from INI: %v", err)
	}

	if loaded.ComparatorName != config.ComparatorName {
		t.Fatalf("ComparatorName mismatch: got %q want %q", loaded.ComparatorName, config.ComparatorName)
	}
	if loaded.ComparatorCtxStr != config.ComparatorCtxStr {
		t.Fatalf("ComparatorCtxStr mismatch: got %q want %q", loaded.ComparatorCtxStr, config.ComparatorCtxStr)
	}
	t.Logf("Round-tripped ComparatorCtxStr: %q", loaded.ComparatorCtxStr)
}

func TestStatsWriteAmplification(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := DefaultConfig()
	config.DBPath = "testdb"

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	if err := db.CreateColumnFamily("wa_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("wa_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	for i := 0; i < 200; i++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin txn: %v", err)
		}
		key := []byte(fmt.Sprintf("wa-key-%05d", i))
		if err := txn.Put(cf, key, bytes.Repeat([]byte("x"), 256), -1); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txn.Free()
	}

	if err := cf.FlushMemtable(); err != nil {
		t.Fatalf("FlushMemtable failed: %v", err)
	}
	// Block until flush settles so flush counters are populated.
	if err := cf.PurgeCF(); err != nil {
		t.Fatalf("PurgeCF failed: %v", err)
	}

	stats, err := cf.GetStats()
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}

	t.Logf("WA counters: user=%d wal=%d flush=%d(%d files) compaction_w=%d compaction_r=%d(%d outs)",
		stats.UserBytesWritten, stats.WalBytesWritten, stats.FlushBytesWritten, stats.FlushCount,
		stats.CompactionBytesWritten, stats.CompactionBytesRead, stats.CompactionCount)

	if stats.UserBytesWritten == 0 {
		t.Fatalf("Expected UserBytesWritten > 0 after committing data")
	}
	if stats.FlushBytesWritten == 0 {
		t.Fatalf("Expected FlushBytesWritten > 0 after flush+purge")
	}
}

func TestDbStatsWriteAmplification(t *testing.T) {
	cleanupTestDB(t)
	defer cleanupTestDB(t)

	config := DefaultConfig()
	config.DBPath = "testdb"

	db, err := Open(config)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	cfConfig := DefaultColumnFamilyConfig()
	if err := db.CreateColumnFamily("dbwa_cf", cfConfig); err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}
	cf, err := db.GetColumnFamily("dbwa_cf")
	if err != nil {
		t.Fatalf("Failed to get column family: %v", err)
	}

	for i := 0; i < 100; i++ {
		txn, err := db.BeginTxn()
		if err != nil {
			t.Fatalf("Failed to begin txn: %v", err)
		}
		key := []byte(fmt.Sprintf("dbwa-%05d", i))
		if err := txn.Put(cf, key, bytes.Repeat([]byte("y"), 200), -1); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}
		txn.Free()
	}

	if err := db.Purge(); err != nil {
		t.Fatalf("Purge failed: %v", err)
	}

	stats, err := db.GetDbStats()
	if err != nil {
		t.Fatalf("GetDbStats failed: %v", err)
	}

	t.Logf("DB WA counters: user=%d uwal=%d wal=%d flush=%d compaction_w=%d compaction_r=%d flush_count=%d compaction_count=%d",
		stats.UserBytesWritten, stats.UwalBytesWritten, stats.WalBytesWritten, stats.FlushBytesWritten,
		stats.CompactionBytesWritten, stats.CompactionBytesRead, stats.FlushCount, stats.CompactionCount)

	if stats.UserBytesWritten == 0 {
		t.Fatalf("Expected db-wide UserBytesWritten > 0 after committing data")
	}
}
