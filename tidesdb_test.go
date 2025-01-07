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
	"os"
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

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true, int(TDB_MEMTABLE_SKIP_LIST))
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

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true, int(TDB_MEMTABLE_SKIP_LIST))
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

	err = db.CreateColumnFamily("test_cf", 1024*1024*64, 12, 0.24, true, int(TDB_COMPRESS_SNAPPY), true, int(TDB_MEMTABLE_SKIP_LIST))
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

// More tests to be added...
