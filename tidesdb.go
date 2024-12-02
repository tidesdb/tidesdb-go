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
#include <tidesdb.h> // you must have TidesDB shared library installed
#include <stdlib.h>
*/
import "C"
import (
	"time"
	"unsafe"
)

// TidesDB struct holds the pointer to the C tidesdb_t struct
type TidesDB struct {
	tdb *C.tidesdb_t
}

// TidesDBErr struct holds the error code and message
type TidesDBErr struct {
	Code    int    // the error code
	Message string // the error message
}

// Open opens a TidesDB instance
func Open(path string, compressedWal bool) (*TidesDB, *TidesDBErr) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	var cTdb *C.tidesdb_t
	var cConfig C.tidesdb_config_t
	cConfig.db_path = cPath
	if compressedWal {
		cConfig.compressed_wal = C._Bool(true)
	} else {
		cConfig.compressed_wal = C._Bool(false)
	}

	cErr := C.tidesdb_open(&cConfig, &cTdb)
	if cErr != nil {
		return nil, &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}

	return &TidesDB{tdb: cTdb}, nil
}

// Close closes the TidesDB instance
func (db *TidesDB) Close() *TidesDBErr {
	cErr := C.tidesdb_close(db.tdb)
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// CreateColumnFamily creates a column family
func (db *TidesDB) CreateColumnFamily(name string, flushThreshold, maxLevel int, probability float32, compressed bool) *TidesDBErr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cErr := C.tidesdb_create_column_family(db.tdb, cName, C.int(flushThreshold), C.int(maxLevel), C.float(probability), C._Bool(compressed))
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// DropColumnFamily drops a column family
func (db *TidesDB) DropColumnFamily(name string) *TidesDBErr {
	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	cErr := C.tidesdb_drop_column_family(db.tdb, cName)
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// CompactSSTables compacts the SSTables for a column family
func (db *TidesDB) CompactSSTables(cf string, maxThreads int) *TidesDBErr {
	cErr := C.tidesdb_compact_sstables(db.tdb, C.CString(cf), C.int(maxThreads))
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// Put puts a key-value pair into the database
func (db *TidesDB) Put(columnFamilyName string, key []byte, value []byte, ttl time.Duration) *TidesDBErr {
	cColumnFamilyName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cColumnFamilyName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	cValue := (*C.uint8_t)(unsafe.Pointer(&value[0]))

	cErr := C.tidesdb_put(db.tdb, cColumnFamilyName, cKey, C.size_t(len(key)), cValue, C.size_t(len(value)), C.time_t(ttl.Seconds()))
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// Get gets a value from the database
func (db *TidesDB) Get(columnFamilyName string, key []byte) ([]byte, *TidesDBErr) {
	cColumnFamilyName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cColumnFamilyName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))
	var cValue *C.uint8_t
	var cValueSize C.size_t

	cErr := C.tidesdb_get(db.tdb, cColumnFamilyName, cKey, C.size_t(len(key)), &cValue, &cValueSize)
	if cErr != nil {
		return nil, &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}

	value := C.GoBytes(unsafe.Pointer(cValue), C.int(cValueSize))
	C.free(unsafe.Pointer(cValue))

	return value, nil
}

// Delete deletes a key-value pair from the database
func (db *TidesDB) Delete(columnFamilyName string, key []byte) *TidesDBErr {
	cColumnFamilyName := C.CString(columnFamilyName)
	defer C.free(unsafe.Pointer(cColumnFamilyName))

	cKey := (*C.uint8_t)(unsafe.Pointer(&key[0]))

	cErr := C.tidesdb_delete(db.tdb, cColumnFamilyName, cKey, C.size_t(len(key)))
	if cErr != nil {
		return &TidesDBErr{
			Code:    int(cErr.code),
			Message: C.GoString(cErr.message),
		}
	}
	return nil
}

// must add cursor support, transaction support
