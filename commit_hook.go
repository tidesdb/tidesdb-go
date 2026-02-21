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
#include <tidesdb/db.h>
extern int set_commit_hook_bridge(tidesdb_column_family_t *cf, void *ctx);
*/
import "C"
import (
	"sync"
	"unsafe"
)

// CommitOp represents a single operation in a committed transaction batch.
type CommitOp struct {
	Key      []byte // Key data (copied; safe to retain after callback returns)
	Value    []byte // Value data (nil for deletes; copied; safe to retain)
	TTL      int64  // Time-to-live Unix timestamp (0 = no expiry)
	IsDelete bool   // true if this is a delete operation, false for put
}

// CommitHookFunc is the callback invoked synchronously after a transaction commits to a column family.
// ops contains the full batch of operations for that CF.
// commitSeq is a monotonic commit sequence number usable as a replication cursor.
// Return 0 on success, non-zero on failure (logged as warning by TidesDB).
type CommitHookFunc func(ops []CommitOp, commitSeq uint64) int

var (
	commitHookMu       sync.RWMutex
	commitHookNextID   uintptr = 1
	commitHookRegistry         = make(map[uintptr]CommitHookFunc)
	commitHookCFMap            = make(map[uintptr]uintptr) // cf pointer -> hook id
)

// SetCommitHook sets a commit hook callback for the column family.
// The hook fires synchronously after WAL write, memtable apply, and commit status marking.
// Hook failure is logged but does not roll back the commit (data is already durable).
// Setting a new hook replaces any previously set hook for this column family.
// Pass nil to disable the hook (equivalent to ClearCommitHook).
func (cf *ColumnFamily) SetCommitHook(fn CommitHookFunc) error {
	if fn == nil {
		return cf.ClearCommitHook()
	}

	commitHookMu.Lock()
	id := commitHookNextID
	commitHookNextID++

	// Clean up any previous hook for this CF
	cfKey := uintptr(unsafe.Pointer(cf.cf))
	if oldID, ok := commitHookCFMap[cfKey]; ok {
		delete(commitHookRegistry, oldID)
	}

	commitHookRegistry[id] = fn
	commitHookCFMap[cfKey] = id
	commitHookMu.Unlock()

	result := C.set_commit_hook_bridge(cf.cf, unsafe.Pointer(id))
	return errorFromCode(result, "failed to set commit hook")
}

// ClearCommitHook removes the commit hook callback from the column family.
func (cf *ColumnFamily) ClearCommitHook() error {
	commitHookMu.Lock()
	cfKey := uintptr(unsafe.Pointer(cf.cf))
	if oldID, ok := commitHookCFMap[cfKey]; ok {
		delete(commitHookRegistry, oldID)
		delete(commitHookCFMap, cfKey)
	}
	commitHookMu.Unlock()

	result := C.tidesdb_cf_set_commit_hook(cf.cf, nil, nil)
	return errorFromCode(result, "failed to clear commit hook")
}

//export goCommitHookCallback
func goCommitHookCallback(ops *C.tidesdb_commit_op_t, numOps C.int, commitSeq C.uint64_t, ctx unsafe.Pointer) C.int {
	id := uintptr(ctx)

	commitHookMu.RLock()
	fn, ok := commitHookRegistry[id]
	commitHookMu.RUnlock()

	if !ok {
		return 0
	}

	// Convert C ops array to Go slice
	n := int(numOps)
	goOps := make([]CommitOp, n)

	if n > 0 {
		cOps := (*[1 << 30]C.tidesdb_commit_op_t)(unsafe.Pointer(ops))[:n:n]
		for i := 0; i < n; i++ {
			goOps[i] = CommitOp{
				Key:      C.GoBytes(unsafe.Pointer(cOps[i].key), C.int(cOps[i].key_size)),
				TTL:      int64(cOps[i].ttl),
				IsDelete: cOps[i].is_delete != 0,
			}
			if cOps[i].value != nil && cOps[i].value_size > 0 {
				goOps[i].Value = C.GoBytes(unsafe.Pointer(cOps[i].value), C.int(cOps[i].value_size))
			}
		}
	}

	return C.int(fn(goOps, uint64(commitSeq)))
}
