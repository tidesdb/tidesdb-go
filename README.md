# tidesdb-go

tidesdb-go is the official GO binding for TidesDB.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This GO binding provides a safe, idiomatic GO interface to TidesDB with full support for all features.

## Features

- Full ACID transaction support with savepoints
- MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
- Column families aka isolated key-value stores with independent configuration
- Bidirectional iterators with forward/backward traversal with seek support
- TTL(time to live) support with automatic key expiration and clean up
- LZ4, LZ4 Fast, ZSTD, or no compression
- Bloom filters with configurable false positive rates
- Global block CLOCK cache for hot blocks
- Automatic with configurable thread pools (sorted runs, compaction)
- Six built-in plus custom registration

## Getting Started

### Prerequisites

You must have the TidesDB shared C library installed on your system.

**Building TidesDB**
```bash
git clone https://github.com/tidesdb/tidesdb.git
cd tidesdb

# Build and install (compile with sanitizer and tests OFF for bindings)
rm -rf build && cmake -S . -B build -DTIDESDB_WITH_SANITIZER=OFF -DTIDESDB_BUILD_TESTS=OFF
cmake --build build
sudo cmake --install build
```

**Dependencies**
- Snappy
- LZ4
- Zstandard

**On Ubuntu/Debian**
```bash
sudo apt install libzstd-dev liblz4-dev libsnappy-dev
```

**On macOS**
```bash
brew install zstd lz4 snappy
```

### Installation

```bash
go get github.com/tidesdb/tidesdb-go
```

### Custom Installation Paths

If you installed TidesDB to a non-standard location, you can specify custom paths using CGO environment variables:

```bash
# Set custom include and library paths
export CGO_CFLAGS="-I/custom/path/include"
export CGO_LDFLAGS="-L/custom/path/lib -ltidesdb"

# Then install/build
go get github.com/tidesdb/tidesdb-go
```

**Custom prefix installation**
```bash
# Install TidesDB to custom location
cd tidesdb
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/opt/tidesdb
cmake --build build
sudo cmake --install build

# Configure Go to use custom location
export CGO_CFLAGS="-I/opt/tidesdb/include"
export CGO_LDFLAGS="-L/opt/tidesdb/lib -ltidesdb"
export LD_LIBRARY_PATH="/opt/tidesdb/lib:$LD_LIBRARY_PATH"  # Linux
# or
export DYLD_LIBRARY_PATH="/opt/tidesdb/lib:$DYLD_LIBRARY_PATH"  # macOS

go get github.com/tidesdb/tidesdb-go
```

## Usage

### Opening and Closing a Database

```go
package main

import (
    "fmt"
    "log"
    
    tidesdb "github.com/tidesdb/tidesdb-go"
)

func main() {
    config := tidesdb.Config{
        DBPath:               "./mydb",
        NumFlushThreads:      2,
        NumCompactionThreads: 2,
        LogLevel:             tidesdb.LogInfo,
        BlockCacheSize:       64 * 1024 * 1024, 
        MaxOpenSSTables:      256,
    }
    
    db, err := tidesdb.Open(config)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    fmt.Println("Database opened successfully")
}
```

### Creating and Dropping Column Families

Column families are isolated key-value stores with independent configuration.

```go
cfConfig := tidesdb.DefaultColumnFamilyConfig()
err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}

// Create with custom configuration based on defaults
cfConfig := tidesdb.DefaultColumnFamilyConfig()

// You can modify the configuration as needed
cfConfig.WriteBufferSize = 128 * 1024 * 1024   
cfConfig.LevelSizeRatio = 10                    
cfConfig.MinLevels = 5                           
cfConfig.CompressionAlgorithm = tidesdb.LZ4Compression
cfConfig.EnableBloomFilter = true
cfConfig.BloomFPR = 0.01                        
cfConfig.EnableBlockIndexes = true
cfConfig.SyncMode = tidesdb.SyncInterval
cfConfig.SyncIntervalUs = 128000                  
cfConfig.DefaultIsolationLevel = tidesdb.IsolationReadCommitted

err = db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}

err = db.DropColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}
```

### CRUD Operations

All operations in TidesDB are performed through transactions for ACID guarantees.

#### Writing Data

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Put a key-value pair (TTL -1 means no expiration)
err = txn.Put(cf, []byte("key"), []byte("value"), -1)
if err != nil {
    log.Fatal(err)
}

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

#### Writing with TTL

```go
import "time"

cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Set expiration time (Unix timestamp)
ttl := time.Now().Add(10 * time.Second).Unix()

err = txn.Put(cf, []byte("temp_key"), []byte("temp_value"), ttl)
if err != nil {
    log.Fatal(err)
}

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

**TTL Examples**
```go
// No expiration
ttl := int64(-1)

// Expire in 5 minutes
ttl := time.Now().Add(5 * time.Minute).Unix()

// Expire in 1 hour
ttl := time.Now().Add(1 * time.Hour).Unix()

// Expire at specific time
ttl := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
```

#### Reading Data

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

value, err := txn.Get(cf, []byte("key"))
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Value: %s\n", value)
```

#### Deleting Data

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

err = txn.Delete(cf, []byte("key"))
if err != nil {
    log.Fatal(err)
}

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

#### Multi-Operation Transactions

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Multiple operations in one transaction, across column families as well
err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

err = txn.Delete(cf, []byte("old_key"))
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

// Commit atomically -- all or nothing
err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Iterating Over Data

Iterators provide efficient bidirectional traversal over key-value pairs.

#### Forward Iteration

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

iter, err := txn.NewIterator(cf)
if err != nil {
    log.Fatal(err)
}
defer iter.Free()

iter.SeekToFirst()

for iter.Valid() {
    key, err := iter.Key()
    if err != nil {
        log.Fatal(err)
    }
    
    value, err := iter.Value()
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Key: %s, Value: %s\n", key, value)
    
    iter.Next()
}
```

#### Backward Iteration

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

iter, err := txn.NewIterator(cf)
if err != nil {
    log.Fatal(err)
}
defer iter.Free()

iter.SeekToLast()

for iter.Valid() {
    key, err := iter.Key()
    if err != nil {
        log.Fatal(err)
    }
    
    value, err := iter.Value()
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Key: %s, Value: %s\n", key, value)
    
    iter.Prev()
}
```

### Getting Column Family Statistics

Retrieve detailed statistics about a column family.

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

stats, err := cf.GetStats()
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Number of Levels: %d\n", stats.NumLevels)
fmt.Printf("Memtable Size: %d bytes\n", stats.MemtableSize)

if stats.Config != nil {
    fmt.Printf("Write Buffer Size: %d\n", stats.Config.WriteBufferSize)
    fmt.Printf("Compression: %d\n", stats.Config.CompressionAlgorithm)
    fmt.Printf("Bloom Filter: %v\n", stats.Config.EnableBloomFilter)
    fmt.Printf("Sync Mode: %d\n", stats.Config.SyncMode)
}
```

### Listing Column Families

```go
cfList, err := db.ListColumnFamilies()
if err != nil {
    log.Fatal(err)
}

fmt.Println("Available column families:")
for _, name := range cfList {
    fmt.Printf("  - %s\n", name)
}
```

### Compaction

#### Manual Compaction

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

// Manually trigger compaction (queues compaction from L1+)
err = cf.Compact()
if err != nil {
    log.Printf("Compaction note: %v", err)
}
```

#### Manual Memtable Flush

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

// Manually trigger memtable flush (queues memtable for sorted run to disk (L1))
err = cf.FlushMemtable()
if err != nil {
    log.Printf("Flush note: %v", err)
}
```

**Benefits**
- Removes tombstones and expired TTL entries
- Merges duplicate keys (keeps latest version)
- Reduces SSTable count
- Background compaction is non-blocking

### Sync Modes

Control the durability vs performance tradeoff.

```go
cfConfig := tidesdb.DefaultColumnFamilyConfig()

// SyncNone -- Fastest, least durable (OS handles flushing on sorted runs and compaction to sync after completion)
cfConfig.SyncMode = tidesdb.SyncNone

// SyncInterval -- Balanced (periodic background syncing)
cfConfig.SyncMode = tidesdb.SyncInterval
cfConfig.SyncIntervalUs = 128000  // Sync every 128ms

// SyncFull -- Most durable (fsync on every write)
cfConfig.SyncMode = tidesdb.SyncFull

err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}
```

### Compression Algorithms

TidesDB supports multiple compression algorithms:

```go
cfConfig := tidesdb.DefaultColumnFamilyConfig()

cfConfig.CompressionAlgorithm = tidesdb.NoCompression     
cfConfig.CompressionAlgorithm = tidesdb.LZ4Compression    
cfConfig.CompressionAlgorithm = tidesdb.LZ4FastCompression 
cfConfig.CompressionAlgorithm = tidesdb.ZstdCompression   

err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}
```

## Error Handling

TidesDB provides detailed error codes for production use.

```go
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

err = txn.Put(cf, []byte("key"), []byte("value"), -1)
if err != nil {
    // Errors include context and error codes
    fmt.Printf("Error: %v\n", err)
    
    // Example error message:
    // "failed to put key-value pair: memory allocation failed (code: -1)"
    
    txn.Rollback()
    return
}

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

**Error Codes**
- `ErrSuccess` (0) -- Operation successful
- `ErrMemory` (-1) -- Memory allocation failed
- `ErrInvalidArgs` (-2) -- Invalid arguments
- `ErrNotFound` (-3) -- Key not found
- `ErrIO` (-4) -- I/O error
- `ErrCorruption` (-5) -- Data corruption
- `ErrExists` (-6) -- Resource already exists
- `ErrConflict` (-7) -- Transaction conflict
- `ErrTooLarge` (-8) -- Key or value too large
- `ErrMemoryLimit` (-9) -- Memory limit exceeded
- `ErrInvalidDB` (-10) -- Invalid database handle
- `ErrUnknown` (-11) -- Unknown error
- `ErrLocked` (-12) -- Database is locked

## Complete Example

```go
package main

import (
    "fmt"
    "log"
    "time"
    
    tidesdb "github.com/tidesdb/tidesdb-go"
)

func main() {
    config := tidesdb.Config{
        DBPath:               "./example_db",
        NumFlushThreads:      2,
        NumCompactionThreads: 2,
        LogLevel:             tidesdb.LogInfo,
        BlockCacheSize:       64 * 1024 * 1024,
        MaxOpenSSTables:      256,
    }
    
    db, err := tidesdb.Open(config)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    cfConfig := tidesdb.DefaultColumnFamilyConfig()
    cfConfig.WriteBufferSize = 64 * 1024 * 1024
    cfConfig.CompressionAlgorithm = tidesdb.LZ4Compression
    cfConfig.EnableBloomFilter = true
    cfConfig.BloomFPR = 0.01
    cfConfig.SyncMode = tidesdb.SyncInterval
    cfConfig.SyncIntervalUs = 128000
    
    err = db.CreateColumnFamily("users", cfConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer db.DropColumnFamily("users")
    
    cf, err := db.GetColumnFamily("users")
    if err != nil {
        log.Fatal(err)
    }
    
    txn, err := db.BeginTxn()
    if err != nil {
        log.Fatal(err)
    }
    
    err = txn.Put(cf, []byte("user:1"), []byte("Alice"), -1)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    err = txn.Put(cf, []byte("user:2"), []byte("Bob"), -1)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    // Add temporary data with TTL
    ttl := time.Now().Add(30 * time.Second).Unix()
    err = txn.Put(cf, []byte("session:abc"), []byte("temp_data"), ttl)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    err = txn.Commit()
    if err != nil {
        log.Fatal(err)
    }
    txn.Free()
    
    readTxn, err := db.BeginTxn()
    if err != nil {
        log.Fatal(err)
    }
    defer readTxn.Free()
    
    value, err := readTxn.Get(cf, []byte("user:1"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("user:1 = %s\n", value)
    
    iter, err := readTxn.NewIterator(cf)
    if err != nil {
        log.Fatal(err)
    }
    defer iter.Free()
    
    fmt.Println("\nAll entries:")
    iter.SeekToFirst()
    for iter.Valid() {
        key, _ := iter.Key()
        value, _ := iter.Value()
        fmt.Printf("  %s = %s\n", key, value)
        iter.Next()
    }
    
    stats, err := cf.GetStats()
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("\nColumn Family Statistics:\n")
    fmt.Printf("  Number of Levels: %d\n", stats.NumLevels)
    fmt.Printf("  Memtable Size: %d bytes\n", stats.MemtableSize)
}
```

## Isolation Levels

TidesDB supports five MVCC isolation levels:

```go
txn, err := db.BeginTxnWithIsolation(tidesdb.IsolationReadCommitted)
if err != nil {
    log.Fatal(err)
}
defer txn.Free()
```

**Available Isolation Levels**
- `IsolationReadUncommitted` -- Sees all data including uncommitted changes
- `IsolationReadCommitted` -- Sees only committed data (default)
- `IsolationRepeatableRead` -- Consistent snapshot, phantom reads possible
- `IsolationSnapshot` -- Write-write conflict detection
- `IsolationSerializable` -- Full read-write conflict detection (SSI)

## Savepoints

Savepoints allow partial rollback within a transaction:

```go
txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

err = txn.Put(cf, []byte("key1"), []byte("value1"), -1)

err = txn.Savepoint("sp1")
err = txn.Put(cf, []byte("key2"), []byte("value2"), -1)

// Rollback to savepoint -- key2 is discarded, key1 remains
err = txn.RollbackToSavepoint("sp1")

// Commit -- only key1 is written
err = txn.Commit()
```

## Testing

```bash
# Run all tests
go test -v

# Run specific test
go test -v -run TestOpenClose

# Run with race detector
go test -race -v
```

## License

Multiple licenses apply:

```
Mozilla Public License Version 2.0 (TidesDB)

BSD 3 Clause (Snappy)
BSD 2 (LZ4)
BSD 2 (xxHash - Yann Collet)
BSD (Zstandard)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or discussions
- GitHub Issues https://github.com/tidesdb/tidesdb-go/issues
- Discord Community https://discord.gg/tWEmjR66cy
- Main TidesDB Repository https://github.com/tidesdb/tidesdb