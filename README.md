# tidesdb-go

tidesdb-go is the official Go binding for TidesDB v1.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This Go binding provides a safe, idiomatic Go interface to TidesDB with full support for all v1 features.

## Features

- **ACID Transactions** - Atomic, consistent, isolated, and durable transactions across column families
- **Optimized Concurrency** - Multiple concurrent readers, writers don't block readers
- **Column Families** - Isolated key-value stores with independent configuration
- **Bidirectional Iterators** - Iterate forward and backward over sorted key-value pairs
- **TTL Support** - Time-to-live for automatic key expiration
- **Compression** - Snappy, LZ4, or ZSTD compression support
- **Bloom Filters** - Reduce disk reads with configurable false positive rates
- **Background Compaction** - Automatic or manual SSTable compaction with parallel execution
- **Sync Modes** - Three durability levels: NONE, BACKGROUND, FULL
- **Custom Comparators** - Register custom key comparison functions
- **Error Handling** - Detailed error codes for production use

## Getting Started

### Prerequisites

You must have the TidesDB v1 shared C library installed on your system.

**Building TidesDB**
```bash
# Clone TidesDB repository
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
- OpenSSL

**On Ubuntu/Debian**
```bash
sudo apt install libzstd-dev liblz4-dev libsnappy-dev libssl-dev
```

**On macOS**
```bash
brew install zstd lz4 snappy openssl
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

**Example: Custom prefix installation**
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
    // Configure and open database
    config := tidesdb.Config{
        DBPath:             "./mydb",
        EnableDebugLogging: false,
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
// Create with default configuration
cfConfig := tidesdb.DefaultColumnFamilyConfig()
err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}

// Create with custom configuration
cfConfig := tidesdb.DefaultColumnFamilyConfig()
cfConfig.MemtableFlushSize = 128 * 1024 * 1024  // 128MB
cfConfig.MaxSSTablesBeforeCompaction = 512       // Trigger compaction at 512 SSTables
cfConfig.CompactionThreads = 4                   // Use 4 threads for parallel compaction
cfConfig.Compressed = true
cfConfig.CompressAlgo = tidesdb.TDB_COMPRESS_LZ4
cfConfig.BloomFilterFPRate = 0.01               // 1% false positive rate
cfConfig.EnableBackgroundCompaction = true
cfConfig.SyncMode = tidesdb.TDB_SYNC_BACKGROUND
cfConfig.SyncInterval = 1000                     // Sync every 1 second

err = db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}

// Drop a column family
err = db.DropColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}
```

### CRUD Operations

All operations in TidesDB v1 are performed through transactions for ACID guarantees.

#### Writing Data

```go
// Begin a write transaction
txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Put a key-value pair (TTL -1 means no expiration)
err = txn.Put("my_cf", []byte("key"), []byte("value"), -1)
if err != nil {
    log.Fatal(err)
}

// Commit the transaction
err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

#### Writing with TTL

```go
import "time"

txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Set expiration time (Unix timestamp)
ttl := time.Now().Add(10 * time.Second).Unix()

err = txn.Put("my_cf", []byte("temp_key"), []byte("temp_value"), ttl)
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
ttl := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC).Unix()
```

#### Reading Data

```go
// Begin a read-only transaction
txn, err := db.BeginReadTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Get a value
value, err := txn.Get("my_cf", []byte("key"))
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Value: %s\n", value)
```

#### Deleting Data

```go
txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

err = txn.Delete("my_cf", []byte("key"))
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
txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

// Multiple operations in one transaction
err = txn.Put("my_cf", []byte("key1"), []byte("value1"), -1)
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

err = txn.Put("my_cf", []byte("key2"), []byte("value2"), -1)
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

err = txn.Delete("my_cf", []byte("old_key"))
if err != nil {
    txn.Rollback()
    log.Fatal(err)
}

// Commit atomically - all or nothing
err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Iterating Over Data

Iterators provide efficient bidirectional traversal over key-value pairs.

#### Forward Iteration

```go
txn, err := db.BeginReadTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

iter, err := txn.NewIterator("my_cf")
if err != nil {
    log.Fatal(err)
}
defer iter.Free()

// Seek to first entry
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
txn, err := db.BeginReadTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

iter, err := txn.NewIterator("my_cf")
if err != nil {
    log.Fatal(err)
}
defer iter.Free()

// Seek to last entry
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
stats, err := db.GetColumnFamilyStats("my_cf")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Column Family: %s\n", stats.Name)
fmt.Printf("Comparator: %s\n", stats.ComparatorName)
fmt.Printf("Number of SSTables: %d\n", stats.NumSSTables)
fmt.Printf("Total SSTable Size: %d bytes\n", stats.TotalSSTableSize)
fmt.Printf("Memtable Size: %d bytes\n", stats.MemtableSize)
fmt.Printf("Memtable Entries: %d\n", stats.MemtableEntries)
fmt.Printf("Compression: %v\n", stats.Config.Compressed)
fmt.Printf("Bloom Filter FP Rate: %.4f\n", stats.Config.BloomFilterFPRate)
fmt.Printf("Sync Mode: %d\n", stats.Config.SyncMode)
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

Compaction merges SSTables, removes tombstones and expired keys.

#### Automatic Background Compaction

```go
// Enable during column family creation
cfConfig := tidesdb.DefaultColumnFamilyConfig()
cfConfig.EnableBackgroundCompaction = true
cfConfig.MaxSSTablesBeforeCompaction = 128  // Trigger at 128 SSTables
cfConfig.CompactionThreads = 4              // Use 4 threads for parallel compaction

err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}

// Background compaction runs automatically when threshold is reached
```

#### Manual Compaction

```go
// Get column family
cf, err := db.GetColumnFamily("my_cf")
if err != nil {
    log.Fatal(err)
}

// Manually trigger compaction (requires minimum 2 SSTables)
err = cf.Compact()
if err != nil {
    log.Printf("Compaction note: %v", err)
}
```

**Benefits**
- Removes tombstones and expired TTL entries
- Merges duplicate keys (keeps latest version)
- Reduces SSTable count
- Parallel compaction speeds up large compactions
- Background compaction is non-blocking

### Sync Modes

Control the durability vs performance tradeoff.

```go
cfConfig := tidesdb.DefaultColumnFamilyConfig()

// TDB_SYNC_NONE - Fastest, least durable (OS handles flushing)
cfConfig.SyncMode = tidesdb.TDB_SYNC_NONE

// TDB_SYNC_BACKGROUND - Balanced (fsync every N milliseconds in background)
cfConfig.SyncMode = tidesdb.TDB_SYNC_BACKGROUND
cfConfig.SyncInterval = 1000  // Sync every 1000ms (1 second)

// TDB_SYNC_FULL - Most durable (fsync on every write)
cfConfig.SyncMode = tidesdb.TDB_SYNC_FULL

err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}
```

### Compression Algorithms

TidesDB supports multiple compression algorithms:

```go
cfConfig := tidesdb.DefaultColumnFamilyConfig()
cfConfig.Compressed = true

// Available compression algorithms
cfConfig.CompressAlgo = tidesdb.TDB_NO_COMPRESSION   // No compression
cfConfig.CompressAlgo = tidesdb.TDB_COMPRESS_SNAPPY  // Snappy (fast)
cfConfig.CompressAlgo = tidesdb.TDB_COMPRESS_LZ4     // LZ4 (very fast)
cfConfig.CompressAlgo = tidesdb.TDB_COMPRESS_ZSTD    // Zstandard (high compression)

err := db.CreateColumnFamily("my_cf", cfConfig)
if err != nil {
    log.Fatal(err)
}
```

## Error Handling

TidesDB v1 provides detailed error codes for production use.

```go
txn, err := db.BeginTxn()
if err != nil {
    log.Fatal(err)
}
defer txn.Free()

err = txn.Put("my_cf", []byte("key"), []byte("value"), -1)
if err != nil {
    // Errors include context and error codes
    fmt.Printf("Error: %v\n", err)
    
    // Example error message:
    // "failed to put key-value pair: memory allocation failed (code: -2)"
    
    txn.Rollback()
    return
}

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

**Error Codes**
- `TDB_SUCCESS` (0) - Operation successful
- `TDB_ERR_MEMORY` (-2) - Memory allocation failed
- `TDB_ERR_INVALID_ARGS` (-3) - Invalid arguments
- `TDB_ERR_IO` (-4) - I/O error
- `TDB_ERR_NOT_FOUND` (-5) - Key not found
- `TDB_ERR_EXISTS` (-6) - Resource already exists
- `TDB_ERR_CORRUPT` (-7) - Data corruption
- `TDB_ERR_LOCK` (-8) - Lock acquisition failed
- `TDB_ERR_TXN_COMMITTED` (-9) - Transaction already committed
- `TDB_ERR_TXN_ABORTED` (-10) - Transaction aborted
- `TDB_ERR_READONLY` (-11) - Write on read-only transaction
- `TDB_ERR_FULL` (-12) - Database full
- `TDB_ERR_INVALID_NAME` (-13) - Invalid name
- `TDB_ERR_COMPARATOR_NOT_FOUND` (-14) - Comparator not found
- `TDB_ERR_MAX_COMPARATORS` (-15) - Max comparators reached
- `TDB_ERR_INVALID_CF` (-16) - Invalid column family
- `TDB_ERR_THREAD` (-17) - Thread operation failed
- `TDB_ERR_CHECKSUM` (-18) - Checksum verification failed

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
    // Open database
    config := tidesdb.Config{
        DBPath:             "./example_db",
        EnableDebugLogging: false,
    }
    
    db, err := tidesdb.Open(config)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Create column family with custom configuration
    cfConfig := tidesdb.DefaultColumnFamilyConfig()
    cfConfig.MemtableFlushSize = 64 * 1024 * 1024
    cfConfig.Compressed = true
    cfConfig.CompressAlgo = tidesdb.TDB_COMPRESS_LZ4
    cfConfig.BloomFilterFPRate = 0.01
    cfConfig.EnableBackgroundCompaction = true
    cfConfig.SyncMode = tidesdb.TDB_SYNC_BACKGROUND
    cfConfig.SyncInterval = 1000
    
    err = db.CreateColumnFamily("users", cfConfig)
    if err != nil {
        log.Fatal(err)
    }
    defer db.DropColumnFamily("users")
    
    // Write data with transaction
    txn, err := db.BeginTxn()
    if err != nil {
        log.Fatal(err)
    }
    
    err = txn.Put("users", []byte("user:1"), []byte("Alice"), -1)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    err = txn.Put("users", []byte("user:2"), []byte("Bob"), -1)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    // Add temporary data with TTL
    ttl := time.Now().Add(30 * time.Second).Unix()
    err = txn.Put("users", []byte("session:abc"), []byte("temp_data"), ttl)
    if err != nil {
        txn.Rollback()
        log.Fatal(err)
    }
    
    err = txn.Commit()
    if err != nil {
        log.Fatal(err)
    }
    txn.Free()
    
    // Read data
    readTxn, err := db.BeginReadTxn()
    if err != nil {
        log.Fatal(err)
    }
    defer readTxn.Free()
    
    value, err := readTxn.Get("users", []byte("user:1"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("user:1 = %s\n", value)
    
    // Iterate over all entries
    iter, err := readTxn.NewIterator("users")
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
    
    // Get statistics
    stats, err := db.GetColumnFamilyStats("users")
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("\nColumn Family Statistics:\n")
    fmt.Printf("  Name: %s\n", stats.Name)
    fmt.Printf("  Memtable Size: %d bytes\n", stats.MemtableSize)
    fmt.Printf("  Memtable Entries: %d\n", stats.MemtableEntries)
    fmt.Printf("  Number of SSTables: %d\n", stats.NumSSTables)
}
```

## Concurrency Model

TidesDB is designed for high concurrency:

- **Multiple readers can read concurrently** - No blocking between readers
- **Writers don't block readers** - Readers can access data during writes
- **Writers block other writers** - Only one writer per column family at a time
- **Read transactions** (`BeginReadTxn`) acquire read locks
- **Write transactions** (`BeginTxn`) acquire write locks on commit
- **Different column families** can be written concurrently

**Optimal for**
- Read-heavy workloads
- Mixed read/write workloads
- Multi-column-family applications

## Performance Tips

1. **Batch operations** in transactions for better performance
2. **Use appropriate sync mode** for your durability requirements
3. **Enable background compaction** for automatic maintenance
4. **Adjust memtable flush size** based on your workload
5. **Use compression** to reduce disk usage and I/O
6. **Configure bloom filters** to reduce unnecessary disk reads
7. **Set appropriate TTL** to automatically expire old data
8. **Use parallel compaction** for faster SSTable merging

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

-- AND --

BSD 3 Clause (Snappy)
BSD 2 (LZ4)
BSD 2 (xxHash - Yann Collet)
BSD (Zstandard)
Apache 2.0 (OpenSSL 3.0+) / OpenSSL License (OpenSSL 1.x)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or discussions
- GitHub Issues https://github.com/tidesdb/tidesdb-go/issues
- Discord Community https://discord.gg/tWEmjR66cy
- Main TidesDB Repository https://github.com/tidesdb/tidesdb

## Links

- [TidesDB Main Repository](https://github.com/tidesdb/tidesdb)
- [TidesDB Documentation](https://github.com/tidesdb/tidesdb#readme)
- [Other Language Bindings](https://github.com/tidesdb/tidesdb#bindings)