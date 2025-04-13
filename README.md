# tidesdb-go
tidesdb-go is the official GO binding for TidesDB.

## Getting started
You must make sure you have the TidesDB shared C library installed on your system.  Be sure to also compile with `TIDESDB_WITH_SANITIZER` and `TIDESDB_BUILD_TESTS` OFF.

### Installation
```bash
go get github.com/tidesdb/tidesdb-go
```

### Usage

#### Opening and closing a database
```go
db, err := tidesdb_go.Open("/path/to/db") // will reopen the database if it already exists
if err != nil {
...
}
defer db.Close()
```

#### Creating and dropping a column family
Column families are used to store data in TidesDB. You can create a column family using the `CreateColumnFamily` method.
```go
err := db.CreateColumnFamily("example_cf", 1024*1024*64, 12, 0.24, true, int(tidesdb_go.TDB_COMPRESS_SNAPPY), true)
if err != nil {
...
}

// You can also drop a column family using the `DropColumnFamily` method.
err = db.DropColumnFamily("example_cf")
if err != nil {
...
}
```

#### CRUD operations

##### Writing data
```go
err := db.Put("example_cf", []byte("key"), []byte("value"), -1)
if err != nil {
...
}
```

With TTL
```go
err := db.Put("example_cf", []byte("key"), []byte("value"), time.Now().Add(10*time.Second).Unix())
if err != nil {
...
}
```

##### Reading data
```go
value, err := db.Get("example_cf", []byte("key"))
if err != nil {
...
}
fmt.Println(string(value))
```


##### Deleting data
```go
err := db.Delete("example_cf", []byte("key"))
if err != nil {
...
}
```

##### Deleting by range
```go
// Delete all key-value pairs between "start_key" and "end_key" atomically
err := db.DeleteByRange("example_cf", []byte("start_key"), []byte("end_key"))
if err != nil {
...
}
```

#### Iterating over data
```go
cursor, err := db.CursorInit("example_cf")
if err != nil {
...
}
defer cursor.Free()

for {
key, value, err := cursor.Get()
if err != nil {
    break
}
fmt.Printf("Key: %s, Value: %s\n", key, value)

cursor.Next() // or cursor.Prev()
}
```

#### Merge Cursor
The merge cursor provides a unified view across both memtable and all SSTables, ensuring proper sorting order.

```go
// Initialize a merge cursor
mergeCursor, err := db.MergeCursorInit("example_cf")
if err != nil {
    // Handle error
}

defer mergeCursor.Free()

// Iterate forward through all sorted key-value pairs
for {
    key, value, err := mergeCursor.Get()
    if err != nil {
        break // End of data or error
    }

    fmt.Printf("Key: %s, Value: %s\n", key, value)

    // Move to next entry
    if err := mergeCursor.Next(); err != nil {
        break
    }
}

// Or iterate backward
mergeCursor, err = db.MergeCursorInit("example_cf")
if err != nil {
    // Handle error
}

defer mergeCursor.Free()

for {
    key, value, err := mergeCursor.Get()
    if err != nil {
        break
    }

    fmt.Printf("Key: %s, Value: %s\n", key, value)

    if err := mergeCursor.Prev(); err != nil {
        break
    }
}


```

#### Transactions
```go
txn, err := db.BeginTxn("example_cf")
if err != nil {
...
}
defer txn.Free()

err = txn.Put([]byte("key"), []byte("value"), 0)
if err != nil {
...
}

// You can also use txn.Delete(), txn.Get()

err = txn.Commit()
if err != nil {
...
}
```

#### Range queries
```go
// Get all key-value pairs between "start_key" and "end_key"
pairs, err := db.Range("example_cf", []byte("start_key"), []byte("end_key"))
if err != nil {
...
}

for _, pair := range pairs {
    key := pair[0]
    value := pair[1]
    fmt.Printf("Key: %s, Value: %s\n", key, value)
}
```

#### Getting Column Family Statistics
You can retrieve detailed statistics about a column family using the `GetColumnFamilyStat` method.

```go
stat, err := db.GetColumnFamilyStat("example_cf")
if err != nil {
    ...
}

// Access various statistics about the column family
fmt.Printf("Column Family: %s\n", stat.Name)
fmt.Printf("Memtable Size: %d bytes\n", stat.MemtableSize)
fmt.Printf("Memtable Entries: %d\n", stat.MemtableEntryCount)
fmt.Printf("Number of SSTables: %d\n", stat.NumSSTables)
fmt.Printf("Incremental Merging: %t\n", stat.IncrementalMerging)

// Access configuration details
fmt.Printf("Flush Threshold: %d\n", stat.Config.FlushThreshold)
fmt.Printf("Max Level: %d\n", stat.Config.MaxLevel)
fmt.Printf("Probability: %f\n", stat.Config.Probability)
fmt.Printf("Compressed: %t\n", stat.Config.Compressed)
fmt.Printf("Bloom Filter: %t\n", stat.Config.BloomFilter)

// Access SSTable statistics
for i, sstStat := range stat.SSTableStats {
    fmt.Printf("SSTable %d - Path: %s, Size: %d bytes, Blocks: %d\n",
    i, sstStat.Path, sstStat.Size, sstStat.NumBlocks)
}
```

#### Listing Column Families
You can retrieve a list of all column families.
```go
cfList, err := db.ListColumnFamilies()
if err != nil {
...
}

fmt.Println("Available column families:", cfList)
```

#### Compaction
Compaction is done manually or in background incrementally.

Merging operations, pair and merge sstables, removing expired keys if TTL set and tombstoned data.

##### Manual
```go
err := db.CompactSSTables("example_cf", 4) // 4 is the number of threads to use for compaction. Each thread will compact a pair of sstables.
if err != nil {
    ...
}
```

##### Background
```go
err := db.StartIncrementalMerge("example_cf", 60, 1000) // merge a pair of sstables starting at oldest pair every 60 seconds only when we have a minimum of 1000 sstables
if err != nil {
...
}
```