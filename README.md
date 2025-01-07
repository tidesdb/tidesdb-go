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
err := db.CreateColumnFamily("example_cf", 1024*1024*64, 12, 0.24, true, int(tidesdb_go.TDB_COMPRESS_SNAPPY), true, int(tidesdb_go.TDB_MEMTABLE_SKIP_LIST))
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

// You can also do txn.Delete()

err = txn.Commit()
if err != nil {
...
}
```

#### Compaction
Compaction is done manually or in background.

Merging operations, pair and merge sstables, removing expired keys if TTL set and tombstoned data; Say you have 100 sstables these methods will compact to 50 sstables.

##### Manual
```go
err := db.CompactSSTables("example_cf", 4) // 4 is the number of threads to use for compaction
if err != nil {
    ...
}
```

##### Background
```go
err := db.StartBackgroundPartialMerge("example_cf", 60, 1000) // merge a pair every 60 seconds only when we have a minimum of 1000 sstables
if err != nil {
...
}
```