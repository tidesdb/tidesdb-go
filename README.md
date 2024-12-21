# tidesdb-go
tidesdb-go is the official GO binding for TidesDB.

## Getting started
You must make sure you have the TidesDB shared C library installed on your system.

### Installation
```bash
go get github.com/tidesdb/tidesdb
```

### Usage

#### Opening and closing a database
```go
db, err := tidesdb_go.Open("/path/to/db") // will reopen the database if it already exists
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

#### Creating and dropping a column family
Column families are used to store data in TidesDB. You can create a column family using the `CreateColumnFamily` method.
```go
err := db.CreateColumnFamily("example_cf", 1000, 7, 0.1, true, tidesdb_go.TDB_COMPRESS_SNAPPY, true, tidesdb_go.TDB_MEMTABLE_SKIP_LIST)
if err != nil {
    log.Fatal(err)
}

// You can also drop a column family using the `DropColumnFamily` method.
err = db.DropColumnFamily("example_cf")
if err != nil {
    log.Fatal(err)
}
```

#### CRUD operations

##### Writing data
```go
err := db.Put("example_cf", []byte("key"), []byte("value"), -1)
if err != nil {
    log.Fatal(err)
}
```

With TTL
```go
err := db.Put("example_cf", []byte("key"), []byte("value"), time.Now().Add(10*time.Second).Unix())
if err != nil {
    log.Fatal(err)
}
```

##### Reading data
```go
value, err := db.Get("example_cf", []byte("key"))
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(value))
```


##### Deleting data
```go
err := db.Delete("example_cf", []byte("key"))
if err != nil {
    log.Fatal(err)
}
```


#### Iterating over data
```go
cursor, err := db.CursorInit("example_cf")
if err != nil {
    log.Fatal(err)
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
    log.Fatal(err)
}
defer txn.Free()

err = txn.Put([]byte("key"), []byte("value"), 0)
if err != nil {
    log.Fatal(err)
}

// You can also do txn.Delete()

err = txn.Commit()
if err != nil {
    log.Fatal(err)
}
```

#### Compaction
Compaction is done manually.  Say you have 100 sstables this method will compact to 50 sstables. Pairing, merging, and removing expired and tombstoned data.
```go
err := db.CompactSSTables("example_cf", 4) // 4 is the number of threads to use for compaction
if err != nil {
    log.Fatal(err)
}
```

