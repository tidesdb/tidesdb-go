# tidesdb-go

tidesdb-go is the official GO binding for TidesDB.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This GO binding provides a safe, idiomatic GO interface to TidesDB with full support for all features.

## Features

- Full ACID transaction support with savepoints
- MVCC with five isolation levels from READ UNCOMMITTED to SERIALIZABLE
- Column families aka isolated key-value stores with independent configuration
- Bidirectional iterators with forward/backward traversal with seek support
- TTL(time to live) support with automatic key expiration and internal garbage collection
- LZ4, LZ4 Fast, ZSTD, Snappy, or no compression
- Bloom filters with configurable false positive rates
- Global block CLOCK cache for hot blocks
- Automatic with configurable thread pools (sorted runs, compaction)
- Six built-in comparators plus custom registration

For GO usage you can go to the TidesDB GO Reference [here](https://tidesdb.com/reference/go/).

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