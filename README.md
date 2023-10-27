# QuackDB

A [DuckDB](https://duckdb.org/) API with ergonomic high-level API without sacrificing the ability to go low level.

## Compared to [duckdb-rs](https://github.com/duckdb/duckdb-rs)

* QuackDB avoids lifetime on API where possible
  * Database objects are reference-counted
  * This avoids the need to store parent objects explicitly
* QuackDB does not attempt to mimic [Rusqlite](https://github.com/rusqlite/rusqlite) API
* QuackDB data access and processing is centered around Arrow

## API Level

The main API is the high level, safe API.

`quackdb-internal` exposes low-level wrapper over types from `libduckdb-sys` with Rust types.
This API can be accessed by the public `handle` field of high-level structures.

Raw FFI handles can be accessed by dereferencing low level structures and used with `libduckdb-sys` functions.

### API Support

| C API             | High Level | Low Level |
| ----------------- | ---------- | --------- |
| Database          | Yes        | Yes       |
| Connection        | Yes        | Yes       |
| Config            | Yes        | Yes       |
| Query             | Arrow      | Yes       |
| Data Chunks       | No         | No        |
| Values            | No         | No        |
| Types             | Partial    | Yes       |
| Statements        | Yes        | Yes       |
| Appender          | Yes        | Yes       |
| Table Functions   | Almost     | Yes       |
| Replacement Scans | No         | No        |

* API which doesn't have low level support can still be used via `quackdb-internal::ffi` module
* Query results require working with [arrow](https://docs.rs/arrow/latest/arrow/) `RecordBatch` directly
* Table functions are supported, but it has to access data chunks directly via FFI
* Currently, DuckDB types, Arrow types, and Rust types are not reconciled

### API conventions

* High level API expose low level handle types as `pub handle` field
* High level API follow Rust naming
* Low level API wraps raw handle and all basic operations
* Low level API dereferences to raw handle
* Low level API use Rust types
* Low level API follow DuckDB naming

## Roadmap -- 0.4.0
* [x] Fix document build
* [x] DuckDb types conversion
  * [x] Rust primitive types to/from DuckDb types
  * [x] Chrono to/from DuckDb

## Roadmap -- 0.5.0
* [ ] Try to remove intermediate API?
* [ ] Serde support
* [ ] Replacement scan

## Roadmap -- Future

* [ ] Clean up error types
* [ ] Clean up `&self` and `&mut self` receivers
* [ ] Make naming consistent
* [ ] Documentation
* [ ] Comprehensive tests
* [ ] Replacement scans
* [ ] Streaming arrow (optional)
* [ ] Polars support (optional)

