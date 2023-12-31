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

`quackdb-internal` contains wrappers over types from `libduckdb-sys` with Rust types.
These types are only used for object lifetime management.
Raw FFI handles can be accessed by dereferencing either high level or wrapper structures.

### API Support

| C API             | High Level |
| ----------------- | ---------- |
| Database          | Yes        |
| Connection        | Yes        |
| Config            | Yes        |
| Query             | Arrow      |
| Data Chunks       | No         |
| Values            | No         |
| Types             | Partial    |
| Statements        | Yes        |
| Appender          | Yes        |
| Table Functions   | Almost     |
| Replacement Scans | Yes        |

* Query results require working with [arrow](https://docs.rs/arrow/latest/arrow/) `RecordBatch` directly
* Table functions are supported, but it has to work with data chunks directly via FFI
* Currently, DuckDB types, Arrow types, and Rust types are not fully reconciled

### API conventions

* High level API expose low level handle types as `pub handle` field
* High level API follow Rust naming
* Low level API wraps raw handle and all basic operations
* Low level API dereferences to raw handle
* Low level API use Rust types
* Low level API follow DuckDB naming

## Roadmap 0.6

* [x] Precise error checking in arrow streaming interface
* [x] Decimal type (bigdecimal)
* [x] Clean up handlers
* [x] Unify replacement scan and table function
* [ ] Registering arrow data
* [ ] Remove intermediate handles that does not need `Drop` implementations
* [ ] Medium-rare interface (CStr, Arrow FFI, etc)
* [ ] Prelude module

## Roadmap


* [ ] Extracted statements
* [ ] Table function trait
* [ ] Replacement scan trait
* [ ] Clean up receivers
* [ ] Clean up panics
* [ ] Serde support
* [ ] Data chunk support
* [ ] Comprehensive documentation
* [ ] Comprehensive tests

