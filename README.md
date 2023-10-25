# QuackDB

A [DuckDB](https://duckdb.org/) API with ergonomic high-level API without sacrificing the ability to go low level.

## Compared to `duckdb-rs`

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

### API conventions

* High level API expose low level handle types as `pub handle` field
* High level API follow Rust naming
* Low level API wraps raw handle and all basic operations
* Low level API dereferences to raw handle
* Low level API use Rust types
* Low level API follow DuckDB naming

## Roadmap -- 0.3.0

* [x] Table functions (wrapper)
* [x] Table functions (almost high level)
* [ ] ~~Row-based iterator~~

## Roadmap -- 0.4.0
* [ ] Fix document build
* [ ] Replacement scan
* [ ] Ergonomic types

## Roadmap -- Future

* [ ] Clean up error types
* [ ] Clean up `&self` and `&mut self` receivers
* [ ] Documentation
* [ ] Comprehensive tests
* [ ] Replacement scans
* [ ] Streaming arrow (optional)
* [ ] Polars support (optional)

