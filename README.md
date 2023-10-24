# QuackDB

Current status: core functionality (connect, query, and retrieve results via Arrow) is usable. Anything more advanced requires you to access the internal, unsafe API, or access FFI directly.

## Roadmap -- 0.2.0

* [x] Appender
* [x] Proper string type: UTF-8, non-null terminated

## Roadmap -- 0.3.0

* [ ] Table functions
* [ ] Replacement scans


## Roadmap -- 0.4.0

* [ ] Row-based iterator
* [ ] Ergonomic types

## Roadmap (Milestone) -- 0.5.0

* [ ] Clean up error types
* [ ] Clean up `&self` and `&mut self` receivers
* [ ] Streaming arrow (optional)
* [ ] Polars support (optional)

## API Level

The main API is the high level, safe API.

`quackdb-internal` exposes low-level wrapper over types from `libduckdb-sys` with Rust types, but without checks.

Raw handles can be used with FFI functions for lowest-level interaction.

### API conventions

* High level API expose low level handle types as pub field
* High level API follow Rust naming
* Low level API wraps raw handle and all basic operations
* Low level API dereferences to raw handle
* Low level API use Rust types
* Low level API follow DuckDB naming