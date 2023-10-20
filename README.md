# QuackDB

Current status: core functionality (connect, query, and retrieve results via Arrow) is usable. Anything more advanced requires you to access the internal, unsafe API, or access FFI directly.

## TODO

* [ ] Proper string type for duckdb: UTF-8, non-null terminated
* [ ] Clean up error types

## Core v0.1

* [x] Move handles to an internal module
* [x] Make sure internal module do not reference higher level types
* [x] Refactor for granular error types
* [x] Implement and document low level interface
  * [x] Database
  * [x] Connection
  * [x] Query results
  * [x] Statements
  * [x] Values
* [x] Implement high level interface
  * [x] Database
  * [x] Connection
  * [x] Query results
  ** [x] RecordBatch
  ** [x] Result batch mapping
  * [x] Statements

## API Level

The main API is the high level, safe API.

`quackdb-internal` exposes low-level wrapper over types from `libduckdb-sys` with Rust types, but without checks.

Raw handles can be used with FFI functions for lowest-level interaction.