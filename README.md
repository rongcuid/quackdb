# QuackDB

Current status: core functionality (connect, query, and retrieve results via Arrow) is usable. Anything more advanced requires you to access the internal, unsafe API, or access FFI directly.

## Roadmap -- 0.2.0

* [ ] Appender
* [ ] Table functions
* [ ] Replacement scans
* [ ] Proper string type: UTF-8, non-null terminated
* [ ] Clean up error types

## Roadmap -- 0.3.0

* [ ] Row-based iterator
* [ ] Primitive type conversion
* [ ] JSON/extended types
* [ ] Polars support (optional)

## API Level

The main API is the high level, safe API.

`quackdb-internal` exposes low-level wrapper over types from `libduckdb-sys` with Rust types, but without checks.

Raw handles can be used with FFI functions for lowest-level interaction.