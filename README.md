# QuackDB

Current status: core functionality (connect, query, and retrieve results via Arrow) is usable. Anything more advanced requires you to access the internal, unsafe API, or access FFI directly.

## Roadmap -- 0.2.0

* [ ] Appender
* [ ] Proper string type: UTF-8, non-null terminated

## Roadmap -- 0.3.0

* [ ] Table functions
* [ ] Replacement scans


## Roadmap -- 0.4.0

* [ ] Row-based iterator
* [ ] Ergonomic types

## Roadmap (Milestone) -- 0.5.0

* [ ] Clean up error types
* [ ] Streaming arrow (optional)
* [ ] Polars support (optional)

## API Level

The main API is the high level, safe API.

`quackdb-internal` exposes low-level wrapper over types from `libduckdb-sys` with Rust types, but without checks.

Raw handles can be used with FFI functions for lowest-level interaction.