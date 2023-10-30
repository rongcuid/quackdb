# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- High level table function register
- Arrow streaming interface
- Top level error type

### Changed
- `TypeId` uses `strum::FromRepr`
- Removed C path utils
- Changed some naming convention
- Removed all `NulError` from error types
- `error()` functions are now unsafe

### Removed
- Cleaned up unused code
- Removed some basic tests

## [0.4.0] - 2023-10-27

### Added
- `ToDuckDb` types
- `chrono` support
- Minimal replacement scan support

### Changed
- Internal bind/append API now uses duckdb types directly

### Removed
- `time` support
- Intermediate API

## [0.3.1] - 2023-10-25
- Documentation build fix
- New feature flag: `bundled`

## [0.3.0] - 2023-10-25

- Basic table function API
  - Low level table function API
  - High level table function API
  - Known issue: still requires FFI access to data chunk API

## [0.2.0] - 2023-10-24

- Appender API
- Ensured string types are correct
- Documented a little about the logic behind API design

## [0.1.0] - 2023-10-20

- Core functionality
- Low level API
- High level connection, query, and arrow results