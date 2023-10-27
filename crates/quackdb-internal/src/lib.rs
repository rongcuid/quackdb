use std::ffi::CStr;

pub mod ffi;

pub mod appender;
pub mod arrow;
pub mod config;
pub mod connection;
pub mod conversion;
pub mod database;
pub mod statement;
pub mod table_function;
pub mod types;

pub fn library_version() -> String {
    unsafe {
        let p = CStr::from_ptr(ffi::duckdb_library_version());
        p.to_string_lossy().to_owned().to_string()
    }
}
