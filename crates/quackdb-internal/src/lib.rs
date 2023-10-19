use std::ffi::CStr;

pub mod ffi;

pub mod arrow;
pub mod config;
pub mod connection;
pub mod database;
// pub mod query_result;
pub mod statement;
pub mod types;
pub mod value;

pub fn library_version() -> String {
    unsafe {
        let p = CStr::from_ptr(ffi::duckdb_library_version());
        p.to_string_lossy().to_owned().to_string()
    }
}
