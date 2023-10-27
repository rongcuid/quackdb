use std::ffi::CStr;

pub mod ffi;

// pub mod types;
pub mod conversion;
pub mod handles;
pub mod type_id;

pub fn library_version() -> String {
    unsafe {
        let p = CStr::from_ptr(ffi::duckdb_library_version());
        p.to_string_lossy().to_owned().to_string()
    }
}
