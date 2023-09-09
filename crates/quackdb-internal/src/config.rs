use std::{
    ffi::{c_char, CString},
    ops::Deref,
    ptr,
};

use cstr::cstr;

use crate::ffi;

/// duckdb configuration
/// Refer to https://github.com/duckdb/duckdb/blob/master/src/main/config.cpp
/// Adapted from `duckdb-rs` crate
/// TODO: support everything in the API
#[derive(Debug)]
pub struct ConfigHandle(ffi::duckdb_config);

impl Default for ConfigHandle {
    fn default() -> Self {
        Self(ptr::null_mut())
    }
}

impl Deref for ConfigHandle {
    type Target = ffi::duckdb_config;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ConfigHandle {
    pub fn create() -> Result<Self, ()> {
        unsafe {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            if ffi::duckdb_create_config(&mut config) != ffi::DuckDBSuccess {
                return Err(());
            }
            Ok(Self::from_raw(config))
        }
    }
    pub unsafe fn from_raw(raw: ffi::duckdb_config) -> Self {
        Self(raw)
    }

    pub unsafe fn set(&mut self, key: *const c_char, value: *const c_char) -> Result<(), ()> {
        let state = unsafe { ffi::duckdb_set_config(self.0, key, value) };
        if state != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
}

impl Drop for ConfigHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_config(&mut self.0) };
    }
}
