use std::{
    ffi::{c_char, CString},
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

impl ConfigHandle {
    /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
    pub unsafe fn access_mode(&mut self, mode: *const c_char) -> Result<&mut Self, ()> {
        self.set(cstr!("access_mode").as_ptr(), mode)?;
        Ok(self)
    }

    /// The order type used when none is specified ([ASC] or DESC)
    pub unsafe fn default_order(&mut self, order: *const c_char) -> Result<&mut Self, ()> {
        self.set(cstr!("default_order").as_ptr(), order)?;
        Ok(self)
    }

    /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
    pub unsafe fn default_null_order(
        &mut self,
        null_order: *const c_char,
    ) -> Result<&mut Self, ()> {
        self.set(cstr!("default_null_order").as_ptr(), null_order)?;
        Ok(self)
    }

    /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
    pub fn enable_external_access(&mut self, enabled: bool) -> Result<&mut Self, ()> {
        unsafe {
            self.set(
                cstr!("enable_external_access").as_ptr(),
                if enabled {
                    cstr!("true").as_ptr()
                } else {
                    cstr!("false").as_ptr()
                },
            )?;
        }
        Ok(self)
    }

    /// Whether or not object cache is used to cache e.g. Parquet metadata
    pub fn enable_object_cache(&mut self, enabled: bool) -> Result<&mut Self, ()> {
        unsafe {
            self.set(
                cstr!("enable_object_cache").as_ptr(),
                if enabled {
                    cstr!("true").as_ptr()
                } else {
                    cstr!("false").as_ptr()
                },
            )?;
        }
        Ok(self)
    }

    /// Allow to load third-party duckdb extensions.
    pub fn allow_unsigned_extensions(&mut self) -> Result<&mut Self, ()> {
        unsafe {
            self.set(
                cstr!("allow_unsigned_extensions").as_ptr(),
                cstr!("true").as_ptr(),
            )?;
        }
        Ok(self)
    }

    /// The maximum memory of the system (e.g. 1GB)
    pub unsafe fn max_memory(&mut self, memory: *const c_char) -> Result<&mut Self, ()> {
        self.set(cstr!("max_memory").as_ptr(), memory)?;
        Ok(self)
    }

    /// The number of total threads used by the system
    pub fn threads(&mut self, thread_num: i64) -> Result<&mut Self, ()> {
        let cstr = CString::new(thread_num.to_string()).expect("integer to cstring");
        unsafe {
            self.set(cstr!("threads").as_ptr(), cstr.as_ptr())?;
        }
        Ok(self)
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
