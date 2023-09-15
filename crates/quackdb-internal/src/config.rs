use std::{ffi::CStr, ops::Deref, ptr};

use crate::ffi;

#[derive(Debug)]
pub struct ConfigHandle(ffi::duckdb_config);

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
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_config) -> Self {
        Self(raw)
    }
    pub fn set(&self, key: &CStr, value: &CStr) -> Result<(), ()> {
        let state = unsafe { ffi::duckdb_set_config(self.0, key.as_ptr(), value.as_ptr()) };
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
