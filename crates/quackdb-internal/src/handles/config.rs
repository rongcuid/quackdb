use std::ops::Deref;

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
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_config) -> Self {
        Self(raw)
    }
}

impl Drop for ConfigHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_config(&mut self.0) };
    }
}
