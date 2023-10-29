use std::{ops::Deref, sync::Arc};

use crate::ffi;

#[derive(Debug)]
pub struct DatabaseHandle {
    raw: ffi::duckdb_database,
}

impl DatabaseHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_database) -> Arc<Self> {
        Arc::new(Self { raw })
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_close(&mut self.raw) }
    }
}

impl Deref for DatabaseHandle {
    type Target = ffi::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}
