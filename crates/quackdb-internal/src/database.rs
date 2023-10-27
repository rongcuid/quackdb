use std::{ops::Deref, ptr, sync::Arc};

use crate::{connection::ConnectionHandle, ffi};

#[derive(Debug)]
pub struct DatabaseHandle(ffi::duckdb_database);

impl DatabaseHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_database) -> Arc<Self> {
        Arc::new(Self(raw))
    }

    /// # Safety
    /// * Ensure all children are closed
    /// * Don't use this object afterwards
    pub unsafe fn close(&mut self) {
        ffi::duckdb_close(&mut self.0);
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        unsafe { self.close() }
    }
}

impl Deref for DatabaseHandle {
    type Target = ffi::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
