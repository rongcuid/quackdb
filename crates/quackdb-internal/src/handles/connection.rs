use std::{ops::Deref, sync::Arc};

use crate::ffi;

use super::DatabaseHandle;

#[derive(Debug)]
pub struct ConnectionHandle {
    raw: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
}

pub struct ConnectionHandleError;

impl ConnectionHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_connection, parent: Arc<DatabaseHandle>) -> Arc<Self> {
        Arc::new(Self {
            raw,
            _parent: parent,
        })
    }
}

impl Deref for ConnectionHandle {
    type Target = ffi::duckdb_connection;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_disconnect(&mut self.raw) }
    }
}
