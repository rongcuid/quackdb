use std::{ops::Deref, sync::Arc};

use crate::ffi;

use super::DatabaseHandle;

#[derive(Debug)]
pub struct ConnectionHandle {
    handle: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
    // _table_functions: Vec<Arc<TableFunctionHandle>>,
}

impl ConnectionHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_connection, parent: Arc<DatabaseHandle>) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
            // _table_functions: vec![],
        })
    }
}

impl Deref for ConnectionHandle {
    type Target = ffi::duckdb_connection;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_disconnect(&mut self.handle) }
    }
}
