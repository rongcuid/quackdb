use std::{ffi::CStr, ops::Deref, sync::Arc};

use crate::{
    arrow::ArrowResultHandle, database::DatabaseHandle, ffi, statement::PreparedStatementHandle,
    table_function::TableFunctionHandle,
};

#[derive(Debug)]
pub struct ConnectionHandle {
    handle: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
    _table_functions: Vec<Arc<TableFunctionHandle>>,
}

impl ConnectionHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_connection, parent: Arc<DatabaseHandle>) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
            _table_functions: vec![],
        })
    }

    /// # Safety
    /// * Make sure any child objects are closed
    /// * Do not use this object afterwards
    pub unsafe fn disconnect(&mut self) {
        ffi::duckdb_disconnect(&mut self.handle);
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
        unsafe {
            self.disconnect();
        }
    }
}
