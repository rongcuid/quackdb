use std::{
    ffi::{c_char, CStr},
    ops::Deref,
    sync::Arc,
};

use crate::{
    database::DatabaseHandle, ffi, result::QueryResultHandle, statement::PreparedStatementHandle,
};

#[derive(Debug)]
pub struct ConnectionHandle {
    handle: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
}

impl ConnectionHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_connection, parent: Arc<DatabaseHandle>) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
        })
    }
    /// # Safety
    /// `sql` must be null-terminated
    pub unsafe fn query(
        self: &Arc<Self>,
        sql: *const c_char,
    ) -> Result<Arc<QueryResultHandle>, String> {
        let mut result: ffi::duckdb_result = std::mem::zeroed();
        let r = ffi::duckdb_query(self.handle, sql, &mut result);
        let h = QueryResultHandle::from_raw_connection(result, self.clone());
        if r != ffi::DuckDBSuccess {
            return Err(h.error());
        }
        Ok(h)
    }
    /// # Safety
    /// `query` must be null-terminated
    pub unsafe fn prepare(
        self: &Arc<Self>,
        query: *const c_char,
    ) -> Result<Arc<PreparedStatementHandle>, String> {
        let mut prepare: ffi::duckdb_prepared_statement = std::mem::zeroed();
        let res = ffi::duckdb_prepare(self.handle, query, &mut prepare);
        if res != ffi::DuckDBSuccess {
            let err = ffi::duckdb_prepare_error(prepare);
            let err = CStr::from_ptr(err).to_string_lossy().to_owned().to_string();
            ffi::duckdb_destroy_prepare(&mut prepare);
            return Err(err);
        }
        Ok(PreparedStatementHandle::from_raw(prepare, self.clone()))
    }
    /// # Safety
    /// Disconnects without checking usage.
    /// Normally you should let Rust automatically manage this.
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
