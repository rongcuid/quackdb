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
    pub fn query(self: &Arc<Self>, sql: &CStr) -> Result<ArrowResultHandle, String> {
        unsafe {
            let mut result: ffi::duckdb_arrow = std::mem::zeroed();
            let r = ffi::duckdb_query_arrow(self.handle, sql.as_ptr(), &mut result);
            let h = ArrowResultHandle::from_raw_connection(result, self.clone());
            if r != ffi::DuckDBSuccess {
                return Err(h.error());
            }
            Ok(h)
        }
    }
    pub fn prepare(self: &Arc<Self>, query: &CStr) -> Result<Arc<PreparedStatementHandle>, String> {
        unsafe {
            let mut prepare: ffi::duckdb_prepared_statement = std::mem::zeroed();
            let res = ffi::duckdb_prepare(self.handle, query.as_ptr(), &mut prepare);
            if res != ffi::DuckDBSuccess {
                let err = ffi::duckdb_prepare_error(prepare);
                let err = CStr::from_ptr(err).to_string_lossy().to_owned().to_string();
                ffi::duckdb_destroy_prepare(&mut prepare);
                return Err(err);
            }
            Ok(PreparedStatementHandle::from_raw(prepare, self.clone()))
        }
    }
    pub fn register_table_function(
        &mut self,
        function: Arc<TableFunctionHandle>,
    ) -> Result<(), ()> {
        let r = unsafe { ffi::duckdb_register_table_function(**self, **function) };
        if r != ffi::DuckDBSuccess {
            return Err(());
        }
        self._table_functions.push(function);
        Ok(())
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
