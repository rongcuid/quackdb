use std::{
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::ffi;

use super::{DatabaseHandle, TableFunctionHandle};

#[derive(Debug)]
pub struct ConnectionHandle {
    raw: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
    table_functions: Mutex<Vec<Arc<TableFunctionHandle>>>,
}

pub struct ConnectionHandleError;

impl ConnectionHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_connection, parent: Arc<DatabaseHandle>) -> Arc<Self> {
        Arc::new(Self {
            raw,
            _parent: parent,
            table_functions: Mutex::new(vec![]),
        })
    }
    pub fn register_table_function(
        &self,
        function: Arc<TableFunctionHandle>,
    ) -> Result<(), ConnectionHandleError> {
        let r = unsafe { ffi::duckdb_register_table_function(self.raw, **function) };
        match r {
            ffi::DuckDBSuccess => {
                self.table_functions.lock().unwrap().push(function);
                Ok(())
            }
            ffi::DuckDBError => Err(ConnectionHandleError),
            _ => unreachable!(),
        }
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
