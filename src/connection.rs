use std::{
    ffi::{CStr, CString},
    ptr,
    sync::Arc,
};

use crate::{database::Database, error::*, ffi, query::QueryResult};

#[derive(Debug, Clone)]
pub struct Connection {
    pub(crate) handle: Arc<ConnectionHandle>,
    pub(crate) _db: Database,
}

#[derive(Debug)]
pub(crate) struct ConnectionHandle(pub(crate) ffi::duckdb_connection);

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("duckdb_connect() error")]
    ConnectError,
    #[error("duckdb_query() error: {0}")]
    QueryError(String),
}

impl Connection {
    pub fn connect(database: &Database) -> DbResult<Self, ConnectionError> {
        let mut handle = ptr::null_mut();
        unsafe {
            let r = ffi::duckdb_connect(database.handle.0, &mut handle);
            if r != ffi::DuckDBSuccess {
                return Ok(Err(ConnectionError::ConnectError));
            }
        }
        Ok(Ok(Connection {
            handle: Arc::new(ConnectionHandle(handle)),
            _db: database.clone(),
        }))
    }

    pub fn interrupt(self: &Arc<Self>) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }

    pub fn query_progress(self: &Arc<Self>) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }

    pub fn query(&self, sql: &str) -> DbResult<QueryResult, ConnectionError> {
        let cstr = CString::new(sql)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            let r = ffi::duckdb_query(self.handle.0, p, &mut result);
            if r != ffi::DuckDBSuccess {
                let err = ffi::duckdb_result_error(&mut result);
                let err = Ok(Err(ConnectionError::QueryError(
                    CStr::from_ptr(err).to_string_lossy().to_string(),
                )));
                ffi::duckdb_destroy_result(&mut result);
                return err;
            }
            let result = QueryResult::from_raw_connection(result, self.clone());
            Ok(Ok(result))
        }
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_disconnect(&mut self.0);
        }
    }
}
