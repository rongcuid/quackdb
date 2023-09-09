use std::{
    ffi::{CStr, CString},
    ops::Deref,
    ptr,
    sync::Arc,
};

use crate::{
    database::{Database, DatabaseHandle},
    error::*,
    ffi,
    query::{QueryResult, QueryResultHandle},
    statement::{PreparedStatement, PreparedStatementHandle},
};

#[derive(Debug)]
pub struct Connection {
    pub handle: Arc<ConnectionHandle>,
}

#[derive(Debug)]
pub struct ConnectionHandle {
    handle: ffi::duckdb_connection,
    _parent: Arc<DatabaseHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("duckdb_connect() error")]
    ConnectError,
    #[error("duckdb_query() error: {0}")]
    QueryError(String),
    #[error("duckdb_prepare() error: {0}")]
    PrepareError(String),
}

impl From<Arc<ConnectionHandle>> for Connection {
    fn from(value: Arc<ConnectionHandle>) -> Self {
        Self { handle: value }
    }
}

impl Connection {
    pub fn connect(database: &Database) -> DbResult<Connection, ConnectionError> {
        let mut handle = ptr::null_mut();
        unsafe {
            let r = ffi::duckdb_connect(**database.handle, &mut handle);
            if r != ffi::DuckDBSuccess {
                return Ok(Err(ConnectionError::ConnectError));
            }
        }
        Ok(Ok(Connection {
            handle: Arc::new(ConnectionHandle {
                handle,
                _parent: database.handle.clone(),
            }),
        }))
    }

    // pub fn interrupt(&self) {
    //     unsafe { unimplemented!("Not in libduckdb-sys yet") }
    // }

    // pub fn query_progress(&self) {
    //     unsafe { unimplemented!("Not in libduckdb-sys yet") }
    // }

    pub fn query(&self, sql: &str) -> DbResult<QueryResult, ConnectionError> {
        let cstr = CString::new(sql)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            let r = ffi::duckdb_query(**self.handle, p, &mut result);
            if r != ffi::DuckDBSuccess {
                let err = ffi::duckdb_result_error(&mut result);
                let err = Ok(Err(ConnectionError::QueryError(
                    CStr::from_ptr(err).to_string_lossy().to_string(),
                )));
                ffi::duckdb_destroy_result(&mut result);
                return err;
            }
            let result = QueryResultHandle::from_raw_connection(result, self.handle.clone()).into();
            Ok(Ok(result))
        }
    }

    pub fn prepare(&self, query: &str) -> DbResult<PreparedStatement, ConnectionError> {
        let cstr = CString::new(query)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut prepare: ffi::duckdb_prepared_statement = std::mem::zeroed();
            let res = ffi::duckdb_prepare(**self.handle, p, &mut prepare);
            if res != ffi::DuckDBSuccess {
                let err = ffi::duckdb_prepare_error(prepare);
                let err = Ok(Err(ConnectionError::PrepareError(
                    CStr::from_ptr(err).to_string_lossy().to_string(),
                )));
                ffi::duckdb_destroy_prepare(&mut prepare);
                return err;
            }
            Ok(Ok(PreparedStatementHandle::from_raw(
                prepare,
                self.handle.clone(),
            )
            .into()))
        }
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
            ffi::duckdb_disconnect(&mut self.handle);
        }
    }
}
