use std::{
    ffi::{CStr, CString},
    ops::Deref,
    ptr,
    sync::Arc,
};

use crate::{database::Database, error::*, ffi, query::QueryResult, statement::PreparedStatement};

#[derive(Debug)]
pub struct Connection {
    pub(crate) handle: ConnectionHandle,
    pub(crate) _db: Arc<Database>,
}

#[derive(Debug)]
pub struct ConnectionHandle(ffi::duckdb_connection);

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("duckdb_connect() error")]
    ConnectError,
    #[error("duckdb_query() error: {0}")]
    QueryError(String),
    #[error("duckdb_prepare() error: {0}")]
    PrepareError(String),
}

impl Connection {
    pub fn connect(database: Arc<Database>) -> DbResult<Arc<Connection>, ConnectionError> {
        let mut handle = ptr::null_mut();
        unsafe {
            let r = ffi::duckdb_connect(*database.handle, &mut handle);
            if r != ffi::DuckDBSuccess {
                return Ok(Err(ConnectionError::ConnectError));
            }
        }
        Ok(Ok(Arc::new(Connection {
            handle: ConnectionHandle(handle),
            _db: database,
        })))
    }

    pub fn interrupt(&self) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }

    pub fn query_progress(&self) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }

    pub fn query(self: &Arc<Self>, sql: &str) -> DbResult<Arc<QueryResult>, ConnectionError> {
        let cstr = CString::new(sql)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            let r = ffi::duckdb_query(*self.handle, p, &mut result);
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

    pub fn prepare(
        self: &Arc<Self>,
        query: &str,
    ) -> DbResult<Arc<PreparedStatement>, ConnectionError> {
        let cstr = CString::new(query)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut prepare: ffi::duckdb_prepared_statement = std::mem::zeroed();
            let res = ffi::duckdb_prepare(*self.handle, p, &mut prepare);
            if res != ffi::DuckDBSuccess {
                let err = ffi::duckdb_prepare_error(prepare);
                let err = Ok(Err(ConnectionError::PrepareError(
                    CStr::from_ptr(err).to_string_lossy().to_string(),
                )));
                ffi::duckdb_destroy_prepare(&mut prepare);
                return err;
            }
            Ok(Ok(PreparedStatement::from_raw(prepare, self.clone())))
        }
    }
}

impl Deref for ConnectionHandle {
    type Target = ffi::duckdb_connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_disconnect(&mut self.0);
        }
    }
}
