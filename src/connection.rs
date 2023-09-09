use std::{ffi::CString, sync::Arc};

use quackdb_internal::connection::ConnectionHandle;

use crate::{error::*, query_result::QueryResult, statement::PreparedStatement};

#[derive(Debug)]
pub struct Connection {
    pub handle: Arc<ConnectionHandle>,
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
            let result = self
                .handle
                .query(p)
                .map_err(ConnectionError::QueryError)
                .map(QueryResult::from);
            Ok(result)
        }
    }

    pub fn prepare(&self, query: &str) -> DbResult<PreparedStatement, ConnectionError> {
        let cstr = CString::new(query)?;
        let p = cstr.as_ptr();
        unsafe {
            Ok(self
                .handle
                .prepare(p)
                .map_err(ConnectionError::PrepareError)
                .map(PreparedStatement::from))
        }
    }
}
