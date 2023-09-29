use std::{ffi::CString, sync::Arc};

use quackdb_internal::connection::ConnectionHandle;

use crate::{arrow::ArrowResult, error::*, statement::PreparedStatement};

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

    /// Execute an SQL statement and return the number of rows changed
    pub fn execute(&self, sql: &str) -> DbResult<u64, ConnectionError> {
        Ok(self.query(sql)?.map(|r| r.rows_changed()))
    }

    /// Perform a query and return the handle.
    pub fn query(&self, sql: &str) -> DbResult<ArrowResult, ConnectionError> {
        let cstr = CString::new(sql)?;
        let result = self
            .handle
            .query(&cstr)
            .map_err(ConnectionError::QueryError)
            .map(ArrowResult::from);

        Ok(result)
    }

    pub fn prepare(&self, query: &str) -> DbResult<PreparedStatement, ConnectionError> {
        let cstr = CString::new(query)?;
        Ok(self
            .handle
            .prepare(&cstr)
            .map_err(ConnectionError::PrepareError)
            .map(PreparedStatement::from))
    }
}

#[cfg(test)]
mod test {
    use crate::database::Database;

    #[test]
    fn test_connect() {
        let db = Database::open(None).unwrap().unwrap();
        let conn = db.connect();
        assert!(conn.is_ok());
    }
    #[test]
    fn test_query() {
        let db = Database::open(None).unwrap().unwrap();
        let conn = db.connect().unwrap();
        let r1 = conn
            .execute(r"CREATE TABLE tbl(id INTEGER)")
            .unwrap()
            .unwrap();
        assert_eq!(r1, 0);
        let r2 = conn
            .execute(r"INSERT INTO tbl VALUES (0)")
            .unwrap()
            .unwrap();
        assert_eq!(r2, 1);
        let r3 = conn
            .execute(r"INSERT INTO tbl VALUES (1), (2), (3)")
            .unwrap()
            .unwrap();
        assert_eq!(r3, 3);
        let r4 = conn.execute(r"SELECT * FROM tbl").unwrap().unwrap();
        assert_eq!(r4, 0);
        // let qr = conn.query(r"SELECT * FROM tbl").unwrap().unwrap();
        // assert_eq!(qr.get::<i32>(0, 0).unwrap(), 0);
        // assert_eq!(qr.get::<i32>(0, 1).unwrap(), 1);
        // assert_eq!(qr.get::<i32>(0, 2).unwrap(), 2);
        // assert_eq!(qr.get::<i32>(0, 3).unwrap(), 3);
    }
}
