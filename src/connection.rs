use std::{
    ffi::{CStr, CString},
    ops::Deref,
    sync::Arc,
};

use quackdb_internal::{
    ffi,
    handles::{AppenderHandle, ArrowResultHandle, ConnectionHandle, PreparedStatementHandle},
};

use crate::{
    appender::Appender, arrow::ArrowResult, statement::PreparedStatement,
    table_function::TableFunction,
};

#[derive(Debug)]
pub struct Connection {
    handle: Arc<ConnectionHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("bad query: {0}")]
    BadQuery(String),
    #[error("bad schema: {0}")]
    BadSchema(String),
    #[error("bad table: {0}")]
    BadTable(String),
    #[error("query error: {0}")]
    QueryError(String),
    #[error("prepare error: {0}")]
    PrepareError(String),
    #[error("appender error: {0}")]
    AppenderError(String),
    #[error("register table function error")]
    RegisterTableFunctionError,
}

impl From<Arc<ConnectionHandle>> for Connection {
    fn from(value: Arc<ConnectionHandle>) -> Self {
        Self { handle: value }
    }
}

impl Connection {
    pub fn interrupt(&self) {
        unsafe { ffi::duckdb_interrupt(**self) }
    }

    pub fn query_progress(&self) -> f64 {
        unsafe { ffi::duckdb_query_progress(**self) }
    }

    /// Perform a query and return the handle.
    pub fn query(&self, query: &str) -> Result<ArrowResult, ConnectionError> {
        let cstr = CString::new(query).map_err(|_| ConnectionError::BadQuery(query.to_owned()))?;
        unsafe {
            let mut result: ffi::duckdb_arrow = std::mem::zeroed();
            let r = ffi::duckdb_query_arrow(**self, cstr.as_ptr(), &mut result);
            let h: ArrowResult =
                ArrowResultHandle::from_raw_connection(result, self.handle.clone()).into();
            if r != ffi::DuckDBSuccess {
                return Err(ConnectionError::QueryError(h.error()));
            }
            Ok(h)
        }
    }

    pub fn prepare(&self, query: &str) -> Result<PreparedStatement, ConnectionError> {
        let cstr = CString::new(query).map_err(|_| ConnectionError::BadQuery(query.to_owned()))?;
        unsafe {
            let mut prepare: ffi::duckdb_prepared_statement = std::mem::zeroed();
            let res = ffi::duckdb_prepare(**self, cstr.as_ptr(), &mut prepare);
            if res != ffi::DuckDBSuccess {
                let err = ffi::duckdb_prepare_error(prepare);
                let err = CStr::from_ptr(err).to_string_lossy().to_owned().to_string();
                ffi::duckdb_destroy_prepare(&mut prepare);
                return Err(ConnectionError::PrepareError(err));
            }
            Ok(PreparedStatementHandle::from_raw(prepare, self.handle.clone()).into())
        }
    }

    pub fn appender(&self, schema: Option<&str>, table: &str) -> Result<Appender, ConnectionError> {
        let schema = schema
            .map(|s| CString::new(s).map_err(|_| ConnectionError::BadSchema(s.to_owned())))
            .transpose()?;
        let table = CString::new(table).map_err(|_| ConnectionError::BadTable(table.to_owned()))?;
        unsafe {
            let mut out_appender: ffi::duckdb_appender = std::mem::zeroed();
            let r = ffi::duckdb_appender_create(
                **self,
                schema.map_or(std::ptr::null(), |s| s.as_ptr()),
                table.as_ptr(),
                &mut out_appender,
            );
            if r != ffi::DuckDBSuccess {
                let err = CStr::from_ptr(ffi::duckdb_appender_error(out_appender));
                let err = err.to_string_lossy().into_owned();
                ffi::duckdb_appender_destroy(&mut out_appender);
                Err(ConnectionError::AppenderError(err))
            } else {
                Ok(AppenderHandle::from_raw(out_appender, self.handle.clone()).into())
            }
        }
    }
    pub fn register_table_function(
        &mut self,
        function: &TableFunction,
    ) -> Result<(), ConnectionError> {
        self.handle
            .register_table_function(function.handle.clone())
            .map_err(|_| ConnectionError::RegisterTableFunctionError)
    }
}

impl Deref for Connection {
    type Target = ffi::duckdb_connection;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{AsArray, PrimitiveArray},
        datatypes::{DataType, Int32Type, Int64Type},
        error::ArrowError,
    };

    use crate::{appender::AppenderError, database::Database, error::QuackError};

    use super::{Connection, ConnectionError};

    #[test]
    fn test_connect() {
        let db = Database::open(None).unwrap();
        let conn = db.connect();
        assert!(conn.is_ok());
    }
    #[test]
    fn test_basic() -> Result<(), QuackError> {
        let db = Database::open(None)?;
        let conn = db.connect()?;
        let r1 = conn.query(r"CREATE TABLE tbl(id INTEGER)")?;
        assert_eq!(r1.rows_changed(), 0);
        let r2 = conn.query(r"INSERT INTO tbl VALUES (0)")?;
        assert_eq!(r2.rows_changed(), 1);
        let r3 = conn.query(r"INSERT INTO tbl VALUES (1), (2), (3)")?;
        assert_eq!(r3.rows_changed(), 3);
        let r4 = conn.query(r"SELECT * FROM tbl")?;
        assert_eq!(r4.rows_changed(), 0);
        let qr = conn.query(r"SELECT * FROM tbl")?;
        Ok(())
    }
    #[test]
    fn test_arrow_1() -> Result<(), QuackError> {
        // Create DB
        let db = Database::open(None)?;
        let conn = db.connect()?;
        conn.query(
            r"
            CREATE TABLE tbl(id BIGINT);
        ",
        )?;
        // Insert data
        let mut appender = conn.appender(None, "tbl")?;
        for i in 0..1000000i64 {
            appender.append(i)?.end_row()?;
        }
        drop(appender);
        // Query data
        let res = conn.query("SELECT * FROM tbl")?;
        let mut stream = res.into_stream()?;
        let sum: i64 = stream.try_fold(0, |acc, r| -> Result<i64, ArrowError> {
            let r = r?;
            Ok(acc
                + r.column(0)
                    .as_primitive::<Int64Type>()
                    .iter()
                    .filter_map(|x| x)
                    .sum::<i64>())
        })?;
        assert_eq!(sum, (0..1000000i64).sum::<i64>());
        Ok(())
    }
    #[test]
    fn test_arrow_2() -> Result<(), QuackError> {
        // Create DB
        let db = Database::open(None)?;
        let conn = db.connect()?;
        conn.query(
            r"
            CREATE TABLE tbl(id BIGINT);
        ",
        )?;
        // Insert data
        let mut appender = conn.appender(None, "tbl")?;
        for i in 0..1000000i64 {
            appender.append(i)?.end_row()?;
        }
        drop(appender);
        // Query data
        let stream = conn.query("SELECT * FROM tbl")?.into_stream()?;
        let recs = stream.collect::<Result<Vec<_>, ArrowError>>()?;
        let sum = recs.iter().fold(0, |acc, r| {
            acc + r
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .filter_map(|x| x)
                .sum::<i64>()
        });
        assert_eq!(sum, (0..1000000i64).sum::<i64>());
        Ok(())
    }
}
