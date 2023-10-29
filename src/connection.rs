use std::{
    ffi::{CStr, CString, NulError},
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
    #[error("duckdb_connect() error")]
    ConnectError,
    #[error("duckdb_query() error: {0}")]
    QueryError(String),
    #[error("duckdb_prepare() error: {0}")]
    PrepareError(String),
    #[error("appender error: {0}")]
    AppenderError(String),
    #[error("register table function error")]
    RegisterTableFunctionError,
    #[error(transparent)]
    NulError(#[from] NulError),
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
    pub fn query(&self, sql: &str) -> Result<ArrowResult, ConnectionError> {
        let cstr = CString::new(sql)?;
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
        let cstr = CString::new(query)?;
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
        let schema = schema.map(CString::new).transpose()?;
        let table = CString::new(table)?;
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
        datatypes::{DataType, Int32Type},
    };

    use crate::{appender::AppenderError, database::Database};

    use super::{Connection, ConnectionError};

    #[test]
    fn test_connect() {
        let db = Database::open(None).unwrap();
        let conn = db.connect();
        assert!(conn.is_ok());
    }
    #[test]
    fn test_basic() {
        let db = Database::open(None).unwrap();
        let conn = db.connect().unwrap();
        let r1 = conn.query(r"CREATE TABLE tbl(id INTEGER)").unwrap();
        assert_eq!(r1.rows_changed(), 0);
        let r2 = conn.query(r"INSERT INTO tbl VALUES (0)").unwrap();
        assert_eq!(r2.rows_changed(), 1);
        let r3 = conn.query(r"INSERT INTO tbl VALUES (1), (2), (3)").unwrap();
        assert_eq!(r3.rows_changed(), 3);
        let r4 = conn.query(r"SELECT * FROM tbl").unwrap();
        assert_eq!(r4.rows_changed(), 0);
        let qr = conn.query(r"SELECT * FROM tbl").unwrap();

        let rec = unsafe { qr.query_array().unwrap() };
        assert_eq!(*rec.column(0).data_type(), DataType::Int32);

        let mut qr = conn
            .query(r"SELECT * FROM tbl")
            .unwrap()
            .batch_map_into(|rec| {
                (0..rec.num_rows()).map(move |r| {
                    let arr: PrimitiveArray<Int32Type> = rec.column(0).to_data().into();
                    arr.value(r)
                })
            });
        assert_eq!(qr.next(), Some(0));
        assert_eq!(qr.next(), Some(1));
        assert_eq!(qr.next(), Some(2));
        assert_eq!(qr.next(), Some(3));
        assert_eq!(qr.next(), None);
    }
    fn db1() -> Connection {
        let db = Database::open(None).unwrap();
        let conn = db.connect().unwrap();
        conn.query(
            r"
            CREATE TABLE tbl(id INTEGER);
            INSERT INTO tbl VALUES (0), (1), (2), (3);
        ",
        )
        .unwrap();
        conn
    }
    #[test]
    fn test_statement_1() {
        let conn = db1();
        let stmt = conn.prepare("SELECT * FROM tbl").unwrap();
        let r1 = stmt
            .execute()
            .unwrap()
            .batch_map_into(|rec| {
                (0..rec.num_rows()).map(move |r| {
                    let arr: PrimitiveArray<Int32Type> = rec.column(0).to_data().into();
                    arr.value(r)
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(r1, vec![0, 1, 2, 3]);
        let mut stmt = conn.prepare("INSERT INTO tbl VALUES (?)").unwrap();
        for i in 4i32..8 {
            stmt.reset().bind(i).unwrap();
            stmt.execute().unwrap();
        }
        let r2 = conn
            .prepare("SELECT * FROM tbl")
            .unwrap()
            .execute()
            .unwrap()
            .batch_map_into(|rec| {
                (0..rec.num_rows()).map(move |r| {
                    let arr: PrimitiveArray<Int32Type> = rec.column(0).to_data().into();
                    arr.value(r)
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(r2, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    fn db2() -> Connection {
        let db = Database::open(None).unwrap();
        let conn = db.connect().unwrap();
        conn.query(
            r"
            CREATE TABLE tbl(id INTEGER, name TEXT);
            INSERT INTO tbl VALUES (0, '0'), (1, '1'), (2, '2'), (3, '3');
        ",
        )
        .unwrap();
        conn
    }
    #[test]
    fn test_statement_2() {
        let conn = db2();
        let mut stmt = conn.prepare("INSERT INTO tbl VALUES (?, ?)").unwrap();
        for i in 4i32..8 {
            stmt.reset().bind(i).unwrap().bind(i.to_string()).unwrap();
            stmt.execute().unwrap();
        }
        let r2 = conn
            .prepare("SELECT * FROM tbl WHERE id > 6")
            .unwrap()
            .execute()
            .unwrap()
            .batch_map_into(|rec| {
                (0..rec.num_rows()).map(move |r| {
                    let arr1 = rec.column(0).as_primitive::<Int32Type>();
                    let arr2 = rec.column(1).as_string::<i32>();
                    (arr1.value(r), arr2.value(r).to_owned())
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(r2, vec![(7, "7".to_owned())]);
    }
    fn db3() -> Result<Connection, ConnectionError> {
        let db = Database::open(None).unwrap();
        let conn = db.connect().unwrap();
        conn.query(
            r"
            CREATE TABLE tbl(id INTEGER, name TEXT);
        ",
        )?;
        let mut appender = conn.appender(None, "tbl")?;
        (|| -> Result<(), AppenderError> {
            appender
                .append(0)?
                .append("0")?
                .end_row()?
                .append(1)?
                .append("1")?
                .end_row()?
                .append(2)?
                .append("2")?
                .end_row()?
                .append(3)?
                .append("3")?
                .end_row()?;
            Ok(())
        })()
        .unwrap();

        Ok(conn)
    }
    #[test]
    fn test_appender_1() {
        let conn = db3().unwrap();
        let r = conn
            .prepare("SELECT * FROM tbl WHERE id >= 3")
            .unwrap()
            .execute()
            .unwrap()
            .batch_map_into(|rec| {
                (0..rec.num_rows()).map(move |r| {
                    let arr1 = rec.column(0).as_primitive::<Int32Type>();
                    let arr2 = rec.column(1).as_string::<i32>();
                    (arr1.value(r), arr2.value(r).to_owned())
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(r, vec![(3, "3".to_owned())]);
    }
}
