use std::{
    ffi::{CString, NulError},
    sync::Arc,
};

use quackdb_internal::{appender::AppenderHandle, connection::ConnectionHandle};

use crate::{appender::Appender, arrow::ArrowResult, statement::PreparedStatement};

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
    #[error("appender error: {0}")]
    AppenderError(String),
    #[error(transparent)]
    NulError(#[from] NulError),
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

    /// Perform a query and return the handle.
    pub fn query(&self, sql: &str) -> Result<ArrowResult, ConnectionError> {
        let cstr = CString::new(sql)?;
        self.handle
            .query(&cstr)
            .map_err(ConnectionError::QueryError)
            .map(ArrowResult::from)
    }

    pub fn prepare(&self, query: &str) -> Result<PreparedStatement, ConnectionError> {
        let cstr = CString::new(query)?;
        self.handle
            .prepare(&cstr)
            .map_err(ConnectionError::PrepareError)
            .map(PreparedStatement::from)
    }

    pub fn appender(&self, schema: Option<&str>, table: &str) -> Result<Appender, ConnectionError> {
        let schema = schema.map(CString::new).transpose()?;
        let table = CString::new(table)?;
        AppenderHandle::create(self.handle.clone(), schema.as_deref(), &table)
            .map_err(ConnectionError::AppenderError)
            .map(Appender::from)
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

        let rec = unsafe { qr.handle.query_array().unwrap().unwrap() };
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
