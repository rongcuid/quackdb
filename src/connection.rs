use std::{
    ffi::{CStr, CString},
    ops::Deref,
    sync::Arc,
};

use libc::c_void;
use quackdb_internal::{
    ffi,
    handles::{AppenderHandle, ArrowResultHandle, ConnectionHandle, PreparedStatementHandle},
};

use crate::{
    appender::Appender,
    arrow::ArrowResult,
    statement::PreparedStatement,
    table_function::{BindInfo, ExtraInfo, FunctionInfo, InitInfo},
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

    pub fn register_table_function<B, I, LI, D, E>(
        &self,
        bind: impl Fn(&BindInfo, &D) -> Result<B, E> + Send + 'static,
        init: impl Fn(&InitInfo, &B, &D) -> Result<I, E> + Send + 'static,
        local_init: impl Fn(&InitInfo, &B, &D) -> Result<LI, E> + Send + Sync + 'static,
        function: impl Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &D) -> Result<(), E>
            + Send
            + Sync
            + 'static,
        projection: bool,
        extra_data: D,
    ) -> Result<(), ConnectionError>
    where
        B: Send + Sync,
        I: Send + Sync,
        LI: Send + Sync,
        D: Send + Sync,
        E: std::error::Error + Send,
    {
        unsafe {
            let table_function = ffi::duckdb_create_table_function();
            ffi::duckdb_table_function_supports_projection_pushdown(table_function, projection);
            // Register callbacks
            ffi::duckdb_table_function_set_bind(table_function, Some(bind_fn::<B, I, LI, D, E>));
            ffi::duckdb_table_function_set_init(table_function, Some(init_fn::<B, I, LI, D, E>));
            ffi::duckdb_table_function_set_local_init(
                table_function,
                Some(local_init_fn::<B, I, LI, D, E>),
            );
            ffi::duckdb_table_function_set_function(
                table_function,
                Some(main_fn::<B, I, LI, D, E>),
            );
            // Store extra info
            let extra = Box::new(ExtraInfo {
                bind: Box::new(bind),
                init: Box::new(init),
                local_init: Box::new(local_init),
                function: Box::new(function),
                extra: extra_data,
            });
            ffi::duckdb_table_function_set_extra_info(
                table_function,
                Box::into_raw(extra).cast(),
                Some(destroy_extra_info::<B, I, LI, D, E>),
            );
            Ok(())
        }
    }
}

extern "C" fn bind_fn<B, I, LI, D, E>(info: ffi::duckdb_bind_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    D: Send + Sync,
    E: std::error::Error + Send,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, E, E> = ffi::duckdb_bind_get_extra_info(info).cast();
        let f = &(*extra).bind;
        let result = f(&BindInfo::from(info), &(*extra).extra);
        match result {
            Ok(b) => {
                let b = Box::new(b);
                ffi::duckdb_bind_set_bind_data(
                    info,
                    Box::into_raw(b).cast(),
                    Some(destroy_box::<B>),
                );
            }
            Err(e) => {
                let err = CString::new(e.to_string().replace('\0', r"\0")).expect("null character");
                ffi::duckdb_bind_set_error(info, err.as_ptr());
            }
        }
    }
}

extern "C" fn init_fn<B, I, LI, D, E>(info: ffi::duckdb_init_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    D: Send + Sync,
    E: std::error::Error + Send,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, D, E> = ffi::duckdb_init_get_extra_info(info).cast();
        let f = &(*extra).init;
        let bind: *const B = ffi::duckdb_init_get_bind_data(info).cast();
        let result = f(&InitInfo::from(info), &*bind, &(*extra).extra);
        match result {
            Ok(i) => {
                let b = Box::new(i);
                ffi::duckdb_init_set_init_data(
                    info,
                    Box::into_raw(b).cast(),
                    Some(destroy_box::<B>),
                );
            }
            Err(e) => {
                let err = CString::new(e.to_string().replace('\0', r"\0")).expect("null character");
                ffi::duckdb_init_set_error(info, err.as_ptr());
            }
        }
    }
}

extern "C" fn local_init_fn<B, I, LI, D, E>(info: ffi::duckdb_init_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    D: Send + Sync,
    E: std::error::Error + Send,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, D, E> = ffi::duckdb_init_get_extra_info(info).cast();
        let bind: *const B = ffi::duckdb_init_get_bind_data(info).cast();
        let f = &(*extra).local_init;
        let result = f(&InitInfo::from(info), &*bind, &(*extra).extra);
        match result {
            Ok(i) => {
                let b = Box::new(i);
                ffi::duckdb_init_set_init_data(
                    info,
                    Box::into_raw(b).cast(),
                    Some(destroy_box::<B>),
                );
            }
            Err(e) => {
                let err = CString::new(e.to_string().replace('\0', r"\0")).expect("null character");
                ffi::duckdb_init_set_error(info, err.as_ptr());
            }
        }
    }
}

extern "C" fn main_fn<B, I, LI, D, E>(
    info: ffi::duckdb_function_info,
    data_chunk: ffi::duckdb_data_chunk,
) where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    D: Send + Sync,
    E: std::error::Error + Send,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, D, E> =
            ffi::duckdb_function_get_extra_info(info).cast();
        let f = &(*extra).function;
        let bind: *const B = ffi::duckdb_function_get_bind_data(info).cast();
        let init: *const I = ffi::duckdb_function_get_init_data(info).cast();
        let local_init: *const LI = ffi::duckdb_function_get_local_init_data(info).cast();
        let result = f(
            &FunctionInfo::from(info),
            data_chunk,
            &*bind,
            &*init,
            &*local_init,
            &(*extra).extra,
        );
        if let Err(e) = result {
            let err = CString::new(e.to_string().replace('\0', r"\0")).expect("null character");
            ffi::duckdb_function_set_error(info, err.as_ptr());
        }
    }
}

extern "C" fn destroy_extra_info<B, I, LI, D, E>(ptr: *mut c_void) {
    destroy_box::<ExtraInfo<B, I, LI, D, E>>(ptr)
}

extern "C" fn destroy_box<T>(ptr: *mut c_void) {
    unsafe { drop::<Box<T>>(Box::from_raw(ptr.cast())) }
}

impl Deref for Connection {
    type Target = ffi::duckdb_connection;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use arrow::{array::AsArray, datatypes::Int64Type, error::ArrowError};

    use crate::{database::Database, error::QuackError, replacement_scan::ReplacementScanError};

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
        db.add_replacement_scan(
            |&_, _, &_| Ok::<(), ReplacementScanError<Infallible>>(()),
            (),
        );
        conn.register_table_function(
            |&_, &_| Ok::<(), Infallible>(()),
            |&_, &_, &_| Ok(()),
            |&_, &_, &_| Ok(()),
            |&_, _, &_, &_, &_, &_| Ok(()),
            false,
            (),
        )?;

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
                    .flatten()
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
                .flatten()
                .sum::<i64>()
        });
        assert_eq!(sum, (0..1000000i64).sum::<i64>());
        Ok(())
    }
}
