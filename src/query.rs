use std::{
    ffi::{c_char, CStr},
    ops::DerefMut,
    ptr::NonNull,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    connection::Connection, ffi, logical_type::LogicalType, statement::PreparedStatement,
    types::RawType,
};

pub struct QueryResult {
    pub(crate) handle: Mutex<ffi::duckdb_result>,
    pub(crate) _parent: QueryParent,
}

pub(crate) enum QueryParent {
    Connection(Arc<Connection>),
    Statement(Arc<PreparedStatement>),
}

impl QueryResult {
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_result,
        connection: Arc<Connection>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(handle),
            _parent: QueryParent::Connection(connection),
        })
    }
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_result,
        statement: Arc<PreparedStatement>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(handle),
            _parent: QueryParent::Statement(statement),
        })
    }

    pub unsafe fn column_name_unchecked(&self, col: u64) -> Option<String> {
        let p: *const c_char =
            ffi::duckdb_column_name(self.handle.lock().unwrap().deref_mut(), col);
        let nn = NonNull::new(p as *mut c_char)?;
        let cstr = CStr::from_ptr(nn.as_ptr());
        Some(cstr.to_string_lossy().to_string())
    }

    pub unsafe fn column_type_unchecked(&self, col: u64) -> Option<RawType> {
        RawType::from_raw(ffi::duckdb_column_type(
            self.handle.lock().unwrap().deref_mut(),
            col,
        ))
    }

    pub unsafe fn column_logical_type_unchecked(&self, col: u64) -> Option<LogicalType> {
        LogicalType::from_raw(ffi::duckdb_column_logical_type(
            self.handle.lock().unwrap().deref_mut(),
            col as u64,
        ))
    }

    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_column_count(self.handle.lock().unwrap().deref_mut()) }
    }
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::duckdb_row_count(self.handle.lock().unwrap().deref_mut()) }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe { ffi::duckdb_rows_changed(self.handle.lock().unwrap().deref_mut()) }
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(self.handle.lock().unwrap().deref_mut());
        }
    }
}
