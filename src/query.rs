use std::{
    ffi::{c_char, CStr},
    ops::DerefMut,
    ptr::NonNull,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    connection::Connection,
    ffi,
    types::{DuckType, RawType},
};

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) handle: Mutex<QueryResultHandle>,
    pub(crate) _parent: QueryParent,
}

#[derive(Debug)]
pub(crate) struct QueryResultHandle(pub(crate) ffi::duckdb_result);

#[derive(Debug, Clone)]
pub(crate) enum QueryParent {
    Connection(Connection),
}

impl QueryResult {
    pub(crate) fn from_raw_connection(handle: ffi::duckdb_result, connection: Connection) -> Self {
        Self {
            handle: Mutex::new(QueryResultHandle(handle)),
            _parent: QueryParent::Connection(connection),
        }
    }

    pub fn column_name(&self, col: usize) -> Option<String> {
        unsafe {
            let mut p: *const c_char = ffi::duckdb_column_name(
                &mut self.handle.lock().unwrap_or_else(PoisonError::into_inner).0,
                col as u64,
            );
            let nn = NonNull::new(p as *mut c_char)?;
            let cstr = CStr::from_ptr(nn.as_ptr());
            Some(cstr.to_string_lossy().to_string())
        }
    }

    pub fn column_type(&self, col: usize) -> Option<DuckType> {
        unsafe {
            let t = ffi::duckdb_column_type(
                &mut self.handle.lock().unwrap_or_else(PoisonError::into_inner).0,
                col as u64,
            );
            RawType(t).into()
        }
    }

    pub fn column_count(&self) -> usize {
        unsafe {
            ffi::duckdb_column_count(
                &mut self.handle.lock().unwrap_or_else(PoisonError::into_inner).0,
            ) as usize
        }
    }
    pub fn row_count(self: &Arc<Self>) -> usize {
        unsafe {
            ffi::duckdb_row_count(&mut self.handle.lock().unwrap_or_else(PoisonError::into_inner).0)
                as usize
        }
    }
    pub fn rows_changed(self: &Arc<Self>) -> usize {
        unsafe {
            ffi::duckdb_rows_changed(
                &mut self.handle.lock().unwrap_or_else(PoisonError::into_inner).0,
            ) as usize
        }
    }
}

impl Drop for QueryResultHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(&mut self.0);
        }
    }
}
