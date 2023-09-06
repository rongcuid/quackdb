use std::{
    ffi::{c_char, CStr},
    ops::DerefMut,
    ptr::NonNull,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{connection::Connection, ffi, logical_type::LogicalType, types::RawType};

pub struct QueryResult {
    pub(crate) handle: Mutex<ffi::duckdb_result>,
    pub(crate) _parent: QueryParent,
}

pub(crate) enum QueryParent {
    Connection(Arc<Connection>),
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

    pub fn column_name(&self, col: usize) -> Option<String> {
        unsafe {
            let p: *const c_char =
                ffi::duckdb_column_name(self.handle.lock().unwrap().deref_mut(), col as u64);
            let nn = NonNull::new(p as *mut c_char)?;
            let cstr = CStr::from_ptr(nn.as_ptr());
            Some(cstr.to_string_lossy().to_string())
        }
    }

    pub fn column_type(&self, col: usize) -> Option<LogicalType> {
        unsafe {
            LogicalType::from_raw(ffi::duckdb_column_logical_type(
                self.handle.lock().unwrap().deref_mut(),
                col as u64,
            ))
        }
    }

    pub fn column_count(&self) -> usize {
        unsafe { ffi::duckdb_column_count(self.handle.lock().unwrap().deref_mut()) as usize }
    }
    pub fn row_count(&self) -> usize {
        unsafe { ffi::duckdb_row_count(self.handle.lock().unwrap().deref_mut()) as usize }
    }
    pub fn rows_changed(&self) -> usize {
        unsafe { ffi::duckdb_rows_changed(self.handle.lock().unwrap().deref_mut()) as usize }
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(self.handle.lock().unwrap().deref_mut());
        }
    }
}