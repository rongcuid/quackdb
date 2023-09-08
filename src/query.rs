use std::{
    borrow::{Borrow, BorrowMut},
    cell::RefCell,
    ffi::{c_char, CStr},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    connection::Connection,
    data_chunks::DataChunk,
    ffi,
    statement::PreparedStatement,
    types::{LogicalType, TypeId},
};

#[derive(Debug)]
pub struct QueryResult {
    pub(crate) handle: QueryResultHandle,
    pub(crate) _parent: QueryParent,
}

#[derive(Debug)]
pub struct QueryResultHandle(ffi::duckdb_result);

#[derive(Debug)]
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
            handle: QueryResultHandle(handle),
            _parent: QueryParent::Connection(connection),
        })
    }
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_result,
        statement: Arc<PreparedStatement>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: QueryResultHandle(handle),
            _parent: QueryParent::Statement(statement),
        })
    }

    pub unsafe fn chunk_unchecked(self: &Arc<Self>, chunk_index: u64) -> Arc<DataChunk> {
        let c = ffi::duckdb_result_get_chunk(*self.handle, chunk_index);
        DataChunk::from_raw(c)
    }

    pub unsafe fn column_name_unchecked(&self, col: u64) -> Option<String> {
        let p: *const c_char = ffi::duckdb_column_name(
            &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        );
        let nn = NonNull::new(p as *mut c_char)?;
        let cstr = CStr::from_ptr(nn.as_ptr());
        Some(cstr.to_string_lossy().to_string())
    }

    pub unsafe fn column_type_unchecked(&self, col: u64) -> Option<TypeId> {
        TypeId::from_raw(ffi::duckdb_column_type(
            &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        ))
    }

    pub unsafe fn column_logical_type_unchecked(&self, col: u64) -> Option<LogicalType> {
        LogicalType::from_raw(ffi::duckdb_column_logical_type(
            &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col as u64,
        ))
    }

    pub fn column_count(&self) -> u64 {
        unsafe {
            ffi::duckdb_column_count(
                &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
    pub fn row_count(&self) -> u64 {
        unsafe {
            ffi::duckdb_row_count(
                &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe {
            ffi::duckdb_rows_changed(
                &*self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
}

impl Deref for QueryResultHandle {
    type Target = ffi::duckdb_result;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for QueryResultHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(&mut *self.0.borrow_mut());
        }
    }
}
