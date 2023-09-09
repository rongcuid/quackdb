use std::{
    ffi::{c_char, CStr},
    ops::Deref,
    ptr::NonNull,
    sync::Arc,
};

use crate::{
    connection::{Connection, ConnectionHandle},
    data_chunks::{DataChunk, DataChunkHandle},
    ffi,
    statement::{PreparedStatement, PreparedStatementHandle},
    types::{LogicalType, TypeId},
};

#[derive(Debug)]
pub struct QueryResult {
    pub handle: Arc<QueryResultHandle>,
}

#[derive(Debug)]
pub struct QueryResultHandle {
    handle: ffi::duckdb_result,
    _parent: QueryParent,
}

#[derive(Debug)]
pub(crate) enum QueryParent {
    Connection(Arc<ConnectionHandle>),
    Statement(Arc<PreparedStatementHandle>),
}

impl QueryResult {
    pub fn chunk(&self, chunk_index: u64) -> Arc<DataChunk> {
        todo!()
    }
    pub fn column_name(&self, col: u64) -> Option<String> {
        todo!()
    }
    pub fn column_type(&self, col: u64) -> Option<LogicalType> {
        todo!()
    }
    pub fn column_count(&self) -> u64 {
        self.handle.column_count()
    }
    pub fn row_count(&self) -> u64 {
        self.handle.row_count()
    }
    pub fn rows_changed(&self) -> u64 {
        self.handle.rows_changed()
    }
}

impl From<Arc<QueryResultHandle>> for QueryResult {
    fn from(value: Arc<QueryResultHandle>) -> Self {
        Self { handle: value }
    }
}

impl QueryResultHandle {
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_result,
        connection: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: handle,
            _parent: QueryParent::Connection(connection),
        })
    }
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_result,
        statement: Arc<PreparedStatementHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: handle,
            _parent: QueryParent::Statement(statement),
        })
    }
    pub unsafe fn chunk_unchecked(&self, chunk_index: u64) -> DataChunk {
        let c = ffi::duckdb_result_get_chunk(self.handle, chunk_index);
        DataChunkHandle::from_raw(c).into()
    }
    pub unsafe fn column_name_unchecked(&self, col: u64) -> Option<String> {
        let p: *const c_char = ffi::duckdb_column_name(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        );
        let nn = NonNull::new(p as *mut c_char)?;
        let cstr = CStr::from_ptr(nn.as_ptr());
        Some(cstr.to_string_lossy().to_string())
    }
    pub unsafe fn column_type_unchecked(&self, col: u64) -> Option<TypeId> {
        TypeId::from_raw(ffi::duckdb_column_type(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        ))
    }
    pub unsafe fn column_logical_type_unchecked(&self, col: u64) -> Option<LogicalType> {
        LogicalType::from_raw(ffi::duckdb_column_logical_type(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col as u64,
        ))
    }
    pub fn column_count(&self) -> u64 {
        unsafe {
            ffi::duckdb_column_count(
                &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
    pub fn row_count(&self) -> u64 {
        unsafe {
            ffi::duckdb_row_count(
                &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe {
            ffi::duckdb_rows_changed(
                &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            )
        }
    }
}

impl Deref for QueryResultHandle {
    type Target = ffi::duckdb_result;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for QueryResultHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_result(&mut self.handle);
        }
    }
}
