use std::{
    ffi::{c_char, CStr},
    ops::Deref,
    ptr::NonNull,
    sync::Arc,
};

use crate::{
    connection::ConnectionHandle,
    data_chunks::DataChunkHandle,
    ffi,
    statement::PreparedStatementHandle,
    types::{LogicalTypeHandle, TypeId},
};

#[derive(Debug)]
pub struct QueryResultHandle {
    handle: ffi::duckdb_result,
    _parent: QueryParent,
}

#[derive(Debug)]
pub enum QueryParent {
    Connection(Arc<ConnectionHandle>),
    Statement(Arc<PreparedStatementHandle>),
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
            self.destroy();
        }
    }
}

impl QueryResultHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_result,
        connection: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle,
            _parent: QueryParent::Connection(connection),
        })
    }
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_result,
        statement: Arc<PreparedStatementHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: handle,
            _parent: QueryParent::Statement(statement),
        })
    }
    /// # Safety
    /// Destroys without considering usage. Normally you should let Rust manage this.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_result(&mut self.handle);
    }
    /// # Safety
    /// `chunk_index` must be within valid range.
    pub unsafe fn chunk(&self, chunk_index: u64) -> Arc<DataChunkHandle> {
        let c = ffi::duckdb_result_get_chunk(self.handle, chunk_index);
        DataChunkHandle::from_raw(c)
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_name(&self, col: u64) -> Option<String> {
        let p: *const c_char = ffi::duckdb_column_name(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        );
        let nn = NonNull::new(p as *mut c_char)?;
        let cstr = CStr::from_ptr(nn.as_ptr());
        Some(cstr.to_string_lossy().to_owned().to_string())
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_type(&self, col: u64) -> TypeId {
        TypeId::from_raw(ffi::duckdb_column_type(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
            col,
        ))
        .expect("invalid duckdb type")
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_logical_type(&self, col: u64) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_column_logical_type(
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
    /// # Safety
    /// Does not check if there is actually an error
    pub unsafe fn error(&self) -> String {
        let err = ffi::duckdb_result_error(
            &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result,
        );
        CStr::from_ptr(err).to_string_lossy().into_owned()
    }
}
