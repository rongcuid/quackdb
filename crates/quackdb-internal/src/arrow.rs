use std::{
    ffi::{c_void, CStr},
    marker::PhantomData,
    ops::Deref,
    sync::Arc,
};

use arrow::{
    array::{ArrayData, RecordBatch, StructArray},
    error::ArrowError,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};

use crate::{connection::ConnectionHandle, ffi, statement::PreparedStatementHandle};

#[derive(Debug)]
pub struct ArrowResultHandle {
    pub handle: ffi::duckdb_arrow,
    _parent: ArrowResultParent,
}

#[derive(Debug)]
pub enum ArrowResultParent {
    Connection(Arc<ConnectionHandle>),
    Statement(Arc<PreparedStatementHandle>),
}

impl ArrowResultHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_arrow,
        connection: Arc<ConnectionHandle>,
    ) -> Self {
        Self {
            handle,
            _parent: ArrowResultParent::Connection(connection),
        }
    }
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_arrow,
        statement: Arc<PreparedStatementHandle>,
    ) -> Self {
        Self {
            handle: handle,
            _parent: ArrowResultParent::Statement(statement),
        }
    }
    /// # Safety
    /// Does not check if there is actually an error
    pub unsafe fn error(&self) -> String {
        let err = ffi::duckdb_query_arrow_error(self.handle);
        CStr::from_ptr(err).to_string_lossy().into_owned()
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_column_count(self.handle) }
    }
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_row_count(self.handle) }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_rows_changed(self.handle) }
    }

    /// # Safety
    /// The result must be consumed before calling this again
    pub unsafe fn query_array(&self) -> Result<Result<RecordBatch, ArrowError>, String> {
        let mut out_schema = FFI_ArrowSchema::empty();
        if unsafe {
            ffi::duckdb_query_arrow_schema(
                self.handle,
                &mut std::ptr::addr_of_mut!(out_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            )
        } != ffi::DuckDBSuccess
        {
            return Err("duckdb_query_arrow_schema()".to_owned());
        }
        let mut out_array = FFI_ArrowArray::empty();
        if unsafe {
            ffi::duckdb_query_arrow_array(
                self.handle,
                &mut std::ptr::addr_of_mut!(out_array) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
        } != ffi::DuckDBSuccess
        {
            return Err("duckdb_query_arrow_array()".to_owned());
        }
        let arr = from_ffi(out_array, &out_schema)
            .map(StructArray::from)
            .map(RecordBatch::from);
        Ok(arr)
    }
}

impl Deref for ArrowResultHandle {
    type Target = ffi::duckdb_arrow;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for ArrowResultHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_arrow(&mut self.handle) }
    }
}
