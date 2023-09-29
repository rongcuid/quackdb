use std::{
    ffi::{c_void, CStr},
    sync::Arc,
};

use arrow::{
    array::ArrayData,
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

pub struct ArrayDataHandle {
    pub handle: ArrayData,
    _parent: Arc<ArrowResultHandle>,
}

impl ArrowResultHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_arrow,
        connection: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle,
            _parent: ArrowResultParent::Connection(connection),
        })
    }
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_arrow,
        statement: Arc<PreparedStatementHandle>,
    ) -> Arc<Self> {
        let mut schema = FFI_ArrowSchema::empty();
        Arc::new(Self {
            handle: handle,
            _parent: ArrowResultParent::Statement(statement),
        })
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
    pub fn next_array(self: &Arc<Self>) -> Result<Result<ArrayDataHandle, ArrowError>, ()> {
        let mut out_schema = FFI_ArrowSchema::empty();
        if unsafe {
            ffi::duckdb_query_arrow_schema(
                self.handle,
                &mut std::ptr::addr_of_mut!(out_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            )
        } != ffi::DuckDBSuccess
        {
            return Err(());
        }
        let mut out_array = FFI_ArrowArray::empty();
        if unsafe {
            ffi::duckdb_query_arrow_array(
                self.handle,
                &mut std::ptr::addr_of_mut!(out_array) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
        } != ffi::DuckDBSuccess
        {
            return Err(());
        }
        let arr = from_ffi(out_array, &out_schema).map(|a| ArrayDataHandle {
            handle: a,
            _parent: self.clone(),
        });
        Ok(arr)
    }
}
