use std::{ffi::CStr, ops::Deref, sync::Arc};

use arrow::{
    array::{RecordBatch, StructArray},
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
    /// Takes ownership of `handle`
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
