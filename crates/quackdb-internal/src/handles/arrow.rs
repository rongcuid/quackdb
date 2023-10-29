use std::{ops::Deref, sync::Arc};

use crate::ffi;

use super::{ConnectionHandle, PreparedStatementHandle};

#[derive(Debug)]
pub struct ArrowResultHandle {
    raw: ffi::duckdb_arrow,
    _parent: ArrowResultParent,
}

#[derive(Debug)]
pub enum ArrowResultParent {
    Connection(Arc<ConnectionHandle>),
    Statement(Arc<PreparedStatementHandle>),
}

impl ArrowResultHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw_connection(
        raw: ffi::duckdb_arrow,
        connection: Arc<ConnectionHandle>,
    ) -> Self {
        Self {
            raw,
            _parent: ArrowResultParent::Connection(connection),
        }
    }
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw_statement(
        raw: ffi::duckdb_arrow,
        statement: Arc<PreparedStatementHandle>,
    ) -> Self {
        Self {
            raw,
            _parent: ArrowResultParent::Statement(statement),
        }
    }
}

impl Deref for ArrowResultHandle {
    type Target = ffi::duckdb_arrow;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl Drop for ArrowResultHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_arrow(&mut self.raw) }
    }
}
