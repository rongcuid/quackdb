use std::{ops::Deref, sync::Arc};

use thiserror::Error;

use crate::ffi;

use super::ConnectionHandle;

#[derive(Debug)]
pub struct PreparedStatementHandle {
    handle: ffi::duckdb_prepared_statement,
    _parent: Arc<ConnectionHandle>,
}

#[derive(Error, Debug)]
#[error("prepared statement bind error")]
pub struct BindError();

impl Deref for PreparedStatementHandle {
    type Target = ffi::duckdb_prepared_statement;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for PreparedStatementHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_prepare(&mut self.handle) }
    }
}

/// # Safety
/// * All parameter indices must be in range
impl PreparedStatementHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(
        raw: ffi::duckdb_prepared_statement,
        parent: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
        })
    }
}
