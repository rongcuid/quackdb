use std::{ops::Deref, sync::Arc};

use crate::ffi;

use super::ConnectionHandle;

#[derive(Debug)]
pub struct PreparedStatementHandle {
    raw: ffi::duckdb_prepared_statement,
    _parent: Arc<ConnectionHandle>,
}

impl Deref for PreparedStatementHandle {
    type Target = ffi::duckdb_prepared_statement;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl Drop for PreparedStatementHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_prepare(&mut self.raw) }
    }
}

/// # Safety
/// * All parameter indices must be in range
impl PreparedStatementHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(
        raw: ffi::duckdb_prepared_statement,
        parent: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            raw,
            _parent: parent,
        })
    }
}
