use std::{ops::Deref, sync::Arc};

use crate::ffi;

use super::ConnectionHandle;

pub struct AppenderHandle {
    raw: ffi::duckdb_appender,
    _parent: Arc<ConnectionHandle>,
}

impl AppenderHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_appender, connection: Arc<ConnectionHandle>) -> Self {
        Self {
            raw,
            _parent: connection,
        }
    }
}

impl Deref for AppenderHandle {
    type Target = ffi::duckdb_appender;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl Drop for AppenderHandle {
    fn drop(&mut self) {
        unsafe {
            if ffi::duckdb_appender_destroy(&mut self.raw) != ffi::DuckDBSuccess {
                panic!("duckdb_appender_destroy() failed");
            }
        }
    }
}
