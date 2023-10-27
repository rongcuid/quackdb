use paste::paste;
use std::{ffi::CStr, ops::Deref, sync::Arc};
use thiserror::Error;

use crate::{connection::ConnectionHandle, ffi};

pub struct AppenderHandle {
    handle: ffi::duckdb_appender,
    _parent: Arc<ConnectionHandle>,
}

#[derive(Error, Debug)]
#[error("append error")]
pub struct AppendError();

impl AppenderHandle {
    pub unsafe fn from_raw(raw: ffi::duckdb_appender, connection: Arc<ConnectionHandle>) -> Self {
        Self {
            handle: raw,
            _parent: connection,
        }
    }
}

impl Deref for AppenderHandle {
    type Target = ffi::duckdb_appender;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for AppenderHandle {
    fn drop(&mut self) {
        unsafe {
            if ffi::duckdb_appender_destroy(&mut self.handle) != ffi::DuckDBSuccess {
                panic!("duckdb_appender_destroy() failed");
            }
        }
    }
}
