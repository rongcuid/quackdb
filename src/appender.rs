use std::{ffi::CStr, ops::Deref};

use quackdb_internal::{conversion::AppendParam, ffi, handles::AppenderHandle};
use thiserror::Error;

pub struct Appender {
    pub handle: AppenderHandle,
}

#[derive(Error, Debug)]
pub enum AppenderError {
    #[error("appender flush: {0}")]
    FlushError(String),
    #[error("appender error: {0}")]
    AppendError(String),
}

impl Appender {
    pub fn error(&self) -> String {
        let err = unsafe { CStr::from_ptr(ffi::duckdb_appender_error(**self)) };
        err.to_string_lossy().into_owned()
    }
    pub fn flush(&self) -> Result<(), AppenderError> {
        match unsafe { ffi::duckdb_appender_flush(**self) } {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err(AppenderError::FlushError(self.error())),
            _ => unreachable!(),
        }
    }
    pub fn append<T: AppendParam>(&mut self, value: T) -> Result<&mut Self, AppenderError> {
        unsafe {
            value
                .append_param_unchecked(**self)
                .map_err(|_| AppenderError::AppendError(self.error()))
                .and(Ok(self))
        }
    }
    pub fn end_row(&mut self) -> Result<&mut Self, AppenderError> {
        match unsafe { ffi::duckdb_appender_end_row(**self) } {
            ffi::DuckDBSuccess => Ok(self),
            ffi::DuckDBError => Err(AppenderError::AppendError(self.error())),
            _ => unreachable!(),
        }
    }
}

impl From<AppenderHandle> for Appender {
    fn from(value: AppenderHandle) -> Self {
        Self { handle: value }
    }
}

impl Deref for Appender {
    type Target = ffi::duckdb_appender;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
