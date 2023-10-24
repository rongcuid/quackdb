use quackdb_internal::{appender::AppenderHandle, value::AppendParam};
use thiserror::Error;

pub struct Appender {
    pub handle: AppenderHandle,
}

#[derive(Error, Debug)]
pub enum AppenderError {
    #[error("appender flush: {0}")]
    FlushError(String),
    #[error("appender close: {0}")]
    CloseError(String),
    #[error("appender error: {0}")]
    AppendError(String),
}

impl Appender {
    pub fn flush(&self) -> Result<(), AppenderError> {
        self.handle.flush().map_err(AppenderError::FlushError)
    }
    pub fn close(&self) -> Result<(), AppenderError> {
        self.handle.close().map_err(AppenderError::CloseError)
    }
    pub fn append<T: AppendParam>(&mut self, value: T) -> Result<&mut Self, AppenderError> {
        unsafe {
            value
                .append_param_unchecked(&self.handle)
                .map_err(AppenderError::AppendError)
                .and(Ok(self))
        }
    }
    pub fn end_row(&mut self) -> Result<&mut Self, AppenderError> {
        self.handle
            .end_row()
            .map_err(AppenderError::AppendError)
            .and(Ok(self))
    }
}

impl From<AppenderHandle> for Appender {
    fn from(value: AppenderHandle) -> Self {
        Self { handle: value }
    }
}
