use std::sync::Arc;

use quackdb_conversion::BindParam;
use quackdb_internal::statement::PreparedStatementHandle;

use crate::arrow::ArrowResult;

#[derive(Debug)]
pub struct PreparedStatement {
    pub handle: Arc<PreparedStatementHandle>,
    current_index: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum PreparedStatementError {
    #[error("duckdb_clear_bindings() failed")]
    ClearBindingsError,
    #[error("{0}: binding parameter to column {1} failed")]
    BindError(&'static str, u64),
    #[error("attempted binding to column {0} outside bounds 1..={1}")]
    BindOutOfBound(u64, u64),
    #[error("execute failed: {0}")]
    ExecuteError(String),
}

impl PreparedStatement {
    /// Bind one parameter at the next position
    pub fn bind<T: BindParam>(&mut self, param: T) -> Result<&mut Self, PreparedStatementError> {
        self.bind_at(param, self.current_index)?;
        self.current_index += 1;
        Ok(self)
    }
    /// Reset current position. Parameters already bound are kept.
    pub fn reset(&mut self) -> &mut Self {
        self.set_position(1)
    }
    pub fn set_position(&mut self, param_idx: u64) -> &mut Self {
        let _nparams = self.handle.nparams();
        self.current_index = param_idx;
        self
    }
    /// Bind one paramer at specified position
    pub fn bind_at<T: BindParam>(
        &mut self,
        param: T,
        param_idx: u64,
    ) -> Result<(), PreparedStatementError> {
        let nparams = self.handle.nparams();
        if !(1..=nparams).contains(&param_idx) {
            return Err(PreparedStatementError::BindOutOfBound(param_idx, nparams));
        }
        unsafe { param.bind_param_unchecked(&self.handle, param_idx) }
            .map_err(|e| PreparedStatementError::BindError(e, param_idx))
    }
    pub fn execute(&self) -> Result<ArrowResult, PreparedStatementError> {
        self.handle
            .execute()
            .map_err(PreparedStatementError::ExecuteError)
            .map(ArrowResult::from)
    }
}

impl From<Arc<PreparedStatementHandle>> for PreparedStatement {
    fn from(value: Arc<PreparedStatementHandle>) -> Self {
        Self {
            handle: value,
            current_index: 1,
        }
    }
}
