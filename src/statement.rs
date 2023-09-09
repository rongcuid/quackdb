use std::sync::Arc;

use quackdb_internal::statement::PreparedStatementHandle;

#[derive(Debug)]
pub struct PreparedStatement {
    pub handle: Arc<PreparedStatementHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum PreparedStatementError {
    #[error("duckdb_clear_bindings() error")]
    ClearBindingsError,
    #[error("prepared statement bind error")]
    BindError,
    #[error("prepared statement execute error")]
    ExecuteError,
}

impl PreparedStatement {}

impl From<Arc<PreparedStatementHandle>> for PreparedStatement {
    fn from(value: Arc<PreparedStatementHandle>) -> Self {
        Self { handle: value }
    }
}
