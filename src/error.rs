use arrow::error::ArrowError;
use thiserror::Error;

use crate::{
    appender::AppenderError, arrow::ArrowResultError, connection::ConnectionError,
    database::DatabaseError,
};

/// Convenience error type encompassing all sub-errors
#[derive(Error, Debug)]
pub enum QuackError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    #[error(transparent)]
    Appender(#[from] AppenderError),
    #[error(transparent)]
    ArrowResult(#[from] ArrowResultError),
    #[error(transparent)]
    Arrow(#[from] ArrowError),
}
