use std::{ffi::NulError, str::Utf8Error};

use crate::ffi;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("duckdb_set_config() returns {0}: set {1}:{2} error")]
    ConfigSetError(ffi::duckdb_state, String, String),
    #[error("duckdb_open_ext() error: {0}")]
    OpenError(String),
    #[error("duckdb_connect() error")]
    ConnectError,
    #[error("duckdb_query() error: {0}")]
    QueryError(String),
    #[error(transparent)]
    NulError(#[from] NulError),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
