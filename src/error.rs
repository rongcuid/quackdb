use std::{ffi::NulError, str::Utf8Error};

use crate::ffi;

/// Global/system error that is not related to individual operations
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NulError(#[from] NulError),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
}

pub type DbResult<T, E> = Result<Result<T, E>, Error>;
