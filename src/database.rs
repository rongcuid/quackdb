use std::{
    ffi::{CStr, NulError},
    ops::Deref,
    path::Path,
    ptr,
    sync::Arc,
};

use quackdb_internal::{connection::ConnectionHandle, database::DatabaseHandle, ffi};

use crate::{config::Config, connection::Connection, cutils::option_path_to_cstring};

#[derive(Debug)]
pub struct Database {
    handle: Arc<DatabaseHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("duckdb open error: {0}")]
    OpenError(String),
    #[error("duckdb connect error")]
    ConnectError,
    #[error(transparent)]
    NulError(#[from] NulError),
}

impl From<Arc<DatabaseHandle>> for Database {
    fn from(value: Arc<DatabaseHandle>) -> Self {
        Self { handle: value }
    }
}

impl Database {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<&Path>) -> Result<Self, DatabaseError> {
        Self::open_ext(path, None)
    }

    /// Extended open
    pub fn open_ext(path: Option<&Path>, config: Option<&Config>) -> Result<Self, DatabaseError> {
        let c_path = option_path_to_cstring(path)?;
        let mut db: ffi::duckdb_database = ptr::null_mut();
        let mut err = ptr::null_mut();
        let path = c_path.map(|p| p.as_ptr()).unwrap_or(ptr::null());
        let config = config.map(|c| ***c).unwrap_or(ptr::null_mut());
        let r = unsafe { ffi::duckdb_open_ext(path, &mut db, config, &mut err) };
        if r != ffi::DuckDBSuccess {
            let err_cstr = unsafe { CStr::from_ptr(err) };
            let err_str = err_cstr.to_string_lossy().to_string();
            unsafe { ffi::duckdb_free(err as _) };
            return Err(DatabaseError::OpenError(err_str));
        }
        Ok(Self {
            handle: unsafe { DatabaseHandle::from_raw(db) },
        })
    }

    pub fn connect(&self) -> Result<Connection, DatabaseError> {
        let mut handle = ptr::null_mut();
        let r = unsafe { ffi::duckdb_connect(***self, &mut handle) };
        if r != ffi::DuckDBSuccess {
            return Err(DatabaseError::ConnectError);
        }
        Ok(unsafe { ConnectionHandle::from_raw(handle, self.handle.clone()) }.into())
    }
}

impl Deref for Database {
    type Target = DatabaseHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

/// Some tests are adapted from `duckdb-rs`
#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn test_open() {
        let db = Database::open(None);
        assert!(db.is_ok());
    }

    #[test]
    fn test_open_failure() -> Result<(), DatabaseError> {
        let filename = "no_such_file.db";
        let result = Database::open_ext(
            Some(filename.as_ref()),
            Some(
                Config::new()
                    .unwrap()
                    .set("access_mode", "read_only")
                    .unwrap(),
            ),
        );
        match result {
            Ok(_) => panic!("Should fail"),
            Err(DatabaseError::OpenError(_)) => (),
            Err(e) => panic!("Unexpected error: {e}"),
        }
        Ok(())
    }
}
