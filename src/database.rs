use std::{
    ffi::{c_char, CStr, NulError},
    path::Path,
    ptr,
    str::Utf8Error,
    sync::Arc,
};

use crate::{
    config::Config,
    connection::{Connection, ConnectionError},
    cutils::option_path_to_ptr,
    error::*,
    ffi,
};

#[derive(Debug, Clone)]
pub struct Database {
    pub(crate) handle: Arc<DatabaseHandle>,
}

#[derive(Debug)]
pub(crate) struct DatabaseHandle(pub(crate) ffi::duckdb_database);

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("duckdb_open_ext() error: {0}")]
    OpenError(String),
}

impl Database {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<&Path>) -> DbResult<Self, DatabaseError> {
        Self::open_ext(path, &Config::default())
    }

    /// Extended open
    pub fn open_ext(path: Option<&Path>, config: &Config) -> DbResult<Self, DatabaseError> {
        let p_path = option_path_to_ptr(path)?;
        let mut db: ffi::duckdb_database = ptr::null_mut();
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            let r = ffi::duckdb_open_ext(p_path, &mut db, config.duckdb_config(), &mut err);
            if r != ffi::DuckDBSuccess {
                let err_cstr = CStr::from_ptr(err);
                let err_str = err_cstr.to_str()?;
                return Ok(Err(DatabaseError::OpenError(err_str.to_owned())));
            }
            Self::open_from_raw(db)
        }
    }

    #[inline]
    pub unsafe fn open_from_raw(raw: ffi::duckdb_database) -> DbResult<Self, DatabaseError> {
        Ok(Ok(Self {
            handle: Arc::new(DatabaseHandle(raw)),
        }))
    }

    pub fn connect(&self) -> DbResult<Connection, ConnectionError> {
        Ok(Connection::connect(&self)?)
    }

    pub fn library_version() -> DbResult<String, DatabaseError> {
        let mut p: *const c_char = ptr::null();
        unsafe {
            p = ffi::duckdb_library_version();
            Ok(Ok(CStr::from_ptr(p).to_str()?.to_owned()))
        }
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_close(&mut self.0);
        }
    }
}

/// Some tests are adapted from `duckdb-rs`
#[cfg(test)]
mod test {
    use crate::config::AccessMode;

    use super::*;
    #[test]
    fn test_open() {
        let db = Database::open(None);
        assert!(db.is_ok());
    }

    #[test]
    fn test_open_failure() -> DbResult<(), DatabaseError> {
        let filename = "no_such_file.db";
        let result = Database::open_ext(
            // Some(Path::new(filename)).as_deref(),
            Some(filename.as_ref()),
            &Config::default()
                .access_mode(AccessMode::ReadOnly)?
                .unwrap(),
        );
        assert!(matches!(result, Ok(Err(DatabaseError::OpenError(_)))));
        Ok(Ok(()))
    }
}
