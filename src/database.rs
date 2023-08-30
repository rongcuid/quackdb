use std::{
    ffi::{c_char, CStr, NulError},
    path::Path,
    ptr,
    str::Utf8Error,
    sync::Arc,
};

use crate::{
    config::{Config, ConfigError},
    connection::{Connection, ConnectionError},
    cutils::option_path_to_ptr,
    ffi,
};

pub type Result<T, E = DatabaseError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Database {
    pub(crate) handle: ffi::duckdb_database,
}

impl Database {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<&Path>) -> Result<Arc<Database>, DatabaseError> {
        Self::open_ext(path, &Config::default())
    }

    /// Extended open
    pub fn open_ext(path: Option<&Path>, config: &Config) -> Result<Arc<Database>, DatabaseError> {
        let p_path = option_path_to_ptr(path)?;
        let mut db: ffi::duckdb_database = ptr::null_mut();
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            let r = ffi::duckdb_open_ext(p_path, &mut db, config.duckdb_config(), &mut err);
            if r != ffi::DuckDBSuccess {
                let err_cstr = CStr::from_ptr(err);
                let err_str = err_cstr.to_str()?;
                return Err(DatabaseError::OpenError(err_str.to_owned()));
            }
            Self::open_from_raw(db)
        }
    }

    #[inline]
    pub unsafe fn open_from_raw(raw: ffi::duckdb_database) -> Result<Arc<Database>> {
        Ok(Arc::new(Self { handle: raw }))
    }

    pub fn connect(self: &Arc<Self>) -> Result<Arc<Connection>> {
        Ok(Connection::connect(self.clone())?)
    }

    pub fn library_version() -> Result<String> {
        let mut p: *const c_char = ptr::null();
        unsafe {
            p = ffi::duckdb_library_version();
            Ok(CStr::from_ptr(p).to_str()?.to_owned())
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_close(&mut self.handle);
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
    #[error("duckdb_open_ext() error: {0}")]
    OpenError(String),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
    #[error(transparent)]
    NulError(#[from] NulError),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
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
    fn test_open_failure() -> Result<()> {
        let filename = "no_such_file.db";
        let result = Database::open_ext(
            // Some(Path::new(filename)).as_deref(),
            Some(filename.as_ref()),
            &Config::default().access_mode(AccessMode::ReadOnly)?,
        );
        assert!(result.is_err());
        assert!(matches!(result, Err(DatabaseError::OpenError(_))));
        Ok(())
    }
}
