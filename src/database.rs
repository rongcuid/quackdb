use std::{
    ffi::{c_char, CString, NulError},
    path::Path,
    ptr,
    str::Utf8Error,
    sync::Arc,
};

use crate::{
    config::{Config, ConfigError},
    cutils::option_path_to_ptr,
    ffi,
};

pub type Database = Arc<InnerDatabase>;

pub type Result<T, E = DatabaseError> = std::result::Result<T, E>;

pub struct InnerDatabase {
    raw: ffi::duckdb_database,
}

impl InnerDatabase {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<impl AsRef<Path>>) -> Result<Database, DatabaseError> {
        Self::open_ext(path, &Config::default())
    }

    /// Extended open
    pub fn open_ext(
        path: Option<impl AsRef<Path>>,
        config: &Config,
    ) -> Result<Database, DatabaseError> {
        let p_path = option_path_to_ptr(path)?;
        let mut db: ffi::duckdb_database = ptr::null_mut();
        unsafe {
            let mut err: *mut c_char = ptr::null_mut();
            let r = ffi::duckdb_open_ext(p_path, &mut db, config.duckdb_config(), &mut err);
            if r != ffi::DuckDBSuccess {
                let err_cstr = CString::from_raw(err);
                let err_str = err_cstr.to_str()?;
                return Err(DatabaseError::DatabaseOpenError(
                    ffi::Error::new(r),
                    err_str.to_owned(),
                ));
            }
            Self::open_from_raw(db)
        }
    }

    #[inline]
    pub unsafe fn open_from_raw(raw: ffi::duckdb_database) -> Result<Database> {
        Ok(Arc::new(Self { raw }))
    }
}

impl Drop for InnerDatabase {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_close(&mut self.raw);
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    ConfigError(#[from] ConfigError),
    #[error("duckdb_open_ext() returns {0:?}: {1}")]
    DatabaseOpenError(ffi::Error, String),
    #[error(transparent)]
    NulError(#[from] NulError),
    #[error(transparent)]
    Utf8Error(#[from] Utf8Error),
}
