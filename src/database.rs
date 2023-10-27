use cstr::cstr;
use std::{
    ffi::{c_char, c_void, CStr, CString, NulError},
    ops::Deref,
    path::Path,
    ptr,
    sync::Arc,
};

use quackdb_internal::{
    ffi,
    handles::{ConnectionHandle, DatabaseHandle, ReplacementScanInfoHandle},
};

use crate::{
    config::Config,
    connection::Connection,
    cutils::option_path_to_cstring,
    replacement_scan::{ReplacementScanError, ReplacementScanInfo},
};

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
        let r = unsafe { ffi::duckdb_connect(**self, &mut handle) };
        if r != ffi::DuckDBSuccess {
            return Err(DatabaseError::ConnectError);
        }
        Ok(unsafe { ConnectionHandle::from_raw(handle, self.handle.clone()) }.into())
    }

    pub fn add_replacement_scan<F, D, E>(&self, replacement: F, extra: D)
    where
        E: std::error::Error,
        F: Fn(&ReplacementScanInfo, String, &D) -> Result<(), E>,
    {
        struct ExtraData<F, D> {
            replacement: F,
            extra: D,
        }
        extern "C" fn f<F, D, E: std::error::Error>(
            info: ffi::duckdb_replacement_scan_info,
            table_name: *const c_char,
            data: *mut c_void,
        ) where
            F: Fn(&ReplacementScanInfo, String, &D) -> Result<(), E>,
        {
            let data: *const ExtraData<F, D> = data.cast();
            let info: ReplacementScanInfo =
                unsafe { ReplacementScanInfoHandle::from_raw(info) }.into();
            let table_name = unsafe { CStr::from_ptr(table_name) }
                .to_string_lossy()
                .into_owned();
            let res = unsafe { ((*data).replacement)(&info, table_name, &(*data).extra) };
            if let Err(e) = res {
                let msg = CString::new(e.to_string());
                let cstr = msg.as_deref().unwrap_or(cstr!(
                    "replacement scan callback returns error string with Nul"
                ));
                unsafe { ffi::duckdb_replacement_scan_set_error(*info, cstr.as_ptr()) }
            }
        }
        extern "C" fn drop_extra_data<F, D>(ptr: *mut c_void) {
            unsafe { drop::<Box<ExtraData<F, D>>>(Box::from_raw(ptr.cast())) }
        }
        let extra_data = Box::new(ExtraData { replacement, extra });
        unsafe {
            ffi::duckdb_add_replacement_scan(
                **self,
                Some(f::<F, D, E>),
                Box::into_raw(extra_data).cast(),
                Some(drop_extra_data::<F, D>),
            );
        }
    }
}

impl Deref for Database {
    type Target = ffi::duckdb_database;

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
