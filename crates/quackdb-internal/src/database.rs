use std::{ffi::CStr, ops::Deref, ptr, sync::Arc};

use crate::{config::ConfigHandle, connection::ConnectionHandle, ffi};

#[derive(Debug)]
pub struct DatabaseHandle(ffi::duckdb_database);

impl DatabaseHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_database) -> Arc<Self> {
        Arc::new(Self(raw))
    }
    pub unsafe fn open(path: &CStr) -> Result<Arc<Self>, String> {
        Self::open_ext(path, &ConfigHandle::from_raw(ptr::null_mut()))
    }
    pub fn open_ext(path: &CStr, config: &ConfigHandle) -> Result<Arc<Self>, String> {
        unsafe {
            let mut db: ffi::duckdb_database = ptr::null_mut();
            let mut err = ptr::null_mut();
            let r = ffi::duckdb_open_ext(path.as_ptr(), &mut db, **config, &mut err);
            if r != ffi::DuckDBSuccess {
                let err_cstr = CStr::from_ptr(err);
                let err_str = err_cstr.to_string_lossy().to_string();
                ffi::duckdb_free(err as _);
                return Err(err_str);
            }
            Ok(Self::from_raw(db))
        }
    }

    pub fn connect(self: &Arc<Self>) -> Result<Arc<ConnectionHandle>, ()> {
        let mut handle = ptr::null_mut();
        let r = unsafe { ffi::duckdb_connect(self.0, &mut handle) };
        if r != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(unsafe { ConnectionHandle::from_raw(handle, self.clone()) })
    }
    /// # Safety
    /// Force close connection without checking for usage.
    /// Normally you should let Rust handle this.
    pub unsafe fn close(&mut self) {
        ffi::duckdb_close(&mut self.0);
    }

    pub fn library_version() -> String {
        unsafe {
            let p = CStr::from_ptr(ffi::duckdb_library_version());
            p.to_string_lossy().to_owned().to_string()
        }
    }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        unsafe { self.close() }
    }
}

impl Deref for DatabaseHandle {
    type Target = ffi::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
