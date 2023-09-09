use std::{
    ffi::{c_char, CStr},
    ops::Deref,
    ptr,
    sync::Arc,
};

use crate::{config::ConfigHandle, connection::ConnectionHandle, ffi};

#[derive(Debug)]
pub struct DatabaseHandle(ffi::duckdb_database);

impl DatabaseHandle {
    pub unsafe fn from_raw(raw: ffi::duckdb_database) -> Arc<Self> {
        Arc::new(Self(raw))
    }
    pub unsafe fn open(path: *const c_char) -> Result<Arc<Self>, String> {
        Self::open_ext(path, &ConfigHandle::from_raw(ptr::null_mut()))
    }
    pub unsafe fn open_ext(
        path: *const c_char,
        config: &ConfigHandle,
    ) -> Result<Arc<Self>, String> {
        let mut db: ffi::duckdb_database = ptr::null_mut();
        let mut err: *mut c_char = ptr::null_mut();
        let r = ffi::duckdb_open_ext(path, &mut db, **config, &mut err);
        if r != ffi::DuckDBSuccess {
            let err_cstr = CStr::from_ptr(err);
            let err_str = err_cstr.to_string_lossy().to_string();
            return Err(err_str);
        }
        Ok(Self::from_raw(db))
    }

    pub unsafe fn connect(self: &Arc<Self>) -> Result<Arc<ConnectionHandle>, ()> {
        let mut handle = ptr::null_mut();
        let r = ffi::duckdb_connect(self.0, &mut handle);
        if r != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(ConnectionHandle::from_raw(handle, self.clone()))
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
        unsafe {
            ffi::duckdb_close(&mut self.0);
        }
    }
}

impl Deref for DatabaseHandle {
    type Target = ffi::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
