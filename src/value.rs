use std::ffi::{c_void, CStr, CString, NulError};

use crate::{
    error::{DbResult, Error},
    ffi,
};

pub struct RawValue {
    handle: ValueHandle,
}

pub(crate) struct ValueHandle(ffi::duckdb_value);

impl RawValue {
    pub fn from_varchar(text: &str) -> Result<Self, NulError> {
        let cstr = CString::new(text)?;
        unsafe { Ok(Self::from_raw(ffi::duckdb_create_varchar(cstr.as_ptr()))) }
    }
    pub fn from_i64(val: i64) -> Self {
        unsafe { Self::from_raw(ffi::duckdb_create_int64(val)) }
    }
    pub unsafe fn from_raw(handle: ffi::duckdb_value) -> Self {
        Self {
            handle: ValueHandle(handle),
        }
    }
    pub unsafe fn varchar_unchecked(&self) -> String {
        let p = ffi::duckdb_get_varchar(self.handle.0);
        let text = CStr::from_ptr(p).to_string_lossy().to_string();
        ffi::duckdb_free(p as *mut c_void);
        text
    }
    pub unsafe fn i64_unchecked(&self) -> i64 {
        ffi::duckdb_get_int64(self.handle.0)
    }
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_value(&mut self.0);
        }
    }
}
