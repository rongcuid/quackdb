use std::{
    ffi::{c_void, CStr},
    ops::Deref,
};

use crate::ffi;

#[derive(Debug)]
pub struct ValueHandle(ffi::duckdb_value);

impl Deref for ValueHandle {
    type Target = ffi::duckdb_value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        unsafe { self.destroy() }
    }
}

impl ValueHandle {
    pub fn create_varchar(text: &CStr) -> Self {
        Self(unsafe { ffi::duckdb_create_varchar(text.as_ptr()) })
    }
    pub fn create_i64(val: i64) -> Self {
        unsafe { Self(ffi::duckdb_create_int64(val)) }
    }
    /// # Safety
    /// Does not consider usage. Normally, let `Drop` handle this.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_value(&mut self.0);
    }
    /// # Safety
    /// The value must be a varchar value
    pub unsafe fn varchar(&self) -> String {
        let p = ffi::duckdb_get_varchar(self.0);
        let text = CStr::from_ptr(p).to_string_lossy().to_owned().to_string();
        ffi::duckdb_free(p as *mut c_void);
        text
    }
    /// # Safety
    /// The value must be an int64 value
    pub unsafe fn i64(&self) -> i64 {
        ffi::duckdb_get_int64(self.0)
    }
}
