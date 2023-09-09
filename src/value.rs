use std::{
    ffi::{c_char, c_void, CStr},
    ops::Deref,
};

use crate::ffi;

#[derive(Debug)]
pub struct Value {
    pub handle: ValueHandle,
}

#[derive(Debug)]
pub struct ValueHandle(ffi::duckdb_value);

impl From<ValueHandle> for Value {
    fn from(value: ValueHandle) -> Self {
        Self { handle: value }
    }
}

impl Value {
    pub unsafe fn from_raw(raw: ffi::duckdb_value) -> Self {
        ValueHandle(raw).into()
    }
}

impl ValueHandle {
    pub unsafe fn create_varchar(text: *const c_char) -> Self {
        Self(ffi::duckdb_create_varchar(text))
    }
    pub unsafe fn create_i64(val: i64) -> Self {
        Self(ffi::duckdb_create_int64(val))
    }

    pub unsafe fn varchar(&self) -> String {
        let p = ffi::duckdb_get_varchar(self.0);
        let text = CStr::from_ptr(p).to_string_lossy().to_string();
        ffi::duckdb_free(p as *mut c_void);
        text
    }
    pub unsafe fn i64(&self) -> i64 {
        ffi::duckdb_get_int64(self.0)
    }
}

impl Deref for ValueHandle {
    type Target = ffi::duckdb_value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_value(&mut self.0);
        }
    }
}
