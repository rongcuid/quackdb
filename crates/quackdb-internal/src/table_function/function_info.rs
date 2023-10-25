use std::{
    ffi::{c_void, CStr},
    ops::Deref,
};

use crate::{ffi, types::LogicalTypeHandle};

pub struct FunctionInfoHandle {
    handle: ffi::duckdb_function_info,
}

impl FunctionInfoHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_function_info) -> Self {
        Self { handle }
    }
    pub fn get_extra_info(&self) -> *mut c_void {
        unsafe { ffi::duckdb_function_get_extra_info(**self) }
    }
    pub fn get_bind_data(&self) -> *mut c_void {
        unsafe { ffi::duckdb_function_get_bind_data(**self) }
    }
    pub fn get_init_data(&self) -> *mut c_void {
        unsafe { ffi::duckdb_function_get_init_data(**self) }
    }
    pub fn get_local_init_data(&self) -> *mut c_void {
        unsafe { ffi::duckdb_function_get_local_init_data(**self) }
    }
    /// TODO: check the lifetime of `error`
    pub fn set_error(&self, error: &CStr) {
        unsafe { ffi::duckdb_function_set_error(**self, error.as_ptr()) }
    }
}

impl Deref for FunctionInfoHandle {
    type Target = ffi::duckdb_function_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
