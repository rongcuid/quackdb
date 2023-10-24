use std::{
    ffi::{c_void, CStr},
    ops::Deref,
};

use crate::{ffi, types::LogicalTypeHandle};

pub struct BindInfoHandle {
    handle: ffi::duckdb_bind_info,
}

impl BindInfoHandle {
    pub fn from_raw(handle: ffi::duckdb_bind_info) -> Self {
        Self { handle }
    }
    pub fn get_extra_info(&self) -> *mut c_void {
        unsafe { ffi::duckdb_bind_get_extra_info(**self) }
    }
    pub fn add_result_column(&self, name: &CStr, type_: &LogicalTypeHandle) {
        unsafe { ffi::duckdb_bind_add_result_column(**self, name.as_ptr(), **type_) }
    }
    pub fn get_parameter_count(&self) -> u64 {
        unsafe { ffi::duckdb_bind_get_parameter_count(**self) }
    }
    /// # Safety
    /// * Index must be in range
    /// * Result must be destroyed
    pub unsafe fn get_parameter(&self, index: u64) -> ffi::duckdb_value {
        ffi::duckdb_bind_get_parameter(**self, index)
    }
    /// # Safety
    /// * Index must be in range
    /// * Result must be destroyed
    pub unsafe fn get_named_parameter(&self, name: &CStr) -> ffi::duckdb_value {
        ffi::duckdb_bind_get_named_parameter(**self, name.as_ptr())
    }
    /// # Safety
    /// * Takes ownership of `bind_data`
    /// * `destroy` must outlive `self`
    /// * `destroy()` frees `bind_data`
    pub unsafe fn set_bind_data(
        &self,
        bind_data: *mut c_void,
        destroy: ffi::duckdb_delete_callback_t,
    ) {
        ffi::duckdb_bind_set_bind_data(**self, bind_data, destroy)
    }
    pub fn set_cardinality(&self, cardinality: u64, is_exact: bool) {
        unsafe { ffi::duckdb_bind_set_cardinality(**self, cardinality, is_exact) }
    }
    /// TODO: check the lifetime of `error`
    pub fn set_error(&self, error: &CStr) {
        unsafe { ffi::duckdb_bind_set_error(**self, error.as_ptr()) }
    }
}

impl Deref for BindInfoHandle {
    type Target = ffi::duckdb_bind_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
