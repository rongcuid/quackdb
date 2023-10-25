use std::{
    ffi::{c_void, CStr},
    ops::Deref,
};

use crate::{ffi, types::LogicalTypeHandle};

pub struct InitInfoHandle {
    handle: ffi::duckdb_init_info,
}

impl InitInfoHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_init_info) -> Self {
        Self { handle }
    }
    pub fn get_extra_info(&self) -> *mut c_void {
        unsafe { ffi::duckdb_init_get_extra_info(**self) }
    }
    pub fn get_bind_data(&self) -> *mut c_void {
        unsafe { ffi::duckdb_init_get_bind_data(**self) }
    }
    /// # Safety
    /// * Takes ownership of `init_data`
    /// * `destroy` must outlive `self`
    /// * `destroy()` frees `init_data`
    pub unsafe fn set_init_data(
        &self,
        init_data: *mut c_void,
        destroy: ffi::duckdb_delete_callback_t,
    ) {
        ffi::duckdb_init_set_init_data(**self, init_data, destroy)
    }
    pub fn get_column_count(&self) -> u64 {
        unsafe { ffi::duckdb_init_get_column_count(**self) }
    }
    pub fn get_column_index(&self, column_index: u64) -> u64 {
        unsafe { ffi::duckdb_init_get_column_index(**self, column_index) }
    }
    pub fn set_max_threads(&self, max_threads: u64) {
        unsafe { ffi::duckdb_init_set_max_threads(**self, max_threads) }
    }
    pub fn set_error(&self, error: &CStr) {
        unsafe { ffi::duckdb_init_set_error(**self, error.as_ptr()) }
    }
}

impl Deref for InitInfoHandle {
    type Target = ffi::duckdb_init_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
