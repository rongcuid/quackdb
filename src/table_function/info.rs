use std::{ffi::CStr, ops::Deref};

use quackdb_internal::ffi;

use crate::types::LogicalType;

pub struct BindInfo {
    handle: ffi::duckdb_bind_info,
}

impl From<ffi::duckdb_bind_info> for BindInfo {
    fn from(handle: ffi::duckdb_bind_info) -> Self {
        Self { handle }
    }
}

impl BindInfo {
    pub fn add_result_column(&self, name: &CStr, type_: &LogicalType) {
        unsafe { ffi::duckdb_bind_add_result_column(**self, name.as_ptr(), **type_) }
    }
    pub fn parameter_count(&self) -> u64 {
        unsafe { ffi::duckdb_bind_get_parameter_count(**self) }
    }
    pub fn set_cardinality(&self, cardinality: u64, is_exact: bool) {
        unsafe { ffi::duckdb_bind_set_cardinality(**self, cardinality, is_exact) }
    }
    /// # Safety
    /// * Index must be in range
    /// * Result must be destroyed
    pub unsafe fn parameter(&self, index: u64) -> ffi::duckdb_value {
        ffi::duckdb_bind_get_parameter(**self, index)
    }
    /// # Safety
    /// * Index must be in range
    /// * Result must be destroyed
    pub unsafe fn named_parameter(&self, name: &CStr) -> ffi::duckdb_value {
        ffi::duckdb_bind_get_named_parameter(**self, name.as_ptr())
    }
}

impl Deref for BindInfo {
    type Target = ffi::duckdb_bind_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

pub struct InitInfo {
    handle: ffi::duckdb_init_info,
}

impl From<ffi::duckdb_init_info> for InitInfo {
    fn from(handle: ffi::duckdb_init_info) -> Self {
        Self { handle }
    }
}

impl InitInfo {
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_init_get_column_count(**self) }
    }
    pub fn column_index(&self, column_index: u64) -> u64 {
        unsafe { ffi::duckdb_init_get_column_index(**self, column_index) }
    }
    pub fn set_max_threads(&self, max_threads: u64) {
        unsafe { ffi::duckdb_init_set_max_threads(**self, max_threads) }
    }
}

impl Deref for InitInfo {
    type Target = ffi::duckdb_init_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

pub struct FunctionInfo {
    pub handle: ffi::duckdb_function_info,
}

impl From<ffi::duckdb_function_info> for FunctionInfo {
    fn from(handle: ffi::duckdb_function_info) -> Self {
        Self { handle }
    }
}

impl Deref for FunctionInfo {
    type Target = ffi::duckdb_function_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
