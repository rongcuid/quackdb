use std::{
    ffi::{c_void, CStr},
    ops::Deref,
    sync::Arc,
};

use crate::{ffi, handles::LogicalTypeHandle};

#[derive(Debug)]
pub struct TableFunctionHandle {
    raw: ffi::duckdb_table_function,
}

impl TableFunctionHandle {
    pub fn create() -> Arc<Self> {
        Arc::new(Self {
            raw: unsafe { ffi::duckdb_create_table_function() },
        })
    }
    pub fn set_name(&self, name: &CStr) {
        unsafe { ffi::duckdb_table_function_set_name(**self, name.as_ptr()) }
    }
    pub fn add_parameter(&self, type_: &LogicalTypeHandle) {
        unsafe { ffi::duckdb_table_function_add_parameter(**self, **type_) }
    }
    pub fn add_named_parameter(&self, name: &CStr, type_: &LogicalTypeHandle) {
        unsafe { ffi::duckdb_table_function_add_named_parameter(**self, name.as_ptr(), **type_) }
    }
    /// # Safety
    /// * Takes ownership of `*extra_info`
    /// * `destroy` must outlive self
    /// * `destroy()` frees `extra_info`
    pub unsafe fn set_extra_info(
        &self,
        extra_info: *mut c_void,
        destroy: ffi::duckdb_delete_callback_t,
    ) {
        ffi::duckdb_table_function_set_extra_info(**self, extra_info, destroy)
    }
    /// # Safety
    /// * `bind` must outlive self
    pub unsafe fn set_bind(&self, bind: ffi::duckdb_table_function_bind_t) {
        ffi::duckdb_table_function_set_bind(**self, bind)
    }
    /// # Safety
    /// * `init` must outlive self
    pub unsafe fn set_init(&self, init: ffi::duckdb_table_function_init_t) {
        ffi::duckdb_table_function_set_init(**self, init)
    }
    /// # Safety
    /// * `init` must outlive self
    pub unsafe fn set_local_init(&self, init: ffi::duckdb_table_function_init_t) {
        ffi::duckdb_table_function_set_local_init(**self, init)
    }
    /// # Safety
    /// * `init` must outlive self
    pub unsafe fn set_function(&self, function: ffi::duckdb_table_function_t) {
        ffi::duckdb_table_function_set_function(**self, function)
    }
    pub fn supports_projection_pushdown(&self, pushdown: bool) {
        unsafe { ffi::duckdb_table_function_supports_projection_pushdown(**self, pushdown) }
    }
}

impl Drop for TableFunctionHandle {
    fn drop(&mut self) {
        unsafe { ffi::duckdb_destroy_table_function(&mut self.raw) }
    }
}

impl Deref for TableFunctionHandle {
    type Target = ffi::duckdb_table_function;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}
