use std::{ffi::CStr, ops::Deref};

use crate::{ffi, type_id::TypeId};

#[derive(Debug)]
pub struct LogicalTypeHandle(ffi::duckdb_logical_type);

impl LogicalTypeHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Self {
        Self(handle)
    }
    pub unsafe fn from_id(type_: TypeId) -> Self {
        Self::from_raw(ffi::duckdb_create_logical_type(type_.to_raw()))
    }
    pub fn type_id(&self) -> Option<TypeId> {
        unsafe { TypeId::from_raw(ffi::duckdb_get_type_id(self.0)) }
    }
}

impl Deref for LogicalTypeHandle {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for LogicalTypeHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut self.0);
        }
    }
}
