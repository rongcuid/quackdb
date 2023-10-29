use std::ops::Deref;

use crate::{ffi, type_id::TypeId};

#[derive(Debug)]
pub struct LogicalTypeHandle {
    handle: ffi::duckdb_logical_type,
}

impl LogicalTypeHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Self {
        Self { handle }
    }
    pub unsafe fn from_id(type_: TypeId) -> Self {
        Self::from_raw(ffi::duckdb_create_logical_type(type_ as _))
    }
    pub fn type_id(&self) -> Option<TypeId> {
        unsafe { TypeId::from_repr(ffi::duckdb_get_type_id(self.handle)) }
    }
}

impl Deref for LogicalTypeHandle {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for LogicalTypeHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut self.handle);
        }
    }
}
