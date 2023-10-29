use std::ops::Deref;

use crate::{ffi, type_id::TypeId};

#[derive(Debug)]
pub struct LogicalTypeHandle {
    raw: ffi::duckdb_logical_type,
}

impl LogicalTypeHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_logical_type) -> Self {
        Self { raw }
    }
    /// # Safety
    /// * ID must not be Decimal
    pub unsafe fn from_id(type_: TypeId) -> Self {
        Self::from_raw(ffi::duckdb_create_logical_type(type_ as _))
    }
    pub fn type_id(&self) -> Option<TypeId> {
        unsafe { TypeId::from_repr(ffi::duckdb_get_type_id(self.raw)) }
    }
}

impl Deref for LogicalTypeHandle {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}

impl Drop for LogicalTypeHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut self.raw);
        }
    }
}
