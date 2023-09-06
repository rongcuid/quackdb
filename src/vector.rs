use crate::{ffi, logical_type::LogicalType, types::RawType};

pub struct Vector {
    handle: VectorHandle,
}

impl Vector {
    pub unsafe fn from_raw(handle: ffi::duckdb_vector) -> Self {
        Self {
            handle: VectorHandle(handle),
        }
    }
    pub fn column_type(&self) -> Option<LogicalType> {
        unsafe { LogicalType::from_raw(ffi::duckdb_vector_get_column_type(self.handle.0)) }
    }
    // pub fn data(&self)
    // pub fn get_validity
    pub fn ensure_validity_writable(&self) {
        unsafe {
            ffi::duckdb_vector_ensure_validity_writable(self.handle.0);
        }
    }
}
pub(crate) struct VectorHandle(ffi::duckdb_vector);
