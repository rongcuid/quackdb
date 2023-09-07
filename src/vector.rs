use std::sync::Arc;

use crate::{ffi, logical_type::LogicalType, types::RawType};

pub struct Vector {
    handle: VectorHandle,
    parent: Option<Arc<Vector>>,
}

impl Vector {
    pub unsafe fn from_raw(handle: ffi::duckdb_vector, parent: Option<Arc<Vector>>) -> Arc<Self> {
        Arc::new(Self {
            handle: VectorHandle(handle),
            parent,
        })
    }
    pub fn column_type(&self) -> Option<LogicalType> {
        unsafe { LogicalType::from_raw(ffi::duckdb_vector_get_column_type(self.handle.0)) }
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) {
        todo!()
    }
    pub fn ensure_validity_writable(&self) {
        unsafe {
            ffi::duckdb_vector_ensure_validity_writable(self.handle.0);
        }
    }
    pub fn list_child(self: &Arc<Self>) -> Arc<Vector> {
        unsafe {
            Vector::from_raw(
                ffi::duckdb_list_vector_get_child(self.handle.0),
                Some(self.clone()),
            )
        }
    }
}
pub(crate) struct VectorHandle(ffi::duckdb_vector);
