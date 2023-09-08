use std::{ffi::CString, ops::Deref, ptr::NonNull, sync::Arc};

use crate::{error::Error, ffi, types::LogicalType, types::TypeId};

use super::Validity;

#[derive(Debug)]
pub struct Vector {
    handle: VectorHandle,
    parent: Option<Arc<Vector>>,
}

#[derive(Debug)]
pub(crate) struct VectorHandle(ffi::duckdb_vector);

impl Vector {
    pub unsafe fn from_raw(handle: ffi::duckdb_vector, parent: Option<Arc<Vector>>) -> Arc<Self> {
        Arc::new(Self {
            handle: VectorHandle(handle),
            parent,
        })
    }
    pub fn column_type(&self) -> Option<LogicalType> {
        unsafe { LogicalType::from_raw(ffi::duckdb_vector_get_column_type(*self.handle)) }
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<Validity> {
        unsafe {
            let v = ffi::duckdb_vector_get_validity(*self.handle);
            let handle = NonNull::new(v)?.as_ptr();
            Some(Validity::from_raw(handle))
        }
    }
    pub fn ensure_validity_writable(&self) {
        unsafe {
            ffi::duckdb_vector_ensure_validity_writable(*self.handle);
        }
    }
    pub unsafe fn assign_string_element_unchecked(
        &self,
        index: u64,
        str: &str,
    ) -> Result<(), Error> {
        let cstr = CString::new(str)?;
        ffi::duckdb_vector_assign_string_element(*self.handle, index, cstr.as_ptr());
        Ok(())
    }

    pub unsafe fn assign_string_element_len_unchecked(
        &self,
        index: u64,
        str: &str,
        str_len: u64,
    ) -> Result<(), Error> {
        let cstr = CString::new(str)?;
        ffi::duckdb_vector_assign_string_element_len(*self.handle, index, cstr.as_ptr(), str_len);
        Ok(())
    }
    pub unsafe fn list_child_unchecked(self: &Arc<Self>) -> Arc<Vector> {
        Vector::from_raw(
            ffi::duckdb_list_vector_get_child(*self.handle),
            Some(self.clone()),
        )
    }
    pub unsafe fn list_size_unchecked(&self) -> u64 {
        ffi::duckdb_list_vector_get_size(*self.handle)
    }
    pub unsafe fn list_set_size_unchecked(&self, size: u64) {
        let res = ffi::duckdb_list_vector_set_size(*self.handle, size);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    pub unsafe fn list_reserve(&self, required_capacity: u64) {
        let res = ffi::duckdb_list_vector_reserve(*self.handle, required_capacity);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    pub unsafe fn struct_child_unchecked(self: &Arc<Self>, index: u64) -> Arc<Vector> {
        Vector::from_raw(
            ffi::duckdb_struct_vector_get_child(*self.handle, index),
            Some(self.clone()),
        )
    }
}

impl Deref for VectorHandle {
    type Target = ffi::duckdb_vector;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
