use std::{
    borrow::BorrowMut,
    ffi::{c_char, CString},
    ops::Deref,
    ptr::NonNull,
    sync::Arc,
};

use crate::{error::Error, ffi, types::LogicalType, types::TypeId};

use super::{Validity, ValidityHandle};

#[derive(Debug)]
pub struct Vector {
    pub handle: Arc<VectorHandle>,
}

#[derive(Debug)]
pub struct VectorHandle {
    handle: ffi::duckdb_vector,
    _parent: Option<Arc<VectorHandle>>,
}

impl From<Arc<VectorHandle>> for Vector {
    fn from(value: Arc<VectorHandle>) -> Self {
        Self { handle: value }
    }
}

impl Vector {
    pub fn column_type(&self) -> Option<LogicalType> {
        self.handle.column_type()
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<Validity> {
        self.handle.validity()
    }
    pub fn ensure_validity_writable(&self) {
        self.handle.ensure_validity_writable()
    }
    pub fn assign_string_element(&self, index: u64, str: &str) -> Result<(), Error> {
        todo!()
    }
    pub fn child(&self) {
        todo!()
    }
    pub fn set_size(&mut self) {
        todo!()
    }
    pub fn reserve(&mut self) {
        todo!()
    }
}
impl VectorHandle {
    pub unsafe fn from_raw(raw: ffi::duckdb_vector) -> Arc<Self> {
        assert!(raw != std::ptr::null_mut());
        Arc::new(Self {
            handle: raw,
            _parent: None,
        })
    }

    pub fn column_type(&self) -> Option<LogicalType> {
        unsafe { LogicalType::from_raw(ffi::duckdb_vector_get_column_type(self.handle)) }
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<Validity> {
        unsafe {
            let v = ffi::duckdb_vector_get_validity(self.handle);
            let handle = NonNull::new(v)?.as_ptr();
            Some(ValidityHandle::from_raw(handle).into())
        }
    }
    pub fn ensure_validity_writable(&self) {
        unsafe {
            ffi::duckdb_vector_ensure_validity_writable(self.handle);
        }
    }
    pub unsafe fn assign_string_element(&self, index: u64, str: *const c_char) {
        ffi::duckdb_vector_assign_string_element(self.handle, index, str);
    }

    pub unsafe fn assign_string_element_len(
        &self,
        index: u64,
        str: *const c_char,
        str_len: u64,
    ) -> Result<(), Error> {
        ffi::duckdb_vector_assign_string_element_len(self.handle, index, str, str_len);
        Ok(())
    }
    pub unsafe fn list_child(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            handle: ffi::duckdb_list_vector_get_child(self.handle),
            _parent: Some(self.clone()),
        })
    }
    pub unsafe fn list_size(&self) -> u64 {
        ffi::duckdb_list_vector_get_size(self.handle)
    }
    pub unsafe fn list_set_size(&self, size: u64) {
        let res = ffi::duckdb_list_vector_set_size(self.handle, size);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    pub unsafe fn list_reserve(&self, required_capacity: u64) {
        let res = ffi::duckdb_list_vector_reserve(self.handle, required_capacity);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    pub unsafe fn struct_child(self: &Arc<Self>, index: u64) -> Arc<Self> {
        Arc::new(Self {
            handle: ffi::duckdb_struct_vector_get_child(self.handle, index),
            _parent: Some(self.clone()),
        })
    }
}

impl Deref for VectorHandle {
    type Target = ffi::duckdb_vector;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
