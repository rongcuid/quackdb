use std::{ffi::c_char, ops::Deref, ptr::NonNull, sync::Arc};

use crate::{ffi, types::LogicalTypeHandle};

#[derive(Debug)]
pub struct VectorHandle {
    handle: ffi::duckdb_vector,
    _parent: Option<Arc<VectorHandle>>,
}

#[derive(Debug)]
pub struct ValidityHandle(*mut u64);

impl Deref for VectorHandle {
    type Target = ffi::duckdb_vector;

    fn deref(&self) -> &Self::Target {
        &self.handle
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

    pub unsafe fn column_type(&self) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_vector_get_column_type(self.handle))
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<ValidityHandle> {
        unsafe {
            let v = ffi::duckdb_vector_get_validity(self.handle);
            let handle = NonNull::new(v)?.as_ptr();
            Some(ValidityHandle::from_raw(handle))
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

    pub unsafe fn assign_string_element_len(&self, index: u64, str: *const c_char, str_len: u64) {
        ffi::duckdb_vector_assign_string_element_len(self.handle, index, str, str_len);
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

impl Deref for ValidityHandle {
    type Target = *mut u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ValidityHandle {
    pub unsafe fn from_raw(raw: *mut u64) -> Self {
        Self(raw)
    }
    pub unsafe fn row_is_valid(&self, row: u64) -> bool {
        unsafe { ffi::duckdb_validity_row_is_valid(self.0, row) }
    }
    pub unsafe fn set_row_validity(&self, row: u64, valid: bool) {
        ffi::duckdb_validity_set_row_validity(self.0, row, valid);
    }
}
