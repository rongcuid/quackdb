use std::{ffi::c_char, ops::Deref, ptr::NonNull, sync::Arc};

use crate::{data_chunks::DataChunkHandle, ffi, types::LogicalTypeHandle};

use super::{DataHandle, ValidityHandle};

#[derive(Debug)]
pub struct VectorHandle {
    handle: ffi::duckdb_vector,
    parent: VectorParent,
}

#[derive(Debug)]
pub enum VectorParent {
    DataChunk(Arc<DataChunkHandle>),
    ListVector(Arc<VectorHandle>),
    StructVector(Arc<VectorHandle>),
}

impl Deref for VectorHandle {
    type Target = ffi::duckdb_vector;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl VectorHandle {
    /// # Safety
    /// Takes ownership.
    pub unsafe fn from_raw_data_chunk(
        raw: ffi::duckdb_vector,
        parent: Arc<DataChunkHandle>,
    ) -> Arc<Self> {
        assert!(raw != std::ptr::null_mut());
        Arc::new(Self {
            handle: raw,
            parent: VectorParent::DataChunk(parent),
        })
    }
    /// # Safety
    /// Takes ownership.
    pub unsafe fn from_raw_list_vector(
        raw: ffi::duckdb_vector,
        parent: Arc<VectorHandle>,
    ) -> Arc<Self> {
        assert!(raw != std::ptr::null_mut());
        Arc::new(Self {
            handle: raw,
            parent: VectorParent::ListVector(parent),
        })
    }
    /// # Safety
    /// Takes ownership.
    pub unsafe fn from_raw_struct_vector(
        raw: ffi::duckdb_vector,
        parent: Arc<VectorHandle>,
    ) -> Arc<Self> {
        assert!(raw != std::ptr::null_mut());
        Arc::new(Self {
            handle: raw,
            parent: VectorParent::StructVector(parent),
        })
    }
    pub fn size(&self) -> u64 {
        unsafe {
            match &self.parent {
                VectorParent::DataChunk(d) => d.size(),
                VectorParent::ListVector(v) => v.list_size(),
                VectorParent::StructVector(_) => self.column_type().struct_type_child_count(),
            }
        }
    }
    pub fn column_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::from_raw(ffi::duckdb_vector_get_column_type(self.handle)) }
    }
    pub fn data(&self) -> Option<DataHandle> {
        unsafe {
            let d = ffi::duckdb_vector_get_data(self.handle);
            let handle = NonNull::new(d)?.as_ptr();
            Some(DataHandle::from_raw(handle))
        }
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
    /// # Safety
    /// Ensure:
    /// * This is string vector
    /// * `index` is in range
    /// * `str` is null-terminated
    pub unsafe fn assign_string_element(&self, index: u64, str: *const c_char) {
        ffi::duckdb_vector_assign_string_element(self.handle, index, str);
    }
    /// # Safety
    /// Ensure:
    /// * This is string vector
    /// * `index` is in range
    /// * `str_len` does not cause out-of-bounds access
    pub unsafe fn assign_string_element_len(&self, index: u64, str: *const c_char, str_len: u64) {
        ffi::duckdb_vector_assign_string_element_len(self.handle, index, str, str_len);
    }
    /// # Safety
    /// This must be a list vector
    pub unsafe fn list_child(self: &Arc<Self>) -> Arc<Self> {
        Self::from_raw_list_vector(ffi::duckdb_list_vector_get_child(self.handle), self.clone())
    }
    /// # Safety
    /// This must be a list vector
    pub unsafe fn list_size(&self) -> u64 {
        ffi::duckdb_list_vector_get_size(self.handle)
    }
    /// # Safety
    /// * This must be a list vector
    /// * `size` must be valid
    pub unsafe fn list_set_size(&self, size: u64) {
        let res = ffi::duckdb_list_vector_set_size(self.handle, size);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    /// # Safety
    /// * This must be a list vector
    /// * `required_capacity` must be valid
    pub unsafe fn list_reserve(&self, required_capacity: u64) {
        let res = ffi::duckdb_list_vector_reserve(self.handle, required_capacity);
        if res == ffi::DuckDBError {
            unreachable!("Vector pointer is null");
        }
    }
    /// # Safety
    /// This must be a struct vector
    pub unsafe fn struct_child(self: &Arc<Self>, index: u64) -> Arc<Self> {
        // Arc::new(Self {
        //     handle: ffi::duckdb_struct_vector_get_child(self.handle, index),
        //     _parent: Some(self.clone()),
        // })
        Self::from_raw_struct_vector(
            ffi::duckdb_struct_vector_get_child(self.handle, index),
            self.clone(),
        )
    }
}
