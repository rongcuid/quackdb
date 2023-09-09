use std::{ops::Deref, sync::Arc};

use crate::{ffi, types::LogicalTypeHandle, vector::VectorHandle};

#[derive(Debug)]
pub struct DataChunkHandle(ffi::duckdb_data_chunk);

impl DataChunkHandle {
    /// # Safety
    /// `types` must be valid
    pub unsafe fn create(types: &[&LogicalTypeHandle]) -> Arc<Self> {
        let mut types = types.into_iter().map(|x| ***x).collect::<Vec<_>>();
        let handle = ffi::duckdb_create_data_chunk(types.as_mut_ptr(), types.len() as u64);
        Self::from_raw(handle)
    }
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: ffi::duckdb_data_chunk) -> Arc<Self> {
        Arc::new(Self(raw))
    }
    /// # Safety
    /// Does not consider usage. Normally, let Rust automatically manage this.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_data_chunk(&mut self.0);
    }
    pub fn reset(&self) {
        unsafe {
            ffi::duckdb_data_chunk_reset(self.0);
        }
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_column_count(self.0) }
    }
    /// # Safety
    /// `col_idx` must be within valid range
    pub unsafe fn vector(&self, col_idx: u64) -> Arc<VectorHandle> {
        let v = ffi::duckdb_data_chunk_get_vector(self.0, col_idx);
        VectorHandle::from_raw(v)
    }
    pub fn size(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_size(self.0) }
    }
    /// # Safety
    /// Caller ensures size is valid
    pub unsafe fn set_size(&self, size: u64) {
        ffi::duckdb_data_chunk_set_size(self.0, size)
    }
}

impl Deref for DataChunkHandle {
    type Target = ffi::duckdb_data_chunk;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        unsafe { self.destroy() }
    }
}
