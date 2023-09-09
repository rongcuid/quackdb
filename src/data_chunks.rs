use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    ffi,
    types::TypeId,
    vector::{Vector, VectorHandle},
};

#[derive(Debug)]
pub struct DataChunk {
    pub handle: Arc<DataChunkHandle>,
}

#[derive(Debug)]
pub struct DataChunkHandle(ffi::duckdb_data_chunk);

impl DataChunk {
    pub fn new(ty: TypeId, column_count: u64) -> Self {
        unimplemented!()
    }
    pub fn reset(&self) {
        self.handle.reset()
    }
    pub fn column_count(&self) -> u64 {
        self.handle.column_count()
    }
    pub fn vector(&self, col_idx: u64) -> Vector {
        unsafe { self.handle.vector(col_idx) }
    }
    pub fn size(&self) -> u64 {
        self.handle.size()
    }
    pub fn set_size(&self, size: u64) {
        todo!()
    }
}

impl From<Arc<DataChunkHandle>> for DataChunk {
    fn from(value: Arc<DataChunkHandle>) -> Self {
        Self { handle: value }
    }
}

impl DataChunkHandle {
    pub unsafe fn from_raw(raw: ffi::duckdb_data_chunk) -> Arc<Self> {
        Arc::new(Self(raw))
    }
    pub fn reset(&self) {
        unsafe {
            ffi::duckdb_data_chunk_reset(self.0);
        }
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_column_count(self.0) }
    }
    pub unsafe fn vector(&self, col_idx: u64) -> Vector {
        let v = ffi::duckdb_data_chunk_get_vector(self.0, col_idx);
        VectorHandle::from_raw(v).into()
    }
    pub fn size(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_size(self.0) }
    }
    pub unsafe fn set_size_unchecked(&self, size: u64) {
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
        unsafe {
            ffi::duckdb_destroy_data_chunk(&mut self.0);
        }
    }
}
