use std::{
    ops::DerefMut,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{ffi, types::TypeId, vector::Vector};

pub struct DataChunk {
    handle: Mutex<DataChunkHandle>,
}

#[derive(Clone)]
struct DataChunkHandle(ffi::duckdb_data_chunk);

impl DataChunk {
    pub fn new(ty: TypeId, column_count: u64) -> Arc<Self> {
        unimplemented!()
    }
    pub unsafe fn from_raw(handle: ffi::duckdb_data_chunk) -> Arc<Self> {
        Arc::new(Self {
            handle: Mutex::new(DataChunkHandle(handle)),
        })
    }
    pub fn reset(&self) {
        unsafe {
            ffi::duckdb_data_chunk_reset(self.handle.lock().unwrap().0);
        }
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_column_count(self.handle.lock().unwrap().0) }
    }
    pub fn vector(&self, col_idx: u64) -> Arc<Vector> {
        unsafe {
            let v = ffi::duckdb_data_chunk_get_vector(self.handle.lock().unwrap().0, col_idx);
            Vector::from_raw(v, None)
        }
    }
    pub fn size(&self) -> u64 {
        unsafe { ffi::duckdb_data_chunk_get_size(self.handle.lock().unwrap().0) }
    }
    pub unsafe fn set_size_unchecked(&self, size: u64) {
        ffi::duckdb_data_chunk_set_size(self.handle.lock().unwrap().0, size)
    }
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_data_chunk(&mut self.0);
        }
    }
}
