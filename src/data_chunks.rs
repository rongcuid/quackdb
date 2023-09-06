use std::{
    ops::DerefMut,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{ffi, types::RawType, vector::Vector};

pub struct DataChunk {
    handle: Mutex<DataChunkHandle>,
}

#[derive(Clone)]
struct DataChunkHandle(ffi::duckdb_data_chunk);

impl DataChunk {
    pub fn new(ty: RawType, column_count: usize) -> Arc<Self> {
        unimplemented!()
    }
    pub fn reset(&self) {
        unsafe {
            ffi::duckdb_data_chunk_reset(self.handle.lock().unwrap().0);
        }
    }
    pub fn column_count(&self) -> usize {
        unsafe { ffi::duckdb_data_chunk_get_column_count(self.handle.lock().unwrap().0) as usize }
    }
    pub fn vector(&self, col_idx: usize) -> Vector {
        unsafe {
            let v =
                ffi::duckdb_data_chunk_get_vector(self.handle.lock().unwrap().0, col_idx as u64);
            Vector::from_raw(v)
        }
    }
    pub fn size(&self) -> usize {
        unsafe { ffi::duckdb_data_chunk_get_size(self.handle.lock().unwrap().0) as usize }
    }
    pub fn set_size(&self, size: usize) {
        unsafe { ffi::duckdb_data_chunk_set_size(self.handle.lock().unwrap().0, size as u64) }
    }
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_data_chunk(&mut self.0);
        }
    }
}
