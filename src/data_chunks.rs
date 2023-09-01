use std::{
    ops::DerefMut,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{ffi, types::DuckType};

pub struct DataChunk {
    handle: Arc<Mutex<DataChunkHandle>>,
}

#[derive(Clone)]
struct DataChunkHandle(ffi::duckdb_data_chunk);

impl DataChunk {
    pub fn new(ty: DuckType, column_count: usize) -> Self {
        unimplemented!()
    }
    pub fn reset(&self) {}
}

impl Drop for DataChunkHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_data_chunk(&mut self.0);
        }
    }
}
