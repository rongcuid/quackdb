use std::sync::Arc;

use quackdb_internal::{data_chunks::DataChunkHandle, types::TypeId};

use crate::vector::Vector;

#[derive(Debug)]
pub struct DataChunk {
    pub handle: Arc<DataChunkHandle>,
}

impl DataChunk {
    pub fn new(_ty: TypeId, _column_count: u64) -> Self {
        unimplemented!()
    }
    pub fn reset(&self) {
        self.handle.reset()
    }
    pub fn column_count(&self) -> u64 {
        self.handle.column_count()
    }
    pub fn vector(&self, _col_idx: u64) -> Vector {
        todo!()
    }
    pub fn size(&self) -> u64 {
        self.handle.size()
    }
    pub fn set_size(&self, _size: u64) {
        todo!()
    }
}

impl From<Arc<DataChunkHandle>> for DataChunk {
    fn from(value: Arc<DataChunkHandle>) -> Self {
        Self { handle: value }
    }
}
