use std::sync::Arc;

use quackdb_internal::{data_chunks::DataChunkHandle, types::TypeId};

use crate::{types::LogicalType, vector::Vector};

#[derive(Debug)]
pub struct DataChunk {
    pub handle: Arc<DataChunkHandle>,
}

impl DataChunk {
    pub fn create(types: &[LogicalType]) -> Self {
        let types = types.into_iter().map(|x| &x.handle).collect::<Vec<_>>();
        unsafe { DataChunkHandle::create(&types[..]).into() }
    }
    pub fn reset(&self) {
        self.handle.reset()
    }
    pub fn column_count(&self) -> u64 {
        self.handle.column_count()
    }
    pub fn vector(&self, col_idx: u64) -> Vector {
        self.check_column(col_idx);
        unsafe { self.handle.vector(col_idx).into() }
    }
    pub fn size(&self) -> u64 {
        self.handle.size()
    }
    pub fn set_size(&self, _size: u64) {
        todo!("What does this do?")
    }
}

impl DataChunk {
    fn check_column(&self, col: u64) {
        assert!(col < self.column_count());
    }
}

impl From<Arc<DataChunkHandle>> for DataChunk {
    fn from(value: Arc<DataChunkHandle>) -> Self {
        Self { handle: value }
    }
}
