use std::sync::Arc;

use quackdb_internal::{result::QueryResultHandle, types::LogicalType};

use crate::data_chunks::DataChunk;

#[derive(Debug)]
pub struct QueryResult {
    pub handle: Arc<QueryResultHandle>,
}

impl QueryResult {
    pub fn chunk(&self, _chunk_index: u64) -> Arc<DataChunk> {
        todo!()
    }
    pub fn column_name(&self, _col: u64) -> Option<String> {
        todo!()
    }
    pub fn column_type(&self, _col: u64) -> Option<LogicalType> {
        todo!()
    }
    pub fn column_count(&self) -> u64 {
        self.handle.column_count()
    }
    pub fn row_count(&self) -> u64 {
        self.handle.row_count()
    }
    pub fn rows_changed(&self) -> u64 {
        self.handle.rows_changed()
    }
}

impl From<Arc<QueryResultHandle>> for QueryResult {
    fn from(value: Arc<QueryResultHandle>) -> Self {
        Self { handle: value }
    }
}
