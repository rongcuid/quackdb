use std::sync::Arc;

use quackdb_internal::query_result::QueryResultHandle;

use crate::types::LogicalType;

#[derive(Debug)]
pub struct QueryResult {
    pub handle: Arc<QueryResultHandle>,
}

impl From<Arc<QueryResultHandle>> for QueryResult {
    fn from(value: Arc<QueryResultHandle>) -> Self {
        Self { handle: value }
    }
}

impl QueryResult {
    pub fn is_streaming(&self) -> bool {
        self.handle.is_streaming()
    }
    pub fn column_name(&self, col: u64) -> Option<String> {
        self.check_col(col);
        unsafe { self.handle.column_name(col) }
    }
    pub fn column_type(&self, col: u64) -> Option<LogicalType> {
        self.check_col(col);
        unsafe { self.handle.column_type(col).try_into().ok() }
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

impl QueryResult {
    #[inline]
    fn check_col(&self, col: u64) {
        assert!(col < self.column_count());
    }
}
