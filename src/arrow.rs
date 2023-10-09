use std::sync::Arc;

use quackdb_internal::arrow::ArrowResultHandle;

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: ArrowResultHandle,
}

impl From<ArrowResultHandle> for ArrowResult {
    fn from(handle: ArrowResultHandle) -> Self {
        Self { handle }
    }
}

impl ArrowResult {
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
