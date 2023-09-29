use std::{ops::Index, sync::Arc};

use quackdb_internal::{arrow::ArrowResultHandle, types::TypeId};
use thiserror::Error;

use crate::types::LogicalType;

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: Arc<ArrowResultHandle>,
}

#[derive(Error, Debug)]
pub enum QueryResultError {
    #[error("type error")]
    TypeError,
}

impl From<Arc<ArrowResultHandle>> for ArrowResult {
    fn from(handle: Arc<ArrowResultHandle>) -> Self {
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

/// Internal functions
impl ArrowResult {
    fn check_col(&self, col: u64) {
        assert!(col < self.column_count());
    }
    fn check_row(&self, row: u64) {
        assert!(row < self.row_count());
    }
}
