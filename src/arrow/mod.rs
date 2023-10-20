mod iters;

use arrow::{array::RecordBatch, error::ArrowError};
use thiserror::Error;

use quackdb_internal::arrow::ArrowResultHandle;

use iters::TryBatchMap;

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: ArrowResultHandle,
}

#[derive(Error, Debug)]
pub enum ArrowResultError {
    #[error("{0}")]
    QueryNextError(String),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
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

    /// Map each record batch into rows, take the results as iterators. Each item is fallable
    pub fn try_batch_map_into<B, I, F>(self, f: F) -> TryBatchMap<B, F>
    where
        I: Iterator<Item = B>,
        F: FnMut(RecordBatch) -> I,
    {
        TryBatchMap::new(self, f)
    }

    /// Map each record batch into rows, take the results as iterators.
    ///
    /// # Panics
    /// The iterator might panic if an error is encountered
    pub fn batch_map_into<B, I, F>(self, f: F) -> impl Iterator<Item = B>
    where
        I: Iterator<Item = B>,
        F: FnMut(RecordBatch) -> I,
    {
        TryBatchMap::new(self, f).map(|r| r.expect("batch_map_into"))
    }
}
