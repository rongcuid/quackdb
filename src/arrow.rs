use arrow::{array::RecordBatch, error::ArrowError};
use std::{marker::PhantomData, ops::Deref, sync::Arc};
use thiserror::Error;

use quackdb_internal::arrow::ArrowResultHandle;

use crate::rows::TryBatchMap;

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: ArrowResultHandle,
}

#[derive(Debug)]
pub struct RecordBatchHandle<'result> {
    handle: RecordBatch,
    _parent: PhantomData<&'result mut ArrowResultHandle>,
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

/// Lifetime ensures that the record batch is consumed before querying the next chunk
impl<'result> Deref for RecordBatchHandle<'result> {
    type Target = RecordBatch;

    fn deref(&self) -> &Self::Target {
        &self.handle
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

    pub fn try_batch_map_into<B, I, F>(self, f: F) -> TryBatchMap<B, F>
    where
        I: Iterator<Item = B>,
        F: FnMut(RecordBatch) -> I,
    {
        TryBatchMap::new(self, f)
    }
}
