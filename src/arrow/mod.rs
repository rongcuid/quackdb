mod iters;

use std::{ffi::CStr, ops::Deref};

use arrow::{
    array::{RecordBatch, StructArray},
    error::ArrowError,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use thiserror::Error;

use quackdb_internal::{ffi, handles::ArrowResultHandle};

use iters::TryBatchMap;

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: ArrowResultHandle,
}

#[derive(Error, Debug)]
pub enum ArrowResultError {
    #[error("{0}")]
    QueryNextError(&'static str),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

impl From<ArrowResultHandle> for ArrowResult {
    fn from(handle: ArrowResultHandle) -> Self {
        Self { handle }
    }
}

impl ArrowResult {
    pub fn error(&self) -> String {
        unsafe {
            let err = ffi::duckdb_query_arrow_error(**self);
            CStr::from_ptr(err).to_string_lossy().into_owned()
        }
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_column_count(**self) }
    }
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_row_count(**self) }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_rows_changed(**self) }
    }

    /// # Safety
    /// The result must be consumed before calling this again
    pub unsafe fn query_array(&self) -> Result<RecordBatch, ArrowResultError> {
        let mut out_schema = FFI_ArrowSchema::empty();
        if unsafe {
            ffi::duckdb_query_arrow_schema(
                **self,
                &mut std::ptr::addr_of_mut!(out_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
            )
        } != ffi::DuckDBSuccess
        {
            return Err(ArrowResultError::QueryNextError(
                "duckdb_query_arrow_schema()",
            ));
        }
        let mut out_array = FFI_ArrowArray::empty();
        if unsafe {
            ffi::duckdb_query_arrow_array(
                **self,
                &mut std::ptr::addr_of_mut!(out_array) as *mut _ as *mut ffi::duckdb_arrow_array,
            )
        } != ffi::DuckDBSuccess
        {
            return Err(ArrowResultError::QueryNextError(
                "duckdb_query_arrow_array()",
            ));
        }
        from_ffi(out_array, &out_schema)
            .map(StructArray::from)
            .map(RecordBatch::from)
            .map_err(ArrowResultError::from)
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

impl Deref for ArrowResult {
    type Target = ffi::duckdb_arrow;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
