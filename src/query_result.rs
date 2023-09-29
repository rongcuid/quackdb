use std::{ops::Index, sync::Arc};

use quackdb_internal::{query_result::QueryResultHandle, value::FromResult};
use thiserror::Error;

use crate::types::LogicalType;

#[derive(Debug)]
pub struct QueryResult {
    pub handle: Arc<QueryResultHandle>,
    pub types: Vec<LogicalType>,
}

#[derive(Error, Debug)]
pub enum QueryResultError {
    #[error("type error")]
    TypeError,
}

impl From<Arc<QueryResultHandle>> for QueryResult {
    fn from(handle: Arc<QueryResultHandle>) -> Self {
        Self {
            types: unsafe {
                (0..handle.column_count())
                    .map(|c| {
                        handle
                            .column_logical_type(c)
                            .try_into()
                            .expect("logical type")
                    })
                    .collect::<Vec<_>>()
            },
            handle,
        }
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
    /// Safe get
    pub fn get<T: FromResult>(&self, col: u64, row: u64) -> T {
        self.check_col(col);
        self.check_row(row);
        let type_ = &self.types[col as usize];
        todo!()
    }
    /// Get without checking bounds or type
    pub unsafe fn get_unchecked<T: FromResult>(&self, col: u64, row: u64) -> T {
        T::from_result_unchecked(&self.handle, col, row)
    }
}

/// Internal functions
impl QueryResult {
    fn check_col(&self, col: u64) {
        assert!(col < self.column_count());
    }
    fn check_row(&self, row: u64) {
        assert!(row < self.row_count());
    }
}
