use std::collections::VecDeque;

use arrow::array::RecordBatch;

use crate::arrow::{ArrowResult, ArrowResultError};

/// Iterator that maps `ArrowResult` batch by batch
pub struct TryBatchMap<B, F> {
    result: ArrowResult,
    f: F,
    v: VecDeque<B>,
}

impl<B, F> TryBatchMap<B, F> {
    pub(crate) fn new(result: ArrowResult, f: F) -> Self {
        Self {
            result,
            f,
            v: VecDeque::new(),
        }
    }
}

impl<B, I, F> Iterator for TryBatchMap<B, F>
where
    I: Iterator<Item = B>,
    F: FnMut(RecordBatch) -> I,
{
    type Item = Result<B, ArrowResultError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(b) = self.v.pop_front() {
            // If the last batch has not finished emitting, pop that
            Some(Ok(b))
        } else {
            // Get another batch from results
            let rec = unsafe { self.result.handle.query_array() };
            match rec {
                Ok(Ok(rec)) if rec.num_rows() == 0 => None,
                Ok(Ok(rec)) => {
                    let v = (self.f)(rec);
                    self.v = v.collect();
                    Some(Ok(self.v.pop_front().unwrap()))
                }
                Ok(Err(e)) => Some(Err(ArrowResultError::ArrowError(e))),
                Err(err) => Some(Err(ArrowResultError::QueryNextError(err))),
            }
        }
    }
}

/// Iterator that maps `ArrowResult` row by row
pub struct TryMap<F> {
    result: ArrowResult,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    f: F,
}

impl<B, F> Iterator for TryMap<F>
where
    F: Fn(Row) -> B,
{
    type Item = Result<B, ArrowResultError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_batch.is_none()
            || self.current_batch.as_ref().unwrap().num_rows() <= self.current_row
        {
            self.current_row = 0;
            let rec = unsafe { self.result.handle.query_array() };
            match rec {
                Ok(Ok(rec)) => self.current_batch = Some(rec),
                Ok(Err(e)) => return Some(Err(ArrowResultError::ArrowError(e))),
                Err(err) => return Some(Err(ArrowResultError::QueryNextError(err))),
            }
        }
        let batch = self.current_batch.as_ref().unwrap();
        if batch.num_rows() == 0 {
            return None;
        }
        let res = (self.f)(Row {
            batch,
            row: self.current_row,
        });
        self.current_row += 1;
        Some(Ok(res))
    }
}

pub struct Row<'a> {
    batch: &'a RecordBatch,
    row: usize,
}

impl<'a> Row<'a> {}
