use std::collections::VecDeque;

use arrow::{array::RecordBatch, error::ArrowError};

use crate::arrow::{ArrowResult, ArrowResultError};

/// Iterator that maps record batches to rows
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
