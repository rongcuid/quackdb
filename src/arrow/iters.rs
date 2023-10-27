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
            let rec = unsafe { self.result.query_array() };
            match rec {
                Ok(rec) if rec.num_rows() == 0 => None,
                Ok(rec) => {
                    let v = (self.f)(rec);
                    self.v = v.collect();
                    Some(Ok(self.v.pop_front().unwrap()))
                }
                Err(e) => Some(Err(e)),
            }
        }
    }
}
