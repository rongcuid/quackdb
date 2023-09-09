use std::ops::Deref;

use crate::ffi;

#[derive(Debug)]
pub struct ValidityHandle(*mut u64);

impl Deref for ValidityHandle {
    type Target = *mut u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ValidityHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: *mut u64) -> Self {
        Self(raw)
    }
    /// # Safety
    /// * Validity must be writable
    /// * `row` must be in range
    pub unsafe fn row_is_valid(&self, row: u64) -> bool {
        unsafe { ffi::duckdb_validity_row_is_valid(self.0, row) }
    }
    /// # Safety
    /// * Validity must be writable
    /// * `row` must be in range
    pub unsafe fn set_row_validity(&self, row: u64, valid: bool) {
        ffi::duckdb_validity_set_row_validity(self.0, row, valid);
    }
}
