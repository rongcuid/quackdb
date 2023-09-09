use std::ops::Deref;

use crate::ffi;

#[derive(Debug)]
pub struct Validity {
    pub handle: ValidityHandle,
}

impl From<ValidityHandle> for Validity {
    fn from(value: ValidityHandle) -> Self {
        Self { handle: value }
    }
}

impl Validity {
    pub fn row_is_valid(&self, row: u64) -> bool {
        todo!()
    }
    pub fn set_row_validity(&self, row: u64, valid: bool) {
        todo!()
    }
}

impl ValidityHandle {
    pub unsafe fn from_raw(raw: *mut u64) -> Self {
        Self(raw)
    }
    pub unsafe fn row_is_valid(&self, row: u64) -> bool {
        unsafe { ffi::duckdb_validity_row_is_valid(self.0, row) }
    }
    pub unsafe fn set_row_validity(&self, row: u64, valid: bool) {
        ffi::duckdb_validity_set_row_validity(self.0, row, valid);
    }
}

#[derive(Debug)]
pub struct ValidityHandle(*mut u64);

impl Deref for ValidityHandle {
    type Target = *mut u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
