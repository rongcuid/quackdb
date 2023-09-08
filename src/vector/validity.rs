use crate::ffi;

pub struct Validity {
    pub(crate) handle: *mut u64,
}

impl Validity {
    pub fn row_is_valid(&self, row: u64) -> bool {
        unsafe { ffi::duckdb_validity_row_is_valid(self.handle, row) }
    }
    pub unsafe fn set_row_validity_unchecked(&self, row: u64, valid: bool) {
        ffi::duckdb_validity_set_row_validity(self.handle, row, valid);
    }
}
