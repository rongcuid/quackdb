use std::ops::Deref;

use crate::ffi;

#[derive(Debug)]
pub struct ReplacementScanInfoHandle {
    raw: ffi::duckdb_replacement_scan_info,
}

impl ReplacementScanInfoHandle {
    /// # Safety
    /// * Takes ownership of `raw`
    pub unsafe fn from_raw(raw: ffi::duckdb_replacement_scan_info) -> Self {
        Self { raw }
    }
}

impl Deref for ReplacementScanInfoHandle {
    type Target = ffi::duckdb_replacement_scan_info;

    fn deref(&self) -> &Self::Target {
        &self.raw
    }
}
