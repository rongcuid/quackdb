use std::ops::Deref;

use crate::ffi;

#[derive(Debug)]
pub struct ReplacementScanInfoHandle {
    handle: ffi::duckdb_replacement_scan_info,
}

impl ReplacementScanInfoHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_replacement_scan_info) -> Self {
        Self { handle }
    }
}

impl Deref for ReplacementScanInfoHandle {
    type Target = ffi::duckdb_replacement_scan_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
