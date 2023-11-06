use std::{ffi::CString, ops::Deref};

use quackdb_internal::ffi;
use thiserror::Error;

#[derive(Debug)]
pub struct ReplacementScanInfo {
    handle: ffi::duckdb_replacement_scan_info,
}

#[derive(Error, Debug)]
pub enum ReplacementScanError<E> {
    #[error("bad function name: {0}")]
    BadFunctionName(String),
    #[error(transparent)]
    UserError(#[from] E),
}

impl From<ffi::duckdb_replacement_scan_info> for ReplacementScanInfo {
    fn from(value: ffi::duckdb_replacement_scan_info) -> Self {
        Self { handle: value }
    }
}

impl ReplacementScanInfo {
    pub fn set_function_name<E>(&self, function_name: &str) -> Result<(), ReplacementScanError<E>> {
        let cstr = CString::new(function_name)
            .map_err(|_| ReplacementScanError::BadFunctionName(function_name.to_owned()))?;
        unsafe { ffi::duckdb_replacement_scan_set_function_name(**self, cstr.as_ptr()) }
        Ok(())
    }
    pub fn add_parameter(&self, parameter: ffi::duckdb_value) {
        unsafe { ffi::duckdb_replacement_scan_add_parameter(**self, parameter) }
    }
}

impl Deref for ReplacementScanInfo {
    type Target = ffi::duckdb_replacement_scan_info;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
