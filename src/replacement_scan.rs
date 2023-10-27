use std::{
    ffi::{CStr, CString, NulError},
    marker::PhantomData,
    ops::Deref,
};

use quackdb_internal::{ffi, handles::ReplacementScanInfoHandle};
use thiserror::Error;

#[derive(Debug)]
pub struct ReplacementScanInfo {
    handle: ReplacementScanInfoHandle,
}

#[derive(Error, Debug)]
pub enum ReplacementScanError {
    #[error("{0}")]
    ErrorMessage(String),
    #[error(transparent)]
    NulError(#[from] NulError),
}

impl From<ReplacementScanInfoHandle> for ReplacementScanInfo {
    fn from(value: ReplacementScanInfoHandle) -> Self {
        Self { handle: value }
    }
}

impl ReplacementScanInfo {
    pub fn set_function_name(&self, function_name: &str) -> Result<(), ReplacementScanError> {
        let cstr = CString::new(function_name)?;
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
