use std::{
    ffi::{c_char, CStr},
    ops::Deref,
    ptr,
};

use cstr::cstr;

use arrow::{
    error::ArrowError,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use thiserror::Error;

use quackdb_internal::{ffi, handles::ArrowResultHandle};

#[derive(Debug)]
pub struct ArrowResult {
    pub handle: ArrowResultHandle,
}

#[derive(Error, Debug)]
pub enum ArrowResultError {
    #[error("{0}")]
    QueryNextError(&'static str),
    #[error(transparent)]
    ArrowError(#[from] ArrowError),
}

impl From<ArrowResultHandle> for ArrowResult {
    fn from(handle: ArrowResultHandle) -> Self {
        Self { handle }
    }
}

impl ArrowResult {
    /// # Safety
    /// There must actually be an error
    pub unsafe fn error(&self) -> String {
        unsafe {
            let err = ffi::duckdb_query_arrow_error(**self);
            CStr::from_ptr(err).to_string_lossy().into_owned()
        }
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_column_count(**self) }
    }
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_row_count(**self) }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe { ffi::duckdb_arrow_rows_changed(**self) }
    }
    pub fn into_stream(self) -> Result<ArrowArrayStreamReader, ArrowResultError> {
        let stream = FFI_ArrowArrayStream {
            get_schema: Some(get_schema),
            get_next: Some(get_next),
            get_last_error: Some(get_last_error),
            release: Some(release),
            private_data: Box::into_raw(Box::new(self)).cast(),
        };
        Ok(ArrowArrayStreamReader::try_new(stream)?)
    }
}

impl Deref for ArrowResult {
    type Target = ffi::duckdb_arrow;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

unsafe extern "C" fn get_schema(
    stream: *mut FFI_ArrowArrayStream,
    out: *mut FFI_ArrowSchema,
) -> i32 {
    assert!(out != ptr::null_mut());
    let result: *const ArrowResult = (*stream).private_data.cast();
    let res = **result;
    let mut out_schema = FFI_ArrowSchema::empty();
    if ffi::duckdb_query_arrow_schema(
        res,
        &mut std::ptr::addr_of_mut!(out_schema) as *mut _ as *mut ffi::duckdb_arrow_schema,
    ) != ffi::DuckDBSuccess
    {
        libc::EIO
    } else {
        *out = out_schema;
        0
    }
}

unsafe extern "C" fn get_next(stream: *mut FFI_ArrowArrayStream, out: *mut FFI_ArrowArray) -> i32 {
    let result: *const ArrowResult = (*stream).private_data.cast();
    let res = **result;
    let mut out_array = FFI_ArrowArray::empty();
    if ffi::duckdb_query_arrow_array(
        res,
        &mut std::ptr::addr_of_mut!(out_array) as *mut _ as *mut ffi::duckdb_arrow_array,
    ) != ffi::DuckDBSuccess
    {
        libc::EIO
    } else {
        *out = out_array;
        0
    }
}

unsafe extern "C" fn get_last_error(stream: *mut FFI_ArrowArrayStream) -> *const c_char {
    let result: *const ArrowResult = (*stream).private_data.cast();
    let res = **result;
    let ptr = ffi::duckdb_query_arrow_error(res);
    if ptr == ptr::null() {
        cstr!("ArrowArrayStream->get_last_error returned NULL").as_ptr()
    } else {
        ptr
    }
}

unsafe extern "C" fn release(stream: *mut FFI_ArrowArrayStream) {
    drop::<Box<ArrowResult>>(Box::from_raw((*stream).private_data.cast()))
}
