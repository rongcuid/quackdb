use std::{
    ffi::{c_void, CStr},
    ops::Deref,
    ptr::NonNull,
    sync::Arc,
};

use crate::{
    connection::ConnectionHandle,
    ffi,
    statement::PreparedStatementHandle,
    types::{LogicalTypeHandle, TypeId},
};

#[derive(Debug)]
pub struct QueryResultHandle {
    handle: ffi::duckdb_result,
    _parent: QueryParent,
}

#[derive(Debug)]
pub enum QueryParent {
    Connection(Arc<ConnectionHandle>),
    Statement(Arc<PreparedStatementHandle>),
}

impl Deref for QueryResultHandle {
    type Target = ffi::duckdb_result;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for QueryResultHandle {
    fn drop(&mut self) {
        unsafe {
            self.destroy();
        }
    }
}

impl QueryResultHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_connection(
        handle: ffi::duckdb_result,
        connection: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle,
            _parent: QueryParent::Connection(connection),
        })
    }
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw_statement(
        handle: ffi::duckdb_result,
        statement: Arc<PreparedStatementHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: handle,
            _parent: QueryParent::Statement(statement),
        })
    }
    /// # Safety
    /// Destroys without considering usage. Normally you should let Rust manage this.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_result(&mut self.handle);
    }
    pub fn is_streaming(&self) -> bool {
        unsafe { ffi::duckdb_result_is_streaming(self.handle) }
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_name(&self, col: u64) -> Option<String> {
        let p = ffi::duckdb_column_name(self.handle_mut(), col);
        let nn = NonNull::new(p as _)?;
        let cstr = CStr::from_ptr(nn.as_ptr());
        Some(cstr.to_string_lossy().to_owned().to_string())
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_type(&self, col: u64) -> TypeId {
        TypeId::from_raw(ffi::duckdb_column_type(self.handle_mut(), col))
            .expect("invalid duckdb type")
    }
    /// # Safety
    /// `col` must be within valid range.
    pub unsafe fn column_logical_type(&self, col: u64) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_column_logical_type(
            self.handle_mut(),
            col as u64,
        ))
    }
    pub fn column_count(&self) -> u64 {
        unsafe { ffi::duckdb_column_count(self.handle_mut()) }
    }
    pub fn row_count(&self) -> u64 {
        unsafe { ffi::duckdb_row_count(self.handle_mut()) }
    }
    pub fn rows_changed(&self) -> u64 {
        unsafe { ffi::duckdb_rows_changed(self.handle_mut()) }
    }
    /// # Safety
    /// Does not check if there is actually an error
    pub unsafe fn error(&self) -> String {
        let err = ffi::duckdb_result_error(self.handle_mut());
        CStr::from_ptr(err).to_string_lossy().into_owned()
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_bool(&self, col: u64, row: u64) -> bool {
        ffi::duckdb_value_boolean(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_i8(&self, col: u64, row: u64) -> i8 {
        ffi::duckdb_value_int8(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_i16(&self, col: u64, row: u64) -> i16 {
        ffi::duckdb_value_int16(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_i32(&self, col: u64, row: u64) -> i32 {
        ffi::duckdb_value_int32(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_i64(&self, col: u64, row: u64) -> i64 {
        ffi::duckdb_value_int64(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_u8(&self, col: u64, row: u64) -> u8 {
        ffi::duckdb_value_uint8(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_u16(&self, col: u64, row: u64) -> u16 {
        ffi::duckdb_value_uint16(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_u32(&self, col: u64, row: u64) -> i32 {
        ffi::duckdb_value_int32(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_u64(&self, col: u64, row: u64) -> u64 {
        ffi::duckdb_value_uint64(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_f32(&self, col: u64, row: u64) -> f32 {
        ffi::duckdb_value_float(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_f64(&self, col: u64, row: u64) -> f64 {
        ffi::duckdb_value_double(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_string(&self, col: u64, row: u64) -> String {
        let p = ffi::duckdb_value_string(self.handle_mut(), col, row);
        let s = CStr::from_ptr(p.data).to_string_lossy().into_owned();
        ffi::duckdb_free(p.data as *mut c_void);
        s
    }
    /// # Safety
    /// Caller ensures type is correct and col and row are in bound
    pub unsafe fn value_blob(&self, col: u64, row: u64) -> Vec<u8> {
        let p = ffi::duckdb_value_blob(self.handle_mut(), col, row);
        let v = std::slice::from_raw_parts(p.data as *mut u8, p.size as usize).to_vec();
        ffi::duckdb_free(p.data as *mut c_void);
        v
    }
    /// # Safety
    /// Caller ensures col and row are in bound
    pub unsafe fn value_is_null(&self, col: u64, row: u64) -> bool {
        ffi::duckdb_value_is_null(self.handle_mut(), col, row)
    }
    /// # Safety
    /// Unsynchronized
    pub unsafe fn handle_mut(&self) -> *mut ffi::duckdb_result {
        &self.handle as *const ffi::duckdb_result as *mut ffi::duckdb_result
    }
}
