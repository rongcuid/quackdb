use std::{ffi::CStr, ops::Deref, sync::Arc};

use thiserror::Error;

use crate::{
    arrow::ArrowResultHandle,
    connection::ConnectionHandle,
    ffi,
    types::{i128_to_hugeint, TypeId},
};

#[derive(Debug)]
pub struct PreparedStatementHandle {
    handle: ffi::duckdb_prepared_statement,
    _parent: Arc<ConnectionHandle>,
}

#[derive(Error, Debug)]
#[error("prepared statement bind error")]
pub struct BindError();

impl Deref for PreparedStatementHandle {
    type Target = ffi::duckdb_prepared_statement;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for PreparedStatementHandle {
    fn drop(&mut self) {
        unsafe { self.destroy() }
    }
}

/// # Safety
/// * All parameter indices must be in range
impl PreparedStatementHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(
        raw: ffi::duckdb_prepared_statement,
        parent: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
        })
    }
    /// # Safety
    /// Does not consider utilization. Normally, let Rust handle this automatically.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_prepare(&mut self.handle);
    }
    pub fn nparams(&self) -> u64 {
        unsafe { ffi::duckdb_nparams(self.handle) }
    }
    /// # Safety
    /// * `param_idx` must be in range
    pub unsafe fn param_type(&self, param_idx: u64) -> TypeId {
        let ty = ffi::duckdb_param_type(self.handle, param_idx);
        TypeId::from_raw(ty).expect("invalid duckdb type")
    }
    pub fn clear_bindings(&self) -> Result<(), BindError> {
        unsafe {
            let res = ffi::duckdb_clear_bindings(self.handle);
            if res != ffi::DuckDBSuccess {
                return Err(BindError());
            }
            Ok(())
        }
    }

    pub unsafe fn bind_bool(&self, param_idx: u64, val: bool) -> Result<(), BindError> {
        if ffi::duckdb_bind_boolean(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_int8(&self, param_idx: u64, val: i8) -> Result<(), BindError> {
        if ffi::duckdb_bind_int8(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_int16(&self, param_idx: u64, val: i16) -> Result<(), BindError> {
        if ffi::duckdb_bind_int16(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_int32(&self, param_idx: u64, val: i32) -> Result<(), BindError> {
        if ffi::duckdb_bind_int32(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_int64(&self, param_idx: u64, val: i64) -> Result<(), BindError> {
        if ffi::duckdb_bind_int64(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_hugeint(&self, param_idx: u64, val: i128) -> Result<(), BindError> {
        let hugeint = i128_to_hugeint(val);
        if ffi::duckdb_bind_hugeint(self.handle, param_idx, hugeint) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_decimal(
        &self,
        param_idx: u64,
        width: u8,
        scale: u8,
        value: i128,
    ) -> Result<(), BindError> {
        let decimal = ffi::duckdb_decimal {
            width,
            scale,
            value: i128_to_hugeint(value),
        };
        if ffi::duckdb_bind_decimal(self.handle, param_idx, decimal) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_uint8(&self, param_idx: u64, val: u8) -> Result<(), BindError> {
        if ffi::duckdb_bind_uint8(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_uint16(&self, param_idx: u64, val: u16) -> Result<(), BindError> {
        if ffi::duckdb_bind_uint16(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_uint32(&self, param_idx: u64, val: u32) -> Result<(), BindError> {
        if ffi::duckdb_bind_uint32(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_uint64(&self, param_idx: u64, val: u64) -> Result<(), BindError> {
        if ffi::duckdb_bind_uint64(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_float(&self, param_idx: u64, val: f32) -> Result<(), BindError> {
        if ffi::duckdb_bind_float(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_double(&self, param_idx: u64, val: f64) -> Result<(), BindError> {
        if ffi::duckdb_bind_double(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_date(&self, param_idx: u64, val: ffi::duckdb_date) -> Result<(), BindError> {
        if ffi::duckdb_bind_date(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_time(&self, param_idx: u64, val: ffi::duckdb_time) -> Result<(), BindError> {
        if ffi::duckdb_bind_time(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_timestamp(
        &self,
        param_idx: u64,
        val: ffi::duckdb_timestamp,
    ) -> Result<(), BindError> {
        if ffi::duckdb_bind_timestamp(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_interval(
        &self,
        param_idx: u64,
        val: ffi::duckdb_interval,
    ) -> Result<(), BindError> {
        if ffi::duckdb_bind_interval(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_varchar(&self, param_idx: u64, val: &CStr) -> Result<(), BindError> {
        if ffi::duckdb_bind_varchar(self.handle, param_idx, val.as_ptr()) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_varchar_length(&self, param_idx: u64, val: &str) -> Result<(), BindError> {
        let b = val.as_bytes();
        if ffi::duckdb_bind_varchar_length(self.handle, param_idx, b.as_ptr() as _, b.len() as u64)
            != ffi::DuckDBSuccess
        {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_blob(&self, param_idx: u64, data: &[u8]) -> Result<(), BindError> {
        if ffi::duckdb_bind_blob(
            self.handle,
            param_idx,
            data.as_ptr() as _,
            data.len() as u64,
        ) != ffi::DuckDBSuccess
        {
            return Err(BindError());
        }
        Ok(())
    }
    pub unsafe fn bind_null(&self, param_idx: u64) -> Result<(), BindError> {
        if ffi::duckdb_bind_null(self.handle, param_idx) != ffi::DuckDBSuccess {
            return Err(BindError());
        }
        Ok(())
    }
    pub fn execute(self: &Arc<Self>) -> Result<ArrowResultHandle, String> {
        unsafe {
            let mut result: ffi::duckdb_arrow = std::mem::zeroed();
            let r = ffi::duckdb_execute_prepared_arrow(self.handle, &mut result);
            let h = ArrowResultHandle::from_raw_statement(result, self.clone());
            if r != ffi::DuckDBSuccess {
                return Err(h.error());
            }
            Ok(h)
        }
    }
}
