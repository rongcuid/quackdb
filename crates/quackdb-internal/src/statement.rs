use std::{ffi::CStr, ops::Deref, sync::Arc};

use time::{Date, Duration, PrimitiveDateTime, Time};

use crate::{
    arrow::ArrowResultHandle, connection::ConnectionHandle, conversion::*, ffi, types::TypeId,
};

#[derive(Debug)]
pub struct PreparedStatementHandle {
    handle: ffi::duckdb_prepared_statement,
    _parent: Arc<ConnectionHandle>,
}

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
    pub fn clear_bindings(&self) -> Result<(), ()> {
        unsafe {
            let res = ffi::duckdb_clear_bindings(self.handle);
            if res != ffi::DuckDBSuccess {
                return Err(());
            }
            Ok(())
        }
    }

    pub unsafe fn bind_bool(&self, param_idx: u64, val: bool) -> Result<(), ()> {
        if ffi::duckdb_bind_boolean(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_i8(&self, param_idx: u64, val: i8) -> Result<(), ()> {
        if ffi::duckdb_bind_int8(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_i16(&self, param_idx: u64, val: i16) -> Result<(), ()> {
        if ffi::duckdb_bind_int16(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_i32(&self, param_idx: u64, val: i32) -> Result<(), ()> {
        if ffi::duckdb_bind_int32(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_i64(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        if ffi::duckdb_bind_int64(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_i128(&self, param_idx: u64, val: i128) -> Result<(), ()> {
        let hugeint = val.into_duckdb();
        if ffi::duckdb_bind_hugeint(self.handle, param_idx, hugeint) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_decimal(
        &self,
        param_idx: u64,
        width: u8,
        scale: u8,
        value: i128,
    ) -> Result<(), ()> {
        let decimal = ffi::duckdb_decimal {
            width,
            scale,
            value: value.into_duckdb(),
        };
        if ffi::duckdb_bind_decimal(self.handle, param_idx, decimal) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_u8(&self, param_idx: u64, val: u8) -> Result<(), ()> {
        if ffi::duckdb_bind_uint8(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_u16(&self, param_idx: u64, val: u16) -> Result<(), ()> {
        if ffi::duckdb_bind_uint16(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_u32(&self, param_idx: u64, val: u32) -> Result<(), ()> {
        if ffi::duckdb_bind_uint32(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_u64(&self, param_idx: u64, val: u64) -> Result<(), ()> {
        if ffi::duckdb_bind_uint64(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_f32(&self, param_idx: u64, val: f32) -> Result<(), ()> {
        if ffi::duckdb_bind_float(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_f64(&self, param_idx: u64, val: f64) -> Result<(), ()> {
        if ffi::duckdb_bind_double(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_date(&self, param_idx: u64, val: Date) -> Result<(), ()> {
        let date = val.into_duckdb();
        if ffi::duckdb_bind_date(self.handle, param_idx, ffi::duckdb_to_date(date))
            != ffi::DuckDBSuccess
        {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_time(&self, param_idx: u64, val: Time) -> Result<(), ()> {
        let time = val.into_duckdb();
        if ffi::duckdb_bind_time(self.handle, param_idx, ffi::duckdb_to_time(time))
            != ffi::DuckDBSuccess
        {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_timestamp(&self, param_idx: u64, val: PrimitiveDateTime) -> Result<(), ()> {
        let ts = val.into_duckdb();
        if ffi::duckdb_bind_timestamp(self.handle, param_idx, ffi::duckdb_to_timestamp(ts))
            != ffi::DuckDBSuccess
        {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_interval(&self, param_idx: u64, val: Duration) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_varchar(&self, param_idx: u64, val: &CStr) -> Result<(), ()> {
        if ffi::duckdb_bind_varchar(self.handle, param_idx, val.as_ptr()) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_varchar_str(&self, param_idx: u64, val: &str) -> Result<(), ()> {
        let b = val.as_bytes();
        if ffi::duckdb_bind_varchar_length(self.handle, param_idx, b.as_ptr() as _, b.len() as u64)
            != ffi::DuckDBSuccess
        {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_blob(&self, param_idx: u64, data: &[u8]) -> Result<(), ()> {
        if ffi::duckdb_bind_blob(
            self.handle,
            param_idx,
            data.as_ptr() as _,
            data.len() as u64,
        ) != ffi::DuckDBSuccess
        {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_null(&self, param_idx: u64) -> Result<(), ()> {
        if ffi::duckdb_bind_null(self.handle, param_idx) != ffi::DuckDBSuccess {
            return Err(());
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
