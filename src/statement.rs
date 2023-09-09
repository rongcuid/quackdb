use std::{
    ffi::{c_char, c_void},
    ops::Deref,
    sync::Arc,
};

use libduckdb_sys::DuckDBSuccess;

use crate::{
    connection::{Connection, ConnectionHandle},
    ffi,
    query::{QueryResult, QueryResultHandle},
    types::TypeId,
};

#[derive(Debug)]
pub struct PreparedStatement {
    pub handle: Arc<PreparedStatementHandle>,
}

#[derive(Debug)]
pub struct PreparedStatementHandle {
    handle: ffi::duckdb_prepared_statement,
    _parent: Arc<ConnectionHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum PreparedStatementError {
    #[error("duckdb_clear_bindings() error")]
    ClearBindingsError,
    #[error("prepared statement bind error")]
    BindError,
    #[error("prepared statement execute error")]
    ExecuteError,
}

impl PreparedStatement {}

impl From<Arc<PreparedStatementHandle>> for PreparedStatement {
    fn from(value: Arc<PreparedStatementHandle>) -> Self {
        Self { handle: value }
    }
}

impl PreparedStatementHandle {
    pub unsafe fn from_raw(
        raw: ffi::duckdb_prepared_statement,
        parent: Arc<ConnectionHandle>,
    ) -> Arc<Self> {
        Arc::new(Self {
            handle: raw,
            _parent: parent,
        })
    }
    pub fn nparams(&self) -> u64 {
        unsafe { ffi::duckdb_nparams(self.handle) }
    }
    pub unsafe fn param_type_unchecked(&self, param_idx: u64) -> Option<TypeId> {
        let ty = ffi::duckdb_param_type(self.handle, param_idx);
        TypeId::from_raw(ty)
    }
    pub fn clear_bindings(&self) -> Result<(), PreparedStatementError> {
        unsafe {
            let res = ffi::duckdb_clear_bindings(self.handle);
            if res != DuckDBSuccess {
                return Err(PreparedStatementError::ClearBindingsError);
            }
            Ok(())
        }
    }
    // pub fn bind_value(&self, param_idx: u64, val: &Value) -> Result<(), PreparedStatementError> {
    //     unsafe {
    //         let res = ffi::duckdb_bind_value()
    //     }
    // }
    // pub fn bind_parameter_index(&self, name: &str) -> Result<Option<u64>, Error> {
    //     let p = CString::new(name)?.as_ptr();
    //     unsafe {
    //         let mut param_idx: u64 = 0;
    //         let res = ffi::duckdb_bind_parameter_index(self.handle, &mut param_idx, p);
    //         if res != ffi::DuckDBSuccess {
    //             return Ok(None);
    //         }
    //         Ok(Some(param_idx))
    //     }
    // }
    pub unsafe fn bind_boolean_unchecked(
        &self,
        param_idx: u64,
        val: bool,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_boolean(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_i8_unchecked(
        &self,
        param_idx: u64,
        val: i8,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_int8(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_i16_unchecked(
        &self,
        param_idx: u64,
        val: i16,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_int16(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_i32_unchecked(
        &self,
        param_idx: u64,
        val: i32,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_int32(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_i64_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_int64(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_hugeint_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_decimal_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_u8_unchecked(
        &self,
        param_idx: u64,
        val: u8,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_uint8(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_u16_unchecked(
        &self,
        param_idx: u64,
        val: u16,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_uint16(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_u32_unchecked(
        &self,
        param_idx: u64,
        val: u32,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_uint32(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_u64_unchecked(
        &self,
        param_idx: u64,
        val: u64,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_uint64(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_f32_unchecked(
        &self,
        param_idx: u64,
        val: f32,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_float(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_f64_unchecked(
        &self,
        param_idx: u64,
        val: f64,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_double(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_date_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_time_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_timestamp_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_interval_unchecked(
        &self,
        param_idx: u64,
        val: i64,
    ) -> Result<(), PreparedStatementError> {
        todo!()
    }
    pub unsafe fn bind_varchar_unchecked(
        &self,
        param_idx: u64,
        val: *const c_char,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_varchar(self.handle, param_idx, val) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_varchar_length_unchecked(
        &self,
        param_idx: u64,
        val: *const c_char,
        length: u64,
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_varchar_length(self.handle, param_idx, val, length) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_blob_unchecked(
        &self,
        param_idx: u64,
        data: &[u8],
    ) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_blob(
            self.handle,
            param_idx,
            data.as_ptr() as *const c_void,
            data.len() as u64,
        ) != DuckDBSuccess
        {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub unsafe fn bind_null_unchecked(&self, param_idx: u64) -> Result<(), PreparedStatementError> {
        if ffi::duckdb_bind_null(self.handle, param_idx) != DuckDBSuccess {
            return Err(PreparedStatementError::BindError);
        }
        Ok(())
    }
    pub fn execute(self: &Arc<Self>) -> Result<QueryResult, PreparedStatementError> {
        unsafe {
            let mut out_result = std::mem::zeroed();
            if ffi::duckdb_execute_prepared(self.handle, &mut out_result) != DuckDBSuccess {
                ffi::duckdb_destroy_result(&mut out_result);
                return Err(PreparedStatementError::ExecuteError);
            }
            Ok(QueryResultHandle::from_raw_statement(out_result, self.clone()).into())
        }
    }
    pub fn execute_arrow(&self) {
        todo!()
    }
}

impl Deref for PreparedStatementHandle {
    type Target = ffi::duckdb_prepared_statement;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for PreparedStatementHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_prepare(&mut self.handle);
        }
    }
}
