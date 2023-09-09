use std::{
    ffi::{c_char, c_void},
    ops::Deref,
    sync::Arc,
};

use crate::{connection::ConnectionHandle, ffi, result::QueryResultHandle, types::TypeId};

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
        unsafe {
            ffi::duckdb_destroy_prepare(&mut self.handle);
        }
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
    // pub fn bind_value(&self, param_idx: u64, val: &Value) -> Result<(), ()> {
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
    pub unsafe fn bind_boolean(&self, param_idx: u64, val: bool) -> Result<(), ()> {
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
    pub unsafe fn bind_hugeint(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_decimal(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
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
    pub unsafe fn bind_date(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_time(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_timestamp(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_interval(&self, param_idx: u64, val: i64) -> Result<(), ()> {
        todo!()
    }
    pub unsafe fn bind_varchar(&self, param_idx: u64, val: *const c_char) -> Result<(), ()> {
        if ffi::duckdb_bind_varchar(self.handle, param_idx, val) != ffi::DuckDBSuccess {
            return Err(());
        }
        Ok(())
    }
    pub unsafe fn bind_varchar_length(
        &self,
        param_idx: u64,
        val: *const c_char,
        length: u64,
    ) -> Result<(), ()> {
        if ffi::duckdb_bind_varchar_length(self.handle, param_idx, val, length)
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
            data.as_ptr() as *const c_void,
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
    pub fn execute(self: &Arc<Self>) -> Result<Arc<QueryResultHandle>, ()> {
        unsafe {
            let mut out_result = std::mem::zeroed();
            if ffi::duckdb_execute_prepared(self.handle, &mut out_result) != ffi::DuckDBSuccess {
                ffi::duckdb_destroy_result(&mut out_result);
                return Err(());
            }
            Ok(QueryResultHandle::from_raw_statement(out_result, self.clone()).into())
        }
    }
    pub fn execute_arrow(&self) {
        todo!()
    }
}
