use paste::paste;
use std::{ffi::CStr, ops::Deref, sync::Arc};
use time::{Date, Duration, PrimitiveDateTime, Time};

use crate::{
    connection::ConnectionHandle,
    ffi,
    value::{
        date_to_duckdb_date, datetime_to_duckdb_timestamp, i128_to_duckdb_hugeint,
        time_to_duckdb_time,
    },
};

pub struct AppenderHandle {
    handle: ffi::duckdb_appender,
    _parent: Arc<ConnectionHandle>,
}

impl AppenderHandle {
    pub fn create(
        connection: Arc<ConnectionHandle>,
        schema: Option<&CStr>,
        table: &CStr,
    ) -> Result<Self, String> {
        unsafe {
            let mut out_appender: ffi::duckdb_appender = std::mem::zeroed();
            let r = ffi::duckdb_appender_create(
                **connection,
                schema.map_or(std::ptr::null(), |s| s.as_ptr()),
                table.as_ptr(),
                &mut out_appender,
            );
            if r != ffi::DuckDBSuccess {
                let err = CStr::from_ptr(ffi::duckdb_appender_error(out_appender));
                let err = err.to_string_lossy().into_owned();
                ffi::duckdb_appender_destroy(&mut out_appender);
                Err(err)
            } else {
                Ok(Self::from_raw(out_appender, connection))
            }
        }
    }
    pub unsafe fn from_raw(raw: ffi::duckdb_appender, connection: Arc<ConnectionHandle>) -> Self {
        Self {
            handle: raw,
            _parent: connection,
        }
    }
    pub fn error(&self) -> String {
        let err = unsafe { CStr::from_ptr(ffi::duckdb_appender_error(**self)) };
        err.to_string_lossy().into_owned()
    }
    pub fn flush(&self) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_appender_flush(**self) })
    }
    pub fn close(&self) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_appender_close(**self) })
    }
    fn do_or_error(&self, state: ffi::duckdb_state) -> Result<(), String> {
        if state != ffi::DuckDBSuccess {
            Err(self.error())
        } else {
            Ok(())
        }
    }
}

macro_rules! fn_append {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            fn_append!{$ty, [<append_ $duck_ty>], ffi::[<duckdb_append_ $duck_ty>]}
        }
    };
    ($ty:ty, $method:ident, $db_method: expr) => {
        pub fn $method(&self, value: $ty) -> Result<(), String> {
            self.do_or_error(unsafe { $db_method(**self, value) })
        }
    };
}

impl AppenderHandle {
    pub fn begin_row(&self) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_appender_begin_row(**self) })
    }
    pub fn end_row(&self) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_appender_end_row(**self) })
    }
    fn_append! {bool, bool}
    fn_append! {i8, int8}
    fn_append! {i16, int16}
    fn_append! {i32, int32}
    fn_append! {i64, int64}
    pub fn append_hugeint(&self, value: i128) -> Result<(), String> {
        let h = i128_to_duckdb_hugeint(value);
        self.do_or_error(unsafe { ffi::duckdb_append_hugeint(**self, h) })
    }
    fn_append! {u8, uint8}
    fn_append! {u16, uint16}
    fn_append! {u32, uint32}
    fn_append! {u64, uint64}
    fn_append! {f32, float}
    fn_append! {f64, double}
    pub fn append_date(&self, value: Date) -> Result<(), String> {
        let d = date_to_duckdb_date(&value);
        self.do_or_error(unsafe { ffi::duckdb_append_date(**self, ffi::duckdb_to_date(d)) })
    }
    pub fn append_time(&self, value: Time) -> Result<(), String> {
        let t = time_to_duckdb_time(&value);
        self.do_or_error(unsafe { ffi::duckdb_append_time(**self, ffi::duckdb_to_time(t)) })
    }
    pub fn append_timestamp(&self, value: PrimitiveDateTime) -> Result<(), String> {
        let dt = datetime_to_duckdb_timestamp(&value);
        self.do_or_error(unsafe {
            ffi::duckdb_append_timestamp(**self, ffi::duckdb_to_timestamp(dt))
        })
    }
    pub fn append_interval(&self, value: Duration) -> Result<(), String> {
        todo!()
    }
    pub fn append_varchar(&self, value: &CStr) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_append_varchar(**self, value.as_ptr()) })
    }
    pub fn append_varchar_length(&self, value: &str) -> Result<(), String> {
        self.do_or_error(unsafe {
            let b = value.as_bytes();
            ffi::duckdb_append_varchar_length(**self, b.as_ptr() as *const _, b.len() as u64)
        })
    }
    pub fn append_blob(&self, value: &[u8]) -> Result<(), String> {
        self.do_or_error(unsafe {
            ffi::duckdb_append_blob(**self, value.as_ptr() as *const _, value.len() as u64)
        })
    }
    pub fn append_null(&self) -> Result<(), String> {
        self.do_or_error(unsafe { ffi::duckdb_append_null(**self) })
    }
}

impl Deref for AppenderHandle {
    type Target = ffi::duckdb_appender;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for AppenderHandle {
    fn drop(&mut self) {
        unsafe {
            if ffi::duckdb_appender_destroy(&mut self.handle) != ffi::DuckDBSuccess {
                panic!("duckdb_appender_destroy() failed");
            }
        }
    }
}