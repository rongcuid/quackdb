use std::ffi::CStr;

use paste::paste;
use rust_decimal::Decimal;
use time::{error::ComponentRange, Date, Duration, PrimitiveDateTime, Time};

use crate::{appender::AppenderHandle, ffi, statement::PreparedStatementHandle};

use super::DuckDbDecimal;

/// Values that can bind to prepared statements
pub unsafe trait BindParam {
    /// # Safety
    /// Does not need to check whether the type is correct or whether index is in bounds.
    unsafe fn bind_param_unchecked(
        self,
        stmt: &PreparedStatementHandle,
        param_idx: u64,
    ) -> Result<(), &'static str>;
}

pub unsafe trait AppendParam {
    /// # Safety
    /// Does not need to check whether the type is correct
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String>;
}

/// `Option<T>` corresponds to nullable columns
unsafe impl<T> BindParam for Option<T>
where
    T: BindParam,
{
    unsafe fn bind_param_unchecked(
        self,
        stmt: &PreparedStatementHandle,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match self {
            Some(t) => t.bind_param_unchecked(stmt, param_idx),
            None => stmt.bind_null(param_idx).map_err(|_| "duckdb_bind_null()"),
        }
    }
}

unsafe impl<T> AppendParam for Option<T>
where
    T: AppendParam,
{
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String> {
        match self {
            Some(t) => t.append_param_unchecked(appender),
            None => appender.append_null(),
        }
    }
}

macro_rules! impl_bind_param_for_value {
    ($ty:ty) => {
        paste! {
            impl_bind_param_for_value! {$ty, [<bind_ $ty>]}
        }
    };
    ($ty:ty, $method:ident) => {
        paste! {
            impl_bind_param_for_value! {$ty, $method, stringify!([<duckdb_ $method>]())}
        }
    };
    ($ty:ty, $method:ident, $err_msg:expr) => {
        unsafe impl BindParam for $ty {
            unsafe fn bind_param_unchecked(
                self,
                stmt: &PreparedStatementHandle,
                param_idx: u64,
            ) -> Result<(), &'static str> {
                stmt.$method(param_idx, self).map_err(|_| $err_msg)
            }
        }
    };
}

impl_bind_param_for_value! {bool}
impl_bind_param_for_value! {i8}
impl_bind_param_for_value! {i16}
impl_bind_param_for_value! {i32}
impl_bind_param_for_value! {i64}
impl_bind_param_for_value! {i128}
impl_bind_param_for_value! {DuckDbDecimal, bind_decimal}
impl_bind_param_for_value! {u8}
impl_bind_param_for_value! {u16}
impl_bind_param_for_value! {u32}
impl_bind_param_for_value! {u64}
impl_bind_param_for_value! {f32}
impl_bind_param_for_value! {f64}
impl_bind_param_for_value! {&CStr, bind_varchar}
impl_bind_param_for_value! {&str, bind_varchar_str}
impl_bind_param_for_value! {Date, bind_date}
impl_bind_param_for_value! {Time, bind_time}
impl_bind_param_for_value! {PrimitiveDateTime, bind_timestamp}
impl_bind_param_for_value! {Duration, bind_interval}
impl_bind_param_for_value! {&[u8], bind_blob}

macro_rules! impl_append_param_for_value {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param_for_value! {$ty, $duck_ty, [<append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: &AppenderHandle,
            ) -> Result<(), String> {
                appender.$method(self)
            }
        }
    };
}

impl_append_param_for_value! {bool, bool}
impl_append_param_for_value! {i8, int8}
impl_append_param_for_value! {i16, int16}
impl_append_param_for_value! {i32, int32}
impl_append_param_for_value! {i64, int64}
impl_append_param_for_value! {i128, hugeint}
impl_append_param_for_value! {u8, uint8}
impl_append_param_for_value! {u16, uint16}
impl_append_param_for_value! {u32, uint32}
impl_append_param_for_value! {u64, uint64}
impl_append_param_for_value! {f32, float}
impl_append_param_for_value! {f64, double}
impl_append_param_for_value! {&CStr, varchar}
impl_append_param_for_value! {&str, varchar_length}
impl_append_param_for_value! {Date, date}
impl_append_param_for_value! {Time, time}
impl_append_param_for_value! {PrimitiveDateTime, timestamp}
impl_append_param_for_value! {Duration, interval}
impl_append_param_for_value! {&[u8], blob}

unsafe impl BindParam for String {
    unsafe fn bind_param_unchecked(
        self,
        stmt: &PreparedStatementHandle,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        stmt.bind_varchar_str(param_idx, &self)
            .map_err(|_| "duckdb_bind_varchar_str()")
    }
}
