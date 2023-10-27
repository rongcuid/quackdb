use std::ffi::CStr;

use chrono::prelude::*;
use paste::paste;
use quackdb_internal::statement::PreparedStatementHandle;

use crate::to_duckdb::IntoDuckDb;

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

macro_rules! impl_bind_param_for_primitive {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_bind_param_for_primitive! {$ty, $duck_ty, [<bind_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        paste! {
            impl_bind_param_for_primitive! {$ty, $duck_ty, $method, stringify!([<duckdb_ $method>]())}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident, $err_msg:expr) => {
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

macro_rules! impl_bind_param {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_bind_param! {$ty, $duck_ty, [<bind_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        paste! {
            impl_bind_param! {$ty, $duck_ty, $method, stringify!([<duckdb_ $method>]())}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident, $err_msg:expr) => {
        unsafe impl BindParam for $ty {
            unsafe fn bind_param_unchecked(
                self,
                stmt: &PreparedStatementHandle,
                param_idx: u64,
            ) -> Result<(), &'static str> {
                stmt.$method(param_idx, self.into_duckdb())
                    .map_err(|_| $err_msg)
            }
        }
    };
}

impl_bind_param_for_primitive! {bool, bool}
impl_bind_param_for_primitive! {i8, int8}
impl_bind_param_for_primitive! {i16, int16}
impl_bind_param_for_primitive! {i32, int32}
impl_bind_param_for_primitive! {i64, int64}
impl_bind_param! {i128, hugeint}
impl_bind_param_for_primitive! {u8, uint8}
impl_bind_param_for_primitive! {u16, uint16}
impl_bind_param_for_primitive! {u32, uint32}
impl_bind_param_for_primitive! {u64, uint64}
impl_bind_param_for_primitive! {f32, float}
impl_bind_param_for_primitive! {f64, double}
impl_bind_param_for_primitive! {&CStr, varchar}
impl_bind_param_for_primitive! {&str, varchar_length}
impl_bind_param! {NaiveDate, date}
impl_bind_param! {NaiveTime, time}
impl_bind_param! {NaiveDateTime, timestamp}
impl_bind_param_for_primitive! {&[u8], blob}

unsafe impl BindParam for String {
    unsafe fn bind_param_unchecked(
        self,
        stmt: &PreparedStatementHandle,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        stmt.bind_varchar_length(param_idx, &self)
            .map_err(|_| "duckdb_bind_varchar_length()")
    }
}

unsafe impl<Tz: TimeZone> BindParam for DateTime<Tz> {
    unsafe fn bind_param_unchecked(
        self,
        stmt: &PreparedStatementHandle,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        stmt.bind_timestamp(param_idx, self.into_duckdb())
            .map_err(|_| "duckdb_bind_timestamp()")
    }
}
