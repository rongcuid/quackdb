use std::ffi::CStr;

use chrono::prelude::*;
use paste::paste;
use quackdb_internal::ffi;

use crate::to_duckdb::IntoDuckDb;

/// Values that can bind to prepared statements
pub unsafe trait BindParam {
    /// # Safety
    /// Does not need to check whether the type is correct or whether index is in bounds.
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
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
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match self {
            Some(t) => t.bind_param_unchecked(stmt, param_idx),
            None => match ffi::duckdb_bind_null(stmt, param_idx) {
                ffi::DuckDBSuccess => Ok(()),
                ffi::DuckDBError => Err("duckdb_bind_null()"),
                _ => unreachable!(),
            },
        }
    }
}

macro_rules! impl_bind_param_for_primitive {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_bind_param_for_primitive! {$ty, $duck_ty, [<duckdb_bind_ $duck_ty>]}
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
                stmt: ffi::duckdb_prepared_statement,
                param_idx: u64,
            ) -> Result<(), &'static str> {
                match ffi::$method(stmt, param_idx, self) {
                    ffi::DuckDBSuccess => Ok(()),
                    ffi::DuckDBError => Err($err_msg),
                    _ => unreachable!()
                }
            }
        }
    };
}

macro_rules! impl_bind_param {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_bind_param! {$ty, $duck_ty, [<duckdb_bind_ $duck_ty>]}
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
                stmt: ffi::duckdb_prepared_statement,
                param_idx: u64,
            ) -> Result<(), &'static str> {
                match ffi::$method(stmt, param_idx, self.into_duckdb()) {
                    ffi::DuckDBSuccess => Ok(()),
                    ffi::DuckDBError => Err($err_msg),
                    _ => unreachable!(),
                }
            }
        }
    };
}

impl_bind_param_for_primitive! {bool, boolean}
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
impl_bind_param! {NaiveDate, date}
impl_bind_param! {NaiveTime, time}
impl_bind_param! {NaiveDateTime, timestamp}

unsafe impl BindParam for &CStr {
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match ffi::duckdb_bind_varchar(stmt, param_idx, self.as_ptr()) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err("duckdb_bind_varchar()"),
            _ => unreachable!(),
        }
    }
}

unsafe impl BindParam for &str {
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match ffi::duckdb_bind_varchar_length(
            stmt,
            param_idx,
            self.as_ptr().cast(),
            self.len() as u64,
        ) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err("duckdb_bind_varchar_length()"),
            _ => unreachable!(),
        }
    }
}

unsafe impl BindParam for &[u8] {
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match ffi::duckdb_bind_blob(stmt, param_idx, self.as_ptr().cast(), self.len() as u64) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err("duckdb_bind_blob()"),
            _ => unreachable!(),
        }
    }
}

unsafe impl BindParam for String {
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        self.as_str().bind_param_unchecked(stmt, param_idx)
    }
}

unsafe impl<Tz: TimeZone> BindParam for DateTime<Tz> {
    unsafe fn bind_param_unchecked(
        self,
        stmt: ffi::duckdb_prepared_statement,
        param_idx: u64,
    ) -> Result<(), &'static str> {
        match ffi::duckdb_bind_timestamp(stmt, param_idx, self.into_duckdb()) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err("duckdb_bind_timestamp()"),
            _ => unreachable!(),
        }
    }
}
