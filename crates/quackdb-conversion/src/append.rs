use std::ffi::CStr;

use chrono::prelude::*;
use paste::paste;

use quackdb_internal::ffi;

use crate::to_duckdb::IntoDuckDb;

pub unsafe trait AppendParam {
    /// # Safety
    /// Does not need to check whether the type is correct
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String>;
}

fn get_appender_error(appender: ffi::duckdb_appender) -> String {
    let err = unsafe { CStr::from_ptr(ffi::duckdb_appender_error(appender)) };
    err.to_string_lossy().into_owned()
}

unsafe impl<T> AppendParam for Option<T>
where
    T: AppendParam,
{
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        match self {
            Some(t) => t.append_param_unchecked(appender),
            None => match ffi::duckdb_append_null(appender) {
                ffi::DuckDBSuccess => Ok(()),
                ffi::DuckDBError => Err(get_appender_error(appender)),
                _ => unreachable!(),
            },
        }
    }
}

macro_rules! impl_append_param_for_primitive {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param_for_primitive! {$ty, $duck_ty, [<duckdb_append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: ffi::duckdb_appender,
            ) -> Result<(), String> {
                match ffi::$method(appender, self) {
                    ffi::DuckDBSuccess => Ok(()),
                    ffi::DuckDBError => Err(get_appender_error(appender)),
                    _ => unreachable!(),
                }
            }
        }
    };
}

macro_rules! impl_append_param {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param! {$ty, $duck_ty, [<duckdb_append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: ffi::duckdb_appender,
            ) -> Result<(), String> {
                match ffi::$method(appender, self.into_duckdb()) {
                    ffi::DuckDBSuccess => Ok(()),
                    ffi::DuckDBError => Err(get_appender_error(appender)),
                    _ => unreachable!(),
                }
            }
        }
    };
}

impl_append_param_for_primitive! {bool, bool}
impl_append_param_for_primitive! {i8, int8}
impl_append_param_for_primitive! {i16, int16}
impl_append_param_for_primitive! {i32, int32}
impl_append_param_for_primitive! {i64, int64}
impl_append_param! {i128, hugeint}
impl_append_param_for_primitive! {u8, uint8}
impl_append_param_for_primitive! {u16, uint16}
impl_append_param_for_primitive! {u32, uint32}
impl_append_param_for_primitive! {u64, uint64}
impl_append_param_for_primitive! {f32, float}
impl_append_param_for_primitive! {f64, double}
impl_append_param! {NaiveDate, date}
impl_append_param! {NaiveTime, time}
impl_append_param! {NaiveDateTime, timestamp}

unsafe impl AppendParam for &CStr {
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        match ffi::duckdb_append_varchar(appender, self.as_ptr().cast()) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err(get_appender_error(appender)),
            _ => unreachable!(),
        }
    }
}

unsafe impl AppendParam for &str {
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        match ffi::duckdb_append_varchar_length(appender, self.as_ptr().cast(), self.len() as u64) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err(get_appender_error(appender)),
            _ => unreachable!(),
        }
    }
}

unsafe impl AppendParam for &[u8] {
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        match ffi::duckdb_append_blob(appender, self.as_ptr().cast(), self.len() as u64) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err(get_appender_error(appender)),
            _ => unreachable!(),
        }
    }
}

unsafe impl AppendParam for String {
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        self.as_str().append_param_unchecked(appender)
    }
}

unsafe impl<Tz: TimeZone> AppendParam for DateTime<Tz> {
    unsafe fn append_param_unchecked(self, appender: ffi::duckdb_appender) -> Result<(), String> {
        match ffi::duckdb_append_timestamp(appender, self.into_duckdb()) {
            ffi::DuckDBSuccess => Ok(()),
            ffi::DuckDBError => Err(get_appender_error(appender)),
            _ => unreachable!(),
        }
    }
}
