use std::ffi::CStr;

use chrono::prelude::*;
use paste::paste;

use quackdb_internal::appender::AppenderHandle;

use crate::to_duckdb::IntoDuckDb;

pub unsafe trait AppendParam {
    /// # Safety
    /// Does not need to check whether the type is correct
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String>;
}

unsafe impl<T> AppendParam for Option<T>
where
    T: AppendParam,
{
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String> {
        match self {
            Some(t) => t.append_param_unchecked(appender),
            None => appender.append_null().map_err(|_| appender.error()),
        }
    }
}

macro_rules! impl_append_param_for_primitive {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param_for_primitive! {$ty, $duck_ty, [<append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: &AppenderHandle,
            ) -> Result<(), String> {
                appender.$method(self).map_err(|_| appender.error())
            }
        }
    };
}

macro_rules! impl_append_param {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param! {$ty, $duck_ty, [<append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: &AppenderHandle,
            ) -> Result<(), String> {
                appender
                    .$method(self.into_duckdb())
                    .map_err(|_| appender.error())
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
impl_append_param_for_primitive! {&CStr, varchar}
impl_append_param_for_primitive! {&str, varchar_length}
impl_append_param! {NaiveDate, date}
impl_append_param! {NaiveTime, time}
impl_append_param! {NaiveDateTime, timestamp}
impl_append_param_for_primitive! {&[u8], blob}

unsafe impl AppendParam for String {
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String> {
        appender
            .append_varchar_length(&self)
            .map_err(|_| appender.error())
    }
}

unsafe impl<Tz: TimeZone> AppendParam for DateTime<Tz> {
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String> {
        appender
            .append_timestamp(self.into_duckdb())
            .map_err(|_| appender.error())
    }
}
