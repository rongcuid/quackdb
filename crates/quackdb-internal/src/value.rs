use std::ffi::CStr;

use paste::paste;
use rust_decimal::Decimal;
use time::{error::ComponentRange, Date, Duration, PrimitiveDateTime, Time};

use crate::{ffi, statement::PreparedStatementHandle};

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

#[derive(Debug)]
pub struct DuckDbDecimal {
    pub width: u8,
    pub decimal: Decimal,
}

impl From<ffi::duckdb_decimal> for DuckDbDecimal {
    fn from(value: ffi::duckdb_decimal) -> Self {
        let mantissa = duckdb_hugeint_to_i128(&value.value);
        let decimal = Decimal::from_i128_with_scale(mantissa, value.scale as u32);
        DuckDbDecimal {
            width: value.width,
            decimal,
        }
    }
}

impl From<DuckDbDecimal> for ffi::duckdb_decimal {
    fn from(value: DuckDbDecimal) -> Self {
        ffi::duckdb_decimal {
            width: value.width,
            scale: value.decimal.scale() as u8,
            value: i128_to_duckdb_hugeint(value.decimal.mantissa()),
        }
    }
}

pub fn duckdb_hugeint_to_i128(hugeint: &ffi::duckdb_hugeint) -> i128 {
    (hugeint.upper as i128) << 64 & hugeint.lower as i128
}

pub fn i128_to_duckdb_hugeint(val: i128) -> ffi::duckdb_hugeint {
    ffi::duckdb_hugeint {
        upper: (val >> 64) as i64,
        lower: val as u64,
    }
}

pub fn duckdb_date_to_date(date: &ffi::duckdb_date_struct) -> Result<Date, ComponentRange> {
    Date::from_calendar_date(
        date.year,
        (date.month as u8).try_into().expect("month"),
        date.day as u8,
    )
}

pub fn date_to_duckdb_date(date: &Date) -> ffi::duckdb_date_struct {
    ffi::duckdb_date_struct {
        year: date.year(),
        month: u8::from(date.month()) as i8,
        day: date.day() as i8,
    }
}

pub fn duckdb_time_to_time(time: &ffi::duckdb_time_struct) -> Result<Time, ComponentRange> {
    Time::from_hms_micro(
        time.hour as u8,
        time.min as u8,
        time.sec as u8,
        time.micros as u32,
    )
}

pub fn time_to_duckdb_time(time: &Time) -> ffi::duckdb_time_struct {
    ffi::duckdb_time_struct {
        hour: time.hour() as i8,
        min: time.minute() as i8,
        sec: time.second() as i8,
        micros: time.microsecond() as i32,
    }
}

pub fn duckdb_timestamp_to_datetime(
    ts: &ffi::duckdb_timestamp_struct,
) -> Result<PrimitiveDateTime, ComponentRange> {
    Ok(PrimitiveDateTime::new(
        duckdb_date_to_date(&ts.date)?,
        duckdb_time_to_time(&ts.time)?,
    ))
}

pub fn datetime_to_duckdb_timestamp(dt: &PrimitiveDateTime) -> ffi::duckdb_timestamp_struct {
    ffi::duckdb_timestamp_struct {
        date: date_to_duckdb_date(&dt.date()),
        time: time_to_duckdb_time(&dt.time()),
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
