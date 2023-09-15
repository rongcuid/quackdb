use paste::paste;
use rust_decimal::Decimal;
use time::{error::ComponentRange, Date, Duration, PrimitiveDateTime, Time};

use crate::{ffi, query_result::QueryResultHandle};

pub unsafe trait FromResult {
    fn from_result(res: &QueryResultHandle, col: u64, row: u64) -> Self;
}

pub fn duckdb_hugeint_to_i128(hugeint: &ffi::duckdb_hugeint) -> i128 {
    (hugeint.upper as i128) << 64 & hugeint.lower as i128
}

pub fn duckdb_hugeint_to_u128(hugeint: &ffi::duckdb_hugeint) -> u128 {
    (hugeint.upper as u128) << 64 & hugeint.lower as u128
}

pub fn duckdb_date_to_date(date: &ffi::duckdb_date_struct) -> Result<Date, ComponentRange> {
    Date::from_calendar_date(
        date.year,
        (date.month as u8).try_into().expect("month"),
        date.day as u8,
    )
}

pub fn duckdb_time_to_time(time: &ffi::duckdb_time_struct) -> Result<Time, ComponentRange> {
    Time::from_hms_micro(
        time.hour as u8,
        time.min as u8,
        time.sec as u8,
        time.micros as u32,
    )
}

pub fn duckdb_timestamp_to_datetime(
    ts: &ffi::duckdb_timestamp_struct,
) -> Result<PrimitiveDateTime, ComponentRange> {
    Ok(PrimitiveDateTime::new(
        duckdb_date_to_date(&ts.date)?,
        duckdb_time_to_time(&ts.time)?,
    ))
}

macro_rules! impl_from_result_for_value {
    ($ty:ty) => {
        paste! {
            impl_from_result_for_value! {$ty, [<value_ $ty>]}
        }
    };
    ($ty:ty, $method:ident) => {
        unsafe impl FromResult for $ty {
            fn from_result(res: &QueryResultHandle, col: u64, row: u64) -> Self {
                unsafe { res.$method(col, row) }
            }
        }
    };
}

impl_from_result_for_value! {bool}
impl_from_result_for_value! {i8}
impl_from_result_for_value! {i16}
impl_from_result_for_value! {i32}
impl_from_result_for_value! {i64}
impl_from_result_for_value! {i128}
impl_from_result_for_value! {Decimal, value_decimal}
impl_from_result_for_value! {u8}
impl_from_result_for_value! {u16}
impl_from_result_for_value! {u32}
impl_from_result_for_value! {u64}
impl_from_result_for_value! {f32}
impl_from_result_for_value! {f64}
impl_from_result_for_value! {String, value_string}
impl_from_result_for_value! {Date, value_date}
impl_from_result_for_value! {Time, value_time}
impl_from_result_for_value! {PrimitiveDateTime, value_timestamp}
impl_from_result_for_value! {Duration, value_interval}
impl_from_result_for_value! {Vec<u8>, value_blob}
