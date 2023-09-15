use time::{error::ComponentRange, Date, PrimitiveDateTime, Time};

use crate::ffi;

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
