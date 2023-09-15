use std::{
    ffi::{c_void, CStr},
    ops::Deref,
};

use time::{error::ComponentRange, Date, Duration, PrimitiveDateTime, Time};

use crate::ffi;

#[derive(Debug)]
pub struct ValueHandle(ffi::duckdb_value);

impl Deref for ValueHandle {
    type Target = ffi::duckdb_value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ValueHandle {
    fn drop(&mut self) {
        unsafe { self.destroy() }
    }
}

impl ValueHandle {
    pub fn create_varchar(text: &CStr) -> Self {
        Self(unsafe { ffi::duckdb_create_varchar(text.as_ptr()) })
    }
    pub fn create_i64(val: i64) -> Self {
        unsafe { Self(ffi::duckdb_create_int64(val)) }
    }
    /// # Safety
    /// Does not consider usage. Normally, let `Drop` handle this.
    pub unsafe fn destroy(&mut self) {
        ffi::duckdb_destroy_value(&mut self.0);
    }
    /// # Safety
    /// The value must be a varchar value
    pub unsafe fn varchar(&self) -> String {
        let p = ffi::duckdb_get_varchar(self.0);
        let text = CStr::from_ptr(p).to_string_lossy().to_owned().to_string();
        ffi::duckdb_free(p as *mut c_void);
        text
    }
    /// # Safety
    /// The value must be an int64 value
    pub unsafe fn i64(&self) -> i64 {
        ffi::duckdb_get_int64(self.0)
    }
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
