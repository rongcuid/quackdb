use chrono::prelude::*;
use quackdb_internal::{ffi, types::TypeId};

use super::{FromDuckDb, IntoDuckDb, ToDuckDbType};

impl ToDuckDbType for NaiveDate {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Date;

    type DuckDbRepresentation = ffi::duckdb_date;
}
impl IntoDuckDb for NaiveDate {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        let s = ffi::duckdb_date_struct {
            year: self.year(),
            month: self.month() as i8,
            day: self.day() as i8,
        };
        unsafe { ffi::duckdb_to_date(s) }
    }
}
impl FromDuckDb for NaiveDate {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        let date = unsafe { ffi::duckdb_from_date(value) };
        Self::from_ymd_opt(date.year, date.month as u32, date.day as u32).expect("from duckdb_date")
    }
}

impl ToDuckDbType for NaiveTime {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Time;

    type DuckDbRepresentation = ffi::duckdb_time;
}
impl IntoDuckDb for NaiveTime {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        let s = ffi::duckdb_time_struct {
            hour: self.hour() as i8,
            min: self.minute() as i8,
            sec: self.second() as i8,
            micros: (self.nanosecond() / 1000) as i32,
        };
        unsafe { ffi::duckdb_to_time(s) }
    }
}
impl FromDuckDb for NaiveTime {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        Self::from_num_seconds_from_midnight_opt(
            (value.micros / 1000000) as u32,
            (value.micros % 1000000 * 1000) as u32,
        )
        .expect("from duckdb_time")
    }
}

impl ToDuckDbType for NaiveDateTime {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Timestamp;

    type DuckDbRepresentation = ffi::duckdb_timestamp;
}
impl IntoDuckDb for NaiveDateTime {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_timestamp {
            micros: self.timestamp_micros(),
        }
    }
}
impl FromDuckDb for NaiveDateTime {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        Self::from_timestamp_micros(value.micros).expect("from duckdb_timestamp")
    }
}

impl<Tz: TimeZone> ToDuckDbType for DateTime<Tz> {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Timestamp;
    type DuckDbRepresentation = ffi::duckdb_timestamp;
}
impl<Tz: TimeZone> IntoDuckDb for DateTime<Tz> {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_timestamp {
            micros: self.timestamp_micros(),
        }
    }
}
impl FromDuckDb for DateTime<Utc> {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        Self::from_timestamp(
            value.micros / 1000000,
            (value.micros % 1000000 * 1000) as u32,
        )
        .expect("from duckdb_timestamp")
    }
}
