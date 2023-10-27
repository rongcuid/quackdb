use time::{Date, PrimitiveDateTime, Time};

impl ToDuckDbType for Date {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Date;

    type DuckDbRepresentation = ffi::duckdb_date_struct;

    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_date_struct {
            year: self.year(),
            month: u8::from(self.month()) as i8,
            day: self.day() as i8,
        }
    }

    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        Date::from_calendar_date(
            value.year,
            (value.month as u8).try_into().expect("invalid month"),
            value.day as u8,
        )
        .expect("duckdb_date_struct to time::Date")
    }
}

impl ToDuckDbType for Time {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Time;

    type DuckDbRepresentation = ffi::duckdb_time_struct;

    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_time_struct {
            hour: self.hour() as i8,
            min: self.minute() as i8,
            sec: self.second() as i8,
            micros: self.microsecond() as i32,
        }
    }

    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        Time::from_hms_micro(
            value.hour as u8,
            value.min as u8,
            value.sec as u8,
            value.micros as u32,
        )
        .expect("duckdb_time_struct to time::Time")
    }
}

impl ToDuckDbType for PrimitiveDateTime {
    const DUCKDB_TYPE_ID: TypeId = TypeId::Timestamp;

    type DuckDbRepresentation = ffi::duckdb_timestamp_struct;

    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_timestamp_struct {
            date: self.date().into_duckdb(),
            time: self.time().into_duckdb(),
        }
    }

    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        todo!()
    }
}
