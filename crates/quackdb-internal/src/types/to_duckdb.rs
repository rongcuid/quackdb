use libduckdb_sys::duckdb_hugeint;
use time::{Date, PrimitiveDateTime, Time};

use crate::ffi;

use super::{LogicalTypeHandle, TypeId};

/// Rust primitive types to duckdb types
pub trait ToDuckDbType {
    const DUCKDB_TYPE_ID: TypeId;
    /// Representation to interface with DuckDb
    type DuckDbRepresentation;
    /// Create a duckdb logical type structure for this type
    fn logical_type() -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::from_id(Self::DUCKDB_TYPE_ID) }
    }
    /// Convert to DuckDb representation.
    /// # Panic
    /// If unrepresentable
    fn into_duckdb(self) -> Self::DuckDbRepresentation;
    /// Convert from DuckDb representation
    /// # Panic
    /// If unrepresentable
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self;
}

macro_rules! impl_to_duckdb_for_primitive {
    ($ty:ty, $type_id:expr) => {
        impl ToDuckDbType for $ty {
            type DuckDbRepresentation = $ty;
            const DUCKDB_TYPE_ID: TypeId = $type_id;
            fn into_duckdb(self) -> Self::DuckDbRepresentation {
                self
            }
            fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
                value
            }
        }
    };
}

macro_rules! impl_to_arrow_for_primitive {
    ($ty:ty, $arrow_type:ty) => {
        impl ToArrowType for $ty {
            type ArrowType = $arrow_type;
            type ArrowArrayType = PrimitiveArray<$arrow_type>;
            const ARROW_DATA_TYPE: DataType = <$arrow_type>::DATA_TYPE;
        }
    };
}

impl_to_duckdb_for_primitive! { bool, TypeId::Boolean }
impl_to_duckdb_for_primitive! { i8, TypeId::TinyInt }
impl_to_duckdb_for_primitive! { i16, TypeId::SmallInt }
impl_to_duckdb_for_primitive! { i32, TypeId::Integer }
impl_to_duckdb_for_primitive! { i64, TypeId::BigInt }
impl_to_duckdb_for_primitive! { u8, TypeId::UTinyInt }
impl_to_duckdb_for_primitive! { u16, TypeId::USmallInt }
impl_to_duckdb_for_primitive! { u32, TypeId::UInteger }
impl_to_duckdb_for_primitive! { u64, TypeId::UBigInt }

impl ToDuckDbType for i128 {
    const DUCKDB_TYPE_ID: TypeId = TypeId::HugeInt;

    type DuckDbRepresentation = duckdb_hugeint;

    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        ffi::duckdb_hugeint {
            upper: (self >> 64) as i64,
            lower: self as u64,
        }
    }

    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        (value.upper as i128) << 64 & value.lower as i128
    }
}

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

// /// Rust primitive types to arrow types
// pub trait ToArrowType {
//     type ArrowType;
//     type ArrowArrayType: From<ArrayData>;
//     const ARROW_DATA_TYPE: DataType;
// }

// macro_rules! impl_to_arrow {
//     ($ty:ty, $arrow_type:ty, $array_type:ty) => {
//         impl ToArrowType for $ty {
//             type ArrowType = $arrow_type;
//             type ArrowArrayType = $array_type;
//             const ARROW_DATA_TYPE: DataType = <$arrow_type>::DATA_TYPE;
//         }
//     };
// }

// impl_to_arrow! { bool, BooleanType, BooleanArray }
// impl_to_arrow_for_primitive! { i8, Int8Type }

// impl_to_arrow_for_primitive! { i16, Int16Type, TypeId::SmallInt }
// impl_to_arrow_for_primitive! { i32, Int32Type, TypeId::Integer }
// impl_to_arrow_for_primitive! { i64, Int64Type, TypeId::BigInt }
// impl_to_arrow_for_primitive! { u8, UInt8Type, TypeId::UTinyInt }
// impl_to_arrow_for_primitive! { u16, UInt16Type, TypeId::USmallInt }
// impl_to_arrow_for_primitive! { u32, UInt32Type, TypeId::UInteger }
// impl_to_arrow_for_primitive! { u64, UInt64Type, TypeId::UBigInt }
// impl_to_arrow_for_primitive! { f32, Float32Type, TypeId::Float }
// impl_to_arrow_for_primitive! { f64, Float64Type, TypeId::Double }
// impl_to_arrow! { str, Utf8Type, StringArray, TypeId::VarChar }
