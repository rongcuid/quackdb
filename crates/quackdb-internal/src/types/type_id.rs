use std::any;

use time::{Date, Duration, PrimitiveDateTime, Time};

use crate::{ffi, value::DuckDbDecimal};

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum TypeId {
    Boolean,
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    UTinyInt,
    USmallInt,
    UInteger,
    UBigInt,
    Float,
    Double,
    Timestamp,
    Date,
    Time,
    Interval,
    HugeInt,
    VarChar,
    Blob,
    Decimal,
    TimestampS,
    TimestampMs,
    TimestampNs,
    Enum,
    List,
    Struct,
    Map,
    Uuid,
    Union,
    Bit,
}

impl TypeId {
    pub unsafe fn from_raw(ty: ffi::duckdb_type) -> Option<Self> {
        use TypeId::*;
        match ty {
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_INVALID => None,
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Some(Boolean),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Some(TinyInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Some(SmallInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Some(Integer),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Some(BigInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Some(UTinyInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Some(USmallInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Some(UInteger),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Some(UBigInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Some(Float),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Some(Double),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => Some(Timestamp),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE => Some(Date),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME => Some(Time),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => Some(Interval),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Some(HugeInt),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Some(VarChar),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Some(Blob),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Some(Decimal),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => Some(TimestampS),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => Some(TimestampMs),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => Some(TimestampNs),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM => Some(Enum),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST => Some(List),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => Some(Struct),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP => Some(Map),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID => Some(Uuid),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_UNION => Some(Union),
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIT => Some(Bit),
            _ => unreachable!("DUCKDB_TYPE"),
        }
    }
    pub fn to_raw(self) -> ffi::duckdb_type {
        use TypeId::*;
        match self {
            Boolean => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
            TinyInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
            SmallInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
            Integer => ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
            BigInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
            UTinyInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
            USmallInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
            UInteger => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
            UBigInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
            Float => ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
            Double => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
            Timestamp => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
            Date => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE,
            Time => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME,
            Interval => ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
            HugeInt => ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
            VarChar => ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
            Blob => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
            Decimal => ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
            TimestampS => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
            TimestampMs => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
            TimestampNs => ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
            Enum => ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
            List => ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST,
            Struct => ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
            Map => ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP,
            Uuid => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID,
            Union => ffi::DUCKDB_TYPE_DUCKDB_TYPE_UNION,
            Bit => ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIT,
        }
    }
    /// If the type is a `T`
    pub fn is<T: ?Sized + 'static>(self) -> bool {
        let tid = any::TypeId::of::<T>();
        match self {
            TypeId::Boolean => tid == any::TypeId::of::<bool>(),
            TypeId::TinyInt => tid == any::TypeId::of::<i8>(),
            TypeId::SmallInt => tid == any::TypeId::of::<i16>(),
            TypeId::Integer => tid == any::TypeId::of::<i32>(),
            TypeId::BigInt => tid == any::TypeId::of::<i64>(),
            TypeId::UTinyInt => tid == any::TypeId::of::<u8>(),
            TypeId::USmallInt => tid == any::TypeId::of::<u16>(),
            TypeId::UInteger => tid == any::TypeId::of::<u32>(),
            TypeId::UBigInt => tid == any::TypeId::of::<u64>(),
            TypeId::Float => tid == any::TypeId::of::<f32>(),
            TypeId::Double => tid == any::TypeId::of::<f64>(),
            TypeId::Timestamp => tid == any::TypeId::of::<PrimitiveDateTime>(),
            TypeId::Date => tid == any::TypeId::of::<Date>(),
            TypeId::Time => tid == any::TypeId::of::<Time>(),
            TypeId::Interval => tid == any::TypeId::of::<Duration>(),
            TypeId::HugeInt => tid == any::TypeId::of::<i128>(),
            TypeId::VarChar => {
                tid == any::TypeId::of::<&str>() || tid == any::TypeId::of::<String>()
            }
            TypeId::Blob => tid == any::TypeId::of::<&[u8]>(),
            TypeId::Decimal => tid == any::TypeId::of::<DuckDbDecimal>(),
            _ => false,
        }
    }
}
