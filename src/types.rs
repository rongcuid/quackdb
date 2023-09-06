use std::collections::BTreeMap;

use crate::ffi;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
#[repr(u32)]
pub enum RawType {
    Boolean = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    TinyInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
    SmallInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
    Integer = ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    BigInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    UTinyInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    USmallInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    UInteger = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
    UBigInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    Float = ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
    Double = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    Timestamp = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
    Date = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE,
    Time = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    Interval = ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    HugeInt = ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    VarChar = ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    Blob = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
    Decimal = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    TimestampS = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    TimestampMs = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    TimestampNs = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
    Enum = ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
    List = ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST,
    Struct = ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
    Map = ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP,
    Uuid = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    Union = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UNION,
    Bit = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIT,
}

impl RawType {
    pub fn from_raw(ty: ffi::duckdb_type) -> Option<Self> {
        use RawType::*;
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
}
