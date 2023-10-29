use strum::FromRepr;

use crate::ffi;

#[derive(Debug, Clone, Copy, FromRepr)]
#[repr(u32)]
#[non_exhaustive]
pub enum TypeId {
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
