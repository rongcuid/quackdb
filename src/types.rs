use crate::ffi;

#[derive(Debug, Clone, Copy)]
pub enum DuckType {
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
    //Json,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RawType(pub ffi::duckdb_type);

impl From<RawType> for Option<DuckType> {
    fn from(value: RawType) -> Self {
        use DuckType::*;
        match value.0 {
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
            //ffi::DUCKDB_TYPE_DUCKDB_TYPE_JSON => Some(Json),
            _ => unreachable!("DUCKDB_TYPE"),
        }
    }
}
