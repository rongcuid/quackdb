use std::ffi::{c_char, CStr};

use quackdb_internal::{ffi, types::TypeId};

use super::{FromDuckDb, IntoDuckDb, ToDuckDbType};

fn i128_to_hugeint(value: i128) -> ffi::duckdb_hugeint {
    ffi::duckdb_hugeint {
        upper: (value >> 64) as i64,
        lower: value as u64,
    }
}

macro_rules! impl_to_duckdb_for_primitive {
    ($ty:ty, $type_id:expr) => {
        impl ToDuckDbType for $ty {
            type DuckDbRepresentation = $ty;
            const DUCKDB_TYPE_ID: TypeId = $type_id;
        }
        impl IntoDuckDb for $ty {
            fn into_duckdb(self) -> Self::DuckDbRepresentation {
                self
            }
        }
        impl FromDuckDb for $ty {
            fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
                value
            }
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
impl_to_duckdb_for_primitive! { f32, TypeId::Float }
impl_to_duckdb_for_primitive! { f64, TypeId::Double }

impl ToDuckDbType for i128 {
    const DUCKDB_TYPE_ID: TypeId = TypeId::HugeInt;

    type DuckDbRepresentation = ffi::duckdb_hugeint;
}
impl IntoDuckDb for i128 {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        i128_to_hugeint(self)
    }
}
impl FromDuckDb for i128 {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        (value.upper as i128) << 64 & value.lower as i128
    }
}

impl ToDuckDbType for &CStr {
    const DUCKDB_TYPE_ID: TypeId = TypeId::VarChar;

    type DuckDbRepresentation = *const c_char;
}
impl IntoDuckDb for &CStr {
    fn into_duckdb(self) -> Self::DuckDbRepresentation {
        self.as_ptr()
    }
}
impl FromDuckDb for &CStr {
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self {
        unsafe { CStr::from_ptr(value) }
    }
}
