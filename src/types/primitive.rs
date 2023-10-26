use arrow::{
    array::{Array, ArrayData, ArrayRef, BooleanArray, PrimitiveArray, StringArray},
    datatypes::*,
};
use quackdb_internal::types::TypeId;

/// Rust primitive types supported
pub trait PrimitiveType {
    type ArrowType;
    type ArrowArrayType: From<ArrayData>;
    const ARROW_DATA_TYPE: DataType;
    const DUCKDB_TYPE_ID: TypeId;
}

macro_rules! impl_primitive_for_primitive {
    ($ty:ty, $arrow_type:ty, $type_id:expr) => {
        impl PrimitiveType for $ty {
            type ArrowType = $arrow_type;
            type ArrowArrayType = PrimitiveArray<$arrow_type>;
            const ARROW_DATA_TYPE: DataType = <$arrow_type>::DATA_TYPE;
            const DUCKDB_TYPE_ID: TypeId = $type_id;
        }
    };
}

macro_rules! impl_primitive {
    ($ty:ty, $arrow_type:ty, $array_type:ty, $type_id:expr) => {
        impl PrimitiveType for $ty {
            type ArrowType = $arrow_type;
            type ArrowArrayType = $array_type;
            const ARROW_DATA_TYPE: DataType = <$arrow_type>::DATA_TYPE;
            const DUCKDB_TYPE_ID: TypeId = $type_id;
        }
    };
}

impl_primitive! { bool, BooleanType, BooleanArray, TypeId::Boolean }
impl_primitive_for_primitive! { i8, Int8Type, TypeId::TinyInt }
impl_primitive_for_primitive! { i16, Int16Type, TypeId::SmallInt }
impl_primitive_for_primitive! { i32, Int32Type, TypeId::Integer }
impl_primitive_for_primitive! { i64, Int64Type, TypeId::BigInt }
impl_primitive_for_primitive! { u8, UInt8Type, TypeId::UTinyInt }
impl_primitive_for_primitive! { u16, UInt16Type, TypeId::USmallInt }
impl_primitive_for_primitive! { u32, UInt32Type, TypeId::UInteger }
impl_primitive_for_primitive! { u64, UInt64Type, TypeId::UBigInt }
impl_primitive_for_primitive! { f32, Float32Type, TypeId::Float }
impl_primitive_for_primitive! { f64, Float64Type, TypeId::Double }
impl_primitive! { str, Utf8Type, StringArray, TypeId::VarChar }
