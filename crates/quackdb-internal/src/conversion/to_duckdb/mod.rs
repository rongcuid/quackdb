mod primitive;
pub use primitive::*;

mod chrono;
use crate::{handles::LogicalTypeHandle, type_id::TypeId};
pub use chrono::*;

/// Rust primitive types to duckdb types
pub trait ToDuckDbType {
    const DUCKDB_TYPE_ID: TypeId;
    /// Representation to interface with DuckDb
    type DuckDbRepresentation;
    /// Create a duckdb logical type structure for this type
    fn logical_type() -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::from_id(Self::DUCKDB_TYPE_ID) }
    }
}

pub trait IntoDuckDb
where
    Self: ToDuckDbType,
{
    /// Convert to DuckDb representation.
    /// # Panic
    /// If unrepresentable
    fn into_duckdb(self) -> Self::DuckDbRepresentation;
}
pub trait FromDuckDb
where
    Self: ToDuckDbType,
{
    /// Convert from DuckDb representation
    /// # Panic
    /// If unrepresentable
    fn from_duckdb(value: Self::DuckDbRepresentation) -> Self;
}
