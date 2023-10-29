use std::ops::Deref;

use quackdb_internal::{ffi, handles::LogicalTypeHandle, type_id::TypeId};
use thiserror::Error;

#[derive(Debug)]
pub struct LogicalType {
    handle: LogicalTypeHandle,
}

#[derive(Error, Debug)]
pub enum LogicalTypeError {
    #[error("duckdb_create_logical_type() should not be used with DUCKDB_TYPE_DECIMAL")]
    DecimalError,
}

impl From<LogicalTypeHandle> for LogicalType {
    fn from(value: LogicalTypeHandle) -> Self {
        LogicalType { handle: value }
    }
}

impl TryFrom<TypeId> for LogicalType {
    type Error = LogicalTypeError;
    fn try_from(value: TypeId) -> Result<Self, Self::Error> {
        if matches!(value, TypeId::Decimal) {
            return Err(LogicalTypeError::DecimalError);
        }
        let handle = unsafe { LogicalTypeHandle::from_id(value) };
        Ok(Self::from(handle))
    }
}

impl Deref for LogicalType {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
