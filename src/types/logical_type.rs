use std::ops::Deref;

use quackdb_internal::{ffi, handles::LogicalTypeHandle, type_id::TypeId};

#[derive(Debug)]
pub struct LogicalType {
    handle: LogicalTypeHandle,
}

impl From<LogicalTypeHandle> for LogicalType {
    fn from(value: LogicalTypeHandle) -> Self {
        LogicalType { handle: value }
    }
}

impl From<TypeId> for LogicalType {
    fn from(value: TypeId) -> Self {
        if matches!(value, TypeId::Decimal) {
            panic!("duckdb_create_logical_type() should not be used with DUCKDB_TYPE_DECIMAL")
        }
        let handle = unsafe { LogicalTypeHandle::from_id(value) };
        Self::from(handle)
    }
}

impl Deref for LogicalType {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
