use quackdb_internal::types::{LogicalTypeHandle, TypeId};

use super::LogicalKind;

#[derive(Debug)]
pub struct LogicalType {
    pub handle: LogicalTypeHandle,
    pub kind: LogicalKind,
}

impl TryFrom<LogicalTypeHandle> for LogicalType {
    type Error = ();
    fn try_from(value: LogicalTypeHandle) -> Result<Self, Self::Error> {
        Ok(LogicalType {
            kind: LogicalKind::try_from(&value)?,
            handle: value,
        })
    }
}

impl TryFrom<TypeId> for LogicalType {
    type Error = ();
    fn try_from(value: TypeId) -> Result<Self, Self::Error> {
        let handle = unsafe { LogicalTypeHandle::from_id(value) };
        Self::try_from(handle)
    }
}
