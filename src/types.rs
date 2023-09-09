use std::collections::BTreeMap;

use quackdb_internal::types::{LogicalTypeHandle, TypeId};

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

#[derive(Debug)]
pub enum LogicalKind {
    Simple(TypeId),
    Decimal {
        width: u8,
        scale: u8,
    },
    Enum {
        internal: TypeId,
        dictionary: Vec<String>,
    },
    List {
        type_: Box<LogicalType>,
    },
    Map {
        key_type: Box<LogicalType>,
        value_type: Box<LogicalType>,
    },
    Union {
        members: BTreeMap<String, LogicalType>,
    },
    Struct {
        children: BTreeMap<String, LogicalType>,
    },
}

impl TryFrom<&LogicalTypeHandle> for LogicalKind {
    type Error = ();

    fn try_from(handle: &LogicalTypeHandle) -> Result<Self, Self::Error> {
        let type_ = handle.type_id().ok_or(())?;
        unsafe {
            Ok(match type_ {
                TypeId::Decimal => Self::Decimal {
                    width: handle.decimal_width(),
                    scale: handle.decimal_scale(),
                },
                TypeId::Enum => {
                    let internal = handle.enum_internal_type();
                    let size = handle.enum_dictionary_size();
                    let mut dictionary = Vec::new();
                    for i in 0..size {
                        let name = handle.enum_dictionary_value(i as u64);
                        dictionary.push(name);
                    }
                    Self::Enum {
                        internal,
                        dictionary,
                    }
                }
                TypeId::List => Self::List {
                    type_: Box::new(handle.list_type_child_type().try_into()?),
                },
                TypeId::Map => Self::Map {
                    key_type: Box::new(handle.map_type_key_type().try_into()?),
                    value_type: Box::new(handle.map_type_value_type().try_into()?),
                },
                TypeId::Struct => {
                    let count = handle.struct_type_child_count();
                    let mut children = BTreeMap::new();
                    for i in 0..count {
                        let name = handle.struct_type_child_name(i);
                        let type_ = handle.struct_type_child_type(i).try_into()?;
                        children.insert(name, type_);
                    }
                    Self::Struct { children }
                }
                TypeId::Union => {
                    let count = handle.union_type_member_count();
                    let mut members = BTreeMap::new();
                    for i in 0..count {
                        let name = handle.union_type_member_name(i);
                        let type_ = handle.union_type_member_type(i).try_into()?;
                        members.insert(name, type_);
                    }
                    Self::Union { members }
                }
                ty => Self::Simple(ty),
            })
        }
    }
}

impl LogicalKind {}
