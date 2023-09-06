use std::{collections::BTreeMap, ffi::CStr};

use crate::{ffi, types::RawType};

pub struct LogicalType {
    pub(crate) handle: LogicalTypeHandle,
    pub kind: LogicalKind,
}

pub enum LogicalKind {
    Simple {
        type_: RawType,
    },
    Decimal {
        width: u8,
        scale: u8,
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

pub(crate) struct LogicalTypeHandle(pub(crate) ffi::duckdb_logical_type);

impl LogicalType {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Option<Self> {
        Some(Self {
            handle: LogicalTypeHandle(handle),
            kind: LogicalKind::from_raw(handle)?,
        })
    }
}

impl LogicalKind {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Option<Self> {
        let type_: RawType = RawType::from_raw(ffi::duckdb_get_type_id(handle))?;
        Some(match type_ {
            RawType::Decimal => Self::Decimal {
                width: ffi::duckdb_decimal_width(handle),
                scale: ffi::duckdb_decimal_scale(handle),
            },
            RawType::List => Self::List {
                type_: Box::new(LogicalType::from_raw(ffi::duckdb_list_type_child_type(
                    handle,
                ))?),
            },
            RawType::Map => Self::Map {
                key_type: Box::new(LogicalType::from_raw(ffi::duckdb_map_type_key_type(
                    handle,
                ))?),
                value_type: Box::new(LogicalType::from_raw(ffi::duckdb_map_type_value_type(
                    handle,
                ))?),
            },
            RawType::Struct => {
                let count = ffi::duckdb_struct_type_child_count(handle);
                let mut children = BTreeMap::new();
                for i in 0..count {
                    let c = CStr::from_ptr(ffi::duckdb_struct_type_child_name(handle, i));
                    let name = c.to_string_lossy().to_string();
                    let type_ =
                        LogicalType::from_raw(ffi::duckdb_struct_type_child_type(handle, i))?;
                    children.insert(name, type_);
                }
                Self::Struct { children }
            }
            RawType::Union => {
                let count = ffi::duckdb_union_type_member_count(handle);
                let mut members = BTreeMap::new();
                for i in 0..count {
                    let c = CStr::from_ptr(ffi::duckdb_union_type_member_name(handle, i));
                    let name = c.to_string_lossy().to_string();
                    let type_ =
                        LogicalType::from_raw(ffi::duckdb_union_type_member_type(handle, i))?;
                    members.insert(name, type_);
                }
                Self::Struct { children: members }
            }
            ty => Self::Simple { type_ },
        })
    }
}

impl Drop for LogicalTypeHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut self.0);
        }
    }
}
