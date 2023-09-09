use std::{
    collections::BTreeMap,
    ffi::{c_void, CStr},
    ops::Deref,
};

use crate::ffi;

use super::TypeId;

#[derive(Debug)]
pub struct LogicalType {
    pub handle: LogicalTypeHandle,
    pub kind: LogicalKind,
}

impl From<LogicalTypeHandle> for Option<LogicalType> {
    fn from(value: LogicalTypeHandle) -> Self {
        unsafe {
            let raw = value.0;
            Some(LogicalType {
                handle: value,
                kind: LogicalKind::from_raw(raw)?,
            })
        }
    }
}

impl LogicalType {
    pub unsafe fn from_raw(raw: ffi::duckdb_logical_type) -> Option<Self> {
        Some(Self {
            handle: LogicalTypeHandle::from_raw(raw),
            kind: LogicalKind::from_raw(raw)?,
        })
    }
}

#[derive(Debug)]
pub struct LogicalTypeHandle(ffi::duckdb_logical_type);

impl LogicalTypeHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Self {
        Self(handle)
    }
    pub unsafe fn from_id(type_: TypeId) -> Self {
        match type_ {
            TypeId::Decimal => {
                panic!("duckdb_create_logical_type() should not be used with DUCKDB_TYPE_DECIMAL")
            }
            id => Self::from_raw(ffi::duckdb_create_logical_type(id.to_raw())),
        }
    }
    pub fn type_id(&self) -> TypeId {
        unsafe { TypeId::from_raw(ffi::duckdb_get_type_id(self.0)).expect("logical type invalid") }
    }
}

impl Deref for LogicalTypeHandle {
    type Target = ffi::duckdb_logical_type;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for LogicalTypeHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_destroy_logical_type(&mut self.0);
        }
    }
}

#[derive(Debug)]
pub enum LogicalKind {
    Simple {
        type_: TypeId,
    },
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

impl LogicalKind {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Option<Self> {
        let type_: TypeId = TypeId::from_raw(ffi::duckdb_get_type_id(handle))?;
        Some(match type_ {
            TypeId::Decimal => Self::Decimal {
                width: ffi::duckdb_decimal_width(handle),
                scale: ffi::duckdb_decimal_scale(handle),
            },
            TypeId::Enum => {
                let internal = TypeId::from_raw(ffi::duckdb_enum_internal_type(handle))?;
                let size = ffi::duckdb_enum_dictionary_size(handle);
                let mut dictionary = Vec::new();
                for i in 0..size {
                    let p = ffi::duckdb_enum_dictionary_value(handle, i as u64);
                    let name = CStr::from_ptr(p).to_string_lossy().to_string();
                    ffi::duckdb_free(p as *mut c_void);
                    dictionary.push(name);
                }
                Self::Enum {
                    internal,
                    dictionary,
                }
            }
            TypeId::List => Self::List {
                type_: Box::new(LogicalType::from_raw(ffi::duckdb_list_type_child_type(
                    handle,
                ))?),
            },
            TypeId::Map => Self::Map {
                key_type: Box::new(LogicalType::from_raw(ffi::duckdb_map_type_key_type(
                    handle,
                ))?),
                value_type: Box::new(LogicalType::from_raw(ffi::duckdb_map_type_value_type(
                    handle,
                ))?),
            },
            TypeId::Struct => {
                let count = ffi::duckdb_struct_type_child_count(handle);
                let mut children = BTreeMap::new();
                for i in 0..count {
                    let p = ffi::duckdb_struct_type_child_name(handle, i);
                    let name = CStr::from_ptr(p).to_string_lossy().to_string();
                    ffi::duckdb_free(p as *mut c_void);
                    let type_ =
                        LogicalType::from_raw(ffi::duckdb_struct_type_child_type(handle, i))?;
                    children.insert(name, type_);
                }
                Self::Struct { children }
            }
            TypeId::Union => {
                let count = ffi::duckdb_union_type_member_count(handle);
                let mut members = BTreeMap::new();
                for i in 0..count {
                    let p = ffi::duckdb_union_type_member_name(handle, i);
                    let name = CStr::from_ptr(p).to_string_lossy().to_string();
                    ffi::duckdb_free(p as *mut c_void);
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
