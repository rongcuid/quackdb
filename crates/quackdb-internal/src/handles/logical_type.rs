use std::{ffi::CStr, ops::Deref};

use crate::{ffi, type_id::TypeId};

#[derive(Debug)]
pub struct LogicalTypeHandle(ffi::duckdb_logical_type);

impl LogicalTypeHandle {
    pub unsafe fn from_raw(handle: ffi::duckdb_logical_type) -> Self {
        Self(handle)
    }
    pub unsafe fn from_id(type_: TypeId) -> Self {
        Self::from_raw(ffi::duckdb_create_logical_type(type_.to_raw()))
    }
    pub fn type_id(&self) -> Option<TypeId> {
        unsafe { TypeId::from_raw(ffi::duckdb_get_type_id(self.0)) }
    }
    pub unsafe fn decimal_width(&self) -> u8 {
        ffi::duckdb_decimal_width(self.0)
    }
    pub unsafe fn decimal_scale(&self) -> u8 {
        ffi::duckdb_decimal_scale(self.0)
    }
    pub unsafe fn enum_internal_type(&self) -> TypeId {
        TypeId::from_raw(ffi::duckdb_enum_internal_type(self.0)).expect("invalid type")
    }
    pub unsafe fn enum_dictionary_size(&self) -> u32 {
        ffi::duckdb_enum_dictionary_size(self.0)
    }
    pub unsafe fn enum_dictionary_value(&self, index: u64) -> String {
        let p = ffi::duckdb_enum_dictionary_value(self.0, index);
        let name = CStr::from_ptr(p).to_string_lossy().into_owned();
        ffi::duckdb_free(p as _);
        name
    }
    pub unsafe fn list_type_child_type(&self) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_list_type_child_type(self.0))
    }
    pub unsafe fn map_type_key_type(&self) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_map_type_key_type(self.0))
    }
    pub unsafe fn map_type_value_type(&self) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_map_type_value_type(self.0))
    }
    pub unsafe fn struct_type_child_count(&self) -> u64 {
        ffi::duckdb_struct_type_child_count(self.0)
    }
    pub unsafe fn struct_type_child_name(&self, index: u64) -> String {
        let p = ffi::duckdb_struct_type_child_name(self.0, index);
        let name = CStr::from_ptr(p).to_string_lossy().into_owned();
        ffi::duckdb_free(p as _);
        name
    }
    pub unsafe fn struct_type_child_type(&self, index: u64) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_struct_type_child_type(self.0, index))
    }
    pub unsafe fn union_type_member_count(&self) -> u64 {
        ffi::duckdb_union_type_member_count(self.0)
    }
    pub unsafe fn union_type_member_name(&self, index: u64) -> String {
        let p = ffi::duckdb_union_type_member_name(self.0, index);
        let name = CStr::from_ptr(p).to_string_lossy().into_owned();
        ffi::duckdb_free(p as _);
        name
    }
    pub unsafe fn union_type_member_type(&self, index: u64) -> LogicalTypeHandle {
        LogicalTypeHandle::from_raw(ffi::duckdb_union_type_member_type(self.0, index))
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
