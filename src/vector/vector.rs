use std::{ffi::CString, sync::Arc};

use quackdb_internal::{types::TypeId, vector::VectorHandle};

use crate::{
    error::Error,
    types::{LogicalKind, LogicalType},
};

use super::Validity;

#[derive(Debug)]
pub struct Vector {
    pub handle: Arc<VectorHandle>,
    pub type_: LogicalType,
}

#[derive(thiserror::Error, Debug)]
pub enum VectorError {}

impl From<Arc<VectorHandle>> for Vector {
    fn from(value: Arc<VectorHandle>) -> Self {
        Self {
            type_: value.column_type().try_into().expect("invalid type"),
            handle: value,
        }
    }
}

impl Vector {
    pub fn column_type(&self) -> Option<LogicalType> {
        self.handle.column_type().try_into().ok()
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<Validity> {
        Some(self.handle.validity()?.into())
    }
    pub fn ensure_validity_writable(&self) {
        self.handle.ensure_validity_writable()
    }
    // pub fn assign_string_element(&self, index: u64, str: &str) -> Result<(), Error> {
    //     self.check_index(index);
    //     assert!(matches!(
    //         self.type_.kind,
    //         LogicalKind::Simple(TypeId::VarChar)
    //     ));
    //     let p = CString::new(str)?.as_ptr();
    //     unsafe {
    //         self.handle.assign_string_element(index, p);
    //     }
    //     Ok(())
    // }
    // pub fn child(&self) {
    //     todo!()
    // }
    // pub fn set_size(&mut self, size: u64) {
    //     assert!(matches!(self.type_.kind, LogicalKind::List { .. }));
    //     unsafe { self.handle.list_set_size(size) }
    // }
    // pub fn reserve(&mut self) {
    //     todo!()
    // }
}

impl Vector {
    fn check_index(&self, idx: u64) {
        assert!(idx < self.handle.size())
    }
}
