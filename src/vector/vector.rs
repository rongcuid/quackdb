use std::sync::Arc;

use quackdb_internal::{types::LogicalType, vector::VectorHandle};

use crate::error::Error;

use super::Validity;

#[derive(Debug)]
pub struct Vector {
    pub handle: Arc<VectorHandle>,
}

impl From<Arc<VectorHandle>> for Vector {
    fn from(value: Arc<VectorHandle>) -> Self {
        Self { handle: value }
    }
}

impl Vector {
    pub fn column_type(&self) -> Option<LogicalType> {
        unsafe { self.handle.column_type().into() }
    }
    pub fn data(&self) {
        todo!()
    }
    pub fn validity(&self) -> Option<Validity> {
        self.handle.validity().map(Validity::from)
    }
    pub fn ensure_validity_writable(&self) {
        self.handle.ensure_validity_writable()
    }
    pub fn assign_string_element(&self, _index: u64, _str: &str) -> Result<(), Error> {
        todo!()
    }
    pub fn child(&self) {
        todo!()
    }
    pub fn set_size(&mut self) {
        todo!()
    }
    pub fn reserve(&mut self) {
        todo!()
    }
}
