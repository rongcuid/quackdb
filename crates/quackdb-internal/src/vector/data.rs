use std::{ffi::c_void, ops::Deref};

#[derive(Debug)]
pub struct DataHandle(*mut c_void);

impl Deref for DataHandle {
    type Target = *mut c_void;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DataHandle {
    /// # Safety
    /// Takes ownership
    pub unsafe fn from_raw(raw: *mut c_void) -> Self {
        Self(raw)
    }
}
