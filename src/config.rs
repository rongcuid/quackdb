use std::{
    ffi::{CString, NulError},
    ops::Deref,
};

use quackdb_internal::{config::ConfigHandle, ffi};

/// duckdb configuration
#[derive(Debug)]
pub struct Config {
    handle: ConfigHandle,
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("config create error")]
    CreateError,
    #[error("config set error: {0}:{1}")]
    SetError(String, String),
    #[error(transparent)]
    NulError(#[from] NulError),
}

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        unsafe {
            let mut config: ffi::duckdb_config = std::ptr::null_mut();
            if ffi::duckdb_create_config(&mut config) != ffi::DuckDBSuccess {
                return Err(ConfigError::CreateError);
            }
            Ok(Self {
                handle: ConfigHandle::from_raw(config),
            })
        }
    }
    pub fn set<T: ToString>(&mut self, key: &str, value: T) -> Result<&mut Config, ConfigError> {
        let value = value.to_string();
        let c_key = CString::new(key)?;
        let c_value = CString::new(value.clone())?;
        let state =
            unsafe { ffi::duckdb_set_config(*self.handle, c_key.as_ptr(), c_value.as_ptr()) };
        if state != ffi::DuckDBSuccess {
            return Err(ConfigError::SetError(key.to_owned(), value));
        }
        Ok(self)
    }
}

impl Deref for Config {
    type Target = ConfigHandle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}
