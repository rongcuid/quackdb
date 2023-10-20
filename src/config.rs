use std::ffi::{CString, NulError};

use quackdb_internal::config::ConfigHandle;

/// duckdb configuration
#[derive(Default, Debug)]
pub struct Config {
    pub handle: Option<ConfigHandle>,
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
    pub fn set<T: ToString>(&mut self, key: &str, value: T) -> Result<&mut Config, ConfigError> {
        let value = value.to_string();
        if self.handle.is_none() {
            if let Ok(config) = ConfigHandle::create() {
                self.handle = Some(config);
            } else {
                return Err(ConfigError::CreateError);
            }
        }
        let c_key = CString::new(key)?;
        let c_value = CString::new(value.clone())?;
        if self.handle.as_ref().unwrap().set(&c_key, &c_value).is_ok() {
            Ok(self)
        } else {
            Err(ConfigError::SetError(key.to_owned(), value))
        }
    }
}
