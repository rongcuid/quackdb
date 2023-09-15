use std::ffi::CString;

use quackdb_internal::config::ConfigHandle;
use strum::{Display, EnumString};

use crate::error::*;

/// duckdb access mode, default is Automatic
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum AccessMode {
    /// Access mode of the database AUTOMATIC
    #[strum(to_string = "AUTOMATIC")]
    Automatic,
    /// Access mode of the database READ_ONLY
    #[strum(to_string = "READ_ONLY")]
    ReadOnly,
    /// Access mode of the database READ_WRITE
    #[strum(to_string = "READ_WRITE")]
    ReadWrite,
}

/// duckdb default order, default is Asc
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultOrder {
    /// The order type, ASC
    #[strum(to_string = "ASC")]
    Asc,
    /// The order type, DESC
    #[strum(to_string = "DESC")]
    Desc,
}

/// duckdb default null order, default is nulls first
#[derive(Debug, Eq, PartialEq, EnumString, Display)]
pub enum DefaultNullOrder {
    /// Null ordering, NullsFirst
    #[strum(to_string = "NULLS_FIRST")]
    NullsFirst,
    /// Null ordering, NullsLast
    #[strum(to_string = "NULLS_LAST")]
    NullsLast,
}

/// duckdb configuration
/// Refer to https://github.com/duckdb/duckdb/blob/master/src/main/config.cpp
/// Adapted from `duckdb-rs` crate
/// TODO: support everything in the API
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
}

impl Config {
    /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
    pub fn access_mode(mut self, mode: AccessMode) -> DbResult<Config, ConfigError> {
        Ok(self.set("access_mode", &mode.to_string())?.and(Ok(self)))
    }

    /// The order type used when none is specified ([ASC] or DESC)
    pub fn default_order(mut self, order: DefaultOrder) -> DbResult<Config, ConfigError> {
        Ok(self.set("default_order", &order.to_string())?.and(Ok(self)))
    }

    /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
    pub fn default_null_order(
        mut self,
        null_order: DefaultNullOrder,
    ) -> DbResult<Config, ConfigError> {
        Ok(self
            .set("default_null_order", &null_order.to_string())?
            .and(Ok(self)))
    }

    /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
    pub fn enable_external_access(mut self, enabled: bool) -> DbResult<Config, ConfigError> {
        Ok(self
            .set("enable_external_access", &enabled.to_string())?
            .and(Ok(self)))
    }

    /// Whether or not object cache is used to cache e.g. Parquet metadata
    pub fn enable_object_cache(mut self, enabled: bool) -> DbResult<Config, ConfigError> {
        Ok(self
            .set("enable_object_cache", &enabled.to_string())?
            .and(Ok(self)))
    }

    /// Allow to load third-party duckdb extensions.
    pub fn allow_unsigned_extensions(mut self) -> DbResult<Config, ConfigError> {
        Ok(self.set("allow_unsigned_extensions", "true")?.and(Ok(self)))
    }

    /// The maximum memory of the system (e.g. 1GB)
    pub fn max_memory(mut self, memory: &str) -> DbResult<Config, ConfigError> {
        Ok(self.set("max_memory", memory)?.and(Ok(self)))
    }

    /// The number of total threads used by the system
    pub fn threads(mut self, thread_num: i64) -> DbResult<Config, ConfigError> {
        Ok(self.set("threads", &thread_num.to_string())?.and(Ok(self)))
    }

    fn set(&mut self, key: &str, value: &str) -> DbResult<(), ConfigError> {
        if self.handle.is_none() {
            if let Ok(config) = ConfigHandle::create() {
                self.handle = Some(config);
            } else {
                return Ok(Err(ConfigError::CreateError));
            }
        }
        let c_key = CString::new(key)?;
        let c_value = CString::new(value)?;
        if self.handle.as_ref().unwrap().set(&c_key, &c_value).is_ok() {
            Ok(Ok(()))
        } else {
            Ok(Err(ConfigError::SetError(key.to_owned(), value.to_owned())))
        }
    }
}
