use std::{
    ffi::{c_char, CString},
    ptr,
};

use strum::{Display, EnumString};

use crate::ffi;

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
#[derive(Default)]
pub struct Config {
    pub(crate) config: Option<ffi::duckdb_config>,
}

impl Config {
    pub(crate) fn duckdb_config(&self) -> ffi::duckdb_config {
        self.config
            .unwrap_or(std::ptr::null_mut() as ffi::duckdb_config)
    }

    /// Access mode of the database ([AUTOMATIC], READ_ONLY or READ_WRITE)
    pub fn access_mode(mut self, mode: AccessMode) -> Result<Config> {
        self.set("access_mode", &mode.to_string())?;
        Ok(self)
    }

    /// The order type used when none is specified ([ASC] or DESC)
    pub fn default_order(mut self, order: DefaultOrder) -> Result<Config> {
        self.set("default_order", &order.to_string())?;
        Ok(self)
    }

    /// Null ordering used when none is specified ([NULLS_FIRST] or NULLS_LAST)
    pub fn default_null_order(mut self, null_order: DefaultNullOrder) -> Result<Config> {
        self.set("default_null_order", &null_order.to_string())?;
        Ok(self)
    }

    /// Allow the database to access external state (through e.g. COPY TO/FROM, CSV readers, pandas replacement scans, etc)
    pub fn enable_external_access(mut self, enabled: bool) -> Result<Config> {
        self.set("enable_external_access", &enabled.to_string())?;
        Ok(self)
    }

    /// Whether or not object cache is used to cache e.g. Parquet metadata
    pub fn enable_object_cache(mut self, enabled: bool) -> Result<Config> {
        self.set("enable_object_cache", &enabled.to_string())?;
        Ok(self)
    }

    /// Allow to load third-party duckdb extensions.
    pub fn allow_unsigned_extensions(mut self) -> Result<Config> {
        self.set("allow_unsigned_extensions", "true")?;
        Ok(self)
    }

    /// The maximum memory of the system (e.g. 1GB)
    pub fn max_memory(mut self, memory: &str) -> Result<Config> {
        self.set("max_memory", memory)?;
        Ok(self)
    }

    /// The number of total threads used by the system
    pub fn threads(mut self, thread_num: i64) -> Result<Config> {
        self.set("threads", &thread_num.to_string())?;
        Ok(self)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if self.config.is_none() {
            let mut config: ffi::duckdb_config = ptr::null_mut();
            let state = unsafe { ffi::duckdb_create_config(&mut config) };
            assert_eq!(state, ffi::DuckDBSuccess);
            self.config = Some(config);
        }
        let c_key = CString::new(key).unwrap();
        let c_value = CString::new(value).unwrap();
        let state = unsafe {
            ffi::duckdb_set_config(
                self.config.unwrap(),
                c_key.as_ptr() as *const c_char,
                c_value.as_ptr() as *const c_char,
            )
        };
        if state != ffi::DuckDBSuccess {
            return Err(ConfigError::ConfigSetError(
                state,
                key.to_owned(),
                value.to_owned(),
            ));
        }
        Ok(())
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        if self.config.is_some() {
            unsafe { ffi::duckdb_destroy_config(&mut self.config.unwrap()) };
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("duckdb_set_config() returns {0}: set {1}:{2} error")]
    ConfigSetError(ffi::duckdb_state, String, String),
}

type Result<T, E = ConfigError> = std::result::Result<T, E>;
