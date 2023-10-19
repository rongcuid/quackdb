use std::{ffi::NulError, path::Path, sync::Arc};

use quackdb_internal::database::DatabaseHandle;

use crate::{config::Config, connection::Connection, cutils::option_path_to_cstring};

#[derive(Debug)]
pub struct Database {
    pub handle: Arc<DatabaseHandle>,
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("duckdb open error: {0}")]
    OpenError(String),
    #[error("duckdb connect error")]
    ConnectError,
    #[error(transparent)]
    NulError(#[from] NulError),
}

impl From<Arc<DatabaseHandle>> for Database {
    fn from(value: Arc<DatabaseHandle>) -> Self {
        Self { handle: value }
    }
}

impl Database {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<&Path>) -> Result<Self, DatabaseError> {
        Self::open_ext(path, &Config::default())
    }

    /// Extended open
    pub fn open_ext(path: Option<&Path>, config: &Config) -> Result<Database, DatabaseError> {
        let c_path = option_path_to_cstring(path)?;
        DatabaseHandle::open_ext(c_path.as_deref(), config.handle.as_ref())
            .map_err(DatabaseError::OpenError)
            .map(Self::from)
    }

    pub fn connect(&self) -> Result<Connection, DatabaseError> {
        self.handle
            .connect()
            .map(Connection::from)
            .map_err(|_| DatabaseError::ConnectError)
    }
}

/// Some tests are adapted from `duckdb-rs`
#[cfg(test)]
mod test {

    use super::*;
    #[test]
    fn test_open() {
        let db = Database::open(None);
        assert!(db.is_ok());
    }

    #[test]
    fn test_open_failure() -> Result<(), DatabaseError> {
        let filename = "no_such_file.db";
        let result = Database::open_ext(
            // Some(Path::new(filename)).as_deref(),
            Some(filename.as_ref()),
            Config::default().set("access_mode", "read_only").unwrap(),
        );
        assert!(matches!(result, Err(DatabaseError::OpenError(_))));
        Ok(())
    }
}
