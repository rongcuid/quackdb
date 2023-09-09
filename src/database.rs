use std::{path::Path, sync::Arc};

use quackdb_internal::database::DatabaseHandle;

use crate::{config::Config, connection::Connection, cutils::option_path_to_ptr, error::*};

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
}

impl From<Arc<DatabaseHandle>> for Database {
    fn from(value: Arc<DatabaseHandle>) -> Self {
        Self { handle: value }
    }
}

impl Database {
    /// Open a database. `Some(path)` opens a file, while `None` opens an in-memory db.
    pub fn open(path: Option<&Path>) -> DbResult<Self, DatabaseError> {
        Self::open_ext(path, &Config::default())
    }

    /// Extended open
    pub fn open_ext(path: Option<&Path>, config: &Config) -> DbResult<Database, DatabaseError> {
        let p_path = option_path_to_ptr(path)?;
        unsafe {
            Ok(DatabaseHandle::open_ext(p_path, &config.handle)
                .map_err(DatabaseError::OpenError)
                .map(Self::from))
        }
    }

    pub fn connect(&self) -> Result<Connection, DatabaseError> {
        unsafe {
            self.handle
                .connect()
                .map(Connection::from)
                .map_err(|_| DatabaseError::ConnectError)
        }
    }

    pub fn library_version() -> String {
        DatabaseHandle::library_version()
    }
}

/// Some tests are adapted from `duckdb-rs`
#[cfg(test)]
mod test {
    use crate::config::AccessMode;

    use super::*;
    #[test]
    fn test_open() {
        let db = Database::open(None);
        assert!(db.is_ok());
    }

    #[test]
    fn test_open_failure() -> DbResult<(), DatabaseError> {
        let filename = "no_such_file.db";
        let result = Database::open_ext(
            // Some(Path::new(filename)).as_deref(),
            Some(filename.as_ref()),
            &Config::default()
                .access_mode(AccessMode::ReadOnly)?
                .unwrap(),
        );
        assert!(matches!(result, Ok(Err(DatabaseError::OpenError(_)))));
        Ok(Ok(()))
    }
}
