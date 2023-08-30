use std::{ptr, sync::Arc};

use thiserror::Error;

use crate::{database::Database, ffi};

type Result<T, E = ConnectionError> = std::result::Result<T, E>;

pub struct Connection {
    pub(crate) handle: ffi::duckdb_connection,
    pub(crate) _db: Arc<Database>,
}

impl Connection {
    pub fn connect(database: Arc<Database>) -> Result<Arc<Connection>> {
        let mut handle = ptr::null_mut();
        unsafe {
            let r = ffi::duckdb_connect(database.handle, &mut handle);
            if r != ffi::DuckDBSuccess {
                return Err(ConnectionError::ConnectError);
            }
        }
        Ok(Arc::new(Connection {
            handle,
            _db: database,
        }))
    }

    pub fn interrupt(self: &Arc<Self>) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }

    pub fn query_progress(self: &Arc<Self>) {
        unsafe { unimplemented!("Not in libduckdb-sys yet") }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_disconnect(&mut self.handle);
        }
    }
}

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("duckdb_connect() error")]
    ConnectError,
}
