use std::{
    ffi::{CStr, CString},
    ptr,
    sync::Arc,
};

use crate::{database::Database, error::*, ffi};

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
                return Err(Error::ConnectError);
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

    pub fn query(self: &Arc<Self>, sql: &str) -> Result<()> {
        let cstr = CString::new(sql)?;
        todo!()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_disconnect(&mut self.handle);
        }
    }
}
