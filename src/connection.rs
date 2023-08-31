use std::{
    ffi::{CStr, CString},
    mem::MaybeUninit,
    ptr,
    sync::Arc,
};

use crate::{
    database::Database,
    error::*,
    ffi,
    query::{QueryParent, QueryResult},
};

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

    pub fn query(self: &Arc<Self>, sql: &str) -> Result<Arc<QueryResult>> {
        let cstr = CString::new(sql)?;
        let p = cstr.as_ptr();
        unsafe {
            let mut result: ffi::duckdb_result = std::mem::zeroed();
            let r = ffi::duckdb_query(self.handle, p, &mut result);
            // Create the result type to let Rust manage the memory
            let result = QueryResult::from_raw_connection(result, self.clone());
            // Then check if there was an error
            if r != ffi::DuckDBSuccess {
                return Err(Error::QueryError);
            }
            Ok(result)
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_disconnect(&mut self.handle);
        }
    }
}
