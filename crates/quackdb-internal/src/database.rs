use std::{ffi::c_char, ops::Deref};

use crate::ffi;

#[derive(Debug)]
pub struct DatabaseHandle(ffi::duckdb_database);

impl DatabaseHandle {
    // pub unsafe fn open_ext(
    //     path: *const c_char,
    //     config: &Config,
    // ) -> DbResult<Database, DatabaseError> {
    //     let p_path = option_path_to_ptr(path)?;
    //     let mut db: ffi::duckdb_database = ptr::null_mut();
    //     unsafe {
    //         let mut err: *mut c_char = ptr::null_mut();
    //         let r = ffi::duckdb_open_ext(p_path, &mut db, config.duckdb_config(), &mut err);
    //         if r != ffi::DuckDBSuccess {
    //             let err_cstr = CStr::from_ptr(err);
    //             let err_str = err_cstr.to_str()?;
    //             return Ok(Err(DatabaseError::OpenError(err_str.to_owned())));
    //         }
    //         Self::open_from_raw(db)
    //     }
    // }
}

impl Drop for DatabaseHandle {
    fn drop(&mut self) {
        unsafe {
            ffi::duckdb_close(&mut self.0);
        }
    }
}

impl Deref for DatabaseHandle {
    type Target = ffi::duckdb_database;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
