use std::{
    ffi::{CString, NulError},
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr,
};

pub fn option_path_to_ptr(path: Option<&Path>) -> Result<*const i8, NulError> {
    Ok(option_path_to_cstring(path)?.map_or(ptr::null(), |cstr| cstr.into_raw()))
}

pub fn option_path_to_cstring(path: Option<&Path>) -> Result<Option<CString>, NulError> {
    path.map(path_to_cstring).transpose()
}

#[cfg(unix)]
pub fn path_to_cstring(p: &Path) -> Result<CString, NulError> {
    CString::new(p.as_os_str().as_bytes())
}

#[cfg(not(unix))]
pub fn path_to_cstring(p: &Path) -> Result<CString, NulError> {
    let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
    Ok(CString::new(s)?)
}
