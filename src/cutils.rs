use std::{
    ffi::{CString, NulError},
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr,
};

pub fn option_path_to_ptr(path: Option<impl AsRef<Path>>) -> Result<*const i8, NulError> {
    Ok(option_path_to_cstring(path)?.map_or(ptr::null(), |cstr| cstr.into_raw()))
}

pub fn option_path_to_cstring(path: Option<impl AsRef<Path>>) -> Result<Option<CString>, NulError> {
    path.map(|p| path_to_cstring(p.as_ref())).transpose()
}

#[cfg(unix)]
pub fn path_to_cstring(p: &Path) -> Result<CString, NulError> {
    Ok(CString::new(p.as_os_str().as_bytes())?)
}

#[cfg(not(unix))]
pub fn path_to_cstring(p: &Path) -> Result<CString, NulError> {
    let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
    Ok(CString::new(s)?)
}
