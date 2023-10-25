use quackdb_internal::table_function::{BindInfoHandle, FunctionInfoHandle, InitInfoHandle};

pub struct BindInfo {
    pub handle: BindInfoHandle,
}

impl From<BindInfoHandle> for BindInfo {
    fn from(handle: BindInfoHandle) -> Self {
        Self { handle }
    }
}

pub struct InitInfo {
    pub handle: InitInfoHandle,
}

impl From<InitInfoHandle> for InitInfo {
    fn from(handle: InitInfoHandle) -> Self {
        Self { handle }
    }
}

pub struct FunctionInfo {
    pub handle: FunctionInfoHandle,
}

impl From<FunctionInfoHandle> for FunctionInfo {
    fn from(handle: FunctionInfoHandle) -> Self {
        Self { handle }
    }
}
