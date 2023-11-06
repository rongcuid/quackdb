mod info;
pub use info::*;

use std::{
    ffi::{c_void, CString},
    sync::Arc,
};

use quackdb_internal::ffi;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TableFunctionError<E> {
    #[error(transparent)]
    UserError(#[from] E),
}

pub(crate) struct ExtraInfo<B, I, LI, D, E> {
    pub bind: Box<dyn Fn(&BindInfo, &D) -> Result<B, E> + Send>,
    pub init: Box<dyn Fn(&InitInfo, &B, &D) -> Result<I, E> + Send>,
    pub local_init: Option<Box<dyn Fn(&InitInfo, &B, &D) -> Result<LI, E> + Send + Sync>>,
    pub function: Box<
        dyn Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &D) -> Result<(), E>
            + Send
            + Sync,
    >,
    pub extra: D,
}

// impl<B, I, LI, E> TableFunctionBuilder<B, I, LI, E>
// where
//     B: Send + Sync,
//     I: Send + Sync,
//     LI: Send + Sync,
//     E: Send + Sync,
// {
//     pub fn bind<F>(mut self, f: F) -> Self
//     where
//         F: Fn(&BindInfo, &E) -> Result<B, String> + Send + 'static,
//     {
//         self.bind = Some(Box::new(f));
//         self
//     }
//     pub fn init<F>(mut self, f: F) -> Self
//     where
//         F: Fn(&InitInfo, &B, &E) -> Result<I, String> + Send + 'static,
//     {
//         self.init = Some(Box::new(f));
//         self
//     }
//     pub fn local_init<F>(mut self, f: F) -> Self
//     where
//         F: Fn(&InitInfo, &B, &E) -> Result<LI, String> + Send + Sync + 'static,
//     {
//         self.local_init = Some(Box::new(f));
//         self
//     }
//     pub fn function<F>(mut self, f: F) -> Self
//     where
//         F: Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &E) -> Result<(), String>
//             + Send
//             + Sync
//             + 'static,
//     {
//         self.function = Some(Box::new(f));
//         self
//     }
// }
