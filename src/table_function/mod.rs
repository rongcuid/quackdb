mod info;
pub use info::*;

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
    pub local_init: Box<dyn Fn(&InitInfo, &B, &D) -> Result<LI, E> + Send + Sync>,
    pub function: Box<
        dyn Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &D) -> Result<(), E>
            + Send
            + Sync,
    >,
    pub extra: D,
}
