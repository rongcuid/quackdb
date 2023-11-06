mod info;
pub use info::*;

use std::{
    ffi::{c_void, CString},
    sync::Arc,
};

use quackdb_internal::ffi;
use thiserror::Error;

pub struct TableFunction {
    pub handle: Arc<ffi::duckdb_table_function>,
}

type BindFn<B, E> = Box<dyn Fn(&BindInfo, &E) -> Result<B, String> + Send>;
type InitFn<B, I, E> = Box<dyn Fn(&InitInfo, &B, &E) -> Result<I, String> + Send>;
type LocalInitFn<B, LI, E> = Box<dyn Fn(&InitInfo, &B, &E) -> Result<LI, String> + Send + Sync>;
type MainFn<B, I, LI, E> = Box<
    dyn Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &E) -> Result<(), String>
        + Send
        + Sync,
>;

pub struct TableFunctionBuilder<B, I, LI, E> {
    projection: bool,
    bind: Option<BindFn<B, E>>,
    init: Option<InitFn<B, I, E>>,
    local_init: Option<LocalInitFn<B, LI, E>>,
    function: Option<MainFn<B, I, LI, E>>,
    extra: E,
}

struct ExtraInfo<B, I, LI, E> {
    bind: BindFn<B, E>,
    init: InitFn<B, I, E>,
    local_init: Option<LocalInitFn<B, LI, E>>,
    function: MainFn<B, I, LI, E>,
    extra: E,
}

#[derive(Error, Debug)]
pub enum TableFunctionBuilderError {
    #[error("missing bind function")]
    MissingBind,
    #[error("missing init function")]
    MissingInit,
    #[error("missing main function")]
    MissingMain,
}

impl TableFunction {
    pub fn builder<B, I, LI, E>(extra_data: E) -> TableFunctionBuilder<B, I, LI, E> {
        TableFunctionBuilder {
            bind: None,
            init: None,
            local_init: None,
            function: None,
            projection: false,
            extra: extra_data,
        }
    }
}

impl<B, I, LI, E> TableFunctionBuilder<B, I, LI, E>
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    E: Send + Sync,
{
    pub fn bind<F>(mut self, f: F) -> Self
    where
        F: Fn(&BindInfo, &E) -> Result<B, String> + Send + 'static,
    {
        self.bind = Some(Box::new(f));
        self
    }
    pub fn init<F>(mut self, f: F) -> Self
    where
        F: Fn(&InitInfo, &B, &E) -> Result<I, String> + Send + 'static,
    {
        self.init = Some(Box::new(f));
        self
    }
    pub fn local_init<F>(mut self, f: F) -> Self
    where
        F: Fn(&InitInfo, &B, &E) -> Result<LI, String> + Send + Sync + 'static,
    {
        self.local_init = Some(Box::new(f));
        self
    }
    pub fn function<F>(mut self, f: F) -> Self
    where
        F: Fn(&FunctionInfo, ffi::duckdb_data_chunk, &B, &I, &LI, &E) -> Result<(), String>
            + Send
            + Sync
            + 'static,
    {
        self.function = Some(Box::new(f));
        self
    }
    pub fn build(self) -> Result<TableFunction, TableFunctionBuilderError> {
        use TableFunctionBuilderError::*;
        unsafe {
            let table_function = ffi::duckdb_create_table_function();
            ffi::duckdb_table_function_supports_projection_pushdown(
                table_function,
                self.projection,
            );
            let extra = Box::new(ExtraInfo {
                bind: self.bind.ok_or(MissingBind)?,
                init: self.init.ok_or(MissingInit)?,
                local_init: self.local_init,
                function: self.function.ok_or(MissingMain)?,
                extra: self.extra,
            });
            // Register callbacks
            ffi::duckdb_table_function_set_bind(table_function, Some(bind::<B, I, LI, E>));
            ffi::duckdb_table_function_set_init(table_function, Some(init::<B, I, LI, E>));
            if extra.local_init.is_some() {
                ffi::duckdb_table_function_set_local_init(
                    table_function,
                    Some(local_init::<B, I, LI, E>),
                );
            }
            ffi::duckdb_table_function_set_function(table_function, Some(function::<B, I, LI, E>));
            // Store extra info
            ffi::duckdb_table_function_set_extra_info(
                table_function,
                Box::into_raw(extra).cast(),
                Some(destroy_extra_info::<B, I, LI, E>),
            );
            Ok(TableFunction {
                handle: Arc::new(table_function),
            })
        }
    }
}

extern "C" fn bind<B, I, LI, E>(info: ffi::duckdb_bind_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    E: Send + Sync,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, E> = ffi::duckdb_bind_get_extra_info(info).cast();
        let f = &(*extra).bind;
        let result = f(&BindInfo::from(info), &(*extra).extra);
        match result {
            Ok(b) => {
                let b = Box::new(b);
                ffi::duckdb_bind_set_bind_data(
                    info,
                    Box::into_raw(b).cast(),
                    Some(destroy_box::<B>),
                );
            }
            Err(e) => {
                let err = CString::new(e.replace('\0', r"\0")).expect("null character");
                ffi::duckdb_bind_set_error(info, err.as_ptr());
            }
        }
    }
}

extern "C" fn init<B, I, LI, E>(info: ffi::duckdb_init_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    E: Send + Sync,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, E> = ffi::duckdb_init_get_extra_info(info).cast();
        let f = &(*extra).init;
        let bind: *const B = ffi::duckdb_init_get_bind_data(info).cast();
        let result = f(&InitInfo::from(info), &*bind, &(*extra).extra);
        match result {
            Ok(i) => {
                let b = Box::new(i);
                ffi::duckdb_init_set_init_data(
                    info,
                    Box::into_raw(b).cast(),
                    Some(destroy_box::<B>),
                );
            }
            Err(e) => {
                let err = CString::new(e.replace('\0', r"\0")).expect("null character");
                ffi::duckdb_init_set_error(info, err.as_ptr());
            }
        }
    }
}

extern "C" fn local_init<B, I, LI, E>(info: ffi::duckdb_init_info)
where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    E: Send + Sync,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, E> = ffi::duckdb_init_get_extra_info(info).cast();
        if let Some(f) = &(*extra).local_init {
            let bind: *const B = ffi::duckdb_init_get_bind_data(info).cast();
            let result = f(&InitInfo::from(info), &*bind, &(*extra).extra);
            match result {
                Ok(i) => {
                    let b = Box::new(i);
                    ffi::duckdb_init_set_init_data(
                        info,
                        Box::into_raw(b).cast(),
                        Some(destroy_box::<B>),
                    );
                }
                Err(e) => {
                    let err = CString::new(e.replace('\0', r"\0")).expect("null character");
                    ffi::duckdb_init_set_error(info, err.as_ptr());
                }
            }
        }
    }
}

extern "C" fn function<B, I, LI, E>(
    info: ffi::duckdb_function_info,
    data_chunk: ffi::duckdb_data_chunk,
) where
    B: Send + Sync,
    I: Send + Sync,
    LI: Send + Sync,
    E: Send + Sync,
{
    unsafe {
        let extra: *const ExtraInfo<B, I, LI, E> = ffi::duckdb_function_get_extra_info(info).cast();
        let f = &(*extra).function;
        let bind: *const B = ffi::duckdb_function_get_bind_data(info).cast();
        let init: *const I = ffi::duckdb_function_get_init_data(info).cast();
        let local_init: *const LI = ffi::duckdb_function_get_local_init_data(info).cast();
        let result = f(
            &FunctionInfo::from(info),
            data_chunk,
            &*bind,
            &*init,
            &*local_init,
            &(*extra).extra,
        );
        if let Err(e) = result {
            let err = CString::new(e.replace('\0', r"\0")).expect("null character");
            ffi::duckdb_function_set_error(info, err.as_ptr());
        }
    }
}

extern "C" fn destroy_extra_info<B, I, LI, E>(ptr: *mut c_void) {
    destroy_box::<ExtraInfo<B, I, LI, E>>(ptr)
}

extern "C" fn destroy_box<T>(ptr: *mut c_void) {
    unsafe { drop::<Box<T>>(Box::from_raw(ptr.cast())) }
}
