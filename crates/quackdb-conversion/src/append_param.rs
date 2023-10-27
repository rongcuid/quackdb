use std::ffi::CStr;

use paste::paste;
use quackdb_internal::appender::AppenderHandle;

pub unsafe trait AppendParam {
    /// # Safety
    /// Does not need to check whether the type is correct
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String>;
}

unsafe impl<T> AppendParam for Option<T>
where
    T: AppendParam,
{
    unsafe fn append_param_unchecked(self, appender: &AppenderHandle) -> Result<(), String> {
        match self {
            Some(t) => t.append_param_unchecked(appender),
            None => appender.append_null().map_err(|_| appender.error()),
        }
    }
}

macro_rules! impl_append_param_for_value {
    ($ty:ty, $duck_ty:ty) => {
        paste! {
            impl_append_param_for_value! {$ty, $duck_ty, [<append_ $duck_ty>]}
        }
    };
    ($ty:ty, $duck_ty:ty, $method:ident) => {
        unsafe impl AppendParam for $ty {
            unsafe fn append_param_unchecked(
                self,
                appender: &AppenderHandle,
            ) -> Result<(), String> {
                appender.$method(self).map_err(|_| appender.error())
            }
        }
    };
}

impl_append_param_for_value! {bool, bool}
impl_append_param_for_value! {i8, int8}
impl_append_param_for_value! {i16, int16}
impl_append_param_for_value! {i32, int32}
impl_append_param_for_value! {i64, int64}
impl_append_param_for_value! {i128, hugeint}
impl_append_param_for_value! {u8, uint8}
impl_append_param_for_value! {u16, uint16}
impl_append_param_for_value! {u32, uint32}
impl_append_param_for_value! {u64, uint64}
impl_append_param_for_value! {f32, float}
impl_append_param_for_value! {f64, double}
impl_append_param_for_value! {&CStr, varchar}
impl_append_param_for_value! {&str, varchar_length}
// impl_append_param_for_value! {Date, date}
// impl_append_param_for_value! {Time, time}
// impl_append_param_for_value! {PrimitiveDateTime, timestamp}
// impl_append_param_for_value! {Duration, interval}
impl_append_param_for_value! {&[u8], blob}
