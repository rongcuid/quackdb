use std::{ffi::c_char, ops::Deref, ptr::NonNull, sync::Arc};

use crate::{ffi, types::LogicalTypeHandle};

mod data;
pub use data::*;
mod validity;
pub use validity::*;
mod vector;
pub use vector::*;
