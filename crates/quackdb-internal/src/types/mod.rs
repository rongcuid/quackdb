mod type_id;
pub use type_id::*;
mod logical;
pub use logical::*;

use crate::ffi;

pub fn i128_to_hugeint(value: i128) -> ffi::duckdb_hugeint {
    ffi::duckdb_hugeint {
        upper: (value >> 64) as i64,
        lower: value as u64,
    }
}
