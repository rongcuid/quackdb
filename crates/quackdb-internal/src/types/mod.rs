mod type_id;
use rust_decimal::Decimal;
pub use type_id::*;
mod logical;
pub use logical::*;
mod param;
pub use param::*;
mod to_duckdb;
pub use to_duckdb::*;

use crate::ffi;

#[derive(Debug)]
pub struct DuckDbDecimal {
    pub width: u8,
    pub decimal: Decimal,
}

impl From<ffi::duckdb_decimal> for DuckDbDecimal {
    fn from(value: ffi::duckdb_decimal) -> Self {
        let mantissa = i128::from_duckdb(value.value);
        let decimal = Decimal::from_i128_with_scale(mantissa, value.scale as u32);
        DuckDbDecimal {
            width: value.width,
            decimal,
        }
    }
}

impl From<DuckDbDecimal> for ffi::duckdb_decimal {
    fn from(value: DuckDbDecimal) -> Self {
        ffi::duckdb_decimal {
            width: value.width,
            scale: value.decimal.scale() as u8,
            value: value.decimal.mantissa().into_duckdb(),
        }
    }
}
