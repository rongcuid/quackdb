mod cutils;

pub mod appender;
pub mod arrow;
pub mod config;
pub mod connection;
pub mod database;
pub mod replacement_scan;
pub mod statement;
pub mod table_function;
pub mod types;

pub fn library_version() -> String {
    quackdb_internal::library_version()
}
