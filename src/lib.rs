mod cutils;

mod appender;
mod arrow;
mod config;
mod connection;
mod database;
mod replacement_scan;
mod rows;
mod statement;
mod table_function;
mod types;

pub fn library_version() -> String {
    quackdb_internal::library_version()
}
