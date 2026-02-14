//! TDS protocol implementation for Microsoft SQL Server (TDS 7.4).
#![allow(dead_code)]

#[macro_use]
mod macros;

mod connection;
mod from_server;
mod into_sql;
mod query;

pub mod error;
mod protocol;
mod result;
mod row;

mod discovery;

pub use connection::{AuthMethod, Client, Config};
pub(crate) use error::Error;
pub use from_server::{FromServer, FromServerOwned};
pub use into_sql::{IntoSql, IntoSqlOwned};
pub use protocol::{
    EncryptionLevel,
    pipeline::ResultStream,
    temporal,
    wire::{BulkImport, ColumnAttribute, DataType, FixedLenType, RowMessage, SqlValue, VarLenType},
    xml,
};
pub use result::*;
pub use row::{Column, ColumnType, Row};

use protocol::reader::*;
use protocol::wire::*;

/// An alias for a result that holds this module's error type as the error.
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn get_driver_version() -> u64 {
    env!("CARGO_PKG_VERSION")
        .splitn(6, '.')
        .enumerate()
        .fold(0u64, |acc, part| match part.1.parse::<u64>() {
            Ok(num) => acc | num << (part.0 * 8),
            _ => acc | 0 << (part.0 * 8),
        })
}
