//! # tabby
//!
//! A pure Rust implementation of the TDS (Tabular Data Stream) 7.4+ protocol
//! for Microsoft SQL Server.
//!
//! `tabby` provides an async client for connecting to SQL Server, executing
//! queries with parameterized inputs, reading result rows, and performing bulk
//! inserts — all without any C dependencies.
//!
//! # Quick Start
//!
//! ```no_run
//! use tabby::{AuthMethod, Client, Config};
//! use tokio_util::compat::TokioAsyncWriteCompatExt;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut config = Config::new();
//!     config.host("localhost");
//!     config.port(1433);
//!     config.authentication(AuthMethod::sql_server("sa", "your_password"));
//!     config.trust_cert();
//!
//!     let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
//!     tcp.set_nodelay(true)?;
//!
//!     let mut client = Client::connect(config, tcp.compat_write()).await?;
//!
//!     // Execute a query and read rows
//!     let stream = client.execute("SELECT @P1 AS greeting", &[&"hello"]).await?;
//!     let row = stream.into_row().await?.unwrap();
//!     let greeting: &str = row.get("greeting").unwrap();
//!     println!("{greeting}");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Key Types
//!
//! - [`Client`] — the main entry point for executing queries
//! - [`Config`] — connection configuration builder
//! - [`AuthMethod`] — authentication methods (SQL Server, Windows, AAD)
//! - [`ResultStream`] — streaming query results
//! - [`Row`] — a single result row with typed column access
//! - [`Query`] — dynamic parameterized queries
//! - [`IntoSql`] / [`FromServer`] — parameter and result type conversion traits
//! - [`SqlValue`] — the underlying TDS value container
//!
//! # Feature Flags
//!
//! | Feature | Description |
//! |---------|-------------|
//! | `rustls` (default) | TLS via rustls |
//! | `native-tls` | TLS via native-tls (OpenSSL/SChannel/SecureTransport) |
//! | `vendored-openssl` | TLS via vendored OpenSSL |
//! | `integrated-auth-gssapi` | Kerberos/GSSAPI integrated auth on Unix |
//! | `winauth` | NTLM/SSPI integrated auth on Windows |
//! | `chrono` | Date/time conversions with the `chrono` crate |
//! | `time` | Date/time conversions with the `time` crate |
//! | `rust_decimal` | Decimal conversions with `rust_decimal` |
//! | `bigdecimal` | Decimal conversions with `bigdecimal` |
#![allow(dead_code)]

#[macro_use]
mod macros;

mod connection;
mod from_server;
mod into_sql;
mod query;
pub use query::Query;

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
    numeric::Numeric,
    pipeline::{ResultItem, ResultStream},
    temporal,
    wire::{
        BulkImport, ColumnAttribute, DataType, FixedLenType, IntoRowMessage, RowMessage, SqlValue,
        VarLenType,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn driver_version_nonzero() {
        assert!(get_driver_version() > 0);
    }

    #[test]
    fn column_type_from_fixed_len() {
        use protocol::wire::{DataType, FixedLenType};
        assert_eq!(
            ColumnType::Int4,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Int4))
        );
        assert_eq!(
            ColumnType::Bit,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Bit))
        );
        assert_eq!(
            ColumnType::Int1,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Int1))
        );
        assert_eq!(
            ColumnType::Int2,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Int2))
        );
        assert_eq!(
            ColumnType::Int8,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Int8))
        );
        assert_eq!(
            ColumnType::Float4,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Float4))
        );
        assert_eq!(
            ColumnType::Float8,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Float8))
        );
        assert_eq!(
            ColumnType::Money,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Money))
        );
        assert_eq!(
            ColumnType::Money4,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Money4))
        );
        assert_eq!(
            ColumnType::Datetime,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Datetime))
        );
        assert_eq!(
            ColumnType::Datetime4,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Datetime4))
        );
        assert_eq!(
            ColumnType::Null,
            ColumnType::from(&DataType::FixedLen(FixedLenType::Null))
        );
    }

    #[test]
    fn column_new_and_accessors() {
        let col = Column::new("test".into(), ColumnType::Int4);
        assert_eq!("test", col.name());
        assert_eq!(ColumnType::Int4, col.column_type());
        assert!(col.type_info().is_none());
        assert!(col.nullable().is_none());
    }
}
