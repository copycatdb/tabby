# tabby 🐱

[![Crates.io](https://img.shields.io/crates/v/tabby.svg)](https://crates.io/crates/tabby)
[![docs.rs](https://docs.rs/tabby/badge.svg)](https://docs.rs/tabby)
[![MIT License](https://img.shields.io/crates/l/tabby.svg)](LICENSE)

Pure Rust implementation of the TDS (Tabular Data Stream) 7.4+ protocol for Microsoft SQL Server.

tabby is the **wire protocol layer** for the [CopyCat](https://github.com/copycatdb) ecosystem. Its key innovation is the **`RowWriter` trait** — a zero-copy interface that lets consumers (Arrow, Python, ODBC, Node.js) receive decoded column values directly during TDS wire decoding, without intermediate allocation.

## Quick Start

```toml
[dependencies]
tabby = "0.1"
```

```rust
use tabby::{AuthMethod, Client, Config};
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "password"));
    config.trust_cert();

    let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;

    // Parameterized query
    let row = client
        .execute("SELECT @P1 AS id, @P2 AS name", &[&42i32, &"Alice"])
        .await?
        .into_row()
        .await?
        .unwrap();

    let id: i32 = row.get("id").unwrap();
    let name: &str = row.get("name").unwrap();
    println!("id={id}, name={name}");

    Ok(())
}
```

## Supported SQL Server Versions

- SQL Server 2012 and later (TDS 7.4+)
- Azure SQL Database (with automatic redirect support)
- Azure SQL Managed Instance

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `rustls` | ✅ | TLS via pure-Rust rustls |
| `native-tls` | | TLS via OS-native library (SChannel/SecureTransport/OpenSSL) |
| `vendored-openssl` | | TLS via statically-linked OpenSSL |
| `sync` | | Enable `SyncClient` for blocking I/O |
| `chrono` | | Date/time conversions with the `chrono` crate |
| `time` | | Date/time conversions with the `time` crate |
| `rust_decimal` | | Decimal conversions with `rust_decimal` |
| `bigdecimal` | | Decimal conversions with `bigdecimal` |
| `integrated-auth-gssapi` | | Kerberos/GSSAPI integrated auth (Unix) |
| `winauth` | | NTLM/SSPI integrated auth (Windows) |

## The RowWriter Trait

tabby's standout feature: decode TDS rows directly into consumer-owned buffers with **zero intermediate allocation**.

```rust
use tabby::{RowWriter, Column};

struct MyWriter { /* your storage */ }

impl RowWriter for MyWriter {
    fn write_null(&mut self, col: usize) { /* ... */ }
    fn write_i32(&mut self, col: usize, val: i32) { /* append to your buffer */ }
    fn write_str(&mut self, col: usize, val: &str) { /* append to your buffer */ }
    // ... implement all methods for your target format
}

// Then: client.batch_into("SELECT * FROM t", &mut my_writer).await?;
```

This is what enables a single Rust TDS core to power Arrow, Python, ODBC, Node.js, and more — each implementing `RowWriter` for their native data structures.

See [docs/DESIGN.md](docs/DESIGN.md) for the full architecture.

## CopyCat Ecosystem

tabby is consumed by every driver in the CopyCat family:

| Project | What it builds on tabby |
|---------|------------------------|
| [**claw**](https://github.com/copycatdb/claw) | Idiomatic Rust client API |
| [**pounce**](https://github.com/copycatdb/pounce) | Arrow-native ADBC driver (Python) |
| [**whiskers**](https://github.com/copycatdb/whiskers) | DB-API 2.0 Python driver |
| [**hiss**](https://github.com/copycatdb/hiss) | Async Python driver |
| [**furball**](https://github.com/copycatdb/furball) | ODBC driver |
| [**kibble**](https://github.com/copycatdb/kibble) | Node.js driver |
| [**catnip**](https://github.com/copycatdb/catnip) | Go driver |
| [**hairball**](https://github.com/copycatdb/hairball) | JDBC driver |
| [**nuzzle**](https://github.com/copycatdb/nuzzle) | .NET ADO.NET driver |
| [**meow**](https://github.com/copycatdb/meow) | TUI client |
| [**prowl**](https://github.com/copycatdb/prowl) | MCP server |

## Design Document

For deep-dive architecture documentation — TDS protocol primer, connection flow, RowWriter design, type system, streaming decode, and comparison with tiberius — see **[docs/DESIGN.md](docs/DESIGN.md)**.

## Attribution

tabby's TDS protocol implementation is inspired by and derived from [tiberius](https://github.com/prisma/tiberius) by the Prisma team.

Additional inspiration from [FreeTDS](https://www.freetds.org/), [Microsoft TDS spec](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/), and [Microsoft.Data.SqlClient](https://github.com/dotnet/SqlClient).

## License

MIT
