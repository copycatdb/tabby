# tabby Design Document

> A pure Rust implementation of the TDS 7.4+ protocol for Microsoft SQL Server.

## Table of Contents

- [Overview](#overview)
- [What is TDS?](#what-is-tds)
- [Architecture](#architecture)
- [Connection Flow](#connection-flow)
- [Azure SQL DB Redirect](#azure-sql-db-redirect)
- [The RowWriter Trait](#the-rowwriter-trait)
- [Type System](#type-system)
- [Streaming Decode](#streaming-decode)
- [Synchronous Client](#synchronous-client)
- [Error Handling](#error-handling)
- [Feature Flags](#feature-flags)
- [Comparison with tiberius](#comparison-with-tiberius)

---

## Overview

tabby is the wire protocol layer for the [CopyCat](https://github.com/copycatdb) ecosystem. It implements the Tabular Data Stream (TDS) protocol version 7.4+, which is the binary protocol used by Microsoft SQL Server for all client-server communication.

tabby is **not** a high-level database client. It provides:

- TDS packet framing and token parsing
- Authentication (SQL, AAD token, Windows/NTLM, Kerberos)
- TLS negotiation (via rustls, native-tls, or vendored OpenSSL)
- Query execution (raw batches and parameterized via `sp_executesql`)
- Bulk insert support
- **The `RowWriter` trait** — a zero-copy interface that lets consumers (Arrow, Python, ODBC, Node.js) receive decoded column values directly without intermediate allocation

Higher-level abstractions (connection pooling, result iteration, ergonomic APIs) live in [claw](https://github.com/copycatdb/claw).

## What is TDS?

**Tabular Data Stream (TDS)** is a binary application-layer protocol created by Sybase in the 1980s, later adopted and extended by Microsoft for SQL Server. The protocol is documented in the [MS-TDS specification](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/).

### Packet Structure

Every TDS message is split into packets with an 8-byte header:

```
┌──────────┬──────────┬──────────────┬──────────┬──────────┬──────────┐
│ Type (1) │ Status(1)│ Length (2)   │ SPID (2) │ PktID(1) │ Window(1)│
├──────────┴──────────┴──────────────┴──────────┴──────────┴──────────┤
│ Payload (up to negotiated packet size - 8)                         │
└────────────────────────────────────────────────────────────────────┘
```

- **Type**: Pre-Login (0x12), Login7 (0x10), SQL Batch (0x01), RPC (0x03), etc.
- **Status**: End-of-message (0x01), reset connection (0x08), etc.
- **Length**: Total packet length including header (max 32,767 bytes default, negotiated up to 32K)

### Token Stream

Within a response message, the server sends a stream of **tokens** — self-describing typed chunks:

| Token | Hex | Purpose |
|-------|-----|---------|
| `COLMETADATA` | 0x81 | Column names, types, nullability for a result set |
| `ROW` | 0xD1 | One row of column values |
| `NBCROW` | 0xD2 | Row with null-bitmap compression |
| `DONE` | 0xFD | Statement complete, row count |
| `DONEPROC` | 0xFE | Stored procedure complete |
| `DONEINPROC` | 0xFF | Intermediate done within a procedure |
| `ERROR` | 0xAA | SQL Server error message |
| `INFO` | 0xAB | Informational message (PRINT, RAISERROR ≤ sev 10) |
| `ENVCHANGE` | 0xE3 | Session state change (database, packet size, transaction) |
| `RETURNSTATUS` | 0x79 | Stored procedure return value |
| `RETURNVALUE` | 0xAC | Output parameter value |
| `ORDER` | 0xA9 | Column ordering information |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Consumer Layer                               │
│  pounce (Arrow) · whiskers (Python) · furball (ODBC) · kibble (JS) │
│                                                                     │
│  Implements RowWriter to receive decoded values directly            │
├─────────────────────────────────────────────────────────────────────┤
│                        tabby Public API                             │
│  Config · Client · SyncClient · RowWriter · Column · ColumnType    │
│  AuthMethod · ExecuteResult · batch_into · batch_start/fetch_row   │
├──────────────────┬──────────────────┬───────────────────────────────┤
│   Connection     │   Query Engine   │   Result Processing           │
│                  │                  │                               │
│   TLS handshake  │   SQL Batch      │   Token stream parsing        │
│   Pre-login      │   RPC/sp_exec    │   Row decode (SqlValue path)  │
│   Login7         │   Bulk Insert    │   Direct decode (RowWriter)   │
│   Azure redirect │   Parameters     │   NBC row decompression       │
├──────────────────┴──────────┬───────┴───────────────────────────────┤
│              TDS Codec (protocol/)                                   │
│   Packet framing · Token parsing · Type encode/decode               │
│   FrameCodec (async) · SyncConnection (blocking)                    │
├─────────────────────────────────────────────────────────────────────┤
│              Transport                                               │
│   TCP + optional TLS (rustls / native-tls / vendored-openssl)       │
│   Async: tokio TcpStream + futures-io compat                        │
│   Sync: std::net::TcpStream                                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Source Layout

| Path | Purpose |
|------|---------|
| `src/lib.rs` | Public API surface, re-exports |
| `src/connection/` | `Config`, `Client`, `Connection`, `AuthMethod`, TLS |
| `src/connection/config.rs` | Connection configuration builder |
| `src/connection/auth.rs` | Authentication methods |
| `src/connection/connection.rs` | `Connection` struct, TLS negotiation, login |
| `src/connection/tls.rs` | TLS wrapper (MaybeTlsStream) |
| `src/connection/tls_stream/` | TLS backend implementations |
| `src/row_writer.rs` | `RowWriter` trait + `SqlValueWriter` |
| `src/row.rs` | `Row`, `Column`, `ColumnType` |
| `src/result.rs` | `ExecuteResult` |
| `src/sync_client.rs` | `SyncClient` (blocking I/O) |
| `src/protocol/` | TDS wire protocol implementation |
| `src/protocol/wire/` | Packet types, token encode/decode |
| `src/protocol/wire/decode_direct.rs` | Zero-copy decode into RowWriter |
| `src/protocol/pipeline.rs` | `TokenStream`, `ResultStream` |
| `src/protocol/session.rs` | `Context` (packet IDs, transaction state) |

## Connection Flow

```
Client                                              SQL Server
  │                                                       │
  ├──── TCP Connect ─────────────────────────────────────►│
  │                                                       │
  ├──── PRELOGIN (0x12) ────────────────────────────────►│
  │     version, encryption, instance name                │
  │◄──── PRELOGIN Response ──────────────────────────────┤
  │     server version, encryption negotiation            │
  │                                                       │
  ├──── TLS Handshake (if encryption ≥ Off) ────────────►│
  │◄──── TLS Handshake ─────────────────────────────────┤
  │     (wrapped inside TDS packets during negotiation)   │
  │                                                       │
  ├──── LOGIN7 (0x10) ──────────────────────────────────►│
  │     TDS version, packet size, client info,            │
  │     credentials / AAD token / SSPI blob               │
  │                                                       │
  │◄──── Login Response ─────────────────────────────────┤
  │     ENVCHANGE (database, packet size, transaction)    │
  │     INFO messages                                     │
  │     LOGINACK or ERROR                                 │
  │     DONE                                              │
  │                                                       │
  ├──── SQL Batch / RPC ────────────────────────────────►│
  │◄──── COLMETADATA + ROW* + DONE ─────────────────────┤
```

### Pre-Login

The pre-login exchange negotiates:
- **TDS version** (tabby requests 7.4 / SQL Server 2012+)
- **Encryption** level (Off, On, NotSupported, Required)
- **Instance name** (for SQL Browser named instances)
- **Federated authentication** required flag (for AAD)

### TLS Negotiation

TDS has a unique TLS setup: the TLS handshake packets are wrapped inside TDS Pre-Login packets. After the handshake completes, the TLS stream either wraps all subsequent traffic (encryption = On/Required) or only the Login7 packet (encryption = Off).

tabby supports three TLS backends:
- **rustls** (default) — pure Rust, no system dependencies
- **native-tls** — OS-provided TLS (SChannel on Windows, SecureTransport on macOS, OpenSSL on Linux)
- **vendored-openssl** — statically linked OpenSSL

### Login7

The Login7 message includes:
- TDS version, packet size, client PID
- Application name, server name, database
- Authentication credentials (SQL auth password is XOR-obfuscated in the packet)
- Feature extension data (for federated auth, UTF-8 support, etc.)

## Azure SQL DB Redirect

Azure SQL Database uses a gateway architecture. When a client connects:

1. The TLS + Login7 handshake completes with the **gateway**
2. The gateway responds with a **routing** ENVCHANGE token containing the actual worker node's host and port
3. The client must disconnect and reconnect to the worker node

tabby handles this via `Client::connect_with_redirect`:

```rust
let client = Client::connect_with_redirect(config, |host, port| async move {
    let tcp = TcpStream::connect(format!("{}:{}", host, port)).await?;
    Ok(tcp.compat_write())
}).await?;
```

The `Error::Routing { host, port }` variant signals a redirect, and the method automatically retries the connection to the new endpoint.

## The RowWriter Trait

**This is tabby's key innovation** and the reason the CopyCat ecosystem can offer zero-copy bindings for Arrow, Python, ODBC, Node.js, and more from a single Rust core.

### The Problem

The traditional decode path is:

```
TDS wire bytes → SqlValue enum → pattern match → consumer's native type
```

Every column value passes through an intermediate `SqlValue` allocation. For a million-row result set with 20 columns, that's 20 million allocations — just to throw them away immediately.

### The Solution

`RowWriter` is a trait with one method per SQL type:

```rust
pub trait RowWriter {
    fn write_null(&mut self, col: usize);
    fn write_bool(&mut self, col: usize, val: bool);
    fn write_i32(&mut self, col: usize, val: i32);
    fn write_i64(&mut self, col: usize, val: i64);
    fn write_f64(&mut self, col: usize, val: f64);
    fn write_str(&mut self, col: usize, val: &str);
    fn write_bytes(&mut self, col: usize, val: &[u8]);
    fn write_date(&mut self, col: usize, days: i32);
    fn write_time(&mut self, col: usize, nanos: i64);
    fn write_datetime(&mut self, col: usize, micros: i64);
    fn write_datetimeoffset(&mut self, col: usize, micros: i64, offset_minutes: i16);
    fn write_decimal(&mut self, col: usize, value: i128, precision: u8, scale: u8);
    fn write_guid(&mut self, col: usize, bytes: &[u8; 16]);
    fn write_utf16(&mut self, col: usize, val: &[u16]);
    // lifecycle callbacks
    fn on_metadata(&mut self, columns: &[Column]);
    fn on_row_done(&mut self);
    fn on_info(&mut self, number: u32, message: &str);
    fn on_done(&mut self, rows: u64);
    fn on_result_set_end(&mut self);
}
```

The TDS decoder calls these methods **directly during wire decoding**. No intermediate representation. The consumer writes straight into its own storage:

```
TDS wire bytes ──decode──► RowWriter::write_i32() ──► Arrow Int32Builder::append()
                           RowWriter::write_str()  ──► Arrow StringArray builder
                           RowWriter::write_null()  ──► Arrow null bitmap
```

### How Each Project Uses RowWriter

| Project | RowWriter Implementation | Target Storage |
|---------|-------------------------|----------------|
| **pounce** (ADBC/Arrow) | Arrow array builders | Arrow RecordBatch |
| **whiskers** (Python DB-API) | Python list builders | Python tuples |
| **hiss** (async Python) | Similar to whiskers | Python tuples |
| **furball** (ODBC) | Column-bound buffers | ODBC SQLGetData buffers |
| **kibble** (Node.js) | N-API value builders | JavaScript arrays |
| **claw** (Rust API) | `SqlValueWriter` | `Vec<SqlValue>` (convenience) |

### Two Decode Paths

tabby supports both paths simultaneously:

1. **SqlValue path** (traditional): `Client::execute()` → `ResultStream` → `Row` → `SqlValue`
   - Convenient for ad-hoc queries, small result sets
   - Used by `claw` for its ergonomic API

2. **RowWriter path** (zero-copy): `Client::batch_into()` / `batch_start()` + `batch_fetch_row()`
   - Zero intermediate allocation
   - Used by all high-performance bindings (pounce, furball, whiskers, kibble)

## Type System

### ColumnType

`ColumnType` is a simplified enum of SQL Server types. It's derived from the full `DataType` information in the COLMETADATA token:

| ColumnType | SQL Server Type(s) | RowWriter Method |
|------------|-------------------|-----------------|
| `Bit`, `Bitn` | `bit` | `write_bool` |
| `Int1` | `tinyint` | `write_u8` |
| `Int2` | `smallint` | `write_i16` |
| `Int4` | `int` | `write_i32` |
| `Int8` | `bigint` | `write_i64` |
| `Float4` | `real` | `write_f32` |
| `Float8` | `float` | `write_f64` |
| `NVarchar`, `NChar` | `nvarchar`, `nchar` | `write_str` (UTF-16 → UTF-8) |
| `BigVarChar`, `BigChar` | `varchar`, `char` | `write_str` |
| `BigVarBin`, `BigBinary` | `varbinary`, `binary` | `write_bytes` |
| `Daten` | `date` | `write_date` (days since epoch) |
| `Timen` | `time` | `write_time` (nanos since midnight) |
| `Datetime`, `Datetime2`, `Datetime4`, `Datetimen` | `datetime`, `datetime2`, `smalldatetime` | `write_datetime` (micros since epoch) |
| `DatetimeOffsetn` | `datetimeoffset` | `write_datetimeoffset` |
| `Decimaln`, `Numericn` | `decimal`, `numeric` | `write_decimal` (i128 + precision + scale) |
| `Guid` | `uniqueidentifier` | `write_guid` |
| `Money`, `Money4` | `money`, `smallmoney` | `write_f64` (converted) |
| `Xml` | `xml` | `write_str` |
| `Text`, `NText`, `Image` | deprecated LOB types | `write_str` / `write_bytes` |

### Temporal Representation

All temporal values are converted to simple numeric representations during decode:
- **Date**: days since Unix epoch (1970-01-01) as `i32`
- **Time**: nanoseconds since midnight as `i64`
- **Datetime**: microseconds since Unix epoch as `i64`
- **DatetimeOffset**: microseconds since Unix epoch (UTC) + offset in minutes

This avoids dependency on any specific date/time library in the core protocol layer.

### Decimal/Numeric

Decimals are represented as `(i128, precision, scale)` — the raw integer value with metadata. The consumer can convert to its preferred decimal type (Arrow Decimal128, Python Decimal, rust_decimal, bigdecimal, etc.).

## Streaming Decode

tabby offers three levels of streaming control:

### 1. `batch_into` — Fire and Forget

Send a query, decode all results into a single RowWriter:

```rust
client.batch_into("SELECT * FROM big_table", &mut my_writer).await?;
```

The writer receives `on_metadata` → `write_*` per column per row → `on_row_done` → ... → `on_done`.

### 2. `batch_start` + `batch_fetch_row` — Row-Level Control

For consumers that need row-by-row control (ODBC `SQLFetch`, Python `fetchone`):

```rust
let columns = client.batch_start("SELECT * FROM t").await?;
// ... use columns to set up writer ...
loop {
    match client.batch_fetch_row(&mut writer, &mut str_buf, &mut bytes_buf).await? {
        BatchFetchResult::Row => { /* row written to writer */ }
        BatchFetchResult::Done(n) => break,
        BatchFetchResult::MoreResults => {
            let next_cols = client.batch_fetch_metadata().await?;
            // ... handle next result set ...
        }
    }
}
```

### 3. `query_direct` / `batch_direct` — Callback-Based

For consumers that want a fresh writer per result set:

```rust
let writer = client.batch_direct("SELECT 1; SELECT 2;",
    |columns| MyWriter::new(columns),  // called per result set
    |writer| true,                      // called per row, return false to stop
).await?;
```

## Synchronous Client

`SyncClient` mirrors the async `Client` API but uses blocking I/O over `std::net::TcpStream`. It's used by:

- **furball** (ODBC) — ODBC is inherently synchronous
- **whiskers** (Python DB-API) — Python's DB-API 2.0 is synchronous

The sync path has its own TLS negotiation, packet framing, and decode logic (`decode_row_into_sync`, `decode_nbc_row_into_sync`) that avoids the tokio runtime entirely.

```rust
use std::net::TcpStream;
use tabby::{Config, AuthMethod, SyncClient};

let mut config = Config::new();
config.host("localhost");
config.authentication(AuthMethod::sql_server("sa", "password"));
config.trust_cert();

let tcp = TcpStream::connect(config.get_addr())?;
let mut client = SyncClient::connect(config, tcp)?;

let columns = client.batch_start("SELECT 1 AS x")?;
```

## Error Handling

tabby uses a unified `Error` enum:

| Variant | Source |
|---------|--------|
| `Io` | Network / I/O failures |
| `Protocol` | Invalid TDS packets or unexpected tokens |
| `Server` | SQL Server error (contains error number, severity, message, state) |
| `Tls` | TLS handshake failure |
| `Conversion` | Type conversion failure |
| `Encoding` | Unsupported encoding |
| `Utf8` / `Utf16` | String encoding errors |
| `Routing` | Azure SQL redirect (handled internally by `connect_with_redirect`) |

Server errors include the SQL Server error number, accessible via `Error::code()`. The convenience method `Error::is_deadlock()` checks for error 1205.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| `rustls` | ✅ | TLS via pure-Rust rustls |
| `native-tls` | | TLS via OS-native library |
| `vendored-openssl` | | TLS via statically-linked OpenSSL |
| `sync` | | Enable `SyncClient` and blocking I/O path |
| `chrono` | | `chrono` type conversions for temporal values |
| `time` | | `time` crate conversions for temporal values |
| `rust_decimal` | | `rust_decimal` conversions for decimal values |
| `bigdecimal` | | `bigdecimal` conversions for decimal values |
| `integrated-auth-gssapi` | | Kerberos/GSSAPI auth on Unix |
| `winauth` | | NTLM/SSPI auth on Windows |

## Comparison with tiberius

[tiberius](https://github.com/prisma/tiberius) is the other Rust TDS library, created by the Prisma team. tabby was originally inspired by tiberius but diverges significantly in design goals.

| | tabby | tiberius |
|---|---|---|
| **Primary use** | Protocol layer for multi-language ecosystem | Standalone Rust SQL Server client |
| **RowWriter (zero-copy)** | ✅ Core innovation | ❌ All values go through `ColumnData` enum |
| **Sync client** | ✅ Native blocking I/O | ❌ Async only |
| **Azure redirect** | ✅ Built-in | ✅ |
| **Bulk insert** | ✅ | ✅ |
| **Direct decode** | ✅ Decode straight into Arrow/Python/ODBC buffers | ❌ |
| **TDS version** | 7.4+ only | 7.2+ |
| **Maintenance** | Active (CopyCat ecosystem) | Maintained by Prisma |

The fundamental difference is **RowWriter**. tiberius decodes every value into a `ColumnData` enum, which the consumer then pattern-matches. tabby's direct decode path eliminates this entirely — the TDS decoder calls typed methods on a trait object, and the consumer writes directly to its final destination. For bulk data transfer, this means zero intermediate allocations.

This design is what makes it possible to have a single Rust TDS core powering Arrow (pounce), Python (whiskers/hiss), ODBC (furball), Node.js (kibble), Go (catnip), JDBC (hairball), and .NET (nuzzle) bindings — each implementing `RowWriter` for their native data structures.
