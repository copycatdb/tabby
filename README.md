# tabby 🐱

Pure Rust implementation of the TDS 7.4+ protocol for Microsoft SQL Server.

The heart of [CopyCat](https://github.com/copycatdb). Every other project in the org is just a cat sitting on top of tabby.

## What is this?

tabby speaks the Tabular Data Stream (TDS) protocol — the binary wire protocol that SQL Server uses. It handles:

- **Authentication** — SQL auth, Windows auth (NTLM/NTLMv2)
- **Encryption** — TLS negotiation and encrypted connections
- **Query execution** — batch mode, RPC calls (`sp_executesql`), prepared statements
- **Type system** — all SQL Server types decoded into Rust types
- **Token streaming** — full TDS token stream processing (DONE, ROW, COLMETADATA, INFO, ERROR, etc.)
- **Bulk operations** — BCP/bulk insert support
- **TDS 7.4+** — modern protocol only, no legacy baggage

## Why?

Because talking to SQL Server shouldn't require:
- A 50MB ODBC driver download
- A PhD in driver manager configuration
- Sacrificing a goat to the FreeTDS gods
- Linking against C libraries from 2003

tabby is pure Rust. It compiles. It works. It doesn't ask you to install anything.

## Usage

tabby is primarily consumed by other CopyCat projects:

| Project | What it builds on tabby |
|---------|------------------------|
| [**pounce**](https://github.com/copycatdb/pounce) | Arrow-native ADBC driver (Python) |
| [**hiss**](https://github.com/copycatdb/hiss) | Async Python driver |
| [**whiskers**](https://github.com/copycatdb/whiskers) | DB-API 2.0 Python driver |
| [**claw**](https://github.com/copycatdb/claw) | Idiomatic Rust API |
| [**furball**](https://github.com/copycatdb/furball) | ODBC driver |
| [**kibble**](https://github.com/copycatdb/kibble) | Node.js driver |
| [**catnip**](https://github.com/copycatdb/catnip) | Go driver |
| [**hairball**](https://github.com/copycatdb/hairball) | JDBC driver |
| [**nuzzle**](https://github.com/copycatdb/nuzzle) | .NET ADO.NET driver |

```toml
# Cargo.toml
[dependencies]
tabby = { git = "https://github.com/copycatdb/tabby" }
```

```rust
use tabby::{Client, Config, AuthMethod};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.authentication(AuthMethod::sql_server("sa", "password"));
    config.trust_cert();

    let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;

    let stream = client.query("SELECT @@VERSION", &[]).await?;
    let row = stream.into_row().await?.unwrap();
    println!("{}", row.get::<&str, _>(0).unwrap());

    Ok(())
}
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                     Public API                        │
│  Client · Config · Query · Row · ColumnData           │
├──────────────┬───────────────┬───────────────────────┤
│  Connection  │    Query      │   Result              │
│  Manager     │    Engine     │   Processing          │
│              │               │                       │
│  TLS, auth,  │  batch mode,  │  token stream,        │
│  negotiation │  RPC, params, │  row decoding,        │
│              │  bulk insert  │  type conversion      │
├──────────────┴───────┬───────┴───────────────────────┤
│  TDS Codec                                           │
│  packet framing, token parsing, type encoding        │
├──────────────────────────────────────────────────────┤
│  Transport (async TCP + TLS)                         │
└──────────────────────────────────────────────────────┘
```

## Attribution

tabby's TDS protocol implementation is inspired by and derived from [tiberius](https://github.com/prisma/tiberius), an excellent TDS driver for Rust by the Prisma team. We stand on the shoulders of cats who came before us.

Additional inspiration from:
- [FreeTDS](https://www.freetds.org/) — the OG open-source TDS implementation (since 1998!)
- [Microsoft TDS documentation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/) — the spec that makes it all possible
- [Microsoft.Data.SqlClient](https://github.com/dotnet/SqlClient) — the reference implementation

## License

MIT
