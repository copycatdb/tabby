mod auth;
mod config;
#[allow(clippy::module_inception)]
mod connection;

mod tls;
#[cfg(any(
    feature = "rustls",
    feature = "native-tls",
    feature = "vendored-openssl"
))]
mod tls_stream;

pub use auth::*;
pub use config::*;
pub(crate) use connection::*;

use crate::protocol::pipeline::ServerMessage;
use crate::protocol::wire::{
    PacketHeader, ProcedureCall, ProcedureParam, RawQuery, RpcProcId, SqlValue,
};
use crate::{
    BulkImport, ColumnAttribute, IntoSql, ProtocolReader,
    protocol::{
        pipeline::{ResultStream, TokenStream},
        wire::IteratorJoin,
    },
    result::ExecuteResult,
};
use enumflags2::BitFlags;
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::TryStreamExt;
use std::{borrow::Cow, fmt::Debug};

/// The main entry point for interacting with SQL Server.
///
/// A `Client` wraps an authenticated TDS connection and exposes methods for
/// executing parameterized queries ([`execute`](Self::execute)), raw SQL
/// ([`execute_raw`](Self::execute_raw)), DML statements
/// ([`run`](Self::run)), and bulk inserts ([`bulk_insert`](Self::bulk_insert)).
///
/// Construct a `Client` by calling [`Client::connect`] with a [`Config`] and
/// an async stream (typically a [`TcpStream`](tokio::net::TcpStream) wrapped
/// with [`compat_write`](tokio_util::compat::TokioAsyncWriteCompatExt::compat_write)).
///
/// # Example
///
/// ```no_run
/// use tabby::{AuthMethod, Client, Config};
/// use tokio_util::compat::TokioAsyncWriteCompatExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut config = Config::new();
/// config.host("localhost");
/// config.port(1433);
/// config.authentication(AuthMethod::sql_server("sa", "your_password"));
/// config.trust_cert();
///
/// let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// tcp.set_nodelay(true)?;
///
/// let mut client = Client::connect(config, tcp.compat_write()).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Client<S: AsyncRead + AsyncWrite + Unpin + Send> {
    pub(crate) connection: Connection<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Client<S> {
    /// Establishes a connection to SQL Server using the given [`Config`] and
    /// async stream.
    ///
    /// The stream is typically a `TcpStream` wrapped with
    /// [`compat_write()`](tokio_util::compat::TokioAsyncWriteCompatExt::compat_write)
    /// to bridge Tokio and futures I/O traits.
    ///
    /// # Errors
    ///
    /// Returns an error if the TLS handshake, login, or authentication fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{AuthMethod, Client, Config};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut config = Config::new();
    /// config.host("localhost");
    /// config.authentication(AuthMethod::sql_server("sa", "password"));
    /// config.trust_cert();
    ///
    /// let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// tcp.set_nodelay(true)?;
    /// let mut client = Client::connect(config, tcp.compat_write()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: Config, tcp_stream: S) -> crate::Result<Client<S>> {
        Ok(Client {
            connection: Connection::connect(config, tcp_stream).await?,
        })
    }

    /// Executes SQL statements and returns the number of rows affected.
    ///
    /// Useful for `INSERT`, `UPDATE`, and `DELETE` statements. Parameters are
    /// positional, referenced as `@P1`, `@P2`, etc. in the query string.
    ///
    /// For `SELECT` queries that return rows, use [`execute`](Self::execute)
    /// instead. For dynamic parameters, see [`Query`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{AuthMethod, Client, Config};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut config = Config::new();
    /// # config.host("localhost");
    /// # config.authentication(AuthMethod::sql_server("sa", "password"));
    /// # config.trust_cert();
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = Client::connect(config, tcp.compat_write()).await?;
    /// let result = client
    ///     .run(
    ///         "INSERT INTO #Users (name) VALUES (@P1), (@P2)",
    ///         &[&"Alice", &"Bob"],
    ///     )
    ///     .await?;
    ///
    /// assert_eq!(2, result.total());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or a parameter type mismatch occurs.
    pub async fn run<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
        params: &[&dyn IntoSql],
    ) -> crate::Result<ExecuteResult> {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        let params = params.iter().map(|s| s.to_sql());
        self.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params)
            .await?;

        ExecuteResult::new(&mut self.connection).await
    }

    /// Executes a parameterized query and returns a [`ResultStream`] for
    /// reading rows.
    ///
    /// Parameters are positional, referenced as `@P1`, `@P2`, etc. Multiple
    /// queries can be delimited with `;`, producing multiple result sets in
    /// the stream.
    ///
    /// For type mappings, see [`IntoSql`] (writing) and [`FromServer`] (reading).
    /// For dynamic parameters, see [`Query`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{AuthMethod, Client, Config};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut config = Config::new();
    /// # config.host("localhost");
    /// # config.authentication(AuthMethod::sql_server("sa", "password"));
    /// # config.trust_cert();
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = Client::connect(config, tcp.compat_write()).await?;
    /// let row = client
    ///     .execute("SELECT @P1 AS id, @P2 AS name", &[&42i32, &"Alice"])
    ///     .await?
    ///     .into_row()
    ///     .await?
    ///     .unwrap();
    ///
    /// let id: i32 = row.get("id").unwrap();
    /// let name: &str = row.get("name").unwrap();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or a parameter type mismatch occurs.
    pub async fn execute<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        params: &'b [&'b dyn IntoSql],
    ) -> crate::Result<ResultStream<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);

        let params = params.iter().map(|p| p.to_sql());
        self.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params)
            .await?;

        let ts = TokenStream::new(&mut self.connection);
        let mut result = ResultStream::new(ts.try_unfold());
        result.forward_to_metadata().await?;

        Ok(result)
    }

    /// Executes raw (unparameterized) SQL and returns a [`ResultStream`].
    ///
    /// Useful for DDL statements, multiple batched queries, or cases where
    /// `sp_executesql` parameterization is not desired.
    ///
    /// # Warning
    ///
    /// **Do not** pass user-supplied input to this method — use
    /// [`execute`](Self::execute) with parameters instead to prevent SQL injection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{AuthMethod, Client, Config};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut config = Config::new();
    /// # config.host("localhost");
    /// # config.authentication(AuthMethod::sql_server("sa", "password"));
    /// # config.trust_cert();
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = Client::connect(config, tcp.compat_write()).await?;
    /// let row = client
    ///     .execute_raw("SELECT 1 AS col")
    ///     .await?
    ///     .into_row()
    ///     .await?
    ///     .unwrap();
    ///
    /// assert_eq!(Some(1i32), row.get("col"));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_raw<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
    ) -> crate::Result<ResultStream<'a>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

        let ts = TokenStream::new(&mut self.connection);

        let mut result = ResultStream::new(ts.try_unfold());
        result.forward_to_metadata().await?;

        Ok(result)
    }

    /// Starts a `BULK INSERT` operation for efficiently loading many rows into
    /// a table.
    ///
    /// The returned [`BulkImport`] handle lets you send rows one at a time.
    /// Each row must match the table schema (excluding identity / non-updatable
    /// columns, which are filtered automatically). Call
    /// [`finalize()`](BulkImport::finalize) when done.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{AuthMethod, Client, Config, IntoRowMessage};
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let mut config = Config::new();
    /// # config.host("localhost");
    /// # config.authentication(AuthMethod::sql_server("sa", "password"));
    /// # config.trust_cert();
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = Client::connect(config, tcp.compat_write()).await?;
    /// // Assume ##bulk_test(id INT IDENTITY, val INT NOT NULL) exists.
    /// let mut req = client.bulk_insert("##bulk_test").await?;
    ///
    /// for i in [10i32, 20, 30] {
    ///     req.send(i.into_row()).await?;
    /// }
    ///
    /// let res = req.finalize().await?;
    /// assert_eq!(3, res.total());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bulk_insert<'a>(&'a mut self, table: &'a str) -> crate::Result<BulkImport<'a, S>> {
        // Start the bulk request
        self.connection.flush_stream().await?;

        // retrieve column metadata from server
        let query = format!("SELECT TOP 0 * FROM {}", table);

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

        let token_stream = TokenStream::new(&mut self.connection).try_unfold();

        let columns = token_stream
            .try_fold(None, |mut columns, token| async move {
                if let ServerMessage::NewResultset(metadata) = token {
                    columns = Some(metadata.columns.clone());
                };

                Ok(columns)
            })
            .await?;

        // now start bulk upload
        let columns: Vec<_> = columns
            .ok_or_else(|| {
                crate::Error::Protocol("expecting column metadata from query but not found".into())
            })?
            .into_iter()
            .filter(|column| column.base.flags.contains(ColumnAttribute::Updateable))
            .collect();

        self.connection.flush_stream().await?;
        let col_data = columns.iter().map(|c| format!("{}", c)).join(", ");
        let query = format!("INSERT BULK {} ({})", table, col_data);

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();

        self.connection.send(PacketHeader::batch(id), req).await?;

        let ts = TokenStream::new(&mut self.connection);
        ts.flush_done().await?;

        BulkImport::new(&mut self.connection, columns)
    }

    /// Gracefully closes the connection to the server.
    ///
    /// This flushes any pending data and shuts down the underlying transport.
    /// The `Client` is consumed by this call.
    pub async fn close(self) -> crate::Result<()> {
        self.connection.close().await
    }

    pub(crate) fn rpc_params<'a>(query: impl Into<Cow<'a, str>>) -> Vec<ProcedureParam<'a>> {
        vec![
            ProcedureParam {
                name: Cow::Borrowed("stmt"),
                flags: BitFlags::empty(),
                value: SqlValue::String(Some(query.into())),
            },
            ProcedureParam {
                name: Cow::Borrowed("params"),
                flags: BitFlags::empty(),
                value: SqlValue::I32(Some(0)),
            },
        ]
    }

    pub(crate) async fn rpc_perform_query<'a, 'b>(
        &'a mut self,
        proc_id: RpcProcId,
        mut rpc_params: Vec<ProcedureParam<'b>>,
        params: impl Iterator<Item = SqlValue<'b>>,
    ) -> crate::Result<()>
    where
        'a: 'b,
    {
        let mut param_str = String::new();

        for (i, param) in params.enumerate() {
            if i > 0 {
                param_str.push(',')
            }
            param_str.push_str(&format!("@P{} ", i + 1));
            param_str.push_str(&param.type_name());

            rpc_params.push(ProcedureParam {
                name: Cow::Owned(format!("@P{}", i + 1)),
                flags: BitFlags::empty(),
                value: param,
            });
        }

        if let Some(params) = rpc_params.iter_mut().find(|x| x.name == "params") {
            params.value = SqlValue::String(Some(param_str.into()));
        }

        let req = ProcedureCall::new(
            proc_id,
            rpc_params,
            self.connection.context().transaction_descriptor(),
        );

        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::rpc(id), req).await?;

        Ok(())
    }
}
