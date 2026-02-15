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
pub use connection::*;

use crate::protocol::pipeline::ServerMessage;

/// Convenience type alias for `Connection` over a tokio TCP stream (with compat layer).
pub type TcpConnection = Connection<tokio_util::compat::Compat<tokio::net::TcpStream>>;

/// Convenience type alias for `Client` over a tokio TCP stream (with compat layer).
pub type TcpClient = Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;
use crate::protocol::wire::{
    ColumnSchema, CompletionMessage, OrderMessage, PacketHeader, ProcedureCall, ProcedureParam,
    RawQuery, ReturnValue, RpcProcId, ServerError, ServerNotice, SessionChange, SqlValue,
};
use crate::{
    BulkImport, Column, ColumnAttribute, IntoSql, MessageKind, ProtocolReader,
    protocol::{
        pipeline::{ResultStream, TokenStream},
        wire::IteratorJoin,
    },
    result::ExecuteResult,
};

/// Result of a single `batch_fetch_row` call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchFetchResult {
    /// A row was decoded and written to the RowWriter.
    Row,
    /// The current result set is complete. Contains the number of rows affected.
    Done(u64),
    /// Another result set follows. Call `batch_fetch_metadata` to read its columns.
    MoreResults,
}
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

    /// Connects to the database, automatically following Azure SQL Database
    /// routing redirects.
    ///
    /// Azure SQL Database uses a gateway that redirects clients to the actual
    /// worker node. This method handles that transparently by using the
    /// provided `connector` closure to establish a new TCP connection when
    /// a redirect is received.
    ///
    /// The `connector` closure receives `(host, port)` and should return an
    /// async stream (e.g., a TLS-wrapped `TcpStream`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use tabby::{Client, Config, AuthMethod};
    /// # use tokio::net::TcpStream;
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut config = Config::new();
    /// config.host("myserver.database.windows.net");
    /// config.authentication(AuthMethod::sql_server("user", "password"));
    /// config.trust_cert();
    ///
    /// let mut client = Client::connect_with_redirect(config, |host, port| async move {
    ///     let addr = format!("{}:{}", host, port);
    ///     let tcp = TcpStream::connect(&addr).await?;
    ///     tcp.set_nodelay(true)?;
    ///     Ok(tcp.compat_write())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_redirect<F, Fut>(
        config: Config,
        connector: F,
    ) -> crate::Result<Client<S>>
    where
        F: Fn(String, u16) -> Fut,
        Fut: std::future::Future<
                Output = std::result::Result<S, Box<dyn std::error::Error + Send + Sync>>,
            >,
    {
        let host = config.get_host().to_string();
        let port = config.get_port();

        let initial_stream = connector(host, port).await.map_err(|e| crate::Error::Io {
            kind: std::io::ErrorKind::ConnectionRefused,
            message: e.to_string(),
        })?;

        match Connection::connect(config.clone(), initial_stream).await {
            Ok(connection) => Ok(Client { connection }),
            Err(crate::Error::Routing { host, port }) => {
                let redirected_stream =
                    connector(host.clone(), port)
                        .await
                        .map_err(|e| crate::Error::Io {
                            kind: std::io::ErrorKind::ConnectionRefused,
                            message: format!(
                                "Failed to connect to redirected address {}:{}: {}",
                                host, port, e
                            ),
                        })?;

                let mut redirected_config = config;
                redirected_config.host(&host);
                redirected_config.port(port);

                Ok(Client {
                    connection: Connection::connect(redirected_config, redirected_stream).await?,
                })
            }
            Err(e) => Err(e),
        }
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

    pub fn rpc_params<'a>(query: impl Into<Cow<'a, str>>) -> Vec<ProcedureParam<'a>> {
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

    pub async fn rpc_perform_query<'a, 'b>(
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

    /// Execute a query and decode rows directly into a RowWriter, bypassing
    /// SqlValue allocation entirely.
    pub async fn query_direct<'a, 'b, W: crate::row_writer::RowWriter>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        params: &'b [&'b dyn IntoSql],
        mut on_metadata: impl FnMut(&[Column]) -> W,
        mut on_row_done: impl FnMut(&mut W) -> bool,
    ) -> crate::Result<Option<W>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;
        let rpc_params = Self::rpc_params(query);
        let params_iter = params.iter().map(|p| p.to_sql());
        self.rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, params_iter)
            .await?;

        let mut writer: Option<W> = None;
        let mut string_buf = String::with_capacity(4096);
        let mut bytes_buf = Vec::with_capacity(4096);

        loop {
            if self.connection.is_eof() {
                break;
            }

            let ty_byte = self.connection.read_u8().await?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta =
                        std::sync::Arc::new(ColumnSchema::decode(&mut self.connection).await?);
                    self.connection.context_mut().set_last_meta(meta.clone());

                    let columns: Vec<Column> = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: crate::row::ColumnType::from(&x.base.ty),
                            type_info: Some(x.base.ty.clone()),
                            nullable: Some(x.base.flags.contains(ColumnAttribute::Nullable)),
                        })
                        .collect();

                    writer = Some(on_metadata(&columns));
                }
                MessageKind::Row => {
                    if let Some(ref mut w) = writer {
                        crate::protocol::wire::decode_direct::decode_row_into(
                            &mut self.connection,
                            w,
                            &mut string_buf,
                            &mut bytes_buf,
                        )
                        .await?;
                        on_row_done(w);
                    }
                }
                MessageKind::NbcRow => {
                    if let Some(ref mut w) = writer {
                        crate::protocol::wire::decode_direct::decode_nbc_row_into(
                            &mut self.connection,
                            w,
                            &mut string_buf,
                            &mut bytes_buf,
                        )
                        .await?;
                        on_row_done(w);
                    }
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let _done = CompletionMessage::decode(&mut self.connection).await?;
                    if self.connection.is_eof() {
                        break;
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le().await?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode(&mut self.connection).await?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode(&mut self.connection).await?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let _info = ServerNotice::decode(&mut self.connection).await?;
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode(&mut self.connection).await?;
                    match change {
                        SessionChange::PacketSize(new_size, _) => {
                            self.connection.context_mut().set_packet_size(new_size);
                        }
                        SessionChange::BeginTransaction(desc) => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor(desc);
                        }
                        SessionChange::CommitTransaction
                        | SessionChange::RollbackTransaction
                        | SessionChange::DefectTransaction => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor([0; 8]);
                        }
                        _ => (),
                    }
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode(&mut self.connection).await?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in query_direct", ty).into(),
                    ));
                }
            }
        }

        Ok(writer)
    }

    /// Execute raw SQL and decode rows directly into a RowWriter, bypassing
    /// SqlValue allocation entirely. Like [`query_direct`](Self::query_direct)
    /// but for raw SQL batches (no parameterized query support).
    ///
    /// The `on_metadata` callback is invoked once per result set with the
    /// column schema, returning a fresh writer. `on_row_done` is called after
    /// each row is decoded. If `on_row_done` returns `false`, remaining rows
    /// in the current result set are skipped.
    ///
    /// Returns the writer for the last result set, or `None` if the batch
    /// produced no result sets.
    pub async fn batch_direct<'a, 'b, W: crate::row_writer::RowWriter>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        mut on_metadata: impl FnMut(&[Column]) -> W,
        mut on_row_done: impl FnMut(&mut W) -> bool,
    ) -> crate::Result<Option<W>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

        let mut writer: Option<W> = None;
        let mut string_buf = String::with_capacity(4096);
        let mut bytes_buf = Vec::with_capacity(4096);

        loop {
            if self.connection.is_eof() {
                break;
            }

            let ty_byte = self.connection.read_u8().await?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta =
                        std::sync::Arc::new(ColumnSchema::decode(&mut self.connection).await?);
                    self.connection.context_mut().set_last_meta(meta.clone());

                    let columns: Vec<Column> = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: crate::row::ColumnType::from(&x.base.ty),
                            type_info: Some(x.base.ty.clone()),
                            nullable: Some(x.base.flags.contains(ColumnAttribute::Nullable)),
                        })
                        .collect();

                    writer = Some(on_metadata(&columns));
                }
                MessageKind::Row => {
                    if let Some(ref mut w) = writer {
                        crate::protocol::wire::decode_direct::decode_row_into(
                            &mut self.connection,
                            w,
                            &mut string_buf,
                            &mut bytes_buf,
                        )
                        .await?;
                        on_row_done(w);
                    }
                }
                MessageKind::NbcRow => {
                    if let Some(ref mut w) = writer {
                        crate::protocol::wire::decode_direct::decode_nbc_row_into(
                            &mut self.connection,
                            w,
                            &mut string_buf,
                            &mut bytes_buf,
                        )
                        .await?;
                        on_row_done(w);
                    }
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let _done = CompletionMessage::decode(&mut self.connection).await?;
                    if self.connection.is_eof() {
                        break;
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le().await?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode(&mut self.connection).await?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode(&mut self.connection).await?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let _info = ServerNotice::decode(&mut self.connection).await?;
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode(&mut self.connection).await?;
                    match change {
                        SessionChange::PacketSize(new_size, _) => {
                            self.connection.context_mut().set_packet_size(new_size);
                        }
                        SessionChange::BeginTransaction(desc) => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor(desc);
                        }
                        SessionChange::CommitTransaction
                        | SessionChange::RollbackTransaction
                        | SessionChange::DefectTransaction => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor([0; 8]);
                        }
                        _ => (),
                    }
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode(&mut self.connection).await?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in batch_direct", ty).into(),
                    ));
                }
            }
        }

        Ok(writer)
    }

    /// Execute raw SQL and decode rows directly into a single RowWriter.
    /// Unlike `batch_direct`, this keeps ONE writer across all result sets,
    /// calling `RowWriter::on_metadata` when a new result set starts and
    /// `RowWriter::on_row_done` after each row. Ideal for multi-result-set
    /// queries where you want to accumulate all results.
    pub async fn batch_into<'a, 'b, W: crate::row_writer::RowWriter>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
        writer: &mut W,
    ) -> crate::Result<()>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

        let mut string_buf = String::with_capacity(4096);
        let mut bytes_buf = Vec::with_capacity(4096);

        loop {
            if self.connection.is_eof() {
                break;
            }

            let ty_byte = self.connection.read_u8().await?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta =
                        std::sync::Arc::new(ColumnSchema::decode(&mut self.connection).await?);
                    self.connection.context_mut().set_last_meta(meta.clone());

                    let columns: Vec<Column> = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: crate::row::ColumnType::from(&x.base.ty),
                            type_info: Some(x.base.ty.clone()),
                            nullable: Some(x.base.flags.contains(ColumnAttribute::Nullable)),
                        })
                        .collect();

                    writer.on_metadata(&columns);
                }
                MessageKind::Row => {
                    crate::protocol::wire::decode_direct::decode_row_into(
                        &mut self.connection,
                        writer,
                        &mut string_buf,
                        &mut bytes_buf,
                    )
                    .await?;
                    writer.on_row_done();
                }
                MessageKind::NbcRow => {
                    crate::protocol::wire::decode_direct::decode_nbc_row_into(
                        &mut self.connection,
                        writer,
                        &mut string_buf,
                        &mut bytes_buf,
                    )
                    .await?;
                    writer.on_row_done();
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let done = CompletionMessage::decode(&mut self.connection).await?;
                    writer.on_done(done.rows());
                    if done.has_more() && !self.connection.is_eof() {
                        writer.on_result_set_end();
                    }
                    if self.connection.is_eof() {
                        break;
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le().await?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode(&mut self.connection).await?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode(&mut self.connection).await?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let info = ServerNotice::decode(&mut self.connection).await?;
                    writer.on_info(info.number, &info.message);
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode(&mut self.connection).await?;
                    match change {
                        SessionChange::PacketSize(new_size, _) => {
                            self.connection.context_mut().set_packet_size(new_size);
                        }
                        SessionChange::BeginTransaction(desc) => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor(desc);
                        }
                        SessionChange::CommitTransaction
                        | SessionChange::RollbackTransaction
                        | SessionChange::DefectTransaction => {
                            self.connection
                                .context_mut()
                                .set_transaction_descriptor([0; 8]);
                        }
                        _ => (),
                    }
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode(&mut self.connection).await?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in batch_into", ty).into(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Send a raw SQL batch and read tokens until the first ColMetaData,
    /// returning the column metadata. The connection is left positioned to
    /// read Row tokens via [`batch_fetch_row`].
    ///
    /// If the batch produces no result set (e.g. INSERT/UPDATE), this reads
    /// all tokens and returns an empty Vec.
    pub async fn batch_start<'a, 'b>(
        &'a mut self,
        query: impl Into<Cow<'b, str>>,
    ) -> crate::Result<Vec<Column>>
    where
        'a: 'b,
    {
        self.connection.flush_stream().await?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req).await?;

        self.batch_read_until_metadata().await
    }

    /// Read tokens until ColMetaData is found, returning columns.
    /// Used by batch_start and batch_fetch_metadata.
    async fn batch_read_until_metadata(&mut self) -> crate::Result<Vec<Column>> {
        loop {
            if self.connection.is_eof() {
                return Ok(Vec::new());
            }

            let ty_byte = self.connection.read_u8().await?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta =
                        std::sync::Arc::new(ColumnSchema::decode(&mut self.connection).await?);
                    self.connection.context_mut().set_last_meta(meta.clone());

                    let columns: Vec<Column> = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: crate::row::ColumnType::from(&x.base.ty),
                            type_info: Some(x.base.ty.clone()),
                            nullable: Some(x.base.flags.contains(ColumnAttribute::Nullable)),
                        })
                        .collect();

                    return Ok(columns);
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let _done = CompletionMessage::decode(&mut self.connection).await?;
                    if self.connection.is_eof() {
                        return Ok(Vec::new());
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le().await?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode(&mut self.connection).await?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode(&mut self.connection).await?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let _info = ServerNotice::decode(&mut self.connection).await?;
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode(&mut self.connection).await?;
                    self.apply_env_change(change);
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode(&mut self.connection).await?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in batch_start", ty).into(),
                    ));
                }
            }
        }
    }

    /// Read the next token from the TDS stream after a `batch_start` call.
    ///
    /// - Returns `BatchFetchResult::Row` if a row was decoded into the writer.
    /// - Returns `BatchFetchResult::Done(n)` when the result set is complete.
    /// - Returns `BatchFetchResult::MoreResults` when another result set follows.
    ///
    /// After `MoreResults`, call `batch_fetch_metadata` to read the next
    /// result set's column schema.
    pub async fn batch_fetch_row<W: crate::row_writer::RowWriter>(
        &mut self,
        writer: &mut W,
        string_buf: &mut String,
        bytes_buf: &mut Vec<u8>,
    ) -> crate::Result<BatchFetchResult> {
        loop {
            if self.connection.is_eof() {
                return Ok(BatchFetchResult::Done(0));
            }

            let ty_byte = self.connection.read_u8().await?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::Row => {
                    crate::protocol::wire::decode_direct::decode_row_into(
                        &mut self.connection,
                        writer,
                        string_buf,
                        bytes_buf,
                    )
                    .await?;
                    return Ok(BatchFetchResult::Row);
                }
                MessageKind::NbcRow => {
                    crate::protocol::wire::decode_direct::decode_nbc_row_into(
                        &mut self.connection,
                        writer,
                        string_buf,
                        bytes_buf,
                    )
                    .await?;
                    return Ok(BatchFetchResult::Row);
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let done = CompletionMessage::decode(&mut self.connection).await?;
                    let rows = done.rows();
                    if done.has_more() && !self.connection.is_eof() {
                        return Ok(BatchFetchResult::MoreResults);
                    }
                    if self.connection.is_eof() {
                        return Ok(BatchFetchResult::Done(rows));
                    }
                    // Non-final done without More flag but not EOF — keep reading
                    // (can happen with intermediate DONE tokens)
                    return Ok(BatchFetchResult::Done(rows));
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le().await?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode(&mut self.connection).await?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode(&mut self.connection).await?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let info = ServerNotice::decode(&mut self.connection).await?;
                    writer.on_info(info.number, &info.message);
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode(&mut self.connection).await?;
                    self.apply_env_change(change);
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode(&mut self.connection).await?;
                }
                MessageKind::ColMetaData => {
                    // New result set metadata — this shouldn't happen during row fetch
                    // but handle it gracefully by signaling MoreResults
                    let meta =
                        std::sync::Arc::new(ColumnSchema::decode(&mut self.connection).await?);
                    self.connection.context_mut().set_last_meta(meta);
                    return Ok(BatchFetchResult::MoreResults);
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in batch_fetch_row", ty).into(),
                    ));
                }
            }
        }
    }

    /// After `batch_fetch_row` returns `MoreResults`, call this to read the
    /// next result set's column metadata.
    pub async fn batch_fetch_metadata(&mut self) -> crate::Result<Vec<Column>> {
        self.batch_read_until_metadata().await
    }

    /// Drain remaining tokens until the connection is at EOF.
    /// Useful for canceling a streaming query mid-way.
    pub async fn batch_drain(&mut self) -> crate::Result<()> {
        let mut dummy_str = String::new();
        let mut dummy_bytes = Vec::new();
        let mut dummy_writer = crate::row_writer::SqlValueWriter::with_capacity(0);
        loop {
            match self
                .batch_fetch_row(&mut dummy_writer, &mut dummy_str, &mut dummy_bytes)
                .await?
            {
                BatchFetchResult::Row => continue,
                BatchFetchResult::MoreResults => continue,
                BatchFetchResult::Done(_) => return Ok(()),
            }
        }
    }

    fn apply_env_change(&mut self, change: SessionChange) {
        match change {
            SessionChange::PacketSize(new_size, _) => {
                self.connection.context_mut().set_packet_size(new_size);
            }
            SessionChange::BeginTransaction(desc) => {
                self.connection
                    .context_mut()
                    .set_transaction_descriptor(desc);
            }
            SessionChange::CommitTransaction
            | SessionChange::RollbackTransaction
            | SessionChange::DefectTransaction => {
                self.connection
                    .context_mut()
                    .set_transaction_descriptor([0; 8]);
            }
            _ => (),
        }
    }
}
