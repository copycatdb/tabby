use std::borrow::Cow;

use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{
    Client, ExecuteResult, IntoSqlOwned, ResultStream, SqlValue,
    protocol::{pipeline::TokenStream, wire::RpcProcId},
};

/// A dynamically parameterized query object.
///
/// Use `Query` when the number of parameters is not known at compile time.
/// For static parameter lists, [`Client::execute`] and [`Client::run`] are
/// simpler alternatives.
///
/// Parameters are referenced as `@P1`, `@P2`, … in the SQL string and bound
/// via [`bind`](Self::bind).
///
/// # Example
///
/// ```no_run
/// # use tabby::{AuthMethod, Client, Config, Query};
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
/// let mut query = Query::new("SELECT @P1 AS a, @P2 AS b");
/// query.bind(1i32);
/// query.bind("hello");
///
/// let row = query.query(&mut client).await?.into_row().await?.unwrap();
/// let a: i32 = row.get("a").unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Query<'a> {
    sql: Cow<'a, str>,
    params: Vec<SqlValue<'a>>,
}

impl<'a> Query<'a> {
    /// Construct a new query object with the given SQL. If the SQL is
    /// parameterized, the given number of parameters must be bound to the
    /// object before executing.
    ///
    /// The `sql` can define the parameter placement by annotating them with
    /// `@PN`, where N is the index of the parameter, starting from `1`.
    pub fn new(sql: impl Into<Cow<'a, str>>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// Bind a new parameter to the query. Must be called exactly as many times
    /// as there are parameters in the given SQL. Otherwise the query will fail
    /// on execution.
    pub fn bind(&mut self, param: impl IntoSqlOwned<'a> + 'a) {
        self.params.push(param.into_sql());
    }

    /// Executes the query and returns an [`ExecuteResult`] with row counts.
    ///
    /// See [`Client::run`] for the simpler static-parameter equivalent.
    pub async fn execute<S>(self, client: &mut Client<S>) -> crate::Result<ExecuteResult>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        client.connection.flush_stream().await?;

        let rpc_params = Client::<S>::rpc_params(self.sql);

        client
            .rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, self.params.into_iter())
            .await?;

        ExecuteResult::new(&mut client.connection).await
    }

    /// Executes the query and returns a [`ResultStream`] for reading rows.
    ///
    /// See [`Client::execute`] for the simpler static-parameter equivalent.
    pub async fn query<'b, S>(self, client: &'b mut Client<S>) -> crate::Result<ResultStream<'b>>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        client.connection.flush_stream().await?;
        let rpc_params = Client::<S>::rpc_params(self.sql);

        client
            .rpc_perform_query(RpcProcId::ExecuteSQL, rpc_params, self.params.into_iter())
            .await?;

        let ts = TokenStream::new(&mut client.connection);
        let mut result = ResultStream::new(ts.try_unfold());
        result.forward_to_metadata().await?;

        Ok(result)
    }
}
