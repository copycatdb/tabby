use crate::protocol::pipeline::ServerMessage;
use crate::protocol::wire::ColumnAttribute;
use crate::protocol::wire::ServerNotice;
use crate::{Column, Row, row::ColumnType};
use futures_util::{
    ready,
    stream::{BoxStream, Peekable, Stream, StreamExt, TryStreamExt},
};
use std::{
    fmt::Debug,
    pin::Pin,
    sync::Arc,
    task::{self, Poll},
};

/// A set of `Streams` of [`ResultItem`] values, which can be either result
/// metadata or a row.
///
/// The `ResultStream` needs to be polled empty before sending another query to
/// the [`Client`], failing to do so causes a flush before the next query,
/// slowing it down in an undeterministic way.
///
/// Every stream starts with metadata, describing the structure of the incoming
/// rows, e.g. the columns in the order they are presented in every row.
///
/// If after consuming rows from the stream, another metadata result arrives, it
/// means the stream has multiple results from different queries. This new
/// metadata item will describe the next rows from here forwards.
///
/// If having one set of results in the response, using [`into_row_stream`]
/// might be more convenient to use.
///
/// The struct provides non-streaming APIs with [`into_results`],
/// [`into_first_result`] and [`into_row`].
///
/// # Example
///
/// ```ignore
/// # use tabby::{Config, ResultItem};
/// # use tokio_util::compat::TokioAsyncWriteCompatExt;
/// # use std::env;
/// # use futures_util::stream::TryStreamExt;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let c_str = env::var("TDS_TEST_CONNECTION_STRING").unwrap_or(
/// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
/// # );
/// # config.host("localhost"); config.authentication(AuthMethod::sql_server("sa", "password"));
/// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// # tcp.set_nodelay(true)?;
/// # let mut client = tabby::Client::connect(config, tcp.compat_write()).await?;
/// let mut stream = client
///     .execute(
///         "SELECT @P1 AS first; SELECT @P2 AS second",
///         &[&1i32, &2i32],
///     )
///     .await?;
///
/// // The stream consists of four items, in the following order:
/// // - Metadata from `SELECT 1`
/// // - The only resulting row from `SELECT 1`
/// // - Metadata from `SELECT 2`
/// // - The only resulting row from `SELECT 2`
/// while let Some(item) = stream.try_next().await? {
///     match item {
///         // our first item is the column data always
///         ResultItem::Metadata(meta) if meta.result_index() == 0 => {
///             // the first result column info can be handled here
///         }
///         // ... and from there on from 0..N rows
///         ResultItem::Row(row) if row.result_index() == 0 => {
///             assert_eq!(Some(1), row.get(0));
///         }
///         // the second result set returns first another metadata item
///         ResultItem::Metadata(meta) => {
///             // .. handling
///         }
///         // ...and, again, we get rows from the second resultset
///         ResultItem::Row(row) => {
///             assert_eq!(Some(2), row.get(0));
///         }
///     }
/// }
/// # Ok(())
/// # }
/// ```
///
/// [`Client`]: struct.Client.html
/// [`into_row_stream`]: struct.ResultStream.html#method.into_row_stream
/// [`into_results`]: struct.ResultStream.html#method.into_results
/// [`into_first_result`]: struct.ResultStream.html#method.into_first_result
/// [`into_row`]: struct.ResultStream.html#method.into_row
pub struct ResultStream<'a> {
    token_stream: Peekable<BoxStream<'a, crate::Result<ServerMessage>>>,
    columns: Option<Arc<Vec<Column>>>,
    result_set_index: Option<usize>,
    info_messages: Vec<ServerNotice>,
}

impl<'a> Debug for ResultStream<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResultStream")
            .field(
                "token_stream",
                &"BoxStream<'a, crate::Result<ServerMessage>>",
            )
            .finish()
    }
}

impl<'a> ResultStream<'a> {
    pub(crate) fn new(token_stream: BoxStream<'a, crate::Result<ServerMessage>>) -> Self {
        Self {
            token_stream: token_stream.peekable(),
            columns: None,
            result_set_index: None,
            info_messages: Vec::new(),
        }
    }

    /// Moves the stream forward until having result metadata, stream end or an
    /// error.
    pub(crate) async fn forward_to_metadata(&mut self) -> crate::Result<()> {
        loop {
            let item = Pin::new(&mut self.token_stream)
                .peek()
                .await
                .map(|r| r.as_ref().map_err(|e| e.clone()))
                .transpose()?;

            match item {
                Some(ServerMessage::NewResultset(_)) => break,
                Some(ServerMessage::Info(_)) => {
                    // Consume and capture info tokens
                    if let Some(Ok(ServerMessage::Info(info))) =
                        self.token_stream.try_next().await.transpose()
                    {
                        self.info_messages.push(info);
                    }
                }
                Some(_) => {
                    self.token_stream.try_next().await?;
                }
                None => break,
            }
        }

        Ok(())
    }

    /// The list of columns either for the current result set, or for the next
    /// one. If the stream is just created, or if the next item in the stream
    /// contains metadata, the metadata will be taken from the stream. Otherwise
    /// the columns will be returned from the cache and reflect on the current
    /// result set.
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use tabby::Config;
    /// # use tokio_util::compat::TokioAsyncWriteCompatExt;
    /// # use std::env;
    /// # use futures_util::stream::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let c_str = env::var("TDS_TEST_CONNECTION_STRING").unwrap_or(
    /// #     "server=tcp:localhost,1433;integratedSecurity=true;TrustServerCertificate=true".to_owned(),
    /// # );
    /// # config.host("localhost"); config.authentication(AuthMethod::sql_server("sa", "password"));
    /// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
    /// # tcp.set_nodelay(true)?;
    /// # let mut client = tabby::Client::connect(config, tcp.compat_write()).await?;
    /// let mut stream = client
    ///     .execute(
    ///         "SELECT @P1 AS first; SELECT @P2 AS second",
    ///         &[&1i32, &2i32],
    ///     )
    ///     .await?;
    ///
    /// // Nothing is fetched, the first result set starts.
    /// let cols = stream.columns().await?.unwrap();
    /// assert_eq!("first", cols[0].name());
    ///
    /// // Move over the metadata.
    /// stream.try_next().await?;
    ///
    /// // We're in the first row, seeing the metadata for that set.
    /// let cols = stream.columns().await?.unwrap();
    /// assert_eq!("first", cols[0].name());
    ///
    /// // Move over the only row in the first set.
    /// stream.try_next().await?;
    ///
    /// // End of the first set, getting the metadata by peaking the next item.
    /// let cols = stream.columns().await?.unwrap();
    /// assert_eq!("second", cols[0].name());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn columns(&mut self) -> crate::Result<Option<&[Column]>> {
        use ServerMessage::*;

        loop {
            let item = Pin::new(&mut self.token_stream)
                .peek()
                .await
                .map(|r| r.as_ref().map_err(|e| e.clone()))
                .transpose()?;

            match item {
                Some(token) => match token {
                    NewResultset(metadata) => {
                        self.columns = Some(Arc::new(metadata.columns().collect()));
                        break;
                    }
                    Row(_) => {
                        break;
                    }
                    _ => {
                        self.token_stream.try_next().await?;
                        continue;
                    }
                },
                None => {
                    break;
                }
            }
        }

        Ok(self.columns.as_ref().map(|c| c.as_slice()))
    }

    /// Collects results from all queries in the stream into memory in the order
    /// of querying.
    pub async fn into_results(mut self) -> crate::Result<Vec<Vec<Row>>> {
        let mut results: Vec<Vec<Row>> = Vec::new();
        let mut result: Option<Vec<Row>> = None;

        while let Some(item) = self.try_next().await? {
            match (item, &mut result) {
                (ResultItem::Row(row), None) => {
                    result = Some(vec![row]);
                }
                (ResultItem::Row(row), Some(result)) => result.push(row),
                (ResultItem::Metadata(_), None) => {
                    result = Some(Vec::new());
                }
                (ResultItem::Metadata(_), ref mut previous_result) => {
                    results.push(previous_result.take().unwrap());
                    result = None;
                }
                (ResultItem::Message(_), _) => { /* skip info messages */ }
            }
        }

        if let Some(result) = result {
            results.push(result);
        }

        Ok(results)
    }

    /// Collects the output of the first query, dropping any further
    /// results.
    pub async fn into_first_result(self) -> crate::Result<Vec<Row>> {
        let mut results = self.into_results().await?.into_iter();
        let rows = results.next().unwrap_or_default();

        Ok(rows)
    }

    /// Collects the first row from the output of the first query, dropping any
    /// further rows.
    pub async fn into_row(self) -> crate::Result<Option<Row>> {
        let mut results = self.into_first_result().await?.into_iter();

        Ok(results.next())
    }

    /// Convert the stream into a stream of rows, skipping metadata items.
    pub fn into_row_stream(self) -> BoxStream<'a, crate::Result<Row>> {
        let s = self.try_filter_map(|item| async {
            match item {
                ResultItem::Row(row) => Ok(Some(row)),
                _ => Ok(None),
            }
        });

        Box::pin(s)
    }
}

/// Info about the following stream of rows.
#[derive(Debug, Clone)]
pub struct ResultSchema {
    columns: Arc<Vec<Column>>,
    result_index: usize,
}

impl ResultSchema {
    /// Column info. The order is the same as in the following rows.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// The number of the result set, an incrementing value starting from zero,
    /// which gives an indication of the position of the result set in the
    /// stream.
    pub fn result_index(&self) -> usize {
        self.result_index
    }
}

/// Resulting data from a query.
#[derive(Debug)]
pub enum ResultItem {
    /// A single row of data.
    Row(Row),
    /// Information of the upcoming row data.
    Metadata(ResultSchema),
    /// An informational message from the server (e.g. PRINT output).
    Message(ServerNotice),
}

impl ResultItem {
    pub(crate) fn metadata(columns: Arc<Vec<Column>>, result_index: usize) -> Self {
        Self::Metadata(ResultSchema {
            columns,
            result_index,
        })
    }

    /// Returns a reference to the metadata, if the item is of a correct variant.
    pub fn as_metadata(&self) -> Option<&ResultSchema> {
        match self {
            ResultItem::Metadata(metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns a reference to the row, if the item is of a correct variant.
    pub fn as_row(&self) -> Option<&Row> {
        match self {
            ResultItem::Row(row) => Some(row),
            _ => None,
        }
    }

    /// Returns the metadata, if the item is of a correct variant.
    pub fn into_metadata(self) -> Option<ResultSchema> {
        match self {
            ResultItem::Metadata(metadata) => Some(metadata),
            _ => None,
        }
    }

    /// Returns the row, if the item is of a correct variant.
    pub fn into_row(self) -> Option<Row> {
        match self {
            ResultItem::Row(row) => Some(row),
            _ => None,
        }
    }
}

impl<'a> Stream for ResultStream<'a> {
    type Item = crate::Result<ResultItem>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // First yield any info messages captured during forward_to_metadata
        if !this.info_messages.is_empty() {
            let info = this.info_messages.remove(0);
            return Poll::Ready(Some(Ok(ResultItem::Message(info))));
        }

        loop {
            let token = match ready!(this.token_stream.poll_next_unpin(cx)) {
                Some(res) => res?,
                None => return Poll::Ready(None),
            };

            return match token {
                ServerMessage::NewResultset(meta) => {
                    let column_meta = meta
                        .columns
                        .iter()
                        .map(|x| Column {
                            name: x.col_name.to_string(),
                            column_type: ColumnType::from(&x.base.ty),
                            type_info: Some(x.base.ty.clone()),
                            nullable: Some(x.base.flags.contains(ColumnAttribute::Nullable)),
                        })
                        .collect::<Vec<_>>();

                    let column_meta = Arc::new(column_meta);
                    this.columns = Some(column_meta.clone());

                    this.result_set_index = this.result_set_index.map(|i| i + 1);

                    let query_item =
                        ResultItem::metadata(column_meta, *this.result_set_index.get_or_insert(0));

                    return Poll::Ready(Some(Ok(query_item)));
                }
                ServerMessage::Row(data) => {
                    let columns = this.columns.as_ref().unwrap().clone();
                    let result_index = this.result_set_index.unwrap();

                    let row = Row {
                        columns,
                        data,
                        result_index,
                    };

                    Poll::Ready(Some(Ok(ResultItem::Row(row))))
                }
                ServerMessage::Info(info) => Poll::Ready(Some(Ok(ResultItem::Message(info)))),
                _ => continue,
            };
        }
    }
}
