use crate::protocol::pipeline::ServerMessage;
use crate::protocol::wire::ColumnAttribute;
use crate::protocol::wire::ServerNotice;
use crate::{
    Column,
    row::{ColumnType, Row},
};
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

/// A stream of [`ResultItem`] values returned from a query execution.
///
/// Each query in a batch produces a metadata item followed by zero or more
/// row items. When a new metadata item appears, it signals the start of the
/// next result set.
///
/// The stream **must** be consumed (or dropped) before sending another query
/// to the [`Client`](crate::Client).
///
/// # Convenience Methods
///
/// - [`into_row`](Self::into_row) — first row of the first result
/// - [`into_first_result`](Self::into_first_result) — all rows of the first result
/// - [`into_results`](Self::into_results) — all result sets collected in memory
/// - [`into_row_stream`](Self::into_row_stream) — a stream of rows, skipping metadata
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
    pub fn new(token_stream: BoxStream<'a, crate::Result<ServerMessage>>) -> Self {
        Self {
            token_stream: token_stream.peekable(),
            columns: None,
            result_set_index: None,
            info_messages: Vec::new(),
        }
    }

    /// Moves the stream forward until having result metadata, stream end or an
    /// error.
    pub async fn forward_to_metadata(&mut self) -> crate::Result<()> {
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

    /// Returns the column metadata for the current (or upcoming) result set.
    ///
    /// If the next item in the stream is metadata, it will be consumed and
    /// cached. Otherwise the previously cached columns are returned.
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

    /// Collects all result sets into memory as `Vec<Vec<Row>>`.
    ///
    /// Each inner `Vec<Row>` corresponds to one result set, in query order.
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

    /// Collects the rows of the first result set, dropping any subsequent
    /// result sets.
    pub async fn into_first_result(self) -> crate::Result<Vec<Row>> {
        let mut results = self.into_results().await?.into_iter();
        let rows = results.next().unwrap_or_default();

        Ok(rows)
    }

    /// Returns the first row of the first result set, or `None` if empty.
    pub async fn into_row(self) -> crate::Result<Option<Row>> {
        let mut results = self.into_first_result().await?.into_iter();

        Ok(results.next())
    }

    /// Converts this stream into a stream of [`Row`]s only, filtering out
    /// metadata and message items.
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
    pub fn metadata(columns: Arc<Vec<Column>>, result_index: usize) -> Self {
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
