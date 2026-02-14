use crate::{
    connection::Connection,
    protocol::pipeline::{ServerMessage, TokenStream},
};
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::TryStreamExt;
use std::fmt::Debug;

/// The result of a DML execution, containing the number of rows affected.
///
/// When executing multiple statements separated by `;`, each statement
/// produces a separate count accessible via [`rows_affected`](Self::rows_affected).
/// Use [`total`](Self::total) to get the sum.
///
/// Implements [`IntoIterator`] over the individual row counts.
#[derive(Debug)]
pub struct ExecuteResult {
    rows_affected: Vec<u64>,
}

impl<'a> ExecuteResult {
    pub(crate) async fn new<S: AsyncRead + AsyncWrite + Unpin + Send>(
        connection: &'a mut Connection<S>,
    ) -> crate::Result<Self> {
        let mut token_stream = TokenStream::new(connection).try_unfold();
        let mut rows_affected = Vec::new();

        while let Some(token) = token_stream.try_next().await? {
            match token {
                ServerMessage::DoneProc(done) if done.is_final() => (),
                ServerMessage::DoneProc(done) => rows_affected.push(done.rows()),
                ServerMessage::DoneInProc(done) => rows_affected.push(done.rows()),
                ServerMessage::Done(done) => rows_affected.push(done.rows()),
                _ => (),
            }
        }

        Ok(Self { rows_affected })
    }

    /// A slice of numbers of rows affected in the same order as the given
    /// queries.
    pub fn rows_affected(&self) -> &[u64] {
        self.rows_affected.as_slice()
    }

    /// Aggregates all resulting row counts into a sum.
    ///
    /// Consumes the `ExecuteResult`.
    pub fn total(self) -> u64 {
        self.rows_affected.into_iter().sum()
    }
}

impl IntoIterator for ExecuteResult {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows_affected.into_iter()
    }
}
