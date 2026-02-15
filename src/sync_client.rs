//! Synchronous TDS client — the blocking equivalent of `Client`.
//!
//! `SyncClient` wraps a `SyncConnection` and exposes the batch-oriented API
//! used by furball (ODBC) and whiskers (DB-API 2.0).

use crate::MessageKind;
use crate::connection::{BatchFetchResult, Config};
use crate::protocol::wire::{
    ColumnAttribute, ColumnSchema, CompletionMessage, OrderMessage, PacketHeader, RawQuery,
    ReturnValue, ServerError, ServerNotice, SessionChange,
};
use crate::row::Column;
use crate::row_writer::RowWriter;
use crate::sync_connection::SyncConnection;
use crate::sync_decode_direct::{decode_nbc_row_into_sync, decode_row_into_sync};
use crate::sync_reader::SyncProtocolReader;
use std::io::{Read, Write};
use std::sync::Arc;

/// A synchronous TDS client. No tokio, no async, no futures.
///
/// Provides the same streaming batch API as the async `Client`, but using
/// blocking I/O over `std::net::TcpStream` (or any `Read + Write` stream).
#[derive(Debug)]
pub struct SyncClient<S: Read + Write> {
    connection: SyncConnection<S>,
}

impl<S: Read + Write> SyncClient<S> {
    /// Connect to SQL Server over a blocking stream.
    ///
    /// # Example
    /// ```no_run
    /// use std::net::TcpStream;
    /// use tabby::{AuthMethod, Config, SyncClient};
    ///
    /// let mut config = Config::new();
    /// config.host("localhost");
    /// config.port(1433);
    /// config.authentication(AuthMethod::sql_server("sa", "password"));
    /// config.trust_cert();
    ///
    /// let tcp = TcpStream::connect(config.get_addr()).unwrap();
    /// tcp.set_nodelay(true).unwrap();
    /// let mut client = SyncClient::connect(config, tcp).unwrap();
    /// ```
    pub fn connect(config: Config, stream: S) -> crate::Result<Self> {
        Ok(SyncClient {
            connection: SyncConnection::connect(config, stream)?,
        })
    }

    /// Send a raw SQL batch and read tokens until the first ColMetaData,
    /// returning the column metadata.
    pub fn batch_start(&mut self, query: &str) -> crate::Result<Vec<Column>> {
        let mut dummy = 0u64;
        self.batch_start_with_rowcount(query, &mut dummy)
    }

    /// Send a raw SQL batch and read tokens until the first ColMetaData.
    /// The `rows_affected` out-param captures DML row counts.
    pub fn batch_start_with_rowcount(
        &mut self,
        query: &str,
        rows_affected: &mut u64,
    ) -> crate::Result<Vec<Column>> {
        self.connection.flush_stream()?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req)?;

        *rows_affected = 0;
        self.read_until_metadata(rows_affected)
    }

    fn read_until_metadata(&mut self, rows_affected: &mut u64) -> crate::Result<Vec<Column>> {
        loop {
            if self.connection.is_eof() {
                return Ok(Vec::new());
            }

            let ty_byte = SyncProtocolReader::read_u8(&mut self.connection)?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta = Arc::new(ColumnSchema::decode_sync(&mut self.connection)?);
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
                    let done = CompletionMessage::decode_sync(&mut self.connection)?;
                    *rows_affected += done.rows();
                    if self.connection.is_eof() {
                        return Ok(Vec::new());
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le()?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode_sync(&mut self.connection)?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode_sync(&mut self.connection)?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let _info = ServerNotice::decode_sync(&mut self.connection)?;
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode_sync(&mut self.connection)?;
                    self.connection.apply_env_change(change);
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode_sync(&mut self.connection)?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("unexpected token {:?} in batch_start", ty).into(),
                    ));
                }
            }
        }
    }

    /// Read the next token after `batch_start`. Returns row/done/more-results.
    pub fn batch_fetch_row<W: RowWriter>(
        &mut self,
        writer: &mut W,
        string_buf: &mut String,
        bytes_buf: &mut Vec<u8>,
    ) -> crate::Result<BatchFetchResult> {
        loop {
            if self.connection.is_eof() {
                return Ok(BatchFetchResult::Done(0));
            }

            let ty_byte = SyncProtocolReader::read_u8(&mut self.connection)?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::Row => {
                    decode_row_into_sync(&mut self.connection, writer, string_buf, bytes_buf)?;
                    return Ok(BatchFetchResult::Row);
                }
                MessageKind::NbcRow => {
                    decode_nbc_row_into_sync(&mut self.connection, writer, string_buf, bytes_buf)?;
                    return Ok(BatchFetchResult::Row);
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let done = CompletionMessage::decode_sync(&mut self.connection)?;
                    let rows = done.rows();
                    if done.has_more() && !self.connection.is_eof() {
                        return Ok(BatchFetchResult::MoreResults);
                    }
                    return Ok(BatchFetchResult::Done(rows));
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le()?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode_sync(&mut self.connection)?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode_sync(&mut self.connection)?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let info = ServerNotice::decode_sync(&mut self.connection)?;
                    writer.on_info(info.number, &info.message);
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode_sync(&mut self.connection)?;
                    self.connection.apply_env_change(change);
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode_sync(&mut self.connection)?;
                }
                MessageKind::ColMetaData => {
                    let meta = Arc::new(ColumnSchema::decode_sync(&mut self.connection)?);
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

    /// Read next result set's column metadata after `MoreResults`.
    pub fn batch_fetch_metadata(&mut self) -> crate::Result<Vec<Column>> {
        let mut dummy = 0u64;
        self.read_until_metadata(&mut dummy)
    }

    /// Drain remaining tokens until EOF.
    pub fn batch_drain(&mut self) -> crate::Result<()> {
        let mut dummy_str = String::new();
        let mut dummy_bytes = Vec::new();
        let mut dummy_writer = crate::row_writer::SqlValueWriter::with_capacity(0);
        loop {
            match self.batch_fetch_row(&mut dummy_writer, &mut dummy_str, &mut dummy_bytes)? {
                BatchFetchResult::Row | BatchFetchResult::MoreResults => continue,
                BatchFetchResult::Done(_) => return Ok(()),
            }
        }
    }

    /// Execute raw SQL and decode all rows into a single RowWriter.
    pub fn batch_into<W: RowWriter>(&mut self, query: &str, writer: &mut W) -> crate::Result<()> {
        self.connection.flush_stream()?;

        let req = RawQuery::new(query, self.connection.context().transaction_descriptor());
        let id = self.connection.context_mut().next_packet_id();
        self.connection.send(PacketHeader::batch(id), req)?;

        let mut string_buf = String::with_capacity(4096);
        let mut bytes_buf = Vec::with_capacity(4096);

        loop {
            if self.connection.is_eof() {
                break;
            }

            let ty_byte = SyncProtocolReader::read_u8(&mut self.connection)?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(format!("invalid token type {:x}", ty_byte).into())
            })?;

            match ty {
                MessageKind::ColMetaData => {
                    let meta = Arc::new(ColumnSchema::decode_sync(&mut self.connection)?);
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
                    decode_row_into_sync(
                        &mut self.connection,
                        writer,
                        &mut string_buf,
                        &mut bytes_buf,
                    )?;
                    writer.on_row_done();
                }
                MessageKind::NbcRow => {
                    decode_nbc_row_into_sync(
                        &mut self.connection,
                        writer,
                        &mut string_buf,
                        &mut bytes_buf,
                    )?;
                    writer.on_row_done();
                }
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let done = CompletionMessage::decode_sync(&mut self.connection)?;
                    writer.on_done(done.rows());
                    if done.has_more() && !self.connection.is_eof() {
                        writer.on_result_set_end();
                    }
                    if self.connection.is_eof() {
                        break;
                    }
                }
                MessageKind::ReturnStatus => {
                    let _status = self.connection.read_u32_le()?;
                }
                MessageKind::ReturnValue => {
                    let _rv = ReturnValue::decode_sync(&mut self.connection)?;
                }
                MessageKind::Error => {
                    let err = ServerError::decode_sync(&mut self.connection)?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::Info => {
                    let info = ServerNotice::decode_sync(&mut self.connection)?;
                    writer.on_info(info.number, &info.message);
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode_sync(&mut self.connection)?;
                    self.connection.apply_env_change(change);
                }
                MessageKind::Order => {
                    let _order = OrderMessage::decode_sync(&mut self.connection)?;
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
}
