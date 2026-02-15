//! Synchronous TDS connection transport.
//!
//! `SyncConnection` handles TDS packet framing over a blocking `Read + Write`
//! stream (typically `std::net::TcpStream` or a TLS wrapper). It reads
//! packets, reassembles their payloads, and exposes a `Read` impl that
//! presents the payload as a contiguous byte stream to the token decoder.

use crate::connection::{AuthMethod, Config};
use crate::protocol::wire::{
    Encode, Handshake, LoginRequest, PacketHeader, PacketStatus, WireDecode,
};
use crate::protocol::{Context, EncryptionLevel, HEADER_BYTES};
use crate::sync_reader::SyncProtocolReader;
use bytes::BytesMut;
use std::cmp;
use std::io::{self, Read, Write};
use tracing::{Level, event};

/// A synchronous TDS connection over a blocking stream.
///
/// Handles TDS packet framing: each TDS packet has an 8-byte header followed
/// by payload. This struct reads full packets and buffers their payloads,
/// presenting a continuous `Read` interface to the token decoder.
pub struct SyncConnection<S: Read + Write> {
    stream: S,
    context: Context,
    /// Reassembled payload bytes from TDS packets.
    buf: Vec<u8>,
    buf_pos: usize,
    /// True when the last packet had EndOfMessage status.
    flushed: bool,
}

impl<S: Read + Write> std::fmt::Debug for SyncConnection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncConnection")
            .field("buf_len", &(self.buf.len() - self.buf_pos))
            .field("flushed", &self.flushed)
            .finish()
    }
}

impl<S: Read + Write> SyncConnection<S> {
    /// Connect to SQL Server: PRELOGIN → TLS handshake → LOGIN7 → read response.
    pub fn connect(config: Config, stream: S) -> crate::Result<Self> {
        let context = {
            let mut ctx = Context::new();
            ctx.set_spn(config.get_host(), config.get_port());
            ctx
        };

        let mut conn = Self {
            stream,
            context,
            buf: Vec::with_capacity(8192),
            buf_pos: 0,
            flushed: false,
        };

        let fed_auth_required = matches!(config.auth, AuthMethod::AADToken(_));

        // PRELOGIN
        let prelogin = conn.prelogin(config.encryption, fed_auth_required)?;
        let encryption = prelogin.negotiated_encryption(config.encryption);

        // TLS handshake (currently only NotSupported path — TLS requires feature work)
        conn = conn.tls_handshake(&config, encryption)?;

        // LOGIN7
        conn = conn.login(
            config.auth,
            encryption,
            config.database,
            config.host,
            config.application_name,
            config.readonly,
        )?;

        // Flush login response
        conn.flush_done()?;

        Ok(conn)
    }

    /// Send a PRELOGIN message and read the server's PRELOGIN response.
    fn prelogin(
        &mut self,
        encryption: EncryptionLevel,
        fed_auth_required: bool,
    ) -> crate::Result<Handshake> {
        let mut msg = Handshake::new();
        msg.encryption = encryption;
        msg.fed_auth_required = fed_auth_required;

        let id = self.context.next_packet_id();
        self.send(PacketHeader::pre_login(id), msg)?;

        // Read the response — collect all packets into a BytesMut
        let mut response_buf = BytesMut::new();
        loop {
            let (header, payload) = self.read_raw_packet()?;
            let is_last = header.status() == PacketStatus::EndOfMessage;
            response_buf.extend_from_slice(&payload);
            if is_last {
                break;
            }
        }
        let response = Handshake::decode(&mut response_buf)?;
        Ok(response)
    }

    /// TLS handshake. Currently only supports NotSupported (no TLS).
    #[cfg(not(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    )))]
    fn tls_handshake(self, _config: &Config, _encryption: EncryptionLevel) -> crate::Result<Self> {
        event!(
            Level::WARN,
            "TLS encryption is not enabled. All traffic including the login credentials are not encrypted."
        );
        Ok(self)
    }

    #[cfg(any(
        feature = "rustls",
        feature = "native-tls",
        feature = "vendored-openssl"
    ))]
    fn tls_handshake(self, _config: &Config, encryption: EncryptionLevel) -> crate::Result<Self> {
        if encryption != EncryptionLevel::NotSupported {
            // TODO: Implement sync TLS handshake with native-tls
            // For now, return an error if TLS is required
            return Err(crate::Error::Tls(
                "Sync TLS handshake not yet implemented. Use trust_cert() with a non-TLS connection or disable TLS features.".into(),
            ));
        }
        Ok(self)
    }

    /// Send LOGIN7 message.
    fn login(
        mut self,
        auth: AuthMethod,
        encryption: EncryptionLevel,
        db: Option<String>,
        server_name: Option<String>,
        application_name: Option<String>,
        readonly: bool,
    ) -> crate::Result<Self> {
        let mut login_message = LoginRequest::new();

        if let Some(db) = db {
            login_message.db_name(db);
        }
        if let Some(server_name) = server_name {
            login_message.server_name(server_name);
        }
        if let Some(app_name) = application_name {
            login_message.app_name(app_name);
        }
        login_message.readonly(readonly);

        match auth {
            AuthMethod::None => {
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::SqlServer(auth) => {
                login_message.user_name(auth.user());
                login_message.password(auth.password());
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
            AuthMethod::AADToken(token) => {
                login_message.aad_token(token, false, None);
                let id = self.context.next_packet_id();
                self.send(PacketHeader::login(id), login_message)?;
                self = self.post_login_encryption(encryption);
            }
        }

        Ok(self)
    }

    fn post_login_encryption(self, _encryption: EncryptionLevel) -> Self {
        // For sync, we don't switch TLS off after login (we don't have TLS yet)
        self
    }

    /// Flush the token stream until a DONE token with no more-results.
    fn flush_done(&mut self) -> crate::Result<()> {
        use crate::MessageKind;
        use crate::protocol::wire::{CompletionMessage, SessionChange};

        loop {
            if self.is_eof() {
                return Ok(());
            }

            let ty_byte = SyncProtocolReader::read_u8(self)?;
            let ty = MessageKind::try_from(ty_byte).map_err(|_| {
                crate::Error::Protocol(
                    format!("flush_done: invalid token type {:x}", ty_byte).into(),
                )
            })?;

            match ty {
                MessageKind::Done | MessageKind::DoneProc | MessageKind::DoneInProc => {
                    let _done = CompletionMessage::decode_sync(self)?;
                    if self.is_eof() {
                        return Ok(());
                    }
                }
                MessageKind::EnvChange => {
                    let change = SessionChange::decode_sync(self)?;
                    self.apply_env_change(change);
                }
                MessageKind::Info => {
                    let _info = crate::protocol::wire::ServerNotice::decode_sync(self)?;
                }
                MessageKind::LoginAck => {
                    let _ack = crate::protocol::wire::LoginResponse::decode_sync(self)?;
                }
                MessageKind::FeatureExtAck => {
                    let _ack = crate::protocol::wire::FeatureAckMessage::decode_sync(self)?;
                }
                MessageKind::Error => {
                    let err = crate::protocol::wire::ServerError::decode_sync(self)?;
                    return Err(crate::Error::Server(err));
                }
                MessageKind::ReturnStatus => {
                    let _status = SyncProtocolReader::read_u32_le(self)?;
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("flush_done: unexpected token {:?}", ty).into(),
                    ));
                }
            }
        }
    }

    pub(crate) fn apply_env_change(&mut self, change: crate::protocol::wire::SessionChange) {
        use crate::protocol::wire::SessionChange;
        match change {
            SessionChange::PacketSize(new_size, _) => {
                self.context.set_packet_size(new_size);
            }
            SessionChange::BeginTransaction(desc) => {
                self.context.set_transaction_descriptor(desc);
            }
            SessionChange::CommitTransaction
            | SessionChange::RollbackTransaction
            | SessionChange::DefectTransaction => {
                self.context.set_transaction_descriptor([0; 8]);
            }
            SessionChange::Routing { host, port } => {
                // For now, just log it. Full redirect support can come later.
                event!(Level::WARN, "Server requested routing to {}:{}", host, port);
            }
            _ => (),
        }
    }

    /// Send a TDS message, splitting into packets if needed.
    pub fn send<E: Sized + Encode<BytesMut>>(
        &mut self,
        mut header: PacketHeader,
        item: E,
    ) -> crate::Result<()> {
        self.flushed = false;
        let packet_size = (self.context.packet_size() as usize) - HEADER_BYTES;

        let mut payload = BytesMut::new();
        item.encode(&mut payload)?;

        while !payload.is_empty() {
            let writable = cmp::min(payload.len(), packet_size);
            let split_payload = payload.split_to(writable);

            if payload.is_empty() {
                header.set_status(PacketStatus::EndOfMessage);
            } else {
                header.set_status(PacketStatus::NormalMessage);
            }

            self.write_raw_packet(header, &split_payload)?;
        }

        self.stream.flush()?;
        Ok(())
    }

    /// Write a single TDS packet (header + payload) to the wire.
    fn write_raw_packet(&mut self, header: PacketHeader, payload: &[u8]) -> crate::Result<()> {
        let length = (payload.len() + HEADER_BYTES) as u16;
        let mut header_buf = BytesMut::with_capacity(HEADER_BYTES);
        header.encode(&mut header_buf)?;
        // Fix length field (bytes 2-3, big-endian)
        let len_bytes = length.to_be_bytes();
        header_buf[2] = len_bytes[0];
        header_buf[3] = len_bytes[1];

        self.stream.write_all(&header_buf)?;
        self.stream.write_all(payload)?;
        Ok(())
    }

    /// Read a raw TDS packet from the wire: header + payload.
    fn read_raw_packet(&mut self) -> crate::Result<(PacketHeader, Vec<u8>)> {
        let mut header_bytes = [0u8; HEADER_BYTES];
        self.stream.read_exact(&mut header_bytes)?;

        let mut hdr_buf = BytesMut::from(&header_bytes[..]);
        let header = PacketHeader::decode(&mut hdr_buf)?;

        let payload_len = header.length() as usize;
        if payload_len < HEADER_BYTES {
            return Err(crate::Error::Protocol("Invalid packet length".into()));
        }
        let data_len = payload_len - HEADER_BYTES;
        let mut payload = vec![0u8; data_len];
        if data_len > 0 {
            self.stream.read_exact(&mut payload)?;
        }

        Ok((header, payload))
    }

    /// Ensure the internal buffer has at least `needed` bytes by reading
    /// more TDS packets from the wire.
    fn fill_buf(&mut self, needed: usize) -> io::Result<()> {
        let available = self.buf.len() - self.buf_pos;
        if available >= needed {
            return Ok(());
        }

        // Compact the buffer
        if self.buf_pos > 0 {
            self.buf.drain(..self.buf_pos);
            self.buf_pos = 0;
        }

        while self.buf.len() < needed {
            let (header, payload) = self.read_raw_packet().map_err(|e| match e {
                crate::Error::Io { kind, message } => io::Error::new(kind, message),
                other => io::Error::new(io::ErrorKind::Other, other.to_string()),
            })?;

            self.flushed = header.status() == PacketStatus::EndOfMessage;
            self.buf.extend_from_slice(&payload);

            event!(
                Level::TRACE,
                "Sync: read packet ({} payload bytes, flushed={})",
                payload.len(),
                self.flushed,
            );
        }

        Ok(())
    }

    /// True when all data from the current message has been consumed.
    pub fn is_eof(&self) -> bool {
        self.flushed && (self.buf.len() - self.buf_pos) == 0
    }

    /// Flush/discard remaining data in the current message.
    pub fn flush_stream(&mut self) -> crate::Result<()> {
        self.buf.clear();
        self.buf_pos = 0;

        if self.flushed {
            return Ok(());
        }

        // Read and discard remaining packets
        loop {
            let (header, _payload) = self.read_raw_packet()?;
            if header.status() == PacketStatus::EndOfMessage {
                self.flushed = true;
                break;
            }
        }

        Ok(())
    }
}

impl<S: Read + Write> Read for SyncConnection<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let needed = buf.len();
        self.fill_buf(needed)?;

        let available = self.buf.len() - self.buf_pos;
        if available < needed {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "No more packets in the wire",
            ));
        }

        buf.copy_from_slice(&self.buf[self.buf_pos..self.buf_pos + needed]);
        self.buf_pos += needed;
        Ok(needed)
    }
}

impl<S: Read + Write> SyncProtocolReader for SyncConnection<S> {
    fn context(&self) -> &Context {
        &self.context
    }

    fn context_mut(&mut self) -> &mut Context {
        &mut self.context
    }
}
