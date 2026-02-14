use crate::protocol::wire::AuthChallenge;
use crate::{
    Error, MessageKind, ProtocolReader,
    connection::Connection,
    protocol::wire::{
        ColumnSchema, CompletionMessage, FeatureAckMessage, LoginResponse, OrderMessage,
        ReturnValue, RowMessage, ServerError, ServerNotice, SessionChange,
    },
};
use futures_util::{
    io::{AsyncRead, AsyncWrite},
    stream::{BoxStream, TryStreamExt},
};
use std::{convert::TryFrom, sync::Arc};
use tracing::{Level, event};

#[derive(Debug)]
#[allow(dead_code)]
pub enum ServerMessage {
    NewResultset(Arc<ColumnSchema<'static>>),
    Row(RowMessage<'static>),
    Done(CompletionMessage),
    DoneInProc(CompletionMessage),
    DoneProc(CompletionMessage),
    ReturnStatus(u32),
    ReturnValue(ReturnValue),
    Order(OrderMessage),
    EnvChange(SessionChange),
    Info(ServerNotice),
    LoginAck(LoginResponse),
    Sspi(AuthChallenge),
    FeatureExtAck(FeatureAckMessage),
    Error(ServerError),
}

pub(crate) struct TokenStream<'a, S: AsyncRead + AsyncWrite + Unpin + Send> {
    conn: &'a mut Connection<S>,
    last_error: Option<Error>,
}

impl<'a, S> TokenStream<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(crate) fn new(conn: &'a mut Connection<S>) -> Self {
        Self {
            conn,
            last_error: None,
        }
    }

    pub(crate) async fn flush_done(self) -> crate::Result<CompletionMessage> {
        let mut stream = self.try_unfold();
        let mut last_error = None;
        let mut routing = None;

        loop {
            match stream.try_next().await? {
                Some(ServerMessage::Error(error)) => {
                    if last_error.is_none() {
                        last_error = Some(error);
                    }
                }
                Some(ServerMessage::Done(token)) => match (last_error, routing) {
                    (Some(error), _) => return Err(Error::Server(error)),
                    (_, Some(routing)) => return Err(routing),
                    (_, _) => return Ok(token),
                },
                Some(ServerMessage::EnvChange(SessionChange::Routing { host, port })) => {
                    routing = Some(Error::Routing { host, port });
                }
                Some(_) => (),
                None => {
                    return Err(crate::Error::Protocol("Never got DONE token.".into()));
                }
            }
        }
    }

    #[cfg(any(windows, feature = "integrated-auth-gssapi"))]
    pub(crate) async fn flush_sspi(self) -> crate::Result<AuthChallenge> {
        let mut stream = self.try_unfold();
        let mut last_error = None;

        loop {
            match stream.try_next().await? {
                Some(ServerMessage::Error(error)) => {
                    if last_error.is_none() {
                        last_error = Some(error);
                    }
                }
                Some(ServerMessage::Sspi(token)) => return Ok(token),
                Some(_) => (),
                None => match last_error {
                    Some(err) => return Err(crate::Error::Server(err)),
                    None => {
                        return Err(crate::Error::Protocol("Never got SSPI token.".into()));
                    }
                },
            }
        }
    }

    async fn get_col_metadata(&mut self) -> crate::Result<ServerMessage> {
        let meta = Arc::new(ColumnSchema::decode(self.conn).await?);
        self.conn.context_mut().set_last_meta(meta.clone());

        event!(Level::TRACE, ?meta);

        Ok(ServerMessage::NewResultset(meta))
    }

    async fn get_row(&mut self) -> crate::Result<ServerMessage> {
        let return_value = RowMessage::decode(self.conn).await?;

        event!(Level::TRACE, message = ?return_value);
        Ok(ServerMessage::Row(return_value))
    }

    async fn get_nbc_row(&mut self) -> crate::Result<ServerMessage> {
        let return_value = RowMessage::decode_nbc(self.conn).await?;

        event!(Level::TRACE, message = ?return_value);
        Ok(ServerMessage::Row(return_value))
    }

    async fn get_return_value(&mut self) -> crate::Result<ServerMessage> {
        let return_value = ReturnValue::decode(self.conn).await?;
        event!(Level::TRACE, message = ?return_value);
        Ok(ServerMessage::ReturnValue(return_value))
    }

    async fn get_return_status(&mut self) -> crate::Result<ServerMessage> {
        let status = self.conn.read_u32_le().await?;
        Ok(ServerMessage::ReturnStatus(status))
    }

    async fn get_error(&mut self) -> crate::Result<ServerMessage> {
        let err = ServerError::decode(self.conn).await?;

        if self.last_error.is_none() {
            self.last_error = Some(Error::Server(err.clone()));
        }

        event!(Level::ERROR, message = %err.message, code = err.code);
        Ok(ServerMessage::Error(err))
    }

    async fn get_order(&mut self) -> crate::Result<ServerMessage> {
        let order = OrderMessage::decode(self.conn).await?;
        event!(Level::TRACE, message = ?order);
        Ok(ServerMessage::Order(order))
    }

    async fn get_done_value(&mut self) -> crate::Result<ServerMessage> {
        let done = CompletionMessage::decode(self.conn).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ServerMessage::Done(done))
    }

    async fn get_done_proc_value(&mut self) -> crate::Result<ServerMessage> {
        let done = CompletionMessage::decode(self.conn).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ServerMessage::DoneProc(done))
    }

    async fn get_done_in_proc_value(&mut self) -> crate::Result<ServerMessage> {
        let done = CompletionMessage::decode(self.conn).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ServerMessage::DoneInProc(done))
    }

    async fn get_env_change(&mut self) -> crate::Result<ServerMessage> {
        let change = SessionChange::decode(self.conn).await?;

        match change {
            SessionChange::PacketSize(new_size, _) => {
                self.conn.context_mut().set_packet_size(new_size);
            }
            SessionChange::BeginTransaction(desc) => {
                self.conn.context_mut().set_transaction_descriptor(desc);
            }
            SessionChange::CommitTransaction
            | SessionChange::RollbackTransaction
            | SessionChange::DefectTransaction => {
                self.conn.context_mut().set_transaction_descriptor([0; 8]);
            }
            _ => (),
        }

        event!(Level::INFO, "{}", change);

        Ok(ServerMessage::EnvChange(change))
    }

    async fn get_info(&mut self) -> crate::Result<ServerMessage> {
        let info = ServerNotice::decode(self.conn).await?;
        event!(Level::INFO, "{}", info.message);
        Ok(ServerMessage::Info(info))
    }

    async fn get_login_ack(&mut self) -> crate::Result<ServerMessage> {
        let ack = LoginResponse::decode(self.conn).await?;
        event!(Level::INFO, "{} version {}", ack.prog_name, ack.version);
        Ok(ServerMessage::LoginAck(ack))
    }

    async fn get_feature_ext_ack(&mut self) -> crate::Result<ServerMessage> {
        let ack = FeatureAckMessage::decode(self.conn).await?;
        event!(
            Level::INFO,
            "FeatureExtAck with {} features",
            ack.features.len()
        );
        Ok(ServerMessage::FeatureExtAck(ack))
    }

    async fn get_sspi(&mut self) -> crate::Result<ServerMessage> {
        let sspi = AuthChallenge::decode_async(self.conn).await?;
        event!(Level::TRACE, "SSPI response");
        Ok(ServerMessage::Sspi(sspi))
    }

    pub fn try_unfold(self) -> BoxStream<'a, crate::Result<ServerMessage>> {
        let stream = futures_util::stream::try_unfold(self, |mut this| async move {
            if this.conn.is_eof() {
                match this.last_error {
                    None => return Ok(None),
                    Some(error) => return Err(error),
                }
            }

            let ty_byte = this.conn.read_u8().await?;

            let ty = MessageKind::try_from(ty_byte)
                .map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;

            let token = match ty {
                MessageKind::ReturnStatus => this.get_return_status().await?,
                MessageKind::ColMetaData => this.get_col_metadata().await?,
                MessageKind::Row => this.get_row().await?,
                MessageKind::NbcRow => this.get_nbc_row().await?,
                MessageKind::Done => this.get_done_value().await?,
                MessageKind::DoneProc => this.get_done_proc_value().await?,
                MessageKind::DoneInProc => this.get_done_in_proc_value().await?,
                MessageKind::ReturnValue => this.get_return_value().await?,
                MessageKind::Error => this.get_error().await?,
                MessageKind::Order => this.get_order().await?,
                MessageKind::EnvChange => this.get_env_change().await?,
                MessageKind::Info => this.get_info().await?,
                MessageKind::LoginAck => this.get_login_ack().await?,
                MessageKind::Sspi => this.get_sspi().await?,
                MessageKind::FeatureExtAck => this.get_feature_ext_ack().await?,
                _ => panic!("Token {:?} unimplemented!", ty),
            };

            Ok(Some((token, this)))
        });

        Box::pin(stream)
    }
}
