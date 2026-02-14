use crate::{protocol::reader::ProtocolReader, protocol::wire::Encode};
use bytes::BytesMut;
use futures_util::io::AsyncReadExt;

#[derive(Debug)]
pub struct AuthChallenge(Vec<u8>);

impl AsRef<[u8]> for AuthChallenge {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AuthChallenge {
    #[cfg(any(windows, all(unix, feature = "integrated-auth-gssapi")))]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub(crate) async fn decode_async<R>(src: &mut R) -> crate::Result<Self>
    where
        R: ProtocolReader + Unpin,
    {
        let len = src.read_u16_le().await? as usize;
        let mut bytes = vec![0; len];
        src.read_exact(&mut bytes[0..len]).await?;

        Ok(Self(bytes))
    }
}

impl Encode<BytesMut> for AuthChallenge {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        dst.extend(self.0);
        Ok(())
    }
}
