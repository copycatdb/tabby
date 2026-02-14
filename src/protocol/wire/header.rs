use super::{Encode, WireDecode};
use crate::Error;
use bytes::{Buf, BufMut, BytesMut};
use std::convert::TryFrom;

uint_enum! {
    /// the type of the packet [2.2.3.1.1]#[repr(u32)]
    #[repr(u8)]
    pub enum PacketType {
        SQLBatch = 1,
        /// unused
        PreTDSv7Login = 2,
        Rpc = 3,
        TabularResult = 4,
        AttentionSignal = 6,
        BulkLoad = 7,
        /// Federated Authentication Token
        Fat = 8,
        TransactionManagerReq = 14,
        TDSv7Login = 16,
        Sspi = 17,
        PreLogin = 18,
    }
}

uint_enum! {
    /// the message state [2.2.3.1.2]
    #[repr(u8)]
    pub enum PacketStatus {
        NormalMessage = 0,
        EndOfMessage = 1,
        /// [client to server ONLY] (EndOfMessage also required)
        IgnoreEvent = 3,
        /// [client to server ONLY] [>= TDSv7.1]
        ResetConnection = 0x08,
        /// [client to server ONLY] [>= TDSv7.3]
        ResetConnectionSkipTran = 0x10,
    }
}

/// packet header consisting of 8 bytes [2.2.3.1]
#[derive(Debug, Clone, Copy)]
pub struct PacketHeader {
    ty: PacketType,
    status: PacketStatus,
    /// [BE] the length of the packet (including the 8 header bytes)
    /// must match the negotiated size sending from client to server [since TDSv7.3] after login
    /// (only if not EndOfMessage)
    length: u16,
    /// [BE] the process ID on the server, for debugging purposes only
    spid: u16,
    /// packet id
    id: u8,
    /// currently unused
    window: u8,
}

impl PacketHeader {
    pub fn new(length: usize, id: u8) -> PacketHeader {
        assert!(length <= u16::MAX as usize);
        PacketHeader {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::ResetConnection,
            length: length as u16,
            spid: 0,
            id,
            window: 0,
        }
    }

    pub fn rpc(id: u8) -> Self {
        Self {
            ty: PacketType::Rpc,
            status: PacketStatus::NormalMessage,
            ..Self::new(0, id)
        }
    }

    pub fn pre_login(id: u8) -> Self {
        Self {
            ty: PacketType::PreLogin,
            status: PacketStatus::EndOfMessage,
            ..Self::new(0, id)
        }
    }

    pub fn login(id: u8) -> Self {
        Self {
            ty: PacketType::TDSv7Login,
            status: PacketStatus::EndOfMessage,
            ..Self::new(0, id)
        }
    }

    pub fn batch(id: u8) -> Self {
        Self {
            ty: PacketType::SQLBatch,
            status: PacketStatus::NormalMessage,
            ..Self::new(0, id)
        }
    }

    pub fn bulk_import(id: u8) -> Self {
        Self {
            ty: PacketType::BulkLoad,
            status: PacketStatus::NormalMessage,
            ..Self::new(0, id)
        }
    }

    pub fn set_status(&mut self, status: PacketStatus) {
        self.status = status;
    }

    pub fn set_type(&mut self, ty: PacketType) {
        self.ty = ty;
    }

    pub fn status(&self) -> PacketStatus {
        self.status
    }

    pub fn r#type(&self) -> PacketType {
        self.ty
    }

    pub fn length(&self) -> u16 {
        self.length
    }
}

impl<B> Encode<B> for PacketHeader
where
    B: BufMut,
{
    fn encode(self, dst: &mut B) -> crate::Result<()> {
        dst.put_u8(self.ty as u8);
        dst.put_u8(self.status as u8);
        dst.put_u16(self.length);
        dst.put_u16(self.spid);
        dst.put_u8(self.id);
        dst.put_u8(self.window);

        Ok(())
    }
}

impl WireDecode<BytesMut> for PacketHeader {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let raw_ty = src.get_u8();

        let ty = PacketType::try_from(raw_ty).map_err(|_| {
            Error::Protocol(format!("header: invalid packet type: {}", raw_ty).into())
        })?;

        let status = PacketStatus::try_from(src.get_u8())
            .map_err(|_| Error::Protocol("header: invalid packet status".into()))?;

        let header = PacketHeader {
            ty,
            status,
            length: src.get_u16(),
            spid: src.get_u16(),
            id: src.get_u8(),
            window: src.get_u8(),
        };

        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn packet_header_new() {
        let h = PacketHeader::new(100, 5);
        assert_eq!(100, h.length());
        assert_eq!(PacketStatus::ResetConnection, h.status());
    }

    #[test]
    fn packet_header_rpc() {
        let h = PacketHeader::rpc(1);
        assert_eq!(PacketType::Rpc, h.r#type());
        assert_eq!(PacketStatus::NormalMessage, h.status());
    }

    #[test]
    fn packet_header_pre_login() {
        let h = PacketHeader::pre_login(2);
        assert_eq!(PacketType::PreLogin, h.r#type());
        assert_eq!(PacketStatus::EndOfMessage, h.status());
    }

    #[test]
    fn packet_header_login() {
        let h = PacketHeader::login(3);
        assert_eq!(PacketType::TDSv7Login, h.r#type());
        assert_eq!(PacketStatus::EndOfMessage, h.status());
    }

    #[test]
    fn packet_header_batch() {
        let h = PacketHeader::batch(4);
        assert_eq!(PacketType::SQLBatch, h.r#type());
        assert_eq!(PacketStatus::NormalMessage, h.status());
    }

    #[test]
    fn packet_header_bulk_import() {
        let h = PacketHeader::bulk_import(5);
        assert_eq!(PacketType::BulkLoad, h.r#type());
        assert_eq!(PacketStatus::NormalMessage, h.status());
    }

    #[test]
    fn packet_header_set_status_and_type() {
        let mut h = PacketHeader::new(0, 0);
        h.set_status(PacketStatus::EndOfMessage);
        h.set_type(PacketType::Rpc);
        assert_eq!(PacketStatus::EndOfMessage, h.status());
        assert_eq!(PacketType::Rpc, h.r#type());
    }

    #[test]
    fn packet_header_encode_decode_roundtrip() {
        let original = PacketHeader::batch(42);
        let mut buf = BytesMut::new();
        original.encode(&mut buf).unwrap();
        // Manually set length for decode
        assert_eq!(8, buf.len());
        let decoded = PacketHeader::decode(&mut buf).unwrap();
        assert_eq!(original.r#type(), decoded.r#type());
        assert_eq!(original.status(), decoded.status());
    }

    #[test]
    fn packet_type_try_from_valid() {
        assert_eq!(Ok(PacketType::SQLBatch), PacketType::try_from(1u8));
        assert_eq!(Ok(PacketType::Rpc), PacketType::try_from(3u8));
        assert_eq!(Ok(PacketType::TabularResult), PacketType::try_from(4u8));
        assert_eq!(Ok(PacketType::PreLogin), PacketType::try_from(18u8));
    }

    #[test]
    fn packet_type_try_from_invalid() {
        assert!(PacketType::try_from(255u8).is_err());
    }

    #[test]
    fn packet_status_try_from() {
        assert_eq!(Ok(PacketStatus::NormalMessage), PacketStatus::try_from(0u8));
        assert_eq!(Ok(PacketStatus::EndOfMessage), PacketStatus::try_from(1u8));
        assert!(PacketStatus::try_from(255u8).is_err());
    }

    #[test]
    fn decode_invalid_packet_type() {
        let mut buf = BytesMut::from(&[255u8, 0, 0, 8, 0, 0, 0, 0][..]);
        let result = PacketHeader::decode(&mut buf);
        assert!(result.is_err());
    }
}
