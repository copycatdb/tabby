use super::{Encode, HEADER_BYTES, PacketHeader, PacketStatus, WireDecode};
use bytes::BytesMut;

#[derive(Debug)]
pub struct Frame {
    pub(crate) header: PacketHeader,
    pub(crate) payload: BytesMut,
}

impl Frame {
    pub(crate) fn new(header: PacketHeader, payload: BytesMut) -> Self {
        Self { header, payload }
    }

    pub(crate) fn is_last(&self) -> bool {
        self.header.status() == PacketStatus::EndOfMessage
    }

    pub(crate) fn into_parts(self) -> (PacketHeader, BytesMut) {
        (self.header, self.payload)
    }
}

impl Encode<BytesMut> for Frame {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let size = (self.payload.len() as u16 + HEADER_BYTES as u16).to_be_bytes();

        self.header.encode(dst)?;
        dst.extend(self.payload);

        dst[2] = size[0];
        dst[3] = size[1];

        Ok(())
    }
}

impl WireDecode<BytesMut> for Frame {
    fn decode(src: &mut BytesMut) -> crate::Result<Self> {
        Ok(Self {
            header: PacketHeader::decode(src)?,
            payload: src.split(),
        })
    }
}

impl Extend<u8> for Frame {
    fn extend<T: IntoIterator<Item = u8>>(&mut self, iter: T) {
        self.payload.extend(iter)
    }
}

impl<'a> Extend<&'a u8> for Frame {
    fn extend<T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.payload.extend(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::wire::header::PacketType;
    use bytes::BytesMut;

    #[test]
    fn frame_is_last() {
        let h = PacketHeader::batch(0); // NormalMessage
        let frame = Frame::new(h, BytesMut::new());
        assert!(!frame.is_last());

        let h2 = PacketHeader::pre_login(0); // EndOfMessage
        let frame2 = Frame::new(h2, BytesMut::new());
        assert!(frame2.is_last());
    }

    #[test]
    fn frame_into_parts() {
        let h = PacketHeader::batch(1);
        let payload = BytesMut::from(&b"hello"[..]);
        let frame = Frame::new(h, payload);
        let (header, data) = frame.into_parts();
        assert_eq!(PacketType::SQLBatch, header.r#type());
        assert_eq!(&b"hello"[..], &data[..]);
    }

    #[test]
    fn frame_extend_u8() {
        let h = PacketHeader::batch(0);
        let mut frame = Frame::new(h, BytesMut::new());
        frame.extend(vec![1u8, 2, 3]);
        assert_eq!(&[1, 2, 3], &frame.payload[..]);
    }

    #[test]
    fn frame_extend_ref() {
        let h = PacketHeader::batch(0);
        let mut frame = Frame::new(h, BytesMut::new());
        frame.extend(&[4u8, 5, 6]);
        assert_eq!(&[4, 5, 6], &frame.payload[..]);
    }

    #[test]
    fn frame_encode_decode_roundtrip() {
        let h = PacketHeader::pre_login(1);
        let payload = BytesMut::from(&b"test"[..]);
        let frame = Frame::new(h, payload);

        let mut buf = BytesMut::new();
        frame.encode(&mut buf).unwrap();

        // Decode: first 8 bytes are header, rest is payload
        let decoded = Frame::decode(&mut buf).unwrap();
        assert_eq!(PacketType::PreLogin, decoded.header.r#type());
        assert_eq!(&b"test"[..], &decoded.payload[..]);
    }
}
