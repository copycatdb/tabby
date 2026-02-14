use super::{Frame, FrameCodec, HEADER_BYTES, PacketHeader};
use crate::Error;
use asynchronous_codec::Decoder;
use bytes::{Buf, BytesMut};
use tracing::{Level, event};

pub trait WireDecode<B: Buf> {
    fn decode(src: &mut B) -> crate::Result<Self>
    where
        Self: Sized;
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_BYTES {
            src.reserve(HEADER_BYTES);
            return Ok(None);
        }

        let header = PacketHeader::decode(&mut BytesMut::from(&src[0..HEADER_BYTES]))?;
        let length = header.length() as usize;

        if src.len() < length {
            src.reserve(length);
            return Ok(None);
        }

        event!(
            Level::TRACE,
            "Reading a {:?} ({} bytes)",
            header.r#type(),
            length,
        );

        let header = PacketHeader::decode(src)?;

        if length < HEADER_BYTES {
            return Err(Error::Protocol("Invalid packet length".into()));
        }

        let payload = src.split_to(length - HEADER_BYTES);

        Ok(Some(Frame::new(header, payload)))
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(buf)? {
            Some(frame) => Ok(Some(frame)),
            None => {
                if buf.is_empty() {
                    Ok(None)
                } else {
                    Err(std::io::Error::other("bytes remaining on stream").into())
                }
            }
        }
    }
}
