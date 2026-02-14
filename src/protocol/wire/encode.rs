use super::{Frame, FrameCodec};
use asynchronous_codec::Encoder;
use bytes::{BufMut, BytesMut};

pub trait Encode<B: BufMut> {
    fn encode(self, dst: &mut B) -> crate::Result<()>;
}

impl Encoder for FrameCodec {
    type Item<'a> = Frame;
    type Error = crate::Error;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst)?;
        Ok(())
    }
}
