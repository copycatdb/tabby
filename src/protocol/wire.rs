mod bulk_import;
mod data_type;
mod decode;
pub(crate) mod decode_direct;
mod encode;
mod frame;
pub(crate) mod guid;
mod handshake;
mod header;
mod iterator_ext;
mod login;
mod messages;
mod procedure_call;
mod raw_query;
pub(crate) mod types;

pub use bulk_import::*;
use bytes::BytesMut;
pub use data_type::*;
pub use decode::*;
pub(crate) use encode::*;
pub use frame::*;
use futures_util::stream::{Stream, TryStreamExt};
pub use handshake::*;
pub use header::*;
pub(crate) use iterator_ext::*;
pub use login::*;
pub use messages::*;
pub use procedure_call::*;
pub use raw_query::*;
pub use types::*;

const HEADER_BYTES: usize = 8;
const ALL_HEADERS_LEN_TX: usize = 22;

#[derive(Debug)]
#[repr(u16)]
#[allow(dead_code)]
enum AllHeaderTy {
    QueryDescriptor = 1,
    TransactionDescriptor = 2,
    TraceActivity = 3,
}

pub struct FrameCodec;

pub(crate) async fn collect_from<S, T>(stream: &mut S) -> crate::Result<T>
where
    T: WireDecode<BytesMut> + Sized,
    S: Stream<Item = crate::Result<Frame>> + Unpin,
{
    let mut buf = BytesMut::new();

    while let Some(packet) = stream.try_next().await? {
        let is_last = packet.is_last();
        let (_, payload) = packet.into_parts();
        buf.extend(payload);

        if is_last {
            break;
        }
    }

    T::decode(&mut buf)
}
