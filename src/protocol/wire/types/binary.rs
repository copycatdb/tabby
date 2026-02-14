use std::borrow::Cow;

use crate::{SqlValue, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R, len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let data = super::plp::decode(src, len).await?.map(Cow::from);

    Ok(SqlValue::Binary(data))
}
