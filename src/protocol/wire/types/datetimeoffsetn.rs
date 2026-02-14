use crate::{SqlValue, protocol::reader::ProtocolReader, temporal::DateTimeOffset};

pub(crate) async fn decode<R>(src: &mut R, len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let rlen = src.read_u8().await?;

    let dto = match rlen {
        0 => SqlValue::DateTimeOffset(None),
        _ => {
            let dto = DateTimeOffset::decode(src, len, rlen - 5).await?;
            SqlValue::DateTimeOffset(Some(dto))
        }
    };

    Ok(dto)
}
