use crate::{SqlValue, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let ptr_len = src.read_u8().await? as usize;

    if ptr_len == 0 {
        return Ok(SqlValue::Binary(None));
    }

    for _ in 0..ptr_len {
        src.read_u8().await?;
    }

    src.read_i32_le().await?; // days
    src.read_u32_le().await?; // second fractions

    let len = src.read_u32_le().await? as usize;
    let mut buf = Vec::with_capacity(len);

    for _ in 0..len {
        buf.push(src.read_u8().await?);
    }

    Ok(SqlValue::Binary(Some(buf.into())))
}
