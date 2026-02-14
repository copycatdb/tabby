use crate::{SqlValue, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R, type_len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let recv_len = src.read_u8().await? as usize;

    let res = match (recv_len, type_len) {
        (0, 1) => SqlValue::U8(None),
        (0, 2) => SqlValue::I16(None),
        (0, 4) => SqlValue::I32(None),
        (0, _) => SqlValue::I64(None),
        (1, _) => SqlValue::U8(Some(src.read_u8().await?)),
        (2, _) => SqlValue::I16(Some(src.read_i16_le().await?)),
        (4, _) => SqlValue::I32(Some(src.read_i32_le().await?)),
        (8, _) => SqlValue::I64(Some(src.read_i64_le().await?)),
        _ => unimplemented!(),
    };

    Ok(res)
}
