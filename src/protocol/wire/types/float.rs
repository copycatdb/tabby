use crate::{SqlValue, error::Error, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R, type_len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let len = src.read_u8().await? as usize;

    let res = match (len, type_len) {
        (0, 4) => SqlValue::F32(None),
        (0, _) => SqlValue::F64(None),
        (4, _) => SqlValue::F32(Some(src.read_f32_le().await?)),
        (8, _) => SqlValue::F64(Some(src.read_f64_le().await?)),
        _ => {
            return Err(Error::Protocol(
                format!("floatn: length of {} is invalid", len).into(),
            ));
        }
    };

    Ok(res)
}
