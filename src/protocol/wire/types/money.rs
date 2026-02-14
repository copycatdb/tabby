use crate::{SqlValue, error::Error, protocol::numeric::Numeric, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R, len: u8) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let res = match len {
        0 => SqlValue::Numeric(None),
        4 => {
            let val = src.read_i32_le().await? as i128;
            SqlValue::Numeric(Some(Numeric::new_with_scale(val, 4)))
        }
        8 => {
            let high = src.read_i32_le().await? as i64;
            let low = src.read_u32_le().await? as i64;
            let val = ((high as i128) << 32) | (low as i128);
            SqlValue::Numeric(Some(Numeric::new_with_scale(val, 4)))
        }
        _ => {
            return Err(Error::Protocol(
                format!("money: length of {} is invalid", len).into(),
            ));
        }
    };

    Ok(res)
}
