use crate::Error;

use crate::{SqlValue, protocol::reader::ProtocolReader, temporal::Date};

pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let len = src.read_u8().await?;

    let res = match len {
        0 => SqlValue::Date(None),
        3 => SqlValue::Date(Some(Date::decode(src).await?)),
        _ => {
            return Err(Error::Protocol(
                format!("daten: length of {} is invalid", len).into(),
            ));
        }
    };

    Ok(res)
}
