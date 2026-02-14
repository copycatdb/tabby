use crate::{SqlValue, protocol::reader::ProtocolReader, temporal::Time};

pub(crate) async fn decode<R>(src: &mut R, len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let rlen = src.read_u8().await?;

    let time = match rlen {
        0 => SqlValue::Time(None),
        _ => {
            let time = Time::decode(src, len, rlen as usize).await?;
            SqlValue::Time(Some(time))
        }
    };

    Ok(time)
}
