use crate::{SqlValue, protocol::reader::ProtocolReader, temporal::DateTime2};

pub(crate) async fn decode<R>(src: &mut R, len: usize) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let rlen = src.read_u8().await?;

    let date = match rlen {
        0 => SqlValue::DateTime2(None),
        rlen => {
            let dt = DateTime2::decode(src, len, rlen as usize - 3).await?;
            SqlValue::DateTime2(Some(dt))
        }
    };

    Ok(date)
}
