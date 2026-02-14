use crate::{SqlValue, error::Error, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let recv_len = src.read_u8().await? as usize;

    let res = match recv_len {
        0 => SqlValue::Bit(None),
        1 => SqlValue::Bit(Some(src.read_u8().await? > 0)),
        v => {
            return Err(Error::Protocol(
                format!("bitn: length of {} is invalid", v).into(),
            ));
        }
    };

    Ok(res)
}
