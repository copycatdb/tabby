use crate::{
    SqlValue,
    error::Error,
    protocol::reader::ProtocolReader,
    temporal::{DateTime, SmallDateTime},
};

pub(crate) async fn decode<R>(src: &mut R, rlen: u8, len: u8) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let datetime = match (rlen, len) {
        (0, 4) => SqlValue::SmallDateTime(None),
        (0, 8) => SqlValue::DateTime(None),
        (4, _) => SqlValue::SmallDateTime(Some(SmallDateTime::decode(src).await?)),
        (8, _) => SqlValue::DateTime(Some(DateTime::decode(src).await?)),
        _ => {
            return Err(Error::Protocol(
                format!("datetimen: length of {} is invalid", len).into(),
            ));
        }
    };

    Ok(datetime)
}
