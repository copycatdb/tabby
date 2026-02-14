use super::BaseColumnDescriptor;
use crate::{Error, ProtocolReader, protocol::wire::SqlValue};

#[derive(Debug)]
#[allow(dead_code)]
pub struct ReturnValue {
    pub param_ordinal: u16,
    pub param_name: String,
    /// return value of user defined function
    pub udf: bool,
    pub meta: BaseColumnDescriptor,
    pub value: SqlValue<'static>,
}

impl ReturnValue {
    pub(crate) async fn decode<R>(src: &mut R) -> crate::Result<Self>
    where
        R: ProtocolReader + Unpin,
    {
        let param_ordinal = src.read_u16_le().await?;
        let param_name = src.read_b_varchar().await?;

        let udf = match src.read_u8().await? {
            0x01 => false,
            0x02 => true,
            _ => return Err(Error::Protocol("ReturnValue: invalid status".into())),
        };

        let meta = BaseColumnDescriptor::decode(src).await?;
        let value = SqlValue::decode(src, &meta.ty).await?;

        let token = ReturnValue {
            param_ordinal,
            param_name,
            udf,
            meta,
            value,
        };

        Ok(token)
    }
}
