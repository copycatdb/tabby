use crate::{FixedLenType, SqlValue, protocol::reader::ProtocolReader};

pub(crate) async fn decode<R>(
    src: &mut R,
    r#type: &FixedLenType,
) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let data = match r#type {
        FixedLenType::Null => SqlValue::Bit(None),
        FixedLenType::Bit => SqlValue::Bit(Some(src.read_u8().await? != 0)),
        FixedLenType::Int1 => SqlValue::U8(Some(src.read_u8().await?)),
        FixedLenType::Int2 => SqlValue::I16(Some(src.read_i16_le().await?)),
        FixedLenType::Int4 => SqlValue::I32(Some(src.read_i32_le().await?)),
        FixedLenType::Int8 => SqlValue::I64(Some(src.read_i64_le().await?)),
        FixedLenType::Float4 => SqlValue::F32(Some(src.read_f32_le().await?)),
        FixedLenType::Float8 => SqlValue::F64(Some(src.read_f64_le().await?)),
        FixedLenType::Datetime => super::datetimen::decode(src, 8, 8).await?,
        FixedLenType::Datetime4 => super::datetimen::decode(src, 4, 8).await?,
        FixedLenType::Money4 => super::money::decode(src, 4).await?,
        FixedLenType::Money => super::money::decode(src, 8).await?,
    };

    Ok(data)
}
