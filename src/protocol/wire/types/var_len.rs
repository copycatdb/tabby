use crate::{
    SqlValue, VarLenType, protocol::reader::ProtocolReader, protocol::wire::VarLenDescriptor,
};

pub(crate) async fn decode<R>(
    src: &mut R,
    ctx: &VarLenDescriptor,
) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    use VarLenType::*;

    let ty = ctx.r#type();
    let len = ctx.len();
    let collation = ctx.collation();

    let res = match ty {
        Bitn => super::bit::decode(src).await?,
        Intn => super::int::decode(src, len).await?,
        Floatn => super::float::decode(src, len).await?,
        Guid => super::guid::decode(src).await?,
        BigChar | BigVarChar | NChar | NVarchar => {
            SqlValue::String(super::string::decode(src, ty, len, collation).await?)
        }
        Money => {
            let len = src.read_u8().await?;
            super::money::decode(src, len).await?
        }
        Datetimen => {
            let rlen = src.read_u8().await?;
            super::datetimen::decode(src, rlen, len as u8).await?
        }
        Daten => super::date::decode(src).await?,
        Timen => super::time::decode(src, len).await?,
        Datetime2 => super::datetime2::decode(src, len).await?,
        DatetimeOffsetn => super::datetimeoffsetn::decode(src, len).await?,
        BigBinary | BigVarBin => super::binary::decode(src, len).await?,
        Text => super::text::decode(src, collation).await?,
        NText => super::text::decode(src, None).await?,
        Image => super::image::decode(src).await?,
        SSVariant => decode_sql_variant(src).await?,
        Udt => {
            // UDT: always uses PLP encoding regardless of max_len
            super::binary::decode(src, 0xffff).await?
        }
        t => unimplemented!("{:?}", t),
    };

    Ok(res)
}

/// Decode a sql_variant value from the wire.
/// Wire format: u32 total_len, then base_type byte, then type-specific metadata + data.
async fn decode_sql_variant<R>(src: &mut R) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    use futures_util::io::AsyncReadExt;

    let total_len = src.read_u32_le().await? as usize;
    if total_len == 0 {
        return Ok(SqlValue::String(None));
    }

    let base_type = src.read_u8().await?;
    let prop_bytes = src.read_u8().await?;

    // Read property bytes
    let mut props = vec![0u8; prop_bytes as usize];
    if prop_bytes > 0 {
        src.read_exact(&mut props).await?;
    }

    let data_len = total_len - 2 - prop_bytes as usize;

    match base_type {
        // INT types
        0x30 => {
            // TINYINT
            let v = src.read_u8().await?;
            Ok(SqlValue::U8(Some(v)))
        }
        0x34 => {
            // SMALLINT
            let v = src.read_i16_le().await?;
            Ok(SqlValue::I16(Some(v)))
        }
        0x38 => {
            // INT
            let v = src.read_i32_le().await?;
            Ok(SqlValue::I32(Some(v)))
        }
        0x7F | 0x26 => {
            // BIGINT / INTN
            match data_len {
                1 => Ok(SqlValue::U8(Some(src.read_u8().await?))),
                2 => Ok(SqlValue::I16(Some(src.read_i16_le().await?))),
                4 => Ok(SqlValue::I32(Some(src.read_i32_le().await?))),
                8 => Ok(SqlValue::I64(Some(src.read_i64_le().await?))),
                _ => {
                    let mut buf = vec![0u8; data_len];
                    src.read_exact(&mut buf).await?;
                    Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
                }
            }
        }
        0x32 => {
            // BIT
            let v = src.read_u8().await?;
            Ok(SqlValue::Bit(Some(v != 0)))
        }
        0x3B => {
            // FLOAT4
            let v = src.read_f32_le().await?;
            Ok(SqlValue::F32(Some(v)))
        }
        0x3E => {
            // FLOAT8
            let v = src.read_f64_le().await?;
            Ok(SqlValue::F64(Some(v)))
        }
        0x6D => {
            // FLOATN
            match data_len {
                4 => Ok(SqlValue::F32(Some(src.read_f32_le().await?))),
                8 => Ok(SqlValue::F64(Some(src.read_f64_le().await?))),
                _ => {
                    let mut buf = vec![0u8; data_len];
                    src.read_exact(&mut buf).await?;
                    Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
                }
            }
        }
        0x3C | 0x3D | 0x7A => {
            // MONEY, DATETIME, MONEY4 - fixed types, read as binary
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
        }
        0x6E => {
            // MONEYN
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
        }
        0x6F => {
            // DATETIMEN
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
        }
        0x28 => {
            // DATE
            let mut bytes = [0u8; 4];
            src.read_exact(&mut bytes[..3]).await?;
            let ce_days = u32::from_le_bytes(bytes);
            Ok(SqlValue::Date(Some(crate::temporal::Date::new(ce_days))))
        }
        0x24 => {
            // UNIQUEIDENTIFIER
            let mut data = [0u8; 16];
            src.read_exact(&mut data).await?;
            crate::protocol::wire::guid::reorder_bytes(&mut data);
            Ok(SqlValue::Guid(Some(uuid::Uuid::from_bytes(data))))
        }
        0x6A | 0x6C => {
            // DECIMAL / NUMERIC
            // props contain precision and scale
            let scale = if props.len() >= 2 { props[1] } else { 0 };
            let num = crate::protocol::Numeric::decode_bytes(src, data_len, scale).await?;
            Ok(SqlValue::Numeric(num))
        }
        0xA7 | 0xAF => {
            // BIGVARCHAR / BIGCHAR
            // props: collation (5 bytes)
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            // Try UTF-8 lossy for non-unicode strings
            let s = String::from_utf8_lossy(&buf).into_owned();
            Ok(SqlValue::String(Some(std::borrow::Cow::Owned(s))))
        }
        0xE7 | 0xEF => {
            // NVARCHAR / NCHAR
            // props: collation (5 bytes), then max_len (2 bytes)
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            if !buf.len().is_multiple_of(2) {
                return Err(crate::Error::Protocol(
                    "sql_variant nvarchar: odd length".into(),
                ));
            }
            let iter = buf
                .chunks_exact(2)
                .map(|c| u16::from_le_bytes([c[0], c[1]]));
            let s: String = char::decode_utf16(iter)
                .map(|r| r.unwrap_or('\u{FFFD}'))
                .collect();
            Ok(SqlValue::String(Some(std::borrow::Cow::Owned(s))))
        }
        0xA5 | 0xAD => {
            // BIGVARBINARY / BIGBINARY
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
        }
        0x29 => {
            // TIME
            let scale = if !props.is_empty() { props[0] } else { 7 };
            let time = crate::temporal::Time::decode(src, scale as usize, data_len).await?;
            Ok(SqlValue::Time(Some(time)))
        }
        0x2A => {
            // DATETIME2
            let scale = if !props.is_empty() { props[0] } else { 7 };
            let dt2 = crate::temporal::DateTime2::decode(src, scale as usize, data_len - 3).await?;
            Ok(SqlValue::DateTime2(Some(dt2)))
        }
        0x2B => {
            // DATETIMEOFFSET
            let scale = if !props.is_empty() { props[0] } else { 7 };
            let dto =
                crate::temporal::DateTimeOffset::decode(src, scale as usize, data_len as u8 - 5)
                    .await?;
            Ok(SqlValue::DateTimeOffset(Some(dto)))
        }
        _ => {
            // Unknown base type: read remaining data as binary
            let mut buf = vec![0u8; data_len];
            src.read_exact(&mut buf).await?;
            Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf))))
        }
    }
}
