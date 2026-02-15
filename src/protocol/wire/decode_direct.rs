//! Direct decode: TDS wire bytes → RowWriter, bypassing SqlValue allocation.
//!
//! Each function mirrors its counterpart in `types/` but writes directly
//! to a `RowWriter` trait object instead of constructing a `SqlValue`.

use crate::protocol::reader::ProtocolReader;
use crate::protocol::wire::{DataType, FixedLenType, VarLenDescriptor, VarLenType};
use crate::row_writer::RowWriter;
use byteorder::{ByteOrder, LittleEndian};
use futures_util::io::AsyncReadExt;

/// Days from CE epoch (0001-01-01) to Unix epoch (1970-01-01).
const CE_TO_UNIX_DAYS: i64 = 719_162;
/// Days from SQL Server epoch (1900-01-01) to Unix epoch (1970-01-01).
const SQL_TO_UNIX_DAYS: i64 = 25_567;
/// Microseconds per day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Decode a single column value from the TDS wire and write it directly
/// to the RowWriter. No SqlValue is created.
pub async fn decode_column_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    ty: &DataType,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    match ty {
        DataType::FixedLen(fixed) => decode_fixed_into(src, col, fixed, writer).await,
        DataType::VarLenSized(vlc) => {
            decode_varlen_into(src, col, vlc, writer, string_buf, bytes_buf).await
        }
        DataType::VarLenSizedPrecision {
            ty,
            scale,
            precision,
            ..
        } => match ty {
            VarLenType::Decimaln | VarLenType::Numericn => {
                decode_numeric_into(src, col, *precision, *scale, writer).await
            }
            _ => {
                // Fallback: decode as SqlValue
                let val = crate::protocol::wire::SqlValue::decode(
                    src,
                    &DataType::VarLenSizedPrecision {
                        ty: *ty,
                        size: 0,
                        precision: *precision,
                        scale: *scale,
                    },
                )
                .await?;
                write_sqlvalue_into(col, &val, writer);
                Ok(())
            }
        },
        DataType::Xml { size: _, schema: _ } => {
            // XML is rare; just decode as SqlValue and dispatch
            let val = crate::protocol::wire::SqlValue::decode(src, ty).await?;
            write_sqlvalue_into(col, &val, writer);
            Ok(())
        }
    }
}

/// Fixed-length types: no length prefix on the wire.
async fn decode_fixed_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    ty: &FixedLenType,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match ty {
        FixedLenType::Null => writer.write_null(col),
        FixedLenType::Bit => writer.write_bool(col, src.read_u8().await? != 0),
        FixedLenType::Int1 => writer.write_u8(col, src.read_u8().await?),
        FixedLenType::Int2 => writer.write_i16(col, src.read_i16_le().await?),
        FixedLenType::Int4 => writer.write_i32(col, src.read_i32_le().await?),
        FixedLenType::Int8 => writer.write_i64(col, src.read_i64_le().await?),
        FixedLenType::Float4 => writer.write_f32(col, src.read_f32_le().await?),
        FixedLenType::Float8 => writer.write_f64(col, src.read_f64_le().await?),
        FixedLenType::Datetime => {
            decode_datetime_into(src, col, 8, 8, writer).await?;
        }
        FixedLenType::Datetime4 => {
            decode_datetime_into(src, col, 4, 8, writer).await?;
        }
        FixedLenType::Money => {
            decode_money_into(src, col, 8, writer).await?;
        }
        FixedLenType::Money4 => {
            decode_money_into(src, col, 4, writer).await?;
        }
    }
    Ok(())
}

/// Variable-length types.
async fn decode_varlen_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    vlc: &VarLenDescriptor,
    writer: &mut impl RowWriter,
    _string_buf: &mut String,
    _bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    let ty = vlc.r#type();
    let len = vlc.len();
    let collation = vlc.collation();

    match ty {
        VarLenType::Bitn => {
            let recv_len = src.read_u8().await?;
            match recv_len {
                0 => writer.write_null(col),
                1 => writer.write_bool(col, src.read_u8().await? > 0),
                v => {
                    return Err(crate::Error::Protocol(
                        format!("bitn: invalid length {}", v).into(),
                    ));
                }
            }
        }
        VarLenType::Intn => {
            let recv_len = src.read_u8().await? as usize;
            match (recv_len, len) {
                (0, 1) => writer.write_null(col),
                (0, 2) => writer.write_null(col),
                (0, 4) => writer.write_null(col),
                (0, _) => writer.write_null(col),
                (1, _) => writer.write_u8(col, src.read_u8().await?),
                (2, _) => writer.write_i16(col, src.read_i16_le().await?),
                (4, _) => writer.write_i32(col, src.read_i32_le().await?),
                (8, _) => writer.write_i64(col, src.read_i64_le().await?),
                _ => unimplemented!(),
            }
        }
        VarLenType::Floatn => {
            let recv_len = src.read_u8().await? as usize;
            match (recv_len, len) {
                (0, 4) => writer.write_null(col),
                (0, _) => writer.write_null(col),
                (4, _) => writer.write_f32(col, src.read_f32_le().await?),
                (8, _) => writer.write_f64(col, src.read_f64_le().await?),
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("floatn: invalid length {}", recv_len).into(),
                    ));
                }
            }
        }
        VarLenType::Guid => {
            let recv_len = src.read_u8().await? as usize;
            match recv_len {
                0 => writer.write_null(col),
                16 => {
                    let mut data = [0u8; 16];
                    for item in &mut data {
                        *item = src.read_u8().await?;
                    }
                    crate::protocol::wire::guid::reorder_bytes(&mut data);
                    writer.write_guid(col, &data);
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("guid: invalid length {}", recv_len).into(),
                    ));
                }
            }
        }
        VarLenType::BigChar | VarLenType::BigVarChar => {
            // Non-unicode string with collation encoding
            let data = super::types::plp::decode(src, len).await?;
            match data {
                Some(buf) => {
                    let collation_ref = collation.as_ref().unwrap();
                    let encoder = collation_ref.encoding()?;
                    let s = encoder
                        .decode_without_bom_handling_and_without_replacement(buf.as_ref())
                        .ok_or_else(|| crate::Error::Encoding("invalid sequence".into()))?;
                    writer.write_str(col, &s);
                }
                None => writer.write_null(col),
            }
        }
        VarLenType::NChar | VarLenType::NVarchar => {
            // UTF-16 string — pass raw u16 slice via write_utf16 to avoid
            // the UTF-16 → UTF-8 → UTF-16 round-trip for ODBC consumers.
            let data = super::types::plp::decode(src, len).await?;
            match data {
                Some(buf) => {
                    if buf.len() % 2 != 0 {
                        return Err(crate::Error::Protocol(
                            "nvarchar: invalid plp length".into(),
                        ));
                    }
                    // SAFETY: buf is aligned to u8, but u16 from LE bytes is safe via
                    // from_le_bytes. We reinterpret as &[u16] only on LE platforms,
                    // otherwise collect.
                    #[cfg(target_endian = "little")]
                    {
                        // SAFETY: buf.len() is even, and on LE the byte representation
                        // of &[u8] pairs is identical to &[u16] in LE order.
                        let ptr = buf.as_ptr() as *const u16;
                        let utf16_slice = unsafe { std::slice::from_raw_parts(ptr, buf.len() / 2) };
                        writer.write_utf16(col, utf16_slice);
                    }
                    #[cfg(not(target_endian = "little"))]
                    {
                        let utf16_buf: Vec<u16> = buf
                            .chunks_exact(2)
                            .map(|c| u16::from_le_bytes([c[0], c[1]]))
                            .collect();
                        writer.write_utf16(col, &utf16_buf);
                    }
                }
                None => writer.write_null(col),
            }
        }
        VarLenType::BigBinary | VarLenType::BigVarBin => {
            let data = super::types::plp::decode(src, len).await?;
            match data {
                Some(buf) => writer.write_bytes(col, &buf),
                None => writer.write_null(col),
            }
        }
        VarLenType::Money => {
            let money_len = src.read_u8().await?;
            decode_money_into(src, col, money_len, writer).await?;
        }
        VarLenType::Datetimen => {
            let rlen = src.read_u8().await?;
            decode_datetime_into(src, col, rlen, len as u8, writer).await?;
        }
        VarLenType::Daten => {
            let recv_len = src.read_u8().await?;
            match recv_len {
                0 => writer.write_null(col),
                3 => {
                    let mut bytes = [0u8; 4];
                    src.read_exact(&mut bytes[..3]).await?;
                    let ce_days = LittleEndian::read_u32(&bytes) as i64;
                    let unix_days = (ce_days - CE_TO_UNIX_DAYS) as i32;
                    writer.write_date(col, unix_days);
                }
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("daten: invalid length {}", recv_len).into(),
                    ));
                }
            }
        }
        VarLenType::Timen => {
            let rlen = src.read_u8().await?;
            match rlen {
                0 => writer.write_null(col),
                _ => {
                    let time = crate::temporal::Time::decode(src, len, rlen as usize).await?;
                    let nanos = time.increments() as i64 * 10i64.pow(9 - time.scale() as u32);
                    writer.write_time(col, nanos);
                }
            }
        }
        VarLenType::Datetime2 => {
            let rlen = src.read_u8().await?;
            match rlen {
                0 => writer.write_null(col),
                rlen => {
                    let dt =
                        crate::temporal::DateTime2::decode(src, len, rlen as usize - 3).await?;
                    let micros = datetime2_to_micros(&dt);
                    writer.write_datetime(col, micros);
                }
            }
        }
        VarLenType::DatetimeOffsetn => {
            let rlen = src.read_u8().await?;
            match rlen {
                0 => writer.write_null(col),
                _ => {
                    let dto = crate::temporal::DateTimeOffset::decode(src, len, rlen - 5).await?;
                    let micros = datetime2_to_micros(&dto.datetime2());
                    // datetime2 part is already UTC per TDS spec, no offset subtraction needed
                    writer.write_datetimeoffset(col, micros, dto.offset());
                }
            }
        }
        VarLenType::Text => {
            // Legacy text type — rare, fallback to SqlValue
            let val =
                crate::protocol::wire::SqlValue::decode(src, &DataType::VarLenSized(*vlc)).await?;
            write_sqlvalue_into(col, &val, writer);
        }
        VarLenType::NText => {
            let val =
                crate::protocol::wire::SqlValue::decode(src, &DataType::VarLenSized(*vlc)).await?;
            write_sqlvalue_into(col, &val, writer);
        }
        VarLenType::Image => {
            let val =
                crate::protocol::wire::SqlValue::decode(src, &DataType::VarLenSized(*vlc)).await?;
            write_sqlvalue_into(col, &val, writer);
        }
        t => {
            // SSVariant and Udt: fall back to SqlValue decode path
            match t {
                VarLenType::SSVariant | VarLenType::Udt => {
                    let val =
                        crate::protocol::wire::SqlValue::decode(src, &DataType::VarLenSized(*vlc))
                            .await?;
                    write_sqlvalue_into(col, &val, writer);
                }
                _ => unimplemented!("{:?}", t),
            }
        }
    }
    Ok(())
}

/// Decode numeric/decimal directly into RowWriter.
async fn decode_numeric_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    precision: u8,
    scale: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    let num = crate::protocol::Numeric::decode(src, scale).await?;
    match num {
        Some(n) => writer.write_decimal(col, n.value(), precision, n.scale()),
        None => writer.write_null(col),
    }
    Ok(())
}

/// Decode datetime/smalldatetime directly.
async fn decode_datetime_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    rlen: u8,
    type_len: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match (rlen, type_len) {
        (0, _) => writer.write_null(col),
        (4, _) => {
            // SmallDateTime: u16 days since 1900-01-01, u16 minutes since midnight
            let days = src.read_u16_le().await? as i64;
            let mins = src.read_u16_le().await? as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
            let micros = unix_days * MICROS_PER_DAY + mins * 60 * 1_000_000;
            writer.write_datetime(col, micros);
        }
        (8, _) => {
            // DateTime: i32 days since 1900-01-01, u32 ticks (1/300th second)
            let days = src.read_i32_le().await? as i64;
            let ticks = src.read_u32_le().await? as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
            // Match legacy precision: truncate to whole milliseconds first
            let total_ms = ticks * 1000 / 300;
            let micros = unix_days * MICROS_PER_DAY + total_ms * 1000;
            writer.write_datetime(col, micros);
        }
        _ => {
            return Err(crate::Error::Protocol(
                format!("datetimen: invalid length {}", rlen).into(),
            ));
        }
    }
    Ok(())
}

/// Decode money types directly.
async fn decode_money_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    col: usize,
    len: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match len {
        0 => writer.write_null(col),
        4 => {
            let val = src.read_i32_le().await? as i128;
            writer.write_decimal(col, val, 19, 4);
        }
        8 => {
            let high = src.read_i32_le().await? as i64;
            let low = src.read_u32_le().await? as i64;
            let val = ((high as i128) << 32) | (low as i128);
            writer.write_decimal(col, val, 19, 4);
        }
        _ => {
            return Err(crate::Error::Protocol(
                format!("money: invalid length {}", len).into(),
            ));
        }
    }
    Ok(())
}

/// Convert DateTime2 to microseconds since Unix epoch using pure integer math.
fn datetime2_to_micros(dt: &crate::temporal::DateTime2) -> i64 {
    let ce_days = dt.date().days() as i64;
    let unix_days = ce_days - CE_TO_UNIX_DAYS;
    let t = dt.time();
    let nanos = t.increments() as i64 * 10i64.pow(9 - t.scale() as u32);
    let time_micros = nanos / 1_000;
    unix_days * MICROS_PER_DAY + time_micros
}

/// Fallback: write a SqlValue through the RowWriter (for rare types like XML, Text, Image).
pub(crate) fn write_sqlvalue_into(
    col: usize,
    val: &crate::protocol::wire::SqlValue<'_>,
    writer: &mut impl RowWriter,
) {
    use crate::protocol::wire::SqlValue;
    match val {
        SqlValue::Bit(None)
        | SqlValue::U8(None)
        | SqlValue::I16(None)
        | SqlValue::I32(None)
        | SqlValue::I64(None)
        | SqlValue::F32(None)
        | SqlValue::F64(None)
        | SqlValue::String(None)
        | SqlValue::Binary(None)
        | SqlValue::Guid(None)
        | SqlValue::Numeric(None)
        | SqlValue::Xml(None)
        | SqlValue::DateTime(None)
        | SqlValue::SmallDateTime(None)
        | SqlValue::DateTime2(None)
        | SqlValue::Date(None)
        | SqlValue::Time(None)
        | SqlValue::DateTimeOffset(None) => writer.write_null(col),
        SqlValue::Bit(Some(v)) => writer.write_bool(col, *v),
        SqlValue::U8(Some(v)) => writer.write_u8(col, *v),
        SqlValue::I16(Some(v)) => writer.write_i16(col, *v),
        SqlValue::I32(Some(v)) => writer.write_i32(col, *v),
        SqlValue::I64(Some(v)) => writer.write_i64(col, *v),
        SqlValue::F32(Some(v)) => writer.write_f32(col, *v),
        SqlValue::F64(Some(v)) => writer.write_f64(col, *v),
        SqlValue::String(Some(v)) => writer.write_str(col, v.as_ref()),
        SqlValue::Binary(Some(v)) => writer.write_bytes(col, v.as_ref()),
        SqlValue::Guid(Some(v)) => writer.write_guid(col, v.as_bytes()),
        SqlValue::Numeric(Some(v)) => {
            writer.write_decimal(col, v.value(), v.precision(), v.scale())
        }
        SqlValue::Xml(Some(v)) => {
            let s = format!("{}", v);
            writer.write_str(col, &s);
        }
        SqlValue::DateTime(Some(dt)) => {
            let days = dt.days() as i64;
            let ticks = dt.seconds_fragments() as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
            let total_ms = ticks * 1000 / 300;
            let micros = unix_days * MICROS_PER_DAY + total_ms * 1000;
            writer.write_datetime(col, micros);
        }
        SqlValue::SmallDateTime(Some(dt)) => {
            let days = dt.days() as i64;
            let mins = dt.seconds_fragments() as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
            let micros = unix_days * MICROS_PER_DAY + mins * 60 * 1_000_000;
            writer.write_datetime(col, micros);
        }
        SqlValue::DateTime2(Some(dt)) => {
            writer.write_datetime(col, datetime2_to_micros(dt));
        }
        SqlValue::Date(Some(d)) => {
            let ce_days = d.days() as i64;
            let unix_days = (ce_days - CE_TO_UNIX_DAYS) as i32;
            writer.write_date(col, unix_days);
        }
        SqlValue::Time(Some(t)) => {
            let nanos = t.increments() as i64 * 10i64.pow(9 - t.scale() as u32);
            writer.write_time(col, nanos);
        }
        SqlValue::DateTimeOffset(Some(dto)) => {
            let micros = datetime2_to_micros(&dto.datetime2());
            // datetime2 part is already UTC per TDS spec
            writer.write_datetimeoffset(col, micros, dto.offset());
        }
    }
}

/// Decode an entire row directly into a RowWriter.
/// Column metadata comes from the ProtocolReader context.
pub async fn decode_row_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    let col_meta = src.context().last_meta().unwrap();
    let columns: Vec<_> = col_meta.columns.iter().collect();

    for (col, column) in columns.iter().enumerate() {
        decode_column_into(src, col, &column.base.ty, writer, string_buf, bytes_buf).await?;
    }

    Ok(())
}

/// Decode an NBC (Null Bitmap Compressed) row directly into a RowWriter.
pub async fn decode_nbc_row_into<R: ProtocolReader + Unpin>(
    src: &mut R,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    let col_meta = src.context().last_meta().unwrap();
    let num_cols = col_meta.columns.len();
    let columns: Vec<_> = col_meta.columns.iter().collect();

    // Read null bitmap
    let bitmap_size = num_cols.div_ceil(8);
    let mut bitmap = vec![0u8; bitmap_size];
    src.read_exact(&mut bitmap[0..bitmap_size]).await?;

    for (col, column) in columns.iter().enumerate() {
        let byte_idx = col / 8;
        let bit_idx = col % 8;
        let is_null = bitmap[byte_idx] & (1 << bit_idx) > 0;

        if is_null {
            writer.write_null(col);
        } else {
            decode_column_into(src, col, &column.base.ty, writer, string_buf, bytes_buf).await?;
        }
    }

    Ok(())
}
