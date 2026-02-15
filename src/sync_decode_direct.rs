//! Sync direct decode: TDS wire bytes → RowWriter, bypassing SqlValue allocation.
//!
//! This is the synchronous equivalent of `protocol::wire::decode_direct`.

use crate::protocol::wire::{DataType, FixedLenType, VarLenDescriptor, VarLenType};
use crate::row_writer::RowWriter;
use crate::sync_decode::plp_decode_sync;
use crate::sync_reader::SyncProtocolReader;
use byteorder::{ByteOrder, LittleEndian};

/// Days from CE epoch (0001-01-01) to Unix epoch (1970-01-01).
const CE_TO_UNIX_DAYS: i64 = 719_162;
/// Days from SQL Server epoch (1900-01-01) to Unix epoch (1970-01-01).
const SQL_TO_UNIX_DAYS: i64 = 25_567;
/// Microseconds per day.
const MICROS_PER_DAY: i64 = 86_400_000_000;

pub fn decode_column_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    col: usize,
    ty: &DataType,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    match ty {
        DataType::FixedLen(fixed) => decode_fixed_into_sync(src, col, fixed, writer),
        DataType::VarLenSized(vlc) => {
            decode_varlen_into_sync(src, col, vlc, writer, string_buf, bytes_buf)
        }
        DataType::VarLenSizedPrecision {
            ty,
            scale,
            precision,
            ..
        } => match ty {
            VarLenType::Decimaln | VarLenType::Numericn => {
                decode_numeric_into_sync(src, col, *precision, *scale, writer)
            }
            _ => {
                let val = crate::protocol::wire::SqlValue::decode_sync(
                    src,
                    &DataType::VarLenSizedPrecision {
                        ty: *ty,
                        size: 0,
                        precision: *precision,
                        scale: *scale,
                    },
                )?;
                write_sqlvalue_into(col, &val, writer);
                Ok(())
            }
        },
        DataType::Xml { .. } => {
            let val = crate::protocol::wire::SqlValue::decode_sync(src, ty)?;
            write_sqlvalue_into(col, &val, writer);
            Ok(())
        }
    }
}

fn decode_fixed_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    col: usize,
    ty: &FixedLenType,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match ty {
        FixedLenType::Null => writer.write_null(col),
        FixedLenType::Bit => writer.write_bool(col, SyncProtocolReader::read_u8(src)? != 0),
        FixedLenType::Int1 => writer.write_u8(col, SyncProtocolReader::read_u8(src)?),
        FixedLenType::Int2 => writer.write_i16(col, src.read_i16_le()?),
        FixedLenType::Int4 => writer.write_i32(col, src.read_i32_le()?),
        FixedLenType::Int8 => writer.write_i64(col, src.read_i64_le()?),
        FixedLenType::Float4 => writer.write_f32(col, src.read_f32_le()?),
        FixedLenType::Float8 => writer.write_f64(col, src.read_f64_le()?),
        FixedLenType::Datetime => {
            decode_datetime_into_sync(src, col, 8, 8, writer)?;
        }
        FixedLenType::Datetime4 => {
            decode_datetime_into_sync(src, col, 4, 8, writer)?;
        }
        FixedLenType::Money => {
            decode_money_into_sync(src, col, 8, writer)?;
        }
        FixedLenType::Money4 => {
            decode_money_into_sync(src, col, 4, writer)?;
        }
    }
    Ok(())
}

fn decode_varlen_into_sync<R: SyncProtocolReader>(
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
            let recv_len = SyncProtocolReader::read_u8(src)?;
            match recv_len {
                0 => writer.write_null(col),
                1 => writer.write_bool(col, SyncProtocolReader::read_u8(src)? > 0),
                v => {
                    return Err(crate::Error::Protocol(
                        format!("bitn: invalid length {}", v).into(),
                    ));
                }
            }
        }
        VarLenType::Intn => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match (recv_len, len) {
                (0, _) => writer.write_null(col),
                (1, _) => writer.write_u8(col, SyncProtocolReader::read_u8(src)?),
                (2, _) => writer.write_i16(col, src.read_i16_le()?),
                (4, _) => writer.write_i32(col, src.read_i32_le()?),
                (8, _) => writer.write_i64(col, src.read_i64_le()?),
                _ => unimplemented!(),
            }
        }
        VarLenType::Floatn => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match (recv_len, len) {
                (0, _) => writer.write_null(col),
                (4, _) => writer.write_f32(col, src.read_f32_le()?),
                (8, _) => writer.write_f64(col, src.read_f64_le()?),
                _ => {
                    return Err(crate::Error::Protocol(
                        format!("floatn: invalid length {}", recv_len).into(),
                    ));
                }
            }
        }
        VarLenType::Guid => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match recv_len {
                0 => writer.write_null(col),
                16 => {
                    let mut data = [0u8; 16];
                    src.read_exact_bytes(&mut data)?;
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
            let data = plp_decode_sync(src, len)?;
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
            let data = plp_decode_sync(src, len)?;
            match data {
                Some(buf) => {
                    if buf.len() % 2 != 0 {
                        return Err(crate::Error::Protocol(
                            "nvarchar: invalid plp length".into(),
                        ));
                    }
                    #[cfg(target_endian = "little")]
                    {
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
            let data = plp_decode_sync(src, len)?;
            match data {
                Some(buf) => writer.write_bytes(col, &buf),
                None => writer.write_null(col),
            }
        }
        VarLenType::Money => {
            let money_len = SyncProtocolReader::read_u8(src)?;
            decode_money_into_sync(src, col, money_len, writer)?;
        }
        VarLenType::Datetimen => {
            let rlen = SyncProtocolReader::read_u8(src)?;
            decode_datetime_into_sync(src, col, rlen, len as u8, writer)?;
        }
        VarLenType::Daten => {
            let recv_len = SyncProtocolReader::read_u8(src)?;
            match recv_len {
                0 => writer.write_null(col),
                3 => {
                    let mut bytes = [0u8; 4];
                    src.read_exact_bytes(&mut bytes[..3])?;
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
            let rlen = SyncProtocolReader::read_u8(src)?;
            match rlen {
                0 => writer.write_null(col),
                _ => {
                    let time = crate::temporal::Time::decode_sync(src, len, rlen as usize)?;
                    let nanos = time.increments() as i64 * 10i64.pow(9 - time.scale() as u32);
                    writer.write_time(col, nanos);
                }
            }
        }
        VarLenType::Datetime2 => {
            let rlen = SyncProtocolReader::read_u8(src)?;
            match rlen {
                0 => writer.write_null(col),
                rlen => {
                    let dt = crate::temporal::DateTime2::decode_sync(src, len, rlen as usize - 3)?;
                    let micros = datetime2_to_micros(&dt);
                    writer.write_datetime(col, micros);
                }
            }
        }
        VarLenType::DatetimeOffsetn => {
            let rlen = SyncProtocolReader::read_u8(src)?;
            match rlen {
                0 => writer.write_null(col),
                _ => {
                    let dto = crate::temporal::DateTimeOffset::decode_sync(src, len, rlen - 5)?;
                    let micros = datetime2_to_micros(&dto.datetime2());
                    writer.write_datetimeoffset(col, micros, dto.offset());
                }
            }
        }
        VarLenType::Text | VarLenType::NText | VarLenType::Image => {
            let val =
                crate::protocol::wire::SqlValue::decode_sync(src, &DataType::VarLenSized(*vlc))?;
            write_sqlvalue_into(col, &val, writer);
        }
        t => match t {
            VarLenType::SSVariant | VarLenType::Udt => {
                let val = crate::protocol::wire::SqlValue::decode_sync(
                    src,
                    &DataType::VarLenSized(*vlc),
                )?;
                write_sqlvalue_into(col, &val, writer);
            }
            _ => unimplemented!("{:?}", t),
        },
    }
    Ok(())
}

fn decode_numeric_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    col: usize,
    precision: u8,
    scale: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    let num = crate::protocol::Numeric::decode_sync(src, scale)?;
    match num {
        Some(n) => writer.write_decimal(col, n.value(), precision, n.scale()),
        None => writer.write_null(col),
    }
    Ok(())
}

fn decode_datetime_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    col: usize,
    rlen: u8,
    type_len: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match (rlen, type_len) {
        (0, _) => writer.write_null(col),
        (4, _) => {
            let days = src.read_u16_le()? as i64;
            let mins = src.read_u16_le()? as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
            let micros = unix_days * MICROS_PER_DAY + mins * 60 * 1_000_000;
            writer.write_datetime(col, micros);
        }
        (8, _) => {
            let days = src.read_i32_le()? as i64;
            let ticks = src.read_u32_le()? as i64;
            let unix_days = days - SQL_TO_UNIX_DAYS;
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

fn decode_money_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    col: usize,
    len: u8,
    writer: &mut impl RowWriter,
) -> crate::Result<()> {
    match len {
        0 => writer.write_null(col),
        4 => {
            let val = src.read_i32_le()? as i128;
            writer.write_decimal(col, val, 19, 4);
        }
        8 => {
            let high = src.read_i32_le()? as i64;
            let low = src.read_u32_le()? as i64;
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

fn datetime2_to_micros(dt: &crate::temporal::DateTime2) -> i64 {
    let ce_days = dt.date().days() as i64;
    let unix_days = ce_days - CE_TO_UNIX_DAYS;
    let t = dt.time();
    let nanos = t.increments() as i64 * 10i64.pow(9 - t.scale() as u32);
    let time_micros = nanos / 1_000;
    unix_days * MICROS_PER_DAY + time_micros
}

fn write_sqlvalue_into(
    col: usize,
    val: &crate::protocol::wire::SqlValue<'_>,
    writer: &mut impl RowWriter,
) {
    // Reuse the exact same logic from the async version
    crate::protocol::wire::decode_direct::write_sqlvalue_into(col, val, writer);
}

pub fn decode_row_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    let col_meta = src.context().last_meta().unwrap();
    let columns: Vec<_> = col_meta.columns.iter().collect();

    for (col, column) in columns.iter().enumerate() {
        decode_column_into_sync(src, col, &column.base.ty, writer, string_buf, bytes_buf)?;
    }

    Ok(())
}

pub fn decode_nbc_row_into_sync<R: SyncProtocolReader>(
    src: &mut R,
    writer: &mut impl RowWriter,
    string_buf: &mut String,
    bytes_buf: &mut Vec<u8>,
) -> crate::Result<()> {
    let col_meta = src.context().last_meta().unwrap();
    let num_cols = col_meta.columns.len();
    let columns: Vec<_> = col_meta.columns.iter().collect();

    let bitmap_size = num_cols.div_ceil(8);
    let mut bitmap = vec![0u8; bitmap_size];
    src.read_exact_bytes(&mut bitmap)?;

    for (col, column) in columns.iter().enumerate() {
        let byte_idx = col / 8;
        let bit_idx = col % 8;
        let is_null = bitmap[byte_idx] & (1 << bit_idx) > 0;

        if is_null {
            writer.write_null(col);
        } else {
            decode_column_into_sync(src, col, &column.base.ty, writer, string_buf, bytes_buf)?;
        }
    }

    Ok(())
}
