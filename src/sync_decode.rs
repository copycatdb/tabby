//! Sync decode functions for TDS tokens.
//!
//! These are blocking equivalents of the async `decode` methods on each
//! token type. They read from a `SyncProtocolReader` instead of an
//! async `ProtocolReader`.

use crate::protocol::wire::*;
use crate::sync_reader::SyncProtocolReader;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use std::convert::TryFrom;
use std::io::{Cursor, Read};

// ── CompletionMessage ────────────────────────────────────────────────

impl CompletionMessage {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        use enumflags2::BitFlags;

        let status = BitFlags::from_bits(src.read_u16_le()?)
            .map_err(|_| crate::Error::Protocol("done(variant): invalid status".into()))?;
        let cur_cmd = src.read_u16_le()?;
        let done_row_count_bytes = src.context().version().done_row_count_bytes();
        let done_rows = match done_row_count_bytes {
            8 => src.read_u64_le()?,
            4 => src.read_u32_le()? as u64,
            _ => unreachable!(),
        };

        Ok(CompletionMessage::from_parts(status, cur_cmd, done_rows))
    }
}

// ── ServerError ──────────────────────────────────────────────────────

impl ServerError {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let _length = src.read_u16_le()? as usize;
        let code = src.read_u32_le()?;
        let state = SyncProtocolReader::read_u8(src)?;
        let class = SyncProtocolReader::read_u8(src)?;
        let message = src.read_us_varchar()?;
        let server = src.read_b_varchar()?;
        let procedure = src.read_b_varchar()?;
        let line = if src.context().version() > FeatureLevel::SqlServer2005 {
            src.read_u32_le()?
        } else {
            src.read_u16_le()? as u32
        };

        Ok(ServerError {
            code,
            state,
            class,
            message,
            server,
            procedure,
            line,
        })
    }
}

// ── ServerNotice ─────────────────────────────────────────────────────

impl ServerNotice {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let _length = src.read_u16_le()?;
        let number = src.read_u32_le()?;
        let state = SyncProtocolReader::read_u8(src)?;
        let class = SyncProtocolReader::read_u8(src)?;
        let message = src.read_us_varchar()?;
        let server = src.read_b_varchar()?;
        let procedure = src.read_b_varchar()?;
        let line = src.read_u32_le()?;

        Ok(ServerNotice {
            number,
            state,
            class,
            message,
            server,
            procedure,
            line,
        })
    }
}

// ── SessionChange ────────────────────────────────────────────────────

impl SessionChange {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let len = src.read_u16_le()? as usize;
        let mut bytes = vec![0u8; len];
        src.read_exact_bytes(&mut bytes)?;

        // Reuse the same cursor-based parsing as the async version.
        let mut buf = Cursor::new(bytes);
        let ty_byte = buf.read_u8()?;
        let ty = EnvChangeTy::try_from(ty_byte).map_err(|_| {
            crate::Error::Protocol(format!("invalid envchange type {:x}", ty_byte).into())
        })?;

        let token = match ty {
            EnvChangeTy::Database => {
                let len = buf.read_u8()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let new_value = String::from_utf16(&u16s)?;

                let len = buf.read_u8()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let old_value = String::from_utf16(&u16s)?;

                SessionChange::Database(new_value, old_value)
            }
            EnvChangeTy::PacketSize => {
                let len = buf.read_u8()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let new_value = String::from_utf16(&u16s)?;

                let len = buf.read_u8()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let old_value = String::from_utf16(&u16s)?;

                SessionChange::PacketSize(new_value.parse()?, old_value.parse()?)
            }
            EnvChangeTy::SqlCollation => {
                let len = buf.read_u8()? as usize;
                let mut new_value = vec![0u8; len];
                buf.read_exact(&mut new_value)?;

                let new = if len == 5 {
                    let new_sortid = new_value[4];
                    let new_info = u32::from_le_bytes([
                        new_value[0],
                        new_value[1],
                        new_value[2],
                        new_value[3],
                    ]);
                    Some(crate::protocol::Collation::new(new_info, new_sortid))
                } else {
                    None
                };

                let len = buf.read_u8()? as usize;
                let mut old_value = vec![0u8; len];
                buf.read_exact(&mut old_value)?;

                let old = if len == 5 {
                    let old_sortid = old_value[4];
                    let old_info = u32::from_le_bytes([
                        old_value[0],
                        old_value[1],
                        old_value[2],
                        old_value[3],
                    ]);
                    Some(crate::protocol::Collation::new(old_info, old_sortid))
                } else {
                    None
                };

                SessionChange::SqlCollation { new, old }
            }
            EnvChangeTy::BeginTransaction | EnvChangeTy::EnlistDTCTransaction => {
                let len = buf.read_u8()?;
                assert!(len == 8);
                let mut desc = [0u8; 8];
                buf.read_exact(&mut desc)?;
                SessionChange::BeginTransaction(desc)
            }
            EnvChangeTy::CommitTransaction => SessionChange::CommitTransaction,
            EnvChangeTy::RollbackTransaction => SessionChange::RollbackTransaction,
            EnvChangeTy::DefectTransaction => SessionChange::DefectTransaction,
            EnvChangeTy::Routing => {
                buf.read_u16::<LittleEndian>()?;
                buf.read_u8()?;
                let port = buf.read_u16::<LittleEndian>()?;
                let len = buf.read_u16::<LittleEndian>()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let host = String::from_utf16(&u16s)?;
                SessionChange::Routing { host, port }
            }
            EnvChangeTy::Rtls => {
                let len = buf.read_u8()? as usize;
                let mut u16s = vec![0u16; len];
                for item in u16s.iter_mut() {
                    *item = buf.read_u16::<LittleEndian>()?;
                }
                let mirror_name = String::from_utf16(&u16s)?;
                SessionChange::ChangeMirror(mirror_name)
            }
            ty => SessionChange::Ignored(ty),
        };

        Ok(token)
    }
}

// ── LoginResponse ────────────────────────────────────────────────────

impl LoginResponse {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let _length = src.read_u16_le()?;
        let interface = SyncProtocolReader::read_u8(src)?;
        let tds_version = FeatureLevel::try_from(src.read_u32_be()?)
            .map_err(|_| crate::Error::Protocol("Login ACK: Invalid TDS version".into()))?;
        let prog_name = src.read_b_varchar()?;
        let version = src.read_u32_le()?;

        Ok(LoginResponse {
            interface,
            tds_version,
            prog_name,
            version,
        })
    }
}

// ── FeatureAck ───────────────────────────────────────────────────────

impl FeatureAckMessage {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let mut features = Vec::new();
        loop {
            let feature_id = SyncProtocolReader::read_u8(src)?;
            if feature_id == FEA_EXT_TERMINATOR {
                break;
            } else if feature_id == FEA_EXT_FEDAUTH {
                let data_len = src.read_u32_le()?;
                let nonce = if data_len == 32 {
                    let mut n = [0u8; 32];
                    src.read_exact_bytes(&mut n)?;
                    Some(n)
                } else if data_len == 0 {
                    None
                } else {
                    panic!("invalid Feature_Ext_Ack token");
                };
                features.push(FeatureAck::FedAuth(FedAuthAck::SecurityToken { nonce }));
            } else {
                unimplemented!("unsupported feature {}", feature_id);
            }
        }
        Ok(FeatureAckMessage { features })
    }
}

// ── OrderMessage ─────────────────────────────────────────────────────

impl OrderMessage {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let len = src.read_u16_le()? / 2;
        let mut column_indexes = Vec::with_capacity(len as usize);
        for _ in 0..len {
            column_indexes.push(src.read_u16_le()?);
        }
        Ok(OrderMessage { column_indexes })
    }
}

// ── ColumnSchema ─────────────────────────────────────────────────────

impl ColumnSchema<'static> {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let column_count = src.read_u16_le()?;
        let mut columns = Vec::with_capacity(column_count as usize);

        if column_count > 0 && column_count < 0xffff {
            for _ in 0..column_count {
                let base = BaseColumnDescriptor::decode_sync(src)?;
                let col_name = std::borrow::Cow::from(src.read_b_varchar()?);
                columns.push(ColumnDescriptor { base, col_name });
            }
        }

        Ok(ColumnSchema { columns })
    }
}

impl BaseColumnDescriptor {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        use VarLenType::*;
        use enumflags2::BitFlags;

        let _user_ty = src.read_u32_le()?;
        let flags = BitFlags::from_bits(src.read_u16_le()?)
            .map_err(|_| crate::Error::Protocol("column metadata: invalid flags".into()))?;

        let ty = DataType::decode_sync(src)?;

        if let DataType::VarLenSized(cx) = &ty {
            if let Text | NText | Image = cx.r#type() {
                let num_of_parts = SyncProtocolReader::read_u8(src)?;
                for _ in 0..num_of_parts {
                    src.read_us_varchar()?;
                }
            }
        }

        Ok(BaseColumnDescriptor { flags, ty })
    }
}

// ── DataType ─────────────────────────────────────────────────────────

impl DataType {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let ty = SyncProtocolReader::read_u8(src)?;

        if let Ok(ty) = FixedLenType::try_from(ty) {
            return Ok(DataType::FixedLen(ty));
        }

        match VarLenType::try_from(ty) {
            Err(()) => Err(crate::Error::Protocol(
                format!("invalid or unsupported column type: {:?}", ty).into(),
            )),
            Ok(VarLenType::Xml) => {
                let has_schema = SyncProtocolReader::read_u8(src)?;
                let schema = if has_schema == 1 {
                    let db_name = src.read_b_varchar()?;
                    let owner = src.read_b_varchar()?;
                    let collection = src.read_us_varchar()?;
                    Some(std::sync::Arc::new(crate::protocol::xml::XmlSchema::new(
                        db_name, owner, collection,
                    )))
                } else {
                    None
                };
                Ok(DataType::Xml {
                    schema,
                    size: 0xfffffffffffffffe_usize,
                })
            }
            Ok(ty) => {
                let len = match ty {
                    VarLenType::Timen | VarLenType::DatetimeOffsetn | VarLenType::Datetime2 => {
                        SyncProtocolReader::read_u8(src)? as usize
                    }
                    VarLenType::Daten => 3,
                    VarLenType::Bitn
                    | VarLenType::Intn
                    | VarLenType::Floatn
                    | VarLenType::Decimaln
                    | VarLenType::Numericn
                    | VarLenType::Guid
                    | VarLenType::Money
                    | VarLenType::Datetimen => SyncProtocolReader::read_u8(src)? as usize,
                    VarLenType::NChar
                    | VarLenType::BigChar
                    | VarLenType::NVarchar
                    | VarLenType::BigVarChar
                    | VarLenType::BigBinary
                    | VarLenType::BigVarBin => src.read_u16_le()? as usize,
                    VarLenType::Image | VarLenType::Text | VarLenType::NText => {
                        src.read_u32_le()? as usize
                    }
                    VarLenType::SSVariant => src.read_u32_le()? as usize,
                    VarLenType::Udt => {
                        let max_len = src.read_u16_le()? as usize;
                        let _db_name = src.read_b_varchar()?;
                        let _schema_name = src.read_b_varchar()?;
                        let _type_name = src.read_b_varchar()?;
                        let _asm_name = src.read_us_varchar()?;
                        max_len
                    }
                    _ => todo!("not yet implemented for {:?}", ty),
                };

                let collation = match ty {
                    VarLenType::NText
                    | VarLenType::Text
                    | VarLenType::BigChar
                    | VarLenType::NChar
                    | VarLenType::NVarchar
                    | VarLenType::BigVarChar => {
                        let info = src.read_u32_le()?;
                        let sort_id = SyncProtocolReader::read_u8(src)?;
                        Some(crate::protocol::Collation::new(info, sort_id))
                    }
                    _ => None,
                };

                let vty = match ty {
                    VarLenType::Decimaln | VarLenType::Numericn => {
                        let precision = SyncProtocolReader::read_u8(src)?;
                        let scale = SyncProtocolReader::read_u8(src)?;
                        DataType::VarLenSizedPrecision {
                            size: len,
                            ty,
                            precision,
                            scale,
                        }
                    }
                    _ => {
                        let cx = VarLenDescriptor::new(ty, len, collation);
                        DataType::VarLenSized(cx)
                    }
                };

                Ok(vty)
            }
        }
    }
}

// ── ReturnValue ──────────────────────────────────────────────────────

impl ReturnValue {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let param_ordinal = src.read_u16_le()?;
        let param_name = src.read_b_varchar()?;
        let udf = match SyncProtocolReader::read_u8(src)? {
            0x01 => false,
            0x02 => true,
            _ => return Err(crate::Error::Protocol("ReturnValue: invalid status".into())),
        };
        let meta = BaseColumnDescriptor::decode_sync(src)?;
        let value = SqlValue::decode_sync(src, &meta.ty)?;
        Ok(ReturnValue {
            param_ordinal,
            param_name,
            udf,
            meta,
            value,
        })
    }
}

// ── PLP decode (sync) ────────────────────────────────────────────────

pub(crate) fn plp_decode_sync<R: SyncProtocolReader>(
    src: &mut R,
    len: usize,
) -> crate::Result<Option<Vec<u8>>> {
    match len {
        len if len < 0xffff => {
            let len = src.read_u16_le()? as u64;
            match len {
                0xffff => Ok(None),
                _ => {
                    let mut data = vec![0u8; len as usize];
                    src.read_exact_bytes(&mut data)?;
                    Ok(Some(data))
                }
            }
        }
        _ => {
            let len = src.read_u64_le()?;
            let mut data = match len {
                0xffffffffffffffff => return Ok(None),
                0xfffffffffffffffe => Vec::new(),
                _ => Vec::with_capacity(len as usize),
            };

            loop {
                let chunk_size = src.read_u32_le()? as usize;
                if chunk_size == 0 {
                    break;
                }
                let start = data.len();
                data.resize(start + chunk_size, 0);
                src.read_exact_bytes(&mut data[start..])?;
            }

            Ok(Some(data))
        }
    }
}

// ── Numeric decode (sync) ────────────────────────────────────────────

impl crate::protocol::Numeric {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(
        src: &mut R,
        scale: u8,
    ) -> crate::Result<Option<Self>> {
        fn decode_d128(buf: &[u8]) -> u128 {
            let low_part = byteorder::LittleEndian::read_u64(&buf[0..]) as u128;
            if !buf[8..].iter().any(|x| *x != 0) {
                return low_part;
            }
            let high_part = match buf.len() {
                12 => byteorder::LittleEndian::read_u32(&buf[8..]) as u128,
                16 => byteorder::LittleEndian::read_u64(&buf[8..]) as u128,
                _ => unreachable!(),
            };
            #[cfg(target_endian = "big")]
            let (low_part, high_part) = (high_part, low_part);
            let high_part = high_part * (u64::MAX as u128 + 1);
            low_part + high_part
        }

        let len = SyncProtocolReader::read_u8(src)?;
        if len == 0 {
            Ok(None)
        } else {
            let sign = match SyncProtocolReader::read_u8(src)? {
                0 => -1i128,
                1 => 1i128,
                _ => return Err(crate::Error::Protocol("decimal: invalid sign".into())),
            };

            let value = match len {
                5 => src.read_u32_le()? as i128 * sign,
                9 => src.read_u64_le()? as i128 * sign,
                13 => {
                    let mut bytes = [0u8; 12];
                    src.read_exact_bytes(&mut bytes)?;
                    decode_d128(&bytes) as i128 * sign
                }
                17 => {
                    let mut bytes = [0u8; 16];
                    src.read_exact_bytes(&mut bytes)?;
                    decode_d128(&bytes) as i128 * sign
                }
                x => {
                    return Err(crate::Error::Protocol(
                        format!("decimal/numeric: invalid length of {} received", x).into(),
                    ));
                }
            };

            Ok(Some(crate::protocol::Numeric::new_with_scale(value, scale)))
        }
    }
}

// ── SqlValue decode (sync) — fallback for rare types ─────────────────

impl<'a> SqlValue<'a> {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(
        src: &mut R,
        ctx: &DataType,
    ) -> crate::Result<SqlValue<'static>> {
        // For the sync path, we only need fallback decoding for rare types.
        // Most common types are handled by decode_direct_sync.
        // This is a simplified version that handles what decode_direct needs.
        match ctx {
            DataType::FixedLen(fty) => decode_fixed_sqlvalue_sync(src, fty),
            DataType::VarLenSized(vlc) => decode_varlen_sqlvalue_sync(src, vlc),
            DataType::VarLenSizedPrecision {
                ty,
                scale,
                precision: _,
                ..
            } => match ty {
                VarLenType::Decimaln | VarLenType::Numericn => {
                    let num = crate::protocol::Numeric::decode_sync(src, *scale)?;
                    Ok(SqlValue::Numeric(num))
                }
                _ => Err(crate::Error::Protocol(
                    format!("sync decode: unsupported VarLenSizedPrecision {:?}", ty).into(),
                )),
            },
            DataType::Xml { .. } => {
                // XML: read as PLP string
                let data = plp_decode_sync(src, 0xfffffffffffffffe)?;
                match data {
                    Some(buf) => {
                        let s = String::from_utf8(buf)
                            .map_err(|_| crate::Error::Encoding("invalid XML UTF-8".into()))?;
                        Ok(SqlValue::String(Some(std::borrow::Cow::Owned(s))))
                    }
                    None => Ok(SqlValue::Xml(None)),
                }
            }
        }
    }
}

fn decode_fixed_sqlvalue_sync<R: SyncProtocolReader>(
    src: &mut R,
    ty: &FixedLenType,
) -> crate::Result<SqlValue<'static>> {
    match ty {
        FixedLenType::Null => Ok(SqlValue::U8(None)),
        FixedLenType::Bit => Ok(SqlValue::Bit(Some(SyncProtocolReader::read_u8(src)? != 0))),
        FixedLenType::Int1 => Ok(SqlValue::U8(Some(SyncProtocolReader::read_u8(src)?))),
        FixedLenType::Int2 => Ok(SqlValue::I16(Some(src.read_i16_le()?))),
        FixedLenType::Int4 => Ok(SqlValue::I32(Some(src.read_i32_le()?))),
        FixedLenType::Int8 => Ok(SqlValue::I64(Some(src.read_i64_le()?))),
        FixedLenType::Float4 => Ok(SqlValue::F32(Some(src.read_f32_le()?))),
        FixedLenType::Float8 => Ok(SqlValue::F64(Some(src.read_f64_le()?))),
        FixedLenType::Datetime => {
            let dt = crate::temporal::DateTime::decode_sync(src)?;
            Ok(SqlValue::DateTime(Some(dt)))
        }
        FixedLenType::Datetime4 => {
            let dt = crate::temporal::SmallDateTime::decode_sync(src)?;
            Ok(SqlValue::SmallDateTime(Some(dt)))
        }
        FixedLenType::Money => {
            // 8 bytes: high i32, low u32
            let high = src.read_i32_le()? as i64;
            let low = src.read_u32_le()? as i64;
            let val = ((high as i128) << 32) | (low as i128);
            Ok(SqlValue::Numeric(Some(
                crate::protocol::Numeric::new_with_scale(val, 4),
            )))
        }
        FixedLenType::Money4 => {
            let val = src.read_i32_le()? as i128;
            Ok(SqlValue::Numeric(Some(
                crate::protocol::Numeric::new_with_scale(val, 4),
            )))
        }
    }
}

fn decode_varlen_sqlvalue_sync<R: SyncProtocolReader>(
    src: &mut R,
    vlc: &VarLenDescriptor,
) -> crate::Result<SqlValue<'static>> {
    let ty = vlc.r#type();
    let len = vlc.len();

    match ty {
        VarLenType::Bitn => {
            let recv_len = SyncProtocolReader::read_u8(src)?;
            match recv_len {
                0 => Ok(SqlValue::Bit(None)),
                1 => Ok(SqlValue::Bit(Some(SyncProtocolReader::read_u8(src)? > 0))),
                v => Err(crate::Error::Protocol(
                    format!("bitn: invalid length {}", v).into(),
                )),
            }
        }
        VarLenType::Intn => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match recv_len {
                0 => Ok(SqlValue::I32(None)),
                1 => Ok(SqlValue::U8(Some(SyncProtocolReader::read_u8(src)?))),
                2 => Ok(SqlValue::I16(Some(src.read_i16_le()?))),
                4 => Ok(SqlValue::I32(Some(src.read_i32_le()?))),
                8 => Ok(SqlValue::I64(Some(src.read_i64_le()?))),
                _ => unimplemented!(),
            }
        }
        VarLenType::Floatn => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match recv_len {
                0 => Ok(SqlValue::F64(None)),
                4 => Ok(SqlValue::F32(Some(src.read_f32_le()?))),
                8 => Ok(SqlValue::F64(Some(src.read_f64_le()?))),
                _ => Err(crate::Error::Protocol(
                    format!("floatn: invalid length {}", recv_len).into(),
                )),
            }
        }
        VarLenType::NChar | VarLenType::NVarchar | VarLenType::BigChar | VarLenType::BigVarChar => {
            let data = plp_decode_sync(src, len)?;
            match data {
                Some(buf) => {
                    if ty == VarLenType::NChar || ty == VarLenType::NVarchar {
                        let utf16: Vec<u16> = buf
                            .chunks_exact(2)
                            .map(|c| u16::from_le_bytes([c[0], c[1]]))
                            .collect();
                        let s = String::from_utf16(&utf16).map_err(|_| crate::Error::Utf16)?;
                        Ok(SqlValue::String(Some(std::borrow::Cow::Owned(s))))
                    } else {
                        let collation = vlc.collation();
                        let collation_ref = collation.as_ref().unwrap();
                        let encoder = collation_ref.encoding()?;
                        let s = encoder
                            .decode_without_bom_handling_and_without_replacement(&buf)
                            .ok_or_else(|| crate::Error::Encoding("invalid sequence".into()))?;
                        Ok(SqlValue::String(Some(std::borrow::Cow::Owned(
                            s.into_owned(),
                        ))))
                    }
                }
                None => Ok(SqlValue::String(None)),
            }
        }
        VarLenType::BigBinary | VarLenType::BigVarBin => {
            let data = plp_decode_sync(src, len)?;
            match data {
                Some(buf) => Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(buf)))),
                None => Ok(SqlValue::Binary(None)),
            }
        }
        VarLenType::Guid => {
            let recv_len = SyncProtocolReader::read_u8(src)? as usize;
            match recv_len {
                0 => Ok(SqlValue::Guid(None)),
                16 => {
                    let mut data = [0u8; 16];
                    src.read_exact_bytes(&mut data)?;
                    crate::protocol::wire::guid::reorder_bytes(&mut data);
                    Ok(SqlValue::Guid(Some(uuid::Uuid::from_bytes(data))))
                }
                _ => Err(crate::Error::Protocol(
                    format!("guid: invalid length {}", recv_len).into(),
                )),
            }
        }
        VarLenType::Text | VarLenType::NText | VarLenType::Image => {
            // Legacy types: read textptr + timestamp + data
            let textptr_len = SyncProtocolReader::read_u8(src)?;
            if textptr_len == 0 {
                return Ok(SqlValue::String(None));
            }
            let mut textptr = vec![0u8; textptr_len as usize];
            src.read_exact_bytes(&mut textptr)?;
            let mut timestamp = [0u8; 8];
            src.read_exact_bytes(&mut timestamp)?;
            let data_len = src.read_u32_le()? as usize;
            let mut data = vec![0u8; data_len];
            src.read_exact_bytes(&mut data)?;

            match ty {
                VarLenType::Text => {
                    let collation = vlc.collation();
                    let collation_ref = collation.as_ref().unwrap();
                    let encoder = collation_ref.encoding()?;
                    let s = encoder
                        .decode_without_bom_handling_and_without_replacement(&data)
                        .ok_or_else(|| crate::Error::Encoding("invalid sequence".into()))?;
                    Ok(SqlValue::String(Some(std::borrow::Cow::Owned(
                        s.into_owned(),
                    ))))
                }
                VarLenType::NText => {
                    let utf16: Vec<u16> = data
                        .chunks_exact(2)
                        .map(|c| u16::from_le_bytes([c[0], c[1]]))
                        .collect();
                    let s = String::from_utf16(&utf16).map_err(|_| crate::Error::Utf16)?;
                    Ok(SqlValue::String(Some(std::borrow::Cow::Owned(s))))
                }
                VarLenType::Image => Ok(SqlValue::Binary(Some(std::borrow::Cow::Owned(data)))),
                _ => unreachable!(),
            }
        }
        _ => Err(crate::Error::Protocol(
            format!("sync decode: unsupported varlen type {:?}", ty).into(),
        )),
    }
}

// ── Temporal decode (sync) ───────────────────────────────────────────

impl crate::temporal::DateTime {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let days = src.read_i32_le()?;
        let seconds_fragments = src.read_u32_le()?;
        Ok(Self::new(days, seconds_fragments))
    }
}

impl crate::temporal::SmallDateTime {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let days = src.read_u16_le()?;
        let seconds_fragments = src.read_u16_le()?;
        Ok(Self::new(days, seconds_fragments))
    }
}

impl crate::temporal::Date {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(src: &mut R) -> crate::Result<Self> {
        let mut bytes = [0u8; 4];
        src.read_exact_bytes(&mut bytes[..3])?;
        let days = u32::from_le_bytes(bytes);
        Ok(Self::new(days))
    }
}

impl crate::temporal::Time {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(
        src: &mut R,
        n: usize,
        rlen: usize,
    ) -> crate::Result<Self> {
        let val = match (n, rlen) {
            (0..=2, 3) => {
                let hi = src.read_u16_le()? as u64;
                let lo = SyncProtocolReader::read_u8(src)? as u64;
                hi | lo << 16
            }
            (3..=4, 4) => src.read_u32_le()? as u64,
            (5..=7, 5) => {
                let hi = src.read_u32_le()? as u64;
                let lo = SyncProtocolReader::read_u8(src)? as u64;
                hi | lo << 32
            }
            _ => {
                return Err(crate::Error::Protocol(
                    format!("timen: invalid length {}", n).into(),
                ));
            }
        };
        Ok(Self::new(val, n as u8))
    }
}

impl crate::temporal::DateTime2 {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(
        src: &mut R,
        n: usize,
        rlen: usize,
    ) -> crate::Result<Self> {
        let time = crate::temporal::Time::decode_sync(src, n, rlen)?;
        let date = crate::temporal::Date::decode_sync(src)?;
        Ok(Self::new(date, time))
    }
}

impl crate::temporal::DateTimeOffset {
    pub(crate) fn decode_sync<R: SyncProtocolReader>(
        src: &mut R,
        n: usize,
        rlen: u8,
    ) -> crate::Result<Self> {
        let dt2 = crate::temporal::DateTime2::decode_sync(src, n, rlen as usize)?;
        let offset = src.read_i16_le()?;
        Ok(Self::new(dt2, offset))
    }
}
