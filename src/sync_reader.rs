//! Synchronous protocol reader trait — the sync equivalent of `ProtocolReader`.
//!
//! This module provides `SyncProtocolReader`, a blocking I/O trait that mirrors
//! the async `ProtocolReader`. It reads from a contiguous byte buffer that is
//! fed by TDS packet reassembly in `SyncConnection`.

use crate::protocol::Context;
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Read;

/// Synchronous equivalent of [`ProtocolReader`](crate::ProtocolReader).
///
/// Provides typed read helpers over a blocking byte stream. Implementations
/// must supply `context()`, `context_mut()`, and the `Read` trait.
pub trait SyncProtocolReader: Read {
    fn context(&self) -> &Context;
    fn context_mut(&mut self) -> &mut Context;

    fn read_u8(&mut self) -> crate::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    fn read_i8(&mut self) -> crate::Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16_le(&mut self) -> crate::Result<u16> {
        Ok(ReadBytesExt::read_u16::<LittleEndian>(self)?)
    }

    fn read_i16_le(&mut self) -> crate::Result<i16> {
        Ok(ReadBytesExt::read_i16::<LittleEndian>(self)?)
    }

    fn read_u32_le(&mut self) -> crate::Result<u32> {
        Ok(ReadBytesExt::read_u32::<LittleEndian>(self)?)
    }

    fn read_i32_le(&mut self) -> crate::Result<i32> {
        Ok(ReadBytesExt::read_i32::<LittleEndian>(self)?)
    }

    fn read_u64_le(&mut self) -> crate::Result<u64> {
        Ok(ReadBytesExt::read_u64::<LittleEndian>(self)?)
    }

    fn read_i64_le(&mut self) -> crate::Result<i64> {
        Ok(ReadBytesExt::read_i64::<LittleEndian>(self)?)
    }

    fn read_u32_be(&mut self) -> crate::Result<u32> {
        Ok(ReadBytesExt::read_u32::<byteorder::BigEndian>(self)?)
    }

    fn read_f32_le(&mut self) -> crate::Result<f32> {
        Ok(ReadBytesExt::read_f32::<LittleEndian>(self)?)
    }

    fn read_f64_le(&mut self) -> crate::Result<f64> {
        Ok(ReadBytesExt::read_f64::<LittleEndian>(self)?)
    }

    /// Read a B_VARCHAR: u8 length prefix (in UTF-16 chars), then that many u16 values.
    fn read_b_varchar(&mut self) -> crate::Result<String> {
        let len = SyncProtocolReader::read_u8(self)? as usize;
        let mut buf = vec![0u16; len];
        for item in buf.iter_mut() {
            *item = self.read_u16_le()?;
        }
        String::from_utf16(&buf)
            .map_err(|_| crate::Error::Encoding("Invalid UTF-16 data in b_varchar".into()))
    }

    /// Read a US_VARCHAR: u16 length prefix (in UTF-16 chars), then that many u16 values.
    fn read_us_varchar(&mut self) -> crate::Result<String> {
        let len = self.read_u16_le()? as usize;
        let mut buf = vec![0u16; len];
        for item in buf.iter_mut() {
            *item = self.read_u16_le()?;
        }
        String::from_utf16(&buf)
            .map_err(|_| crate::Error::Encoding("Invalid UTF-16 data in us_varchar".into()))
    }

    /// Read exactly `n` bytes.
    fn read_exact_bytes(&mut self, buf: &mut [u8]) -> crate::Result<()> {
        self.read_exact(buf).map_err(|e| e.into())
    }
}
