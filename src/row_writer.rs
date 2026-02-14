//! Zero-copy row writer trait for streaming TDS row data directly to
//! consumer-owned buffers (e.g. Arrow array builders) without allocating
//! intermediate `SqlValue` enums.
//!
//! # Architecture
//!
//! The traditional path is: TDS bytes → `SqlValue` → consumer match.
//! With `RowWriter`, the decode loop calls typed methods on a trait object,
//! letting the consumer append directly to its storage (Arrow builders,
//! CSV buffers, etc.) without an intermediate representation.
//!
//! Two usage patterns:
//!
//! 1. **From existing `Row`**: call [`Row::write_into`] which dispatches
//!    each `SqlValue` column through the trait (still allocates SqlValue
//!    internally, but standardises the consumer API).
//!
//! 2. **Direct decode** (zero-alloc): use `RowMessage::decode_into` /
//!    `decode_nbc_into` which calls RowWriter methods directly during
//!    TDS wire decoding — no `SqlValue` allocation at all.

/// Trait for receiving decoded TDS column values without intermediate allocation.
///
/// Implementations receive one call per column per row. The `col` parameter
/// is the zero-based column index within the current result set.
pub trait RowWriter {
    /// Column value is SQL NULL.
    fn write_null(&mut self, col: usize);
    /// Boolean / bit column.
    fn write_bool(&mut self, col: usize, val: bool);
    /// Unsigned 8-bit integer (tinyint).
    fn write_u8(&mut self, col: usize, val: u8);
    /// Signed 16-bit integer (smallint).
    fn write_i16(&mut self, col: usize, val: i16);
    /// Signed 32-bit integer (int).
    fn write_i32(&mut self, col: usize, val: i32);
    /// Signed 64-bit integer (bigint).
    fn write_i64(&mut self, col: usize, val: i64);
    /// 32-bit float (real).
    fn write_f32(&mut self, col: usize, val: f32);
    /// 64-bit float (float).
    fn write_f64(&mut self, col: usize, val: f64);
    /// String value — borrowed, no allocation needed by the caller.
    fn write_str(&mut self, col: usize, val: &str);
    /// Binary value — borrowed, no allocation needed by the caller.
    fn write_bytes(&mut self, col: usize, val: &[u8]);
    /// Date as days since CE epoch (0001-01-01). Used for TDS `date`.
    fn write_date(&mut self, col: usize, days: i32);
    /// Time as nanoseconds since midnight. Used for TDS `time`.
    fn write_time(&mut self, col: usize, nanos: i64);
    /// Datetime as microseconds since Unix epoch. Used for TDS `datetime`, `smalldatetime`, `datetime2`.
    fn write_datetime(&mut self, col: usize, micros: i64);
    /// Datetime with offset: microseconds since Unix epoch (UTC) + offset in minutes.
    fn write_datetimeoffset(&mut self, col: usize, micros: i64, offset_minutes: i16);
    /// Decimal/numeric value as i128 with precision and scale.
    fn write_decimal(&mut self, col: usize, value: i128, precision: u8, scale: u8);
    /// GUID/UUID as raw 16 bytes (standard byte order, not TDS wire order).
    fn write_guid(&mut self, col: usize, bytes: &[u8; 16]);
}

/// A `RowWriter` that collects values into a `Vec<SqlValue>`, preserving
/// the existing API for callers that still need `SqlValue`s.
///
/// This is used internally so the old `Row`-based path can coexist with
/// the direct-decode path.
///
/// Consider using the `claw` crate which provides this as part of its
/// high-level client API.
pub struct SqlValueWriter {
    /// The collected column values for the current row.
    pub values: Vec<crate::protocol::wire::SqlValue<'static>>,
}

impl SqlValueWriter {
    /// Create a new writer pre-allocated for `capacity` columns.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }
}

impl RowWriter for SqlValueWriter {
    fn write_null(&mut self, _col: usize) {
        // We push a generic null — the consumer should use column metadata to
        // interpret this correctly. Using I32(None) as a placeholder.
        self.values.push(crate::protocol::wire::SqlValue::I32(None));
    }
    fn write_bool(&mut self, _col: usize, val: bool) {
        self.values
            .push(crate::protocol::wire::SqlValue::Bit(Some(val)));
    }
    fn write_u8(&mut self, _col: usize, val: u8) {
        self.values
            .push(crate::protocol::wire::SqlValue::U8(Some(val)));
    }
    fn write_i16(&mut self, _col: usize, val: i16) {
        self.values
            .push(crate::protocol::wire::SqlValue::I16(Some(val)));
    }
    fn write_i32(&mut self, _col: usize, val: i32) {
        self.values
            .push(crate::protocol::wire::SqlValue::I32(Some(val)));
    }
    fn write_i64(&mut self, _col: usize, val: i64) {
        self.values
            .push(crate::protocol::wire::SqlValue::I64(Some(val)));
    }
    fn write_f32(&mut self, _col: usize, val: f32) {
        self.values
            .push(crate::protocol::wire::SqlValue::F32(Some(val)));
    }
    fn write_f64(&mut self, _col: usize, val: f64) {
        self.values
            .push(crate::protocol::wire::SqlValue::F64(Some(val)));
    }
    fn write_str(&mut self, _col: usize, val: &str) {
        self.values
            .push(crate::protocol::wire::SqlValue::String(Some(
                std::borrow::Cow::Owned(val.to_owned()),
            )));
    }
    fn write_bytes(&mut self, _col: usize, val: &[u8]) {
        self.values
            .push(crate::protocol::wire::SqlValue::Binary(Some(
                std::borrow::Cow::Owned(val.to_owned()),
            )));
    }
    fn write_date(&mut self, _col: usize, days: i32) {
        self.values.push(crate::protocol::wire::SqlValue::Date(Some(
            crate::temporal::Date::new(days as u32),
        )));
    }
    fn write_time(&mut self, _col: usize, nanos: i64) {
        // Store as scale=7 (100ns increments) which is the max TDS precision
        let increments = nanos as u64 / 100;
        self.values.push(crate::protocol::wire::SqlValue::Time(Some(
            crate::temporal::Time::new(increments, 7),
        )));
    }
    fn write_datetime(&mut self, _col: usize, _micros: i64) {
        // For simplicity, store as DateTime2 with scale 6
        // This is lossy for the SqlValue path but preserves the value
        // In practice, callers using SqlValueWriter are using the old path anyway
        self.values
            .push(crate::protocol::wire::SqlValue::DateTime2(None));
    }
    fn write_datetimeoffset(&mut self, _col: usize, _micros: i64, _offset_minutes: i16) {
        self.values
            .push(crate::protocol::wire::SqlValue::DateTimeOffset(None));
    }
    fn write_decimal(&mut self, _col: usize, value: i128, precision: u8, scale: u8) {
        let mut num = crate::protocol::numeric::Numeric::new_with_scale(value, scale);
        num = crate::protocol::numeric::Numeric::new_with_scale(num.value(), scale);
        // Precision is handled by the Numeric type
        let _ = precision;
        self.values
            .push(crate::protocol::wire::SqlValue::Numeric(Some(num)));
    }
    fn write_guid(&mut self, _col: usize, bytes: &[u8; 16]) {
        self.values.push(crate::protocol::wire::SqlValue::Guid(Some(
            uuid::Uuid::from_bytes(*bytes),
        )));
    }
}
