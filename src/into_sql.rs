use crate::{
    protocol::{Numeric, wire::SqlValue},
    xml::XmlData,
};
use std::borrow::Cow;
use uuid::Uuid;

/// A conversion trait to a TDS type.
///
/// A `IntoSql` implementation for a Rust type is needed for using it as a
/// parameter in the [`Client#query`] or [`Client#execute`] methods. The
/// following Rust types are already implemented to match the given server
/// types:
///
/// |Rust type|Server type|
/// |--------|--------|
/// |`u8`|`tinyint`|
/// |`i16`|`smallint`|
/// |`i32`|`int`|
/// |`i64`|`bigint`|
/// |`f32`|`float(24)`|
/// |`f64`|`float(53)`|
/// |`bool`|`bit`|
/// |`String`/`&str` (< 4000 characters)|`nvarchar(4000)`|
/// |`String`/`&str`|`nvarchar(max)`|
/// |`Vec<u8>`/`&[u8]` (< 8000 bytes)|`varbinary(8000)`|
/// |`Vec<u8>`/`&[u8]`|`varbinary(max)`|
/// |[`Uuid`]|`uniqueidentifier`|
/// |[`Numeric`]|`numeric`/`decimal`|
/// |[`Decimal`] (with feature flag `rust_decimal`)|`numeric`/`decimal`|
/// |[`BigDecimal`] (with feature flag `bigdecimal`)|`numeric`/`decimal`|
/// |[`XmlData`]|`xml`|
/// |[`NaiveDate`] (with `chrono` feature, TDS 7.3 >)|`date`|
/// |[`NaiveTime`] (with `chrono` feature, TDS 7.3 >)|`time`|
/// |[`DateTime`] (with `chrono` feature, TDS 7.3 >)|`datetimeoffset`|
/// |[`NaiveDateTime`] (with `chrono` feature, TDS 7.3 >)|`datetime2`|
/// |[`NaiveDateTime`] (with `chrono` feature, TDS 7.2)|`datetime`|
///
/// It is possible to use some of the types to write into columns that are not
/// of the same type. For example on systems following the TDS 7.3 standard (SQL
/// Server 2008 and later), the chrono type `NaiveDateTime` can also be used to
/// write to `datetime`, `datetime2` and `smalldatetime` columns. All string
/// types can also be used with `ntext`, `text`, `varchar`, `nchar` and `char`
/// columns. All binary types can also be used with `binary` and `image`
/// columns.
///
/// See the [`time`] module for more information about the date and time structs.
///
/// [`Client#query`]: struct.Client.html#method.query
/// [`Client#execute`]: struct.Client.html#method.execute
/// [`time`]: time/index.html
/// [`Uuid`]: struct.Uuid.html
/// [`Numeric`]: numeric/struct.Numeric.html
/// [`Decimal`]: numeric/struct.Decimal.html
/// [`BigDecimal`]: numeric/struct.BigDecimal.html
/// [`XmlData`]: xml/struct.XmlData.html
/// [`NaiveDateTime`]: time/chrono/struct.NaiveDateTime.html
/// [`NaiveDate`]: time/chrono/struct.NaiveDate.html
/// [`NaiveTime`]: time/chrono/struct.NaiveTime.html
/// [`DateTime`]: time/chrono/struct.DateTime.html
pub trait IntoSql: Send + Sync {
    /// Convert to a value understood by the SQL Server. Conversion
    /// by-reference.
    fn to_sql(&self) -> SqlValue<'_>;
}

/// A by-value conversion trait to a TDS type.
pub trait IntoSqlOwned<'a>: Send + Sync {
    /// Convert to a value understood by the SQL Server. Conversion by-value.
    fn into_sql(self) -> SqlValue<'a>;
}

impl<'a> IntoSqlOwned<'a> for &'a str {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(Some(Cow::Borrowed(self)))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a str> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(self.map(Cow::Borrowed))
    }
}

impl<'a> IntoSqlOwned<'a> for &'a String {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(Some(Cow::Borrowed(self)))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a String> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(self.map(Cow::from))
    }
}

impl<'a> IntoSqlOwned<'a> for &'a [u8] {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(Some(Cow::Borrowed(self)))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a [u8]> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(self.map(Cow::Borrowed))
    }
}

impl<'a> IntoSqlOwned<'a> for &'a Vec<u8> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(Some(Cow::from(self)))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a Vec<u8>> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(self.map(Cow::from))
    }
}

impl<'a> IntoSqlOwned<'a> for Cow<'a, str> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(Some(self))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<Cow<'a, str>> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::String(self)
    }
}

impl<'a> IntoSqlOwned<'a> for Cow<'a, [u8]> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(Some(self))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<Cow<'a, [u8]>> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Binary(self)
    }
}

impl<'a> IntoSqlOwned<'a> for &'a XmlData {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Xml(Some(Cow::Borrowed(self)))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a XmlData> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Xml(self.map(Cow::Borrowed))
    }
}

impl<'a> IntoSqlOwned<'a> for &'a Uuid {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Guid(Some(*self))
    }
}

impl<'a> IntoSqlOwned<'a> for Option<&'a Uuid> {
    fn into_sql(self) -> SqlValue<'a> {
        SqlValue::Guid(self.copied())
    }
}

into_sql!(self_,
          String: (SqlValue::String, Cow::from(self_));
          Vec<u8>: (SqlValue::Binary, Cow::from(self_));
          Numeric: (SqlValue::Numeric, self_);
          XmlData: (SqlValue::Xml, Cow::Owned(self_));
          Uuid: (SqlValue::Guid, self_);
          bool: (SqlValue::Bit, self_);
          u8: (SqlValue::U8, self_);
          i16: (SqlValue::I16, self_);
          i32: (SqlValue::I32, self_);
          i64: (SqlValue::I64, self_);
          f32: (SqlValue::F32, self_);
          f64: (SqlValue::F64, self_);
);

to_sql!(self_,
        bool: (SqlValue::Bit, *self_);
        u8: (SqlValue::U8, *self_);
        i16: (SqlValue::I16, *self_);
        i32: (SqlValue::I32, *self_);
        i64: (SqlValue::I64, *self_);
        f32: (SqlValue::F32, *self_);
        f64: (SqlValue::F64, *self_);
        &str: (SqlValue::String, Cow::from(*self_));
        String: (SqlValue::String, Cow::from(self_));
        Cow<'_, str>: (SqlValue::String, self_.clone());
        &[u8]: (SqlValue::Binary, Cow::from(*self_));
        Cow<'_, [u8]>: (SqlValue::Binary, self_.clone());
        Vec<u8>: (SqlValue::Binary, Cow::from(self_));
        Numeric: (SqlValue::Numeric, *self_);
        XmlData: (SqlValue::Xml, Cow::Borrowed(self_));
        Uuid: (SqlValue::Guid, *self_);
);
