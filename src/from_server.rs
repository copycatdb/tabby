use crate::{SqlValue, protocol::Numeric, xml::XmlData};
use uuid::Uuid;

/// A conversion trait from a TDS type by-reference.
///
/// A `FromServer` implementation for a Rust type is needed for using it as a
/// return parameter from [`Row#get`] or [`Row#try_get`] methods. The following
/// Rust types are already implemented to match the given server types:
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
/// |`String`/`&str`|`nvarchar`/`varchar`/`nchar`/`char`/`ntext`/`text`|
/// |`Vec<u8>`/`&[u8]`|`binary`/`varbinary`/`image`|
/// |[`Uuid`]|`uniqueidentifier`|
/// |[`Numeric`]|`numeric`/`decimal`|
/// |[`Decimal`] (with feature flag `rust_decimal`)|`numeric`/`decimal`|
/// |[`XmlData`]|`xml`|
/// |[`NaiveDateTime`] (with feature flag `chrono`)|`datetime`/`datetime2`/`smalldatetime`|
/// |[`NaiveDate`] (with feature flag `chrono`)|`date`|
/// |[`NaiveTime`] (with feature flag `chrono`)|`time`|
/// |[`DateTime`] (with feature flag `chrono`)|`datetimeoffset`|
///
/// See the [`time`] module for more information about the date and time structs.
///
/// [`Row#get`]: struct.Row.html#method.get
/// [`Row#try_get`]: struct.Row.html#method.try_get
/// [`time`]: time/index.html
/// [`Uuid`]: struct.Uuid.html
/// [`Numeric`]: numeric/struct.Numeric.html
/// [`Decimal`]: numeric/struct.Decimal.html
/// [`XmlData`]: xml/struct.XmlData.html
/// [`NaiveDateTime`]: time/chrono/struct.NaiveDateTime.html
/// [`NaiveDate`]: time/chrono/struct.NaiveDate.html
/// [`NaiveTime`]: time/chrono/struct.NaiveTime.html
/// [`DateTime`]: time/chrono/struct.DateTime.html
pub trait FromServer<'a>
where
    Self: Sized + 'a,
{
    /// Returns the value, `None` being a null value, copying the value.
    fn from_sql(value: &'a SqlValue<'static>) -> crate::Result<Option<Self>>;
}

/// A conversion trait from a TDS type by-value.
pub trait FromServerOwned
where
    Self: Sized,
{
    /// Returns the value, `None` being a null value, taking the ownership.
    fn from_sql_owned(value: SqlValue<'static>) -> crate::Result<Option<Self>>;
}

from_sql!(bool: SqlValue::Bit(val) => (*val, val));
from_sql!(u8: SqlValue::U8(val) => (*val, val), SqlValue::I32(None) => (None, None));
from_sql!(i16: SqlValue::I16(val) => (*val, val), SqlValue::U8(None) => (None, None), SqlValue::I32(None) => (None, None));
from_sql!(i32: SqlValue::I32(val) => (*val, val), SqlValue::U8(None) => (None, None));
from_sql!(i64: SqlValue::I64(val) => (*val, val), SqlValue::U8(None) => (None, None), SqlValue::I32(None) => (None, None));
from_sql!(f32: SqlValue::F32(val) => (*val, val));
from_sql!(f64: SqlValue::F64(val) => (*val, val));
from_sql!(Uuid: SqlValue::Guid(val) => (*val, val));
from_sql!(Numeric: SqlValue::Numeric(n) => (*n, n));

impl FromServerOwned for XmlData {
    fn from_sql_owned(value: SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::Xml(data) => Ok(data.map(|data| data.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromServer<'a> for &'a XmlData {
    fn from_sql(value: &'a SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::Xml(data) => Ok(data.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl FromServerOwned for String {
    fn from_sql_owned(value: SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::String(s) => Ok(s.map(|s| s.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromServer<'a> for &'a str {
    fn from_sql(value: &'a SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::String(s) => Ok(s.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl FromServerOwned for Vec<u8> {
    fn from_sql_owned(value: SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::Binary(b) => Ok(b.map(|s| s.into_owned())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a String value", v).into(),
            )),
        }
    }
}

impl<'a> FromServer<'a> for &'a [u8] {
    fn from_sql(value: &'a SqlValue<'static>) -> crate::Result<Option<Self>> {
        match value {
            SqlValue::Binary(b) => Ok(b.as_ref().map(|s| s.as_ref())),
            v => Err(crate::Error::Conversion(
                format!("cannot interpret {:?} as a &[u8] value", v).into(),
            )),
        }
    }
}
