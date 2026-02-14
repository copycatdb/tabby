use crate::{
    FromServer,
    error::Error,
    protocol::wire::{DataType, FixedLenType, RowMessage, SqlValue, VarLenType},
};
use std::{fmt::Display, sync::Arc};

/// A column of data from a query result.
///
/// Provides the column name, its [`ColumnType`], optional detailed
/// [`DataType`](crate::DataType) information, and nullability.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
    pub type_info: Option<crate::protocol::wire::DataType>,
    pub nullable: Option<bool>,
}

impl Column {
    /// Construct a new Column.
    pub fn new(name: String, column_type: ColumnType) -> Self {
        Self {
            name,
            column_type,
            type_info: None,
            nullable: None,
        }
    }

    /// The name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The type of the column.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    /// The detailed type information, if available.
    pub fn type_info(&self) -> Option<&crate::protocol::wire::DataType> {
        self.type_info.as_ref()
    }

    /// Whether the column is nullable, if known.
    pub fn nullable(&self) -> Option<bool> {
        self.nullable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The high-level SQL Server column type.
///
/// This is a simplified view of the underlying TDS data type. For full
/// details (precision, scale, collation), see [`Column::type_info`].
pub enum ColumnType {
    /// The column doesn't have a specified type.
    Null,
    /// A bit or boolean value.
    Bit,
    /// An 8-bit integer value.
    Int1,
    /// A 16-bit integer value.
    Int2,
    /// A 32-bit integer value.
    Int4,
    /// A 64-bit integer value.
    Int8,
    /// A 32-bit datetime value.
    Datetime4,
    /// A 32-bit floating point value.
    Float4,
    /// A 64-bit floating point value.
    Float8,
    /// Money value.
    Money,
    /// A TDS 7.2 datetime value.
    Datetime,
    /// A 32-bit money value.
    Money4,
    /// A unique identifier, UUID.
    Guid,
    /// N-bit integer value (variable).
    Intn,
    /// A bit value in a variable-length type.
    Bitn,
    /// A decimal value (same as `Numericn`).
    Decimaln,
    /// A numeric value (same as `Decimaln`).
    Numericn,
    /// A n-bit floating point value.
    Floatn,
    /// A n-bit datetime value (TDS 7.2).
    Datetimen,
    /// A n-bit date value (TDS 7.3).
    Daten,
    /// A n-bit time value (TDS 7.3).
    Timen,
    /// A n-bit datetime2 value (TDS 7.3).
    Datetime2,
    /// A n-bit datetime value with an offset (TDS 7.3).
    DatetimeOffsetn,
    /// A variable binary value.
    BigVarBin,
    /// A large variable string value.
    BigVarChar,
    /// A binary value.
    BigBinary,
    /// A string value.
    BigChar,
    /// A variable string value with UTF-16 encoding.
    NVarchar,
    /// A string value with UTF-16 encoding.
    NChar,
    /// A XML value.
    Xml,
    /// User-defined type.
    Udt,
    /// A text value (deprecated).
    Text,
    /// A image value (deprecated).
    Image,
    /// A text value with UTF-16 encoding (deprecated).
    NText,
    /// An SQL variant type.
    SSVariant,
}

impl From<&DataType> for ColumnType {
    fn from(ti: &DataType) -> Self {
        match ti {
            DataType::FixedLen(flt) => match flt {
                FixedLenType::Int1 => Self::Int1,
                FixedLenType::Bit => Self::Bit,
                FixedLenType::Int2 => Self::Int2,
                FixedLenType::Int4 => Self::Int4,
                FixedLenType::Datetime4 => Self::Datetime4,
                FixedLenType::Float4 => Self::Float4,
                FixedLenType::Money => Self::Money,
                FixedLenType::Datetime => Self::Datetime,
                FixedLenType::Float8 => Self::Float8,
                FixedLenType::Money4 => Self::Money4,
                FixedLenType::Int8 => Self::Int8,
                FixedLenType::Null => Self::Null,
            },
            DataType::VarLenSized(cx) => match cx.r#type() {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => match cx.len() {
                    1 => Self::Int1,
                    2 => Self::Int2,
                    4 => Self::Int4,
                    8 => Self::Int8,
                    _ => Self::Intn,
                },
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => match cx.len() {
                    4 => Self::Float4,
                    8 => Self::Float8,
                    _ => Self::Floatn,
                },
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            DataType::VarLenSizedPrecision { ty, .. } => match ty {
                VarLenType::Guid => Self::Guid,
                VarLenType::Intn => Self::Intn,
                VarLenType::Bitn => Self::Bitn,
                VarLenType::Decimaln => Self::Decimaln,
                VarLenType::Numericn => Self::Numericn,
                VarLenType::Floatn => Self::Floatn,
                VarLenType::Money => Self::Money,
                VarLenType::Datetimen => Self::Datetimen,
                VarLenType::Daten => Self::Daten,
                VarLenType::Timen => Self::Timen,
                VarLenType::Datetime2 => Self::Datetime2,
                VarLenType::DatetimeOffsetn => Self::DatetimeOffsetn,
                VarLenType::BigVarBin => Self::BigVarBin,
                VarLenType::BigVarChar => Self::BigVarChar,
                VarLenType::BigBinary => Self::BigBinary,
                VarLenType::BigChar => Self::BigChar,
                VarLenType::NVarchar => Self::NVarchar,
                VarLenType::NChar => Self::NChar,
                VarLenType::Xml => Self::Xml,
                VarLenType::Udt => Self::Udt,
                VarLenType::Text => Self::Text,
                VarLenType::Image => Self::Image,
                VarLenType::NText => Self::NText,
                VarLenType::SSVariant => Self::SSVariant,
            },
            DataType::Xml { .. } => Self::Xml,
        }
    }
}

/// A single row of data returned from a query.
///
/// Access column values by name or zero-based index using [`get`](Self::get)
/// (panics on error) or [`try_get`](Self::try_get) (returns `Result`).
/// Nullable columns should be read as `Option<T>`.
///
/// The row also implements [`IntoIterator`], yielding owned [`SqlValue`]s.
///
/// # Example
///
/// ```no_run
/// # use tabby::{AuthMethod, Client, Config};
/// # use tokio_util::compat::TokioAsyncWriteCompatExt;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let mut config = Config::new();
/// # config.host("localhost");
/// # config.authentication(AuthMethod::sql_server("sa", "password"));
/// # config.trust_cert();
/// # let tcp = tokio::net::TcpStream::connect(config.get_addr()).await?;
/// # tcp.set_nodelay(true)?;
/// # let mut client = Client::connect(config, tcp.compat_write()).await?;
/// let row = client
///     .execute("SELECT @P1 AS id, @P2 AS name", &[&1i32, &"Alice"])
///     .await?
///     .into_row()
///     .await?
///     .unwrap();
///
/// // By name
/// let name: &str = row.get("name").unwrap();
///
/// // By index
/// let id: i32 = row.get(0).unwrap();
///
/// // Nullable column — get already returns Option
/// let maybe: Option<&str> = row.get("name");
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Row {
    pub columns: Arc<Vec<Column>>,
    pub data: RowMessage<'static>,
    pub result_index: usize,
}

pub trait QueryIdx
where
    Self: Display,
{
    fn idx(&self, row: &Row) -> Option<usize>;
}

impl QueryIdx for usize {
    fn idx(&self, _row: &Row) -> Option<usize> {
        Some(*self)
    }
}

impl QueryIdx for &str {
    fn idx(&self, row: &Row) -> Option<usize> {
        row.columns.iter().position(|c| c.name() == *self)
    }
}

impl Row {
    /// Columns defining the row data. Columns listed here are in the same order
    /// as the resulting data.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Return an iterator over row column-value pairs.
    pub fn cells(&self) -> impl Iterator<Item = (&Column, &SqlValue<'static>)> {
        self.columns().iter().zip(self.data.iter())
    }

    /// The result set number, starting from zero and increasing if the stream
    /// has results from more than one query.
    pub fn result_index(&self) -> usize {
        self.result_index
    }

    /// Returns the number of columns in the row.
    /// Returns the number of columns in the row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Retrieve a column value by index (zero-based `usize`) or by column name
    /// (`&str`). Returns `Some(value)` or `None` for SQL `NULL`.
    ///
    /// # Panics
    ///
    /// Panics if the column does not exist or the type conversion fails.
    /// Use [`try_get`](Self::try_get) for a non-panicking alternative.
    #[track_caller]
    pub fn get<'a, R, I>(&'a self, idx: I) -> Option<R>
    where
        R: FromServer<'a>,
        I: QueryIdx,
    {
        self.try_get(idx).unwrap()
    }

    /// Retrieve a column's value for a given column index.
    #[track_caller]
    pub fn try_get<'a, R, I>(&'a self, idx: I) -> crate::Result<Option<R>>
    where
        R: FromServer<'a>,
        I: QueryIdx,
    {
        let idx = idx.idx(self).ok_or_else(|| {
            Error::Conversion(format!("Could not find column with index {}", idx).into())
        })?;

        let data = self.data.get(idx).unwrap();

        R::from_sql(data)
    }
}

impl IntoIterator for Row {
    type Item = SqlValue<'static>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
