//! The XML containers
use super::wire::Encode;
use bytes::{BufMut, BytesMut};
use std::borrow::BorrowMut;
use std::sync::Arc;

/// Provides information of the location for the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlSchema {
    db_name: String,
    owner: String,
    collection: String,
}

impl XmlSchema {
    pub(crate) fn new(
        db_name: impl ToString,
        owner: impl ToString,
        collection: impl ToString,
    ) -> Self {
        Self {
            db_name: db_name.to_string(),
            owner: owner.to_string(),
            collection: collection.to_string(),
        }
    }

    /// Specifies the name of the database where the schema collection is defined.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }

    /// Specifies the name of the relational schema containing the schema collection.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Specifies the name of the XML schema collection to which the type is
    /// bound.
    pub fn collection(&self) -> &str {
        &self.collection
    }
}

/// A representation of XML data in TDS. Holds the data as a UTF-8 string and
/// and optional information about the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XmlData {
    data: String,
    schema: Option<Arc<XmlSchema>>,
}

impl XmlData {
    /// Create a new XmlData with the given string. Validation of the XML data
    /// happens in the database.
    pub fn new(data: impl ToString) -> Self {
        Self {
            data: data.to_string(),
            schema: None,
        }
    }

    pub(crate) fn set_schema(&mut self, schema: Arc<XmlSchema>) {
        self.schema = Some(schema);
    }

    /// Returns information about the schema of the XML file, if existing.
    #[allow(clippy::option_as_ref_deref)]
    pub fn schema(&self) -> Option<&XmlSchema> {
        self.schema.as_ref().map(|s| &**s)
    }

    /// Takes the XML string out from the struct.
    pub fn into_string(self) -> String {
        self.data
    }
}

impl std::fmt::Display for XmlData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.data)
    }
}

impl AsRef<str> for XmlData {
    fn as_ref(&self) -> &str {
        self.data.as_ref()
    }
}

impl Encode<BytesMut> for XmlData {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        // unknown size
        dst.put_u64_le(0xfffffffffffffffe_u64);

        // first blob
        let mut length = 0u32;
        let len_pos = dst.len();

        // writing the length later
        dst.put_u32_le(length);

        for chr in self.data.encode_utf16() {
            length += 1;
            dst.put_u16_le(chr);
        }

        // PLP_TERMINATOR, no next blobs
        dst.put_u32_le(0);

        let dst: &mut [u8] = dst.borrow_mut();
        let mut dst = &mut dst[len_pos..];
        dst.put_u32_le(length * 2);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_data_new() {
        let xml = XmlData::new("<root/>");
        assert_eq!("<root/>", xml.as_ref());
    }

    #[test]
    fn xml_data_display() {
        let xml = XmlData::new("<a>b</a>");
        assert_eq!("<a>b</a>", format!("{}", xml));
    }

    #[test]
    fn xml_data_into_string() {
        let xml = XmlData::new("<test/>");
        assert_eq!("<test/>", xml.into_string());
    }

    #[test]
    fn xml_data_schema_none() {
        let xml = XmlData::new("<x/>");
        assert!(xml.schema().is_none());
    }

    #[test]
    fn xml_data_with_schema() {
        let mut xml = XmlData::new("<x/>");
        let schema = Arc::new(XmlSchema::new("mydb", "dbo", "mycoll"));
        xml.set_schema(schema);
        let s = xml.schema().unwrap();
        assert_eq!("mydb", s.db_name());
        assert_eq!("dbo", s.owner());
        assert_eq!("mycoll", s.collection());
    }

    #[test]
    fn xml_data_clone_eq() {
        let x1 = XmlData::new("<a/>");
        let x2 = x1.clone();
        assert_eq!(x1, x2);
    }

    #[test]
    fn xml_data_encode() {
        let xml = XmlData::new("hi");
        let mut buf = BytesMut::new();
        xml.encode(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn xml_schema_debug() {
        let s = XmlSchema::new("db", "own", "coll");
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("db"));
    }
}
