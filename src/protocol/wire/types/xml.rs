use std::{borrow::Cow, sync::Arc};

use crate::{
    SqlValue, VarLenType,
    protocol::reader::ProtocolReader,
    xml::{XmlData, XmlSchema},
};

pub(crate) async fn decode<R>(
    src: &mut R,
    len: usize,
    schema: Option<Arc<XmlSchema>>,
) -> crate::Result<SqlValue<'static>>
where
    R: ProtocolReader + Unpin,
{
    let xml = super::string::decode(src, VarLenType::Xml, len, None)
        .await?
        .map(|data| {
            let mut data = XmlData::new(data);

            if let Some(schema) = schema {
                data.set_schema(schema);
            }

            Cow::Owned(data)
        });

    Ok(SqlValue::Xml(xml))
}
