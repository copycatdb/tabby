/// UUIDs use network byte order (big endian) for the first 3 groups,
/// while GUIDs use native byte order (little endian).
///
/// https://github.com/microsoft/mssql-jdbc/blob/bec39dbba9544aef5f5f6a5495d5acf533efd6da/src/main/java/com/microsoft/sqlserver/jdbc/Util.java#L708-L730
pub(crate) fn reorder_bytes(bytes: &mut uuid::Bytes) {
    bytes.swap(0, 3);
    bytes.swap(1, 2);
    bytes.swap(4, 5);
    bytes.swap(6, 7);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reorder_bytes_roundtrip() {
        let original: uuid::Bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut bytes = original;
        reorder_bytes(&mut bytes);
        // First group reversed
        assert_eq!(bytes[0], 4);
        assert_eq!(bytes[1], 3);
        assert_eq!(bytes[2], 2);
        assert_eq!(bytes[3], 1);
        // Second group reversed
        assert_eq!(bytes[4], 6);
        assert_eq!(bytes[5], 5);
        // Third group reversed
        assert_eq!(bytes[6], 8);
        assert_eq!(bytes[7], 7);
        // Rest unchanged
        assert_eq!(&bytes[8..], &original[8..]);
        // Double reorder = original
        reorder_bytes(&mut bytes);
        assert_eq!(bytes, original);
    }
}
