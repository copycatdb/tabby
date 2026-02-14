//! Error module
pub use crate::protocol::wire::ServerError;
pub use std::io::ErrorKind as IoErrorKind;
use std::{borrow::Cow, convert::Infallible, io};
use thiserror::Error;

/// A unified error enum that contains several errors that might occurr during
/// the lifecycle of this driver
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum Error {
    #[error("An error occured during the attempt of performing I/O: {}", message)]
    /// An error occured when performing I/O to the server.
    Io {
        /// A list specifying general categories of I/O error.
        kind: IoErrorKind,
        /// The error description.
        message: String,
    },
    #[error("Protocol error: {}", _0)]
    /// An error happened during the request or response parsing.
    Protocol(Cow<'static, str>),
    #[error("Encoding error: {}", _0)]
    /// Server responded with encoding not supported.
    Encoding(Cow<'static, str>),
    #[error("Conversion error: {}", _0)]
    /// Conversion failure from one type to another.
    Conversion(Cow<'static, str>),
    #[error("UTF-8 error")]
    /// Tried to convert data to UTF-8 that was not valid.
    Utf8,
    #[error("UTF-16 error")]
    /// Tried to convert data to UTF-16 that was not valid.
    Utf16,
    #[error("Error parsing an integer: {}", _0)]
    /// Tried to parse an integer that was not an integer.
    ParseInt(std::num::ParseIntError),
    #[error("Token error: {}", _0)]
    /// An error returned by the server.
    Server(ServerError),
    #[error("Error forming TLS connection: {}", _0)]
    /// An error in the TLS handshake.
    Tls(String),
    #[cfg(any(all(unix, feature = "integrated-auth-gssapi"), doc))]
    #[cfg_attr(
        feature = "docs",
        doc(cfg(all(unix, feature = "integrated-auth-gssapi")))
    )]
    /// An error from the GSSAPI library.
    #[error("GSSAPI Error: {}", _0)]
    Gssapi(String),
    #[error(
        "Server requested a connection to an alternative address: `{}:{}`",
        host,
        port
    )]
    /// Server requested a connection to an alternative address.
    Routing {
        /// The requested hostname
        host: String,
        /// The requested port.
        port: u16,
    },
    #[error("BULK UPLOAD input failure: {0}")]
    /// Invalid input in Bulk Upload
    BulkInput(Cow<'static, str>),
}

impl Error {
    /// True, if the error was caused by a deadlock.
    pub fn is_deadlock(&self) -> bool {
        self.code().map(|c| c == 1205).unwrap_or(false)
    }

    /// Returns the error code, if the error originates from the
    /// server.
    pub fn code(&self) -> Option<u32> {
        match self {
            Error::Server(e) => Some(e.code()),
            _ => None,
        }
    }
}

impl From<uuid::Error> for Error {
    fn from(e: uuid::Error) -> Self {
        Self::Conversion(format!("Error convertiong a Guid value {}", e).into())
    }
}

#[cfg(feature = "native-tls")]
impl From<async_native_tls::Error> for Error {
    fn from(v: async_native_tls::Error) -> Self {
        Error::Tls(format!("{}", v))
    }
}

#[cfg(feature = "vendored-openssl")]
impl From<opentls::Error> for Error {
    fn from(v: opentls::Error) -> Self {
        Error::Tls(format!("{}", v))
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Self::Io {
            kind: err.kind(),
            message: format!("{}", err),
        }
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::ParseInt(err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_: std::str::Utf8Error) -> Error {
        Error::Utf8
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_err: std::string::FromUtf8Error) -> Error {
        Error::Utf8
    }
}

impl From<std::string::FromUtf16Error> for Error {
    fn from(_err: std::string::FromUtf16Error) -> Error {
        Error::Utf16
    }
}

impl From<connection_string::Error> for Error {
    fn from(err: connection_string::Error) -> Error {
        let err = Cow::Owned(format!("{}", err));
        Error::Conversion(err)
    }
}

#[cfg(all(unix, feature = "integrated-auth-gssapi"))]
#[cfg_attr(
    feature = "docs",
    doc(cfg(all(unix, feature = "integrated-auth-gssapi")))
)]
impl From<libgssapi::error::Error> for Error {
    fn from(err: libgssapi::error::Error) -> Error {
        Error::Gssapi(format!("{}", err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_io() {
        let err = Error::Io {
            kind: IoErrorKind::ConnectionRefused,
            message: "refused".into(),
        };
        assert!(format!("{}", err).contains("refused"));
    }

    #[test]
    fn error_display_protocol() {
        let err = Error::Protocol("bad protocol".into());
        assert!(format!("{}", err).contains("bad protocol"));
    }

    #[test]
    fn error_display_encoding() {
        let err = Error::Encoding("bad enc".into());
        assert!(format!("{}", err).contains("bad enc"));
    }

    #[test]
    fn error_display_conversion() {
        let err = Error::Conversion("bad conv".into());
        assert!(format!("{}", err).contains("bad conv"));
    }

    #[test]
    fn error_display_utf8() {
        let err = Error::Utf8;
        assert!(format!("{}", err).contains("UTF-8"));
    }

    #[test]
    fn error_display_utf16() {
        let err = Error::Utf16;
        assert!(format!("{}", err).contains("UTF-16"));
    }

    #[test]
    fn error_display_tls() {
        let err = Error::Tls("tls error".into());
        assert!(format!("{}", err).contains("tls error"));
    }

    #[test]
    fn error_display_routing() {
        let err = Error::Routing {
            host: "h".into(),
            port: 123,
        };
        let s = format!("{}", err);
        assert!(s.contains("h") && s.contains("123"));
    }

    #[test]
    fn error_display_bulk_input() {
        let err = Error::BulkInput("bad input".into());
        assert!(format!("{}", err).contains("bad input"));
    }

    #[test]
    fn error_code_none_for_non_server() {
        assert_eq!(None, Error::Utf8.code());
        assert!(!Error::Utf8.is_deadlock());
    }

    #[test]
    fn error_from_io() {
        let e: Error = io::Error::new(io::ErrorKind::NotFound, "missing").into();
        assert!(matches!(e, Error::Io { .. }));
    }

    #[test]
    fn error_from_parse_int() {
        let e: Error = "abc".parse::<i32>().unwrap_err().into();
        assert!(matches!(e, Error::ParseInt(_)));
    }

    #[test]
    fn error_from_utf8_error() {
        let e: Error = std::str::from_utf8(b"\xff").unwrap_err().into();
        assert!(matches!(e, Error::Utf8));
    }

    #[test]
    fn error_from_string_utf8() {
        let e: Error = String::from_utf8(vec![0xff]).unwrap_err().into();
        assert!(matches!(e, Error::Utf8));
    }

    #[test]
    fn error_from_utf16_error() {
        let e: Error = String::from_utf16(&[0xD800]).unwrap_err().into();
        assert!(matches!(e, Error::Utf16));
    }

    #[test]
    fn error_from_uuid() {
        let e: Error = uuid::Uuid::parse_str("not-a-uuid").unwrap_err().into();
        assert!(matches!(e, Error::Conversion(_)));
    }

    #[test]
    fn error_clone_and_eq() {
        let e1 = Error::Utf8;
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }

    #[test]
    fn error_debug() {
        let err = Error::Protocol("test".into());
        let s = format!("{:?}", err);
        assert!(s.contains("Protocol"));
    }
}
