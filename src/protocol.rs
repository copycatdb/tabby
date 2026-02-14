mod collation;
pub mod numeric;
pub mod pipeline;
pub mod reader;
mod session;
pub mod temporal;
pub mod wire;
pub mod xml;

pub(crate) use collation::*;
pub(crate) use numeric::*;
pub use session::*;

/// The amount of bytes a packet header consists of
pub const HEADER_BYTES: usize = 8;

uint_enum! {
    /// The configured encryption level specifying if encryption is required.
    ///
    /// With a TLS feature enabled (`rustls`, `native-tls`, or `vendored-openssl`),
    /// the default is [`Required`](Self::Required). Without TLS, the default is
    /// [`NotSupported`](Self::NotSupported).
    #[repr(u8)]
    pub enum EncryptionLevel {
        /// Only use encryption for the login procedure
        Off = 0,
        /// Encrypt everything if possible
        On = 1,
        /// Do not encrypt anything
        NotSupported = 2,
        /// Encrypt everything and fail if not possible
        Required = 3,
    }

}
