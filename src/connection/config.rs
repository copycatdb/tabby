use std::path::PathBuf;

use super::AuthMethod;
use crate::EncryptionLevel;

#[derive(Clone, Debug)]
/// Configuration for connecting to a SQL Server instance.
///
/// Use the builder methods to construct a configuration, then pass it to
/// [`Client`] to establish a connection.
///
/// [`Client`]: struct.Client.html
pub struct Config {
    pub(crate) host: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) database: Option<String>,
    pub(crate) instance_name: Option<String>,
    pub(crate) application_name: Option<String>,
    pub(crate) encryption: EncryptionLevel,
    pub(crate) trust: TrustConfig,
    pub(crate) auth: AuthMethod,
    pub(crate) readonly: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum TrustConfig {
    #[allow(dead_code)]
    CaCertificateLocation(PathBuf),
    TrustAll,
    Default,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: None,
            port: None,
            database: None,
            instance_name: None,
            application_name: None,
            #[cfg(any(
                feature = "rustls",
                feature = "native-tls",
                feature = "vendored-openssl"
            ))]
            encryption: EncryptionLevel::Required,
            #[cfg(not(any(
                feature = "rustls",
                feature = "native-tls",
                feature = "vendored-openssl"
            )))]
            encryption: EncryptionLevel::NotSupported,
            trust: TrustConfig::Default,
            auth: AuthMethod::None,
            readonly: false,
        }
    }
}

impl Config {
    /// Create a new `Config` with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// A host or ip address to connect to.
    ///
    /// - Defaults to `localhost`.
    pub fn host(&mut self, host: impl ToString) {
        self.host = Some(host.to_string());
    }

    /// The server port.
    ///
    /// - Defaults to `1433`.
    pub fn port(&mut self, port: u16) {
        self.port = Some(port);
    }

    /// The database to connect to.
    ///
    /// - Defaults to `master`.
    pub fn database(&mut self, database: impl ToString) {
        self.database = Some(database.to_string())
    }

    /// The instance name as defined in the SQL Browser. Only available on
    /// Windows platforms.
    ///
    /// If specified, the port is replaced with the value returned from the
    /// browser.
    ///
    /// - Defaults to no name specified.
    pub fn instance_name(&mut self, name: impl ToString) {
        self.instance_name = Some(name.to_string());
    }

    /// Sets the application name to the connection, queryable with the
    /// `APP_NAME()` command.
    ///
    /// - Defaults to no name specified.
    pub fn application_name(&mut self, name: impl ToString) {
        self.application_name = Some(name.to_string());
    }

    /// Set the preferred encryption level.
    ///
    /// - With `tls` feature, defaults to `Required`.
    /// - Without `tls` feature, defaults to `NotSupported`.
    pub fn encryption(&mut self, encryption: EncryptionLevel) {
        self.encryption = encryption;
    }

    /// If set, the server certificate will not be validated and it is accepted
    /// as-is.
    ///
    /// On production setting, the certificate should be added to the local key
    /// storage (or use `trust_cert_ca` instead), using this setting is potentially dangerous.
    ///
    /// # Panics
    /// Will panic in case `trust_cert_ca` was called before.
    ///
    /// - Defaults to `default`, meaning server certificate is validated against system-truststore.
    pub fn trust_cert(&mut self) {
        if let TrustConfig::CaCertificateLocation(_) = &self.trust {
            panic!("'trust_cert' and 'trust_cert_ca' are mutual exclusive! Only use one.")
        }
        self.trust = TrustConfig::TrustAll;
    }

    /// If set, the server certificate will be validated against the given CA certificate in
    /// in addition to the system-truststore.
    /// Useful when using self-signed certificates on the server without having to disable the
    /// trust-chain.
    ///
    /// # Panics
    /// Will panic in case `trust_cert` was called before.
    ///
    /// - Defaults to validating the server certificate is validated against system's certificate storage.
    pub fn trust_cert_ca(&mut self, path: impl ToString) {
        if let TrustConfig::TrustAll = &self.trust {
            panic!("'trust_cert' and 'trust_cert_ca' are mutual exclusive! Only use one.")
        } else {
            self.trust = TrustConfig::CaCertificateLocation(PathBuf::from(path.to_string()))
        }
    }

    /// Sets the authentication method.
    ///
    /// - Defaults to `None`.
    pub fn authentication(&mut self, auth: AuthMethod) {
        self.auth = auth;
    }

    /// Sets ApplicationIntent readonly.
    ///
    /// - Defaults to `false`.
    pub fn readonly(&mut self, readnoly: bool) {
        self.readonly = readnoly;
    }

    pub(crate) fn get_host(&self) -> &str {
        self.host
            .as_deref()
            .filter(|v| v != &".")
            .unwrap_or("localhost")
    }

    pub(crate) fn get_port(&self) -> u16 {
        match (self.port, self.instance_name.as_ref()) {
            // A user-defined port, we must use that.
            (Some(port), _) => port,
            // If using a named instance, we'll give the default port of SQL
            // Browser.
            (None, Some(_)) => 1434,
            // Otherwise the defaulting to the default SQL Server port.
            (None, None) => 1433,
        }
    }

    /// Get the host address including port
    pub fn get_addr(&self) -> String {
        format!("{}:{}", self.get_host(), self.get_port())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::new();
        assert_eq!("localhost", config.get_host());
        assert_eq!(1433, config.get_port());
        assert_eq!("localhost:1433", config.get_addr());
    }

    #[test]
    fn custom_host_and_port() {
        let mut config = Config::new();
        config.host("myhost");
        config.port(5000);
        assert_eq!("myhost", config.get_host());
        assert_eq!(5000, config.get_port());
        assert_eq!("myhost:5000", config.get_addr());
    }

    #[test]
    fn dot_host_becomes_localhost() {
        let mut config = Config::new();
        config.host(".");
        assert_eq!("localhost", config.get_host());
    }

    #[test]
    fn instance_name_uses_browser_port() {
        let mut config = Config::new();
        config.instance_name("INST");
        assert_eq!(1434, config.get_port());
    }

    #[test]
    fn explicit_port_overrides_instance_name() {
        let mut config = Config::new();
        config.instance_name("INST");
        config.port(9999);
        assert_eq!(9999, config.get_port());
    }

    #[test]
    fn database_setting() {
        let mut config = Config::new();
        config.database("mydb");
        assert_eq!(Some("mydb".to_string()), config.database);
    }

    #[test]
    fn application_name_setting() {
        let mut config = Config::new();
        config.application_name("myapp");
        assert_eq!(Some("myapp".to_string()), config.application_name);
    }

    #[test]
    fn encryption_setting() {
        let mut config = Config::new();
        config.encryption(EncryptionLevel::NotSupported);
        assert_eq!(EncryptionLevel::NotSupported, config.encryption);
    }

    #[test]
    fn trust_cert_setting() {
        let mut config = Config::new();
        config.trust_cert();
        assert!(matches!(config.trust, TrustConfig::TrustAll));
    }

    #[test]
    fn trust_cert_ca_setting() {
        let mut config = Config::new();
        config.trust_cert_ca("/path/to/ca.pem");
        assert!(matches!(
            config.trust,
            TrustConfig::CaCertificateLocation(_)
        ));
    }

    #[test]
    #[should_panic(expected = "mutual exclusive")]
    fn trust_cert_then_ca_panics() {
        let mut config = Config::new();
        config.trust_cert();
        config.trust_cert_ca("/path/to/ca.pem");
    }

    #[test]
    #[should_panic(expected = "mutual exclusive")]
    fn trust_ca_then_cert_panics() {
        let mut config = Config::new();
        config.trust_cert_ca("/path/to/ca.pem");
        config.trust_cert();
    }

    #[test]
    fn readonly_setting() {
        let mut config = Config::new();
        config.readonly(true);
        assert!(config.readonly);
    }

    #[test]
    fn auth_sql_server() {
        let mut config = Config::new();
        config.authentication(AuthMethod::sql_server("user", "pass"));
        assert!(matches!(config.auth, AuthMethod::SqlServer(_)));
    }

    #[test]
    fn auth_aad_token() {
        let mut config = Config::new();
        config.authentication(AuthMethod::aad_token("my-token"));
        assert!(matches!(config.auth, AuthMethod::AADToken(_)));
    }

    #[test]
    fn config_clone() {
        let mut config = Config::new();
        config.host("test");
        config.port(5555);
        let cloned = config.clone();
        assert_eq!("test:5555", cloned.get_addr());
    }
}
