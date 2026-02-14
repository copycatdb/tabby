use std::fmt::Debug;

#[derive(Clone, PartialEq, Eq)]
pub struct SqlServerAuth {
    user: String,
    password: String,
}

impl SqlServerAuth {
    pub(crate) fn user(&self) -> &str {
        &self.user
    }

    pub(crate) fn password(&self) -> &str {
        &self.password
    }
}

impl Debug for SqlServerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlServerAuth")
            .field("user", &self.user)
            .field("password", &"<HIDDEN>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
#[cfg(any(all(windows, feature = "winauth"), doc))]
#[cfg_attr(feature = "docs", doc(all(windows, feature = "winauth")))]
pub struct WindowsAuth {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) domain: Option<String>,
}

#[cfg(any(all(windows, feature = "winauth"), doc))]
#[cfg_attr(feature = "docs", doc(all(windows, feature = "winauth")))]
impl Debug for WindowsAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WindowsAuth")
            .field("user", &self.user)
            .field("password", &"<HIDDEN>")
            .field("domain", &self.domain)
            .finish()
    }
}

/// Defines the method of authentication to the server.
///
/// Use the constructor methods ([`sql_server`](Self::sql_server),
/// [`windows`](Self::windows), [`aad_token`](Self::aad_token)) to create
/// instances, then pass to [`Config::authentication`].
///
/// # Example
///
/// ```
/// use tabby::AuthMethod;
///
/// // SQL Server authentication
/// let auth = AuthMethod::sql_server("sa", "my_password");
///
/// // Azure AD token authentication
/// let auth = AuthMethod::aad_token("eyJ0eXAi...");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthMethod {
    /// Authenticate with SQL Server credentials (username + password).
    SqlServer(SqlServerAuth),
    /// Authenticate with Windows/NTLM credentials (domain + username + password).
    #[cfg(any(all(windows, feature = "winauth"), doc))]
    #[cfg_attr(feature = "docs", doc(cfg(all(windows, feature = "winauth"))))]
    Windows(WindowsAuth),
    /// Authenticate as the currently logged-in user via SSPI (Windows) or
    /// Kerberos/GSSAPI (Unix). Requires the `winauth` or
    /// `integrated-auth-gssapi` feature.
    #[cfg(any(
        all(windows, feature = "winauth"),
        all(unix, feature = "integrated-auth-gssapi"),
        doc
    ))]
    #[cfg_attr(
        feature = "docs",
        doc(cfg(any(windows, all(unix, feature = "integrated-auth-gssapi"))))
    )]
    Integrated,
    /// Authenticate with an Azure Active Directory (AAD) token. The token
    /// should encode an AAD user or service principal with SQL Server access.
    AADToken(String),
    #[doc(hidden)]
    None,
}

impl AuthMethod {
    /// Creates a SQL Server authentication method with the given username and
    /// password.
    pub fn sql_server(user: impl ToString, password: impl ToString) -> Self {
        Self::SqlServer(SqlServerAuth {
            user: user.to_string(),
            password: password.to_string(),
        })
    }

    /// Creates a Windows/NTLM authentication method.
    ///
    /// The `user` can be in `DOMAIN\username` format; the domain is extracted
    /// automatically.
    #[cfg(any(all(windows, feature = "winauth"), doc))]
    #[cfg_attr(feature = "docs", doc(cfg(all(windows, feature = "winauth"))))]
    pub fn windows(user: impl AsRef<str>, password: impl ToString) -> Self {
        let (domain, user) = match user.as_ref().find('\\') {
            Some(idx) => (Some(&user.as_ref()[..idx]), &user.as_ref()[idx + 1..]),
            _ => (None, user.as_ref()),
        };

        Self::Windows(WindowsAuth {
            user: user.to_string(),
            password: password.to_string(),
            domain: domain.map(|s| s.to_string()),
        })
    }

    /// Creates an AAD token authentication method with the given bearer token.
    pub fn aad_token(token: impl ToString) -> Self {
        Self::AADToken(token.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_server_auth() {
        let auth = AuthMethod::sql_server("user", "pass");
        match &auth {
            AuthMethod::SqlServer(sa) => {
                assert_eq!("user", sa.user());
                assert_eq!("pass", sa.password());
            }
            _ => panic!("Expected SqlServer"),
        }
    }

    #[test]
    fn sql_server_auth_debug_hides_password() {
        let auth = SqlServerAuth {
            user: "sa".into(),
            password: "secret".into(),
        };
        let dbg = format!("{:?}", auth);
        assert!(dbg.contains("sa"));
        assert!(dbg.contains("<HIDDEN>"));
        assert!(!dbg.contains("secret"));
    }

    #[test]
    fn sql_server_auth_clone_eq() {
        let a1 = AuthMethod::sql_server("u", "p");
        let a2 = a1.clone();
        assert_eq!(a1, a2);
    }

    #[test]
    fn aad_token_auth() {
        let auth = AuthMethod::aad_token("my-token");
        match auth {
            AuthMethod::AADToken(t) => assert_eq!("my-token", t),
            _ => panic!("Expected AADToken"),
        }
    }

    #[test]
    fn auth_none() {
        let auth = AuthMethod::None;
        assert_eq!(AuthMethod::None, auth);
    }

    #[test]
    fn auth_method_debug() {
        let auth = AuthMethod::sql_server("user", "pass");
        let dbg = format!("{:?}", auth);
        assert!(dbg.contains("SqlServer"));
    }
}
