//! S3 credentials (`[auth]`; optional section — when present, both keys are
//! required; generated on first start with ≥ 16/32 bytes CSPRNG, per
//! data-model.md Credentials).

#[cfg(feature = "config")]
use garde::Validate;
#[cfg(feature = "config")]
use secrecy::ExposeSecret;
#[cfg(feature = "config")]
use serde::{Deserialize, Serialize};
pub use tinio_util::secret::{Secret, SecretKey};

#[cfg(feature = "config")]
fn validate_auth_secret_key(value: &Secret, _context: &()) -> garde::Result {
    super::reject_empty(
        "auth.secret_key must not be empty",
        value.expose_secret().is_empty(),
    )
}

/// S3 credentials (`[auth]`; optional section — when present, both keys are
/// required; generated on first start with ≥ 16/32 bytes CSPRNG, per
/// data-model.md Credentials).
///
/// There is deliberately no `anonymous` key — anonymous mode is flag/env
/// only (the key is rejected as unknown).
///
/// **An absent section is the loopback-only development pair.** With no
/// `[auth]`, the served credentials are the built-in `minioadmin`/
/// `minioadmin` pair — not a credential at all, so the CLI refuses to start
/// on a non-loopback bind without this section (or `TINIO_ACCESS_KEY`/
/// `TINIO_SECRET_KEY`), and warns on the loopback one it does serve.
///
/// # Examples
///
/// ```rust
/// use secrecy::ExposeSecret;
/// use tinio_server::identity::auth::{Config, SecretKey};
///
/// let auth = Config {
///     access_key: "minioadmin".into(),
///     secret_key: SecretKey::from("minioadmin-secret").into(),
/// };
/// assert!(!auth.access_key.is_empty());
/// assert!(!auth.secret_key.expose_secret().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize, Validate))]
pub struct Config {
    /// Access key (≥ 16 bytes when generated).
    #[cfg_attr(feature = "config", garde(length(min = 1)))]
    pub access_key: String,
    /// Secret key (≥ 32 bytes when generated).
    #[cfg_attr(feature = "config", garde(custom(validate_auth_secret_key)))]
    pub secret_key: Secret,
}

#[cfg(all(test, feature = "config"))]
mod schema_tests {
    use garde::Validate;

    use super::*;

    #[test]
    fn empty_access_key_rejected() {
        let auth: Config = toml::from_str("access_key = \"\"\nsecret_key = \"secret\"\n").unwrap();
        let err = auth.validate().unwrap_err();
        assert!(err.to_string().contains("access_key"), "{err}");
    }

    #[test]
    fn empty_secret_key_rejected() {
        let auth: Config = toml::from_str("access_key = \"AKID\"\nsecret_key = \"\"\n").unwrap();
        let err = auth.validate().unwrap_err();
        assert!(err.to_string().contains("secret_key"), "{err}");
    }
}
