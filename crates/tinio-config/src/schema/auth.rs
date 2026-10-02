use garde::Validate;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};

pub use crate::_util::secret::{Secret, SecretKey};

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
/// use tinio_config::Config;
///
/// let config = Config::parse(
///     r#"
///     version = 1
///     [auth]
///     access_key = "minioadmin"
///     secret_key = "minioadmin-secret"
///     "#,
/// )
/// .unwrap();
/// let auth = config.auth.as_ref().unwrap();
/// assert!(!auth.access_key.is_empty());
/// assert!(!auth.secret_key.expose_secret().is_empty());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Validate, PartialEq)]
pub struct Config {
    /// Access key (≥ 16 bytes when generated).
    #[garde(length(min = 1))]
    pub access_key: String,
    /// Secret key (≥ 32 bytes when generated).
    #[garde(custom(validate_auth_secret_key))]
    pub secret_key: Secret,
}
