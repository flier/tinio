use garde::Validate;
use secrecy::{CloneableSecret, ExposeSecret, SecretBox, SerializableSecret, zeroize::Zeroize};
use serde::{Deserialize, Serialize};

fn validate_auth_secret_key(value: &SecretBox<SecretKey>, _context: &()) -> garde::Result {
    super::reject_empty(
        "auth.secret_key must not be empty",
        value.expose_secret().is_empty(),
    )
}

/// Secret key material: zeroized on drop; [`SerializableSecret`] opts into serde
/// serialization for config round-trips.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretKey(String);

impl Zeroize for SecretKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

impl SerializableSecret for SecretKey {}

impl CloneableSecret for SecretKey {}

impl std::ops::Deref for SecretKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for SecretKey {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl From<String> for SecretKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<SecretKey> for SecretBox<SecretKey> {
    fn from(key: SecretKey) -> Self {
        SecretBox::new(Box::new(key))
    }
}

/// S3 credentials (`[auth]`; optional section — when present, both keys are
/// required; generated on first start with ≥ 16/32 bytes CSPRNG, per
/// data-model.md Credentials).
///
/// There is deliberately no `anonymous` key — anonymous mode is flag/env
/// only (the key is rejected as unknown).
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
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct Config {
    /// Access key (≥ 16 bytes when generated).
    #[garde(length(min = 1))]
    pub access_key: String,
    /// Secret key (≥ 32 bytes when generated).
    #[garde(custom(validate_auth_secret_key))]
    pub secret_key: SecretBox<SecretKey>,
}

impl PartialEq for Config {
    fn eq(&self, other: &Self) -> bool {
        self.access_key == other.access_key
            && self.secret_key.expose_secret() == other.secret_key.expose_secret()
    }
}
