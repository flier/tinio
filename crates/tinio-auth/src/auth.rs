//! s3s authn provider over the configured identity map.

use std::sync::Arc;

use s3s::{
    S3Result,
    auth::{S3Auth, SecretKey},
};
use tracing::{Span, field, instrument};

use crate::{
    identity::{Identity, invalid_access_key_id},
    metrics,
};

/// The s3s `S3Auth` provider: resolves the secret per configured user;
/// an unknown access key answers `InvalidAccessKeyId` (the root-only
/// `StaticAuth` phrasing, kept for the FR-008 interop expectation).
pub struct ConfigAuth {
    identity: Arc<Identity>,
}

impl ConfigAuth {
    /// An authn provider over the identity map.
    pub fn new(identity: Arc<Identity>) -> Self {
        Self { identity }
    }
}

#[async_trait::async_trait]
impl S3Auth for ConfigAuth {
    /// The access key itself is never a field.
    #[instrument(
        level = "info",
        target = "tinio::auth",
        skip(self, access_key),
        fields(op = "get_secret_key", result = field::Empty)
    )]
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.identity.users.get(access_key) {
            Some(user) => {
                Span::current().record("result", "found");
                metrics::record_credential_lookup(true);
                Ok(user.secret.clone())
            }
            None => {
                Span::current().record("result", "unknown");
                metrics::record_credential_lookup(false);
                Err(invalid_access_key_id())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tinio_util::testing::metrics::counter;

    use super::*;
    use crate::identity::{Identity, User};

    fn auth() -> ConfigAuth {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        ConfigAuth {
            identity: Arc::new(id),
        }
    }

    #[tokio::test]
    async fn config_auth_answers_invalid_access_key_id_for_unknown() {
        let err = auth().get_secret_key("WHO").await.err().unwrap();
        assert_eq!(err.code().as_str(), "InvalidAccessKeyId");
    }

    #[tokio::test]
    async fn config_auth_returns_the_configured_secret() {
        let secret = auth().get_secret_key("AKID").await.unwrap();
        assert_eq!(secret.expose(), "secret");
    }

    /// Both lookup arms book their own `result` label: a configured key is
    /// `found`, an unknown key `unknown`. The registry is process-global, so
    /// the assertion is a delta (`>`), never an absolute value.
    #[tokio::test]
    async fn credential_lookups_book_found_and_unknown() {
        let found = || {
            counter(
                "tinio_auth_credential_lookups_total",
                &[("result", "found")],
            )
        };
        let unknown = || {
            counter(
                "tinio_auth_credential_lookups_total",
                &[("result", "unknown")],
            )
        };
        let (before_found, before_unknown) = (found(), unknown());

        let auth = auth();
        auth.get_secret_key("AKID").await.unwrap();
        assert!(auth.get_secret_key("WHO").await.is_err());

        assert!(found() > before_found, "the found arm books a sample");
        assert!(unknown() > before_unknown, "the unknown arm books a sample");
    }
}
