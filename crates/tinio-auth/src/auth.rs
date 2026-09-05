//! s3s authn provider over the configured identity map.

use std::sync::Arc;

use s3s::{
    S3Result,
    auth::{S3Auth, SecretKey},
    s3_error,
};

use crate::identity::Identity;

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
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.identity.users.get(access_key) {
            Some(user) => Ok(user.secret.clone()),
            None => Err(s3_error!(
                InvalidAccessKeyId,
                "The AWS Access Key Id you provided does not exist in our records."
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::identity::{Identity, User};

    #[tokio::test]
    async fn config_auth_answers_invalid_access_key_id_for_unknown() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let auth = ConfigAuth {
            identity: Arc::new(id),
        };
        let err = auth.get_secret_key("WHO").await.err().unwrap();
        assert_eq!(err.code().as_str(), "InvalidAccessKeyId");
    }

    #[tokio::test]
    async fn config_auth_returns_the_configured_secret() {
        let id = Identity::test(vec![User::test("AKID", "secret", "user1")]);
        let auth = ConfigAuth::new(Arc::new(id));
        let secret = auth.get_secret_key("AKID").await.unwrap();
        assert_eq!(secret.expose(), "secret");
    }
}
