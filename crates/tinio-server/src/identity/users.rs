//! One configured S3 user (`[[users]]`; optional array — absent = root
//! only, spec §6). Access key ≥ 1 char, secret key non-empty (the `[auth]`
//! rule body), canonical ID defaults to `hex(SHA-256(access_key))`,
//! display name to the access key; `local_uid` is unix-only (Windows fails
//! validation at parse).

use derive_more::PartialEq;
#[cfg(feature = "config")]
use garde::Validate;
#[cfg(feature = "config")]
use secrecy::ExposeSecret;
#[cfg(feature = "config")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "config")]
use super::auth::Secret;
#[cfg(all(windows, feature = "config"))]
use super::validate_local_uid;
#[cfg(feature = "config")]
use super::{reject_empty, validate_canonical_id_opt};
use crate::_core::acl::{OwnerId, derive_canonical_id};

/// One configured S3 user (`[[users]]`).
///
/// # Examples
///
/// ```rust
/// use secrecy::ExposeSecret;
/// use tinio_server::identity::{auth::SecretKey, users::Config};
///
/// let user = Config {
///     access_key: "AKID".into(),
///     secret_key: SecretKey::from("secret").into(),
///     canonical_id: None,
///     display_name: None,
///     local_uid: None,
/// };
/// assert_eq!(user.effective_display_name(), "AKID");
/// assert_eq!(user.resolved_canonical_id().as_str().len(), 64);
/// assert_eq!(&**user.secret_key.expose_secret(), "secret");
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize, Validate))]
#[cfg_attr(feature = "config", garde(allow_unvalidated))]
pub struct Config {
    /// The SigV4 access key (≥ 1 char).
    #[cfg_attr(feature = "config", garde(length(min = 1)))]
    pub access_key: String,
    /// The SigV4 secret key (non-empty — the `[auth]` rule body).
    #[cfg_attr(feature = "config", garde(custom(validate_secret_key)))]
    pub secret_key: super::auth::Secret,
    /// The canonical account ID (default `hex(SHA-256(access_key))`).
    #[cfg_attr(feature = "config", serde(default))]
    #[cfg_attr(feature = "config", garde(custom(validate_canonical_id_opt)))]
    pub canonical_id: Option<String>,
    /// The presentation display name (default the access key).
    #[cfg_attr(feature = "config", serde(default))]
    pub display_name: Option<String>,
    /// This user's files chown to this uid on unix (unix-only; a Windows
    /// config with the key set fails validation at parse). The field
    /// stays on every platform (the portable type); Windows arms the
    /// rule.
    #[cfg_attr(all(windows, feature = "config"), garde(custom(validate_local_uid)))]
    pub local_uid: Option<u32>,
}

impl Config {
    /// The effective canonical ID: the configured one, or the spec §6
    /// default `hex(SHA-256(access_key))` (validated at config parse;
    /// valid by construction — the `#[expect]` below carries the written
    /// argument).
    #[expect(
        clippy::expect_used,
        reason = "argued: a configured `canonical_id` is validated at config parse — `validate_canonical_id_opt` is this field's garde rule (`identity.rs`) and runs when the CLI's root document validates (`tinio_cli::config::Config::parse`/`load`), and the `None` arm derives instead; the `pub` field's remaining hole is a hand-built struct, and no in-repo production caller builds one (checked 2026-10-09)"
    )]
    pub fn resolved_canonical_id(&self) -> OwnerId {
        self.effective_canonical_id()
            .expect("canonical_id is validated at config parse")
    }

    /// The effective canonical ID, or `None` when the configured one is invalid (the per-field
    /// rule reports that at parse). The one definition of the `None` default
    /// `hex(SHA-256(access_key))`, shared by the assembly and the uniqueness validation.
    pub fn effective_canonical_id(&self) -> Option<OwnerId> {
        match &self.canonical_id {
            Some(id) => OwnerId::new(id.clone()).ok(),
            None => Some(derive_canonical_id(&self.access_key)),
        }
    }

    /// The effective display name: the configured one, or the access key.
    pub fn effective_display_name(&self) -> String {
        self.display_name
            .clone()
            .unwrap_or_else(|| self.access_key.clone())
    }
}

/// The secret-key rule (spec §6): the `[auth]` non-empty check.
#[cfg(feature = "config")]
fn validate_secret_key(value: &Secret, _context: &()) -> garde::Result {
    reject_empty(
        "users.secret_key must not be empty",
        value.expose_secret().is_empty(),
    )
}

#[cfg(all(test, feature = "config"))]
mod schema_tests {
    use garde::Validate;
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn user_defaults_derive_canonical_id_and_display_name() {
        // Spec §6: canonical_id defaults to hex(SHA-256(access_key)),
        // display_name to the access key.
        let user: Config =
            toml::from_str("access_key = \"AKID\"\nsecret_key = \"secret\"\n").unwrap();
        user.validate().unwrap();
        assert_eq!(
            user.resolved_canonical_id().as_str(),
            "2c8a2a08ad81dddf7e7830cbc75310f731a1381431bb29afb55a76ee07e81721"
        );
        assert_eq!(user.effective_display_name(), "AKID");
        assert_eq!(&**user.secret_key.expose_secret(), "secret");
    }

    #[test]
    fn explicit_canonical_id_and_display_name_override_the_defaults() {
        let user: Config = toml::from_str(
            "access_key = \"AKID\"\nsecret_key = \"secret\"\ncanonical_id = \"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100\"\ndisplay_name = \"alice\"\n",
        )
        .unwrap();
        user.validate().unwrap();
        assert_eq!(
            user.resolved_canonical_id().as_str(),
            "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
        );
        assert_eq!(user.effective_display_name(), "alice");
    }

    #[test]
    fn empty_access_key_rejected() {
        let user: Config = toml::from_str("access_key = \"\"\nsecret_key = \"secret\"\n").unwrap();
        assert!(user.validate().is_err());
    }

    #[test]
    fn empty_secret_key_rejected() {
        // The `[auth]` secret-key rule body (non-empty only).
        let user: Config = toml::from_str("access_key = \"AKID\"\nsecret_key = \"\"\n").unwrap();
        let err = user.validate().unwrap_err();
        assert!(err.to_string().contains("secret_key"), "{err}");
    }

    #[test]
    fn user_never_accepts_the_anonymous_canonical_id() {
        // Task 7 ruling: a configured user with the anonymous ID would
        // BECOME the anonymous principal.
        let user: Config = toml::from_str(
            "access_key = \"AKID\"\nsecret_key = \"secret\"\ncanonical_id = \"65a011a29cdf8ec533ec3d1ccaae921c\"\n",
        )
        .unwrap();
        let err = user.validate().unwrap_err();
        assert!(err.to_string().contains("anonymous"), "{err}");
    }

    #[cfg(windows)]
    #[test]
    fn user_local_uid_is_rejected_on_windows() {
        let user: Config =
            toml::from_str("access_key = \"AKID\"\nsecret_key = \"secret\"\nlocal_uid = 1000\n")
                .unwrap();
        let err = user.validate().unwrap_err();
        assert!(err.to_string().contains("local_uid"), "{err}");
    }
}
