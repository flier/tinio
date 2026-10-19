//! The root owner element (`[owner]`; optional section — absent = the
//! core defaults: canonical `hex(SHA-256("tinio"))`, display name
//! `"tinio"`, no local mapping). The owner element is the root user's
//! canonical-account identity (spec §6) and the lazy owner for rows with
//! none — the single `[owner]`/`[[users]]` assembly resolves it
//! (tinio-server's `identity::Identity::from_config`).

#[cfg(feature = "config")]
use garde::Validate;
#[cfg(feature = "config")]
use serde::{Deserialize, Serialize};
use smart_default::SmartDefault;

#[cfg(feature = "config")]
use super::validate_canonical_id;
#[cfg(all(windows, feature = "config"))]
use super::validate_local_uid;
use crate::_core::acl::{DEFAULT_OWNER_DISPLAY_NAME, OwnerId, default_owner_id};

/// The root owner element. Defaults are the core constants (the single
/// source); `local_uid` is unix-only — Windows configs fail validation at
/// parse (spec §6).
///
/// # Examples
///
/// ```rust
/// use tinio_server::identity::owner::Config;
///
/// let config = Config::default();
/// assert_eq!(config.display_name, "tinio");
/// assert_eq!(config.canonical_id.len(), 64);
/// ```
#[derive(Debug, Clone, PartialEq, SmartDefault)]
#[cfg_attr(feature = "config", derive(Serialize, Deserialize, Validate))]
#[cfg_attr(feature = "config", garde(allow_unvalidated))]
#[cfg_attr(feature = "config", serde(default))]
pub struct Config {
    /// The root user's canonical account ID (default `hex(SHA-256("tinio"))`).
    #[default(_code = "default_owner_id().as_str().to_string()")]
    #[cfg_attr(feature = "config", garde(custom(validate_canonical_id)))]
    pub canonical_id: String,
    /// The root user's presentation display name (default `"tinio"`).
    #[default(_code = "DEFAULT_OWNER_DISPLAY_NAME.to_string()")]
    pub display_name: String,
    /// The root user's files chown to this uid on unix (unix-only; a
    /// Windows config with the key set fails validation at parse). The
    /// field stays on every platform (the portable type); Windows arms
    /// the rule.
    #[cfg_attr(all(windows, feature = "config"), garde(custom(validate_local_uid)))]
    pub local_uid: Option<u32>,
}

impl Config {
    /// The canonical ID as the [`OwnerId`] type (validated at config
    /// parse; valid by construction — the `#[expect]` below carries the
    /// written argument).
    #[expect(
        clippy::expect_used,
        reason = "argued: `canonical_id` is validated at config parse — `validate_canonical_id` is this field's garde rule (`identity.rs`) and runs when the CLI's root document validates (`tinio_cli::config::Config::parse`/`load`), and the field's default route codes `default_owner_id()`; the `pub` field's remaining hole is a hand-built struct, and no in-repo production caller builds one (checked 2026-10-09)"
    )]
    pub fn resolved_canonical_id(&self) -> OwnerId {
        OwnerId::new(self.canonical_id.clone()).expect("canonical_id is validated at config parse")
    }
}

#[cfg(all(test, feature = "config"))]
mod schema_tests {
    use garde::Validate;

    use super::*;

    #[test]
    fn owner_defaults_are_stable() {
        // Spec §6: canonical_id defaults to hex(SHA-256("tinio")), display
        // name to "tinio" — the core constants, the single source.
        let owner = Config::default();
        assert_eq!(owner.canonical_id, default_owner_id().as_str());
        assert_eq!(owner.display_name, DEFAULT_OWNER_DISPLAY_NAME);
        assert_eq!(
            owner.canonical_id,
            "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"
        );
        assert_eq!(owner.resolved_canonical_id(), default_owner_id());
        assert_eq!(owner.local_uid, None);
    }

    #[test]
    fn owner_settings_apply_and_resolve() {
        let owner: Config = toml::from_str(
            "canonical_id = \"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100\"\ndisplay_name = \"ops\"\n",
        )
        .unwrap();
        owner.validate().unwrap();
        assert_eq!(owner.display_name, "ops");
        assert_eq!(
            owner.resolved_canonical_id().as_str(),
            "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
        );
    }

    #[test]
    fn owner_never_accepts_the_anonymous_canonical_id() {
        // Task 7 ruling: an owner configured with the anonymous canonical
        // ID would BE the anonymous principal (every unsigned request
        // would be its owner).
        let owner: Config =
            toml::from_str("canonical_id = \"65a011a29cdf8ec533ec3d1ccaae921c\"").unwrap();
        let err = owner.validate().unwrap_err();
        assert!(err.to_string().contains("anonymous"), "{err}");
    }

    #[test]
    fn owner_canonical_id_must_be_64_hex() {
        let owner: Config = toml::from_str("canonical_id = \"not-hex\"").unwrap();
        assert!(owner.validate().is_err());
    }

    #[cfg(windows)]
    #[test]
    fn owner_local_uid_is_rejected_on_windows() {
        let owner: Config = toml::from_str("local_uid = 1000").unwrap();
        let err = owner.validate().unwrap_err();
        assert!(err.to_string().contains("local_uid"), "{err}");
    }
}
