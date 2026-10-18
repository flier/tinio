//! Config-driven identity assembly (spec §5a/§6): the `[auth]` +
//! `[owner]` + `[[users]]` sections become the acl identity map
//! ([`IdentityFromConfig::from_config`]) and the chown `OwnerId → uid`
//! map ([`owner_uids_for`]). Plain data in, plain data out — the only
//! config consumers of the identity types (feature `acl`).
//!
//! The section types live here with their per-field garde rules; the
//! module's [`Config`] is assembled after parsing by the caller (the CLI
//! composes it from the root document), so it derives no serde.

use std::collections::HashMap;
#[cfg(feature = "acl")]
use std::sync::Arc;

#[cfg(feature = "config")]
use garde::Error as GardeError;
#[cfg(feature = "acl")]
use s3s::auth::SecretKey;
use secrecy::ExposeSecret;

#[cfg(feature = "acl")]
use crate::_auth::{Identity, User};
#[cfg(feature = "config")]
use crate::_core::acl::ANONYMOUS_CANONICAL_ID;
/// The root access-key fallback (`"minioadmin"`), **published here because the policy that names
/// it is this module's**: [`uses_builtin_fallback`] is the predicate a caller asks, and a caller
/// rendering a sentence about the fallback — `tinio-cli`'s start refusal and its loopback warning
/// — names the key through this path rather than spelling the value a second time.
///
/// The constant's one definer stays `tinio_core::acl::DEFAULT_ROOT_ACCESS_KEY` (the value the
/// config uniqueness validation reserves); this is a re-export of it, not a second home.
///
/// **The change it carries is audit run-2 P2's loopback-only development credential**: the CLI's
/// policy refuses a non-loopback bind that would serve this pair, and it reads the key from here
/// because the predicate beside it is what decides whether the pair is the fallback at all.
///
/// # Examples
///
/// ```
/// use tinio_server::identity::DEFAULT_ROOT_ACCESS_KEY;
///
/// assert_eq!(DEFAULT_ROOT_ACCESS_KEY, "minioadmin");
/// ```
pub use crate::_core::acl::DEFAULT_ROOT_ACCESS_KEY;
use crate::_core::acl::OwnerId;

pub mod auth;
pub mod owner;
pub mod users;

/// The identity module's configuration: the three values `Identity::from_config`,
/// [`root_auth_pair`], [`uses_builtin_fallback`] and [`owner_uids_for`] read.
///
/// Assembled after parsing — the CLI composes it from the root document and
/// applies the `[owner]` default on the way in — so it derives no serde.
///
/// # Examples
///
/// ```
/// use tinio_server::identity::Config;
///
/// let config = Config::default();
/// assert!(config.auth.is_none());
/// assert_eq!(config.owner.display_name, "tinio");
/// assert!(config.users.is_empty());
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Config {
    /// The root `[auth]` credential pair (`None` = the built-in fallback pair).
    pub auth: Option<auth::Config>,
    /// The root owner element (the core defaults when `[owner]` is absent).
    pub owner: owner::Config,
    /// The configured `[[users]]` entries (empty when absent).
    pub users: Vec<users::Config>,
}

/// The shared "must not be empty" garde rule body: the auth secret key and
/// the users secret key reject empty values with their own messages (one
/// home for the boilerplate, so the rule cannot drift).
#[cfg(feature = "config")]
fn reject_empty(message: &str, is_empty: bool) -> garde::Result {
    if is_empty {
        Err(GardeError::new(message))
    } else {
        Ok(())
    }
}

/// The canonical-ID garde rule (spec §6, shared by `[owner]` and
/// `[[users]]`): 64 lowercase hex digits per [`OwnerId::new`] — and never
/// the anonymous special-grantee ID, whose holder would BE the anonymous
/// uploader (Task 7 ruling).
#[cfg(feature = "config")]
fn validate_canonical_id(value: &str, _context: &()) -> garde::Result {
    if value == ANONYMOUS_CANONICAL_ID {
        return Err(GardeError::new(
            "must not be the anonymous canonical ID (it would make the principal the anonymous uploader)",
        ));
    }
    match OwnerId::new(value) {
        Ok(_) => Ok(()),
        Err(_) => Err(GardeError::new(
            "must be 64 lowercase hex digits (the canonical-account ID shape)",
        )),
    }
}

/// The optional-canonical-ID variant of [`validate_canonical_id`] (the
/// `[[users]]` field; `None` = the derived default).
#[cfg(feature = "config")]
fn validate_canonical_id_opt(value: &Option<String>, context: &()) -> garde::Result {
    match value {
        Some(id) => validate_canonical_id(id, context),
        None => Ok(()),
    }
}

/// The Windows `local_uid` rejection (spec §6, fail fast): the type stays
/// portable, the key parses, and validation errors instead — a unix-only
/// key in a Windows config is an invalid value, not a silent ignore.
#[cfg(all(windows, feature = "config"))]
fn validate_local_uid(value: &Option<u32>, _context: &()) -> garde::Result {
    match value {
        Some(uid) => Err(GardeError::new(format!(
            "local_uid ({uid}) is unsupported on Windows — remove the key or run on a unix system"
        ))),
        None => Ok(()),
    }
}

/// The root credential pair: the configured `[auth]` keys, or the US1
/// interop convention pair (`minioadmin`/`minioadmin`) when no `[auth]`
/// section is present — the harness's default, kept verbatim (spec §6;
/// the fallback access key is [`DEFAULT_ROOT_ACCESS_KEY`], the same
/// constant the config uniqueness validation reserves).
pub fn root_auth_pair(config: &Config) -> (String, String) {
    match &config.auth {
        Some(auth) => (
            auth.access_key.clone(),
            auth.secret_key.expose_secret().to_string(),
        ),
        None => (DEFAULT_ROOT_ACCESS_KEY.into(), "minioadmin".into()),
    }
}

/// True when the resolved pair is the built-in fallback — no `[auth]` section is configured.
///
/// **The fallback is a development credential, and where that becomes a policy is the CLI**: a
/// start that would serve it to another host refuses to start, while one on loopback serves it
/// with a warning (`tinio-cli`'s `commands::start::serve`). The server stays agnostic —
/// [`root_auth_pair`] answers the pair either way, and the data plane has no notion of which host
/// can reach it.
///
/// # Examples
///
/// ```
/// use tinio_server::identity::{Config, auth, uses_builtin_fallback};
///
/// assert!(uses_builtin_fallback(&Config::default()));
/// let configured = Config {
///     auth: Some(auth::Config {
///         access_key: "ak".into(),
///         secret_key: auth::SecretKey::from("sk").into(),
///     }),
///     ..Config::default()
/// };
/// assert!(!uses_builtin_fallback(&configured));
/// ```
pub fn uses_builtin_fallback(config: &Config) -> bool {
    config.auth.is_none()
}

/// The fs store's owner→uid map (unix chown on write): config-supplied
/// only, never request-derived (no injection vector). Windows configs
/// cannot carry a mapping (parse rejects any `local_uid`), so the map is
/// empty there by construction; the empty map = no chown, objects stay
/// server-user-owned (the hardened default).
pub fn owner_uids_for(config: &Config) -> HashMap<OwnerId, u32> {
    let mut map = HashMap::new();
    if let Some(uid) = config.owner.local_uid {
        map.insert(config.owner.resolved_canonical_id(), uid);
    }
    for user in &config.users {
        if let Some(uid) = user.local_uid {
            map.insert(user.resolved_canonical_id(), uid);
        }
    }
    map
}

/// The configured-identity assembly (spec §6): the root user is the
/// `[auth]` credential pair presenting the `[owner]` element, plus one
/// [`User`] per `[[users]]` entry. The ONLY assembly serving the acl
/// plane.
#[cfg(feature = "acl")]
pub trait IdentityFromConfig {
    /// Build the identity map for the acl plane from the composed config.
    fn from_config(config: &Config) -> Arc<Identity>;
}

#[cfg(feature = "acl")]
impl IdentityFromConfig for Identity {
    fn from_config(config: &Config) -> Arc<Identity> {
        let owner = &config.owner;
        let owner_id = owner.resolved_canonical_id();
        let (access_key, secret) = root_auth_pair(config);
        let root = User {
            access_key: access_key.clone(),
            canonical_id: owner_id.clone(),
            display_name: owner.display_name.clone(),
            secret: SecretKey::from(secret),
        };
        let mut users: HashMap<String, User> = HashMap::with_capacity(1 + config.users.len());
        users.insert(root.access_key.clone(), root);
        for user in &config.users {
            users.insert(
                user.access_key.clone(),
                User {
                    access_key: user.access_key.clone(),
                    canonical_id: user.resolved_canonical_id(),
                    display_name: user.effective_display_name(),
                    secret: SecretKey::from(user.secret_key.expose_secret().as_str()),
                },
            );
        }
        Arc::new(Identity::new(users, owner_id, owner.display_name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configured() -> Config {
        Config {
            auth: Some(auth::Config {
                access_key: "root-ak".into(),
                secret_key: auth::SecretKey::from("root-sk").into(),
            }),
            owner: owner::Config {
                canonical_id: "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100"
                    .into(),
                display_name: "ops".into(),
                local_uid: None,
            },
            users: vec![
                users::Config {
                    access_key: "AKID".into(),
                    secret_key: auth::SecretKey::from("user-sk-1").into(),
                    canonical_id: None,
                    display_name: Some("alice".into()),
                    local_uid: None,
                },
                users::Config {
                    access_key: "BKID".into(),
                    secret_key: auth::SecretKey::from("user-sk-2").into(),
                    canonical_id: None,
                    display_name: None,
                    local_uid: None,
                },
            ],
        }
    }

    #[cfg(feature = "acl")]
    fn uid() -> OwnerId {
        OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
    }

    #[cfg(feature = "acl")]
    #[test]
    fn from_config_assembles_root_and_users() {
        let config = configured();
        let identity = Identity::from_config(&config);
        assert_eq!(identity.users.len(), 3);
        let root = identity.users.get("root-ak").unwrap();
        assert_eq!(root.canonical_id, uid());
        assert_eq!(root.display_name, "ops");
        assert_eq!(root.secret.expose(), "root-sk");
        let alice = identity.users.get("AKID").unwrap();
        assert_eq!(
            alice.canonical_id.as_str(),
            "2c8a2a08ad81dddf7e7830cbc75310f731a1381431bb29afb55a76ee07e81721"
        );
        assert_eq!(alice.display_name, "alice");
        assert_eq!(alice.secret.expose(), "user-sk-1");
        let bob = identity.users.get("BKID").unwrap();
        assert_eq!(bob.display_name, "BKID");
        assert_eq!(identity.default_owner, uid());
        assert_eq!(identity.default_display_name, "ops");
    }

    #[cfg(feature = "acl")]
    #[test]
    fn from_config_without_sections_falls_back_to_the_minioadmin_pair() {
        // The US1 interop convention pair stays the root when no [auth]
        // section is configured; the owner element is the core default.
        let identity = Identity::from_config(&Config::default());
        let root = identity.users.get("minioadmin").unwrap();
        assert_eq!(
            root.canonical_id.as_str(),
            "d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306"
        );
        assert_eq!(root.display_name, "tinio");
        assert_eq!(root.secret.expose(), "minioadmin");
        assert_eq!(identity.default_owner.as_str(), root.canonical_id.as_str());
    }

    #[test]
    fn root_auth_pair_falls_back_to_the_minioadmin_pair() {
        assert_eq!(
            root_auth_pair(&Config::default()),
            ("minioadmin".into(), "minioadmin".into())
        );
        assert_eq!(
            root_auth_pair(&configured()),
            ("root-ak".into(), "root-sk".into())
        );
    }

    #[test]
    fn owner_uids_map_is_empty_without_mappings() {
        // No-chown default: the empty map keeps every object
        // server-user-owned (the hardened mode); Windows configs cannot
        // carry a mapping at all (parse rejects it).
        assert!(owner_uids_for(&Config::default()).is_empty());
        assert!(
            owner_uids_for(&Config {
                users: vec![users::Config {
                    access_key: "ak".into(),
                    secret_key: auth::SecretKey::from("sk").into(),
                    canonical_id: None,
                    display_name: None,
                    local_uid: None,
                }],
                ..Config::default()
            })
            .is_empty()
        );
    }

    #[cfg(unix)]
    #[test]
    fn owner_uids_for_assembles_configured_mappings() {
        let config = Config {
            owner: owner::Config {
                local_uid: Some(1000),
                ..owner::Config::default()
            },
            users: vec![users::Config {
                access_key: "AKID".into(),
                secret_key: auth::SecretKey::from("sk").into(),
                canonical_id: None,
                display_name: None,
                local_uid: Some(1001),
            }],
            ..Config::default()
        };
        let map = owner_uids_for(&config);
        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(
                &OwnerId::new("d16b7e8c0bb9728d01e3bf9c30940a32622195f821325af577a87bd6284ac306")
                    .unwrap()
            ),
            Some(&1000)
        );
        assert_eq!(
            map.get(
                &OwnerId::new("2c8a2a08ad81dddf7e7830cbc75310f731a1381431bb29afb55a76ee07e81721")
                    .unwrap()
            ),
            Some(&1001)
        );
    }
}
