//! Config-driven identity assembly (spec §5a/§6): the `[auth]` +
//! `[owner]` + `[[users]]` sections become the acl identity map
//! ([`IdentityFromConfig::from_config`]) and the chown `OwnerId → uid`
//! map ([`owner_uids_for`]). Plain data in, plain data out — the only
//! config consumers of the identity types (feature `acl`).

use std::collections::HashMap;
#[cfg(feature = "acl")]
use std::sync::Arc;
#[cfg(unix)]
use std::{
    os::unix::fs::{DirBuilderExt, chown},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "acl")]
use s3s::auth::SecretKey;
use secrecy::ExposeSecret;

#[cfg(feature = "acl")]
use crate::_auth::{Identity, User};
use crate::{
    _config::Config,
    _core::acl::{DEFAULT_ROOT_ACCESS_KEY, OwnerId},
};

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

/// The fs store's owner→uid map (unix chown on write): config-supplied
/// only, never request-derived (no injection vector). Windows configs
/// cannot carry a mapping (parse rejects any `local_uid`), so the map is
/// empty there by construction; the empty map = no chown, objects stay
/// server-user-owned (the hardened default).
pub fn owner_uids_for(config: &Config) -> HashMap<OwnerId, u32> {
    let mut map = HashMap::new();
    let owner = config.owner();
    if let Some(uid) = owner.local_uid {
        map.insert(owner.resolved_canonical_id(), uid);
    }
    for user in config.users() {
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
    /// Build the identity map for the acl plane from the parsed config.
    fn from_config(config: &Config) -> Arc<Identity>;
}

#[cfg(feature = "acl")]
impl IdentityFromConfig for Identity {
    fn from_config(config: &Config) -> Arc<Identity> {
        let owner = config.owner();
        let owner_id = owner.resolved_canonical_id();
        let (access_key, secret) = root_auth_pair(config);
        let root = User {
            access_key: access_key.clone(),
            canonical_id: owner_id.clone(),
            display_name: owner.display_name.clone(),
            secret: SecretKey::from(secret),
        };
        let mut users: HashMap<String, User> = HashMap::with_capacity(1 + config.users().len());
        users.insert(root.access_key.clone(), root);
        for user in config.users() {
            users.insert(
                user.access_key.clone(),
                User {
                    access_key: user.access_key.clone(),
                    canonical_id: user.resolved_canonical_id(),
                    display_name: user.effective_display_name(),
                    secret: SecretKey::from(&**user.secret_key.expose_secret()),
                },
            );
        }
        Arc::new(Identity {
            users,
            default_owner: owner_id,
            default_display_name: owner.display_name.clone(),
        })
    }
}

/// Spec §6 OS-mapping validation (unix): when `[owner]`/`[[users]]`
/// configure ANY `local_uid`, startup must be able to chown object files
/// to the mapped uids — root (euid 0) or CAP_CHOWN — and fails with a
/// config error otherwise (fail-closed: no silent degradation to
/// server-user ownership). Windows configs never reach this — the
/// parser rejects the key.
///
/// The probe reuses the real mechanism (`std::os::unix::fs::chown`) on a
/// private temp file inside `dir` rather than testing `geteuid()` only,
/// so a CAP_CHOWN-only process (the mitigation the design prefers over
/// full root) verifies as privileged too.
#[cfg(unix)]
pub fn ensure_chown_privilege(config: &Config, dir: &Path) -> Result<(), crate::_config::Error> {
    let configured = config.owner().local_uid.is_some()
        || config.users().iter().any(|user| user.local_uid.is_some());
    if !configured {
        return Ok(());
    }
    probe_chown(dir).map_err(|err| {
        crate::_config::Error::invalid_value(
            "local_uid",
            format!(
                "chown privilege (root or CAP_CHOWN) is required for the configured local_uid mappings but the process lacks it — startup fails closed: {err}. Remove the mappings or grant the capability"
            ),
        )
    })
}

/// The chown probe: create a private 0700 dir + file under `dir`, chown
/// the file away from our own uid (to 0) — allowed only with root euid
/// or CAP_CHOWN — and clean up (the 0700 dir stays removable even after
/// the file is root-owned).
#[cfg(unix)]
fn probe_chown(dir: &Path) -> std::io::Result<()> {
    let nonce = format!(
        "{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let probe_dir = dir.join(format!(".tinio-chown-probe-{nonce}"));
    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    builder.create(&probe_dir)?;
    let file = probe_dir.join("probe");
    let outcome = (|| {
        std::fs::File::create(&file)?;
        chown(&file, Some(0), None)
    })();
    let _ = std::fs::remove_file(&file);
    let _ = std::fs::remove_dir(&probe_dir);
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Config {
        Config::parse(text).unwrap()
    }

    fn uid() -> OwnerId {
        OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100").unwrap()
    }

    #[cfg(feature = "acl")]
    #[test]
    fn from_config_assembles_root_and_users() {
        let config = parse(
            "version = 1\n[auth]\naccess_key = \"root-ak\"\nsecret_key = \"root-sk\"\n[owner]\ncanonical_id = \"ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100\"\ndisplay_name = \"ops\"\n[[users]]\naccess_key = \"AKID\"\nsecret_key = \"user-sk-1\"\ndisplay_name = \"alice\"\n[[users]]\naccess_key = \"BKID\"\nsecret_key = \"user-sk-2\"\n",
        );
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
        let config = parse("version = 1\n[auth]\naccess_key = \"ak\"\nsecret_key = \"sk\"\n");
        assert_eq!(root_auth_pair(&config), ("ak".into(), "sk".into()));
    }

    #[test]
    fn owner_uids_map_is_empty_without_mappings() {
        // No-chown default: the empty map keeps every object
        // server-user-owned (the hardened mode); Windows configs cannot
        // carry a mapping at all (parse rejects it).
        assert!(owner_uids_for(&Config::default()).is_empty());
        assert!(
            owner_uids_for(&parse(
                "version = 1\n[owner]\n[[users]]\naccess_key = \"ak\"\nsecret_key = \"sk\"\n"
            ))
            .is_empty()
        );
    }

    #[cfg(unix)]
    #[test]
    fn owner_uids_for_assembles_configured_mappings() {
        let config = parse(
            "version = 1\n[owner]\nlocal_uid = 1000\n[[users]]\naccess_key = \"AKID\"\nsecret_key = \"sk\"\nlocal_uid = 1001\n",
        );
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

    #[cfg(unix)]
    #[test]
    fn mapping_configured_without_chown_privilege_fails_startup() {
        // Spec §6: fail closed — any local_uid mapping requires chown
        // privilege (root or CAP_CHOWN); without it startup fails with a
        // config error. Skipped when the runner is privileged (the probe
        // passes — the failure is unobservable).
        let root = tempfile::tempdir().unwrap();
        if probe_chown(root.path()).is_ok() {
            return;
        }
        let config = parse("version = 1\n[owner]\nlocal_uid = 1000\n");
        let err = ensure_chown_privilege(&config, root.path()).unwrap_err();
        assert!(
            matches!(err, crate::_config::Error::InvalidValue { .. }),
            "{err}"
        );
        assert!(err.to_string().contains("local_uid"), "{err}");
        // Without any mapping the check passes even unprivileged.
        ensure_chown_privilege(&Config::default(), root.path()).unwrap();
    }
}
