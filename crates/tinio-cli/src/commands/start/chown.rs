//! Spec §6 OS-mapping validation: the fail-closed chown-privilege probe.
//!
//! When `[owner]`/`[[users]]` configure any `local_uid`, startup must be able to chown object
//! files to the mapped uids — root (euid 0) or CAP_CHOWN — and fails with a config error
//! otherwise (no silent degradation to server-user ownership). The probe lives with its only
//! caller, the CLI's startup path; Windows configs never reach it — the parser rejects the key.

use std::path::Path;
#[cfg(unix)]
use std::{
    os::unix::fs::{DirBuilderExt, chown},
    time::{SystemTime, UNIX_EPOCH},
};

use tinio_server::identity;

use crate::config::Error;

/// Spec §6 OS-mapping validation (unix): when `[owner]`/`[[users]]` configure ANY `local_uid`,
/// startup must be able to chown object files to the mapped uids — root (euid 0) or CAP_CHOWN —
/// and fails with a config error otherwise (fail-closed: no silent degradation to server-user
/// ownership). Windows configs never reach this — the parser rejects the key.
///
/// The probe reuses the real mechanism (`std::os::unix::fs::chown`) on a private temp file inside
/// `dir` rather than testing `geteuid()` only, so a CAP_CHOWN-only process (the mitigation the
/// design prefers over full root) verifies as privileged too.
#[cfg(unix)]
pub(crate) fn ensure_chown_privilege(config: &identity::Config, dir: &Path) -> Result<(), Error> {
    if identity::owner_uids_for(config).is_empty() {
        return Ok(());
    }
    probe_chown(dir).map_err(|err| {
        Error::invalid_value(
            "local_uid",
            format!(
                "chown privilege (root or CAP_CHOWN) is required for the configured local_uid mappings but the process lacks it — startup fails closed: {err}. Remove the mappings or grant the capability"
            ),
        )
    })
}

/// Windows never maps `local_uid` — the parser rejects the key — so there is nothing to probe.
#[cfg(not(unix))]
pub(crate) fn ensure_chown_privilege(config: &identity::Config, dir: &Path) -> Result<(), Error> {
    let _ = (config, dir);
    Ok(())
}

/// The chown probe: create a private 0700 dir + file under `dir`, chown the file away from our own
/// uid (to 0) — allowed only with root euid or CAP_CHOWN — and clean up (the 0700 dir stays
/// removable even after the file is root-owned).
#[cfg(unix)]
fn probe_chown(dir: &Path) -> std::io::Result<()> {
    use std::{
        fs,
        fs::{DirBuilder, File},
        process,
    };
    let nonce = format!(
        "{}-{}",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let probe_dir = dir.join(format!(".tinio-chown-probe-{nonce}"));
    let mut builder = DirBuilder::new();
    builder.mode(0o700);
    builder.create(&probe_dir)?;
    let file = probe_dir.join("probe");
    let outcome = (|| {
        File::create(&file)?;
        chown(&file, Some(0), None)
    })();
    let _ = fs::remove_file(&file);
    let _ = fs::remove_dir(&probe_dir);
    outcome
}

#[cfg(all(test, unix))]
mod tests {
    use tinio_server::identity::owner;

    use super::*;

    #[test]
    fn mapping_configured_without_chown_privilege_fails_startup() {
        // Spec §6: fail closed — any local_uid mapping requires chown privilege (root or
        // CAP_CHOWN); without it startup fails with a config error. Skipped when the runner is
        // privileged (the probe passes — the failure is unobservable).
        let root = tempfile::tempdir().unwrap();
        if probe_chown(root.path()).is_ok() {
            return;
        }
        let config = identity::Config {
            owner: owner::Config {
                local_uid: Some(1000),
                ..owner::Config::default()
            },
            ..identity::Config::default()
        };
        let err = ensure_chown_privilege(&config, root.path()).unwrap_err();
        assert!(matches!(err, Error::InvalidValue { .. }), "{err}");
        assert!(err.to_string().contains("local_uid"), "{err}");
        // Without any mapping the check passes even unprivileged.
        ensure_chown_privilege(&identity::Config::default(), root.path()).unwrap();
    }
}
