//! Where the jail lives on the host, and where each exposed path lands inside it.

use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use tracing::debug;

use crate::_core::{Error, Result, refused};

/// The `step` every refusal about an exposed path carries, wherever it is
/// built.
///
/// **One string for four refusal reasons across two modules**, which is why it
/// is a constant rather than a literal at each site: the four `Error::BadPath`s
/// below and `enter`'s "does not exist" refusal are the same operator-facing
/// operation — *this path cannot be used as an exposure* — and shared §7's rule
/// is that the event and the `Error` it is emitted beside name the same
/// operation, so that a log line and a returned failure cannot be read as two
/// different things. Two modules spelling `"expose"` separately are two places
/// to change the day one of them is renamed, which is the drift that rule
/// exists to prevent.
pub(crate) const EXPOSE_STEP: &str = "expose";

/// The directory the tmpfs jail is mounted on.
///
/// The name is derived from the served root, carries **no pid**, and is the
/// same for every spelling of one root: a hard kill then leaves at most one
/// directory per root rather than one per run, and the next start takes that
/// one over (design decision 10).
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_linux::jail_root;
///
/// let jail = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
/// assert!(jail.starts_with("/tmp"));
/// ```
pub fn jail_root(tmpdir: &Path, served_root: &Path) -> PathBuf {
    // **The components, not the raw bytes, and the difference is a spelling.**
    // `as_encoded_bytes()` would hash `/srv/data`, `/srv/data/` and
    // `/srv/./data` to three different names, so a restart with the trailing
    // separator the shell completed for the operator would compute a second
    // directory and the first would never be taken over — which is exactly the
    // residue design decision 10 exists to bound. The leading separator is
    // re-supplied between components so the encoding stays injective: `/ab/c`
    // and `/a/bc` are different roots and must not collide. `..` is emitted as
    // its own marker for the same reason, though `enter` has already refused
    // such an exposure through `mirror_under` before this is ever reached.
    let mut key = Vec::new();
    for component in served_root.components() {
        match component {
            Component::Normal(part) => {
                key.push(b'/');
                key.extend_from_slice(part.as_encoded_bytes());
            }
            Component::ParentDir => key.extend_from_slice(b"/.."),
            _ => {}
        }
    }
    let mut hasher = Sha256::new();
    hasher.update(&key);
    let digest = hex::encode(hasher.finalize());
    let jail = tmpdir.join(format!(".tinio-jail-{}", &digest[..16]));
    // One event per operation, with the operation's own arguments as fields
    // rather than as a sentence (shared §7). The crate installs no subscriber,
    // so this writes nothing unless the process that owns the runtime has
    // installed one; in `serve` that is the stderr subscriber, and the jail's
    // path is what an operator needs when a mount does not land.
    debug!(step = "jail_root", root = %served_root.display(), jail = %jail.display(), "jail directory derived");
    jail
}

/// Where `host` must be mounted **inside** the jail so that its path string is
/// unchanged by the pivot.
///
/// # Errors
///
/// [`Error::BadPath`] when `host` is relative (the jail has no meaningful place
/// to put it), when it contains a `..` component (which would walk the mirror
/// back out of the jail), or when the composed mirror is the jail root itself —
/// which only a host of `/` produces, and which would make the first entry of
/// the mount plan a self-bind of the jail tmpfs.
///
/// **Each refusal is logged where it is built, with [`EXPOSE_STEP`] as its
/// `step`** (shared §7). This is a public function, so the caller may be
/// outside the crate — and until 2026-09-26 the only event a refused exposure
/// produced was emitted by `enter`, which a direct caller never runs: the
/// failure came back with no record of it anywhere. Folding the event into the
/// construction site is what makes the log and the `Error` the same fact for
/// every caller rather than for this crate's own.
///
/// # Examples
///
/// ```
/// use std::path::{Path, PathBuf};
///
/// use tinio_sandbox_linux::mirror_under;
///
/// let inside = mirror_under(Path::new("/tmp/j"), Path::new("/srv/data")).unwrap();
/// assert_eq!(inside, PathBuf::from("/tmp/j/srv/data"));
/// ```
pub fn mirror_under(jail: &Path, host: &Path) -> Result<PathBuf> {
    if !host.is_absolute() {
        return Err(refused(
            EXPOSE_STEP,
            Error::BadPath {
                path: host.to_path_buf(),
                reason: "not absolute",
            },
        ));
    }
    // `RootDir` is the leading `/` the jail already supplies and `CurDir` is a
    // no-op, so both are skipped and `/srv/./data` and `/srv/data` land
    // together. `..` is refused rather than pushed: `components()` does not
    // fold it away, so `/srv/../etc` would mirror to `<jail>/srv/../etc`, which
    // resolves to `<jail>/etc` when the kernel is in a good mood and to
    // something outside the jail when it is not — and nothing above this
    // function has a reason to represent a path that climbs.
    let mut out = jail.to_path_buf();
    for component in host.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                return Err(refused(
                    EXPOSE_STEP,
                    Error::BadPath {
                        path: host.to_path_buf(),
                        reason: "contains `..`",
                    },
                ));
            }
            // `Normal` on every platform this crate builds for; `Prefix` is
            // Windows-only and cannot appear in a Linux path.
            other => out.push(other.as_os_str()),
        }
    }
    // **The composed mirror is what is validated, not the host spelling.** The
    // spelling-versus-spelling comparison this replaces was a tautology in
    // production — the jail is derived from the temp directory and the host is
    // whatever the operator exposed, so `host == jail` is false on every host
    // anyone runs — while the case that bites is its opposite: a host whose
    // mirror *is* the jail. That is `/`, and only `/`: every component is a
    // `RootDir`, the loop skips them all, and the result is the jail root. The
    // plan's first record would then be a self-bind of the jail tmpfs, every
    // later target would land on the host's own `/`, and `apply_mount` would
    // create those directories on the host before any confinement exists.
    if out == jail {
        return Err(refused(
            EXPOSE_STEP,
            Error::BadPath {
                path: host.to_path_buf(),
                reason: "jail root",
            },
        ));
    }
    // Unreachable while `..` is refused and every pushed component is a
    // `Normal` one — kept because "the mirror is strictly below the jail root"
    // is the invariant the mount plan, `apply_mount` and the pivot all rest on,
    // and one comparison is a cheap way to keep it true if the loop above ever
    // grows a case.
    if !out.starts_with(jail) {
        return Err(refused(
            EXPOSE_STEP,
            Error::BadPath {
                path: host.to_path_buf(),
                reason: "escapes the jail",
            },
        ));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::testing::{assert_step, record_events};

    #[test]
    fn jail_root_name_is_stable_and_root_derived() {
        let a = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        let b = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        assert_eq!(a, b, "same root must give the same jail directory");
        assert_ne!(a, jail_root(Path::new("/tmp"), Path::new("/srv/other")));
    }

    #[test]
    fn jail_root_name_is_the_same_for_every_spelling_of_one_root() {
        // Design decision 10: the name is fixed so that a hard kill leaves at
        // most one directory per root and the next start takes it over. That
        // holds only if one root has one name — hashing the path's raw bytes
        // gave three, because the trailing separator and the `.` are part of
        // them, so a restart spelled the way a shell completed it left the
        // first jail behind. **The components are hashed instead, and this is
        // the test that says so**: the neighbouring
        // `jail_root_name_is_stable_and_root_derived` varies the *root*, which
        // is a different property.
        let jail = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        for spelling in ["/srv/data/", "/srv/./data", "/srv//data"] {
            assert_eq!(
                jail,
                jail_root(Path::new("/tmp"), Path::new(spelling)),
                "{spelling}"
            );
        }
        // And the encoding stays injective, which is why the separator is
        // re-supplied between components rather than dropped: these two are
        // different roots and may not share a jail.
        assert_ne!(
            jail_root(Path::new("/tmp"), Path::new("/ab/c")),
            jail_root(Path::new("/tmp"), Path::new("/a/bc")),
        );
    }

    #[test]
    fn jail_root_name_is_fixed_not_pid_suffixed() {
        // Design decision 10: a hard kill must leave at most one directory per
        // root, so the name may not carry the pid.
        let name = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        let file = name.file_name().and_then(|n| n.to_str()).unwrap();
        assert!(file.starts_with(".tinio-jail-"), "{file}");
        assert_eq!(file.len(), ".tinio-jail-".len() + 16, "{file}");
        // The length assertion above **is** the pid check: a name that carried
        // the pid would be longer than the prefix plus a 16-character digest.
        // Asserting `!file.contains(&process::id().to_string())` instead is not
        // equivalent and is not what the design says — the digest is 16 hex
        // characters, so any pid whose decimal spelling occurs inside it (a
        // single digit is the common case) fails a correct implementation.
    }

    #[test]
    fn mirror_keeps_the_original_absolute_path() {
        assert_eq!(
            mirror_under(Path::new("/tmp/.tinio-jail-abc"), Path::new("/srv/data")).unwrap(),
            PathBuf::from("/tmp/.tinio-jail-abc/srv/data")
        );
    }

    #[test]
    fn mirror_normalises_cur_dir_components() {
        assert_eq!(
            mirror_under(Path::new("/tmp/j"), Path::new("/srv/./data")).unwrap(),
            mirror_under(Path::new("/tmp/j"), Path::new("/srv/data")).unwrap()
        );
    }

    #[test]
    fn mirror_rejects_relative_paths() {
        let err = mirror_under(Path::new("/tmp/j"), Path::new("relative/path")).unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "not absolute",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn mirror_rejects_parent_dir_components() {
        // Without this the loop pushes `..` like any other component and the
        // mirror resolves back out of the jail: `<jail>/srv/../etc` is
        // `<jail>/etc` on a good day and the host's `/etc` on a bad one.
        for host in ["/srv/../etc", "/..", "/srv/data/../.."] {
            let err = mirror_under(Path::new("/tmp/j"), Path::new(host)).unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::BadPath {
                        reason: "contains `..`",
                        ..
                    }
                ),
                "{host}: {err:?}"
            );
        }
    }

    #[test]
    fn mirror_rejects_the_host_root_which_mirrors_onto_the_jail() {
        // **The case the composed check exists for.** Every component of `/` is
        // a `RootDir`, the loop skips all of them, and the mirror comes out as
        // the jail itself — a self-bind of the jail tmpfs as the plan's first
        // record, which a later recursive bind turns into the host root over
        // the jail root. A guard that compared the host spelling against the
        // jail string would not see it: the jail is derived from the temp
        // directory and never equals a served root in production.
        let err = mirror_under(Path::new("/tmp/j"), Path::new("/")).unwrap_err();
        assert!(
            matches!(
                err,
                Error::BadPath {
                    reason: "jail root",
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn every_accepted_mirror_is_strictly_below_the_jail_root() {
        for host in ["/srv/data", "/srv/./data", "/srv/data/deep", "/a"] {
            let mirrored = mirror_under(Path::new("/tmp/j"), Path::new(host)).unwrap();
            assert!(
                mirrored.starts_with("/tmp/j"),
                "{host} → {}",
                mirrored.display()
            );
            assert_ne!(
                mirrored,
                Path::new("/tmp/j"),
                "{host} mirrors onto the jail itself"
            );
        }
    }

    #[test]
    fn the_jail_root_derivation_is_traced_with_its_step() {
        // **The `debug!` half of shared §7, and the event an operator reaches
        // for when a mount does not land**: the jail's own path is not in the
        // mount plan's `info!` set, so this is where it is recorded. The
        // recorder is `crate::testing`'s — one for the whole crate, because the
        // `step` field is the assertion in every module that makes it and the
        // helper's doc carries the reason it is a global capture rather than a
        // thread-scoped one.
        let (jail, log) = record_events(|| jail_root(Path::new("/tmp"), Path::new("/srv/data")));
        assert_step(&log, "jail_root");
        assert!(log.contains(&jail.display().to_string()), "{log}");
    }

    #[test]
    fn a_refused_exposure_is_logged_where_it_is_refused() {
        // **A public function's failure needs its own event, and this is the
        // caller that never had one** (shared §7: the event belongs where the
        // returned `Error` is built): `mirror_under` is reachable from outside
        // the crate, and the only event a refused exposure used to produce came
        // from `enter`, which such a caller never runs. The four reasons share
        // one `step` — the operation, not the reason — and the reason is a field
        // of the `Error`, which is what this asserts beside the step.
        for (host, reason) in [
            ("relative/path", "not absolute"),
            ("/srv/../etc", "contains `..`"),
            ("/", "jail root"),
        ] {
            let (error, logged) =
                record_events(|| mirror_under(Path::new("/tmp/j"), Path::new(host)));
            let Error::BadPath {
                reason: refused, ..
            } = error.expect_err("refused")
            else {
                panic!("{host} must be refused as a bad path");
            };
            assert_eq!(refused, reason, "{host}");
            assert_step(&logged, EXPOSE_STEP);
            assert!(logged.contains(host), "{logged}");
        }
    }
}
