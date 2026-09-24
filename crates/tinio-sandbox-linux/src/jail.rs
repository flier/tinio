//! Where the jail lives on the host, and where each exposed path lands inside it.

use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use tracing::debug;

use crate::_core::{Error, Result};

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
        return Err(Error::BadPath {
            path: host.to_path_buf(),
            reason: "not absolute",
        });
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
                return Err(Error::BadPath {
                    path: host.to_path_buf(),
                    reason: "contains `..`",
                });
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
        return Err(Error::BadPath {
            path: host.to_path_buf(),
            reason: "jail root",
        });
    }
    // Unreachable while `..` is refused and every pushed component is a
    // `Normal` one — kept because "the mirror is strictly below the jail root"
    // is the invariant the mount plan, `apply_mount` and the pivot all rest on,
    // and one comparison is a cheap way to keep it true if the loop above ever
    // grows a case.
    if !out.starts_with(jail) {
        return Err(Error::BadPath {
            path: host.to_path_buf(),
            reason: "escapes the jail",
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    };

    use tracing::{Level, subscriber::with_default};
    use tracing_subscriber::fmt;

    use super::*;

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
        // `tracing` emits nothing until a subscriber exists, so "this operation
        // is traced" is only assertable against a sink (shared §7) — and the
        // `step` field is the assertion because it is the string a failure
        // event and an `Error::Step` have to share.
        let (jail, log) = traced(|| jail_root(Path::new("/tmp"), Path::new("/srv/data")));
        assert!(log.contains(r#"step="jail_root""#), "{log}");
        assert!(log.contains(&jail.display().to_string()), "{log}");
    }

    /// Run `f` under a subscriber that writes into a buffer, and hand back what
    /// `f` returned together with the log it produced.
    ///
    /// A thread-local default rather than a global one, so it cannot leak into
    /// the tests libtest runs beside this one.
    fn traced<T>(f: impl FnOnce() -> T) -> (T, String) {
        let sink = Sink::default();
        let subscriber = fmt()
            .with_writer({
                let sink = sink.clone();
                move || sink.clone()
            })
            .with_max_level(Level::DEBUG)
            // ANSI is on whenever the `ansi` feature is enabled and `NO_COLOR`
            // is unset, and the escapes land *inside* `step="jail_root"` — the
            // assertion below would then turn on the operator's environment
            // rather than on the event.
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .finish();
        let value = with_default(subscriber, f);
        let log = String::from_utf8(sink.0.lock().unwrap().clone()).expect("utf-8 log");
        (value, log)
    }

    /// The `io::Write` half of `traced`; `MakeWriter` is implemented for a
    /// closure returning one of these, which is the shortest way to read a
    /// subscriber's output back out of a test.
    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
