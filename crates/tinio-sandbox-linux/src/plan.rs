//! The ordered list of mounts that turns a [`Sandbox`] into a jail.

use std::path::{Path, PathBuf};

use crate::{
    _core::{AccessMode, Error, Result, Sandbox, refused},
    Mechanism,
    jail::mirror_under,
    options::Options,
};

/// The four device files bound into every jail that asks for them.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::DEVICE_FILES;
///
/// assert_eq!(
///     DEVICE_FILES,
///     ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"]
/// );
/// ```
pub const DEVICE_FILES: [&str; 4] = ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"];

/// One mount to perform, in order.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_linux::{Mechanism, Mount, Options, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let opts = Options::default();
/// let mounts: Vec<Mount> = plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &opts).unwrap();
/// assert_eq!(mounts[1].source, Path::new("/srv/data"));
/// assert_eq!(mounts[1].target, Path::new("/tmp/j/srv/data"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// What is mounted. For [`MountKind::JailTmpfs`] this is the literal
    /// `"tmpfs"` — there is no source path.
    pub source: PathBuf,
    /// Where it lands: at or below the jail root, at the exposed path's own
    /// absolute path. [`MountKind::JailTmpfs`]'s own target **is** the jail
    /// root, and every later record is strictly below it.
    pub target: PathBuf,
    /// The access the confined process gets through this mount.
    pub access: AccessMode,
    /// How to perform it.
    pub kind: MountKind,
}

/// How a [`Mount`] is performed.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_linux::{Mechanism, MountKind, Options, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
/// let opts = Options::default();
/// let mounts = plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &opts).unwrap();
/// let data = mounts
///     .iter()
///     .find(|mount| mount.source == Path::new("/srv/data"))
///     .unwrap();
/// assert_eq!(
///     data.kind,
///     MountKind::Bind,
///     "a read-only exposure is a plain bind"
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    /// A tmpfs on the jail root; always first, so every later target has a
    /// parent directory.
    JailTmpfs,
    /// `mount --bind <source> <target>`. A read-only access is enforced *after*
    /// the bind, by the remount `apply_mount` performs and by the overlay it
    /// substitutes where the kernel refuses that remount — **the source's locked
    /// per-mount flags decide which of the two, not this plan**, which is why
    /// every read-only exposure is this kind in both models.
    Bind,
    /// An overlay whose lower is the source, mounted `MS_RDONLY` from the
    /// start. **The fallback, not the default**: `apply_mount` substitutes it
    /// for a read-only bind whose remount the kernel refuses with `EPERM` —
    /// which is what **locked** per-mount flags answer, since every host mount
    /// is copied into a namespace owned by another user namespace with nosuid,
    /// nodev, noexec, ro and atime locked and must be re-asserted in full.
    /// [`plan`] therefore never emits it: which flags a served root's bind
    /// carries is not something this side of the syscall can know.
    OverlayReadOnly,
}

/// The two writable directories an [`MountKind::OverlayReadOnly`] needs. They
/// live inside the jail's own tmpfs, so nothing is written to the served root —
/// and, for the same reason, they are reachable inside the jail afterwards at
/// `/.overlay` and share the jail tmpfs's quota (1 MiB, see `enter`): a
/// confined process can write **there**, and a write that spends the last page
/// fails with `ENOSPC`.
///
/// **A write *through* the mounted exposure is `EROFS`, never `ENOSPC`, and the
/// two cases used to be written as one.** Such a write is refused at the mount
/// — it is `MS_RDONLY` from the start, so the tmpfs is never asked — which is
/// what `AccessMode::ReadOnly` means on this platform. The quota is the second
/// wall, and it stands behind the other door: the overlay's own directories,
/// which the confined process reaches because they live in the jail's tmpfs and
/// no mount covers them.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
///
/// use tinio_sandbox_linux::OverlayDirs;
///
/// let overlay = OverlayDirs {
///     upper: PathBuf::from("/tmp/j/.overlay/upper"),
///     work: PathBuf::from("/tmp/j/.overlay/work"),
/// };
/// assert_ne!(overlay.upper, overlay.work, "one overlay, two directories");
/// assert!(overlay.upper.starts_with("/tmp/j") && overlay.work.starts_with("/tmp/j"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayDirs {
    /// The overlay's upper layer.
    pub upper: PathBuf,
    /// The overlay's work directory.
    pub work: PathBuf,
}

/// Compose the jail's mount table.
///
/// **`model` must already be resolved before it arrives.** [`Mechanism::Auto`]
/// is a request rather than a decision — `Mechanism::resolved` is what turns it
/// into one — and it reaches this function with the guard below skipped, since
/// the guard is keyed on the model it can check.
///
/// **`opts` is read for one fact, and the model is deliberately not taken from
/// it.** [`Options::device_files`] is this platform's own knob — the flag moved
/// off `Sandbox`, which carried a Linux-only request on every platform — and it
/// is read here because this is the function that appends the four device binds.
/// The model stays an argument: `opts.model()` may be [`Mechanism::Auto`], and
/// reading it here as well would put the unresolved request back in front of a
/// guard that can only check the resolved one.
///
/// # Errors
///
/// [`Error::BadPath`] for an exposure [`mirror_under`] rejects, and
/// [`Error::Unsupported`] when the user-namespace model is asked for more than
/// one read-only exposure — the overlay pair is created once, an overlay upper
/// is bound to a single lower, and the pair is what a refused remount falls back
/// to, so the second read-only exposure is refused by name rather than left to
/// race the first as an unreadable overlayfs error.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_linux::{Mechanism, Options, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let opts = Options::default();
/// let mounts = plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &opts).unwrap();
/// assert_eq!(mounts.len(), 2, "the jail tmpfs plus the one exposure");
/// ```
pub fn plan(
    sandbox: &Sandbox,
    jail: &Path,
    model: Mechanism,
    opts: &Options,
) -> Result<Vec<Mount>> {
    let mut mounts = vec![Mount {
        source: PathBuf::from("tmpfs"),
        target: jail.to_path_buf(),
        access: AccessMode::ReadWrite,
        kind: MountKind::JailTmpfs,
    }];

    if model == Mechanism::UserNamespace
        && sandbox
            .exposures()
            .iter()
            .filter(|exposure| exposure.access == AccessMode::ReadOnly)
            .count()
            > 1
    {
        // **Logged where the `Error` is built** (shared §7). `plan` is public,
        // so a direct caller is a caller this refusal never happened for until
        // 2026-09-26 — the event came from `enter`, which wraps the call — and
        // the `step` is the operation this function performs rather than the
        // caller's name for it, so one failure is one event with one step
        // whichever side of the crate boundary it was reached from.
        return Err(refused(
            "plan",
            Error::Unsupported {
                detail: "the user-namespace model supports at most one read-only exposure: the \
                         fallback overlay is created once, and every read-only mount a refused \
                         remount falls back to would share its upper directory, which an overlay \
                         binds to a single lower",
            },
        ));
    }

    for exposure in sandbox.exposures() {
        let target = mirror_under(jail, &exposure.host)?;
        // **Every exposure is a plain bind, read-only ones included.** Which
        // mechanism makes a read-only bind read-only — a remount, or the
        // `MS_RDONLY` overlay where the kernel refuses it — is decided by the
        // source's **locked per-mount flags** when `apply_mount` attempts it,
        // and this function performs no syscall: there is nothing here that
        // could know which ones a given served root's bind carries. The
        // `access` field is what carries the read-only intent forward.
        mounts.push(Mount {
            source: exposure.host.clone(),
            target,
            access: exposure.access,
            kind: MountKind::Bind,
        });
    }

    if opts.device_files() {
        for device in DEVICE_FILES {
            mounts.push(Mount {
                source: PathBuf::from(device),
                target: mirror_under(jail, Path::new(device))?,
                access: AccessMode::ReadWrite,
                kind: MountKind::Bind,
            });
        }
    }

    Ok(mounts)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::_core::{AccessMode, Error, Sandbox};

    const JAIL: &str = "/tmp/.tinio-jail-abc";

    #[test]
    fn the_jail_tmpfs_is_always_first() {
        let sandbox = Sandbox::new().expose("/srv/deep/data", AccessMode::ReadWrite);
        let opts = Options::default();
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::Root, &opts).unwrap();
        assert_eq!(mounts[0].kind, MountKind::JailTmpfs);
        assert_eq!(mounts[0].target, PathBuf::from(JAIL));
    }

    #[test]
    fn read_write_exposure_is_a_plain_bind_in_both_models() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        for model in [Mechanism::UserNamespace, Mechanism::Root] {
            let mounts = plan(&sandbox, Path::new(JAIL), model, &Options::default()).unwrap();
            let data = mounts
                .iter()
                .find(|m| m.source == Path::new("/srv/data"))
                .unwrap();
            assert_eq!(data.kind, MountKind::Bind);
            assert_eq!(data.target, PathBuf::from(format!("{JAIL}/srv/data")));
        }
    }

    #[test]
    fn read_only_is_a_plain_bind_in_both_models() {
        // **The mechanism is the kernel's to accept, not the plan's to pick,
        // so there is no model test left to make here**: what refuses a
        // read-only remount is **locked** per-mount flags on the bind's source
        // — every host mount is copied into a namespace owned by another user
        // namespace with them locked, and the request must re-assert all of
        // them — so the `MS_RDONLY` overlay is only what `apply_mount` falls
        // back to there. A plan that chose `OverlayReadOnly` on a model test
        // would be choosing by the wrong property, and would be wrong on the
        // very hosts a served root lives on: the mistake this test catches.
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
        for model in [Mechanism::UserNamespace, Mechanism::Root] {
            let mounts = plan(&sandbox, Path::new(JAIL), model, &Options::default()).unwrap();
            let data = mounts
                .iter()
                .find(|m| m.source == Path::new("/srv/data"))
                .unwrap();
            assert_eq!(data.kind, MountKind::Bind, "{model:?}");
            assert!(data.access.is_read_only(), "{model:?}");
        }
    }

    #[test]
    fn writable_exposures_stay_plain_binds_even_with_a_read_only_root() {
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadOnly)
            .expose("/home/u/.tinio/roots/h", AccessMode::ReadWrite);
        let opts = Options::default();
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::UserNamespace, &opts).unwrap();
        let state = mounts
            .iter()
            .find(|m| m.source == Path::new("/home/u/.tinio/roots/h"))
            .unwrap();
        assert_eq!(
            state.kind,
            MountKind::Bind,
            "a writable exposure takes no read-only mechanism"
        );
        assert!(!state.access.is_read_only());
    }

    #[test]
    fn device_files_come_last_and_are_binds() {
        // **The flag is the options' now**, so the fixture that used to ask the
        // `Sandbox` for them builds the `Options` the plan reads instead — and
        // this is the one test whose options are not the defaults.
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        let opts = Options::default().with_device_files(true);
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::Root, &opts).unwrap();
        let tail = &mounts[mounts.len() - DEVICE_FILES.len()..];
        let names: Vec<&Path> = tail.iter().map(|mount| mount.source.as_path()).collect();
        assert_eq!(
            names,
            DEVICE_FILES.map(Path::new),
            "the four device files, in order"
        );
        for mount in tail {
            assert_eq!(mount.kind, MountKind::Bind, "{mount:?}");
            assert_eq!(
                mount.target,
                PathBuf::from(format!("{JAIL}{}", mount.source.display())),
                "a device file is mirrored at its own absolute path"
            );
            assert!(
                !mount.access.is_read_only(),
                "a device file is bound read-write"
            );
        }
    }

    #[test]
    fn a_second_read_only_exposure_is_refused_under_the_user_namespace() {
        // One overlay pair, one lower — and the pair is the *fallback's*
        // resource: a second read-only exposure would race the first for the
        // same upper directory exactly when the kernel refuses both remounts,
        // which is the user namespace's case. Refused by name here rather than
        // surfaced later as an overlayfs error.
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadOnly)
            .expose("/srv/other", AccessMode::ReadOnly);
        let opts = Options::default();
        let err = plan(&sandbox, Path::new(JAIL), Mechanism::UserNamespace, &opts).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        assert!(err.to_string().contains("read-only exposure"), "{err}");

        // The same two exposures are fine under root, where the remount is not
        // refused and the pair is therefore never reached.
        assert!(plan(&sandbox, Path::new(JAIL), Mechanism::Root, &opts).is_ok());
    }
}
