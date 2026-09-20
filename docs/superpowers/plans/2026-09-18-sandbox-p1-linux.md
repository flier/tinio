# `--sandbox` P1 (Linux) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Confine the tinio server process to the paths the operator exposes, on Linux, using a private mount namespace and `pivot_root`, and cut it off from outbound network access.

**Architecture:** The Linux backend of the `Backend` trait P0 defines. It composes a tmpfs jail that re-creates each exposed path at its **original absolute path**, binds them, `pivot_root`s into it, drops every capability it no longer needs, and installs a seccomp filter denying outbound connection setup. Unlike the other two platforms it confines **in place** — `unshare` and `pivot_root` apply to the calling process — so there is no child, no payload and no supervision. The `serve` example is restructured from `#[tokio::main]` into a plain `fn main` that confines first and builds its runtime second, because `unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process.

**Tech Stack:** Rust 2024, `rustix` 1.1 (`fs`, `mount`, `pipe`, `process`, `runtime`, `thread`) for every syscall — `pipe` for the availability probe's channel, `runtime` for its `kernel_fork` and its `exit_group`, and `fs` for the `fcntl_setfl` that makes the probe's read end non-blocking (a bounded probe needs it: `rustix` sets no timers) — plus `sha2` + `hex` for the jail-directory name, `tracing` for the event every operation emits (shared §7), and `tracing-subscriber` in the crate's dev-dependencies because one test asserts on an event.

**No `assert_cmd` here, and that is a deletion rather than an omission.** The end-to-end test spawns the confined server with `std::process::Command` and the jail test spawns the child with `CARGO_BIN_EXE_sandbox-child`, so a dependency on it would be a manifest line nothing in this phase uses. The one thing it would offer is `assert_cmd::cargo::cargo_bin`, and that resolves *binary* targets: the `serve` example it would be asked for is not one, which is exactly why Task 8 resolves the example from the test binary's own profile directory instead.

**Spec:** `docs/superpowers/specs/2026-09-18-sandbox-design.md` — the design document and the authority for every decision cited below as `design §N` or `decision N`. Read it alongside this plan.

**Prerequisite: `2026-09-18-sandbox-p0-core.md`.** That plan builds `tinio-sandbox-core`, the facade and the three platform-crate stubs, and it is where the launcher-shared surface is defined. This plan consumes it and adds nothing to it. In particular it relies on, from P0:

- `AccessMode`, `Exposure`, `Sandbox` (`new`, `expose`, `device_files`, `exposures`, `device_files_enabled`, `from_exposures`), `Error`, `Result`
- `Backend`, `Confinement`, and `tinio_sandbox_linux::Backend` existing as a stub this plan fills in
- `Event` (`encode`), `LaunchPlan`, `LaunchPayload`, the `listener_fd_*` helpers
- the facade `tinio_sandbox` re-exporting all of it

**What this plan does *not* take from P0 is as telling as what it does.** There is no `Child`, no `supervise`, no `Observer` and no `reemit` here: Linux confines the running process, so there is no child to wait for, nothing to relay and no terminal event to record (design decision 6). Those four exist for the two launcher platforms; a Linux plan that imported them would be describing machinery it never runs. **None of that exempts this platform from writing `Event` frames of its own, and the earlier reading of this paragraph had it doing exactly that**: the readiness frame is `serve`'s here as on every other platform, and the `Error` frame a refusal sends goes out from this same process rather than through a relay — which is the correction Task 7 Step 5 records, and the reason a harness reading stdout no longer has to know which platform it is on.

**Sibling plans:** `-p2-windows.md` and `-p3-macos.md` fill in the other two stubs. They consume the same P0 surface; if any of them needs something shared that P0 does not have, it goes into P0, not into a platform crate. **What all four documents share rather than any one of them building — the conventions, the crate topology and its two shapes, the list of what a launcher platform owns, the names two documents must spell alike, and the three relays — is `2026-09-18-sandbox-shared.md`**, and each plan refers to it by section rather than writing it out again. **One shared fact is this plan's to hold rather than shared's: `serve`'s signature** — all three plans call it, and only this one may change it, because Task 7 is what restructures the entry point (the signature and the reason its last three parameters are optional are stated once there).

## Global Constraints

**The conventions the four plans share are stated once, in `2026-09-18-sandbox-shared.md` — *shared* from here on — §1 of which owns test-first, the panic rule, the doc-example rule, imports, the manifest rules, the per-crate lint allocation, English only, attribution, hard-wrapping and git writes. What is left here is this plan's own:

- **No `unsafe` outside the three enumerated sites** in `tinio-sandbox-linux`: Task 3's `kernel_fork` in the user-namespace probe and its **one** `unshare` wrapper — used again by Task 5's `enter` — and Task 4's seccomp install. `#[allow(unsafe_code)]` sits on those three functions and nowhere else. **Three rather than the design's two**, because `rustix` ships no safe `fork`: `rustix::process` exports none, and `rustix::runtime::kernel_fork` is `unsafe fn` (the probe's invariant is design §10's ordering — no thread exists yet — not the signature's). Design decision 14 already says the count "is established by the implementation, corrected here rather than asserted ahead of it", so this is that correction, and the design's site list gains the third entry as this phase lands. This is also the phase that records the rule for all three backends in `docs/cargo.md`, because it is the earliest one to reach `deny` (Task 9 Step 4).
- **This plan's imports backlog is about 14 sites**, mostly in test setup and FFI plumbing, where a bare `std::fs::write` or `std::mem::size_of` reads better than a wall of imports. Correct them in place as you write the code; do not relax the rule (shared §1).
- **Every operation this plan performs emits a `tracing` event, and the rule is shared §7's rather than a second copy of it.** What is this plan's own is the sink, and it is one sentence: `tinio-sandbox-linux` **installs no subscriber** — it emits events, and the process that owns the runtime decides where they go. In `serve` that subscriber already exists and writes to stderr, which is what keeps stdout free for the event protocol (design decision 19) and is also why the subscriber has to be installed *before* `confine` runs: events emitted into a process with no subscriber are dropped, and a confinement failure is exactly what an operator has to reconstruct from a log (Task 7 Step 5). The events land where their operations do — the jail directory (Task 1), the model resolution, the availability probe and each `unshare` (Task 3), the seccomp install and its one `warn!` (Task 4), each mount, each id-map write, `pivot_root`, the detach, the capability drop and the `capset` (Task 5) — and one of them is asserted by a test, so "it logs" is a property this crate can lose (Task 1 Step 1).

---

### Task 1: The jail root name and path mirroring (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/jail.rs`
- Modify: `crates/tinio-sandbox-linux/Cargo.toml`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: `Error`, `Result`.
- Produces: `jail_root(tmpdir: &Path, served_root: &Path) -> PathBuf`, `mirror_under(jail: &Path, host: &Path) -> Result<PathBuf>`.

`mirror_under` is the heart of design decision 9: it re-creates a host path inside the jail **at the same absolute path**, by stripping the leading separator and joining. That is the whole reason no path string changes across the pivot — and the reason the function validates the path it *composes* rather than the path it was handed: a mirror that is not strictly below the jail root is a jail that does not hold.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use super::*;

    #[test]
    fn jail_root_name_is_stable_and_root_derived() {
        let a = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        let b = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        assert_eq!(a, b, "same root must give the same jail directory");
        assert_ne!(a, jail_root(Path::new("/tmp"), Path::new("/srv/other")));
    }

    #[test]
    fn jail_root_name_is_fixed_not_pid_suffixed() {
        // Design decision 10: a hard kill must leave at most one directory per
        // root, so the name may not carry the pid.
        let name = jail_root(Path::new("/tmp"), Path::new("/srv/data"));
        let file = name.file_name().and_then(|n| n.to_str()).unwrap();
        assert!(file.starts_with(".tinio-jail-"), "{file}");
        assert!(!file.contains(&std::process::id().to_string()), "{file}");
        assert_eq!(file.len(), ".tinio-jail-".len() + 16, "{file}");
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
        assert!(matches!(err, Error::BadPath { reason: "not absolute", .. }), "{err:?}");
    }

    #[test]
    fn mirror_rejects_parent_dir_components() {
        // Without this the loop pushes `..` like any other component and the
        // mirror resolves back out of the jail: `<jail>/srv/../etc` is
        // `<jail>/etc` on a good day and the host's `/etc` on a bad one.
        for host in ["/srv/../etc", "/..", "/srv/data/../.."] {
            let err = mirror_under(Path::new("/tmp/j"), Path::new(host)).unwrap_err();
            assert!(
                matches!(err, Error::BadPath { reason: "contains `..`", .. }),
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
        assert!(matches!(err, Error::BadPath { reason: "jail root", .. }), "{err:?}");
    }

    #[test]
    fn every_accepted_mirror_is_strictly_below_the_jail_root() {
        for host in ["/srv/data", "/srv/./data", "/srv/data/deep", "/a"] {
            let mirrored = mirror_under(Path::new("/tmp/j"), Path::new(host)).unwrap();
            assert!(mirrored.starts_with("/tmp/j"), "{host} → {}", mirrored.display());
            assert_ne!(mirrored, Path::new("/tmp/j"), "{host} mirrors onto the jail itself");
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
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let sink = sink.clone();
                move || sink.clone()
            })
            .with_max_level(tracing::Level::DEBUG)
            .without_time()
            .with_target(false)
            .finish();
        let value = tracing::subscriber::with_default(subscriber, f);
        let log = String::from_utf8(sink.0.lock().unwrap().clone()).expect("utf-8 log");
        (value, log)
    }

    /// The `io::Write` half of `traced`; `MakeWriter` is implemented for a
    /// closure returning one of these, which is the shortest way to read a
    /// subscriber's output back out of a test.
    #[derive(Clone, Default)]
    struct Sink(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Sink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux jail::`
Expected: FAIL — `cannot find function 'jail_root'`.

- [ ] **Step 3: Write the implementation**

```rust
//! Where the jail lives on the host, and where each exposed path lands inside it.

use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use tinio_sandbox_core::{Error, Result};
use tracing::debug;

/// The directory the tmpfs jail is mounted on.
///
/// The name is derived from the served root and carries **no pid**: a hard
/// kill then leaves at most one directory per root rather than one per run,
/// and the next start takes it over (design decision 10).
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
    let mut hasher = Sha256::new();
    hasher.update(served_root.as_os_str().as_encoded_bytes());
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
        return Err(Error::BadPath { path: host.to_path_buf(), reason: "not absolute" });
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
                return Err(Error::BadPath { path: host.to_path_buf(), reason: "contains `..`" });
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
        return Err(Error::BadPath { path: host.to_path_buf(), reason: "jail root" });
    }
    // Unreachable while `..` is refused and every pushed component is a
    // `Normal` one — kept because "the mirror is strictly below the jail root"
    // is the invariant the mount plan, `apply_mount` and the pivot all rest on,
    // and one comparison is a cheap way to keep it true if the loop above ever
    // grows a case.
    if !out.starts_with(jail) {
        return Err(Error::BadPath { path: host.to_path_buf(), reason: "escapes the jail" });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

Register `pub mod jail;` and re-export `jail_root` and `mirror_under` from `tinio-sandbox-linux/src/lib.rs`.

**Two manifest lines come with this task**, and they are the logging dependency rather than a convenience: `tracing.workspace = true` under `[dependencies]`, because the `debug!` above is what shared §7 asks every core operation for, and `tracing-subscriber.workspace = true` under `[dev-dependencies]`, because the test above asserts on one of those events — which is the only thing that makes "this operation is traced" a property rather than a habit (shared §7). Both are already pinned in the workspace's `[workspace.dependencies]`, so the workspace gains no new third-party dependency.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS.

---

### Task 2: The mount plan (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/plan.rs`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: `Exposure`, `AccessMode`, `jail_root`, `mirror_under`, `Mechanism`.
- Produces: `MountKind { JailTmpfs, Bind, OverlayReadOnly }`, `Mount { source, target, access, kind }`, `OverlayDirs { upper, work }`, `DEVICE_FILES`, `plan(sandbox: &Sandbox, jail: &Path, model: Mechanism, overlay: &OverlayDirs) -> Result<Vec<Mount>>`.

This is the pure composition of design §4's table. **What it no longer decides is which mechanism makes a read-only exposure read-only, and that is the change the design's 2026-09-19 re-measurement forced.** The rule this paragraph used to state — "a `ReadOnly` exposure under the user-namespace model needs an overlay, because an unprivileged user namespace cannot `remount,bind,ro` a bind of a host-owned superblock" — is **false**, and the design's §4 and its evidence table now carry the measurement that falsified it: an ext4 bind and a drvfs bind both take `MS_REMOUNT|MS_BIND|MS_RDONLY` under an unprivileged namespace, and a write through them then fails `EROFS`. What refuses the remount is the family a user namespace may mount itself — tmpfs, devtmpfs, procfs, sysfs, and a subdirectory of a tmpfs with them — so **the mechanism follows the filesystem and the privilege, not the model**. The design settles it, and this plan implements what the design settles: every `ReadOnly` exposure enters the table as [`MountKind::Bind`] with read-only access under either model, and Task 5's `apply_mount` is what turns it read-only — the remount first, and the `MS_RDONLY` overlay **as its fallback** where the kernel answers the remount with `EPERM`. A reader who knew the old rule should expect no model test here; this paragraph says so in full because the code will otherwise look wrong to exactly that reader.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tinio_sandbox_core::{AccessMode, Error, Sandbox};

    use super::*;

    const JAIL: &str = "/tmp/.tinio-jail-abc";

    fn overlay() -> OverlayDirs {
        OverlayDirs {
            upper: PathBuf::from("/tmp/.tinio-jail-abc/.overlay/upper"),
            work: PathBuf::from("/tmp/.tinio-jail-abc/.overlay/work"),
        }
    }

    #[test]
    fn the_jail_tmpfs_is_always_first() {
        let sandbox = Sandbox::new().expose("/srv/deep/data", AccessMode::ReadWrite);
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::Root, &overlay()).unwrap();
        assert_eq!(mounts[0].kind, MountKind::JailTmpfs);
        assert_eq!(mounts[0].target, PathBuf::from(JAIL));
    }

    #[test]
    fn read_write_exposure_is_a_plain_bind_in_both_models() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
        for model in [Mechanism::UserNamespace, Mechanism::Root] {
            let mounts = plan(&sandbox, Path::new(JAIL), model, &overlay()).unwrap();
            let data = mounts.iter().find(|m| m.source == Path::new("/srv/data")).unwrap();
            assert_eq!(data.kind, MountKind::Bind);
            assert_eq!(data.target, PathBuf::from(format!("{JAIL}/srv/data")));
        }
    }

    #[test]
    fn read_only_is_a_plain_bind_in_both_models() {
        // **The mechanism is the filesystem's to accept, not the plan's to
        // pick, so there is no model test left to make here** (design §4,
        // re-measured 2026-09-19): an ext4 or a drvfs bind remounts read-only
        // under *either* model, and the `MS_RDONLY` overlay is only what
        // `apply_mount` falls back to where the kernel refuses that remount —
        // the kernel-managed families. A plan that chose `OverlayReadOnly` on a
        // model test would be choosing by the wrong property and would be wrong
        // on exactly the filesystems a served root lives on, which is the
        // mistake this test exists to catch.
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
        for model in [Mechanism::UserNamespace, Mechanism::Root] {
            let mounts = plan(&sandbox, Path::new(JAIL), model, &overlay()).unwrap();
            let data = mounts.iter().find(|m| m.source == Path::new("/srv/data")).unwrap();
            assert_eq!(data.kind, MountKind::Bind, "{model:?}");
            assert!(data.access.is_read_only(), "{model:?}");
        }
    }

    #[test]
    fn writable_exposures_stay_plain_binds_even_with_a_read_only_root() {
        let sandbox = Sandbox::new()
            .expose("/srv/data", AccessMode::ReadOnly)
            .expose("/home/u/.tinio/roots/h", AccessMode::ReadWrite);
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::UserNamespace, &overlay()).unwrap();
        let state = mounts
            .iter()
            .find(|m| m.source == Path::new("/home/u/.tinio/roots/h"))
            .unwrap();
        assert_eq!(state.kind, MountKind::Bind, "a writable exposure takes no read-only mechanism");
        assert!(!state.access.is_read_only());
    }

    #[test]
    fn device_files_come_last_and_are_binds() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite).device_files();
        let mounts = plan(&sandbox, Path::new(JAIL), Mechanism::Root, &overlay()).unwrap();
        let tail = &mounts[mounts.len() - DEVICE_FILES.len()..];
        let names: Vec<&Path> = tail.iter().map(|mount| mount.source.as_path()).collect();
        assert_eq!(names, DEVICE_FILES.map(Path::new), "the four device files, in order");
        for mount in tail {
            assert_eq!(mount.kind, MountKind::Bind, "{mount:?}");
            assert_eq!(
                mount.target,
                PathBuf::from(format!("{JAIL}{}", mount.source.display())),
                "a device file is mirrored at its own absolute path"
            );
            assert!(!mount.access.is_read_only(), "a device file is bound read-write");
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
        let err = plan(&sandbox, Path::new(JAIL), Mechanism::UserNamespace, &overlay()).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
        assert!(err.to_string().contains("read-only exposure"), "{err}");

        // The same two exposures are fine under root, where the remount is not
        // refused and the pair is therefore never reached.
        assert!(plan(&sandbox, Path::new(JAIL), Mechanism::Root, &overlay()).is_ok());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux plan::`
Expected: FAIL — `cannot find function 'plan'`.

- [ ] **Step 3: Write the implementation**

```rust
//! The ordered list of mounts that turns a [`Sandbox`] into a jail.

use std::path::{Path, PathBuf};

use tinio_sandbox_core::{AccessMode, Result, Sandbox};

use crate::jail::mirror_under;
use crate::mechanism::Mechanism;

/// The four device files bound into every jail that asks for them.
pub const DEVICE_FILES: [&str; 4] = ["/dev/null", "/dev/zero", "/dev/random", "/dev/urandom"];

/// One mount to perform, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mount {
    /// What is mounted. For [`MountKind::JailTmpfs`] this is the literal
    /// `"tmpfs"` — there is no source path.
    pub source: PathBuf,
    /// Where it lands: always inside the jail, at the exposed path's own
    /// absolute path.
    pub target: PathBuf,
    /// The access the confined process gets through this mount.
    pub access: AccessMode,
    /// How to perform it.
    pub kind: MountKind,
}

/// How a [`Mount`] is performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountKind {
    /// A tmpfs on the jail root; always first, so every later target has a
    /// parent directory.
    JailTmpfs,
    /// `mount --bind <source> <target>`. A read-only access is enforced *after*
    /// the bind, by the remount `apply_mount` performs and by the overlay it
    /// substitutes where the kernel refuses that remount — **the mechanism is
    /// the filesystem's to accept, not this plan's to choose**, which is why
    /// every read-only exposure is this kind in both models.
    Bind,
    /// An overlay whose lower is the source, mounted `MS_RDONLY` from the
    /// start. **The fallback, not the default**: `apply_mount` substitutes it
    /// for a read-only bind whose remount the kernel refuses with `EPERM`,
    /// which is the kernel-managed families' answer — tmpfs, devtmpfs, procfs,
    /// sysfs, and a subdirectory of a tmpfs with them (design §4, re-measured
    /// 2026-09-19). [`plan`] therefore never emits it: whether the remount is
    /// accepted is a property of the served root's filesystem, and nothing this
    /// side of the syscall can know it.
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverlayDirs {
    /// The overlay's upper layer.
    pub upper: PathBuf,
    /// The overlay's work directory.
    pub work: PathBuf,
}

/// Compose the jail's mount table.
///
/// # Errors
///
/// [`tinio_sandbox_core::Error::BadPath`] for an exposure [`mirror_under`]
/// rejects, and [`tinio_sandbox_core::Error::Unsupported`] when the
/// user-namespace model is asked for more than one read-only exposure — the
/// overlay pair is created once, an overlay upper is bound to a single lower,
/// and the pair is what a refused remount falls back to, so the second
/// read-only exposure is refused by name rather than left to race the first as
/// an unreadable overlayfs error.
///
/// # Examples
///
/// ```
/// use std::path::Path;
///
/// use tinio_sandbox_core::{AccessMode, Sandbox};
/// use tinio_sandbox_linux::{Mechanism, OverlayDirs, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let overlay = OverlayDirs { upper: "/tmp/j/.up".into(), work: "/tmp/j/.work".into() };
/// let mounts = plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &overlay).unwrap();
/// assert_eq!(mounts.len(), 2, "the jail tmpfs plus the one exposure");
/// ```
pub fn plan(
    sandbox: &Sandbox,
    jail: &Path,
    model: Mechanism,
    _overlay: &OverlayDirs,
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
        return Err(tinio_sandbox_core::Error::Unsupported {
            detail: "the user-namespace model supports at most one read-only exposure: the \
                     fallback overlay is created once, and every read-only mount a refused \
                     remount falls back to would share its upper directory, which an overlay \
                     binds to a single lower",
        });
    }

    for exposure in sandbox.exposures() {
        let target = mirror_under(jail, &exposure.host)?;
        // **Every exposure is a plain bind, read-only ones included.** Which
        // mechanism makes a read-only bind read-only — a remount, or the
        // `MS_RDONLY` overlay where the kernel refuses it — is decided by the
        // *filesystem* when `apply_mount` attempts it (design §4, re-measured
        // 2026-09-19), and this function performs no syscall: there is nothing
        // here that could know which one a given served root will take. The
        // `access` field is what carries the read-only intent forward.
        mounts.push(Mount {
            source: exposure.host.clone(),
            target,
            access: exposure.access,
            kind: MountKind::Bind,
        });
    }

    if sandbox.device_files_enabled() {
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
    // ... as written in Step 1
}
```

Register `pub mod plan;` and re-export `Mount`, `MountKind`, `OverlayDirs`, `plan`, `DEVICE_FILES`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS.

---

### Task 3: The model and the capability policy (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/{mechanism.rs,caps.rs}`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: `AccessMode`, `Error`.
- Produces: `Mechanism::from_env`, `Mechanism::resolved(euid, userns) -> Resolved`, `userns_available() -> Result<bool>`, **the crate's one `unshare(flags, step_name) -> Result<()>` wrapper** — `pub(crate)`, because Task 5's `enter` imports it and a private item cannot be imported — the second of its two `unsafe` sites, declared here because the probe is its first caller — `Resolved { UserNamespace, Root, Unavailable(&'static str) }`; `Cap { Chown }`, `retained(mode: AccessMode, local_uid_configured: bool) -> &'static [Cap]`. The `Mechanism` enum itself is P0's, declared complete in this crate's `lib.rs` and **re-exported from `mechanism.rs`**, which is the path every other module in the crate imports it from.

`resolved` is the adaptive rule of design decision 7's decision table, pure and tested with an injected euid and an injected probe answer, so every branch is unit-testable without privileges. It takes the probe's answer as a parameter rather than calling it, which is what keeps the table exhaustively testable **and** is why the probe lives beside it as `userns_available()`: the parent cannot attempt an `unshare` and then fall back, because the call changes the calling process for good, so the question is asked by forking a child that is discarded. **The fork is bounded at both ends** — the read and the reap share one deadline — because the design's own evidence table records what an unbounded reap costs an operator: a probe that hangs is a startup that never fails (`userns_available` carries the row and the reasoning).

- [ ] **Step 1: Write the failing tests**

`mechanism.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_selects_the_model() {
        assert_eq!(Mechanism::from_env(Some("userns")).unwrap(), Mechanism::UserNamespace);
        assert_eq!(Mechanism::from_env(Some("root")).unwrap(), Mechanism::Root);
        assert_eq!(Mechanism::from_env(None).unwrap(), Mechanism::Auto);
        assert_eq!(Mechanism::from_env(Some("")).unwrap(), Mechanism::Auto);
        assert!(Mechanism::from_env(Some("bogus")).is_err(), "a typo must not pick a model");
    }

    #[test]
    fn auto_prefers_the_user_namespace_when_the_probe_grants_it() {
        assert_eq!(Mechanism::Auto.resolved(1000, true), Resolved::UserNamespace);
        assert_eq!(Mechanism::Auto.resolved(0, true), Resolved::UserNamespace);
    }

    #[test]
    fn auto_falls_back_to_root_when_the_probe_fails_and_we_are_root() {
        // Design decision 7's second tier: a hardened host that forbids
        // unprivileged user namespaces still works when started as root.
        assert_eq!(Mechanism::Auto.resolved(0, false), Resolved::Root);
    }

    #[test]
    fn auto_fails_closed_when_neither_tier_is_available() {
        let Resolved::Unavailable(detail) = Mechanism::Auto.resolved(1000, false) else {
            panic!("expected Unavailable");
        };
        assert!(detail.contains("sudo"), "{detail}");
        assert!(detail.contains("unprivileged user namespace"), "{detail}");
    }

    #[test]
    fn forcing_the_user_namespace_never_falls_back_to_root() {
        // Even as root: forcing a model means that model or nothing, so a
        // host with no user namespaces gets a refusal rather than a silent
        // root confinement the operator did not ask for.
        assert_eq!(Mechanism::UserNamespace.resolved(0, true), Resolved::UserNamespace);
        assert!(matches!(
            Mechanism::UserNamespace.resolved(0, false),
            Resolved::Unavailable(_)
        ));
    }

    #[test]
    fn a_forced_root_model_is_refused_when_not_root() {
        assert_eq!(Mechanism::Root.resolved(0, false), Resolved::Root);
        assert!(matches!(Mechanism::Root.resolved(1000, false), Resolved::Unavailable(_)));
    }

    #[test]
    fn the_unavailable_reason_names_a_remedy() {
        let Resolved::Unavailable(detail) = Mechanism::Root.resolved(1000, false) else {
            panic!("expected Unavailable");
        };
        assert!(detail.contains("sudo") || detail.contains("root"), "{detail}");
        assert!(detail.contains("userns"), "{detail}");
    }

    #[test]
    fn the_probe_answers_without_privileges_and_leaves_us_unshared() {
        // The probe forks, so the parent is never inside a namespace whatever
        // the answer is. This is the property the two-tier fallback rests on:
        // if the probe leaked its unshare into the parent, `Auto` could never
        // back out of a failed attempt.
        let before = rustix::process::geteuid();
        let _ = userns_available().unwrap();
        assert_eq!(rustix::process::geteuid(), before, "the probe must not confine us");
    }
}
```

`caps.rs`:

```rust
#[cfg(test)]
mod tests {
    use tinio_sandbox_core::AccessMode;

    use super::*;

    #[test]
    fn read_only_retains_nothing() {
        assert!(retained(AccessMode::ReadOnly, false).is_empty());
        assert!(retained(AccessMode::ReadOnly, true).is_empty());
    }

    #[test]
    fn read_write_retains_chown_only_when_a_uid_mapping_exists() {
        assert!(retained(AccessMode::ReadWrite, false).is_empty());
        assert_eq!(retained(AccessMode::ReadWrite, true), &[Cap::Chown]);
    }

    #[test]
    fn every_capability_has_a_kernel_name() {
        assert_eq!(Cap::Chown.name(), "CAP_CHOWN");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

**Run: `cargo test -p tinio-sandbox-linux mechanism::` then `cargo test -p tinio-sandbox-linux caps::`**
Expected: FAIL — `Resolved` and `Cap` do not exist.

**Two commands rather than one filter with a `\|` in it.** Cargo's test filter is a *substring* of the test's path, not a regex: `'mechanism::\|caps::'` is a literal no test name contains, so the run would match nothing and exit 0 — a red test that reports green, which is the one failure mode a red step exists to prevent. Either half alone is a correct substring.

- [ ] **Step 3: Write `mechanism.rs`**

```rust
//! Which confinement mechanism to use, and whether it is available.
//!
//! The `Mechanism` enum itself lives in this crate's `lib.rs`, where P0 declares it
//! complete (all three variants) so this task's tests and Task 2's can name
//! them without a compile-order dependency. **It is re-exported from here** —
//! `pub use crate::Mechanism;` below — because that is the path the rest of the
//! crate imports it from (`crate::mechanism::Mechanism` in `plan.rs`, `options.rs`,
//! `enter.rs` and Task 5's test module), and an `impl Mechanism` block in a module
//! that does not name the type does not compile. What is added here is the
//! behaviour: `Resolved`, `Mechanism::from_env`, `Mechanism::resolved` and the
//! availability probe that makes `Auto` mean something.

use std::io;
use std::time::{Duration, Instant};

pub use crate::Mechanism;

use tinio_sandbox_core::{Error, Result};
use tracing::{debug, error};

/// Build this crate's syscall error, **log it where it is built**, and hand it
/// back.
///
/// The event and the `Error::Step` carry the same `step` string by
/// construction rather than by review, which is what shared §7 asks for; the
/// sink is the caller's, because this crate installs no subscriber.
fn fail(step: &'static str, source: impl Into<io::Error>) -> Error {
    let source = source.into();
    let error = Error::Step { step, source };
    error!(step, error = %error, "sandbox step failed");
    error
}

/// The `.map_err` shape of [`fail`], for the call sites that read better as a
/// closure.
///
/// Generic over the error type because the callers are not all `std`: `rustix`
/// returns [`rustix::io::Errno`], which converts into `std::io::Error` but is
/// not it. `enter.rs` carries the same pair — a copy rather than an import,
/// because `enter.rs` does not exist until Task 5 and this task's tests have to
/// compile on their own.
fn step<Source: Into<io::Error>>(step_name: &'static str) -> impl Fn(Source) -> Error {
    move |source: Source| fail(step_name, source)
}

/// What [`Mechanism::resolved`] decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// Confine with an unprivileged user namespace.
    UserNamespace,
    /// Confine with real root's `CAP_SYS_ADMIN`.
    Root,
    /// Neither is available; startup must fail.
    Unavailable(&'static str),
}

impl Mechanism {
    /// Read `TINIO_SANDBOX_MODEL`. Absent or empty means [`Mechanism::Auto`].
    ///
    /// # Errors
    ///
    /// [`Error::Unsupported`] for any other value, so a typo cannot silently
    /// select a different mechanism.
    pub fn from_env(raw: Option<&str>) -> Result<Self> {
        match raw {
            None | Some("") => Ok(Self::Auto),
            Some("userns") => Ok(Self::UserNamespace),
            Some("root") => Ok(Self::Root),
            Some(wrong) => {
                // Logged where the `Error` is built (shared §7): this refusal
                // is otherwise silent, because the caller's `?` only prints it
                // on the way out, and the value that was wrong is the whole
                // diagnosis.
                error!(step = "TINIO_SANDBOX_MODEL", value = wrong, "unsupported model");
                Err(Error::Unsupported {
                    detail: "TINIO_SANDBOX_MODEL must be `userns` or `root`",
                })
            }
        }
    }

    /// Resolve the request against the probe's answer and the effective uid.
    ///
    /// The user namespace is preferred because it needs no privilege at all:
    /// the process maps itself to root inside a namespace it owns, so a bug in
    /// the jailed server cannot reach anything the invoking user could not.
    /// Root is the fallback for hosts that forbid unprivileged user
    /// namespaces.
    ///
    /// `userns` comes from [`userns_available`] rather than from a sysctl,
    /// because the syscall is the authority: a container can report that
    /// unprivileged user namespaces are enabled and still refuse the call. The
    /// probe is a separate function so this decision table stays pure and
    /// exhaustively unit-testable (design decision 7).
    pub fn resolved(self, euid: u32, userns: bool) -> Resolved {
        match self {
            Self::UserNamespace if userns => Resolved::UserNamespace,
            Self::UserNamespace => Resolved::Unavailable(
                "TINIO_SANDBOX_MODEL=userns was forced but this kernel refused the user \
                 namespace: on Ubuntu 24.04+ set kernel.apparmor_restrict_unprivileged_userns=0 \
                 or give this binary an AppArmor profile; on Debian-family kernels check \
                 kernel.unprivileged_userns_clone; or run as root; or unset TINIO_SANDBOX_MODEL",
            ),
            Self::Auto if userns => Resolved::UserNamespace,
            Self::Auto | Self::Root if euid == 0 => Resolved::Root,
            Self::Root => Resolved::Unavailable(
                "the root model was forced but the process is not root: run with sudo, \
                 or use TINIO_SANDBOX_MODEL=userns",
            ),
            Self::Auto => Resolved::Unavailable(
                "neither confinement model is available: this kernel refused an unprivileged \
                 user namespace and the process is not root — allow unprivileged user \
                 namespaces (Ubuntu 24.04+ blocks them: kernel.apparmor_restrict_unprivileged_userns=0 \
                 or an AppArmor profile for this binary; Debian-family kernels: \
                 kernel.unprivileged_userns_clone) or run with sudo",
            ),
        }
    }
}

/// How long [`userns_available`] waits for its forked child, at each end of the
/// handshake — the read and the reap share the one deadline.
///
/// Generous on purpose rather than tight: the child's whole job is one syscall,
/// one byte and an exit, so a healthy run answers in microseconds and anything
/// this far out is a child that is gone or stuck rather than one that is slow.
/// The value is a bound on a hang, not a performance budget.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether this kernel grants an unprivileged `unshare(CLONE_NEWUSER)`.
///
/// **Answered by forking, not by reading a sysctl.** `unshare` changes the
/// calling process for good, so the parent cannot attempt it and then fall back
/// — but a child can attempt it and report. The child unshares, writes one byte
/// to a pipe and exits; the parent reads the pipe and is itself untouched. A
/// sysctl would be cheaper and is the wrong instrument: containers routinely
/// report unprivileged user namespaces as enabled while the seccomp or AppArmor
/// policy refuses the call, and the design's rule is that the syscall is the
/// authority (design decision 7).
///
/// # Errors
///
/// [`Error::Step`] when the fork or the pipe itself fails, **or when either end
/// of the handshake runs past [`PROBE_TIMEOUT`]** — distinct from a probe that
/// ran and answered "no", which is `Ok(false)` and an ordinary outcome.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::userns_available;
///
/// // The answer is a property of the host, not of the call: both are legal.
/// let available = userns_available().unwrap();
/// let _ = available;
/// ```
pub fn userns_available() -> Result<bool> {
    use std::fs::File;
    use std::io::{Read as _, Write as _};
    use std::os::fd::OwnedFd;

    use rustix::fs::{OFlags, fcntl_setfl};
    use rustix::pipe::pipe;
    use rustix::process::{WaitOptions, waitpid};
    use rustix::runtime::{Fork, exit_group, kernel_fork};
    use rustix::thread::UnshareFlags;

    // A child that reaches the unshare and a parent that never does: the answer
    // travels as one byte, and the child always exits without running anything
    // else of ours.
    let (reader, writer): (OwnedFd, OwnedFd) = pipe().map_err(step("userns probe pipe"))?;
    // **`kernel_fork`, and it is the crate's third `unsafe` site.** `rustix` has
    // no safe `fork`: `rustix::process` exports none at all, and the one it does
    // ship is `rustix::runtime::kernel_fork`, which is `unsafe fn` because a
    // fork in a multi-threaded process is not something a signature can promise.
    // The invariant here is the design's own ordering rather than this call's
    // doing: the probe runs before any thread exists (design §10, decision 26),
    // so the child is a copy of a single-threaded process and touches nothing
    // but its own pipe end, the `unshare` wrapper's one event and
    // `exit_group` — and an event writes nothing at all until a subscriber
    // exists, which in that child none does. It also needs rustix's `runtime` feature,
    // on top of the `pipe` one `pipe()` above already uses — both are in the
    // crate's manifest, and both are stated in this plan's tech stack.
    let fork = unsafe { kernel_fork() }.map_err(step("userns probe fork"))?;
    match fork {
        Fork::Child(_) => {
            drop(reader);
            // Through the crate's one wrapper, not a second `unsafe` block: it
            // is the site that carries the safety argument, and this call is
            // the easy half of it — `NEWUSER` alone, in a child that is about
            // to exit.
            let granted = unshare(UnshareFlags::NEWUSER, "userns probe").is_ok();
            let mut writer = File::from(writer);
            // A failed write means the parent is gone; exiting is the whole job.
            let _ = writer.write_all(&[u8::from(granted)]);
            // **`exit_group`, because `rustix::process` has no `exit`**: the
            // exit syscall is `rustix::runtime::exit_group`, which terminates
            // the child's whole thread group — a distinction that costs nothing
            // here, because the child has exactly one thread, and the same
            // ordering argument (design §10) is what says so.
            exit_group(0);
        }
        Fork::ParentOf(child) => {
            drop(writer);
            // **Both halves of this handshake are bounded, and the reason is the
            // design's own evidence table**: "An unbounded `waitpid` after a
            // fallback `kill` is a harness hazard … A supervisor that reaps a
            // child it may not be able to signal must bound that wait." The
            // probe that row describes is bounded at both ends — an `alarm` in
            // the child and a `NOHANG` loop in the parent — and this one can
            // only have the second: `rustix` ships no `alarm` (nothing in it
            // sets a timer), and a hand-rolled FFI declaration for one would be
            // a fourth `unsafe` site, which this crate's ledger does not have.
            // So the parent carries both, and the read end is made
            // non-blocking — which is what lets the read share the reap's
            // deadline instead of blocking past it.
            let mut reader = File::from(reader);
            fcntl_setfl(&reader, OFlags::NONBLOCK).map_err(step("userns probe nonblocking"))?;
            let deadline = Instant::now() + PROBE_TIMEOUT;
            let mut answer = [0u8; 1];
            let available = loop {
                match reader.read(&mut answer) {
                    // EOF: the child died without answering. A failed probe,
                    // not a "no" — `Ok(false)` is a child that ran and reported
                    // a refusal, which is this probe's ordinary negative.
                    Ok(0) => {
                        return Err(fail(
                            "userns probe read",
                            io::Error::from(io::ErrorKind::UnexpectedEof),
                        ));
                    }
                    Ok(_) => break answer[0] != 0,
                    // `Interrupted` is retried for the same reason `read_exact`
                    // used to retry it internally — a resting pipe is not a
                    // reason to call a probe failed — and `WouldBlock` is what
                    // the non-blocking end reports between the child's write and
                    // the deadline.
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        // **Expiry is a refusal, never a false `Ok`.** A probe
                        // that answered "unavailable" on a timeout would send
                        // `Auto` to the root model on a host that never refused
                        // anything — a silent downgrade produced by a hang
                        // (design §11's fail-closed, applied to the question the
                        // model table is built on).
                        if Instant::now() >= deadline {
                            return Err(fail(
                                "userns probe read",
                                io::Error::from(io::ErrorKind::TimedOut),
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(source) => return Err(fail("userns probe read", source)),
                }
            };
            // The reap, on the same deadline and refusing the same way. The
            // child has written and exited before the read returned on every
            // run that answers, so a `NOHANG` miss that persists to the
            // deadline is a child that is gone or stuck rather than one that is
            // slow. (`WaitOptions::NOHANG` is the kernel's `WNOHANG`; rustix
            // spells the constant without the leading `W`.)
            loop {
                match waitpid(Some(child), WaitOptions::NOHANG) {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Ok(None) => {
                        return Err(fail(
                            "userns probe reap",
                            io::Error::from(io::ErrorKind::TimedOut),
                        ));
                    }
                    Err(source) => return Err(fail("userns probe reap", source)),
                }
            }
            debug!(step = "userns probe", available, "unprivileged user namespace probed");
            Ok(available)
        }
    }
}

/// Enter one namespace, and say which one failed when it does.
///
/// **This is the crate's one `unshare` wrapper and one of its three `unsafe`
/// sites**, and it lives here rather than beside `confine` because the probe
/// above is its first caller: two wrappers around one syscall would be two
/// safety arguments to keep in step, and the second could only ever be an
/// accident. **It is `pub(crate)` for the same reason** — Task 5's `enter`
/// imports it rather than writing a second body, and a private item cannot be
/// imported at all.
///
/// The two unshares of the confinement go through this one function because
/// they are one decision — `NEWUSER` and then `NEWNS`, with the map writes
/// between them — and splitting it in half would quietly make the `unsafe`
/// surface three.
///
/// # Safety
///
/// rustix marks `unshare_unsafe` unsafe because `CLONE_FILES` would break a
/// multi-threaded process. This crate never passes it: the confinement calls
/// this twice with `NEWUSER` and `NEWNS`, and the probe passes `NEWUSER` in a
/// forked child that does nothing else and exits. The entry point guarantees no
/// thread exists before either of them (design §10).
#[allow(unsafe_code)]
pub(crate) fn unshare(flags: UnshareFlags, step_name: &'static str) -> Result<()> {
    // SAFETY: the flags are the caller's, and every caller in this crate passes
    // a namespace flag — never `CLONE_FILES` (see above).
    unsafe { unshare_unsafe(flags) }
        .map_err(step(step_name))
        // One event per `unshare`, named by the caller's own step string, so
        // the log says which namespace was entered and a failure and its
        // success are the same operation (shared §7).
        .inspect(|_| debug!(step = step_name, flags = ?flags, "namespace unshared"))
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

- [ ] **Step 4: Write `caps.rs`**

```rust
//! Which capabilities the confined process keeps.

use rustix::thread::CapabilitySet;
use tinio_sandbox_core::AccessMode;

/// A capability the confinement may retain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cap {
    /// `CAP_CHOWN` — needed only by FR-033's `local_uid` chown on newly
    /// written objects.
    Chown,
}

impl Cap {
    /// The kernel's name for it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Chown => "CAP_CHOWN",
        }
    }

    /// The `rustix` bit for it: a single-bit [`CapabilitySet`].
    ///
    /// **Not `rustix::thread::Capability`**, which is what the obvious spelling
    /// would be. The type carries `#[deprecated(since = "1.1.0", note = "Use
    /// CapabilitySet with a single bit instead")]`, and `rustix::thread` *does*
    /// re-export it — the module is `mod prctl;` plus `pub use prctl::*;`, so it
    /// is the deep `rustix::thread::prctl::Capability` path that fails to
    /// resolve and the short one that compiles. Naming it therefore buys a
    /// deprecation warning under `-D warnings`, which is the workspace's gate,
    /// for nothing: `remove_capability_from_bounding_set` takes `impl
    /// CompatCapability`, which `CapabilitySet` implements, so the whole policy
    /// is expressible without it.
    pub const fn set(self) -> CapabilitySet {
        match self {
            Self::Chown => CapabilitySet::CHOWN,
        }
    }
}

/// The capabilities to keep for this mode.
///
/// Everything else goes — including `CAP_SYS_ADMIN`, which the mount table
/// needed while it was being built and nothing needs afterwards. Read-only
/// mode needs no capability at all: it never writes, so it never chowns.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::AccessMode;
/// use tinio_sandbox_linux::retained;
///
/// assert!(retained(AccessMode::ReadOnly, true).is_empty());
/// assert_eq!(retained(AccessMode::ReadWrite, true).len(), 1);
/// ```
pub const fn retained(mode: AccessMode, local_uid_configured: bool) -> &'static [Cap] {
    match (mode, local_uid_configured) {
        (AccessMode::ReadOnly, _) => &[],
        (AccessMode::ReadWrite, false) => &[],
        (AccessMode::ReadWrite, true) => &[Cap::Chown],
    }
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

Register `pub mod caps;` and `pub mod mechanism;`, and add to the crate's re-exports: `pub use self::mechanism::{Resolved, userns_available};`. **`Mechanism` is deliberately not in that list** — P0 declares `pub enum Mechanism` in `lib.rs`, so it is already public there, and `use`-ing it back in beside its own definition is `E0255` ("must be defined only once in the type namespace of this module"). What `mechanism.rs`'s own `pub use crate::Mechanism` buys is the *other* direction: the crate-internal path `crate::mechanism::Mechanism` that `plan.rs`, `options.rs` and `enter.rs` import, which is where the earlier spelling of this plan had the type out of scope.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS.

---

### Task 4: The seccomp outbound filter (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/seccomp.rs`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: `Error`, `Result`.
- Produces: `SockFilter`, `BPF_*` / `SECCOMP_*` constants, `Arch { X86_64, Aarch64 }` (`current`, `audit_arch`, `syscall_nr`), `Syscall { Connect, SendTo, SendMsg, SendMmsg }`, `filter_program(arch) -> Vec<SockFilter>`, `install() -> Result<Seccomp>`.

The denial shape is design decision 13: deny the *outbound* syscalls and leave accepting and answering untouched, because `CLONE_NEWNET` would cut the host off from the server. `connect` is how an outbound TCP connection starts; `sendto`, `sendmsg` and `sendmmsg` are how a process would exfiltrate over UDP without ever calling `connect` — `sendmmsg` is the batch form of `sendmsg` and an equally direct route out, so leaving it out would be a hole in the same wall.

**What stays available is `write`/`writev`, and that is enough** — but the distinction is sharper than "a server answers with `write`/`send`": glibc's `send(2)` *is* the `sendto` syscall, so `send()` is denied along with `sendto`/`sendmsg`/`sendmmsg`. Rust and tokio answer with `write`/`writev`, which is why the tinio server is unaffected; a C dependency that answers with `send()` would not be.

Two boundaries this filter cannot close, recorded in the design's Risks rather than papered over: `io_uring`'s `IORING_OP_CONNECT`/`IORING_OP_SENDMSG` never reach a filtered syscall, and the filter sees no address family — so `connect(AF_UNIX)` is denied too (design decision 13).

**A third gap is the x32 ABI, and it is this plan's to record rather than the design's existing entry.** The program compares `seccomp_data.arch` and nothing else, and on `x86_64` the x32 ABI reports the *same* `AUDIT_ARCH_X86_64` — what distinguishes it is `__X32_SYSCALL_BIT` (`0x4000_0000`) OR'd into the syscall number. `connect` through that ABI is not number 42, so it meets the tail and is allowed. Closing it means a second comparison against `nr & !__X32_SYSCALL_BIT` (or a mask test before the four `JEQ`s), which is a change to the filter's instruction count rather than a comment; what makes it unelevated here is that x32 is a Linux 3.x-era ABI most kernels do not enable (`CONFIG_X86_X32`) — and it is still a real coverage gap, so it goes beside the other two in the design's Risks as this phase lands, and it is why the denial list below says what it denies rather than claiming the network is closed.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_program_has_thirteen_instructions() {
        assert_eq!(filter_program(Arch::X86_64).len(), 13);
        assert_eq!(filter_program(Arch::Aarch64).len(), 13);
    }

    #[test]
    fn the_four_denials_return_eperm() {
        let program = filter_program(Arch::X86_64);
        for index in [5, 7, 9, 11] {
            assert_eq!(program[index].code, BPF_RET | BPF_K);
            assert_eq!(program[index].k, SECCOMP_RET_ERRNO | EPERM);
        }
    }

    #[test]
    fn the_tail_allows_everything_else() {
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[12].code, BPF_RET | BPF_K);
        assert_eq!(program[12].k, SECCOMP_RET_ALLOW);
    }

    #[test]
    fn the_arch_guard_kills_a_mismatched_process() {
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[1].k, Arch::X86_64.audit_arch());
        assert_eq!(program[1].jt, 1, "a matching arch skips the kill");
        assert_eq!(program[1].jf, 0, "a mismatched arch falls into it");
        assert_eq!(program[2].k, SECCOMP_RET_KILL_PROCESS);
    }

    #[test]
    fn the_syscall_number_is_loaded_first_and_the_arch_second() {
        // `seccomp_data` is `{ int nr; __u32 arch; ... }`, so the offsets are
        // fixed and getting them backwards would filter the wrong words.
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[0].k, 4, "arch is the second u32");
        assert_eq!(program[0].code, BPF_LD | BPF_W | BPF_ABS);
        assert_eq!(program[3].k, 0, "nr is the first u32");
    }

    #[test]
    fn jump_offsets_land_on_each_denial_or_skip_it() {
        let program = filter_program(Arch::X86_64);
        for index in [4, 6, 8, 10] {
            assert_eq!(program[index].code, BPF_JMP | BPF_JEQ | BPF_K);
            assert_eq!(program[index].jt, 0, "equal falls through to the denial");
            assert_eq!(program[index].jf, 1, "unequal skips it");
        }
        assert_eq!(program[4].k, Arch::X86_64.syscall_nr(Syscall::Connect));
        assert_eq!(program[6].k, Arch::X86_64.syscall_nr(Syscall::SendTo));
        assert_eq!(program[8].k, Arch::X86_64.syscall_nr(Syscall::SendMsg));
        assert_eq!(program[10].k, Arch::X86_64.syscall_nr(Syscall::SendMmsg));
    }

    #[test]
    fn syscall_numbers_are_per_architecture() {
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::Connect), 42);
        assert_eq!(Arch::Aarch64.syscall_nr(Syscall::Connect), 203);
        assert_eq!(Arch::X86_64.syscall_nr(Syscall::SendMmsg), 307);
        assert_eq!(Arch::Aarch64.syscall_nr(Syscall::SendMmsg), 269);
        assert_ne!(
            Arch::X86_64.syscall_nr(Syscall::SendMsg),
            Arch::Aarch64.syscall_nr(Syscall::SendMsg)
        );
    }

    #[test]
    fn the_running_architecture_is_known_on_the_supported_targets() {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(Arch::current().is_some());
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux seccomp::`
Expected: FAIL — `cannot find function 'filter_program'`.

- [ ] **Step 3: Write the implementation**

```rust
//! The seccomp filter that denies outbound connection setup.
//!
//! The filter is installed exactly once, after every mount and after
//! `PR_SET_NO_NEW_PRIVS`, and is irreversible (design decisions 13, 26).

use tinio_sandbox_core::{Error, Result};
use tracing::{debug, error, warn};

/// `struct sock_filter` — one BPF instruction.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SockFilter {
    /// Operation code.
    pub code: u16,
    /// Jump-if-true offset, relative to the next instruction.
    pub jt: u8,
    /// Jump-if-false offset, relative to the next instruction.
    pub jf: u8,
    /// The constant operand.
    pub k: u32,
}

/// `struct sock_fprog`.
#[repr(C)]
struct SockFprog {
    len: u16,
    filter: *const SockFilter,
}

/// `BPF_LD`
pub const BPF_LD: u16 = 0x00;
/// `BPF_W`
pub const BPF_W: u16 = 0x00;
/// `BPF_ABS`
pub const BPF_ABS: u16 = 0x20;
/// `BPF_JMP`
pub const BPF_JMP: u16 = 0x05;
/// `BPF_JEQ`
pub const BPF_JEQ: u16 = 0x10;
/// `BPF_K`
pub const BPF_K: u16 = 0x00;
/// `BPF_RET`
pub const BPF_RET: u16 = 0x06;
/// `SECCOMP_RET_ALLOW`
pub const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
/// `SECCOMP_RET_ERRNO`
pub const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
/// `SECCOMP_RET_KILL_PROCESS`
pub const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
/// `EPERM`
pub const EPERM: u32 = 1;

/// `SECCOMP_MODE_FILTER`
const SECCOMP_MODE_FILTER: usize = 2;
/// `PR_SET_SECCOMP`
const PR_SET_SECCOMP: i32 = 22;

/// The architecture the filter is compiled for.
///
/// A process of a different architecture has different syscall numbers, so a
/// mismatch is fatal rather than filtered — the filter cannot safely interpret
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Arch {
    /// `x86_64`
    X86_64,
    /// `aarch64`
    Aarch64,
}

/// The syscalls the filter denies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Syscall {
    /// `connect` — the only way to start an outbound TCP connection.
    Connect,
    /// `sendto` — datagram exfiltration without `connect`.
    SendTo,
    /// `sendmsg` — the same, via an iovec.
    SendMsg,
    /// `sendmmsg` — the same, batched. Denied for the same reason as
    /// [`Syscall::SendMsg`]: it is a separate syscall with its own number
    /// (307 on `x86_64`, 269 on `aarch64`) and reaches the network without
    /// ever calling `connect`.
    SendMmsg,
}

impl Arch {
    /// The architecture of the running process, when the filter supports it.
    pub const fn current() -> Option<Self> {
        #[cfg(target_arch = "x86_64")]
        {
            Some(Self::X86_64)
        }
        #[cfg(target_arch = "aarch64")]
        {
            Some(Self::Aarch64)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            None
        }
    }

    /// The `AUDIT_ARCH_*` value `seccomp_data::arch` carries.
    pub const fn audit_arch(self) -> u32 {
        match self {
            Self::X86_64 => 0xc000_003e,
            Self::Aarch64 => 0xc000_00b7,
        }
    }

    /// The syscall number for this architecture.
    pub const fn syscall_nr(self, syscall: Syscall) -> u32 {
        match (self, syscall) {
            (Self::X86_64, Syscall::Connect) => 42,
            (Self::X86_64, Syscall::SendTo) => 44,
            (Self::X86_64, Syscall::SendMsg) => 46,
            (Self::X86_64, Syscall::SendMmsg) => 307,
            (Self::Aarch64, Syscall::Connect) => 203,
            (Self::Aarch64, Syscall::SendTo) => 206,
            (Self::Aarch64, Syscall::SendMsg) => 211,
            (Self::Aarch64, Syscall::SendMmsg) => 269,
        }
    }
}

/// Build the filter program. Pure — the whole denial policy is testable
/// without installing anything.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::{Arch, filter_program};
///
/// assert_eq!(filter_program(Arch::X86_64).len(), 13);
/// ```
pub fn filter_program(arch: Arch) -> Vec<SockFilter> {
    let load = |offset: u32| SockFilter { code: BPF_LD | BPF_W | BPF_ABS, jt: 0, jf: 0, k: offset };
    let jump = |nr: u32| SockFilter {
        code: BPF_JMP | BPF_JEQ | BPF_K,
        jt: 0,
        jf: 1,
        k: nr,
    };
    let deny = SockFilter { code: BPF_RET | BPF_K, jt: 0, jf: 0, k: SECCOMP_RET_ERRNO | EPERM };
    let ret = |k: u32| SockFilter { code: BPF_RET | BPF_K, jt: 0, jf: 0, k };

    // **What this program cannot see is the x32 ABI, and it is a real gap.**
    // The arch guard compares `seccomp_data.arch` and nothing else, and on
    // `x86_64` an x32 process reports the same `AUDIT_ARCH_X86_64` as every
    // other one: the ABI lives in the *syscall number* (`__X32_SYSCALL_BIT`
    // `0x4000_0000` OR'd into it — `connect` is `0x4000_002a`, not 42), so an
    // x32 `connect` meets the tail below and is allowed. Closing it means a
    // mask and a second compare ahead of the four `JEQ`s, which is a different
    // program with a different instruction count and a different test; the
    // task's prose records it beside the design's other two filter boundaries
    // rather than pretending the guard covers it.
    vec![
        load(4),
        SockFilter { code: BPF_JMP | BPF_JEQ | BPF_K, jt: 1, jf: 0, k: arch.audit_arch() },
        ret(SECCOMP_RET_KILL_PROCESS),
        load(0),
        jump(arch.syscall_nr(Syscall::Connect)),
        deny,
        jump(arch.syscall_nr(Syscall::SendTo)),
        deny,
        jump(arch.syscall_nr(Syscall::SendMsg)),
        deny,
        jump(arch.syscall_nr(Syscall::SendMmsg)),
        deny,
        ret(SECCOMP_RET_ALLOW),
    ]
}

// The `prctl` declaration is **not** here, and that is not a matter of style:
// under edition 2024 an `extern` block is itself an unsafe construct, so at
// module scope it is a fourth one that no `#[allow(unsafe_code)]` on a sibling
// item covers — and the crate is `deny`. It is declared inside `install`
// instead, where that function's own attribute covers the declaration as well
// as the call, which is what keeps this crate's enumerated count at three
// sites rather than four constructs.

/// What [`install`] did.
///
/// **A return value *as well as* the `warn!` below, and the two are not in
/// tension.** The value exists because the decision to *act* on the degradation
/// belongs to the caller: a library that announced it with `eprintln!` could
/// neither be tested nor silenced, and a returned value cannot be dropped on
/// the floor because the caller is holding it. The event is a different
/// instrument on purpose — `tracing` emits nothing at all until a subscriber is
/// installed, and this crate installs none, so the `warn!` costs a caller
/// nothing and reaches a log only where the process that owns the runtime has
/// asked for one (shared §7).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::seccomp::Seccomp;
///
/// let skipped = Seccomp::SkippedUnsupportedArch;
/// assert!(!matches!(skipped, Seccomp::Installed));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seccomp {
    /// The filter is live and irreversible for this thread.
    Installed,
    /// This architecture has no syscall table here, so nothing was installed.
    /// The directory boundary is unaffected; the network guarantee is not
    /// delivered, and the caller is expected to say so.
    SkippedUnsupportedArch,
}

/// Install the filter on the calling thread.
///
/// Must run before any thread exists: a seccomp filter applies per-thread
/// unless `SECCOMP_FILTER_FLAG_TSYNC` is used, and there is nothing to
/// synchronise while the process is single-threaded (design §8).
///
/// # Errors
///
/// [`Error::Step`] when the kernel refuses the filter. **A refusal is fatal,
/// and an unknown architecture is not** — the two are deliberately different
/// outcomes: an architecture this crate has no table for is a limitation of
/// the table, while a refused `prctl` means the filter the code believes is
/// installed is not, which is exactly the silent downgrade the design refuses
/// everywhere else.
///
/// # Safety
///
/// This is one of the crate's three authorised `unsafe` sites (design decision
/// 14). Its invariants: `program` outlives the call — `fprog` borrows it and
/// nothing stores either — the `SockFprog` layout matches the kernel's
/// `struct sock_fprog` for this target, and the process is single-threaded so
/// no task can observe a half-installed filter.
#[allow(unsafe_code)]
pub fn install() -> Result<Seccomp> {
    use std::io::Error as IoError;

    // Declared here, not at module scope: edition 2024 makes an `extern` block
    // unsafe in its own right, and this attribute covers the body's items while
    // a sibling item's would not (see the note at the end of `filter_program`).
    unsafe extern "C" {
        fn prctl(option: i32, ...) -> i32;
    }

    let Some(arch) = Arch::current() else {
        // The one documented degradation (design §11) and this crate's only
        // `warn!`: the directory boundary still holds, the network guarantee
        // does not, and that is a sentence an operator has to be given rather
        // than left to infer from a filter that was never installed.
        warn!(
            step = "seccomp",
            "this architecture has no syscall table, so outbound connections are NOT blocked"
        );
        return Ok(Seccomp::SkippedUnsupportedArch);
    };
    let program = filter_program(arch);
    let len = u16::try_from(program.len()).map_err(|_| Error::Unsupported {
        detail: "seccomp program too long",
    })?;
    let fprog = SockFprog { len, filter: program.as_ptr() };

    // SAFETY: `program` is alive for the whole call and `fprog` points at its
    // first element; PR_SET_SECCOMP copies the program and retains nothing.
    let rc = unsafe {
        prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &fprog as *const SockFprog, 0usize, 0usize)
    };
    if rc != 0 {
        let source = IoError::last_os_error();
        let error = Error::Step { step: "seccomp", source };
        // Logged where the `Error` is built (shared §7): a refused `prctl`
        // means the filter this code believes is installed is not, which is the
        // silent downgrade the design refuses everywhere else.
        error!(step = "seccomp", error = %error, "sandbox step failed");
        return Err(error);
    }
    debug!(
        step = "seccomp",
        arch = ?arch,
        instructions = program.len(),
        "outbound connect/sendto/sendmsg/sendmmsg denied"
    );
    Ok(Seccomp::Installed)
}

#[cfg(test)]
mod tests {
    // ... as written in Step 1
}
```

`install()` is Linux-only by construction — the whole crate is.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS. `install()` is not called by any unit test; the install is exercised by Task 6.

- [ ] **Step 5: Confirm the unsafe surface is the three enumerated functions**

Run: `rg -n 'allow\(unsafe_code\)' crates/tinio-sandbox-linux/src`
Expected: exactly three hits — `mechanism::userns_available` (the probe's `kernel_fork`), `mechanism::unshare`, and this task's `seccomp::install`. Task 3 added the first two and this task adds the third, and the count stops here: a fourth would mean a second body for one of those syscalls, which is a decision rather than a detail.

---

### Task 5: `confine` — the one-way door (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/enter.rs`
- Create: `crates/tinio-sandbox-linux/src/options.rs`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: everything above.
- Produces: `LinuxOptions { model, local_uid_configured, tmpdir, euid, served_root_access }` (`Default`, `from_env`, `with_model`, `with_tmpdir`, `with_euid`, `with_served_root_access`, `with_local_uid`, plus the getters and `resolved_tmpdir`), `Backend::with_options(LinuxOptions) -> Backend`, and the real `impl Backend for Backend`. **There is no `new`**: `Default` is what the type has, and a constructor that took five positional arguments would be a fifth place to add a field to.

The ordering is the whole design: validate, then `unshare`, then compose, then pivot, then retire the old root, then drop capabilities, then install the filter. Capabilities must go after the mounts (they need `CAP_SYS_ADMIN`) and before the filter (`PR_SET_NO_NEW_PRIVS` must already be set for a filter to install without it) — and **inside** the capability drop the bounding set is swept before the capset, because `PR_CAPBSET_DROP` needs an effective `CAP_SETPCAP` that the capset is about to remove (measured; see `drop_capabilities`). **Read-only is enforced by attempt-then-fallback rather than by a choice made in the plan**: `apply_mount` remounts a read-only bind `RDONLY|BIND`, and only `EPERM` — the kernel-managed families' answer — substitutes the `MS_RDONLY` overlay for it, which is the rule design §4's table and decision 8 now state, with Task 2's paragraph carrying the measurement behind it.

**One precondition is the host's rather than the request's, and it stands in front of that whole sequence: `$TMPDIR`.** The jail is created *inside* the temp directory — `std::env::temp_dir()`'s answer, which is `$TMPDIR` on a normal run, unless `LinuxOptions::with_tmpdir` overrides it — so a read-only, missing or full temp directory makes the confinement fail at the first `create_dir_all`: before any mount exists, and with nothing written anywhere else. Nothing about the exposures is at fault and the errno alone (`EACCES`, `EROFS`, `ENOSPC`) does not say which resource refused, so the step string names it: **`create jail root in $TMPDIR`**. It names the variable rather than a path because P0's `Error::Step` requires that (`step` names the operation, so the message stays stable when a temporary path is involved), and the operator's fix — a different `TMPDIR` — is the same either way. Two consequences follow for the phases that run this code: a test harness must not assume the jail lands beside its own `tempfile::tempdir()`, and a service must not be started with an `$TMPDIR` the service account cannot write.

- [ ] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, Error, LaunchPlan, Sandbox};

    use crate::mechanism::Mechanism;
    use crate::options::LinuxOptions;
    use crate::Backend;

    use super::refused_remount_needs_the_overlay;

    #[test]
    fn an_empty_sandbox_is_refused() {
        // `Error::Step`, not `Unsupported`: nothing exposed is the caller's bug,
        // and `Unsupported` would send the operator to look at their kernel
        // (design §11). P0's stub returns the same variant for the same reason.
        // The check runs before the availability probe, so this test needs no
        // namespace and gets one answer on every host.
        let err = Backend::default()
            .confine(&Sandbox::new(), &LaunchPlan::new())
            .unwrap_err();
        assert!(matches!(err, Error::Step { step: "confine", .. }), "{err:?}");
    }

    #[test]
    fn a_relative_exposure_is_refused_before_anything_irreversible_happens() {
        // Validation must run before any unshare, so an operator typo does not
        // leave the process half-confined. `Auto` is the model here because it
        // resolves on any host this test can run on, which leaves the exposure
        // loop as the only thing that can refuse.
        let sandbox = Sandbox::new().expose("relative", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(LinuxOptions::default().with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::BadPath { .. }), "{err:?}");
    }

    #[test]
    fn a_missing_path_is_refused_before_anything_irreversible_happens() {
        let sandbox = Sandbox::new().expose("/no/such/path/at/all", AccessMode::ReadOnly);
        let mut backend =
            Backend::with_options(LinuxOptions::default().with_model(Mechanism::Root).with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::BadPath { reason: "does not exist", .. }), "{err:?}");
    }

    #[test]
    fn an_unavailable_model_is_refused() {
        // Forced root without root: decided from the injected euid alone, with
        // no probe and therefore the same answer on every host.
        let sandbox = Sandbox::new().expose("/", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(
            LinuxOptions::default().with_model(Mechanism::Root).with_euid(1000),
        );
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::Unsupported { .. }), "{err:?}");
    }

    #[test]
    fn a_local_uid_mapping_is_refused_under_the_user_namespace() {
        // An unmapped uid cannot be chowned to, so the combination is a
        // configuration error rather than a runtime surprise (design §8). A
        // *forced* user namespace is refused from the request alone — no probe,
        // no privileges, no namespace — because the refusal is the same whether
        // or not this host would grant one. The `Auto` half of the same rule
        // cannot be decided before the probe, so it is checked against the
        // **resolved** model in `enter`; no unit test can drive that path,
        // because what the probe answers is a property of the host.
        let sandbox = Sandbox::new().expose("/", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(
            LinuxOptions::default()
                .with_model(Mechanism::UserNamespace)
                .with_local_uid(true)
                .with_euid(1000),
        );
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(err.to_string().contains("local_uid"), "{err}");
        assert!(err.to_string().contains("root"), "{err}");
    }

    #[test]
    fn the_jail_is_derived_from_the_first_exposure() {
        // The first exposure is the served root by construction: `serve`
        // exposes it first. Options that do not change that are the only ones
        // unit-testable, because everything after validation is irreversible.
        let opts = LinuxOptions::default().with_tmpdir(PathBuf::from("/tmp"));
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
    }

    #[test]
    fn only_eperm_selects_the_overlay_fallback() {
        // **The read-only mechanism is the filesystem's answer, so the reading
        // of that answer has to be exact** (design §4, re-measured 2026-09-19):
        // `EPERM` is what the kernel-managed families return for a read-only
        // remount and takes the `MS_RDONLY` overlay; every other errno stays a
        // failure, because a fallback taken on one of them would mount
        // something other than what was refused and report a read-only exposure
        // it never established. This is also where the overlay's own test lives
        // now: nothing pure can pin it any more — `plan` emits `Bind` for every
        // read-only exposure — and the branch itself needs a filesystem, which
        // is what Task 6's jail run supplies.
        assert!(refused_remount_needs_the_overlay(rustix::io::Errno::PERM));
        assert!(!refused_remount_needs_the_overlay(rustix::io::Errno::INVAL));
        assert!(!refused_remount_needs_the_overlay(rustix::io::Errno::ACCESS));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux enter::`
Expected: FAIL — `cannot find struct 'LinuxOptions'`.

- [ ] **Step 3: Write `options.rs`**

```rust
//! The Linux backend's construction-time options.
//!
//! Platform-specific knobs live here rather than in a shared type, so the
//! `Backend` trait's signature stays the same on every platform (design
//! decision 4 of the 2026-09-18 grilling — a shared options type would
//! accumulate a field per platform that no other platform reads).

use std::path::PathBuf;

use tinio_sandbox_core::{AccessMode, Result};

use crate::mechanism::Mechanism;

/// How to confine, and the facts the backend needs that the sandbox does not
/// own.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::{LinuxOptions, Mechanism};
///
/// let opts = LinuxOptions::default().with_model(Mechanism::Root);
/// assert_eq!(opts.model(), Mechanism::Root);
/// ```
#[derive(Debug, Clone)]
pub struct LinuxOptions {
    model: Mechanism,
    local_uid_configured: bool,
    tmpdir: Option<PathBuf>,
    euid: Option<u32>,
    served_root_access: AccessMode,
}

impl Default for LinuxOptions {
    fn default() -> Self {
        Self {
            model: Mechanism::default(),
            local_uid_configured: false,
            tmpdir: None,
            euid: None,
            served_root_access: AccessMode::ReadWrite,
        }
    }
}

impl LinuxOptions {
    /// The defaults, with the model read from `TINIO_SANDBOX_MODEL`.
    ///
    /// **This is the only thing that reads that variable.** It is what makes
    /// the two verification legs actually different — the WSL2 script sets
    /// `TINIO_SANDBOX_MODEL=userns` and `=root`, and without a caller for
    /// `Mechanism::from_env` both legs would run `Auto` and the root model would
    /// never be exercised (design §12). `Default` stays free of the
    /// environment so it cannot fail and cannot depend on how the test process
    /// was started.
    ///
    /// # Errors
    ///
    /// [`tinio_sandbox_core::Error::Unsupported`] for a value that is neither
    /// `userns` nor `root`, so a typo cannot silently pick a mechanism.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::LinuxOptions;
    ///
    /// // Whatever the environment says, the call either answers or explains.
    /// let options = LinuxOptions::from_env()?;
    /// let _ = options.model();
    /// # Ok::<(), tinio_sandbox_core::Error>(())
    /// ```
    pub fn from_env() -> Result<Self> {
        let raw = std::env::var("TINIO_SANDBOX_MODEL").ok();
        Ok(Self::default().with_model(Mechanism::from_env(raw.as_deref())?))
    }

    /// The requested model (design decision 7).
    pub fn with_model(mut self, model: Mechanism) -> Self {
        self.model = model;
        self
    }

    /// Whether FR-033's `local_uid` mapping is configured — the one reason a
    /// read-write jail keeps a capability (design §8).
    pub fn with_local_uid(mut self, configured: bool) -> Self {
        self.local_uid_configured = configured;
        self
    }

    /// Where to put the jail directory. Unset uses `std::env::temp_dir()`,
    /// which is `$TMPDIR` on a normal run — and that directory is a
    /// precondition of the whole confinement rather than a detail: the jail
    /// tmpfs is *created* inside it, so a read-only, missing or full one fails
    /// at the first `create_dir_all` with nothing else in the run to blame.
    /// The failure names it: `create jail root in $TMPDIR` (Task 5's `enter`
    /// says so in full).
    pub fn with_tmpdir(mut self, tmpdir: PathBuf) -> Self {
        self.tmpdir = Some(tmpdir);
        self
    }

    /// Override the effective uid. Tests use it; production reads the kernel.
    pub fn with_euid(mut self, euid: u32) -> Self {
        self.euid = Some(euid);
        self
    }

    /// The access the served root gets; decides the capability set.
    pub fn with_served_root_access(mut self, access: AccessMode) -> Self {
        self.served_root_access = access;
        self
    }

    /// The requested model.
    pub fn model(&self) -> Mechanism {
        self.model
    }

    /// The jail directory's parent, when one was set.
    pub fn tmpdir(&self) -> Option<&PathBuf> {
        self.tmpdir.as_ref()
    }

    /// The access the served root gets.
    pub fn served_root_access(&self) -> AccessMode {
        self.served_root_access
    }

    /// Whether a `local_uid` mapping is configured.
    pub fn local_uid_configured(&self) -> bool {
        self.local_uid_configured
    }

    /// The effective uid to resolve the model against.
    pub fn euid(&self) -> u32 {
        self.euid
            .unwrap_or_else(|| rustix::process::geteuid().as_raw())
    }

    /// The tmpdir to place the jail in.
    pub fn resolved_tmpdir(&self) -> PathBuf {
        self.tmpdir.clone().unwrap_or_else(std::env::temp_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_a_read_write_root_with_an_automatic_model() {
        let opts = LinuxOptions::default();
        assert_eq!(opts.model(), Mechanism::Auto);
        assert_eq!(opts.served_root_access(), AccessMode::ReadWrite);
        assert!(!opts.local_uid_configured());
    }

    #[test]
    fn every_setter_round_trips() {
        let opts = LinuxOptions::default()
            .with_model(Mechanism::Root)
            .with_local_uid(true)
            .with_tmpdir(PathBuf::from("/tmp"))
            .with_euid(1000)
            .with_served_root_access(AccessMode::ReadOnly);
        assert_eq!(opts.model(), Mechanism::Root);
        assert!(opts.local_uid_configured());
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
        assert_eq!(opts.euid(), 1000);
        assert_eq!(opts.served_root_access(), AccessMode::ReadOnly);
    }
}
```

- [ ] **Step 4: Write `enter.rs`**

```rust
//! `confine`: build the jail and step into it.

use std::ffi::CString;
use std::fs;
use std::io;
use std::path::Path;

use rustix::mount::{
    MountFlags, MountPropagationFlags, UnmountFlags, mount, mount_bind_recursive, mount_change,
    mount_remount, unmount,
};
use rustix::process::pivot_root;
use rustix::thread::UnshareFlags;
use tinio_sandbox_core::{
    AccessMode, Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox,
};
use tracing::{debug, error};

use crate::caps::{self, Cap};
use crate::jail::{jail_root, mirror_under};
use crate::mechanism::{Mechanism, Resolved, unshare, userns_available};
use crate::options::LinuxOptions;
use crate::plan::{Mount, MountKind, OverlayDirs, plan};
use crate::seccomp;

/// Log a refusal and hand it back, so every refusing `return` in this module is
/// one call and the log cannot drift from the error (shared §7).
///
/// The request-level refusals go through here as well as the syscall failures:
/// a refusal is exactly what an operator has to reconstruct from a log, and the
/// caller's `?` only prints it on the way out of the process.
fn refused(step: &'static str, error: Error) -> Error {
    error!(step, error = %error, "sandbox step refused");
    error
}

/// [`refused`] for a syscall's own error, which is what nearly every call site
/// has.
///
/// Generic over the error type because the callers are not all `std`: `rustix`
/// returns [`rustix::io::Errno`], which converts into `std::io::Error` through
/// its own `From` impl but is not that type. Without the bound every one of
/// this crate's ~14 syscall sites would need its own closure — and, worse, a
/// `|err: io::Error| …` written at one of them does not compile at all, because
/// nothing coerces `Errno` to `io::Error` on its own.
fn fail(step: &'static str, source: impl Into<io::Error>) -> Error {
    let source = source.into();
    let error = Error::Step { step, source };
    refused(step, error)
}

/// The `.map_err` shape of [`fail`], for the call sites that read better as a
/// closure.
fn step<Source: Into<io::Error>>(step_name: &'static str) -> impl Fn(Source) -> Error {
    move |source: Source| fail(step_name, source)
}

/// Write one id map through the descriptor that was opened for it.
///
/// **Through the descriptor, not through a fresh `fs::write`**: these files are
/// only writable from the namespace they describe, and the open has to happen
/// after the `unshare` that created it — writing through the held handle is what
/// keeps that true by construction instead of by comment (design decision 12).
/// The name is a parameter so the error names the map that failed without
/// re-deriving it from the path.
fn write_map(file: &mut fs::File, name: &'static str, contents: &str) -> Result<()> {
    use std::io::Write as _;

    file.write_all(contents.as_bytes())
        .map_err(step(name))
        // One event per map, named by the caller: which map was written and
        // what it says is the difference between "the jail did not build" and
        // "the uid map was refused" (shared §7). The contents are a mapping,
        // not a secret — they are the invoking user's own ids.
        .inspect(|_| debug!(step = name, contents, "id map written"))
}

/// The one combination the user-namespace model cannot carry (design §8).
///
/// One function so the two checks that need it — the request and the
/// resolution — cannot word the refusal differently.
fn local_uid_conflicts_with_userns() -> Error {
    Error::Unsupported {
        detail: "a local_uid mapping cannot be used with the user-namespace model — an unmapped \
                 uid cannot be chowned to; start with TINIO_SANDBOX_MODEL=root (or as root), or \
                 drop the mapping",
    }
}

impl crate::Backend {
    /// Confine the calling process.
    ///
    /// Returns [`Confinement::InPlace`] on success; the process is jailed by
    /// then, and every failure is fatal (design §11).
    ///
    /// # Errors
    ///
    /// [`Error::BadPath`] before anything irreversible happens;
    /// [`Error::Unsupported`] when the model is unavailable or a combination
    /// the design refuses was asked for; [`Error::Step`] for any refusing
    /// syscall, named by the step.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn enter(&mut self, sandbox: &Sandbox, opts: &LinuxOptions) -> Result<Confinement> {
        // 1. Validate everything a mistake could be made about, while the
        //    process can still be repaired by exiting.

        // The combination that cannot work, refused before anything is probed
        // or created: under a user namespace the process is chowned *to* an
        // unmapped uid, so a `local_uid` mapping has nothing to land in
        // (design §8).
        //
        // A *forced* user namespace is decided from the request alone — no
        // probe, no privilege, no namespace — because the refusal is the same
        // whether or not this host would grant the namespace, and asking the
        // request is what makes the answer identical on every host. The other
        // half of the same rule is checked against the **resolved** model below.
        if opts.model() == Mechanism::UserNamespace && opts.local_uid_configured() {
            return Err(refused("local_uid", local_uid_conflicts_with_userns()));
        }

        // Nothing exposed is the caller's bug, not the platform's shortcoming,
        // and it is decided **before the probe** so the answer does not depend on
        // what this host happens to allow. `Error::Unsupported` would send the
        // operator to look at their kernel, which is the wrong place (design §11).
        let served_root = sandbox
            .exposures()
            .first()
            .map(|e| e.host.clone())
            .ok_or_else(|| fail("confine", io::Error::other("nothing exposed to confine to")))?;

        // Design decision 7's three tiers, decided by asking the kernel rather
        // than guessing: `userns_available` forks a child, so a refusal costs
        // nothing here. `Auto` needs the answer only when the user namespace
        // might be the choice; the forced models that do not depend on it skip
        // the fork.
        let needs_probe = matches!(opts.model(), Mechanism::UserNamespace | Mechanism::Auto);
        let resolved_now =
            opts.model().resolved(opts.euid(), if needs_probe { userns_available()? } else { false });
        if let Resolved::Unavailable(detail) = resolved_now {
            // `Error::Unsupported`, because the platform really cannot confine
            // this process — the one thing P0's `Error` reserves that variant
            // for.
            return Err(refused("model", Error::Unsupported { detail }));
        }

        // **The other half of the `local_uid` rule, and the half the request
        // cannot answer.** `Auto` resolves *into* the user namespace whenever
        // the probe grants one, so a check that only looked at `opts.model()`
        // would let `Auto` + a configured `local_uid` proceed on exactly the
        // hosts where the mapping is unusable — and it would keep `CAP_CHOWN`
        // inside a namespace where the configured uid is not mapped, which
        // design §8 and §11 both call a startup configuration error. The
        // resolved model is the only thing that knows.
        if matches!(resolved_now, Resolved::UserNamespace) && opts.local_uid_configured() {
            return Err(refused("local_uid", local_uid_conflicts_with_userns()));
        }
        // One event for the resolution, carrying the request, the kernel's
        // answer and the outcome. Which model actually ran is the first
        // question anyone asks of a log from a run that confined the wrong way
        // (shared §7).
        debug!(
            step = "model",
            requested = ?opts.model(),
            euid = opts.euid(),
            resolved = ?resolved_now,
            "confinement model resolved"
        );

        // Existence first: `/tmp/does-not-exist` should say so, not report a
        // jail-root mismatch, and the jail directory created just below must
        // not be the thing that makes a mistyped path look present.
        //
        // The `/` passed as the jail here is a sentinel, and it is a real
        // check: `mirror_under` validates the path it composes, so this refuses
        // an exposure of `/` — whose mirror under *any* jail is the jail itself
        // — as well as a relative one, and it does it before a directory is
        // created anywhere.
        for exposure in sandbox.exposures() {
            if !exposure.host.exists() {
                return Err(refused(
                    "expose",
                    Error::BadPath { path: exposure.host.clone(), reason: "does not exist" },
                ));
            }
            mirror_under(Path::new("/"), &exposure.host).map_err(|error| refused("expose", error))?;
        }

        let model = resolved_model(resolved_now);
        let jail = jail_root(&opts.resolved_tmpdir(), &served_root);
        let overlay = OverlayDirs {
            upper: jail.join(".overlay/upper"),
            work: jail.join(".overlay/work"),
        };
        let mounts =
            plan(sandbox, &jail, model, &overlay).map_err(|error| refused("plan", error))?;

        // 2. A private user namespace first, and on its own. The uid and gid
        //    maps below can only be written through `/proc/<pid>/…`, which is
        //    reachable exactly while the mount namespace is the host's — so the
        //    two unshares are separate steps with the map write between them
        //    (design decision 12). A descriptor opened *before* this call is
        //    bound to the initial namespace and its write returns EPERM, which
        //    is why the files are opened after it.
        let map_fds = if matches!(resolved_now, Resolved::UserNamespace) {
            let euid = opts.euid();
            let egid = rustix::process::getegid().as_raw();
            unshare(UnshareFlags::NEWUSER, "unshare user namespace")?;

            // Opened here, after the unshare, and written **through**: a
            // descriptor opened before it belongs to the initial namespace, so
            // its write is what is refused (design decision 12). Holding the
            // handle and writing through it makes that ordering structural
            // rather than a comment someone can move away from.
            let mut setgroups = open_map("/proc/self/setgroups")?;
            write_map(&mut setgroups, "setgroups", "deny")?;
            let mut uid_map = open_map("/proc/self/uid_map")?;
            write_map(&mut uid_map, "uid_map", &format!("0 {euid} 1"))?;
            let mut gid_map = open_map("/proc/self/gid_map")?;
            write_map(&mut gid_map, "gid_map", &format!("0 {egid} 1"))?;
            Some((setgroups, uid_map, gid_map))
        } else {
            None
        };

        // 3. The private mount namespace, now that the maps exist. One thread
        //    still, which is what makes both unshares legal (design §10).
        unshare(UnshareFlags::NEWNS, "unshare mount namespace")?;

        // 4. `pivot_root` refuses while the old root is shared, so the whole
        //    tree is made private first. On the host measured this is already
        //    true; the call is unconditional because the hosts where it is not
        //    are exactly the ones that would otherwise fail with a bare EINVAL.
        //
        //    **`mount_change` takes propagation flags, and this is the call it
        //    is for.** It emits `mount(NULL, target, NULL, flags, NULL)` with no
        //    `MS_REMOUNT`, so `PRIVATE | REC` here is what makes the tree
        //    private — and the very same function cannot make anything
        //    read-only, which is why the remount in `apply_mount` is a
        //    different call (see the note there).
        mount_change("/", MountPropagationFlags::PRIVATE | MountPropagationFlags::REC)
            .map_err(step("make / private"))?;
        debug!(step = "make / private", "the mount tree is private");

        // 5. The jail root, **inside `$TMPDIR`** — the first host resource the
        //    confinement touches, and the one whose failure names nothing on its
        //    own. It may already exist — the previous run's leftover, which
        //    design decision 10 says to take over. The step string is the
        //    difference between an operator who knows to look at their temp
        //    directory and one reading an `EACCES` about a path they never
        //    chose: the jail's parent is `std::env::temp_dir()` unless
        //    `LinuxOptions::with_tmpdir` says otherwise, and the error is
        //    `create jail root in $TMPDIR` for that reason. The variable is
        //    named rather than its value because P0's `Error::Step` keeps paths
        //    out of `step` so the message stays stable.
        fs::create_dir_all(&jail).map_err(step("create jail root in $TMPDIR"))?;

        // 6. The mounts, in plan order.
        for entry in &mounts {
            apply_mount(entry, &overlay)?;
        }

        // 7. The maps have been read by every mount that needed them; the
        //    descriptors are closed explicitly because nothing here ever
        //    `exec`s, so `CLOEXEC` never gets a chance to close them and they
        //    would otherwise be inherited by the whole server lifetime.
        drop(map_fds);

        // 8. The pivot. `put_old` must already exist inside the new root, and
        //    the jail is a tmpfs we own, so creating it writes nothing to the
        //    served root (design §4).
        let put_old = jail.join(".oldroot");
        fs::create_dir_all(&put_old).map_err(step("create put_old"))?;
        std::env::set_current_dir(&jail).map_err(step("chdir jail"))?;
        pivot_root(".", ".oldroot").map_err(step("pivot_root"))?;
        debug!(step = "pivot_root", root = %jail.display(), "root changed");
        std::env::set_current_dir("/").map_err(step("chdir /"))?;
        unmount("/.oldroot", UnmountFlags::DETACH).map_err(step("umount old root"))?;
        debug!(step = "umount old root", "the old root is detached");

        // 9. Drop everything the jail no longer needs, then cut the network.
        //    The filter's outcome is recorded on the backend rather than
        //    dropped: `serve` reads it to warn when the one degradable step
        //    degraded (design §11).
        drop_capabilities(caps::retained(opts.served_root_access(), opts.local_uid_configured()))?;
        self.seccomp = Some(seccomp::install()?);

        Ok(Confinement::InPlace)
    }
}

// `unshare` is not defined here: it is `crate::mechanism`'s, declared there because
// its first caller — the user-namespace probe — lives in that module, and
// imported above. One wrapper means one safety argument (and one `unsafe`
// site) for one syscall; a second body here would be a second place to get
// `CLONE_FILES` wrong. It is `pub(crate)` in that module for this import and no
// other reason — a private item cannot be imported at all, so the earlier
// spelling of this line was a compile error waiting for the first build.

/// Open a `/proc` map file, keeping the descriptor so the close is explicit.
fn open_map(path: &str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(step("open id map"))
}

/// Whether a refused read-only remount selects the overlay fallback (design §4).
///
/// **`EPERM` is the trigger and nothing else is.** It is the kernel-managed
/// families' answer — the filesystems a user namespace may mount itself, and a
/// subdirectory of one, re-measured 2026-09-19 — while every other errno is a
/// failure to report rather than a mechanism to switch to: a fallback taken on
/// `EINVAL` would quietly replace a malformed mount with a different one, and
/// the run would report a read-only exposure it never established.
///
/// A function rather than an inline guard because this *is* the read-only
/// decision: `apply_mount` cannot be unit-tested without mounting a filesystem,
/// and which errno takes the fallback is everything about it worth pinning.
fn refused_remount_needs_the_overlay(source: rustix::io::Errno) -> bool {
    source == rustix::io::Errno::PERM
}

fn apply_mount(entry: &Mount, overlay: &OverlayDirs) -> Result<()> {
    match entry.kind {
        MountKind::JailTmpfs => {
            if let Some(parent) = entry.target.parent() {
                fs::create_dir_all(parent).map_err(step("create jail parent"))?;
            }
            // **`size=1m` is enough, and the arithmetic is the reason rather
            // than a guess.** What lands in this tmpfs is directories: one for
            // each prefix of each mirrored path (`/srv`, `/srv/data`, `/dev`,
            // …), one for `.oldroot`, the four device nodes, and the overlay's
            // pair. The deepest realistic set — a served root three components
            // down, a state directory beside it, four device files — is on the
            // order of twenty directories, and a tmpfs directory is at most one
            // 4 KiB page plus its entries: under 100 KiB, against 1 MiB, which
            // is 256 such pages. The quota's real subject is the overlay's
            // upper, the only thing in the jail that can ever hold data — and
            // the mount that exposes the served root through it is `MS_RDONLY`,
            // so a write aimed at that exposure is refused at the mount with
            // `EROFS` and never reaches this quota (`OverlayDirs` says which
            // write `ENOSPC` is actually for).
            mount(
                "tmpfs",
                &entry.target,
                "tmpfs",
                MountFlags::NOSUID | MountFlags::NODEV,
                Some(c"size=1m,mode=0755"),
            )
            .map_err(step("mount tmpfs"))?;
        }
        MountKind::Bind => {
            // **"Is the source a directory?", not "is it a file?"** The four
            // device files are character devices, so `is_file()` is false for
            // every one of them: that spelling runs the *directory* branch for
            // `/dev/null`, creating a directory at `<jail>/dev/null` and then
            // binding a chardev onto it — `ENOTDIR` at best, and at worst a
            // jail that never builds. Task 6's precondition is what makes the
            // second one visible: a half-built jail is a *failure* there rather
            // than a host without namespaces.
            if entry.source.is_dir() {
                fs::create_dir_all(&entry.target).map_err(step("create mount point"))?;
            } else {
                if let Some(parent) = entry.target.parent() {
                    fs::create_dir_all(parent).map_err(step("create device parent"))?;
                }
                fs::File::create(&entry.target).map_err(step("create device node"))?;
            }
            // Recursive, not plain: a bare `MS_BIND` of a mount-point source is
            // refused with EINVAL inside a user namespace (measured), and for a
            // source with no submounts the two forms are identical — so the
            // recursive call is the one to write everywhere.
            mount_bind_recursive(&entry.source, &entry.target).map_err(step("mount --bind"))?;
            if entry.access.is_read_only() {
                // **`mount_remount`, not `mount_change`, and this is the primary
                // enforcement of `AccessMode::ReadOnly`** (design decision 8).
                // `mount_change` takes `MountPropagationFlags` — `MountFlags`
                // has no `PRIVATE`, and `RDONLY`/`BIND` are not propagation
                // flags at all — and what it emits is
                // `mount(NULL, target, NULL, flags, NULL)`, with no `MS_REMOUNT`
                // anywhere in it, so a read-only request spelled through it
                // would return success and leave the served root writable. A
                // silent success here is worse than a failure: the run reports
                // confinement and the operator has none, which is exactly the
                // class of failure design §11 refuses.
                //
                // **And the overlay is the other half of the same rule, not an
                // alternative to it.** This remount is what an ext4 or a drvfs
                // bind accepts under *either* model (re-measured 2026-09-19);
                // the kernel-managed families — tmpfs, devtmpfs, procfs, sysfs,
                // and a subdirectory of a tmpfs with them — refuse it with
                // `EPERM`, and for those the `MS_RDONLY` overlay is what makes
                // the exposure read-only. The mount plan no longer chooses
                // between them, because nothing that performs no syscall can
                // (Task 2): this is the decision, and it is the filesystem's
                // answer that makes it.
                //
                // Re-entering `apply_mount` with the other kind, rather than
                // duplicating the overlay's body here, keeps the trace below
                // true: exactly one `mount applied` event per mount, carrying
                // the kind that actually landed.
                match mount_remount(&entry.target, MountFlags::RDONLY | MountFlags::BIND, "") {
                    Ok(()) => debug!(
                        step = "remount ro",
                        target = %entry.target.display(),
                        "bind remounted read-only"
                    ),
                    Err(source) if refused_remount_needs_the_overlay(source) => {
                        debug!(
                            step = "remount ro",
                            target = %entry.target.display(),
                            "the remount was refused; falling back to the read-only overlay"
                        );
                        let fallback = Mount { kind: MountKind::OverlayReadOnly, ..entry.clone() };
                        return apply_mount(&fallback, overlay);
                    }
                    Err(source) => return Err(fail("remount ro", source)),
                }
            }
        }
        MountKind::OverlayReadOnly => {
            // **The fallback arm, and the only way in is the substitution
            // above.** `plan` emits no entry of this kind (Task 2), and keeping
            // the mechanism a `MountKind` rather than an inline branch is what
            // lets that substitution reuse this body instead of a second copy
            // of it.
            fs::create_dir_all(&entry.target).map_err(step("create overlay mount point"))?;
            fs::create_dir_all(&overlay.upper).map_err(step("create overlay upper"))?;
            fs::create_dir_all(&overlay.work).map_err(step("create overlay work"))?;
            let options = format!(
                "lowerdir={},upperdir={},workdir={}",
                entry.source.display(),
                overlay.upper.display(),
                overlay.work.display()
            );
            let options = CString::new(options).map_err(|_| {
                refused(
                    "mount overlay",
                    Error::Unsupported { detail: "an exposed path contains a NUL byte" },
                )
            })?;
            // **`as_c_str()` and not `&options`**: `mount`'s data parameter is
            // `Data: Into<Option<&CStr>>`, and a `CString` reference is not one
            // of those — the conversion is explicit all the way down.
            mount(
                "overlay",
                &entry.target,
                "overlay",
                MountFlags::RDONLY | MountFlags::NOSUID | MountFlags::NODEV,
                Some(options.as_c_str()),
            )
            .map_err(step("mount overlay ro"))?;
        }
    }
    // One event per mount, carrying the source, the target, the kind and the
    // access the confined process gets through it (shared §7). At the end
    // rather than once per arm, so all three kinds are traced by construction
    // and a fourth kind cannot be added without being traced.
    debug!(
        step = "mount",
        source = %entry.source.display(),
        target = %entry.target.display(),
        kind = ?entry.kind,
        access = ?entry.access,
        "mount applied"
    );
    Ok(())
}

fn resolved_model(resolved: Resolved) -> Mechanism {
    match resolved {
        Resolved::UserNamespace => Mechanism::UserNamespace,
        Resolved::Root => Mechanism::Root,
        Resolved::Unavailable(_) => Mechanism::Root,
    }
}

fn drop_capabilities(keep: &[Cap]) -> Result<()> {
    use rustix::thread::{
        CapabilitySet, capabilities, remove_capability_from_bounding_set, set_capabilities,
        set_no_new_privs,
    };

    // No new privileges first: it is what lets the seccomp filter install
    // without CAP_SYS_ADMIN.
    set_no_new_privs(true).map_err(step("no_new_privs"))?;
    debug!(step = "no_new_privs", "no new privileges set");

    // The policy itself, in one line, before anything is dropped from it:
    // "which capabilities did this run keep" is the question a log is read to
    // answer (shared §7).
    debug!(step = "capabilities", retained = ?keep, "capability policy applied");

    // **The bounding set goes first, and that order is load-bearing.**
    // `PR_CAPBSET_DROP` requires `CAP_SETPCAP` in the *effective* set at the
    // moment of the call, and the capset below is what takes it away — so a
    // sweep written after the capset fails with `EPERM` on its first capability
    // and the bounding set is never touched. Measured on WSL2: with caps intact
    // the drop succeeds; after an empty capset it returns `EPERM` and a
    // `PR_CAPBSET_READ` still reports the capability as present. Dropping from
    // the bounding set does not clear the effective bit, so the sweep may drop
    // `CAP_SETPCAP` itself and keep going.
    for cap in ALL_CAPABILITIES {
        if keep.iter().any(|kept| kept.set() == cap) {
            continue;
        }
        // Neither errno is ignorable, and they mean different things: `EINVAL`
        // is "past `CAP_LAST_CAP`" — unreachable while this array stays the
        // kernel's own list, so seeing it means the array drifted — and `EPERM`
        // means the ordering above is wrong or a capability was asked for that
        // this namespace does not hold.
        //
        // **Two `Errno`s are compared directly, and the earlier spelling is
        // worth naming because it does not compile**: `raw_os_error()` returns
        // an `i32` on both types, so comparing it against
        // `Some(Errno::INVAL.raw_os_error())` puts an `i32` beside an
        // `Option<i32>` — and `Errno` derives `PartialEq`, so the comparison
        // the code wants is the one it can have.
        remove_capability_from_bounding_set(cap).map_err(|source| {
            let name = if source == rustix::io::Errno::INVAL {
                "drop capability from bounding set: past CAP_LAST_CAP"
            } else {
                "drop capability from bounding set"
            };
            fail(name, source)
        })?;
        // One event per drop, so the run's own capability set is reconstructible
        // from the log rather than from the array above it (shared §7).
        debug!(step = "drop capability", capability = cap.bits(), "dropped from the bounding set");
    }

    // Only now is the process's own set narrowed to what it keeps.
    let mut wanted = CapabilitySet::empty();
    for cap in keep {
        wanted |= cap.set();
    }

    let mut sets = capabilities(None).map_err(step("read capabilities"))?;
    sets.effective = wanted;
    sets.permitted = wanted;
    sets.inheritable = CapabilitySet::empty();
    set_capabilities(None, sets).map_err(step("capset"))?;
    debug!(step = "capset", effective = wanted.bits(), "capability sets narrowed");

    // The bounding set is what makes the drop stick across `execve`: with euid 0
    // the kernel recomputes the permitted set as full on exec, and only the
    // bounding set bounds it. Every capability not kept has left it by now.
    Ok(())
}

/// Every capability `CAP_CHOWN` (0) through `CAP_CHECKPOINT_RESTORE` (40), in
/// kernel numbering order.
///
/// Spelled out because `rustix` publishes no iterator over them: `CapabilitySet`
/// is a `bitflags` type whose `iter()` walks the bits *set in a value*, not the
/// universe of capabilities, and `Flags::iter_defined_names()` would drag an
/// explicit `bitflags` dependency in for one loop. The array doubles as the
/// sweep's bound — 41 is `CAP_LAST_CAP + 1`, so the loop can never reach the
/// `EINVAL` the kernel returns past that.
const ALL_CAPABILITIES: [CapabilitySet; 41] = [
    CapabilitySet::CHOWN,
    // ... the remaining forty constants, in kernel numbering order, through
    // `CapabilitySet::CHECKPOINT_RESTORE`
];
```

Fill `ALL_CAPABILITIES` from the actual constant list in the registry's `rustix-1.1.*/src/thread/libcap.rs`: read the `CapabilitySet` `bitflags!` block, take its 41 named constants (`CHOWN` … `CHECKPOINT_RESTORE`, the file's own comments carry the kernel numbers) and write them into the array in numeric order. **Do not reach for `rustix::thread::Capability`**: it is deprecated since 1.1 — `rustix::thread` does re-export it, so it compiles, and that is exactly the trap — and every use of it is a warning under `-D warnings`. If a later rustix exposes a real iterator over the universe of capabilities, prefer it and delete the constant.

- [ ] **Step 5: Wire the trait implementation**

In `crates/tinio-sandbox-linux/src/lib.rs`, replace P0 Task 3's placeholder `impl BackendTrait for Backend` with:

```rust
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: LinuxOptions,
    /// What the last `confine` did about the network filter. Interior
    /// mutability because `Backend::confine` takes `&self` (P0's trait) and the
    /// value can only be known once the filter has been attempted.
    ///
    /// The alternative was to carry it in `Confinement`, which is P0's type and
    /// would then drag a Linux-only fact into both launcher platforms; or to
    /// return it from `confine`, whose return type the trait fixes.
    /// A caller that wants the answer — and `serve` does, to warn — reads it
    /// here after confining, which the `&mut self` receiver makes ordinary
    /// rather than something to work around with a `Cell`.
    seccomp: Option<Seccomp>,
}

impl Backend {
    /// A backend with explicit options.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::{Backend, LinuxOptions, Mechanism};
    ///
    /// let backend = Backend::with_options(LinuxOptions::default().with_model(Mechanism::Root));
    /// let _ = backend;
    /// ```
    pub fn with_options(options: LinuxOptions) -> Self {
        Self { options, seccomp: None }
    }

    /// What the last [`Backend::confine`] did about the seccomp filter, or
    /// `None` if it has not been called.
    ///
    /// The one degradation in the Linux backend is an architecture with no
    /// syscall table, and this is where the caller finds out (design §11): a
    /// directory boundary with no network cut is not the guarantee that was
    /// asked for, so it is *returned* rather than left for someone to notice.
    /// It is also **logged**, with a `warn!` at the point `install` decides it,
    /// because a process that has a subscriber should not have to read a field
    /// to learn that its network is open — the two are not in tension, and
    /// `Seccomp`'s own doc says why.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Backend;
    ///
    /// assert!(Backend::default().seccomp().is_none(), "nothing confined yet");
    /// ```
    pub fn seccomp(&self) -> Option<Seccomp> {
        self.seccomp
    }
}

impl BackendTrait for Backend {
    fn confine(&mut self, sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        // Linux confines in place: it never launches a child, so the launch
        // plan is unused here (design decision 6).
        self.enter(sandbox, &self.options)
    }
}
```

and register `pub mod {caps, enter, jail, model, options, plan, seccomp};` with the re-exports `jail_root`, `mirror_under`, `plan`, `Mount`, `MountKind`, `OverlayDirs`, `DEVICE_FILES`, `Resolved`, `userns_available`, `Cap`, `retained`, `LinuxOptions`, `Seccomp` — and `Mechanism`, which is P0's own `pub enum` in `lib.rs` and therefore needs no `use` line to be public (naming it in one is `E0255`; Task 3 says so in full).

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS. The five `enter` tests all refuse before the first irreversible call, so they need no privileges.

- [ ] **Step 7: Confirm the unsafe surface**

Run: `rg -n 'allow\(unsafe_code\)' crates/tinio-sandbox-linux/src`
Expected: exactly three hits — `seccomp::install`, `mechanism::unshare` and `mechanism::userns_available`'s `kernel_fork`. A fourth would mean a second body for one of those syscalls, and that is a decision rather than a detail.

---

### Task 6: The subprocess integration test

**Files:**
- Create: `crates/tinio-sandbox-linux/src/bin/sandbox-child.rs`
- Create: `crates/tinio-sandbox-linux/tests/jail.rs`

**Interfaces:**
- Consumes: `Backend`, `LinuxOptions`, `Mechanism`, `Sandbox`, `AccessMode`.
- Produces: nothing the library uses.

`pivot_root` is irreversible, so the confined process cannot also be the asserting one: the jail runs in a **separate executable** and the parent reads its report from stdout.

**Why a separate executable rather than a re-executed test binary.** `unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process (design's own evidence table), and `libtest` starts every test on its own worker thread — so a `#[test] fn child_role()` re-executed with `--exact` is *already* multi-threaded before it reaches the first syscall. The failure is indistinguishable from "this host cannot confine", which is why the earlier shape of this task could never assert anything: it would have reported `skipping` on every host, including the ones that can confine. A plain `fn main` is single-threaded by construction and needs no flag or harness cooperation.

**Why `src/bin/` and not `examples/`.** Two measured facts decide it. `CARGO_BIN_EXE_<name>` is set for binary targets only — an example named in `env!(…)` is a hard *"environment variable not defined at compile time"* error, on stable and nightly alike — and, second, a filtered `cargo test --test jail` builds binary targets but **not** examples, so the walk-up-the-profile fallback an example would need panics with `NotFound` on exactly the command this task is run with. The binary target is the only shape that works under every invocation style.

**The two acceptance legs run different code, and that is what `TINIO_SANDBOX_MODEL` is for.** `TINIO_SANDBOX_MODEL=userns` and `sudo -E TINIO_SANDBOX_MODEL=root` are not two spellings of one run — **but not for the reason an earlier draft of this paragraph gave, which is the rule the design's 2026-09-19 re-measurement falsified.** The mechanism that makes a read-only exposure read-only is chosen by the *filesystem*, not by the model (design §4, and Task 2's paragraph says so in full): this test's served root is a `tempfile::tempdir()` under `/tmp`, which is a tmpfs, and a tmpfs bind is one of the kernel-managed families an **unprivileged** user namespace may not remount read-only — so the `userns` leg reaches `EPERM` and takes the `MS_RDONLY` overlay, while the privileged leg's remount is accepted and it takes `remount,bind,ro`. The two legs differ because one of them is unprivileged, and the two implementations of `AccessMode::ReadOnly` on this platform are the two that rule reaches (design §9, §12). The child therefore resolves its model from the environment through the one reader of that variable, `LinuxOptions::from_env()` — and *not* by discovering one for itself, which is what the first draft did: `Auto`, plus a hand-rolled "root when euid is 0 and the probe failed", picks the user namespace on a userns-capable host under `sudo` exactly as it does unprivileged, so both legs would run the overlay path and the remount path would never be exercised while the suite reported success. The precondition below is the other half of that: a run that did not confine **fails**, and only a host whose capability is absent before the child starts is allowed to skip.

- [ ] **Step 1: Write the confined child**

`crates/tinio-sandbox-linux/src/bin/sandbox-child.rs`:

```rust
//! The confined half of the jail test: build the jail, then report what is
//! reachable from inside it, one `key: value` line per check on stdout.
//!
//! A separate executable rather than a re-executed test binary, because
//! `unshare(CLONE_NEWUSER)` refuses in a multi-threaded process and libtest
//! runs every test on a worker thread. `fn main` is single-threaded, which is
//! the one property this file depends on.
//!
//! A `[[bin]]` target rather than an example, which is also a measured choice:
//! `CARGO_BIN_EXE_<name>` exists for binaries only (naming an example in
//! `env!(…)` fails to compile), and a filtered `cargo test --test jail` builds
//! binaries but not examples — so the profile-walking fallback an example would
//! need breaks on exactly the command this test is run with.
//!
//! stdout rather than a file: after `pivot_root` the only paths that exist are
//! the ones the jail exposes, so a report written to the parent's temp
//! directory would fail precisely when it is most needed — on the host that
//! cannot confine and must say so.

use std::fs;
use std::path::Path;

use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, Sandbox};
use tinio_sandbox_linux::{Backend, LinuxOptions};

#[cfg(target_os = "linux")]
fn main() {
    let root = std::env::var("TINIO_SANDBOX_TEST_ROOT").expect("child needs a root");
    let access = match std::env::var("TINIO_SANDBOX_TEST_ACCESS").as_deref() {
        Ok("read-only") => AccessMode::ReadOnly,
        _ => AccessMode::ReadWrite,
    };
    let state = std::env::var("TINIO_SANDBOX_TEST_STATE").ok();

    let mut sandbox = Sandbox::new().expose(&root, access).device_files();
    if let Some(state) = &state {
        sandbox = sandbox.expose(state, AccessMode::ReadWrite);
    }

    // **The model comes from the environment, through its one reader.** The two
    // legs of the WSL2 run must not run the same code — and the code they
    // differ in is the read-only mechanism, which the *filesystem* picks rather
    // than the model (design §4, re-measured 2026-09-19): the served root here
    // is a tempdir under `/tmp`, a tmpfs, and a tmpfs bind is one of the
    // kernel-managed families an unprivileged user namespace may not remount
    // read-only. So `=userns` reaches `EPERM` and takes the `MS_RDONLY` overlay
    // fallback, while `=root` under `sudo` gets the remount the design calls
    // the primary mechanism; those two are the implementations of
    // `AccessMode::ReadOnly` this platform has (design §9, §12). The model read from
    // the environment is what decides *which of the two a leg can reach at
    // all*. A child that discovered its own model — `Auto`, or a hand-rolled
    // "root when euid is 0 and the probe failed" — would choose the *same* one
    // on a userns-capable host under `sudo`, so the remount path would never run
    // while the suite reported success for having tested half of the matrix.
    // Unset is still `Auto`, which is the right answer for a hand run.
    //
    // No `with_euid`: this process *is* the one being confined, so
    // `LinuxOptions::euid()` reads the kernel — the production path, and the
    // one worth exercising.
    let options = match LinuxOptions::from_env() {
        Ok(options) => options.with_served_root_access(access),
        Err(err) => {
            // **Not `confine-failed:`.** A typo in the variable is a
            // configuration error the parent has to fail on, not a host that
            // cannot confine; the prefix is what tells the two apart.
            println!("bad-model: {err}");
            return;
        }
    };
    let mut backend = Backend::with_options(options);

    if let Err(err) = backend.confine(&sandbox, &Default::default()) {
        println!("confine-failed: {err}");
        return;
    }

    match fs::read_to_string(Path::new(&root).join("bucket/obj.txt")) {
        Ok(body) => println!("read-ok: {}", body.trim()),
        Err(err) => println!("read-failed: {err}"),
    }
    println!("host-etc-visible: {}", Path::new("/etc/passwd").exists());
    println!(
        "host-home-visible: {}",
        std::env::var("HOME")
            .map(|home| Path::new(&home).join(".ssh").exists())
            .unwrap_or(false)
    );
    match fs::write(Path::new(&root).join("bucket/should-not-exist"), b"nope") {
        Ok(()) => println!("served-root-write: ALLOWED"),
        Err(err) => println!("served-root-write: refused ({})", err.kind()),
    }
    if let Some(state) = &state {
        match fs::write(Path::new(state).join("log.txt"), b"ok") {
            Ok(()) => println!("state-dir-write: ALLOWED"),
            Err(err) => println!("state-dir-write: refused ({})", err.kind()),
        }
    }
    println!(
        "outbound-connect: {}",
        match std::net::TcpStream::connect("1.1.1.1:53") {
            Ok(_) => "SUCCEEDED".to_string(),
            Err(err) => format!("refused ({err})"),
        }
    );
    println!("loopback-listen: {}", std::net::TcpListener::bind("127.0.0.1:0").is_ok());
}

/// The workspace builds on every host, so this target has to exist on the ones
/// that cannot confine: an empty `main` keeps `cargo check --workspace` and
/// `cargo clippy --all-targets` honest everywhere. Only the Linux-gated test
/// ever spawns this binary, so on any other target there is nothing to run.
#[cfg(not(target_os = "linux"))]
fn main() {}
```

- [ ] **Step 2: Write the asserting parent**

`crates/tinio-sandbox-linux/tests/jail.rs`:

```rust
//! End-to-end confinement. The jail is a one-way door, so the assertions run
//! here and the jail runs in `src/bin/sandbox-child.rs`.

#![cfg(target_os = "linux")]

use std::fs;
use std::net::TcpStream;
use std::path::Path;
use std::process::{Command, Stdio};

use tinio_sandbox_linux::Mechanism;

/// The address the outbound assertion probes. A host with no route to it fails
/// the test's *precondition* rather than passing the confinement check, which
/// is the difference between proving isolation and proving nothing.
const OUTBOUND_PROBE: &str = "1.1.1.1:53";

/// The model the WSL2 script asked for, read the way the child reads it: the
/// same variable through the same one reader.
fn requested_model() -> Mechanism {
    Mechanism::from_env(std::env::var("TINIO_SANDBOX_MODEL").ok().as_deref())
        .expect("TINIO_SANDBOX_MODEL must name a model")
}

/// Whether this host can confine under `model` — asked **before** the child
/// runs, and asked as a capability rather than as a substring of a failure.
///
/// **This is the whole difference between a green suite and a quiet one.** The
/// earlier shape skipped whenever the child's log contained `confine-failed:`,
/// which made every confinement bug indistinguishable from a host that cannot
/// confine: a mount table that never built, a device node created as a
/// directory (Task 5's `apply_mount`) or a `prctl` that was refused all printed
/// `skipping:` and exited 0. The question has a positive answer, it is
/// answerable here, and asking it first is what lets the child's failure be a
/// failure.
///
/// **`unshare(1)` rather than the backend's own `userns_available()`**: that
/// probe forks, and its safety argument is that nothing else exists in the
/// process yet (design §10) — a libtest worker thread is not that place. The
/// command is the one the WSL2 script gates on, so the script and the test
/// agree by construction rather than by review.
fn host_can_confine(model: Mechanism) -> bool {
    // Each variant is asked for the namespaces it actually needs: `--user
    // --map-root-user` for the user-namespace model, and a bare `--mount` for
    // the root model, whose single capability is `CAP_SYS_ADMIN`. Asking the
    // kernel rather than reading `geteuid()` is the point — a container can run
    // as uid 0 and still refuse a mount namespace, which is a host where this
    // model would fail at the first mount rather than at the probe.
    let user = || unshare_probe(&["--user", "--map-root-user"]);
    let mount = || unshare_probe(&["--mount"]);
    match model {
        Mechanism::UserNamespace => user(),
        Mechanism::Root => mount(),
        Mechanism::Auto => user() || mount(),
    }
}

/// `unshare(1)` with `flags`, as a process of its own.
///
/// A missing `unshare(1)` panics rather than skipping: the precondition is then
/// unknowable, and a suite that cannot tell "this host cannot confine" from "I
/// could not ask" is the shape this function exists to delete.
fn unshare_probe(flags: &[&str]) -> bool {
    Command::new("unshare")
        .args(flags)
        .arg("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or_else(|err| panic!("the precondition needs util-linux's `unshare(1)`: {err}"))
}

/// Spawn the confined child and return its report, or `None` when this host
/// cannot confine under the model the run asked for.
///
/// The skip is decided before the child exists, so everything the child reports
/// afterwards is a failure (design §12).
fn run_child(root: &Path, state: Option<&Path>, access: &str) -> Option<String> {
    let model = requested_model();
    if !host_can_confine(model) {
        eprintln!(
            "skipping: this host cannot confine under {model:?} — WSL2 is where both legs are \
             verified (design §12)"
        );
        return None;
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_sandbox-child"));
    command
        .env("TINIO_SANDBOX_TEST_ROOT", root)
        .env("TINIO_SANDBOX_TEST_ACCESS", access);
    if let Some(state) = state {
        command.env("TINIO_SANDBOX_TEST_STATE", state);
    }
    let output = command.output().expect("spawn confined child");
    let log = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(!log.contains("bad-model"), "the child refused TINIO_SANDBOX_MODEL:\n{log}");
    assert!(
        !log.contains("confine-failed"),
        "this host can confine under {model:?} but the child did not:\n{log}\nchild stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some(log)
}

#[test]
fn the_jail_confines_the_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    fs::create_dir_all(root.join("bucket")).expect("create root");
    fs::write(root.join("bucket/obj.txt"), "hello from the served root\n").expect("seed");

    // Design §12: prove the probe address is reachable *before* asserting the
    // confinement blocks it. Without this, a machine with no route passes the
    // isolation check for the wrong reason.
    let reachable = TcpStream::connect(OUTBOUND_PROBE).is_ok();

    let Some(log) = run_child(&root, None, "read-write") else { return };

    assert!(log.contains("read-ok: hello from the served root"), "{log}");
    assert!(log.contains("host-etc-visible: false"), "{log}");
    assert!(log.contains("host-home-visible: false"), "{log}");
    assert!(log.contains("served-root-write: ALLOWED"), "{log}");
    assert!(log.contains("loopback-listen: true"), "{log}");
    if reachable {
        assert!(log.contains("outbound-connect: refused"), "{log}");
    } else {
        eprintln!("note: {OUTBOUND_PROBE} is unreachable outside the jail too — the outbound \
                   assertion is skipped rather than passed for the wrong reason");
    }
}

#[test]
fn a_read_only_jail_keeps_the_served_root_unwritable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    let state = tmp.path().join("state");
    fs::create_dir_all(root.join("bucket")).expect("create root");
    fs::create_dir_all(&state).expect("create state");
    fs::write(root.join("bucket/obj.txt"), "hello\n").expect("seed");

    let Some(log) = run_child(&root, Some(&state), "read-only") else { return };

    assert!(log.contains("read-ok: hello"), "{log}");
    assert!(log.contains("served-root-write: refused"), "{log}");
    assert!(log.contains("state-dir-write: ALLOWED"), "{log}");
    // The served root is untouched on the host too — the overlay's upper lives
    // in the jail's tmpfs, never in the data (design §4).
    assert!(!root.join("bucket/should-not-exist").exists());
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-linux --test jail`
Expected: FAIL on `served-root-write` or `host-etc-visible` until the mount table is right — the test compiles from Task 5 onward, so the failure is behavioural, not a compile error.

If the child reports `confine-failed: sandbox step \`unshare user namespace\` failed: … EINVAL`, the process was multi-threaded: check that this really is `src/bin/sandbox-child.rs` (a plain `fn main`) and not a re-executed libtest binary, and that no dependency has started a worker thread before `confine`.

**`EPERM` on the same step is a different failure with a different cause**: the kernel refused the user namespace itself — on Ubuntu 24.04+ that is usually `kernel.apparmor_restrict_unprivileged_userns=1`, which the `Resolved::Unavailable` message names. On a host that forbids it outright the test never gets this far: `host_can_confine` asks `unshare(1)` first and skips, so a child that still fails here means the two answers disagree — and that is a finding rather than a skip, because the AppArmor restriction is attached to the *binary being confined*, so a host can grant the probe and refuse the jail. The assertion is there to make that difference readable instead of filtered.

- [ ] **Step 4: Make it pass**

If `host-etc-visible` is `true`, `/oldroot` was not detached: check the `umount("/.oldroot", UnmountFlags::DETACH)` and that the working directory moved off the old root before it — a process whose cwd is inside the old root pins it.

If `outbound-connect` succeeded, the seccomp filter is not installed: check that `seccomp::install()` is the last call in `enter` and that `Arch::current()` returned `Some`.

If the child fails on the first `mkdir` with `EOVERFLOW`, the gid map was not written: an unmapped gid makes file creation fail even though `mount` and `pivot_root` succeed.

If the child reports `create jail root in $TMPDIR` with `EACCES`, `EROFS` or `ENOSPC`, the temp directory is the problem and the exposures are not: the jail is created under `std::env::temp_dir()` — `TMPDIR` unless `LinuxOptions::with_tmpdir` says otherwise, and **not** under the test's own `tempfile::tempdir()`, which is the served root — so a read-only, missing or full one fails before any mount exists. Point `TMPDIR` at a directory the child can write, or pass `with_tmpdir`.

If a `mount --bind` fails with `EINVAL`, a bare `MS_BIND` reached a mount-point source: check that `apply_mount` calls `mount_bind_recursive` and not `mount_bind`.

If `state-dir-write` is refused, the state directory was not exposed, or the overlay's read-only mount covered it — the state directory must never be overlaid (Task 2 pins this).

- [ ] **Step 5: Verify under both models**

Run:
```bash
TINIO_SANDBOX_MODEL=userns cargo test -p tinio-sandbox-linux --test jail
sudo -E TINIO_SANDBOX_MODEL=root cargo test -p tinio-sandbox-linux --test jail
```
Expected: PASS both times. The unprivileged run exercises the user-namespace model and the overlay read-only path; the `sudo` run exercises the root model and the remount read-only path — **for the filesystem reason the paragraph above the child gives, which is also where the fallback branch gets its end-to-end coverage**: the served root is a tempdir on a tmpfs (WSL2's `/tmp` is one, and Task 9 records the measurement), an unprivileged user namespace cannot remount a tmpfs bind read-only, and so the `userns` leg is the one run in the suite that takes `apply_mount`'s `EPERM` branch and mounts the overlay — which is the very configuration design §4's evidence table measured working, `upperdir` on the namespace-owned tmpfs and the lower left byte-for-byte untouched. No unit test can do that — the branch needs a filesystem, not a fixture (Task 5's test pins the errno reading instead). **The two runs are guaranteed to differ because the child reads the variable rather than discovering a model for itself** — without the second line, a `sudo` leg on a userns-capable host would take the same path as the first and the remount would go untested (the paragraph above the child says so in full). Both must pass — this is P1's acceptance gate (design §12).

---

### Task 7: Restructure `serve` and gate it behind the feature

**Files:**
- Modify: `crates/tinio-server/Cargo.toml`
- Modify: `crates/tinio-server/examples/serve.rs`

**Interfaces:**
- Consumes: `tinio_sandbox::{AccessMode, Backend, LaunchPlan, Sandbox, platform}`.
- Produces: nothing the library uses.

`unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process (verified), and `#[tokio::main]` builds its runtime inside `main`. The entry point therefore becomes a plain `fn main` that confines first and builds the runtime second (design §10).

- [ ] **Step 1: Add the feature and the example gate**

`crates/tinio-server/Cargo.toml` — add to `[dependencies]`, alphabetically after `tinio-core.workspace = true`:

```toml
tinio-sandbox.workspace = true
```

**Not `optional`, and that is decision 4 taken literally rather than a convenience.** Decision 4 says "default-on means compiled in, not enabled": the sandbox code is in every build and only *running* it is opt-in. It also keeps decision 19 consistent — `serve` emits its readiness frame on stdout in every build, including one without the feature, and `Event` comes from this crate. An `optional` dependency would put the readiness line itself behind the feature, so a `--no-default-features` build could not serve the harness at all.

Add to `[features]` a feature that gates **behaviour**, not compilation:

```toml
sandbox = []
```

Add `"sandbox"` to the `default` list, and add:

```toml
[[example]]
name = "serve"
required-features = ["sandbox"]
```

Then the backend itself, in a target-gated table of its own:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
tinio-sandbox-linux.workspace = true
```

**Why a second dependency rather than something on the facade.** `serve` needs `LinuxOptions` — the model and the `tmpdir` knob are platform options, deliberately outside P0's shared surface — and the facade re-exports only `Backend as Platform` and `backend as platform` (P0). Naming the platform crate directly is what keeps a Linux-only options type out of the shared layer, and the target gate is the shape `tinio-fs` already uses for its own `rustix`. It only spares the *consumers* on other targets; `tinio-sandbox-linux` is still a workspace member everywhere and carries its own `#![cfg(target_os = "…")]` gate for that (P0).

- [ ] **Step 2: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` at the bottom of `crates/tinio-server/examples/serve.rs`:

```rust
#[test]
fn exposures_carry_the_mode_and_skip_an_in_root_state_dir() {
    let root = Path::new("/srv/data");
    let inside = Path::new("/srv/data/.tinio");
    let outside = Path::new("/home/u/.tinio/roots/h");

    let exposures = sandbox_exposures(root, inside, AccessMode::ReadWrite);
    assert_eq!(exposures.len(), 1, "an in-root state dir is already covered by the root");
    assert_eq!(exposures[0].host, root);
    assert_eq!(exposures[0].access, AccessMode::ReadWrite);

    let exposures = sandbox_exposures(root, outside, AccessMode::ReadOnly);
    assert_eq!(exposures.len(), 2, "an out-of-root state dir needs its own entry");
    assert_eq!(exposures[0].access, AccessMode::ReadOnly);
    assert_eq!(exposures[1].access, AccessMode::ReadWrite, "the state dir is always writable");
}

#[test]
fn parse_args_recognises_the_sandbox_flag() {
    // The flag and the root path are both bare arguments, so a parser that
    // stops looking at position one would serve `--sandbox` as a directory
    // name — a failure that is invisible until someone reads their filesystem.
    let args = parse_args([OsString::from("/srv/data"), OsString::from("--sandbox")]).unwrap();
    assert_eq!(args.root, "/srv/data");
    assert!(args.sandbox);

    let args = parse_args([OsString::from("/srv/data")]).unwrap();
    assert_eq!(args.root, "/srv/data");
    assert!(!args.sandbox);

    // The flag is recognised wherever it appears, not only after the root.
    let args = parse_args([OsString::from("--sandbox"), OsString::from("/srv/data")]).unwrap();
    assert_eq!(args.root, "/srv/data");
    assert!(args.sandbox);
}

#[test]
fn a_missing_root_is_a_usage_error() {
    assert!(parse_args([OsString::from("--sandbox")]).is_err());
}

#[test]
fn the_sandbox_asks_for_the_device_files() {
    // `serve` builds its `Sandbox` by hand rather than through
    // `sandbox_exposures` (that helper answers "which paths", and the device
    // files are not paths the operator gave). The two calls have to be checked
    // together or `.device_files()` can be dropped without a test noticing.
    let sandbox = Sandbox::new()
        .expose(Path::new("/srv/data"), AccessMode::ReadWrite)
        .device_files();
    assert!(
        sandbox.device_files_enabled(),
        "the Linux jail needs /dev/null and friends (design decision 12)"
    );
    assert_eq!(sandbox.exposures().len(), 1);
}

#[test]
fn the_executable_directory_is_not_exposed_on_linux() {
    // Linux confines the process in place, so its image is already mapped and
    // needs no exposure (design decision 22).
    let exe_dir = std::env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let exposures = sandbox_exposures(Path::new("/srv/data"), Path::new("/srv/data"), AccessMode::ReadWrite);
    assert!(!exposures.iter().any(|e| e.host == exe_dir));
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-server --example serve`
Expected: FAIL — `cannot find function 'sandbox_exposures'`.

- [ ] **Step 4: Write the helper**

In `crates/tinio-server/examples/serve.rs`, add near the other helpers:

```rust
/// The paths the confined server must reach. The served root carries the
/// access the mode dictates; the state directory is always read-write, because
/// the access log, the redb metadata and the staging areas live there. An
/// in-root state directory needs no entry of its own — the served root's
/// exposure already covers it (design §9).
#[cfg(feature = "sandbox")]
fn sandbox_exposures(root: &Path, state_dir: &Path, root_access: AccessMode) -> Vec<Exposure> {
    let mut sandbox = Sandbox::new().expose(root, root_access);
    if !state_dir.starts_with(root) {
        sandbox = sandbox.expose(state_dir, AccessMode::ReadWrite);
    }
    sandbox.exposures().to_vec()
}
```

with, at the top of the file:

```rust
use std::ffi::OsString;
use std::io::{Write as _, stdout};

use tinio_sandbox::{AccessMode, Confinement, Event, Exposure, LaunchPlan, Sandbox, supervise};
use tokio::sync::watch;
```

`OsString`, the `std::io` line, the `tinio_sandbox` line and the `watch` import are additions; `std::process` is **not** — the file already imports it in its grouped `use std::{…, process, …}` and `usage()` calls `process::exit(2)`, so a second `use std::process;` would be a duplicate-import error rather than a convenience. Merge the new names into the file's existing groups if you keep them grouped; the `io` line is what `emit` writes and flushes the frame with — it is the frame writer's only dependency, which is why it stays a two-segment call — and `watch` is the signature's last parameter — this phase only *names* the type, because the channel is the launcher platforms' to create (and tokio's `sync` feature is already in this crate's feature list, so it is an import rather than a manifest change).

**Unconditional, because the dependency is.** `Event` is what `serve` writes to stdout in *every* build (decision 19), so a `#[cfg(feature = "sandbox")]` here would take the readiness frame away from the `--no-default-features` build and leave the harness unable to see the server come up. The feature gates behaviour inside the sandbox arm; it no longer gates whether the crate is linkable.

- [ ] **Step 5: Replace `#[tokio::main]` with the two-phase entry point**

Change the attribute to `fn main() -> Result<(), Box<dyn Error + Send + Sync>>` and split the body.

**This step is written as invariants, not as a diff to apply blind.** The read-only work (`--read-only`, `TINIO_READ_ONLY`, `PlaneConfig`, `AccessMode`, the state-directory relocation) has **landed** — commit `a46a1d1` — and it is now part of the file this restructure has to carry across, not a branch it merges with. So the shapes below are the *seams*: each one has a contract, and the code shows the shape a seam must end up in rather than the exact bytes at today's line numbers. Re-read the file before editing, and keep every one of read-only's decisions intact.

Five invariants, in the order they must hold:

1. **No thread exists before `confine`** — the entry point is a plain `fn main` and the runtime is built afterwards (design §10, decision 26).
2. **Everything that touches a host path happens before the door closes**: the argument parse, `Config::load`, the read-only resolution, `fs::create_dir_all(root)` where the mode allows it, `identity::ensure_chown_privilege`, the state-directory resolution, the `FsStorage` whose state directory is one of the exposures, and — one step further than the rest of this list — **the log subscriber**. **The split is not where it used to be** — read-only moved it: `let pipeline_config = …` is now `serve.rs:238`, and `FsStorage::new` comes *after* it at `serve.rs:243-257`, because the storage's `state_dir` and `access` are resolved just above. In today's file the boundary is therefore **down to and including `subscriber::set_global_default`**, and everything from `storage.set_max_concurrent_uploads(…)` onward is unchanged and runs *inside* the jail.
3. **`--sandbox` is never silently ignored** (decision 5): a build without the feature refuses it, and so does a build for a platform whose backend another plan owns.
4. **stdout carries events and nothing else** (decision 19): the endpoint line becomes one flushed SSE frame, and every log stays on stderr.
5. **The check that needs a privilege runs before the privilege is dropped** — `ensure_chown_privilege` probes for `CAP_CHOWN`, which the confinement is about to remove.

**The read-only work has landed, and every part of it has to survive this restructure**: the `--read-only` flag and its place in the usage text, `TINIO_READ_ONLY` through `sources::env_read_only`, the `AccessMode` it resolves, the **two `if !read_only` guards** (the root is not created and the chown probe does not run in read-only mode — the probe writes into the served root, which is the one thing that mode promises to leave alone), the state-directory relocation through `state::env_state_dir` and `state::default_home_state_dir`, and `plane_for`/`Capabilities` → `PlaneConfig`. The state directory is read from `storage.state_dir()` below precisely so that relocation keeps working.

**There is no `Args` struct to extend, and that is worth stating rather than discovering.** Today's file parses its arguments as locals in `main` (`root`, `address`, `config_path`, `read_only_flag`) inside a `while let Some(arg) = args.next()` loop, so `Args` is *created* by this step and gains a field for each of those four plus `sandbox` — one of them, `read_only`, being read-only's.

```rust
fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = parse_args(std::env::args_os().skip(1))?;

    // A build without the feature refuses the request rather than ignoring it
    // (design decision 5): silently running unconfined is the failure this
    // whole design exists to prevent. `serve` is declared with
    // `required-features = ["sandbox"]`, so *this* example cannot be produced
    // without the feature — the arm is here because the same `main` is the one
    // the CLI will carry, where the flag and the build do collide. **The refusal
    // is a frame as well as a line on stderr**: stdout is the protocol channel
    // in every build, feature or no feature (decision 19), so a consumer that
    // asked for a confinement hears why it did not get one.
    #[cfg(not(feature = "sandbox"))]
    if args.sandbox {
        let message = "error: this build was compiled without the `sandbox` feature, so \
                       --sandbox cannot be honoured; rebuild with the feature or drop the flag";
        eprintln!("{message}");
        emit_error(message);
        process::exit(2);
    }

    // The other half of decision 5: a platform whose backend belongs to another
    // plan refuses the flag rather than accepting it and confining nothing. P2
    // and P3 replace this arm with theirs and keep the frame — the refusal is
    // the same statement on every platform, and so is the place a consumer reads
    // it.
    #[cfg(all(feature = "sandbox", not(target_os = "linux")))]
    if args.sandbox {
        let message = "error: --sandbox has no backend on this platform yet; the Linux one is \
                       the one that has landed";
        eprintln!("{message}");
        emit_error(message);
        process::exit(2);
    }

    // Everything that must happen before the process is confined: the bind
    // sources have to exist, the configuration and the storage are read from
    // host paths, and no thread may exist yet — `unshare(CLONE_NEWUSER)`
    // returns EINVAL in a multi-threaded process, and a seccomp filter
    // installed before the runtime starts needs no cross-thread
    // synchronisation (design §10).
    let config = match &args.config_path {
        Some(path) => Some(Config::load(path)?),
        None => None,
    };

    // FR-023's resolution, kept exactly as it is today and in the same order:
    // the file's `[server] read_only`, overlaid by `TINIO_READ_ONLY` and then by
    // the flag. `access` is what `FsOptions` and `CleanupOptions` take, and it is
    // also what the exposure set needs — a read-only root is exposed `ReadOnly`
    // and only the state directory stays writable.
    let file_read_only = config.as_ref().is_some_and(|config| config.server.read_only);
    let read_only = sources::env_read_only()?.unwrap_or(file_read_only) || args.read_only;
    let access = AccessMode::from(read_only);

    // Read-only mode serves a root that must already exist: creating it would
    // write the one thing the mode promises to leave alone (FR-023, landed).
    if !read_only {
        fs::create_dir_all(&args.root)?;
    }
    // The probe writes `<root>/.tinio-chown-probe-<nonce>/probe` into the served
    // root, so it is skipped in read-only mode for the same reason the line
    // above is (FR-023).
    #[cfg(unix)]
    if !read_only {
        identity::ensure_chown_privilege(
            config.as_ref().unwrap_or(&Config::default()),
            Path::new(&args.root),
        )?;
    }

    // The `FsStorage::new(root, FsOptions { … })` call the body makes today
    // (`serve.rs:243-257`) moves here — **the options literal unchanged**, and
    // the root as `&args.root`, because the root is the parsed `PathBuf` now
    // rather than a local `String` — and `storage` is passed into `serve`
    // instead of being built there. Its state directory is one of the paths the
    // jail must expose, and asking the storage for it is what keeps that true
    // now that the state can live outside the served root: read-only mode
    // relocates it under `~/.tinio/roots/<hash>/` (`TINIO_STATE_DIR` wins when
    // set), so a hand-built `<root>/.tinio` would expose the wrong path and
    // leave the confined server unable to write its own state.
    let storage = FsStorage::new(&args.root, /* the options literal, verbatim from the body */)?;

    // **The log subscriber is installed here, one step earlier than the body
    // had it, and that is the whole reason the events this phase adds are
    // visible.** `confine` emits them — the jail's path, the model it resolved,
    // every mount, and an `error!` for whatever refused — and `tracing` drops
    // an event emitted while no subscriber exists. Left where it was, in the
    // moved body below, every one of them would go into a process with nowhere
    // to put them, and the failure an operator most needs a log for (a jail
    // that did not build) would be exactly the one with no line in it. Its two
    // neighbours move with it unchanged: the state directory has to exist
    // before the access log inside it is opened.
    fs::create_dir_all(storage.state_dir())?;
    let subscriber = server_log::build_subscriber(/* the five arguments, verbatim from the body */)?;
    subscriber::set_global_default(subscriber)?;

    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    if args.sandbox {
        // The **resolved** mode and not the configuration file's own field:
        // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
        // `access` above, so reading `config.server.read_only` here would expose
        // the served root `ReadWrite` on a run the operator asked to be
        // read-only — losing the kernel-level half of FR-023 that design §9
        // promises, and doing it silently.
        // No conversion, and nothing left to keep in step: `access` **is** the
        // type the exposure set takes. This line used to rebuild an `Access`
        // from it, because the sandbox owned a second enum for the same fact.
        let root_access = access;

        // One call decides the whole exposure set — and the device-file flag is
        // an argument rather than something the rebuilt set remembers, so it is
        // passed here rather than re-applied afterwards.
        let sandbox = Sandbox::from_exposures(
            sandbox_exposures(&args.root, storage.state_dir(), root_access),
            true,
        );

        // The model comes from the environment in P1 (`TINIO_SANDBOX_MODEL`);
        // the CLI carries it as a flag when it owns `serve` (design §1). It
        // reaches the backend through `LinuxOptions`, which is why this arm
        // names the Linux crate directly instead of going through the facade's
        // `platform()` — the facade returns whatever backend the build
        // targets, and an options value only one of them has cannot be handed
        // to it. Each launcher platform's own arm does the same with its own
        // options type.
        // **Every refusal this arm produces on Linux goes to stdout as an
        // `Error` frame, in the same shape both launcher platforms use**
        // (decision 19's "a startup failure is an event rather than an
        // absence"). P1 had none of them until this pass, for a reason that
        // turned out to be wrong: Linux confines **in place**, so there is no
        // child whose frames a parent relays, and the `Error` frame read as the
        // launcher platforms' business. It is not theirs. A consumer of stdout
        // must not have to know which platform it is on — on a launcher a
        // failure arrives as a frame and on Linux it arrived as nothing at all,
        // which a harness reads as a timeout rather than as a cause. The two
        // refusals below are the ones this arm can produce here: the model
        // variable naming something that is not a model, and `confine` refusing
        // — the unavailable-model refusal among its reasons, which is the one an
        // operator is most likely to meet. (The `Supervise` arm's own `?` is the
        // third call that can fail, and it is unreachable on this platform by
        // construction.) Each also logs where its `Error` is built, so stderr
        // carries the same message through the subscriber installed above
        // (shared §7).
        let options = match tinio_sandbox_linux::LinuxOptions::from_env() {
            Ok(options) => options,
            Err(err) => {
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };
        let mut backend = tinio_sandbox_linux::Backend::with_options(options);
        // A refusal here is a startup failure like the launcher platforms'
        // (`confine_or_launch` is the Windows spelling of the same call site):
        // the frame goes out before the `Error` does, because a caller that
        // returns without a frame leaves its consumer waiting for a `Listening`
        // that can never come.
        let confinement = match backend.confine(&sandbox, &LaunchPlan::new()) {
            Ok(confinement) => confinement,
            Err(err) => {
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };
        match confinement {
            Confinement::InPlace => {
                // The one degradable step says so rather than passing quietly
                // (design §11). **Both statements are kept, because they are
                // not the same statement**: the backend emitted a `warn!` event
                // for the degradation through the subscriber installed above,
                // which is what puts it in the log, and this line is `serve`'s
                // own — read by whoever is watching a startup rather than a
                // log, written here because a library cannot assume a
                // subscriber exists. Every log goes to stderr; stdout is the
                // event channel below.
                if backend.seccomp() == Some(tinio_sandbox_linux::Seccomp::SkippedUnsupportedArch) {
                    eprintln!(
                        "warning: this architecture has no seccomp table, so outbound \
                         connections are NOT blocked; the directory confinement still holds"
                    );
                }
            }
            Confinement::Supervise(child) => {
                // Unreachable on Linux by construction, kept so the call site is
                // platform-neutral; the launcher plans own its real behaviour.
                // The shape is theirs too: `supervise` borrows the child, and
                // the terminal read that follows is what tells a child which
                // never reported from one that is merely quiet (decision 19).
                let mut child = child;
                let status = supervise(&mut child, || ())?;
                if child.terminal().is_none() {
                    eprintln!("sandbox: the confined child exited without reporting readiness");
                }
                process::exit(status.code().unwrap_or(1));
            }
        }
    }

    // Only now may threads exist. The two `None`s are Linux's half of decision
    // 6 read back out of the signature: this process bound its own listener
    // rather than adopting a child's, and nothing forwards a stop to a process
    // that is not a child.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(serve(args, config, storage, None, None))
}
```

Two helpers carry the frames, and they belong to `serve.rs` rather than to the backend — the same rule as "a library installs no subscriber" (shared §7) seen from the other side: a library does not write to a process's stdout either. They are the file's only copies: all three platforms' arms live in this one `main`, so a second `emit` beside this one is `E0428`, and the launcher phases call these rather than defining their own (P2 and P3 each carry a copy of `emit` in their own task text today; theirs is deleted when this phase lands, which is the correction `serve`'s signature needed for the same reason — shared §1's rule that a fact two plans could state has one owner).

```rust
/// Write one event frame to stdout and flush it.
///
/// **The only write to stdout in this process** (design decision 19), and one
/// function rather than one per call site: the readiness frame below and the
/// `Error` frames a refusal sends both go through it, so "stdout carries events
/// and nothing else" is a property of one body instead of a rule to re-check.
/// Flushing is not decoration — stdout is line-buffered to a terminal and
/// block-buffered to a pipe, and a harness reads a pipe, so an unflushed frame
/// is a readiness hang rather than a missing event. A failed write is ignored on
/// purpose: it means the reader is gone, and there is nowhere left to report it.
fn emit(event: &Event) {
    let mut out = stdout().lock();
    let _ = out.write_all(event.encode().as_bytes());
    let _ = out.flush();
}

/// A refusal in the sandbox arm, on stdout, **before** its `Err` is returned.
///
/// Decision 19's "a startup failure is an event rather than an absence", which
/// this arm did not honour: a consumer of this stream waits for a frame, and
/// without one it reports a timeout where the launcher platforms report the
/// cause. Both of them send this same frame through a helper of this same name,
/// and the message is the `Error`'s own rendering, so stdout and stderr say the
/// same thing about the same refusal.
fn emit_error(message: &str) {
    emit(&Event::Error { message: message.to_string() });
}
```

**What the subscriber shows is a level, not a missing event, and that is the honest limit of this phase's observability.** `build_subscriber` is handed `Verbosity::Info` and filters on that level rather than on an environment variable, so the backend's per-operation `debug!` events sit below it and stay off stderr until the `[log]` wiring lands with the CLI. The two that matter most are above it and arrive on every run: the `warn!` for a skipped seccomp filter, and the `error!` that every refusal logs where its `Error` is built. The crate emits shared §7's events either way; the sink's level decides which of them an operator reads.

Extract the existing hand-rolled argument loop into `fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Args, Box<dyn Error + Send + Sync>>` with `struct Args { root: PathBuf, address: SocketAddr, config_path: Option<PathBuf>, read_only: bool, sandbox: bool }` — **`read_only` is read-only's field and must not be dropped**, since `--read-only` is one of the arms today's loop already matches — and extend `usage()`:

**`root` is a `PathBuf`, not a `String`.** The parser reads `OsString`s because a path that is not valid UTF-8 is legal on Linux, and forcing it into a `String` at the boundary would refuse a served root for a reason the operator cannot see; `fs::create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway. Match the *flags* through `arg.to_str()` — a flag that is not UTF-8 is a usage error, while the values after it stay `OsString`s — rather than the current `arg.as_str()`.

```rust
eprintln!("usage: serve <root> [--port N] [--address HOST:PORT] [--config <config.toml>] [--read-only] [--sandbox]");
```

**The iterator parameter is not decoration.** It is what lets the unit tests above feed a fabricated `argv` instead of mutating the test process's own arguments, and it is the shape the launcher platforms already assume — their `main` parses the arguments left after the listener element is filtered out, so a `parse_args()` that read `std::env::args()` itself could not be used there at all.

**Move the rest of the existing body into `serve`, whose signature is this plan's and which the other two plans consume unchanged.** It is introduced here because this task is what restructures the entry point, and P2 and P3 fill the two optional parameters from their own arms of this `main`:

```rust
async fn serve(
    args: Args,
    config: Option<Config>,
    mut storage: FsStorage,
    inherited: Option<TcpListener>,
    stop: Option<watch::Receiver<bool>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // `storage` is **not** optional, and an earlier version of this signature
    // made it so and had to be corrected: every caller that reaches `serve`
    // serves from a storage, so `None` had no meaning any platform could agree
    // on — this plan read it as a caller bug and errored, while P2 read it as
    // "the body builds one", which is not the same function. Linux's `main`
    // builds it before `confine` (the bind sources must exist and the state
    // directory is one of the jail's mounts) and a launcher's child builds it
    // in `confined`; the launcher itself never reaches `serve` at all.
    //
    // `mut` because the very next call — `set_max_concurrent_uploads(…)`, which
    // is where the moved body starts — takes `&mut self`.

    // The stop channel belongs to the launcher platforms: the child is what
    // reacts to a forwarded stop, so their `confined` creates the channel,
    // gives the sender to `on_stop` and hands the receiver here. Nothing
    // forwards a stop to a process that is not a child — Linux passes `None` —
    // so this phase holds it and does nothing with it, and taking it now is
    // what lets P2 and P3 add their accept-loop arm without touching this
    // signature.
    let _ = stop;

    // ... `storage.set_max_concurrent_uploads(…)` onward, unchanged, including
    //     the endpoint line below.
}
```

**Why two of the five parameters are optional, stated once.** They are optional because the three platforms reach `serve` differently, and each `None` is a fact about a platform rather than a policy of `serve`. **Linux** serves in the process that confined itself, so its `main` builds the storage before `confine` — the bind sources must exist and the state directory is one of the exposures — and calls `serve(args, config, storage, None, None)`: there is no adopted listener, because this process binds its own, and no stop channel, because nothing forwards a signal to something that is not a child. **The launcher platforms** do not serve in the launcher at all on the sandbox path: their `main` builds no storage there — it resolves only the state directory *path*, which is what the exposure set needs — and their `confined()` builds the storage in the child and calls `serve(args, config, storage, Some(listener), Some(stop))`. **`storage` is deliberately not one of the two optional parameters**: every caller that reaches `serve` serves from a storage, so an optional one would have two readings and only one of them could be right — which is exactly what happened before this was corrected.

**`stop` is created in the child and never crosses the process boundary.** The child is what reacts to a forwarded stop, so `confined` makes the `watch` channel there, hands the sender to `on_stop`, and passes the receiver to `serve`. A channel that had to be carried to the other side of an `execve` would be a descriptor and a rebuild on arrival, which is the machinery this shape exists to avoid.

**A launcher must not construct an `FsStorage` on the sandbox path, and that is why its side resolves a path instead.** (On the plain path — the same `main` with no `--sandbox` — it does build one, because there it *is* the server; the rule is about the launcher/child pair, not about the process.) Constructing one *opens* the state database, and the child opens the same database in the same directory: two processes holding one state directory is not something the storage layer is written for — its exclusive-handle and compaction assumptions are exactly what such a pair breaks — so the launcher resolves the state directory as a `PathBuf` for the exposure set and lets the child own the only handle. It is also why that side builds its exposure set from a path rather than reading one back out of a storage.

Everything before the moved body has moved into `main` and must not appear twice: the read-only resolution, the guarded `create_dir_all` and `ensure_chown_privilege`, the state-directory resolution, `Config::load`, the storage construction — which today sits at `serve.rs:243-257`, *after* the `let pipeline_config = …` at `serve.rs:238` that the earlier draft of this plan used as the boundary — and the log subscriber, which moves ahead of `confine` for the reason given in the code above. The one exception *inside* the moved body is the endpoint line, which is the whole of decision 19 on this platform:

The body's endpoint line becomes an event, and it is also where `inherited` is used — the launcher platforms' children take the listener their parent had already bound, and this process binds its own, which is the only arm that exists in this phase:

```rust
    let listener = match inherited {
        Some(listener) => listener,
        None => TcpListener::bind(args.address).await?,
    };
    // stdout is a protocol channel; every log goes to stderr. `emit` above is
    // the only writer, which is what makes the contract cheap to hold (design
    // decision 19) — and this is the serving process's own frame, beside the
    // `Error` frames a refusal sent through the same call before it got here.
    // `local_addr()` is already a `SocketAddr`, so the port travels as a value
    // rather than as something re-parsed from text.
    emit(&Event::Listening { addr: listener.local_addr()? });
```

replacing the existing `println!("listening on {}", listener.local_addr()?)`. Flushing matters: stdout is line-buffered to a terminal and block-buffered to a pipe, and the harness reads a pipe, so an unflushed frame is a readiness hang rather than a missing event — which is `emit`'s job now, and the reason the endpoint line goes through the same call as a refusal's `Error` frame instead of writing its own.

Then verify the exclusivity claim rather than assuming it, because everything else in this contract rests on it:

```bash
rg -n --pcre2 '(?<!e)println!|(?<!e)print!|stdout\(\)' crates/tinio-server/src crates/tinio-server/examples/serve.rs
```

**The negative lookbehind is load-bearing**: `println!` is a substring of `eprintln!`, so the obvious pattern reports every diagnostic the read-only work added to stderr — `serve.rs:66`'s usage text and the two state-directory notes around `serve.rs:221` and `serve.rs:230` — and hides the one line this step is actually asserting.

**Every hit must be inside `emit`, and there is exactly one of them.** The endpoint line this step replaces was the file's only stdout writer when the step was written (`serve.rs:310`, `println!("listening on {}", listener.local_addr()?)`), and it is now a caller of `emit` rather than a writer — so the pair of lines that used to be the hit (`print!` plus `stdout().flush()`) is gone, and the `stdout()` that remains is the one inside `emit`'s body. A `print!` or `println!` hit anywhere else is a decision-19 bug, and `tinio-server/src` must have none at all: its logging is tracing-based and already writes to stderr or to the access-log file. The `Confirmation::Supervise` arm is a no-op on Linux by construction and exists so the call site is platform-neutral; the two launcher plans own its real behaviour.

- [ ] **Step 6: Run the tests and the example**

Run:
```bash
cargo test -p tinio-server --example serve
cargo build -p tinio-server --example serve
cargo run -p tinio-server --example serve -- /tmp/tinio-smoke --port 0
```
Expected: the tests pass; the binary builds; the run prints a single SSE frame — `event: listening` and `data: {"addr":"127.0.0.1:<port>"}`, closed by a blank line — and serves. Stop it with Ctrl-C.

- [ ] **Step 7: Run it confined**

Run: `cargo run -p tinio-server --example serve -- /tmp/tinio-jailed --port 0 --sandbox`
Expected: the same single `event: listening` / `data: {"addr":…}` frame, and a second terminal can `aws s3 mb` against it. This is the feature working end to end by hand.

Then confirm a bad exposure fails closed. The case has to be one the *validation* refuses, and a relative root is the readable one:

Run: `cargo run -p tinio-server --example serve -- ./not-absolute --port 0 --sandbox`
Expected: exit non-zero; stdout carries one `event: error` frame whose message is ``path `./not-absolute` cannot be exposed: not absolute``, and stderr the same failure in the runtime's `Error: …` rendering — `main` returns the boxed error rather than printing it, so the `Display` text an operator reads is the one in the frame (decision 19). The run leaves the directory it created for the root behind, and the state directory inside it.

**The spelling that looks obvious is the one that cannot reach this check, so it is worth saying why nobody should put it back.** `serve /tmp/does-not-exist-xyz --sandbox` was the earlier version of this step, and it cannot fail: whenever the mode is not read-only, `main` runs `fs::create_dir_all(&root)` before the sandbox arm (invariant 2 above — the bind sources have to exist before the door closes), so the very path the exposure check would reject is *created* first, the exposure then exists, and the run confines successfully. A path that exists but cannot be expressed is what the check is for, and `mirror_under` refuses a relative one with `not absolute` after every earlier step — the root creation, the chown probe, the storage — has passed on it. (Task 5's `a_relative_exposure_is_refused_before_anything_irreversible_happens` is the same refusal at the unit level; this run is the operator-visible half.)

- [ ] **Step 8: Verify the feature-off build still works**

Run:
```bash
cargo check -p tinio-server --no-default-features
cargo check -p tinio-server --no-default-features --features multipart,copy,list-v1,list-v2,cors,select,acl
```
Expected: both succeed. The example is skipped by `required-features` in the first.

---

### Task 8: The readiness contract — the harness migration and the end-to-end test

**Files:**
- Create: `crates/tinio-server/tests/sandbox_e2e.rs`
- Modify: `crates/tinio-server/Cargo.toml` (the test target)
- Modify: `crates/tinio-e2e/tests/steps/clients.rs` (the readiness scrape)
- Modify: `crates/tinio-server/tests/serve_read_only.rs` (the second readiness scrape — see below)
- Modify: any cucumber feature that asserts readiness

**Interfaces:**
- Consumes: the built `serve` example, resolved from the test binary's own path; `tinio_sandbox::Event`.
- Produces: nothing.

**This task touches shipped test code, and that is deliberate.** `crates/tinio-e2e/tests/steps/clients.rs` holds `const READY: &str = "listening on ";` and a `wait_for_ready` that reads the child's stdout until it finds it. Decision 19 replaces that scrape with the event stream, so the harness has to move with it — in a task that says so, rather than as a side effect of a sandbox change. The harness migration is not optional and not deferrable: leaving it would mean the e2e suite and the sandbox tests disagree about what readiness looks like, and the cucumber run would fail on every scenario the moment the endpoint line changes.

**There are two such readers, not one, and the second arrived with the read-only work.** `crates/tinio-server/tests/serve_read_only.rs` carries its own `const READY: &str = "listening on ";` at line 66 and a `Serve::ready()` that strips the prefix and parses the remainder as a `SocketAddr`. It is an auto-discovered integration test that CI's `test-port` leg runs (`run-test-leg` invokes `cargo nextest run --workspace --exclude tinio-e2e`), so the endpoint change breaks it exactly as it breaks `clients.rs` — and it is fixed in the same commit rather than discovered by a red CI run. This is the plan that changes the endpoint, so it is the plan that carries both.

- [ ] **Step 1: Write the test**

```rust
//! The confined server, driven the way the interop harness drives it: spawn
//! the example, read the endpoint it prints, and talk to it.

#![cfg(target_os = "linux")]
#![cfg(feature = "sandbox")]

use std::env;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tinio_sandbox_linux::Mechanism;

/// The `serve` example, resolved from this test binary's own location:
/// `<target>/<profile>/deps/<test>` → the sibling `examples/` dir.
///
/// **Not `assert_cmd::cargo::cargo_bin`, and not `env!("CARGO_BIN_EXE_serve")`.**
/// Cargo sets `CARGO_BIN_EXE_*` for binary targets only — naming an example in
/// `env!(…)` is a compile error — and `assert_cmd`'s helper resolves the same
/// set, so it would not find an example either; the crate is not a dependency
/// of this workspace for that reason (the tech stack says so once). The e2e
/// harness resolves the example the same way this does, for the same reason
/// (`crates/tinio-e2e/tests/steps/clients.rs`).
fn serve_bin() -> PathBuf {
    let name = format!("serve{}", if cfg!(windows) { ".exe" } else { "" });
    let exe = env::current_exe().expect("current exe path");
    let profile_dir = exe
        .parent() // .../deps
        .and_then(Path::parent) // .../<profile>
        .expect("test binary lives in <target>/<profile>/deps");
    let path = profile_dir.join("examples").join(name);
    assert!(
        path.exists(),
        "serve example not found at {} — build it with the same profile first: \
         `cargo build -p tinio-server --example serve` (a filtered `cargo test --test …` \
         builds test targets only, not examples)",
        path.display()
    );
    path
}

/// The model the run asked for, read from the same variable the example reads,
/// so the precondition below is asked about the leg that is actually running.
fn requested_model() -> Mechanism {
    Mechanism::from_env(env::var("TINIO_SANDBOX_MODEL").ok().as_deref())
        .expect("TINIO_SANDBOX_MODEL must name a model")
}

/// Whether this host can confine under `model` — the same precondition as the
/// jail test's, and asked **before** the server is spawned.
///
/// The earlier shape of this test read the server's stderr *after* it failed to
/// print an endpoint and skipped on a substring ("sandbox unsupported",
/// "Operation not permitted"), which is a skip a broken confinement can
/// produce as easily as an unprivileged host: anything that makes `serve` die
/// early while mentioning permissions reports a green suite. The question has a
/// positive answer, so it is asked first and the failure becomes a failure.
///
/// Each variant asks for the namespaces it needs — `--user --map-root-user` for
/// the user-namespace model, a bare `--mount` for the root model, whose single
/// capability is `CAP_SYS_ADMIN` — and `Auto` takes either. The two legs of the
/// WSL2 script set `TINIO_SANDBOX_MODEL`, so `requested_model` is what the
/// server will resolve, not a guess about it.
fn host_can_confine(model: Mechanism) -> bool {
    let user = || unshare_probe(&["--user", "--map-root-user"]);
    let mount = || unshare_probe(&["--mount"]);
    match model {
        Mechanism::UserNamespace => user(),
        Mechanism::Root => mount(),
        Mechanism::Auto => user() || mount(),
    }
}

/// `unshare(1)` with `flags`, as a process of its own — the program the WSL2
/// script gates on, so the script and the test agree by construction.
///
/// A missing `unshare(1)` panics rather than skipping: a suite that cannot tell
/// "this host cannot confine" from "I could not ask" is the shape this function
/// exists to delete.
fn unshare_probe(flags: &[&str]) -> bool {
    Command::new("unshare")
        .args(flags)
        .arg("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or_else(|err| panic!("the precondition needs util-linux's `unshare(1)`: {err}"))
}

#[test]
fn a_confined_server_serves() {
    // The precondition, before anything is spawned: this host either can
    // confine under the model the run asked for or the test does not run at all
    // (design §12's "a suite of skips exits 0" in the negative).
    let model = requested_model();
    if !host_can_confine(model) {
        eprintln!("skipping: this host cannot confine under {model:?} — WSL2 covers both legs");
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");

    let mut child = Command::new(serve_bin())
        .args([root.to_str().unwrap(), "--port", "0", "--sandbox"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn serve");

    let stdout = child.stdout.take().expect("stdout");
    // A frame is **two** lines — `event:` then `data:` — closed by a blank line
    // (P0's `Event::encode`). Reading one line would hand `decode` half a frame.
    let mut reader = BufReader::new(stdout);
    let mut first = String::new();
    let mut second = String::new();
    reader.read_line(&mut first).expect("read the event line");
    reader.read_line(&mut second).expect("read the data line");

    if first.trim().is_empty() {
        // No precondition is left to blame, so this is a failure — and the
        // server's stderr is the evidence, because it now carries the
        // backend's own `error!` line for whatever refused.
        let mut stderr = String::new();
        child.stderr.take().expect("stderr").read_to_string(&mut stderr).expect("read stderr");
        panic!("serve printed no endpoint although this host can confine; stderr:\n{stderr}");
    }

    let event = tinio_sandbox::Event::decode(&format!("{first}{second}"))
        .unwrap_or_else(|| panic!("unexpected frame: {first:?} {second:?}"));
    let tinio_sandbox::Event::Listening { addr } = event else {
        // **An `Error` frame is a refusal with a cause attached**, and that is
        // the whole reason the Linux arm sends one (Task 7 Step 5): without it
        // the read above ends at EOF with nothing to show, and this test's
        // failure is the empty-frame branch above — a message about a missing
        // endpoint whose cause can be read only by scraping stderr. With the
        // frame, the harness fails with the confinement's own rendering, which
        // is what the launcher platforms' harnesses do.
        panic!("serve refused to start: {event:?}");
    };

    // A plain TCP connection proves the listener is reachable from outside the
    // jail. Anything more would re-test S3, which the other suites cover.
    // `addr` goes to `connect` as it is: the event's field is already a
    // `SocketAddr`, `SocketAddr` implements `ToSocketAddrs`, and that is the
    // point of the field being typed — there is no text in between that could
    // disagree with what was bound. (There is no `SocketAddr::as_str`.)
    let mut stream = TcpStream::connect(addr).expect("connect to the confined server");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    assert!(response.starts_with("HTTP/1.1 "), "{response}");

    child.kill().expect("kill");
    child.wait().expect("wait");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-server --test sandbox_e2e`
Expected: FAIL to compile until the target is declared, then PASS — or a printed `skipping:` on a host that cannot confine.

- [ ] **Step 3: Wire the test target**

`crates/tinio-server/Cargo.toml`:

```toml
[[test]]
name = "sandbox_e2e"
required-features = ["sandbox"]
```

- [ ] **Step 4: Migrate the e2e harness to the event stream**

`crates/tinio-e2e/tests/steps/clients.rs` currently reads the child's stdout until it finds `listening on `. Replace the scrape with a decode, keeping the same shape — read the stream, stop when an event says the listener is up:

```rust
/// Read the child's stdout until it reports the bound address.
///
/// The child speaks the sandbox's event stream (design decision 19): one SSE
/// frame per event, an `event:` line and a `data:` line, closed by a blank line.
/// **A frame is not a line**, so this accumulates until the blank line and
/// decodes the block — decoding line by line would hand `Event::decode` half a
/// frame, and searching for the old text would find nothing at all. Text that is
/// not a frame — a log a dependency wrote before the subscriber was installed,
/// say — does not decode and is skipped rather than fatal, which is the property
/// the framing buys and the reason this is not a `strip_prefix`.
fn wait_for_ready(stdout: impl Read + Send + 'static) -> Option<SocketAddr> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if line.trim_end().is_empty() {
                if let Some(tinio_sandbox::Event::Listening { addr }) =
                    tinio_sandbox::Event::decode(&frame)
                {
                    let _ = tx.send(addr);
                    return;
                }
                frame.clear();
            } else {
                frame.push_str(&line);
            }
            line.clear();
        }
    });
    rx.recv_timeout(START_TIMEOUT).ok()
}
```

**The address is a `SocketAddr` now, not a `String`.** That is the point of typing the field: `serve` gets it from `TcpListener::local_addr()` and the harness passes it straight to `TcpStream::connect`, with no text in between that could disagree with what was actually bound. Adjust the call site rather than re-formatting it back into a string — `spawn_child`'s `.trim().strip_prefix(READY)` disappears, and its timeout panic should name the event it waited for instead of the old text.

Delete `const READY` — a constant that exists to be searched for has no role once the line is structured. Add `tinio-sandbox` to `tinio-e2e`'s dependencies if it is not there; the harness needs the event type, not a copy of its spelling.

Then find every feature and step that asserts readiness and confirm it still holds:

```bash
rg -n "listening on|READY" crates/tinio-e2e/ crates/tinio-server/tests/
```

The expected residue is the harness's own doc comments. Anything else — a step definition matching the old text, a `.feature` quoting it, `serve_read_only.rs`'s own constant and waiter — changes here too.

**One near-miss worth knowing about, so nobody "fixes" it**: `crates/tinio-server/src/data.rs` has `tracing::info!("S3 data plane listening on {}", …)`. It is the same phrase on the *tracing* channel, which writes to stderr; it is not the protocol and not a second readiness line. The grep above is scoped to the two test directories for exactly that reason — widening it to all of `crates/` will find this line and it should be left alone.

- [ ] **Step 5: Run the e2e suite**

Run: `cargo test -p tinio-e2e --test cucumber`
Expected: PASS. This is the check that the harness migration is complete — the cucumber run spawns the real `serve` example and would hang on readiness if any scrape were left behind.

- [ ] **Step 6: Verify the skip path is legible**

Run the test on a host that cannot confine (or with `TINIO_SANDBOX_MODEL=root` as an unprivileged user, which forces the refusal).
Expected: a `skipping:` line on stderr and a pass — not a spurious failure, and not `#[ignore]`d, because an ignored test is a test nobody runs. **The skip happens at `host_can_confine`, before the server is spawned**, which is what makes it a statement about the host rather than about the server's ability to die quietly.

---

### Task 9: The WSL2 script and the documentation

**Files:**
- Create: `crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`
- Create: `docs/sandbox.md` (the operator-facing section)
- Modify: `docs/tests.md` (the WSL2 section)
- Modify: `docs/cargo.md` (the lint exception)
- Modify: `README.md` (one line, pointing at `docs/sandbox.md`)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing.

WSL2 is where this phase is verified (design §12): it has the kernel, unprivileged user namespaces and passwordless `sudo`, and the repository lives on `/mnt/e`, so the build must be redirected to ext4 exactly as the interop script does.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Verify the Linux sandbox inside WSL2, under both confinement models.
# Usage: bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh [cargo test args...]
set -euo pipefail
if [[ "$(uname -s)" != "Linux" ]]; then
  echo "wsl-chroot.sh needs Linux (run it inside WSL2)"; exit 1
fi
cd "$(dirname "$0")/../../.."   # the repository root, wherever this is invoked from
if [[ "$(pwd)" == /mnt/* ]]; then
  export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}"  # ext4 build artifacts
  echo "on /mnt — building to $CARGO_TARGET_DIR"
fi

# The confinement tests *skip* on a host that cannot confine, and a suite of
# skips exits 0 — so a run on the wrong kernel would report success for having
# tested nothing. Gate on the real precondition first, and fail loudly.
if ! unshare --user --map-root-user true 2>/dev/null; then
  echo "FATAL: this kernel refuses an unprivileged user namespace, so the" >&2
  echo "       user-namespace leg cannot run and every confinement test would" >&2
  echo "       only print 'skipping:'. Check /proc/sys/user/max_user_namespaces" >&2
  echo "       and any container policy, or run the root leg via sudo below." >&2
  exit 1
fi

echo "== user-namespace model =="
TINIO_SANDBOX_MODEL=userns cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== root model =="
# A target directory of its own for the privileged leg. Cargo run under `sudo`
# leaves root-owned artifacts behind, and the user-owned legs that follow then
# fail to rebuild over them — so the two identities never share one. The
# explicit PATH is what finds cargo at all under `sudo`, whose `secure_path`
# rarely includes `~/.cargo/bin`.
SUDO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}-root"
sudo -E env "PATH=$PATH" "CARGO_TARGET_DIR=$SUDO_TARGET_DIR" TINIO_SANDBOX_MODEL=root \
  cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== the confined server, end to end =="
# The example is built first, and separately: a filtered `cargo test --test
# sandbox_e2e` builds test targets only, and the test resolves the serve binary
# from its own profile directory.
cargo build -p tinio-server --example serve
cargo test -p tinio-server --test sandbox_e2e
```

Make it executable: `chmod +x crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`.

**Two WSL2 facts this script exists to work around**, both measured on this machine while writing the design: the distro idle-shuts-down between sessions, which silently discards anything under `/tmp` (a probe report was lost this way), and `/tmp` is itself a tmpfs. A long `cargo test` run keeps the distro alive by itself, but a run that pauses must not be trusted to have kept its scratch files.

- [ ] **Step 2: Run it**

Run from the repository root inside WSL2:

```bash
wsl.exe -d Ubuntu-26.04 -- bash -c 'cd /mnt/e/GitHub/tinio && bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh'
```
Expected: the user-namespace leg and the root leg both PASS, and the end-to-end confined server test passes. If `sudo -E` prompts for a password, the environment is not the passwordless-sudo WSL2 setup this script assumes — run `sudo -v` once first. The two legs deliberately build into different target directories (`…-root` for the privileged one), so the root leg's first run compiles the workspace again from scratch; that is the price of not handing root-owned artifacts to the user-owned legs.

- [ ] **Step 3: Record the commands in `docs/tests.md`**

Under the existing `## Platform-gated verification (WSL2)` section, add a bullet in the file's compressed style:

```markdown
- Confinement (`--sandbox`): `bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` — both models + the confined server.
```

- [ ] **Step 4: Record the lint exception in `docs/cargo.md`**

Change the lint bullet so the exception is stated where the rule is, not only in the design:

```markdown
- `[lints.rust] unsafe_code = "forbid"` on every crate. The sandbox platform backends — `tinio-sandbox-linux`, `tinio-sandbox-macos` and `tinio-sandbox-windows` — become `"deny"` **as each backend lands**, with its `#[allow(unsafe_code)]` sites enumerated in the sandbox design's decision 14; a stub that has no `unsafe` site yet stays `forbid`, because the lint follows the implementation. `forbid` cannot be overridden, so `deny` is what makes the exception expressible (`docs/superpowers/specs/2026-09-18-sandbox-design.md`).
```

**This plan writes the rule once, for all three backends, and it is the only plan that writes it.** The design's Bookkeeping says the rule "gains the exception in the same commit as the first of them", and the first of them is this phase's `tinio-sandbox-linux`, so this is where the text changes. Naming `-macos` and `-windows` in the same line is what keeps the later phases from rewriting it — `2026-09-18-sandbox-p2-windows.md` and `-p3-macos.md` each used to carry their own version of this bullet, worded differently, and those were removed precisely because three documents writing one line is how they drift apart.

**And the line is written in the tense of the rule, not of the moment.** At this commit only `tinio-sandbox-linux` is `deny`; the macOS and Windows crates are still `forbid` stubs with no `unsafe` site to enumerate, which is decision 14(d)'s "the lint follows the implementation" applied to them. A bullet that asserted all three were already `deny` would be false for two crates until P2 and P3 land — so it says when each becomes `deny` instead, and each later phase **verifies** the line and flips its own manifest when its first site arrives rather than restating the rule.

**The bullet states the rule and not the sites, because `docs/cargo.md` is a Cargo.toml convention file and nothing else.** Its sections are Rules, Versions, Features, Groups and Example; a `deny`-versus-`forbid` lint choice is a `[lints.rust]` line and belongs there, while a list of source modules is not a Cargo.toml convention at all and does not. Enumerating which functions carry `#[allow(unsafe_code)]` would also require this phase to know the Windows modules P2 has not written yet and the macOS site P3 has not either; the enumeration's home is decision 14, which the bullet points at. A `docs/cargo.md` that listed sites would go stale in the same commit that added one.

- [ ] **Step 5: Write the operator notes in `docs/sandbox.md`**

**`--sandbox` is a feature an operator switches on and then has to live with, and three of its consequences exist nowhere but in this plan's reasoning** — a reader would have to derive them from design §4's mount table, decision 13's filter and the capability policy to know why their objects started answering `AccessDenied`. None of the three is a defect; each is the confinement working as designed, and each reads as a bug the first time it is met without warning. Create the file with this content (one unbroken line per paragraph, the rule every document under `docs/` follows):

```markdown
# `--sandbox`: what an operator has to know

`--sandbox` confines the server to the paths it is given — a private mount namespace, `pivot_root`, and a seccomp filter over outbound connection setup. The notes below are Linux's, because Linux is the platform that has landed, and each is a consequence of the confinement working as designed rather than a defect: all three read as bugs the first time they are met without warning.

## The process has no capabilities, even when it runs as root

The confinement drops every capability before the server starts, `CAP_DAC_OVERRIDE` and `CAP_DAC_READ_SEARCH` among them, so the process reads files as the account it runs as rather than as the superuser it still is: under the root model it is uid 0 with an empty capability set apart from the single `CAP_CHOWN` a `local_uid` mapping needs. A file under the served root that the service account does not own and that is not readable by others is therefore refused, and an S3 client sees `AccessDenied` for objects that were readable before the flag was added. The user-namespace model states the same rule from the other side: its namespace maps exactly one uid — the invoking user's — so a file owned by any other host id is as unreadable inside the jail as a stranger's file is outside it. Run the server as the account that owns the data, or `chown -R` the served root to that account.

## Outbound connections are denied, and the filter cannot see the address family

The seccomp filter denies `connect`, `sendto`, `sendmsg` and `sendmmsg` by syscall number, so no address family can be let through and `connect(AF_UNIX)` is denied along with the TCP ones: inside the jail, `/dev/log` (syslog), a Docker socket and a D-Bus connection all fail with `EPERM`. This is deliberate — a filter that could see the family would be a different and larger program — and it is survivable because the server's own logs go to stderr rather than to syslog. There is no allowance to add: a Unix socket inside an exposed directory is unreachable too, because the refusal is at `connect`, not at the path.

## The host's path namespace does not exist inside the jail

The jail is a private mount namespace with `pivot_root`, and what it contains is the exposure set and nothing else, each entry at its original absolute path. Everything else is absent rather than refused: `/etc`, `/proc`, the host's `/tmp` and the operator's home do not exist in there, and a path that is not an exposure fails as "no such file or directory" rather than as a permission error. This is where Linux differs from the other two platforms, which confine by policy — a Windows AppContainer's DACLs, a macOS Seatbelt profile — and in those the host's paths are visible and refused. So a configuration file, a helper binary or a data path the server needs on Linux has to be exposed, and what would have been a permission error at startup is a missing file instead.
```

Then one line in `README.md`, so the notes are reachable from where the flag is first met: the `Layout` section's closing sentence names the other `docs/` files (`` Workspace docs: [`docs/`](docs) — cargo conventions, code style, test guide. ``), and `docs/sandbox.md` is added to that list. Nothing else in the README changes: the flag's own documentation is the file, not the front page.

**Why a file of its own, and why this phase writes it.** Task 9 is already the task that records what this phase owes the repository — the WSL2 commands in `docs/tests.md` and the lint exception in `docs/cargo.md` — and these notes are the third of those. They live in `docs/` beside the other reference documents rather than in the README, which is a front page that links out; the README gets one line and no more. **This phase creates the file and owns it**: a launcher phase extends it with its own platform's notes instead of creating a sibling, which is the one-owner rule the shared document states for rules, names and code (shared §1, §4) applied to the one document in this feature that is written for an operator rather than for an implementer.

- [ ] **Step 6: Run the full local gate**

Run the **Standard acceptance (pre-push gate)** block in `docs/tests.md` in full — it is the bar, and it is a longer list than the one this step used to carry (shared §1). The legs this phase adds to it are the two feature-off checks:

```bash
cargo check -p tinio-server --no-default-features
cargo check -p tinio-server --no-default-features --features multipart,copy,list-v1,list-v2,cors,select,acl
```

Expected: all clean. The clippy leg is what proves the `unsafe_code` exception stayed at three sites: `rg -c 'allow\(unsafe_code\)' crates/tinio-sandbox-linux/src` must print `3`.

---

## Commit

**One commit, made by hand, at the end of the phase.** No task in this plan commits, so every change accumulates in the working tree and the phase is reviewed as one diff rather than a commit per task. `CLAUDE.md` owns the authorisation rule — git writes are asked for per operation, and the index is never written by an agent — so this section fixes the phase's **scope** and its message, and is not permission to run it.

Run the full Standard acceptance block (`docs/tests.md`) first, and commit only once the phase's own review is clean. The message is a conventional header plus single-line cause-and-effect bullets: what changed and why, never where it lives. It is a starting point rather than a description of finished work — where the phase's actual run makes a line of it false, that line changes.

```bash
git add crates/tinio-sandbox-linux crates/tinio-server crates/tinio-e2e docs/cargo.md docs/tests.md docs/sandbox.md README.md
git commit -F - <<'EOF'
feat(sandbox-linux): confine serve in place

- Process confines itself: no unconfined supervisor survives the jail
- Launcher platforms cannot do this and must keep one outside
- Runs before the runtime exists: unshare fails EINVAL once a thread is live
- Read-only enforced by remount, or overlay where the filesystem refuses it
- Outbound syscalls denied; filter cannot see address family, so AF_UNIX fails too
- Filter installed last: irreversible, and a mistake cannot be repaired at runtime
- Falls back to root when unprivileged namespaces are refused, else fails closed
- Bounding set swept before the set is narrowed: the reverse order drops nothing
EOF
```

## Self-Review

**Spec coverage.** Walking `docs/superpowers/specs/2026-09-18-sandbox-design.md` against the tasks above. The rows P0 owns — the crate topology, the trait, the launcher-shared surface — are in P0's table, not repeated here.

| Spec item | Task |
|---|---|
| §1 the consumer feature gate | 7 |
| decision 4 default-on means compiled in | 7 |
| decision 5 feature-off refuses | 7 Step 8 checks the build; refusing the *flag* is a CLI concern, deferred with the flag |
| decision 6 Linux confines in place | 5, 7 — and the `Confinement::InPlace` arm is the only one this platform can produce |
| decision 7 the adaptive model | 3, 5 |
| decision 8 read-only is the absence of a write permission | 2, 5, 6 — §4's mechanism (the read-only remount, with the `MS_RDONLY` overlay as its fallback where the filesystem refuses) is chosen by `apply_mount` rather than by the plan, per the 2026-09-19 re-measurement |
| decision 9 path mirroring | 1 |
| decision 10 fixed jail root name | 1 |
| decision 11 keep the uid, drop capabilities | 3, 5 |
| decision 12 no `/proc` | 2 (the device-file list is the whole of `/dev`; no `/proc` entry exists to add) |
| decision 13 seccomp, not a network namespace | 4 |
| decision 14 unsafe scope and lint | 4 Step 5, 5 Step 7, 9 Step 4 |
| decision 16 listener handoff | **P2/P3** — Linux confines in place and binds its own listener |
| decision 19 the endpoint is an event | 7 — the readiness frame *and*, since the operability pass, the `Error` frame a refusal sends before it returns |
| decision 22 exe directory | 7 Step 2 asserts it is *not* exposed on Linux |
| decision 23 the fallback chroot path | **P4** |
| decision 25 the otel conflict | **with the CLI** — `serve` never passes an endpoint and `otel` is not a default feature |
| decision 26 one-way, before the runtime | 5, 7 |
| §12 Linux integration, the event contract, WSL2 | 6, 8, 9 |
| §12 Ubuntu 24.04+'s AppArmor userns restriction | 3 (the remedy text), 6 (the EPERM-vs-EINVAL hint) |

**What the grilling round changed (2026-09-19).** Every row below came from a question with a measured or repository-sourced answer, and each change is recorded where it lands, not only here.

| Change | Where |
|---|---|
| The bounding-set sweep runs **before** the capset: `PR_CAPBSET_DROP` needs an effective `CAP_SETPCAP`, which the capset is what removes. Measured on WSL2: with caps intact the drop succeeds, after an empty capset it returns `EPERM` and the bounding set is untouched | Task 5 `drop_capabilities` |
| `rustix::thread::Capability` is deprecated since 1.1, and `rustix::thread` re-exports it (`mod prctl;` plus `pub use prctl::*;`) — so it compiles and the cost is a deprecation warning under `-D warnings`. The whole policy uses `CapabilitySet` anyway, on the type's own advice | Task 3 `caps.rs`, Task 5 `ALL_CAPABILITIES` |
| The id maps are written **through** the descriptors opened after the `unshare`, instead of through a second `fs::write` that made the held descriptors dead weight | Task 5 `write_map` |
| The empty-sandbox check moved ahead of the availability probe, and it is `Error::Step`: the test, the implementation and P0's stub now agree, and the answer no longer depends on the host | Task 5 |
| `sendmmsg` joined the denial table (307 on `x86_64`, 269 on `aarch64`), and the "a server answers with `write`/`send`" claim was corrected — `send(2)` *is* the `sendto` syscall, so it is denied too | Task 4 |
| The confined child is a `[[bin]]`, not an example: `CARGO_BIN_EXE_*` exists for binary targets only (an example in `env!(…)` does not compile) and a filtered `cargo test --test jail` does not build examples at all. Both measured | Task 6 |
| `serve` reaches `LinuxOptions` through a target-gated dependency on `tinio-sandbox-linux`, because the facade deliberately exports no platform options | Task 7 |
| The state directory comes from `storage.state_dir()` rather than a hand-built `<root>/.tinio`, so read-only mode's relocation cannot silently expose the wrong path | Task 7 |
| `--sandbox` is refused on a platform whose backend another plan owns, instead of being accepted and ignored | Task 7 |
| The `sandbox_e2e` test resolves the `serve` example from its own profile directory (not `assert_cmd::cargo_bin`) and says which command builds it | Task 8 |
| The WSL2 script's privileged leg gets its own target directory and an explicit `PATH` — root-owned artifacts in the user's target directory break the user-owned legs that follow | Task 9 |

**What the second review pass changed (2026-09-19).** Every `rustix` call in this plan was checked against the vendored `rustix-1.1.4` source rather than from recall, and the two defects that found are the kind that compile into a jail that does not hold — so each row is recorded where it lands, as above.

| Change | Where |
|---|---|
| `mirror_under` refuses `..` and validates the **composed** mirror instead of comparing the host spelling to the jail: `expose("/")` mirrors onto the jail root itself, and `apply_mount` was creating host paths from it before any confinement existed | Task 1 |
| The read-only bind is `mount_remount(target, MountFlags::RDONLY \| MountFlags::BIND, "")`. `mount_change` takes `MountPropagationFlags` — `MountFlags` has no `PRIVATE` — and emits `mount(NULL, target, NULL, flags, NULL)` with no `MS_REMOUNT`, so it could never have produced a read-only bind; the private lock on `/` is the call it *is* for | Task 5 `apply_mount`, Task 5 Step 4 |
| Every `step(…)` is generic over `impl Into<io::Error>`, because `rustix` returns `Errno`, which does not coerce to `std::io::Error`: the earlier shape did not compile at any of the ~14 syscall sites. The bounding-set errno test now compares `Errno` to `Errno` rather than an `i32` to an `Option<i32>` | Tasks 3, 5 |
| `rustix::runtime::exit_group` (there is no `rustix::process::exit`), `Some(options.as_c_str())` (`mount`'s data parameter is `Into<Option<&CStr>>`), and `addr` passed straight to `connect` (there is no `SocketAddr::as_str`) | Tasks 3, 5, 8 |
| The bind predicate is "is the source a directory", not `is_file()`: `/dev/null` is a character device, so the old spelling created a **directory** at `<jail>/dev/null` and then bound a chardev onto it — `ENOTDIR`, a jail that never builds, and a skip path hiding it | Task 5 `apply_mount` |
| `Mechanism` is re-exported from `mechanism.rs`: `plan.rs`, `options.rs` and `enter.rs` all import `crate::mechanism::Mechanism`, and an `impl Mechanism` block in a module that does not name the type does not compile. It is deliberately **not** re-exported back into `lib.rs`, where P0 defines it — that direction is `E0255` | Task 3 |
| The child honours `TINIO_SANDBOX_MODEL` through `LinuxOptions::from_env()`, so the two acceptance legs run different code; and both skip paths became a positive precondition (`unshare(1)`, asked before the child or the server starts) instead of a substring of a failure | Tasks 6, 8 |
| `local_uid` is refused against the **resolved** model as well as the request, which is the half `Mechanism::Auto` can reach and the request cannot describe | Task 5 |
| The x32 ABI gap in the seccomp filter is recorded beside the filter and added to the design's Risks as this phase lands | Task 4 |
| `assert_cmd` is gone from the stack and from the manifest claim; `options.rs` is in Task 5's file list and the invented `new` is out of `LinuxOptions`; the red-test filter is two substrings rather than one regex-looking literal that matched nothing and exited 0; `install() -> Result<Seccomp>` matches its implementation | Tasks 3, 4, 5 |
| Every core operation emits a `tracing` event with structured fields, failure events are emitted where the `Error` is built so the log's `step` and the error's cannot drift, and `serve` installs its subscriber **before** `confine` — otherwise every confinement event is dropped | shared §7; Tasks 1, 3, 4, 5, 7 |

**And one row that is an operator ruling rather than a review finding**, kept in the same table because it is the same kind of fact — one document owning a thing three documents would otherwise spell differently: the three plans' `serve` signatures disagreed, so there is now **one signature**, owned here, with three optional parameters whose `None`s are facts about a platform (Linux serves in the process that confined itself; a launcher serves in its child) and with the launcher side resolving the state-directory *path* rather than constructing an `FsStorage`, because two processes on one state directory is not a shape the storage layer supports.

| Change | Where |
|---|---|
| One `serve` signature for all three plans — `(args, config, storage, inherited, stop)`, the last three optional — with `stop` created in the child and the launcher forbidden to construct an `FsStorage` | Task 7 Step 5 |

**What the operability pass changed (2026-09-19).** This round came from reading the plan the way an operator meets it — what a harness watching stdout sees, what an unbounded wait costs a startup, what the tmpfs arithmetic actually is, which resource a `$TMPDIR` failure blames, and what a reader of `--sandbox` is told — and every row lands where it is written above, not only here.

| Change | Where |
|---|---|
| The Linux arm sends an `Event::Error` frame before it returns a refusal — the model variable naming something that is not a model, and `confine` refusing, the unavailable-model refusal among its reasons — through one `emit`/`emit_error` pair that is also the file's only stdout writer. Decision 19's "a startup failure is an event rather than an absence" held on two platforms of three until now, because Linux confines *in place* and has no child whose frames a parent relays: a consumer of stdout should not have to know which platform it is on | Task 7 Step 5 (the helpers, the arm and the stdout-exclusivity grep), Task 8 Step 1 (the frame the harness now fails with) |
| The user-namespace probe is bounded at both ends: the read end is non-blocking and the read and the reap share one deadline, the reap a `WaitOptions::NOHANG` loop — which is the parent's half of the design's evidence row, the child's `alarm` being unavailable (`rustix` sets no timers, and a hand-rolled FFI declaration for one would be a fourth `unsafe` site). Expiry is an `Error::Step`, never a false `Ok(false)`, which would send `Auto` to the root model on a host that refused nothing | Task 3 `userns_available`, `PROBE_TIMEOUT` |
| The jail tmpfs's `size=1m` carries its arithmetic — on the order of twenty directories at most one 4 KiB page each, under 100 KiB against the quota's 256 — and the `ENOSPC` narrative is corrected into the two cases it was conflating: a write **through** the mounted read-only exposure is `EROFS` at the mount and never reaches the quota, while `ENOSPC` is what a write straight into the jail's `/.overlay` meets | Task 2 `OverlayDirs`, Task 5 `apply_mount` |
| `$TMPDIR` is stated as a precondition of the whole confinement rather than discovered as a stray errno: the jail is created inside it, so a read-only, missing or full temp directory fails at the first `create_dir_all`, and the step string names the resource — `create jail root in $TMPDIR` — for the operator whose exposures are not at fault | Task 5 (the entry sequence, the `create_dir_all` step), `LinuxOptions::with_tmpdir`, Task 6 Step 4 |
| `docs/sandbox.md`, the operator-facing section this feature had nowhere to put: the process has no capabilities even as root (an unowned, unreadable file under the served root answers `AccessDenied`), the seccomp filter cannot see an address family so `AF_UNIX` connects are denied with the rest (`/dev/log`, a Docker socket, D-Bus: `EPERM`), and the host's path namespace does not exist inside the jail at all, unlike the two policy-based platforms — with the README's doc index gaining the one line that points at it | Task 9 Steps 5-7 |
| The read-only mechanism is decided by the **filesystem** rather than by the model, per the design's 2026-09-19 re-measurement: every read-only exposure enters the mount plan as `Bind` under either model, `apply_mount` remounts `RDONLY\|BIND` and substitutes the `MS_RDONLY` overlay only on `EPERM`, and the falsified rule is named and removed where it used to live — with the model-based test replaced by a both-models one and the overlay's own test moved to the errno reading, since the branch itself needs a filesystem (Task 6's unprivileged leg, whose served root is a tempdir on a tmpfs, is where it runs) | Task 2 (prose, `MountKind`, `plan`, tests), Task 5 (`apply_mount`, `refused_remount_needs_the_overlay`), Task 6 |
| Task 7 Step 7's "a bad exposure fails closed" check replaced with one the validation actually reaches — a relative root — and the reason the old spelling could not fail (`create_dir_all` in front of the sandbox arm) is now in the step, so nobody re-adds it | Task 7 Step 7 |

**Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N". Two judgement calls are flagged inline rather than hidden: Task 5's `ALL_CAPABILITIES` must be filled from rustix's actual `CapabilitySet` constants — 41 of them, in kernel numbering order — and it is the only part of this crate that has to be transcribed by hand; and Task 7 is written against a file another branch is editing, so it names seams and invariants instead of line numbers, with the alignment points listed in the step.

**Type consistency.** `Sandbox`, `AccessMode`, `Exposure`, `Error` and `Result` come from P0 and are used verbatim. `Mount`, `MountKind`, `OverlayDirs`, `plan`, `DEVICE_FILES` (Task 2) are matched exhaustively in Task 5 — and `MountKind::OverlayReadOnly` is the one kind `plan` never emits, since the read-only mechanism is the filesystem's answer rather than the plan's choice (Task 5's `apply_mount` substitutes it, and `refused_remount_needs_the_overlay` is that decision in full). `jail_root`, `mirror_under` (Task 1) are used by Tasks 2 and 5. `Mechanism`, `Resolved`, `Cap`, `retained`, `LinuxOptions` (Tasks 3, 5) are used by Tasks 2, 5, 6, 7; `Cap::set()` yields a single-bit `CapabilitySet` and never the deprecated `Capability`. `seccomp::{filter_program, install, Arch, Syscall, SockFilter}` (Task 4) is consumed by Task 5, and `Syscall` has four variants everywhere it is matched. `PROBE_TIMEOUT` (Task 3) is the probe's one deadline, shared by its read and its reap. Task 7 introduces exactly two names into `serve.rs`, `emit` and `emit_error`, and they are the file's only definitions of either — all three platforms' arms live in that one `main`, so a launcher phase calls them rather than declaring a second copy. No name is introduced twice with different meanings, and nothing is defined here that belongs on P0's surface.

**What this plan deliberately does not do.** It adds no items to `tinio-sandbox-core`. It is tempting to put a Linux-flavoured convenience there while implementing — the crate is right there and the editor is open — but the whole reason this plan was split out of a larger one is that shared items accumulating in a platform phase is what produced three plans that disagreed about the same API. Anything shared goes to P0.

**One thing this plan changes outside the sandbox, and it is called out in its own task.** Task 8 rewrites `crates/tinio-e2e/tests/steps/clients.rs`, which is shipped test code with cucumber features behind it. It is not smuggled in as a side effect: the task says so, names the files, and requires the cucumber suite to pass as the proof the migration is complete.
