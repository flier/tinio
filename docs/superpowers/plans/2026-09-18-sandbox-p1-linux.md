# `--sandbox` P1 (Linux) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Confine the tinio server process to the paths the operator exposes, on Linux, using a private mount namespace and `pivot_root`, and cut it off from outbound network access.

**Architecture:** The Linux backend of the `Backend` trait P0 defines. It composes a tmpfs jail that re-creates each exposed path at its **original absolute path**, binds them, `pivot_root`s into it, drops every capability it no longer needs, and installs a seccomp filter denying outbound connection setup. Unlike the other two platforms it confines **in place** — `unshare` and `pivot_root` apply to the calling process — so there is no child, no payload and no supervision. The `serve` example is restructured from `#[tokio::main]` into a plain `fn main` that confines first and builds its runtime second, because `unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process.

**Tech Stack:** Rust 2024, `rustix` 1.1 (`fs`, `mount`, `pipe`, `process`, `runtime`, `thread`) for every syscall — `pipe` for the availability probe's channel, `runtime` for its `kernel_fork` and its `exit_group`, and `fs` for the `fcntl_setfl` that makes the probe's read end non-blocking (a bounded probe needs it: `rustix` sets no timers) — plus `linux-raw-sys` 0.12 (`ptrace`, `prctl`, `errno`) for the seccomp-BPF UAPI constants the filter is assembled from (already in the tree via rustix; named here so they are not transcribed), `sha2` + `hex` for the jail-directory name, `tracing` for the event every operation emits (shared §7), and `tracing-subscriber` in the crate's dev-dependencies because one test asserts on an event.

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
- **Every public item in this crate carries a doc comment with at least one runnable rustdoc example (shared §1, constitution III), and these blocks under-document that**: where an item appears below with no `# Examples` block, the implementation added one, and the rule is the authority rather than the block. The items each task gained them for, so a later phase knows the blocks are not exhaustive: Task 2's `DEVICE_FILES`, `Mount`, `MountKind` and `OverlayDirs`; Task 3's `Mechanism`, `Resolved`, `Mechanism::from_env` and `Mechanism::resolved`; Task 4's `SockFilter`, `Arch` (`current`, `audit_arch`, `syscall_nr`), `Syscall` and `install` (a `no_run` example, because the filter is irreversible once installed); Task 5's `Options` — its setters and getters, each example made environment-independent, so `from_env`'s matches on the result rather than asserting one arm and a caller with `TINIO_SANDBOX_MODEL=bogus` in their environment does not turn a doctest red for a reason that is not the code's.
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

- [x] **Step 1: Write the failing tests**

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
        // single digit is the common case) fails a correct implementation. The
        // assertion was written and then deleted for exactly that reason; it is
        // not to be restored.
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
            // ANSI is on whenever the `ansi` feature is enabled and `NO_COLOR`
            // is unset, and the escapes land *inside* `step="jail_root"` — the
            // assertion below would then turn on the operator's environment
            // rather than on the event. The helper needs it; the assertions do
            // not change.
            .with_ansi(false)
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

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux jail::`
Expected: FAIL — `cannot find function 'jail_root'`.

- [x] **Step 3: Write the implementation**

```rust
//! Where the jail lives on the host, and where each exposed path lands inside it.

use std::path::{Component, Path, PathBuf};

use sha2::{Digest, Sha256};
use tinio_sandbox_core::{Error, Result};
use tracing::debug;

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

- [x] **Step 4: Run the tests to verify they pass**

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

This is the pure composition of design §4's table. **What it no longer decides is which mechanism makes a read-only exposure read-only, and the rule it used to state has been wrong twice.** The first rule — "a `ReadOnly` exposure under the user-namespace model needs an overlay, because an unprivileged user namespace cannot `remount,bind,ro` a bind of a host-owned superblock" — was replaced by a second one that a measurement then falsified: **"the filesystem family decides" is not the rule either.** A `strace` of the real `sandbox-child` under `TINIO_SANDBOX_MODEL=userns` shows the bare `MS_REMOUNT|MS_BIND|MS_RDONLY` on a tempdir under `/tmp` refused with `EPERM` while the root leg's identical remount returns `0`, and a hand-mounted tmpfs in the namespace accepts the same call — so the same filesystem is sometimes refused and sometimes accepted, and "tmpfs, devtmpfs, procfs, sysfs, and a subdirectory of a tmpfs with them" is a list of *examples* rather than the rule. **What refuses the remount is a locked per-mount flag.** `lock_mnt_tree()` runs in `copy_mnt_ns()` whenever the new mount namespace belongs to a different user namespace (`fs/namespace.c`), so every host mount is copied in with its per-mount flags locked — `nosuid`, `nodev`, `noexec`, `ro`, `atime` — and an `MS_REMOUNT|MS_BIND` request has to re-assert all of them: dropping one is `EPERM` out of `can_change_locked_flags()`, and re-asserting them all is accepted. That is why `/tmp` looked like a rule — it is a `nosuid,nodev` tmpfs, so its subdirectories arrive locked — and why host tmpfs `/mnt/wsl`, which carries no flags, remounts fine while host ext4 `/mnt/wslg/distro` returns `EPERM` the moment the request clears `RDONLY`. So **the mechanism follows the source mount's locked flags, not its filesystem and not the model**: every `ReadOnly` exposure enters the table as [`MountKind::Bind`] with read-only access under either model, and Task 5's `apply_mount` is what turns it read-only — the remount first, and the `MS_RDONLY` overlay **as its fallback** where the kernel answers the remount with `EPERM`. A reader who knew either older rule should expect no model test here; this paragraph says so in full because the code will otherwise look wrong to exactly that reader. **One hazard the same measurement found, and one alternative it left unadopted.** On an *unlocked* mount, a partial `MS_REMOUNT|MS_BIND` silently clears `nosuid`/`nodev` — a request that does not re-assert a flag does not merely fail, it may drop it — which is why the fallback is keyed on `EPERM` alone and every other errno stays a failure. And passing the source's own locked flags to `mount_remount` makes the remount succeed on every filesystem measured, which would make read-only single-mechanism and let `MountKind::OverlayReadOnly`, `OverlayDirs` and the `$TMPDIR/.overlay` pair be deleted; both mechanisms are measured working, so this is a simplification rather than a bug fix. **Its flag half was adopted on 2026-09-24** — `apply_mount` now passes the source mount's own nosuid/nodev/noexec beside `RDONLY`, because on an *unlocked* mount a partial request silently clears every flag it does not name instead of failing (design §4's paragraph and its Risks row own the adopted state and the measurement) — **and the deletion half was not**: `MountKind::OverlayReadOnly`, `OverlayDirs` and the `$TMPDIR/.overlay` pair stay in the tree, and the overlay arm is now reached only by a locked mount that is remounted without re-asserting its flags.

- [x] **Step 1: Write the failing tests**

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

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux plan::`
Expected: FAIL — `cannot find function 'plan'`.

- [x] **Step 3: Write the implementation**

```rust
//! The ordered list of mounts that turns a [`Sandbox`] into a jail.

use std::path::{Path, PathBuf};

use tinio_sandbox_core::{AccessMode, Result, Sandbox};

use crate::jail::mirror_under;
use crate::Mechanism;

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
/// use tinio_sandbox_linux::{Mechanism, Mount, OverlayDirs, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
/// let overlay = OverlayDirs {
///     upper: "/tmp/j/.up".into(),
///     work: "/tmp/j/.work".into(),
/// };
/// let mounts: Vec<Mount> =
///     plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &overlay).unwrap();
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
    /// root, and every later record is strictly below it — an "always inside
    /// the jail" written without that exception is false for the first record,
    /// and a reader who took it as an invariant would reject the one record
    /// `apply_mount` has to mount first.
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
/// use tinio_sandbox_linux::{Mechanism, MountKind, OverlayDirs, plan};
///
/// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
/// let overlay = OverlayDirs {
///     upper: "/tmp/j/.up".into(),
///     work: "/tmp/j/.work".into(),
/// };
/// let mounts = plan(&sandbox, Path::new("/tmp/j"), Mechanism::Root, &overlay).unwrap();
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

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS.

---

### Task 3: The model and the capability policy (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/{mechanism.rs,caps.rs}`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: `AccessMode`, `Error`.
- Produces: `Mechanism { Auto, UserNamespace, Root }` — defined in this module; `lib.rs` re-exports it so callers keep `tinio_sandbox_linux::Mechanism` and in-crate modules keep `crate::Mechanism` — `Mechanism::from_env`, `Mechanism::resolved(euid, userns) -> Resolved`, `userns_available() -> Result<bool>`, **the crate's one `unshare(flags, step_name) -> Result<()>` wrapper** — `pub(crate)`, because Task 5's `enter` imports it and a private item cannot be imported — the second of its two `unsafe` sites, declared here because the probe is its first caller — `Resolved { UserNamespace, Root, Unavailable(&'static str) }`; `Cap { Chown }`, `retained(mode: AccessMode, local_uid_configured: bool) -> &'static [Cap]`.

`resolved` is the adaptive rule of design decision 7's decision table, pure and tested with an injected euid and an injected probe answer, so every branch is unit-testable without privileges. It takes the probe's answer as a parameter rather than calling it, which is what keeps the table exhaustively testable **and** is why the probe lives beside it as `userns_available()`: the parent cannot attempt an `unshare` and then fall back, because the call changes the calling process for good, so the question is asked by forking a child that is discarded. **The fork is bounded at both ends** — the read and the reap share one deadline — because the design's own evidence table records what an unbounded reap costs an operator: a probe that hangs is a startup that never fails (`userns_available` carries the row and the reasoning).

- [x] **Step 1: Write the failing tests**

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

- [x] **Step 2: Run the tests to verify they fail**

**Run: `cargo test -p tinio-sandbox-linux mechanism::` then `cargo test -p tinio-sandbox-linux caps::`**
Expected: FAIL — `Resolved` and `Cap` do not exist.

**Two commands rather than one filter with a `\|` in it.** Cargo's test filter is a *substring* of the test's path, not a regex: `'mechanism::\|caps::'` is a literal no test name contains, so the run would match nothing and exit 0 — a red test that reports green, which is the one failure mode a red step exists to prevent. Either half alone is a correct substring.

- [x] **Step 3: Write `mechanism.rs`**

```rust
//! Which confinement mechanism to use, and whether it is available.
//!
//! `Mechanism` (the request) lives here with `Resolved`, `from_env`,
//! `resolved` and the availability probe that makes `Auto` mean something.
//! The crate root re-exports the public names, so callers keep writing
//! `tinio_sandbox_linux::Mechanism`.

use std::io;
use std::time::{Duration, Instant};

// `unshare_unsafe` is imported here **and not only in `userns_available`**: the
// `unshare` wrapper at the bottom of this file names it, and a module-scope
// import is the only one that reaches both. The rest of the syscall surface
// (`std::fs::File`, `std::io::{Read, Write}`, `std::os::fd::OwnedFd`,
// `std::thread::sleep`, `rustix::{fs, pipe, process, runtime}`) is hoisted to
// module scope for the same reason the crate's other blocks are corrected in
// place: `clippy::absolute_paths` reports 3+-segment paths used inline, and
// that is shared §1's rule rather than a fact about this task.
use rustix::thread::{UnshareFlags, unshare_unsafe};

use tinio_sandbox_core::{Error, Result};
use tracing::{debug, error};

// **The failure builders are the core's, and they are re-exported here rather
// than re-imported by each module.** This file defined all three until the
// phase-basis correction moved them to `tinio-sandbox-core`, beside the other
// platforms' shapes — three copies of one builder being the state that precedes
// three wordings of one log line, which this crate had already reached. `enter.rs`
// and `seccomp.rs` go on naming them as `crate::mechanism::{fail, refused, step}`,
// so **no call site in this crate changes with the move**; the doc that explained
// the `refused` decision moved to the core's function, trimmed of this crate's
// history.
pub(crate) use crate::_core::{fail, refused, step};

/// Which confinement mechanism Linux uses (design decision 7).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Mechanism;
///
/// assert_eq!(Mechanism::default(), Mechanism::Auto);
/// assert_ne!(Mechanism::Root, Mechanism::UserNamespace);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mechanism {
    /// Prefer the user namespace; resolve the fallback at confinement time.
    #[default]
    Auto,
    /// Force `unshare(CLONE_NEWUSER | CLONE_NEWNS)`.
    UserNamespace,
    /// Force `unshare(CLONE_NEWNS)` with `CAP_SYS_ADMIN`.
    Root,
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
/// # Preconditions
///
/// **It forks, so no thread may exist yet** (design §10): the child is a copy of
/// the calling thread, which is the thing `kernel_fork`'s signature cannot
/// promise, and `serve` confines before it builds its runtime for exactly that
/// reason. **This crate's own test does not meet the precondition** — libtest
/// runs it on a worker thread with the others beside it, and one of those may be
/// holding the tracing lock when the fork happens, leaving the child blocked if
/// its one event reaches that lock. The failure is bounded and loud rather than
/// a hang: the read end is non-blocking and shares the reap's deadline, so a
/// child that never answers expires as an [`Error::Step`] and the test goes red.
/// The test stays, because it is the only pin on the property the two-tier
/// fallback rests on — the probe does not confine its caller.
///
/// # Errors
///
/// [`Error::Step`] when the fork or the pipe itself fails, **or when either end
/// of the handshake runs past `PROBE_TIMEOUT`** — the private constant above,
/// named rather than linked, because `docs/tests.md` sets
/// `-D rustdoc::private-intra-doc-links` and a link to a private item is a doc
/// failure — distinct from a probe that ran and answered "no", which is
/// `Ok(false)` and an ordinary outcome.
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
#[allow(unsafe_code)]
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
    // The invariant it relies on is design §10's ordering rather than anything
    // this call can check: the probe runs before any thread exists (decision 26),
    // so the child is a copy of a single-threaded process and touches nothing but
    // its own pipe end, the `unshare` wrapper's one event and `exit_group`.
    // **That event does fire in production.** `serve` installs its subscriber
    // before `confine` — deliberately, so the confinement's own events are not
    // dropped — and the child inherits it, so a host that refuses the namespace
    // gets an `error!` from the child's failed `unshare` for what the parent
    // reports as an ordinary negative answer. The line is left standing rather
    // than silenced: the unshare genuinely did fail, it costs one entry beside
    // the `Error::Unsupported` the parent logs immediately afterwards, and
    // silencing it would mean the child calling `unshare_unsafe` itself — a
    // fourth `unsafe` site, which decision 14 forbids. It also needs rustix's `runtime` feature,
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

- [x] **Step 4: Write `caps.rs`**

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

Register `pub mod caps;` and `pub mod mechanism;`, and add to the crate's re-exports: `pub use self::mechanism::{Mechanism, Resolved, userns_available};`. That is the one public path: the enum is defined in this module, and `lib.rs` re-exports it so callers keep `tinio_sandbox_linux::Mechanism` and in-crate modules keep `crate::Mechanism`. An earlier spelling left the enum in `lib.rs` and omitted it from this list, because `use`-ing a type back in beside its own definition is `E0255`; that split is gone.

- [x] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS.

---

### Task 4: The seccomp outbound filter (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/seccomp.rs`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`
- Modify: `crates/tinio-sandbox-linux/Cargo.toml` (`linux-raw-sys` with `prctl` and `ptrace`; `errno` is in that crate's defaults)
- Modify: `Cargo.toml` (`linux-raw-sys = "0.12"` in `[workspace.dependencies]`)

**Interfaces:**
- Consumes: `Error`, `Result`.
- Produces: `SockFilter`, `BPF_*` / `SECCOMP_RET_*` / `AUDIT_ARCH_*` / `EPERM` re-exported from `linux-raw-sys`, `Arch { X86_64, Aarch64 }` (`current`, `audit_arch`, `syscall_nr`), `Syscall { Connect, SendTo, SendMsg, SendMmsg, IoUringSetup, IoUringEnter, IoUringRegister, Ptrace, ProcessVmReadv, ProcessVmWritev }` (this listed seven until 2026-09-26; the last three are follow-ups §6(a)'s ptrace family), `filter_program(arch) -> Vec<SockFilter>`, `install() -> Result<Seccomp>`.

The denial shape is design decision 13: deny the *outbound* syscalls and leave accepting and answering untouched, because `CLONE_NEWNET` would cut the host off from the server. `connect` is how an outbound TCP connection starts; `sendto`, `sendmsg` and `sendmmsg` are how a process would exfiltrate over UDP without ever calling `connect` — `sendmmsg` is the batch form of `sendmsg` and an equally direct route out, so leaving it out would be a hole in the same wall. **The landed filter denies more than those four, and the difference is not a network one**: the three `io_uring` ring calls, because a ring submits an `IORING_OP_CONNECT` or an `IORING_OP_SENDMSG` without ever reaching any of the four above, and the ptrace family (`ptrace`, `process_vm_readv`, `process_vm_writev`), because the user namespace does not confine a neighbour — a confined same-uid process could otherwise read that neighbour's memory out, write into it, or follow `/proc/<pid>/root` back across the mount view this phase just built. **The whole x32 ABI is denied as well, by one mask**: `JSET 0x4000_0000` on the syscall number, ahead of every comparison, so an x32 `connect` (`0x4000_002a`) meets the same `EPERM` as the numbers above and so does every other x32 syscall. Ten numbers and one ABI is the set, and both additions landed on 2026-09-26 (follow-ups §6).

**What stays available is `write`/`writev`, and that is enough** — but the distinction is sharper than "a server answers with `write`/`send`": glibc's `send(2)` *is* the `sendto` syscall, so `send()` is denied along with `sendto`/`sendmsg`/`sendmmsg`. Rust and tokio answer with `write`/`writev`, which is why the tinio server is unaffected; a C dependency that answers with `send()` would not be.

**One boundary this filter cannot close**, recorded in the design's Risks rather than papered over: the filter sees no address family — so `connect(AF_UNIX)` is denied too (design decision 13). **`io_uring` was the other and is closed rather than recorded** (2026-09-25): `IORING_OP_CONNECT`/`IORING_OP_SENDMSG` never reach a filtered syscall, so what the filter refuses is the ring calls themselves (425/426/427) — measured, a ring delivered a datagram while `sendto` was refused `EPERM`, and `io_uring_setup(8)` returns `EPERM` now where it had returned a descriptor. **The blocks below are this phase's record and keep the thirteen-instruction program with its four `JEQ`s as written then**; the landed filter is **twenty-seven instructions on `x86_64` and twenty-five on `aarch64`** — ten compare/deny pairs plus, on `x86_64` alone, the x32 mask pair (decision 14 (a) owns the shape).

**UAPI numbers are not transcribed.** `linux-raw-sys` already sits in the tree as rustix's UAPI crate (design Bookkeeping); this module re-exports `BPF_*`, `SECCOMP_RET_*`, `AUDIT_ARCH_*` and `EPERM` from it, reads `PR_SET_SECCOMP` / `SECCOMP_MODE_FILTER` from `prctl` / `ptrace`, and takes `seccomp_data` field offsets via `offset_of`. The opcodes are `u32` there and `sock_filter.code` is `u16`, so the program truncates through `bpf_code`. The dual syscall-number table stays: `linux-raw-sys` only publishes `__NR_*` for the compiling architecture, and doctests name both tables.

**The other gap was the x32 ABI, and it is the design's Risks row's subject as well as this plan's.** The program compares `seccomp_data.arch` and nothing else, and on `x86_64` the x32 ABI reports the *same* `AUDIT_ARCH_X86_64` — what distinguishes it is `__X32_SYSCALL_BIT` (`0x4000_0000`) OR'd into the syscall number. `connect` through that ABI is not number 42, so before the mask it met the tail and was allowed. **This gap is closed rather than recorded now, and it is the mask rather than the second comparison the sentence above anticipated** (2026-09-26): the landed filter carries one `JSET 0x4000_0000` pair on the syscall number, ahead of every equality test, and it refuses **the whole ABI** — not only the ten numbers the list names — which is the shape follow-ups §6(b) ratified, because the program compares `seccomp_data.nr` once and has no second comparison to key on both the bit and a number. The design's Risks row records the closure and the measurement; what is this plan's to keep is that the mask was reached before it was closed (a raw x32 syscall from a 64-bit process, on a kernel built with `CONFIG_X86_X32_ABI=y`), which is what made it a fix rather than an entry.

- [x] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    use linux_raw_sys::general::{__NR_connect, __NR_sendmmsg, __NR_sendmsg, __NR_sendto};

    #[test]
    fn the_program_has_thirteen_instructions() {
        assert_eq!(filter_program(Arch::X86_64).len(), 13);
        assert_eq!(filter_program(Arch::Aarch64).len(), 13);
    }

    #[test]
    fn the_four_denials_return_eperm() {
        let program = filter_program(Arch::X86_64);
        for index in [5, 7, 9, 11] {
            assert_eq!(program[index].code, bpf_code(BPF_RET | BPF_K));
            assert_eq!(program[index].k, SECCOMP_RET_ERRNO | EPERM);
        }
    }

    #[test]
    fn the_tail_allows_everything_else() {
        let program = filter_program(Arch::X86_64);
        assert_eq!(program[12].code, bpf_code(BPF_RET | BPF_K));
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
        assert_eq!(program[0].k, SECCOMP_DATA_ARCH, "arch is the second u32");
        assert_eq!(program[0].code, bpf_code(BPF_LD | BPF_W | BPF_ABS));
        assert_eq!(program[3].k, SECCOMP_DATA_NR, "nr is the first u32");
    }

    #[test]
    fn jump_offsets_land_on_each_denial_or_skip_it() {
        let program = filter_program(Arch::X86_64);
        for index in [4, 6, 8, 10] {
            assert_eq!(program[index].code, bpf_code(BPF_JMP | BPF_JEQ | BPF_K));
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

    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    #[test]
    fn syscall_numbers_match_the_host_uapi() {
        let arch = Arch::current().expect("this test is gated to a table we have");
        assert_eq!(arch.syscall_nr(Syscall::Connect), __NR_connect);
        assert_eq!(arch.syscall_nr(Syscall::SendTo), __NR_sendto);
        assert_eq!(arch.syscall_nr(Syscall::SendMsg), __NR_sendmsg);
        assert_eq!(arch.syscall_nr(Syscall::SendMmsg), __NR_sendmmsg);
    }

    #[test]
    fn the_running_architecture_is_known_on_the_supported_targets() {
        #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
        assert!(Arch::current().is_some());
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux seccomp::`
Expected: FAIL — `cannot find function 'filter_program'`.

- [x] **Step 3: Write the implementation**

```rust
//! The seccomp filter that denies outbound connection setup.
//!
//! The filter is installed exactly once, after every mount and after
//! `PR_SET_NO_NEW_PRIVS`, and is irreversible (design decisions 13, 26).
//! Opcodes, return codes, `AUDIT_ARCH_*` and `seccomp_data` field offsets
//! are `linux-raw-sys`'s UAPI constants; the program is assembled here.

use tinio_sandbox_core::{Error, Result};
use linux_raw_sys::{
    prctl::PR_SET_SECCOMP,
    ptrace::{SECCOMP_MODE_FILTER, seccomp_data},
};
use tracing::{info, warn};

use std::mem::offset_of;

use crate::mechanism::fail;

#[doc(inline)]
pub use linux_raw_sys::{
    errno::EPERM,
    ptrace::{
        AUDIT_ARCH_AARCH64, AUDIT_ARCH_X86_64, BPF_ABS, BPF_JEQ, BPF_JMP, BPF_K, BPF_LD, BPF_RET,
        BPF_W, SECCOMP_RET_ALLOW, SECCOMP_RET_ERRNO, SECCOMP_RET_KILL_PROCESS,
    },
};

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
///
/// **Its fields are written by Rust and read only by the kernel**, through the
/// pointer [`install`] hands to `prctl` — which is why they carry no read here
/// and why the layout is `repr(C)` rather than anything this crate could use.
/// **Do not "fix" the missing reads with `#[expect(dead_code)]`**: with
/// `#[repr(C)]` rustc reports nothing at all, and the attribute is what tells it
/// the layout is not Rust's to reason about — remove it and the lint returns
/// ("fields `len` and `filter` are never read"), restore it and the lint goes
/// quiet. An `#[expect]` here would therefore be an *unfulfilled* expectation,
/// which under `-D warnings` is itself an error: the authorised fix would break
/// the build.
#[repr(C)]
struct SockFprog {
    len: u16,
    filter: *const SockFilter,
}

const fn bpf_code(op: u32) -> u16 {
    op as u16
}

const SECCOMP_DATA_NR: u32 = offset_of!(seccomp_data, nr) as u32;
const SECCOMP_DATA_ARCH: u32 = offset_of!(seccomp_data, arch) as u32;

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
            Self::X86_64 => AUDIT_ARCH_X86_64,
            Self::Aarch64 => AUDIT_ARCH_AARCH64,
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
    let load = |offset: u32| SockFilter { code: bpf_code(BPF_LD | BPF_W | BPF_ABS), jt: 0, jf: 0, k: offset };
    let jump = |nr: u32| SockFilter {
        code: bpf_code(BPF_JMP | BPF_JEQ | BPF_K),
        jt: 0,
        jf: 1,
        k: nr,
    };
    let deny = SockFilter { code: bpf_code(BPF_RET | BPF_K), jt: 0, jf: 0, k: SECCOMP_RET_ERRNO | EPERM };
    let ret = |k: u32| SockFilter { code: bpf_code(BPF_RET | BPF_K), jt: 0, jf: 0, k };

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
        load(SECCOMP_DATA_ARCH),
        SockFilter { code: bpf_code(BPF_JMP | BPF_JEQ | BPF_K), jt: 1, jf: 0, k: arch.audit_arch() },
        ret(SECCOMP_RET_KILL_PROCESS),
        load(SECCOMP_DATA_NR),
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
/// [`Error::Unsupported`] when the program does not fit the `u16` length the
/// kernel's `struct sock_fprog` carries — **unreachable by construction**,
/// because [`filter_program`] always returns thirteen instructions; it is
/// listed so the conversion's failure mode is named rather than left to be
/// discovered at the `u16::try_from`.
///
/// # Safety
///
/// This is one of the crate's three authorised `unsafe` sites (design decision
/// 14). Its invariants: `program` outlives the call — `fprog` borrows it and
/// nothing stores either — the `SockFprog` layout matches the kernel's
/// `struct sock_fprog` for this target, and the process is single-threaded so
/// no task can observe a half-installed filter.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_linux::seccomp::install;
///
/// // `no_run`: the filter is irreversible once it lands, and a kernel that
/// // refuses it reports the refusal rather than degrading silently.
/// let installed = install().unwrap();
/// # let _ = installed;
/// ```
#[allow(unsafe_code)]
pub fn install() -> Result<Seccomp> {
    use std::io;

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
        prctl(PR_SET_SECCOMP as i32, SECCOMP_MODE_FILTER as usize, &fprog as *const SockFprog, 0usize, 0usize)
    };
    if rc != 0 {
        // Through the crate's one failure event rather than an inline
        // `Error::Step` and an `error!` of its own (shared §7): a refused
        // `prctl` means the filter this code believes is installed is not,
        // which is the silent downgrade the design refuses everywhere else,
        // and it is the same kind of failure as every other one this crate
        // reports.
        return Err(fail("seccomp", io::Error::last_os_error()));
    }
    // `info!`, not `debug!`: this is the "or its skip" half of §7's must-survive
    // pair, and the two branches of one decision have to be readable from the
    // same log. The skip below is already a `warn!` — a degradation has to be
    // visible — while the ordinary outcome was a `debug!` that `serve`'s
    // `Verbosity::Info` subscriber dropped, which left a successful confinement
    // saying nothing at all about its network cut.
    info!(
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

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS. `install()` is not called by any unit test; the install is exercised by Task 6.

- [x] **Step 5: Confirm the unsafe surface is the three enumerated functions**

Run: `rg -n '^#\[allow\(unsafe_code\)\]' crates/tinio-sandbox-linux/src`
Expected: exactly three hits — `mechanism::userns_available` (the probe's `kernel_fork`), `mechanism::unshare`, and this task's `seccomp::install`. Task 3 added the first two and this task adds the third, and the count stops here: a fourth would mean a second body for one of those syscalls, which is a decision rather than a detail.

**The pattern is anchored, and the anchor is not decoration.** The unanchored `rg -n 'allow\(unsafe_code\)'` prints **four** lines after this task, not three: the fourth is the module-scope note at the end of this file's `filter_program`, whose prose contains the literal `#[allow(unsafe_code)]` while explaining why the `prctl` declaration cannot live there. The note is true and is left as it stands — rewording a true comment to make a count come out right is the wrong repair — so **the anchored form is the check from here on**, and it prints exactly the three sites. Task 5 Step 7 and Task 9 Step 6 inherit both the anchored command and this explanation.

---

### Task 5: `confine` — the one-way door (Linux)

**Files:**
- Create: `crates/tinio-sandbox-linux/src/enter.rs`
- Create: `crates/tinio-sandbox-linux/src/options.rs`
- Modify: `crates/tinio-sandbox-linux/src/lib.rs`

**Interfaces:**
- Consumes: everything above.
- Produces: `Options { model, local_uid_configured, tmpdir, euid, served_root_access }` (`Default`, `from_env`, `with_model`, `with_tmpdir`, `with_euid`, `with_served_root_access`, `with_local_uid`, plus the getters and `resolved_tmpdir`), `Backend::with_options(Options) -> Backend`, and the real `impl Backend for Backend`. **There is no `new`**: `Default` is what the type has, and a constructor that took five positional arguments would be a fifth place to add a field to. **The type is `Options` and not `LinuxOptions`**, which is what this task's blocks said when they were written: `tinio_sandbox_linux::LinuxOptions` stutters twice, because the crate path already says Linux, and the rename reaches every mention in this task, Task 6 and Task 7 — **including Step 2's red-run expectation below**, which names the missing struct. A consumer reaches it through the facade's cfg-gated `linux` alias (`tinio_sandbox::linux::Options`, Task 7), not by naming the platform crate.

The ordering is the whole design: validate, then the jail directory, then `unshare`, then compose, then pivot, then retire the old root, then drop capabilities, then install the filter. Capabilities must go after the mounts (they need `CAP_SYS_ADMIN`) and before the filter (`PR_SET_NO_NEW_PRIVS` must already be set for a filter to install without it) — and **inside** the capability drop the bounding set is swept before the capset, because `PR_CAPBSET_DROP` needs an effective `CAP_SETPCAP` that the capset is about to remove (measured; see `drop_capabilities`). **Read-only is enforced by attempt-then-fallback rather than by a choice made in the plan**: `apply_mount` remounts a read-only bind `RDONLY|BIND` **together with the source mount's own nosuid/nodev/noexec** — the flag half of Task 2's simplification, adopted 2026-09-24, so an *unlocked* mount cannot silently lose the flags the request does not name (design §4) — and only `EPERM` — a **locked per-mount flag** the request still failed to re-assert — substitutes the `MS_RDONLY` overlay for it, which is the rule design §4's table and decision 8 now state, with Task 2's paragraph carrying the measurement behind it. **The jail directory is created before the first `unshare`** rather than between the two: `$TMPDIR` is a precondition standing in front of the whole sequence, so a read-only, full or missing temp directory is refused while the process can still be repaired by exiting, and nothing below depends on a namespace to create it.

**One precondition is the host's rather than the request's, and it stands in front of that whole sequence: `$TMPDIR`.** The jail is created *inside* the temp directory — `std::env::temp_dir()`'s answer, which is `$TMPDIR` on a normal run, unless `Options::with_tmpdir` overrides it — so a read-only, missing or full temp directory makes the confinement fail at the first `create_dir_all`: before any mount exists and **before the first `unshare`**, with nothing written anywhere else. Nothing about the exposures is at fault and the errno alone (`EACCES`, `EROFS`, `ENOSPC`) does not say which resource refused, so the step string names it: **`create jail root in $TMPDIR`**. It names the variable rather than a path because P0's `Error::Step` requires that (`step` names the operation, so the message stays stable when a temporary path is involved), and the operator's fix — a different `TMPDIR` — is the same either way. Two consequences follow for the phases that run this code: a test harness must not assume the jail lands beside its own `tempfile::tempdir()`, and a service must not be started with an `$TMPDIR` the service account cannot write.

- [x] **Step 1: Write the failing tests**

```rust
#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rustix::io::Errno;
    use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, Error, LaunchPlan, Sandbox};

    use crate::Mechanism;
    use crate::options::Options;
    use crate::Backend;

    use super::{refused_remount_needs_the_overlay, writable_nested_mounts_in};

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
        let mut backend = Backend::with_options(Options::default().with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::BadPath { .. }), "{err:?}");
    }

    #[test]
    fn a_missing_path_is_refused_before_anything_irreversible_happens() {
        let sandbox = Sandbox::new().expose("/no/such/path/at/all", AccessMode::ReadOnly);
        let mut backend =
            Backend::with_options(Options::default().with_model(Mechanism::Root).with_euid(0));
        let err = backend.confine(&sandbox, &LaunchPlan::new()).unwrap_err();
        assert!(matches!(err, Error::BadPath { reason: "does not exist", .. }), "{err:?}");
    }

    #[test]
    fn an_unavailable_model_is_refused() {
        // Forced root without root: decided from the injected euid alone, with
        // no probe and therefore the same answer on every host.
        let sandbox = Sandbox::new().expose("/", AccessMode::ReadOnly);
        let mut backend = Backend::with_options(
            Options::default().with_model(Mechanism::Root).with_euid(1000),
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
            Options::default()
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
        let opts = Options::default().with_tmpdir(PathBuf::from("/tmp"));
        assert_eq!(opts.tmpdir(), Some(&PathBuf::from("/tmp")));
    }

    #[test]
    fn only_eperm_selects_the_overlay_fallback() {
        // **The read-only mechanism is the locked-flag answer, so the reading
        // of that answer has to be exact**: `EPERM` is what a read-only remount
        // of a source with locked flags gets, and it takes the `MS_RDONLY`
        // overlay; every other errno stays a failure, because a fallback taken
        // on one of them would mount something other than what was refused and
        // report a read-only exposure it never established. This is also where
        // the overlay's own test lives now: nothing pure can pin it any more —
        // `plan` emits `Bind` for every read-only exposure — and the branch
        // itself needs a filesystem, which is what Task 6's jail run supplies.
        assert!(refused_remount_needs_the_overlay(Errno::PERM));
        assert!(!refused_remount_needs_the_overlay(Errno::INVAL));
        assert!(!refused_remount_needs_the_overlay(Errno::ACCESS));
    }

    #[test]
    fn only_a_writable_mount_below_the_target_is_reported() {
        // **The hole, and the predicate that separates it from the safe
        // shapes**: the bind is recursive, so `cache` is carried into a jail
        // whose exposure a successful remount makes read-only at the top only,
        // and a write through `cache` then reaches the host. A submount that is
        // already `ro` is not that hole — it cannot be written through, and the
        // confined process cannot remount it writable without `CAP_SYS_ADMIN`
        // in the mount's owning namespace, which the capability drop removes —
        // so reporting it would refuse a configuration that is already as
        // read-only as the exposure asks for. The neighbours must not match
        // either: the target itself, and a path sharing its spelling but not its
        // components.
        // **`archive`'s per-mount options are `ro` while its superblock options
        // after the `-` separator are `rw`, and that pair is the row that pins
        // the field being read.** It is the shape measured on a real remount —
        // the top mount's sixth field goes `ro` and its superblock field stays
        // `rw` — so a helper reading the superblock instead of the mount would
        // find `rw` there, report this mount, and fail the assertion below.
        let table = "\
            24 30 0:23 / /tmp/jail/srv/data rw - tmpfs tmpfs rw\n\
            25 24 8:1 / /tmp/jail/srv/data/cache rw - ext4 /dev/sdb1 rw\n\
            26 24 8:2 / /tmp/jail/srv/data/archive ro - ext4 /dev/sdc1 rw\n\
            27 30 8:3 / /tmp/jail/srv/data2 rw - ext4 /dev/sdd1 rw\n\
            28 30 8:4 / /tmp/elsewhere rw - ext4 /dev/sde1 rw\n";
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/tmp/jail/srv/data")),
            vec![PathBuf::from("/tmp/jail/srv/data/cache")]
        );
        // The whole subtree and not one level of it, with the read-only
        // submount excluded from the middle of it.
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/tmp/jail/srv")).len(),
            3,
            "`data`, `data/cache` and `data2` — not the already-read-only `data/archive`"
        );
        // And nothing below the deepest mount, so a served root with no
        // submounts at all reports nothing.
        assert!(writable_nested_mounts_in(table, Path::new("/tmp/jail/srv/data/cache")).is_empty());
    }

    #[test]
    fn a_mount_point_is_decoded_before_it_is_compared() {
        // `\040` is a space and `\134` is a backslash, and they are decoded
        // because the escaped spelling never matches the path it denotes:
        // `/srv/my\040data/one` does not start with `/srv/my data`, so an
        // undecoded comparison would leave the nested mount unreported — the
        // one outcome this check may not have.
        let table = "\
            24 30 0:23 / /srv/my\\040data rw - tmpfs tmpfs rw\n\
            25 24 8:1 / /srv/my\\040data/one rw - ext4 /dev/sdb1 rw\n\
            26 24 8:2 / /srv/back\\134slash/deep rw - ext4 /dev/sdc1 rw\n";
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/srv/my data")),
            vec![PathBuf::from("/srv/my data/one")]
        );
        assert_eq!(
            writable_nested_mounts_in(table, Path::new("/srv/back\\slash")),
            vec![PathBuf::from("/srv/back\\slash/deep")]
        );
    }
}
```

**Two of these tests need helpers the implementation adds, and the test module imports them from the parent**: `use super::{refused_remount_needs_the_overlay, writable_nested_mounts_in};`, which is why the test module in the landed file names them rather than reaching through `super::*`. The nested-mount refusal those tests pin is the one thing this step cannot show end to end — it needs a real mount — so Task 6's jail run is where it is exercised, and the two tests above are the parser's own.

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-sandbox-linux enter::`
Expected: FAIL — `cannot find struct 'Options'`.

- [x] **Step 3: Write `options.rs`**

```rust
//! The Linux backend's construction-time options.
//!
//! Platform-specific knobs live here rather than in a shared type, so the
//! `Backend` trait's signature stays the same on every platform (design
//! decision 4 of the 2026-09-18 grilling — a shared options type would
//! accumulate a field per platform that no other platform reads).

use std::{env, path::PathBuf};

use rustix::process::geteuid;

use tinio_sandbox_core::{AccessMode, Result};

use crate::Mechanism;

/// How to confine, and the facts the backend needs that the sandbox does not
/// own.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::{Mechanism, Options};
///
/// let opts = Options::default().with_model(Mechanism::Root);
/// assert_eq!(opts.model(), Mechanism::Root);
/// ```
#[derive(Debug, Clone)]
pub struct Options {
    model: Mechanism,
    local_uid_configured: bool,
    tmpdir: Option<PathBuf>,
    euid: Option<u32>,
    served_root_access: AccessMode,
}

impl Default for Options {
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

impl Options {
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
    /// use tinio_sandbox_linux::Options;
    ///
    /// // Whatever the environment says, the call either answers or explains.
    /// let options = Options::from_env()?;
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
        self.euid.unwrap_or_else(|| geteuid().as_raw())
    }

    /// The tmpdir to place the jail in.
    pub fn resolved_tmpdir(&self) -> PathBuf {
        self.tmpdir.clone().unwrap_or_else(env::temp_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_a_read_write_root_with_an_automatic_model() {
        let opts = Options::default();
        assert_eq!(opts.model(), Mechanism::Auto);
        assert_eq!(opts.served_root_access(), AccessMode::ReadWrite);
        assert!(!opts.local_uid_configured());
    }

    #[test]
    fn every_setter_round_trips() {
        let opts = Options::default()
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

- [x] **Step 4: Write `enter.rs`**

```rust
//! `confine`: build the jail and step into it.

use std::env;
use std::ffi::CString;
use std::fs;
use std::io;
use std::path::Path;

use rustix::io::Errno;
use rustix::mount::{
    MountFlags, MountPropagationFlags, UnmountFlags, mount, mount_bind_recursive, mount_change,
    mount_remount, unmount,
};
use rustix::process::{getegid, pivot_root};
// **`CapabilitySet` and the capability syscalls are module-scope imports, and
// that is a compile matter rather than a style one**: `ALL_CAPABILITIES` below
// is a module-scope constant typed `[CapabilitySet; 41]`, so the type has to be
// nameable outside `drop_capabilities` — where the earlier spelling of this
// block declared them, which does not compile. The same paragraph covers every
// other 3+-segment path this file would otherwise spell inline, which
// `clippy::absolute_paths` reports (shared §1).
use rustix::thread::{
    CapabilitySet, UnshareFlags, capabilities, remove_capability_from_bounding_set,
    set_capabilities, set_no_new_privs,
};
use tinio_sandbox_core::{
    AccessMode, Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox,
};
use tracing::{debug, info};

use crate::Mechanism;
use crate::caps::{self, Cap};
use crate::jail::{jail_root, mirror_under};
// **The failure path is `mechanism`'s, and this module imports it rather than
// keeping a copy.** `refused`, `fail` and `step` are one `pub(crate)` set in
// `mechanism.rs` now: the pair this file used to carry had already drifted on
// landing — this module said `sandbox step refused` where the other two said
// `sandbox step failed` — so one failure was two events with two wordings from
// the moment the second copy existed. The plan's earlier instruction to keep
// the copy is superseded by that measurement, and Task 3's block is where the
// one definition lives.
use crate::mechanism::{Resolved, fail, refused, step, unshare, userns_available};
use crate::options::Options;
use crate::plan::{Mount, MountKind, OverlayDirs, plan};
use crate::seccomp;

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
    pub(crate) fn enter(&mut self, sandbox: &Sandbox, opts: &Options) -> Result<Confinement> {
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
        // (shared §7) — and it is `info!` because that question is asked of a
        // *successful* run, whose only subscriber is `serve`'s, installed at a
        // fixed `Verbosity::Info`: as a `debug!` this line existed and was
        // visible to nobody.
        info!(
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

        // 2. The jail root, **inside `$TMPDIR`** — the first host resource the
        //    confinement touches, and the one whose failure names nothing on its
        //    own. It may already exist — the previous run's leftover, which
        //    design decision 10 says to take over. The step string is the
        //    difference between an operator who knows to look at their temp
        //    directory and one reading an `EACCES` about a path they never
        //    chose: the jail's parent is `std::env::temp_dir()` unless
        //    `Options::with_tmpdir` says otherwise, and the error is
        //    `create jail root in $TMPDIR` for that reason. The variable is
        //    named rather than its value because P0's `Error::Step` keeps paths
        //    out of `step` so the message stays stable.
        //
        //    **Before the first `unshare`, and that is the point of the step
        //    string**: `$TMPDIR` is a precondition standing in front of the
        //    whole sequence rather than a step inside it, so a read-only, full
        //    or missing temp directory is refused while the process can still be
        //    repaired by exiting instead of after two namespaces have been
        //    entered. Nothing below depends on a namespace to create it.
        fs::create_dir_all(&jail).map_err(step("create jail root in $TMPDIR"))?;

        // 3. A private user namespace first, and on its own. The uid and gid
        //    maps below can only be written through `/proc/<pid>/…`, which is
        //    reachable exactly while the mount namespace is the host's — so the
        //    two unshares are separate steps with the map write between them
        //    (design decision 12). A descriptor opened *before* this call is
        //    bound to the initial namespace and its write returns EPERM, which
        //    is why the files are opened after it.
        let map_fds = if matches!(resolved_now, Resolved::UserNamespace) {
            let euid = opts.euid();
            let egid = getegid().as_raw();
            unshare(UnshareFlags::NEWUSER, "unshare user namespace")?;

            // Opened here, after the unshare, and written **through**: a
            // descriptor opened before it belongs to the initial namespace, so
            // its write is what is refused (design decision 12). Holding the
            // handle and writing through it makes that ordering structural
            // rather than a comment someone can move away from. **Each open
            // names its own map**: the three fail identically — a container with
            // no `/proc` answers `ENOENT` for all three — so an error that does
            // not say which one was missing leaves the operator to guess.
            let mut setgroups = open_map("/proc/self/setgroups", "open setgroups map")?;
            write_map(&mut setgroups, "setgroups", "deny")?;
            let mut uid_map = open_map("/proc/self/uid_map", "open uid map")?;
            write_map(&mut uid_map, "uid_map", &format!("0 {euid} 1"))?;
            let mut gid_map = open_map("/proc/self/gid_map", "open gid map")?;
            write_map(&mut gid_map, "gid_map", &format!("0 {egid} 1"))?;
            Some((setgroups, uid_map, gid_map))
        } else {
            None
        };

        // 4. The private mount namespace, now that the maps exist. One thread
        //    still, which is what makes both unshares legal (design §10).
        unshare(UnshareFlags::NEWNS, "unshare mount namespace")?;

        // 5. `pivot_root` refuses while the old root is shared, so the whole
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

        // 6. The mounts, in plan order.
        for entry in &mounts {
            apply_mount(entry, &overlay)?;
        }
        // **The exposure set the jail actually applied, in one event.** §7's
        // split is deliberate: the per-mount `debug!` in `apply_mount` is the
        // detail a launcher window shows, while this is the answer to
        // "confined, and to what" that has to survive `serve`'s fixed
        // `Verbosity::Info` subscriber — the same subscriber that made the
        // `debug!`-only rule pointless. Emitted after the loop rather than
        // before it, so the sentence is about the mounts that landed rather
        // than about the ones that were asked for.
        info!(
            step = "exposures",
            exposures = ?sandbox.exposures(),
            "the exposure set is mounted"
        );

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
        env::set_current_dir(&jail).map_err(step("chdir jail"))?;
        pivot_root(".", ".oldroot").map_err(step("pivot_root"))?;
        debug!(step = "pivot_root", root = %jail.display(), "root changed");
        env::set_current_dir("/").map_err(step("chdir /"))?;
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
///
/// **The step string is a parameter for the reason [`write_map`]'s name is
/// one**: the three maps fail identically — a container with no `/proc` answers
/// `ENOENT` for all three — so an error that does not say which map was missing
/// leaves the operator to guess, and the map whose absence is diagnosable
/// (`setgroups` vs `uid_map`) is the whole content of the failure.
fn open_map(path: &str, step_name: &'static str) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(step(step_name))
}

/// Whether a refused read-only remount selects the overlay fallback (design §4).
///
/// **`EPERM` is the trigger and nothing else is.** It is
/// `can_change_locked_flags()` refusing the request: `copy_mnt_ns` calls
/// `lock_mnt_tree` whenever the new mount namespace belongs to a different user
/// namespace, so every host mount arrives with its per-mount flags locked —
/// nosuid, nodev, noexec, ro, atime — and `MS_REMOUNT|MS_BIND` has to re-assert
/// all of them to get through. The source's locked flags decide it, and not its
/// filesystem: a bare tmpfs remounts and a `nosuid,nodev` one does not. Every
/// other errno is a failure to report rather than a mechanism to switch to: a
/// fallback taken on `EINVAL` would quietly replace a malformed mount with a
/// different one, and the run would report a read-only exposure it never
/// established.
///
/// A function rather than an inline guard because this *is* the read-only
/// decision: `apply_mount` cannot be unit-tested without mounting a filesystem,
/// and which errno takes the fallback is everything about it worth pinning.
fn refused_remount_needs_the_overlay(source: Errno) -> bool {
    source == Errno::PERM
}

/// The **writable** mount points strictly below `target` in a
/// `/proc/self/mountinfo` table.
///
/// **Why the check exists.** [`apply_mount`] binds an exposure with
/// `mount_bind_recursive`, which pulls the source's *submounts* in too, and a
/// successful remount makes the top mount read-only while leaving them alone: a
/// nested filesystem would stay writable inside a jail that reports the
/// exposure as read-only, and a write through it would land on the host.
/// `mount_setattr` with `AT_RECURSIVE` is the textbook fix and `rustix` 1.1.4
/// has none for a mount that already exists — its `MountAttrFlags` configures
/// one being *created* through `fsopen` — while reaching the syscall directly
/// would be a fourth `unsafe` site (decision 14). A plain non-recursive bind is
/// no alternative either: the nested mount's *contents* would vanish from the
/// jail instead of becoming read-only. So the shape is refused by name, which
/// is fail-closed.
///
/// **Writability, not existence, is the predicate.** A submount that is already
/// `ro` cannot be written through, and the confined process cannot remount it
/// writable — that takes `CAP_SYS_ADMIN` in the mount's owning namespace, which
/// the capability drop is about to take away — so reporting it would reject a
/// configuration that is already as read-only as the exposure asks for. The
/// mount's own options are the sixth field, and `rw` is always one of them.
///
/// **The fifth whitespace-separated field is the mount point, escaped** —
/// `\040` for a space, `\011` for a tab, `\012` for a newline and `\134` for a
/// backslash — and it is decoded before the comparison, which is the only reason
/// a target with a space in its name can match its own submounts. No raw
/// whitespace can appear in the field, so splitting on it is safe.
fn writable_nested_mounts_in(table: &str, target: &Path) -> Vec<PathBuf> {
    table
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let mount_point = fields.nth(4)?;
            let options = fields.next()?;
            Some((decode_mountinfo(mount_point), options))
        })
        .filter(|(mount, options)| {
            mount.starts_with(target)
                && mount != target
                && options.split(',').any(|option| option == "rw")
        })
        .map(|(mount, _)| mount)
        .collect()
}

/// Decode one mountinfo field: the kernel escapes exactly `\040`, `\011`,
/// `\012` and `\134`, and an escape this function does not know is copied
/// through rather than guessed at, so a field it misreads cannot become a
/// different path.
fn decode_mountinfo(field: &str) -> PathBuf {
    let mut out = String::with_capacity(field.len());
    let mut rest = field;
    while let Some(index) = rest.find('\\') {
        out.push_str(&rest[..index]);
        let escape = &rest[index + 1..];
        match escape.get(..3) {
            Some("040") => out.push(' '),
            Some("011") => out.push('\t'),
            Some("012") => out.push('\n'),
            Some("134") => out.push('\\'),
            _ => {
                out.push('\\');
                rest = escape;
                continue;
            }
        }
        rest = escape.get(3..).unwrap_or_default();
    }
    out.push_str(rest);
    PathBuf::from(out)
}

/// `/proc/self/mountinfo` as text, which is the host's table until the pivot.
///
/// Text rather than bytes because [`writable_nested_mounts_in`] compares paths, and a
/// line that is not UTF-8 — a mount source named in some other encoding —
/// refuses here instead of being decoded lossily: a lossy mount point would miss
/// a nested mount under a target with the same affliction, and missing one is
/// the under-enforcement the caller exists to stop.
fn read_mountinfo() -> Result<String> {
    fs::read_to_string("/proc/self/mountinfo").map_err(step("read /proc/self/mountinfo"))
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
                // alternative to it.** The remount is accepted where the
                // exposure's source carries no locked per-mount flags and
                // refused with `EPERM` where it does — which, once the mount
                // namespace belongs to a different user namespace, is every
                // host mount, `/tmp`'s `nosuid,nodev` tmpfs included. For the
                // refused case the `MS_RDONLY` overlay is what makes the
                // exposure read-only. The mount plan no longer chooses between
                // them, because nothing that performs no syscall can (Task 2):
                // this is the decision, and the source's locked flags make it.
                //
                // Re-entering `apply_mount` with the other kind, rather than
                // duplicating the overlay's body here, keeps the trace below
                // true: exactly one `mount applied` event per mount, carrying
                // the kind that actually landed.
                match mount_remount(&entry.target, MountFlags::RDONLY | MountFlags::BIND, "") {
                    Ok(()) => {
                        // **The landed remount is the only branch that needs the
                        // nested-mount check, and the second arm is why.** It
                        // makes the *top* mount read-only and leaves the
                        // submounts exactly as they were, so a writable one
                        // below is the hole — while the `EPERM` arm's overlay is
                        // `MS_RDONLY` from the start and covers the whole merged
                        // view end to end (measured: a write through a nested
                        // mount under it is refused with `EROFS`, while the host
                        // path stays writable, so the refusal is the overlay's
                        // own and not a permission artefact). Checking before
                        // the attempt would refuse every source with locked
                        // flags and a submount — `/dev` with `/dev/pts` and
                        // `/dev/shm` — where the overlay copes.
                        //
                        // Read here, after the remount, because this is when
                        // both facts are settled, and `/proc` is still the
                        // host's, the pivot having not happened yet. The table
                        // reports *resolved* paths — measured: a mount made
                        // through the symlink `/tmp/x/link/j` is listed as
                        // `/tmp/x/real/j` — so a `$TMPDIR` under a symlinked
                        // ancestor would make an unresolved target miss its own
                        // submounts. Canonicalised when the call answers, and the
                        // target as it stands when it does not, which can cost a
                        // missed refusal but cannot invent one.
                        let target = fs::canonicalize(&entry.target)
                            .unwrap_or_else(|_| entry.target.clone());
                        let nested = writable_nested_mounts_in(&read_mountinfo()?, &target);
                        if let Some(mount) = nested.first() {
                            // **The nested mount is named by its *host* path**,
                            // which is the exposure's plus the remainder below
                            // the target: the jail path it was found at is a name
                            // the operator never chose and would have to
                            // translate. The strip cannot fail — the filter that
                            // selected this mount matched the same prefix — and
                            // if it ever does the jail path is reported rather
                            // than a new failure raised: a cosmetic mapping is not
                            // worth a failure path.
                            let host = mount
                                .strip_prefix(&target)
                                .map_or_else(|_| mount.clone(), |rest| entry.source.join(rest));
                            return Err(fail(
                                "read-only exposure",
                                io::Error::other(format!(
                                    "{} is mounted below {}: the recursive bind carried it into \
                                     the jail but the remount makes only the top mount read-only, \
                                     so a write through it would reach the host — serve that root \
                                     read-write, or move the nested mount outside it",
                                    host.display(),
                                    entry.source.display(),
                                )),
                            ));
                        }
                        debug!(
                            step = "remount ro",
                            target = %entry.target.display(),
                            "bind remounted read-only"
                        );
                    }
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
        // is "past this kernel's `CAP_LAST_CAP`", and `EPERM` means the ordering
        // above is wrong or a capability was asked for that this namespace does
        // not hold.
        //
        // **`EINVAL` is reachable rather than impossible, and on a kernel older
        // than 5.9 it is the ordinary outcome**: capabilities 38-40 —
        // `PERFMON`, `BPF`, `CHECKPOINT_RESTORE` — do not exist there, so the
        // array above is this kernel's own list only from 5.9 on and the sweep
        // hits `EINVAL` at 38. Failing closed is the outcome design §11 asks
        // for; the step string is what makes it diagnosable, because it names
        // the drop that ran past the kernel's bound rather than leaving an
        // unattributed `EINVAL` at a capability nobody can see.
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
    CapabilitySet::DAC_OVERRIDE,
    CapabilitySet::DAC_READ_SEARCH,
    CapabilitySet::FOWNER,
    CapabilitySet::FSETID,
    CapabilitySet::KILL,
    CapabilitySet::SETGID,
    CapabilitySet::SETUID,
    CapabilitySet::SETPCAP,
    CapabilitySet::LINUX_IMMUTABLE,
    CapabilitySet::NET_BIND_SERVICE,
    CapabilitySet::NET_BROADCAST,
    CapabilitySet::NET_ADMIN,
    CapabilitySet::NET_RAW,
    CapabilitySet::IPC_LOCK,
    CapabilitySet::IPC_OWNER,
    CapabilitySet::SYS_MODULE,
    CapabilitySet::SYS_RAWIO,
    CapabilitySet::SYS_CHROOT,
    CapabilitySet::SYS_PTRACE,
    CapabilitySet::SYS_PACCT,
    CapabilitySet::SYS_ADMIN,
    CapabilitySet::SYS_BOOT,
    CapabilitySet::SYS_NICE,
    CapabilitySet::SYS_RESOURCE,
    CapabilitySet::SYS_TIME,
    CapabilitySet::SYS_TTY_CONFIG,
    CapabilitySet::MKNOD,
    CapabilitySet::LEASE,
    CapabilitySet::AUDIT_WRITE,
    CapabilitySet::AUDIT_CONTROL,
    CapabilitySet::SETFCAP,
    CapabilitySet::MAC_OVERRIDE,
    CapabilitySet::MAC_ADMIN,
    CapabilitySet::SYSLOG,
    CapabilitySet::WAKE_ALARM,
    CapabilitySet::BLOCK_SUSPEND,
    CapabilitySet::AUDIT_READ,
    CapabilitySet::PERFMON,
    CapabilitySet::BPF,
    CapabilitySet::CHECKPOINT_RESTORE,
];
```

**`ALL_CAPABILITIES` is given in full above, and it is the one list in this plan that had to be transcribed by hand.** It is `rustix`'s own `CapabilitySet` `bitflags!` block (`rustix-1.1.*/src/thread/libcap.rs`), 41 named constants in kernel numbering order, `CHOWN` (0) through `CHECKPOINT_RESTORE` (40) — 41 because that is `CAP_LAST_CAP + 1` on this kernel, which is also what bounds the sweep. The traps in it, so a later phase transcribing its own list does not fall into them: `NET_BIND_SERVICE` and not `NET_BINDSERVICE`; `LINUX_IMMUTABLE` and not `IMMUTABLE`; `SYSLOG` is one word; `WAKE_ALARM` and not `WAKEUP_ALARM`. **Do not reach for `rustix::thread::Capability`**: it is deprecated since 1.1 — `rustix::thread` does re-export it, so it compiles, and that is exactly the trap — and every use of it is a warning under `-D warnings`. If a later rustix exposes a real iterator over the universe of capabilities, prefer it and delete the constant.

- [x] **Step 5: Wire the trait implementation**

In `crates/tinio-sandbox-linux/src/lib.rs`, replace P0 Task 3's placeholder `impl BackendTrait for Backend` with:

```rust
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: Options,
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
    /// use tinio_sandbox_linux::{Backend, Mechanism, Options};
    ///
    /// let backend = Backend::with_options(Options::default().with_model(Mechanism::Root));
    /// let _ = backend;
    /// ```
    pub fn with_options(options: Options) -> Self {
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
        //
        // **The clone is the borrow checker's answer, not a preference.**
        // `self.enter(sandbox, &self.options)` is `E0502`: `enter` takes
        // `&mut self`, and the shared borrow of the field is still live when
        // the receiver's two-phase reservation is activated. `Options` is small
        // and this runs once per process, so the clone is the cheap fix; the
        // comment is what keeps the next reader from deleting it as a pointless
        // copy.
        let options = self.options.clone();
        self.enter(sandbox, &options)
    }
}
```

**Two more edits in the same file, and the first is an omission rather than a decision.** The free constructor `backend()` — P0's `pub fn backend() -> Backend { Backend }` — stops compiling in this step, because `Backend` is no longer a unit struct: it becomes `Backend::default()`. The facade's `platform()` returns `backend()`, so leaving it alone would break a second crate. And register `pub mod {caps, enter, jail, mechanism, options, plan, seccomp};` with the re-exports `jail_root`, `mirror_under`, `plan`, `Mount`, `MountKind`, `OverlayDirs`, `DEVICE_FILES`, `Mechanism`, `Resolved`, `userns_available`, `Cap`, `retained`, `Options`, `Seccomp` — `Mechanism` included, because Task 3 defines it in `mechanism.rs` and this list is the crate-root path (Task 3 says so in full). **`mechanism` and not `model`**: the module was renamed by the operator's ruling, and this one site survived the mechanical rename because it is a bare name inside a brace list — `pub mod model;` is `E0583 file not found for module model` on the next build. The merged `pub use self::{…};` form is `imports_granularity = "Crate"`'s, and it is what `tinio-sandbox-core`'s own `lib.rs` does.

- [x] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p tinio-sandbox-linux`
Expected: PASS. Every `enter` unit test either refuses before the first irreversible call — so it needs no privileges — or exercises one of the two pure helpers (`refused_remount_needs_the_overlay`, `writable_nested_mounts_in`) against sample text; nothing in the module mounts a filesystem.

- [x] **Step 7: Confirm the unsafe surface**

Run: `rg -n '^#\[allow\(unsafe_code\)\]' crates/tinio-sandbox-linux/src`
Expected: exactly three hits — `seccomp::install`, `mechanism::unshare` and `mechanism::userns_available`'s `kernel_fork`. A fourth would mean a second body for one of those syscalls, and that is a decision rather than a detail. **The pattern is anchored for the reason Task 4 Step 5 gives**: the unanchored one prints a fourth line, which is `seccomp.rs`'s own module-scope note quoting the attribute while explaining why the `prctl` declaration cannot live there.

---

### Task 6: The subprocess integration test

**Files:**
- Create: `crates/tinio-sandbox-linux/src/bin/sandbox-child.rs`
- Create: `crates/tinio-sandbox-linux/tests/jail.rs`

**Interfaces:**
- Consumes: `Backend`, `Options`, `Mechanism`, `Sandbox`, `AccessMode`.
- Produces: nothing the library uses.

`pivot_root` is irreversible, so the confined process cannot also be the asserting one: the jail runs in a **separate executable** and the parent reads its report from stdout.

**Why a separate executable rather than a re-executed test binary.** `unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process (design's own evidence table), and `libtest` starts every test on its own worker thread — so a `#[test] fn child_role()` re-executed with `--exact` is *already* multi-threaded before it reaches the first syscall. The failure is indistinguishable from "this host cannot confine", which is why the earlier shape of this task could never assert anything: it would have reported `skipping` on every host, including the ones that can confine. A plain `fn main` is single-threaded by construction and needs no flag or harness cooperation.

**Why `src/bin/` and not `examples/`.** Two measured facts decide it. `CARGO_BIN_EXE_<name>` is set for binary targets only — an example named in `env!(…)` is a hard *"environment variable not defined at compile time"* error, on stable and nightly alike — and, second, a filtered `cargo test --test jail` builds binary targets but **not** examples, so the walk-up-the-profile fallback an example would need panics with `NotFound` on exactly the command this task is run with. The binary target is the only shape that works under every invocation style.

**The two acceptance legs run different code, and that is what `TINIO_SANDBOX_MODEL` is for.** `TINIO_SANDBOX_MODEL=userns` and the privileged leg's `=root` are not two spellings of one run — **but not for the reason an earlier draft of this paragraph gave, and the rule that draft asserted is not the rule either.** The mechanism that makes a read-only exposure read-only is chosen by the **source mount's locked per-mount flags**, not by the model and not by the filesystem (Task 2's paragraph states the measurement in full): this test's served root is a `tempfile::tempdir()` under `/tmp`, whose tmpfs is `nosuid,nodev`, so its per-mount flags arrive locked in a namespace owned by a different user namespace and an unprivileged `MS_REMOUNT|MS_BIND|MS_RDONLY` on it is refused with `EPERM` — while the privileged leg's identical request returns `0`, because a mount namespace owned by the *same* user namespace is not locked. `strace` of the real `sandbox-child` shows exactly that pair of results. So the `userns` leg reaches `EPERM` and takes the `MS_RDONLY` overlay, and the privileged leg's remount is accepted and it takes `remount,bind,ro`; the two implementations of `AccessMode::ReadOnly` on this platform are the two that rule reaches (design §9, §12). **(2026-09-24: this is the pre-fix state. The remount now re-asserts the source mount's locked per-mount flags, so the `userns` leg's request is accepted too, both legs take `remount,bind,ro`, and the overlay arm is exercised by neither leg — design §4 records the change and the coverage gap it leaves.)** **The child therefore reports which of the two it landed on rather than the parent inferring it** — a `read-only-arm: remount|overlay` line, decided by whether `/.overlay/upper` exists inside the jail, which is a direct consequence of the branch that ran and is immune to the false positive a `statfs` superblock probe would give on a served root that is already an overlay. The parent asserts the value is **one of the two and never which one**, because which arm runs is a property of the host's mount flags rather than of the model. The child resolves its model from the environment through the one reader of that variable, `Options::from_env()` — and *not* by discovering one for itself, which is what the first draft did: `Auto`, plus a hand-rolled "root when euid is 0 and the probe failed", picks the user namespace on a userns-capable host under `sudo` exactly as it does unprivileged, so both legs would run the overlay path and the remount path would never be exercised while the suite reported success. **The privileged leg is invoked with `sudo env`, not `sudo -E`**: `sudo -E` is inert where sudoers says so ("preserving the environment is not supported, '-E' is ignored"), and the variable would then be unset — the root leg would silently run `Auto`, i.e. the same code as the unprivileged leg, while the suite reported success twice. The precondition below is the other half of that: a run that did not confine **fails**, and only a host whose capability is absent before the child starts is allowed to skip, and only after saying so deliberately.

- [x] **Step 1: Write the confined child**

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
//!
//! **Every import is inside the gated `main`, and the gate is why.** A crate
//! root's `#![cfg]` does not cover `src/bin/`, so this target is built on hosts
//! where `tinio-sandbox-linux` is an empty library — and a file-scope
//! `use tinio_sandbox_linux::{…}` there is `E0432: unresolved import`, which
//! stops `cargo check --workspace --all-targets` on every non-Linux host. The
//! std imports sit beside them because the only thing that uses them is the
//! same gated body, and an ungated one would be an unused import there.

#[cfg(target_os = "linux")]
fn main() {
    use std::{
        env, fs,
        net::{TcpListener, TcpStream},
        path::Path,
    };

    use tinio_sandbox_core::{AccessMode, Backend as BackendTrait, Sandbox};
    use tinio_sandbox_linux::{Backend, Options};

    let root = env::var("TINIO_SANDBOX_TEST_ROOT").expect("child needs a root");
    // **The address the parent is listening on, not one on the network.** It is
    // named by the parent because only the parent can hold the listener that
    // makes it reachable by construction, and a probe that is reachable by
    // construction is what stops the outbound assertion from being skipped for
    // an environmental reason.
    let probe = env::var("TINIO_SANDBOX_TEST_PROBE").expect("child needs a probe address");
    let access = match env::var("TINIO_SANDBOX_TEST_ACCESS").as_deref() {
        Ok("read-only") => AccessMode::ReadOnly,
        _ => AccessMode::ReadWrite,
    };
    let state = env::var("TINIO_SANDBOX_TEST_STATE").ok();

    let mut sandbox = Sandbox::new().expose(&root, access).device_files();
    if let Some(state) = &state {
        sandbox = sandbox.expose(state, AccessMode::ReadWrite);
    }

    // **The model comes from the environment, through its one reader.** The two
    // legs of the WSL2 run must not run the same code — and the code they
    // differ in is the read-only mechanism, which the *source mount's flags*
    // pick rather than the model (design §4): the served root here is a tempdir
    // under `/tmp`, whose tmpfs is `nosuid,nodev`, so its per-mount flags
    // arrive locked in a namespace owned by a different user namespace and the
    // read-only remount of it is refused with `EPERM`. So `=userns` reaches
    // that `EPERM` and takes the `MS_RDONLY` overlay fallback, while `=root`
    // under `sudo` gets the remount the design calls the primary mechanism;
    // those two are the implementations of `AccessMode::ReadOnly` this platform
    // has (design §9, §12). The model read from the environment is what decides
    // *which of the two a leg can reach at all*. A child that discovered its
    // own model — `Auto`, or a hand-rolled "root when euid is 0 and the probe
    // failed" — would choose the *same* one
    // on a userns-capable host under `sudo`, so the remount path would never run
    // while the suite reported success for having tested half of the matrix.
    // Unset is still `Auto`, which is the right answer for a hand run.
    //
    // No `with_euid`: this process *is* the one being confined, so
    // `Options::euid()` reads the kernel — the production path, and the one
    // worth exercising.
    let options = match Options::from_env() {
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
    // **The host's own `/etc/passwd` *through* the old root, which is the
    // classic `pivot_root` escape.** `/.oldroot` itself survives the detach — it
    // is the mount point directory `enter` created — so a probe for the
    // directory answers `true` either way and proves nothing; a probe for a file
    // only the host's root has is `true` exactly while the old root is still
    // mounted there, which is what a dropped or broken
    // `unmount("/.oldroot", UnmountFlags::DETACH)` leaves behind.
    println!(
        "old-root-visible: {}",
        Path::new("/.oldroot/etc/passwd").exists()
    );
    // **`$HOME` itself, not `$HOME/.ssh`.** The narrower probe is `false` on a
    // host with no `~/.ssh` whether the process is confined or not, so it could
    // only ever agree with the run; the directory is `true` on the host and
    // `false` in the jail on every host, because the parent makes it and nothing
    // mirrors it in.
    println!(
        "host-home-visible: {}",
        env::var("HOME")
            .map(|home| Path::new(&home).is_dir())
            .unwrap_or(false)
    );
    match fs::write(Path::new(&root).join("bucket/should-not-exist"), b"nope") {
        Ok(()) => println!("served-root-write: ALLOWED"),
        Err(err) => println!("served-root-write: refused ({})", err.kind()),
    }
    // **Which of the two read-only implementations actually ran**, reported as
    // the one fact that separates them: the overlay arm creates `OverlayDirs`'
    // two directories under the jail root and the plain remount arm creates
    // neither, so `/.overlay/upper` inside the jail is a consequence of the
    // branch and not an inference about the host. `statfs`'s
    // `OVERLAYFS_SUPER_MAGIC` would be the inference: a served root that is
    // already an overlay — anything in a container — would read as `overlay` on
    // the remount arm. Nothing to report when nothing was exposed read-only,
    // which is why this line is absent rather than `none` on a read-write run.
    if access.is_read_only() {
        println!(
            "read-only-arm: {}",
            if Path::new("/.overlay/upper").exists() {
                "overlay"
            } else {
                "remount"
            }
        );
    }
    if let Some(state) = &state {
        match fs::write(Path::new(state).join("log.txt"), b"ok") {
            Ok(()) => println!("state-dir-write: ALLOWED"),
            Err(err) => println!("state-dir-write: refused ({})", err.kind()),
        }
    }
    // **The error's kind is reported beside the error, because those two are
    // the whole diagnosis.** A refusal is not enough on its own: `ECONNREFUSED`
    // from a probe nothing is listening on and `EPERM` from the filter are the
    // same word and opposite facts, and only the second is the network cut.
    println!(
        "outbound-connect: {}",
        match TcpStream::connect(probe.as_str()) {
            Ok(_) => "SUCCEEDED".to_string(),
            Err(err) => format!("refused ({:?}: {err})", err.kind()),
        }
    );
    println!(
        "loopback-listen: {}",
        TcpListener::bind("127.0.0.1:0").is_ok()
    );
}

/// The workspace builds on every host, so this target has to exist on the ones
/// that cannot confine: an empty `main` keeps `cargo check --workspace` and
/// `cargo clippy --all-targets` honest everywhere. Only the Linux-gated test
/// ever spawns this binary, so on any other target there is nothing to run.
#[cfg(not(target_os = "linux"))]
fn main() {}
```

- [x] **Step 2: Write the asserting parent**

`crates/tinio-sandbox-linux/tests/jail.rs`:

```rust
//! End-to-end confinement. The jail is a one-way door, so the assertions run
//! here and the jail runs in `src/bin/sandbox-child.rs`.

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Command, Stdio},
};

use tinio_sandbox_linux::Mechanism;

/// The one way past a missing precondition, and it is a deliberate opt-in.
///
/// **A suite that certifies a security boundary does not get to skip in
/// silence**: a precondition that is absent fails the run by default, so a host
/// that cannot confine can never report `2 passed` for a jail that was never
/// built. An operator who knows this machine cannot confine sets this variable
/// and means it.
const ALLOW_SKIP: &str = "TINIO_SANDBOX_ALLOW_SKIP";

/// The model the WSL2 script asked for, read the way the child reads it: the
/// same variable through the same one reader.
fn requested_model() -> Mechanism {
    Mechanism::from_env(env::var("TINIO_SANDBOX_MODEL").ok().as_deref())
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
/// `Ok(())` when the kernel granted every namespace the model needs. `Err` with
/// the `unshare(1)` invocations that answered no, spelled as an operator would
/// retype them — carried out rather than logged, because [`run_child`] is where
/// the answer decides between failing and skipping, and its message has to name
/// the command it tried.
///
/// **`unshare(1)` rather than the backend's own `userns_available()`**: that
/// probe forks, and its safety argument is that nothing else exists in the
/// process yet (design §10) — a libtest worker thread is not that place. The
/// command is the one the WSL2 script gates on, so the script and the test
/// agree by construction rather than by review. It is also a *lower* bound on
/// this binary's capability, and knowingly so: `mechanism.rs` tells operators to
/// give the binary an AppArmor profile, which confines a profiled child where
/// plain `unshare(1)` still fails — so a host can answer this probe with "no"
/// and still confine the child. That is the residual fuzziness a skip has to be
/// deliberate to cross, and it is why the opt-in above exists.
fn host_can_confine(model: Mechanism) -> Result<(), String> {
    // Each variant is asked for the namespaces it actually needs: `--user
    // --map-root-user` for the user-namespace model, and a bare `--mount` for
    // the root model, whose single capability is `CAP_SYS_ADMIN`. Asking the
    // kernel rather than reading `geteuid()` is the point — a container can run
    // as uid 0 and still refuse a mount namespace, which is a host where this
    // model would fail at the first mount rather than at the probe.
    let user: &[&str] = &["--user", "--map-root-user"];
    let mount: &[&str] = &["--mount"];
    let probes: &[&[&str]] = match model {
        Mechanism::UserNamespace => &[user],
        Mechanism::Root => &[mount],
        Mechanism::Auto => &[user, mount],
    };
    if probes.iter().any(|flags| unshare_probe(flags)) {
        return Ok(());
    }
    Err(probes
        .iter()
        .map(|flags| format!("unshare {} true", flags.join(" ")))
        .collect::<Vec<_>>()
        .join(" and "))
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

/// Spawn the confined child and return its report.
///
/// **It fails rather than skips when this host cannot confine**, unless
/// [`ALLOW_SKIP`] was set deliberately. `eprintln!` is not enough on its own:
/// libtest swallows it without `--nocapture`, so the "loud" skip of the earlier
/// shape was a silent one on every ordinary invocation.
///
/// The skip, when it is taken, is decided before the child exists, so
/// everything the child reports afterwards is a failure (design §12).
fn run_child(
    root: &Path,
    state: Option<&Path>,
    home: &Path,
    access: &str,
    probe: SocketAddr,
) -> Option<String> {
    let model = requested_model();
    if let Err(tried) = host_can_confine(model) {
        assert!(
            env::var(ALLOW_SKIP).is_ok_and(|value| !value.is_empty()),
            "this host cannot confine under {model:?}: the precondition `{tried}` failed, so this \
             run would certify a jail that was never built. WSL2 is where both legs are verified \
             (design §12). Set {ALLOW_SKIP}=1 to skip deliberately on a host that genuinely \
             cannot confine."
        );
        eprintln!(
            "skipping: this host cannot confine under {model:?} and {ALLOW_SKIP} is set (design \
             §12)"
        );
        return None;
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_sandbox-child"));
    command
        .env("TINIO_SANDBOX_TEST_ROOT", root)
        .env("TINIO_SANDBOX_TEST_ACCESS", access)
        .env("TINIO_SANDBOX_TEST_PROBE", probe.to_string())
        // **`HOME` is set here rather than inherited**, so the child's
        // `host-home-visible` line has one answer on every run: the directory it
        // probes is the one the parent made for it, which exists on the host and
        // is never mirrored into the jail.
        .env("HOME", home);
    if let Some(state) = state {
        command.env("TINIO_SANDBOX_TEST_STATE", state);
    }
    let output = command.output().expect("spawn confined child");
    let log = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        !log.contains("bad-model"),
        "the child refused TINIO_SANDBOX_MODEL:\n{log}"
    );
    assert!(
        !log.contains("confine-failed"),
        "this host can confine under {model:?} but the child did not:\n{log}\nchild stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Some(log)
}

/// Bind the listener the child is told to connect to, and hand back its address.
///
/// **The listener is the parent's own, and that is what makes the outbound
/// assertion a real one.** A fixed address on the public internet is unreachable
/// on an offline host, and a suite that passes when its probe is unreachable
/// passes with `seccomp::install()` deleted. Loopback is reachable by
/// construction — the child's own `loopback-listen: true` proves the namespace
/// is shared — so the assertion runs everywhere and cannot be skipped for an
/// environmental reason. The filter denies `connect` by syscall number and not
/// by address, so loopback is refused for exactly the reason a public address
/// was.
///
/// **The returned listener must outlive the child**, and the caller's binding
/// does: dropping it first would leave nothing to connect to, and the child
/// would report `ConnectionRefused` — a different kind, which the assertion
/// separates from the filter's `PermissionDenied` on purpose.
fn probe_listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind the probe listener");
    let addr = listener.local_addr().expect("probe address");
    (listener, addr)
}

#[test]
fn the_jail_confines_the_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    let home = tmp.path().join("home");
    fs::create_dir_all(root.join("bucket")).expect("create root");
    fs::create_dir_all(&home).expect("create the home the child probes");
    fs::write(root.join("bucket/obj.txt"), "hello from the served root\n").expect("seed");

    let (_listener, probe) = probe_listener();

    let Some(log) = run_child(&root, None, &home, "read-write", probe) else {
        return;
    };

    assert!(log.contains("read-ok: hello from the served root"), "{log}");
    assert!(log.contains("host-etc-visible: false"), "{log}");
    assert!(log.contains("old-root-visible: false"), "{log}");
    assert!(log.contains("host-home-visible: false"), "{log}");
    assert!(log.contains("served-root-write: ALLOWED"), "{log}");
    assert!(log.contains("loopback-listen: true"), "{log}");
    // Unconditional, and the kind is part of the assertion: the address is a
    // listener this process holds, so `ECONNREFUSED` would mean the listener was
    // gone and `SUCCEEDED` would mean the filter was never installed. Only
    // `PermissionDenied` is the network cut.
    assert!(
        log.contains("outbound-connect: refused (PermissionDenied"),
        "{log}"
    );
}

#[test]
fn a_read_only_jail_keeps_the_served_root_unwritable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    let state = tmp.path().join("state");
    let home = tmp.path().join("home");
    fs::create_dir_all(root.join("bucket")).expect("create root");
    fs::create_dir_all(&state).expect("create state");
    fs::create_dir_all(&home).expect("create the home the child probes");
    fs::write(root.join("bucket/obj.txt"), "hello\n").expect("seed");

    let (_listener, probe) = probe_listener();

    let Some(log) = run_child(&root, Some(&state), &home, "read-only", probe) else {
        return;
    };

    assert!(log.contains("read-ok: hello"), "{log}");
    assert!(log.contains("served-root-write: refused"), "{log}");
    assert!(log.contains("state-dir-write: ALLOWED"), "{log}");
    // **Which arm ran is asserted to be one of the two, never to be a
    // particular one.** The choice is the host's locked mount flags' answer and
    // not the model's, so requiring `overlay` under `userns` would be a
    // host-dependent test — and it is exactly the mistake the earlier shape made
    // in the other direction, by assuming which arm ran and never looking. What
    // this proves is that the child reached a read-only mechanism and said
    // which; which one it is on this host is what the phase's own script reads
    // out of the line.
    assert!(
        log.contains("read-only-arm: remount") || log.contains("read-only-arm: overlay"),
        "{log}"
    );
    // **The parent reports the arm rather than only accepting it.** Membership
    // is the assertion; which arm it was is the fact that separates this leg
    // from the other one, and a run whose output is `2 passed` twice cannot tell
    // two mechanisms exercised from one exercised twice. Printed here, read out
    // of the run by the WSL2 script, which runs this leg uncaptured
    // (`RUST_TEST_NOCAPTURE=1`) for exactly this line.
    println!(
        "{}",
        log.lines()
            .find(|line| line.starts_with("read-only-arm: "))
            .unwrap_or("read-only-arm: ABSENT")
    );
    // The escape is the same in both configurations, and this is the leg whose
    // read-only mechanism differs, so the jail's integrity is asserted here too.
    assert!(log.contains("old-root-visible: false"), "{log}");
    assert!(
        log.contains("outbound-connect: refused (PermissionDenied"),
        "{log}"
    );
    // The served root is untouched on the host too — the overlay's upper lives
    // in the jail's tmpfs, never in the data (design §4).
    assert!(!root.join("bucket/should-not-exist").exists());
}
```

- [x] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-linux --test jail`
Expected: FAIL on `served-root-write` or `host-etc-visible` until the mount table is right — the test compiles from Task 5 onward, so the failure is behavioural, not a compile error.

**This expectation did not reproduce, and nothing was manufactured to make it.** The suite was **green on its first execution**, because Tasks 1-5 had already built every piece it drives: the brief's expectation assumed an incremental build in which the mount table was still wrong, and by the time this task ran there was no such state. This is inherent to how the phase splits its tasks — a task whose deliverable is *evidence about earlier tasks* cannot have a red state of its own — so the step is recorded as run-and-green rather than as a red that was seen. **What it costs is the usual proof that a test can fail**, which is why this task's review carries a heavier question than most — *can this pass while the confinement is broken?* — and why the answer was four concrete edits (the four fixes Step 2's block already carries). The falsification was made afterwards and by other means: with a stub `unshare` first on `PATH` the suite reports `0 passed; 2 failed` with the precondition message; the same stub plus `TINIO_SANDBOX_ALLOW_SKIP=1` restores `2 passed` and prints `skipping:`; and a root filesystem bind-mounted on a directory makes the `old-root-visible` probe report `true`, so it is sensitive to the escape it exists to catch. **That is the standard for a test of a security boundary: a demonstration that it can fail.**

If the child reports `confine-failed: sandbox step \`unshare user namespace\` failed: … EINVAL`, the process was multi-threaded: check that this really is `src/bin/sandbox-child.rs` (a plain `fn main`) and not a re-executed libtest binary, and that no dependency has started a worker thread before `confine`.

**`EPERM` on the same step is a different failure with a different cause**: the kernel refused the user namespace itself — on Ubuntu 24.04+ that is usually `kernel.apparmor_restrict_unprivileged_userns=1`, which the `Resolved::Unavailable` message names. On a host that forbids it outright the test never gets this far: `host_can_confine` asks `unshare(1)` first and skips, so a child that still fails here means the two answers disagree — and that is a finding rather than a skip, because the AppArmor restriction is attached to the *binary being confined*, so a host can grant the probe and refuse the jail. The assertion is there to make that difference readable instead of filtered.

- [x] **Step 4: Make it pass**

If `host-etc-visible` is `true`, or `old-root-visible` is: `/oldroot` was not detached: check the `umount("/.oldroot", UnmountFlags::DETACH)` and that the working directory moved off the old root before it — a process whose cwd is inside the old root pins it. **`old-root-visible` is the probe that actually catches this** — it asks for the *host's* `/etc/passwd` through `/.oldroot`, and the original `host-etc-visible` alone would stay `false` on a process that kept the old root mounted but never looked through it.

If `outbound-connect` is `SUCCEEDED`, or `refused` with any kind other than `PermissionDenied`, the seccomp filter is not installed: check that `seccomp::install()` is the last call in `enter` and that `Arch::current()` returned `Some`. **The probe address is a listener the parent bound and passed in**, so `ConnectionRefused` means the listener was gone and `SUCCEEDED` means the filter was not installed; only `PermissionDenied` is the network cut. That is also why an unreachable probe address is no longer a reason to skip the assertion — the old fixed `1.1.1.1:53` probe made the check pass on any host with no route to it, which the assertion cannot do now.

If the child fails on the first `mkdir` with `EOVERFLOW`, the gid map was not written: an unmapped gid makes file creation fail even though `mount` and `pivot_root` succeed.

If the child reports `create jail root in $TMPDIR` with `EACCES`, `EROFS` or `ENOSPC`, the temp directory is the problem and the exposures are not: the jail is created under `std::env::temp_dir()` — `TMPDIR` unless `Options::with_tmpdir` says otherwise, and **not** under the test's own `tempfile::tempdir()`, which is the served root — so a read-only, missing or full one fails before any mount exists. Point `TMPDIR` at a directory the child can write, or pass `with_tmpdir`.

If a `mount --bind` fails with `EINVAL`, a bare `MS_BIND` reached a mount-point source: check that `apply_mount` calls `mount_bind_recursive` and not `mount_bind`.

If `state-dir-write` is refused, the state directory was not exposed, or the overlay's read-only mount covered it — the state directory must never be overlaid (Task 2 pins this).

- [x] **Step 5: Verify under both models**

Run:
```bash
TINIO_SANDBOX_MODEL=userns RUST_TEST_NOCAPTURE=1 cargo test -p tinio-sandbox-linux --test jail
sudo env "PATH=$PATH" TINIO_SANDBOX_MODEL=root RUST_TEST_NOCAPTURE=1   cargo test -p tinio-sandbox-linux --test jail
```
Expected: PASS both times, and **the two runs must be told apart by their own output rather than by the model they were asked for.** `RUST_TEST_NOCAPTURE=1` is what puts the child's `read-only-arm:` line on the terminal — libtest swallows a passing test's stdout otherwise, and `2 passed` twice is exactly what a run that exercised one mechanism twice looks like. Measured on the verification host: the unprivileged leg reports `overlay` and the privileged leg reports `remount`, which is the pair `strace` of the real `sandbox-child` shows, reproduced by the suite. **(2026-09-24: that pair is the pre-fix one. The remount re-asserts the source mount's locked per-mount flags now, so the unprivileged leg reports `remount` as well and the overlay branch loses its end-to-end leg — the test still passes, because it asserts the value is one of the two and never which; design §4 records the change and the gap.)** The mechanism is the **source mount's locked per-mount flags** and not the model (the paragraph above the child says it in full): the served root is a tempdir under `/tmp`, whose tmpfs is `nosuid,nodev`, so its per-mount flags are locked in a namespace owned by another user namespace and the unprivileged remount is refused with `EPERM`; the privileged leg's namespace is owned by the same user namespace, nothing is locked, and the same call returns `0`. No unit test can do either — the branch needs a filesystem, not a fixture (Task 5's test pins the errno reading instead) — and this is where `apply_mount`'s `EPERM` branch gets its end-to-end coverage, `upperdir` on the namespace-owned tmpfs and the lower left byte-for-byte untouched. **The two runs are guaranteed to differ because the child reads the variable rather than discovering a model for itself** — without the second line, a `sudo` leg on a userns-capable host would take the same path as the first and the remount would go untested. **`sudo env` and not `sudo -E`**: `-E` is ignored where sudoers refuses to preserve the environment, which would leave `TINIO_SANDBOX_MODEL` unset and run the ordinary model under root — the same silent collapse of two legs into one, from the other direction. Both must pass — this is P1's acceptance gate (design §12).

---

### Task 7: Restructure `serve` and gate it behind the feature

**Files:**
- Modify: `crates/tinio-server/Cargo.toml`
- Modify: `crates/tinio-server/examples/serve.rs`

**Interfaces:**
- Consumes: `tinio_sandbox::{Backend, Confinement, Event, Exposure, LaunchPlan, Sandbox, linux, supervise}` — `Event` ungated, the rest through the gates above — plus `AccessMode` from `tinio_core::mode` and the file's existing `Args`/`FsStorage`/`Config`.
- Produces: nothing the library uses.

`unshare(CLONE_NEWUSER)` returns `EINVAL` in a multi-threaded process (verified), and `#[tokio::main]` builds its runtime inside `main`. The entry point therefore becomes a plain `fn main` that confines first and builds the runtime second (design §10).

- [x] **Step 1: Add the feature, and no gate on the example**

`crates/tinio-server/Cargo.toml` — add to `[dependencies]`, alphabetically after `tinio-core.workspace = true`:

```toml
tinio-sandbox.workspace = true
```

**Not `optional`, and that is decision 4 taken literally rather than a convenience.** Decision 4 says "default-on means compiled in, not enabled": the sandbox code is in every build and only *running* it is opt-in. It also keeps decision 19 consistent — `serve` emits its readiness frame on stdout in every build, including one without the feature, and `Event` comes from this crate. An `optional` dependency would put the readiness line itself behind the feature, so a `--no-default-features` build could not serve the harness at all.

Add to `[features]` a feature that gates **behaviour**, not compilation:

```toml
sandbox = []
```

Add `"sandbox"` to the `default` list **and nothing else in this file** — in particular **no `[[example]]` block for `serve`, and no `required-features` anywhere on it.** This step's earlier spelling added

```toml
[[example]]
name = "serve"
required-features = ["sandbox"]
```

and that is a defect three ways. It contradicts design decision 4 (the example must build and serve in **every** feature configuration; what the feature gates is the `--sandbox` arm's body) and decision 19 (the readiness frame is written by a build without the feature too). It broke a CI leg that was already there: `.github/actions/run-test-leg/action.yml`'s `no-default` mode builds `--example serve --no-default-features`, and an explicit `--example` selection whose `required-features` are unmet is a **hard error, exit 101** — while `cargo test --workspace --no-default-features`, which is what this phase's own gate ran, **silently skips** such a target, so the gate could not see the breakage it had caused. And it made the `#[cfg(not(feature = "sandbox"))]` refusal arm in `main` unreachable: with the block in place no build could produce that arm, so the decision-5 refusal existed on paper only. **`docs/tests.md`'s CI-legs section carries the rule** — `sandbox_e2e` may require the feature; the `serve` example may not — and there is now a test for the refusal arm in `serve_read_only.rs`.

**Three manifest lines come with the parser, and they replace the hand-rolled loop rather than sitting beside it.** In the root `Cargo.toml`'s `[workspace.dependencies]`:

```toml
clap = { version = "4", default-features = false }
```

and in `tinio-server`'s **`[dev-dependencies]`**, alphabetically after `bytes`:

```toml
clap = { workspace = true, features = [
    "derive",
    "error-context",
    "std",
    "suggestions",
    "usage",
] }
```

**The feature list is explicit because the workspace pin turns the default set off**, and each of the five is load-bearing for something this file does: `derive` for the attribute itself, and `error-context`/`std`/`suggestions`/`usage` because what `refuse_command_line` prints *is* clap's rendering — the sentence, the suggestion and the usage line. Dropping `usage` does not fail a build; it silently strips the usage line out of the refusal an operator reads.

**It is a dev-dependency because only the example parses arguments**, and the library build is what every dependent of `tinio-server` pays for. Measured in a fresh target directory so a `Compiling clap` line means what it says: `cargo check -p tinio-server` compiles no clap at all, while `--all-targets` and the example build do; `cargo tree -e normal` lists none and `-e normal,dev` lists five. When the CLI carries this file, `clap` becomes a real dependency of that crate, which is its honest home.

**There is no `"help"` feature, and the reason is worth keeping.** Help is disabled at the command level, so it is tempting to conclude the feature is dead — but the mechanism is the other way round: `disable_help_flag` is what keeps `-h`/`--help` from existing (clap's `_check_help_and_version` pushes them whenever the *setting* is unset, feature or no feature), while the feature gates only the help *renderer*. So dropping it changes nothing observable, which the run confirms: eleven refusal cases come back byte-identical with and without it, and the only difference anywhere is 936 bytes off the example binary. **The check that settled it was reading clap's source to find which mechanism kept the flag alive, not assuming the feature was unused because the flag was gone** — and the general form is the one this phase kept meeting: before removing something because it looks unused, find out what was keeping it alive.

**There is no target-gated dependency on the platform crate, and THIS STEP'S EARLIER SPELLING ADDED ONE — that is the correction, not an omission.** `serve` needs `Options` (the model and the `tmpdir` knob are platform options, deliberately outside P0's shared surface) and the facade re-exports only `Backend as Platform` and `backend as platform` (P0), so the brief concluded that `tinio-server` had to name `tinio-sandbox-linux` itself and added:

```toml
[target.'cfg(target_os = "linux")'.dependencies]
tinio-sandbox-linux.workspace = true
```

**Do not add that table, and remove it if it is already there.** It contradicts the facade's own module doc — *"Consumers name this crate and nothing else; the platform split stays behind the selection below"* — and, more decisively, it does not scale: by the time P3 lands, `tinio-server` would carry one target-gated dependency per platform while all three arms of `main` live in the same function. **The facade publishes each platform's crate under a short alias instead**: `pub use crate::_linux as linux;` beside its existing `Backend as Platform` / `backend as platform` re-exports (and the same pair for each platform as those land), and `serve.rs` writes `use tinio_sandbox::linux;` under its own `#[cfg(all(feature = "sandbox", target_os = "linux"))]`. **Design decision 2 is not violated by this**, which is the objection worth answering: it forbids the facade's *shared* surface becoming the union of three platforms' settings, and `tinio_sandbox::linux::Options` is still Linux's own type in Linux's own crate, reached through a cfg-gated path — the intersection the facade shares does not change. The alternative — re-exporting a type *named* for its platform from the shared surface — is the union-of-names that decision 2 rejects. (The design's own sentence saying a backend's settings "are reached by importing `tinio-sandbox-linux` directly" is made false by this and belongs to the design document's correction, not to this plan.)

- [x] **Step 2: Write the failing test**

Append to the existing `#[cfg(test)] mod tests` at the bottom of `crates/tinio-server/examples/serve.rs`:

```rust
    #[test]
    fn exposures_carry_the_mode_and_skip_an_in_root_state_dir() {    fn exposures_carry_the_mode_and_skip_an_in_root_state_dir() {
        let root = Path::new("/srv/data");
        let inside = Path::new("/srv/data/.tinio");
        let outside = Path::new("/home/u/.tinio/roots/h");

        let exposures = sandbox_exposures(root, inside, AccessMode::ReadWrite);
        assert_eq!(
            exposures.len(),
            1,
            "an in-root state dir is already covered by the root"
        );
        assert_eq!(exposures[0].host, root);
        assert_eq!(exposures[0].access, AccessMode::ReadWrite);

        let exposures = sandbox_exposures(root, outside, AccessMode::ReadOnly);
        assert_eq!(
            exposures.len(),
            2,
            "an out-of-root state dir needs its own entry"
        );
        assert_eq!(exposures[0].access, AccessMode::ReadOnly);
        assert_eq!(
            exposures[1].access,
            AccessMode::ReadWrite,
            "the state dir is always writable"
        );
    }

    #[test]
    fn parse_args_recognises_the_sandbox_flag() {
        // The flag and the root path are both bare arguments, so a parser that
        // stops looking at position one would serve `--sandbox` as a directory
        // name — a failure that is invisible until someone reads their filesystem.
        let args = parse_args([OsString::from("/srv/data"), OsString::from("--sandbox")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(args.sandbox);

        let args = parse_args([OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(!args.sandbox);

        // The flag is recognised wherever it appears, not only after the root.
        let args = parse_args([OsString::from("--sandbox"), OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.root, Path::new("/srv/data"));
        assert!(args.sandbox);
    }

    #[test]
    fn a_missing_root_is_a_usage_error() {
        assert!(parse_args([OsString::from("--sandbox")]).is_err());
    }

    #[test]
    fn parse_args_resolves_the_address_the_server_binds() {
        // Three plans read `args.address`, so the resolution lives in one place
        // and is pinned there: `--address` over `--port` over 9000.
        let args = parse_args([OsString::from("/srv/data")]).unwrap();
        assert_eq!(args.address, SocketAddr::from(([127, 0, 0, 1], 9000)));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--port"),
            OsString::from("0"),
        ])
        .unwrap();
        assert_eq!(args.address, SocketAddr::from(([127, 0, 0, 1], 0)));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--port"),
            OsString::from("1234"),
            OsString::from("--address"),
            OsString::from("0.0.0.0:8080"),
        ])
        .unwrap();
        assert_eq!(
            args.address,
            SocketAddr::from(([0, 0, 0, 0], 8080)),
            "--address outranks --port"
        );
    }

    #[test]
    fn help_and_version_are_unknown_arguments() {
        // **The disabled flags are the stdout contract's cheapest half**: clap
        // prints the help and the version it generates to stdout, so this
        // command has none to print and a request for either is refused like
        // any other unknown argument. A parser that answered here would also be
        // one that chose its own stream and exit code.
        for flag in ["--help", "-h", "--version"] {
            assert!(
                parse_args([OsString::from("/srv/data"), OsString::from(flag)]).is_err(),
                "{flag} must be refused rather than answered"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_path_that_is_not_utf8_is_accepted() {
        // The reason `Args.root` is a `PathBuf` and not the `String` it used to
        // be: a path that is not valid UTF-8 is legal on Linux, and `--config`
        // names one just as a root does. Unix-gated because the bytes are how
        // Unix spells a path (Windows spells it in wide units).
        let root = OsString::from_vec(b"/srv/d\xffata".to_vec());
        let args = parse_args([root.clone()]).unwrap();
        assert_eq!(args.root, PathBuf::from(&root));

        let args = parse_args([
            OsString::from("/srv/data"),
            OsString::from("--config"),
            root.clone(),
        ])
        .unwrap();
        assert_eq!(args.config_path, Some(PathBuf::from(&root)));
    }

    #[test]
    fn the_sandbox_asks_for_the_device_files() {
        // **Through `sandbox_request`, which is what `main` calls.** The flag is
        // an argument of the construction rather than something the set
        // remembers, so a test that built its own `Sandbox` would stay green
        // while the shipped one lost its device files: flipping the argument to
        // `false` has to fail *this* test, and it does.
        let sandbox = sandbox_request(
            Path::new("/srv/data"),
            Path::new("/srv/data"),
            AccessMode::ReadWrite,
        );
        assert!(
            sandbox.device_files_enabled(),
            "the Linux jail needs /dev/null and friends (design decision 12)"
        );
        assert_eq!(sandbox.exposures().len(), 1);
    }

    /// **The two `Options` fields `enter` decides the confinement from, and the
    /// test the phase was missing.** Nine reviews passed over the shipped
    /// `linux::Options::from_env()` — which sets neither — because every test
    /// in this file pinned the `Sandbox` and nothing pinned the options: the
    /// device-file flag is a fact about the request, the mode and the
    /// `local_uid` mapping are facts about what the process is allowed to do
    /// once it is inside. Both assertions below are the production call, not a
    /// rebuilt `Options`, for the same reason `sandbox_request` is called
    /// rather than a hand-built `Sandbox`: flipping either argument has to fail
    /// *this* test.
    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    #[test]
    fn the_sandbox_options_carry_the_resolved_mode_and_the_uid_mapping() {
        // Read-only with a mapping: the run that needs both, and the pair whose
        // defaults are wrong in opposite directions — a read-write exposure
        // where FR-023 asked for a read-only one, and no `CAP_CHOWN` where
        // design §8 promises one.
        let config =
            Config::parse("version = 1\n[owner]\nlocal_uid = 1005").expect("a config with a map");
        let options = sandbox_options(
            linux::Options::default(),
            &Some(config),
            AccessMode::ReadOnly,
        );
        assert_eq!(
            options.served_root_access(),
            AccessMode::ReadOnly,
            "a `--read-only` run must expose the served root read-only inside the jail"
        );
        assert!(
            options.local_uid_configured(),
            "a configured `local_uid` mapping must retain CAP_CHOWN (design §8)"
        );

        // And the no-mapping default, so the assertion above is about the
        // config rather than about the setter being stuck on.
        let options = sandbox_options(linux::Options::default(), &None, AccessMode::ReadWrite);
        assert_eq!(options.served_root_access(), AccessMode::ReadWrite);
        assert!(!options.local_uid_configured());
    }

    #[test]
    fn the_executable_directory_is_not_exposed_on_linux() {
        // Linux confines the process in place, so its image is already mapped and
        // needs no exposure (design decision 22). Read off the request `main`
        // builds, for the same reason the flag is: an exposure only the tests
        // add is an exposure the server does not have.
        let exe_dir = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let sandbox = sandbox_request(
            Path::new("/srv/data"),
            Path::new("/srv/data"),
            AccessMode::ReadWrite,
        );
        assert!(!sandbox.exposures().iter().any(|e| e.host == exe_dir));
    }
}
```

**Three of these tests arrived with the `clap` parser and pin its configuration rather than its parsing**: `parse_args_resolves_the_address_the_server_binds` (the `--address` > `--port` > 9000 fold that three plans read), `help_and_version_are_unknown_arguments` (the disabled flags, which is what keeps clap off stdout), and `a_path_that_is_not_utf8_is_accepted` (the `PathBuf` value parser passing bytes through). The last one needs `#[cfg(unix)] use std::os::unix::ffi::OsStringExt as _;` in the test module, beside its `use super::*;`.

- [x] **Step 3: Run the test to verify it fails**

Run: `cargo test -p tinio-server --example serve`
Expected: FAIL — `cannot find function 'sandbox_request'`.

- [x] **Step 4: Write the helper**

In `crates/tinio-server/examples/serve.rs`, add near the other helpers:

```rust
/// The paths the confined server must reach/// The paths the confined server must reach. The served root carries the
/// access the mode dictates; the state directory is always read-write, because
/// the access log, the redb metadata and the staging areas live there. An
/// in-root state directory needs no entry of its own — the served root's
/// exposure already covers it (design §9).
/// The confinement request `serve` makes, built from the paths the operator
/// gave and the mode that was resolved for them.
///
/// **One name for the construction, because the device-file flag is an argument
/// rather than something a rebuilt set remembers.** With the call inline in
/// `main` and a test building its own `Sandbox`, flipping the flag to `false`
/// would leave every test green while the jail silently lost `/dev/null` and
/// friends — design decision 12's whole point, and invisible until something
/// reads a device file. The test is about the code that runs only if the test
/// calls the code that runs.
// The gate is the two places that read this: the Linux arm, and the tests
// below — which exercise it on every platform, and are what keeps the exposure
// rule a property this file can lose. A build with neither would otherwise
// carry a dead function, and every build compiles this file now that the
// example has no `required-features`.
#[cfg(any(test, all(feature = "sandbox", target_os = "linux")))]
fn sandbox_request(root: &Path, state_dir: &Path, access: AccessMode) -> Sandbox {
    Sandbox::from_exposures(sandbox_exposures(root, state_dir, access), true)
}

/// The one name for the construction of this file's confinement: the Linux
/// backend's options, with the environment's model and the two facts `enter`
/// reads carried onto them.
///
/// **Both facts are resolved here rather than at the call site, because
/// `enter` reads them off `Options` and neither default is right for a
/// server.** `enter` takes the exposure set from `served_root_access` and the
/// retained capability set from `(served_root_access, local_uid_configured)`;
/// with the defaults, a `--read-only` run handed the jail a **read-write**
/// served root — the kernel-level half of FR-023, absent with nothing to say
/// so — and `caps::retained` always returned `&[]`, so the single `CAP_CHOWN`
/// design §8 promises for an FR-033 `local_uid` mapping was never retained and
/// the first `chown` of a landed object failed `EPERM` with no capability to
/// fall back on. `access` is the same **resolved** value the exposure set uses
/// — the config file's `[server] read_only`, `TINIO_READ_ONLY` and the flag
/// already folded together — and the mapping is asked of
/// `identity::owner_uids_for`, the same function the storage's chown map comes
/// from, so the capability this keeps and the chowns it performs cannot
/// disagree about whether a mapping exists.
///
/// `base` rather than a fresh `Options` so the caller's `from_env` — the
/// model, and the refusal that names a bad `TINIO_SANDBOX_MODEL` — is not
/// re-derived or lost here.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
fn sandbox_options(
    base: linux::Options,
    config: &Option<Config>,
    access: AccessMode,
) -> linux::Options {
    let mapping_configured =
        !identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())).is_empty();
    base.with_served_root_access(access)
        .with_local_uid(mapping_configured)
}

#[cfg(any(test, all(feature = "sandbox", target_os = "linux")))]
fn sandbox_exposures(root: &Path, state_dir: &Path, root_access: AccessMode) -> Vec<Exposure> {
    let mut sandbox = Sandbox::new().expose(root, root_access);
    if !state_dir.starts_with(root) {
        sandbox = sandbox.expose(state_dir, AccessMode::ReadWrite);
    }
    sandbox.exposures().to_vec()
}
```

with, at the top of the file — **through the facade, and gated per target**, which is what the alias above buys:

```rust
use clap::Parser;
use tinio_sandbox::Event;
// The platform's own vocabulary — `Options`, `Backend`, `Seccomp` — reached
// through the facade rather than by naming the backend crate: a consumer that
// named it would carry one target-gated dependency per platform by the time all
// three arms of `main` exist, and the facade's own rule is that consumers name
// it and nothing else.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use tinio_sandbox::linux;
// Driving a confinement — the trait, its answer, the plan and the supervision —
// is read only by the arm below, and that arm exists for Linux: a target whose
// arm another plan owns refuses the flag and reads nothing else yet.
#[cfg(target_os = "linux")]
use tinio_sandbox::{Backend, Confinement, LaunchPlan, supervise};
// Reading a `Sandbox` is `sandbox_exposures`' business and nobody else's, and
// that helper is read by the Linux arm and by the tests below. `Event` above is
// unconditional because every build writes frames, feature or platform
// regardless (design decision 19).
#[cfg(any(target_os = "linux", test))]
use tinio_sandbox::{Exposure, Sandbox};
use tokio::sync::watch;
```

**`Event` is the one import that carries no gate**, and the reason is decision 19 rather than taste: every build writes frames, feature or platform regardless, so a gate on it would take the readiness frame away from a `--no-default-features` build. The other three are gated because what they name exists only on Linux, and `sandbox_exposures`' gate is `any(target_os = "linux", test)` so the exposure rule stays a property the tests can exercise on any host.

**`net::TcpListener as StdTcpListener` is one more line in the file's `std` group**, and it is the correction's rather than this step's: `serve`'s `inherited` parameter is `std`'s listener, because a launcher's child rebuilds its socket before its runtime exists and `TcpListener::from_std` panics outside one — so the child has to hand over what it has, and the body converts. `TcpListener`, unqualified, is `tokio`'s in this file, which is why the `std` one is aliased. `OsString`, the `io` names and the `watch` import are additions; `std::process` is **not** — the file already imports it in its grouped `use std::{…, process, …}` and `refuse_command_line` calls `process::exit(2)`, so a second `use std::process;` would be a duplicate-import error rather than a convenience. **`use clap::Parser;` is one more addition**, and it sits with the third-party imports rather than in the `std` group. Merge the new names into the file's existing groups if you keep them grouped; `io::{Write as _, stdout}` is what `emit` writes and flushes the frame with, and `watch` is the signature's last parameter — this phase only *names* the type, because the channel is the launcher platforms' to create (and tokio's `sync` feature is already in this crate's feature list, so it is an import rather than a manifest change). **The `io` names sit in the file's existing grouped `use std::{…}` rather than on a line of their own**, which is what `imports_granularity = "Crate"` produces and what `fmt --check` requires.

**The two blocks above are the step's record, and the tree carries one of these two functions.** `sandbox_request` is the name that landed — `crates/tinio-cli/src/sandbox/mod.rs`'s `pub(crate) fn sandbox_request`, the entry point having since moved out of the example — and it inlines the exposure rule and applies `.device_files()` there; `fn sandbox_exposures` has no definition in the tree, and `2026-09-18-sandbox-shared.md` §4 forbids adding one — a caller that rebuilt a `Sandbox` from a returned exposure vector would be a second definer of the rule. Every other mention of `sandbox_exposures` in this plan — the Step 2 test block and the import/gate comments above included — is that superseded name.

- [x] **Step 5: Replace `#[tokio::main]` with the two-phase entry point**

Change the attribute to `fn main() -> Result<(), Box<dyn Error + Send + Sync>>` and split the body.

**This step is written as invariants, not as a diff to apply blind.** The read-only work (`--read-only`, `TINIO_READ_ONLY`, `PlaneConfig`, `AccessMode`, the state-directory relocation) has **landed** — commit `a46a1d1` — and it is now part of the file this restructure has to carry across, not a branch it merges with. So the shapes below are the *seams*: each one has a contract, and the code shows the shape a seam must end up in rather than the exact bytes at today's line numbers. Re-read the file before editing, and keep every one of read-only's decisions intact.

Five invariants, in the order they must hold:

1. **No thread exists before `confine`** — the entry point is a plain `fn main` and the runtime is built afterwards (design §10, decision 26).
2. **Everything that touches a host path happens before the door closes — as far as that is possible, and the limit is a measured one.** The argument parse, `Config::load`, the read-only resolution, `fs::create_dir_all(root)` where the mode allows it, `identity::ensure_chown_privilege`, the state-directory resolution **as a path**, and the log subscriber all run before `confine`. **`Pipelines::build` and `FsStorage::new` cannot, and the earlier spelling of this invariant asserted that they could.** `Pipelines::build` starts three multi-threaded tokio runtimes with eager worker threads, and `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller — reproduced: with the lanes built before the arm, `--sandbox` dies with ``sandbox step `unshare user namespace` failed: Invalid argument``, which is `EINVAL`, the exact signature of invariant 1 below. **So invariant 1 and invariant 2 were in conflict as written and the plan asserted both without noticing.** The resolution preserves each one's *purpose* rather than its letter: the state directory is resolved **as a path** before the door — through the same `tinio_fs::state_dir(&fs::canonicalize(root))` the storage itself calls, so the exposure set and the access-log path are known while the host is still visible — and `Pipelines::build` and `FsStorage::new` run **after** the arm, inside the jail, where the served root and the state directory are both exposed at their original paths anyway. **Why that is sound rather than a workaround**: what the original ordering protected was that the *bind sources exist* before the mounts are planned, and the only bind sources are the served root and the state directory, both now resolved as paths beforehand. One consequence, measured and fail-closed: the read-only-plus-in-root-state-dir refusal now surfaces from inside the jail.
3. **`--sandbox` is never silently ignored** (decision 5): a build without the feature refuses it, and so does a build for a platform whose backend another plan owns.
4. **stdout carries events and nothing else** (decision 19): the endpoint line becomes one flushed SSE frame, and every log stays on stderr.
5. **The check that needs a privilege runs before the privilege is dropped** — `ensure_chown_privilege` probes for `CAP_CHOWN`, which the confinement is about to remove.

**The read-only work has landed, and every part of it has to survive this restructure**: the `--read-only` flag and its place in the usage text, `TINIO_READ_ONLY` through the CLI's clap `env` on the flag (the plan's `sources::env_read_only`, deleted from `sources.rs` on 2026-09-24; the code blocks below keep that spelling as the plan's record), the `AccessMode` it resolves, the **two `if !read_only` guards** (the root is not created and the chown probe does not run in read-only mode — the probe writes into the served root, which is the one thing that mode promises to leave alone), the state-directory relocation through `state::env_state_dir` (deleted with that reader on 2026-09-24; `TINIO_STATE_DIR` is a hidden clap `env` argument) and `state::default_home_state_dir`, and `plane_for`/`Capabilities` → `PlaneConfig`. The state directory is read from `storage.state_dir()` below precisely so that relocation keeps working.

**There is no `Args` struct to extend, and that is worth stating rather than discovering.** When this step was written the file parsed its arguments as locals in `main` (`root`, `address`, `config_path`, `read_only_flag`) inside a `while let Some(arg) = args.next()` loop, so `Args` was *created* by this step and gained a field for each of those four plus `sandbox` — one of them, `read_only`, being read-only's. **The loop is now `clap`'s** (see the parser above): `Args` keeps exactly those five names and types, `RawArgs` is the flag-for-field mirror of them, and `parse_args` is the only place the two meet — so a later phase adding a flag adds it to `RawArgs` and to `Args`, and to nothing else in this file.

```rust
fn main() -> Result<(), Box<dyn Error + Send + Sync>> {fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = match parse_args(env::args_os().skip(1)) {
        Ok(args) => args,
        // One arm for every bad command line there is: a flag nothing matches
        // (`--help` and `--version` among them), a missing root, a value clap
        // will not read. `refuse_command_line` is where the two streams and the
        // exit code are decided.
        Err(err) => refuse_command_line(err),
    };

    // A build without the feature refuses the request rather than ignoring it
    // (design decision 5): silently running unconfined is the failure this
    // whole design exists to prevent. **This arm is reachable, and it is the
    // example's own build that reaches it**: the example carries no
    // `required-features`, because a `--no-default-features` build must still
    // serve the harness and what the feature gates is only the `--sandbox`
    // arm's body (design decisions 4 and 19) — so `serve <root> --sandbox`
    // built without the feature lands here rather than failing to exist. **The
    // refusal is a frame as well as a line on stderr**: stdout is the protocol
    // channel in every build, feature or no feature (decision 19), so a
    // consumer that asked for a confinement hears why it did not get one.
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

    // The real dual-pipeline runtime (pipeline-spec.md §3.3): `--config
    // <file>` consumes the parsed `[pipeline]` section (F07 — the values
    // an operator sets are no longer silently dropped); without it the
    // section defaults apply (an absent `[pipeline]` resolves to the
    // defaults, Q8).
    let config = match &args.config_path {
        Some(path) => Some(emit_on_error(Config::load(path))?),
        None => None,
    };
    // FR-023 resolution, lowest precedence first: the config file's
    // `[server] read_only`, then the `TINIO_READ_ONLY` overlay when set,
    // then the flag. An absent `--config` contributes the default
    // (read-write). `.env` joins when the CLI brings the dotenvy wiring
    // (T066/T067).
    let file_read_only = config
        .as_ref()
        .is_some_and(|config| config.server.read_only);
    let read_only =
        emit_on_error(sources::env_read_only())?.unwrap_or(file_read_only) || args.read_only;
    let access = AccessMode::from(read_only);

    // The harness passes a fresh scratch path — create the root if missing
    // (FsStorage itself requires an existing directory). Read-only mode
    // serves a root that must already exist: creating it would write the
    // one thing the mode promises to leave alone. A missing root fails
    // during state-dir resolution — `default_home_state_dir` canonicalizes
    // it — or, with `TINIO_STATE_DIR` set, in `FsStorage::new`'s own
    // canonicalization.
    if !read_only {
        emit_on_error(fs::create_dir_all(&args.root))?;
    }
    // Spec §6 OS-mapping validation: any `[owner]`/`[[users]] local_uid`
    // mapping requires chown privilege at startup (root or CAP_CHOWN) —
    // fail closed with a config error (unix; Windows already rejected the
    // key at parse). Read-only mode skips the probe: it writes
    // `<root>/.tinio-chown-probe-<nonce>/probe` into the served root
    // (decision 14).
    #[cfg(unix)]
    if !read_only {
        emit_on_error(identity::ensure_chown_privilege(
            config.as_ref().unwrap_or(&Config::default()),
            Path::new(&args.root),
        ))?;
    }
    // Read-only mode relocates the tool's state out of the served root
    // (FR-023); normal mode leaves it at the backend's default
    // `<root>/.tinio/`. The diagnostic goes to stderr: stdout carries event
    // frames alone (`emit` is their only writer), and a diagnostic there
    // would be what a consumer reads as an event (design decision 19).
    let state_dir = if read_only {
        // `TINIO_STATE_DIR` wins over the derived layout: the read-only
        // deployments where there is no writable home are exactly the ones
        // that need the escape hatch.
        let dir = match emit_on_error(state::env_state_dir())? {
            Some(dir) => {
                eprintln!(
                    "read-only mode: state dir {} (from {})",
                    dir.display(),
                    state::STATE_DIR_VAR
                );
                dir
            }
            None => {
                let dir = emit_on_error(state::default_home_state_dir(Path::new(&args.root)))?;
                eprintln!("read-only mode: state dir {}", dir.display());
                dir
            }
        };
        Some(dir)
    } else {
        None
    };
    // **The state directory as a path, and it is asked for here rather than of
    // a `FsStorage`.** The two readers below need it before any storage can
    // exist — the exposure set names it, and the access log is opened inside it
    // — and no storage can exist yet for a reason that is not about paths: the
    // storage's lanes are multi-threaded runtimes, and `unshare(CLONE_NEWUSER)`
    // returns `EINVAL` in a multi-threaded process (design §10, verified). The
    // answer is the storage's own, from the function the storage itself calls:
    // `<root>/.tinio` under the canonical root, or the relocation resolved
    // above. The build sources still exist before the door closes — that is
    // what this line and the `create_dir_all` below are.
    let state_dir_path = match &state_dir {
        Some(dir) => dir.clone(),
        None => emit_on_error(default_state_dir(&emit_on_error(fs::canonicalize(
            &args.root,
        ))?))?,
    };
    // Operational logs to stderr (info), access log to `<state
    // dir>/access.log` (T052) — `<root>/.tinio/` in normal mode, the
    // relocated dir in read-only mode. The `[log]` config wiring lands
    // with the US2 CLI.
    emit_on_error(fs::create_dir_all(&state_dir_path))?;
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
    let subscriber = emit_on_error(server_log::build_subscriber(
        Verbosity::Info,
        Format::Text,
        &AccessFormat::Combined,
        &state_dir_path.join("access.log"),
        None,
    ))?;
    emit_on_error(subscriber::set_global_default(subscriber))?;

    #[cfg(all(feature = "sandbox", target_os = "linux"))]
    if args.sandbox {
        // The **resolved** mode and not the configuration file's own field:
        // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
        // `access` above, so reading `config.server.read_only` here would expose
        // the served root `ReadWrite` on a run the operator asked to be
        // read-only — losing the kernel-level half of FR-023 that design §9
        // promises, and doing it silently. `access` **is** the type the exposure
        // set takes, so it is handed over as it stands.
        let sandbox = sandbox_request(&args.root, &state_dir_path, access);

        // The model comes from the environment in P1 (`TINIO_SANDBOX_MODEL`);
        // the CLI carries it as a flag when it owns `serve` (design §1). It
        // reaches the backend through `Options`, which is why this arm names the
        // Linux crate directly instead of going through the facade's
        // `platform()` — the facade returns whatever backend the build targets,
        // and an options value only one of them has cannot be handed to it.
        // Each launcher platform's own arm does the same with its own options
        // type.
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
        // The two facts `enter` reads that the environment does not carry —
        // the resolved `access` and whether an FR-033 `local_uid` mapping is
        // configured — are folded in by [`sandbox_options`], which is also
        // where they are tested. Left at their defaults they would build a
        // confinement this run did not ask for; see that function.
        let options = match linux::Options::from_env() {
            Ok(options) => sandbox_options(options, &config, access),
            Err(err) => {
                emit_error(&err.to_string());
                return Err(err.into());
            }
        };
        let mut backend = linux::Backend::with_options(options);
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
                if backend.seccomp() == Some(linux::Seccomp::SkippedUnsupportedArch) {
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

    // **No thread may exist above this line, and this is where that stops
    // being true**: `Pipelines::build` starts a multi-threaded runtime per lane
    // (two IO workers, one removal, one DB by default), and
    // `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller with `EINVAL`.
    // So the lanes, the `FsStorage` whose options take them by handle, and the
    // runtime below all sit *after* the confinement rather than beside the
    // config they read — design §10's ordering, and the reason the state
    // directory was resolved as a path above.
    let pipeline_config = match &config {
        Some(config) => config.pipeline.clone().unwrap_or_default(),
        None => pipeline::Config::default(),
    };
    let pipelines = emit_on_error(Pipelines::build(&pipeline_config))?;
    let storage = emit_on_error(FsStorage::new(
        &args.root,
        FsOptions {
            follow_symlinks: false,
            state_dir,
            access,
            compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
            meta_batch_size: DEFAULT_META_BATCH_SIZE,
            meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
            io_pipeline: pipelines.io(),
            remove_pipeline: pipelines.remove(),
            db_pipeline: pipelines.db(),
            owner_uids: identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())),
        },
    ))?;

    // The two `None`s are Linux's half of decision 6 read back out of the
    // signature: this process bound its own listener rather than adopting a
    // child's, and nothing forwards a stop to a process that is not a child.
    // `serve` itself is the one fallible step here that is **not** wrapped: it
    // emits its own `Listening` frame, so a refusal after that point is the
    // serving phase's statement rather than a startup one (`emit_on_error`).
    emit_on_error(Builder::new_multi_thread().enable_all().build())?
        .block_on(serve(args, config, storage, access, pipelines, None, None))
}
```

**Three helpers carry the frames, and there is a fourth this step needs for a different reason.** `emit`, `emit_error` and `emit_on_error` are the block below; `emit_on_error` is the third, and it is the rest of decision 19's "a startup failure is an event rather than an absence": every other fallible step of `main` on the way to the listener — a config that will not load, a root that cannot be created, a state directory that cannot be resolved, a storage that refuses the mode it was handed — returned through a bare `?`, so the process exited with an empty stdout and a consumer waiting for a frame reported a timeout where the launcher platforms report the cause. The **serving call is deliberately not wrapped**: by then this process has emitted `Listening` itself, and an `Error` frame *after* it is a statement about the serving phase rather than a startup refusal. **The fourth is `sandbox_request`**, below the tests: it is the construction `main` actually calls, extracted so the device-file flag is asserted on code that runs rather than on a `Sandbox` the test built for itself.

They belong to `serve.rs` rather than to the backend — the same rule as "a library installs no subscriber" (shared §7) seen from the other side: a library does not write to a process's stdout either. They are the file's only copies: all three platforms' arms live in this one `main`, so a second `emit` beside this one is `E0428`, and the launcher phases call these rather than defining their own (P2 and P3 each carry a copy of `emit` in their own task text today; theirs is deleted when this phase lands, which is the correction `serve`'s signature needed for the same reason — shared §1's rule that a fact two plans could state has one owner).

```rust
/// Write one event frame to stdout/// Write one event frame to stdout and flush it.
///
/// **The only write to stdout in this file** (design decision 19), and one
/// function rather than one per call site: the readiness frame below and the
/// `Error` frames a refusal sends both go through it, so "stdout carries events
/// and nothing else" is a property of one body instead of a rule to re-check.
///
/// **"In this file" is the whole of what a source scan can promise, and that
/// gap is worth stating rather than leaving to the check.** The scan this
/// contract was written against — a `println!`/`print!`/`stdout()` grep with a
/// negative lookbehind for the `e` of `eprintln!` — cannot see a dependency, and
/// the parser is one now: `clap` writes to stdout through its own code when it
/// is allowed to, so a green grep would have kept passing while the guarantee
/// was gone. What holds the parser's half is its configuration and three
/// functions this file never calls — `Error::print`, `Error::exit`,
/// `Command::print_help` — none of which a grep over this source can check.
/// `crates/tinio-server/tests/serve_read_only.rs` counts the child's **stdout
/// lines** after a refused command line and requires exactly the frame's three,
/// which is a fact about the stream rather than about anyone's source, and is
/// what would fail if a clap version ever printed.
///
/// Flushing is not decoration — stdout is line-buffered to a terminal and
/// block-buffered to a pipe, and a harness reads a pipe, so an unflushed frame
/// is a readiness hang rather than a missing event. A failed write is ignored on
/// purpose: it means the reader is gone, and there is nowhere left to report it.
fn emit(event: &Event) {
    let mut out = stdout().lock();
    let _ = out.write_all(event.encode().as_bytes());
    let _ = out.flush();
}
```

**What the subscriber shows is a level, and the level is now the crate's as well as the sink's.** `build_subscriber` is handed `Verbosity::Info` and filters on that level rather than on an environment variable, so anything below `info!` stays off stderr until the `[log]` wiring lands with the CLI. **That is why three events this phase first wrote as `debug!` are `info!` now** — the resolved model, the exposure set the jail actually applied, and the seccomp install (the `warn!` for its *skip* was already above the line) — because each answers "was this process confined, and to what", which is exactly the `info!` half shared §7 reserves, and because a successful `serve --sandbox` used to log **nothing at all** through this subscriber: the confinement's whole record was the per-item `debug!` detail that a fixed `Verbosity::Info` drops. The per-item events were left as `debug!` deliberately — each `unshare`, each map write, each mount, each drop — since a launcher window with no other subscriber may show them, and the split is what keeps the must-survive set small enough to read. The `error!` that every refusal logs where its `Error` is built is above the line too, and is now one event from one place: the crate's failure path is `mechanism.rs`'s `refused`/`fail`/`step`, imported by `enter.rs` and `seccomp.rs` rather than copied — **and those three are the core's functions now, re-exported from `mechanism.rs` since the phase-basis correction moved them to `tinio-sandbox-core`**, with every call site and both import lines unchanged.

Replace the hand-rolled argument loop with `clap`: a private `#[derive(Parser)] struct RawArgs` carries the flag-for-field shape, and `fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Args, Box<dyn Error + Send + Sync>>` — the same signature and the same `Result` — parses it and folds it into the unchanged `struct Args { root: PathBuf, address: SocketAddr, config_path: Option<PathBuf>, read_only: bool, sandbox: bool }`. **`read_only` is read-only's field and must not be dropped**, since `--read-only` is one of the flags the loop already matched. `usage()` goes with the loop; `refuse_command_line`, below, is what replaced it.

**Two types rather than one, and `--port`/`--address` are why.** clap fills a field from the flag that names it, and those are two flags for the one address the server binds, so the resolution lives in `parse_args` and `RawArgs` stays private to the parser — `Args`'s five field names and types are unchanged, because the launcher plans call this function and read those fields, and `address` is still the resolved `SocketAddr`.

**Help and version are disabled outright, and that is a statement about stdout rather than about etiquette.** clap prints the help and the version it generates to **stdout**, which this process reserves for event frames (decision 19), so `--help`, `-h` and `--version` are unknown arguments like any other typo: refused on stderr through `refuse_command_line`, with the same refusal as an `error` frame, and nothing on stdout but that frame. clap's error text carries the usage line, so the operator still gets the guidance; leaving the flags enabled would give this command a second writer to the protocol stream.

**`no_binary_name` is the other attribute that is not cosmetic.** clap consumes the **first element of the iterator** as the program name, and every caller hands over arguments **without `argv[0]`** — `main` skips it, and each launcher platform filters its own element out of what is left — so without the setting the served root is swallowed as that name and every parse fails on a missing root. The iterator parameter is what makes that observable in a unit test, and `no_binary_name` is what makes the parameter's meaning and clap's assumption agree.

**`root` is a `PathBuf`, not a `String`.** The parser reads `OsString`s because a path that is not valid UTF-8 is legal on Linux, and forcing it into a `String` at the boundary would refuse a served root for a reason the operator cannot see; `fs::create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway. **The bytes reach the field intact because the iteration is over `OsString`s all the way down**, which is what the loop this replaces could not do: matching a flag through `arg.to_str()` has to decide what to do with one that is not UTF-8, while clap's `PathBuf` value parser hands the bytes through as they came — and the `#[cfg(unix)]` test above pins exactly that.

```rust
/// The parsed command line./// The parsed command line.
///
/// Created by this step: the entry point used to parse into four locals of
/// `main` (`root`, `address`, `config_path`, `read_only_flag`), which left
/// nothing for a test to feed a fabricated `argv` to and nothing for `serve`
/// to receive.
///
/// **The address is resolved, not parsed** — [`parse_args`] folds `--address`
/// over `--port` over 9000 into it, so every caller binds `args.address`
/// rather than resolving the pair a second time.
struct Args {
    /// The served root, as the operator spelled it. A `PathBuf` rather than the
    /// `String` it used to be: a path that is not valid UTF-8 is legal on Linux,
    /// and `create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway.
    root: PathBuf,
    /// The address the listener binds, `--address` over `--port` over 9000.
    address: SocketAddr,
    /// `--config <file>`, when one was given.
    config_path: Option<PathBuf>,
    /// The `--read-only` flag. **FR-023's field, and not the resolved mode**: the
    /// file's `[server] read_only` and `TINIO_READ_ONLY` are the other two
    /// precedence levels, and `main` folds all three together.
    read_only: bool,
    /// `--sandbox` (design decision 5).
    sandbox: bool,
}

/// The command line as `clap` parses it: the flag-for-field shape of
/// `serve <root> [--port N] [--address HOST:PORT] [--config <file>]
/// [--read-only] [--sandbox]`, which [`parse_args`] resolves into [`Args`].
///
/// **Two types rather than one, because clap fills a field from the flag that
/// names it** and `--port`/`--address` are two flags for the one address the
/// server binds. The resolved `Args` is what the rest of the file reads, so
/// this one stays private to the parser.
///
/// **There is no help and no version, and that is a statement about stdout
/// rather than about etiquette** (design decision 19). Text the operator asks
/// for is still tool output, and clap prints it to stdout — the stream this
/// process reserves for event frames — so both flags are disabled and a
/// request for either is an unknown argument like any other typo: refused on
/// stderr with the `error` frame that goes with it, and nothing on stdout but
/// that frame. clap's error text carries the usage line, so the operator still
/// gets the guidance. **The parser is left with no route to stdout at all**,
/// which is cheaper to hold than a rule about which of its outputs go where.
/// The crate also takes clap without its `help` feature, so there is no help
/// *renderer* compiled in either — and `disable_help_flag` earns its place
/// independently of that: clap's `_check_help_and_version` adds `-h`/`--help`
/// unless the setting is set, feature or no feature, so the flag is what stops
/// the argument existing rather than what stops it being answered.
///
/// **`no_binary_name` is what the iterator parameter already means.** The
/// callers hand over arguments and not `argv[0]` — `main` skips it, and each
/// launcher platform filters its own element out of what is left — while clap
/// reads the first element it is given as the program name. Without the
/// setting the served root would be swallowed as that name and every parse
/// would fail on a missing root.
#[derive(Parser)]
#[command(
    name = "serve",
    no_binary_name = true,
    disable_help_flag = true,
    disable_version_flag = true
)]
struct RawArgs {
    /// The served root: the one bare argument, wherever it appears. The
    /// flag/bare split is clap's, so a `--sandbox` before the root is a flag
    /// rather than the directory to serve, and a second bare argument is a
    /// refusal rather than a silently ignored one.
    root: PathBuf,
    /// `--port N`, which means `127.0.0.1:N` once [`parse_args`] resolves it.
    #[arg(long, value_name = "N")]
    port: Option<u16>,
    /// `--address HOST:PORT`, which outranks `--port`.
    #[arg(long, value_name = "HOST:PORT")]
    address: Option<SocketAddr>,
    /// `--config <file>`. A `PathBuf` rather than the `String` a text-only
    /// parser would take: a path that is not valid UTF-8 is legal on Linux, and
    /// `create_dir_all`, `Config` and `FsStorage::new` all take `Path` anyway —
    /// refusing such a path here would be refusing it for a reason the operator
    /// cannot see. clap's parser for it hands the bytes through as they came.
    #[arg(long, value_name = "CONFIG.TOML")]
    config: Option<PathBuf>,
    /// The `--read-only` flag (FR-023).
    #[arg(long)]
    read_only: bool,
    /// `--sandbox` (design decision 5).
    #[arg(long)]
    sandbox: bool,
}

/// Parse the command line, or report why it cannot be parsed.
///
/// **It returns the failure rather than printing and exiting**, which is what
/// lets the tests below assert on the parse at all — and it is the iterator
/// parameter, not a convenience, that lets them: a parser reading
/// `std::env::args_os()` itself could only be tested by mutating the test
/// process's own arguments, and the launcher platforms' `main` parses the
/// arguments left after the listener element is filtered out. clap takes that
/// iterator as it stands in `try_parse_from`; its `parse` would read the
/// process's own arguments and exit on its own terms.
fn parse_args(
    args: impl IntoIterator<Item = OsString>,
) -> Result<Args, Box<dyn Error + Send + Sync>> {
    let args = RawArgs::try_parse_from(args)?;
    Ok(Args {
        root: args.root,
        // The one precedence there is, resolved here rather than at each of
        // the callers: `--address`, else `--port` on the loopback, else 9000.
        address: args
            .address
            .or_else(|| {
                args.port
                    .map(|port| SocketAddr::from(([127, 0, 0, 1], port)))
            })
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 9000))),
        config_path: args.config,
        read_only: args.read_only,
        sandbox: args.sandbox,
    })
}

/// The command line's refusal, on its way out: clap's rendering — the sentence
/// **and its usage line** — to stderr, the same refusal as an `Error` frame on
/// stdout, then exit 2.
///
/// **Both streams are this file's and neither is clap's.** clap would print
/// the same text to stderr and exit with its own code, but the frame is the
/// half it cannot know about: a consumer of stdout waits for a frame, and a
/// startup refusal that arrived as an absence is a timeout where this is a
/// cause (decision 19). Reached for every failure the parse has — a typo, a
/// missing root, `--help` — so the command line has one refusal and not three.
///
/// Exit 2 is what the hand-rolled `usage()` exited with and what the harness
/// reads; the frame carries the same text as stderr, trimmed of the newline
/// the rendering ends with, so the two registers of one refusal agree.
fn refuse_command_line(err: Box<dyn Error + Send + Sync>) -> ! {
    eprint!("{err}");
    emit_error(err.to_string().trim_end());
    process::exit(2)
}
```

**The iterator parameter is not decoration.** It is what lets the unit tests above feed a fabricated `argv` instead of mutating the test process's own arguments, and it is the shape the launcher platforms already assume — their `main` parses the arguments left after the listener element is filtered out, so a `parse_args()` that read `std::env::args()` itself could not be used there at all.

**Move the rest of the existing body into `serve`, whose signature is this plan's and which the other two plans consume unchanged.** It is introduced here because this task is what restructures the entry point, and P2 and P3 fill the two optional parameters from their own arms of this `main`:

```rust
/// **One signature across the three plans, and this plan owns it.**
///
/// `storage` is **not** optional, and an earlier version of this signature made
/// it so and had to be corrected: every caller that reaches `serve` serves from
/// a storage, so `None` had no meaning any platform could agree on — this plan
/// read it as a caller bug and the Windows plan read it as "the body builds
/// one", which is not the same function. Linux's `main` builds the storage
/// before the runtime exists (its state directory is one of the jail's
/// mounts), and a launcher's child builds it in `confined`; the launcher itself
/// never reaches `serve` at all.
///
/// **Two parameters carry what `main` has already resolved** rather than
/// letting the body resolve it a second time: `access` is FR-023's chain
/// (`[server] read_only` → `TINIO_READ_ONLY` → `--read-only`) folded into one
/// value, and `pipelines` are the lanes the storage was constructed with — the
/// same instances the metrics hook reads and the shutdown sequence drains, which
/// a second `Pipelines::build` here could not be.
///
/// The two optional ones are optional because the three platforms reach `serve`
/// differently, and each `None` is a fact about a platform rather than a policy
/// of `serve`: Linux serves in the process that confined itself, so it passes
/// `None` for both — there is no adopted listener, because this process binds
/// its own, and no stop channel, because nothing forwards a signal to something
/// that is not a child. A launcher's child passes `Some(listener)` and
/// `Some(stop)` instead.
async fn serve(
    args: Args,
    config: Option<Config>,
    mut storage: FsStorage,
    access: AccessMode,
    pipelines: Pipelines,
    inherited: Option<std::net::TcpListener>,
    stop: Option<watch::Receiver<bool>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // The stop channel belongs to the launcher platforms: the child is what
    // reacts to a forwarded stop, so their `confined` creates the channel,
    // gives the sender to `on_stop` and hands the receiver here. Nothing
    // forwards a stop to a process that is not a child — Linux passes `None` —
    // so this phase holds it and does nothing with it, and taking it now is
    // what lets P2 and P3 add their accept-loop arm without touching this
    // signature.
    let _ = stop;

```

**Why two of the seven parameters are optional, stated once — and the other two are not the two this signature started with.** `access` and `pipelines` were added after the first version of this step and both are needed: `access` must be the **resolved** value, because `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced it and the body must not resolve it a second time; `pipelines` are the lanes the storage was constructed with, which an `FsStorage` does not expose (it keeps only the removal lane) and which a second `Pipelines::build` cannot stand in for, because the storage enqueues to the first set while the metrics hook and the shutdown drain would watch the second. Both sit after `storage`, because both are facts the caller has settled by the time it has built one. The two **optional** ones are optional because the three platforms reach `serve` differently, and each `None` is a fact about a platform rather than a policy of `serve`. **Linux** serves in the process that confined itself, so its `main` builds the storage before the runtime exists — the bind sources must exist and the state directory is one of the jail's mounts — and calls `serve(args, config, storage, access, pipelines, None, None)`: there is no adopted listener, because this process binds its own, and no stop channel, because nothing forwards a signal to something that is not a child. **The launcher platforms** do not serve in the launcher at all on the sandbox path: their `main` builds no storage there — it resolves only the state directory *path*, which is what the exposure set needs — and their `confined()` builds the storage in the child and calls `serve(args, config, storage, access, pipelines, Some(listener), Some(stop))`. **`storage` is deliberately not one of the optional parameters**: every caller that reaches `serve` serves from a storage, so an optional one would have two readings and only one of them could be right — which is exactly what happened before this was corrected. **`Pipelines` is named once, by `crates/tinio-cli/src/resolve/mod.rs`** (`pub type Pipelines = ServerPipelines<etag::Result, Result<(), FsError>, Result<(), FsError>>;`), which is the triple `Pipelines::build` infers from the `FsOptions` the storage was handed, so the signature does not spell that triple out again.

**`stop` is created in the child and never crosses the process boundary.** The child is what reacts to a forwarded stop, so `confined` makes the `watch` channel there, hands the sender to `on_stop`, and passes the receiver to `serve`. A channel that had to be carried to the other side of an `execve` would be a descriptor and a rebuild on arrival, which is the machinery this shape exists to avoid.

**A launcher must not construct an `FsStorage` on the sandbox path, and that is why its side resolves a path instead.** (On the plain path — the same `main` with no `--sandbox` — it does build one, because there it *is* the server; the rule is about the launcher/child pair, not about the process.) Constructing one *opens* the state database, and the child opens the same database in the same directory: two processes holding one state directory is not something the storage layer is written for — its exclusive-handle and compaction assumptions are exactly what such a pair breaks — so the launcher resolves the state directory as a `PathBuf` for the exposure set and lets the child own the only handle. It is also why that side builds its exposure set from a path rather than reading one back out of a storage.

Everything before the moved body has moved into `main` and must not appear twice: the read-only resolution, the guarded `create_dir_all` and `ensure_chown_privilege`, the state-directory resolution **as a path**, `Config::load` and the log subscriber, which moves ahead of `confine` for the reason given in the code above. **The storage construction and the lane construction are the exception, and they sit *after* the sandbox arm rather than before it**: `Pipelines::build` starts a multi-threaded runtime per lane and `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller, so building them first makes `--sandbox` fail `EINVAL` (invariant 2 above says so in full). The one exception *inside* the moved body is the endpoint line, which is the whole of decision 19 on this platform:

The body's endpoint line becomes an event, and it is also where `inherited` is used — the launcher platforms' children take the listener their parent had already bound, and this process binds its own, which is the only arm that exists in this phase. **`inherited` is `std::net::TcpListener` and not `tokio`'s** (the phase-basis correction; `2026-09-18-sandbox-shared.md` §4 owns the row): `TcpListener::from_std` panics outside a runtime and the child rebuilds its socket in its pre-runtime path, so the conversion has to sit here — inside the body, which runs within the runtime by construction — rather than at a call site that cannot make it. In the file the parameter is spelled `StdTcpListener`, because `TcpListener` is `tokio`'s there:

```rust
    let listener = match inherited {
        // `std`'s listener becomes the async one here, and here is the first
        // place it can: `from_std` panics outside a runtime, and a launcher's
        // child rebuilds its socket before any runtime exists (shared §4).
        Some(inherited) => TcpListener::from_std(inherited)?,
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

**The negative lookbehind is load-bearing**: `println!` is a substring of `eprintln!`, so the obvious pattern reports every diagnostic that belongs on stderr — `refuse_command_line`'s `eprint!`, and the state-directory and sandbox notes in `main` — and hides the one line this step is actually asserting.

**Every hit must be inside `emit`, and there is exactly one of them.** Run when the step landed, the command printed a single line — `let mut out = stdout().lock();` — which was the example's `emit`'s own body, and that body is `crates/tinio-cli/src/events.rs`'s `emit` today, the entry point having since moved out of the example entirely. The endpoint line this step replaces was the file's only stdout writer when the step was written (`println!("listening on {}", listener.local_addr()?)`), and it is now a caller of `emit` rather than a writer, so the pair of lines that used to be the hit (`print!` plus `stdout().flush()`) is gone. A `print!` or `println!` hit anywhere else is a decision-19 bug, and `tinio-server/src` must have none at all: its logging is tracing-based and already writes to stderr or to the access-log file. The `Confinement::Supervise` arm is a no-op on Linux by construction and exists so the call site is platform-neutral; the two launcher plans own its real behaviour.

**And the grep's reach is bounded, which the clap parser made visible**: a source grep cannot see into a dependency, and `clap` writes to stdout through its own code when it is allowed to. `emit`'s own doc states what holds that half — three functions this file never calls, plus the parser's two disable attributes — and the mechanical check is not a grep at all: `serve_read_only.rs`'s `an_unknown_argument_exits_with_the_usage_status` refuses `--not-a-flag`, asserts exit 2 and the `error` frame naming the flag, and **counts the child's real stdout lines**, requiring exactly the frame's three.

- [x] **Step 6: Run the tests and the example**

Run:
```bash
cargo test -p tinio-server --example serve
cargo build -p tinio-server --example serve
cargo run -p tinio-server --example serve -- /tmp/tinio-smoke --port 0
```
Expected: the tests pass; the binary builds; the run prints a single SSE frame — `event: listening` and `data: {"addr":"127.0.0.1:<port>"}`, closed by a blank line — and serves. Stop it with Ctrl-C.

- [x] **Step 7: Run it confined**

Run: `cargo run -p tinio-server --example serve -- /tmp/tinio-jailed --port 0 --sandbox`
Expected: the same single `event: listening` / `data: {"addr":…}` frame, and a second terminal can `aws s3 mb` against it. This is the feature working end to end by hand.

Then confirm a bad exposure fails closed. The case has to be one the *validation* refuses, and a relative root is the readable one:

Run: `cargo run -p tinio-server --example serve -- ./not-absolute --port 0 --sandbox`
Expected: exit non-zero; stdout carries one `event: error` frame whose message is ``path `./not-absolute` cannot be exposed: not absolute``, and stderr the same failure in the runtime's `Error: …` rendering — `main` returns the boxed error rather than printing it, so the `Display` text an operator reads is the one in the frame (decision 19). The run leaves the directory it created for the root behind, and the state directory inside it.

**The spelling that looks obvious is the one that cannot reach this check, so it is worth saying why nobody should put it back.** `serve /tmp/does-not-exist-xyz --sandbox` was the earlier version of this step, and it cannot fail: whenever the mode is not read-only, `main` runs `fs::create_dir_all(&root)` before the sandbox arm (invariant 2 above — the bind sources have to exist before the door closes), so the very path the exposure check would reject is *created* first, the exposure then exists, and the run confines successfully. A path that exists but cannot be expressed is what the check is for, and `mirror_under` refuses a relative one with `not absolute` after every earlier step — the root creation, the chown probe, the storage — has passed on it. (Task 5's `a_relative_exposure_is_refused_before_anything_irreversible_happens` is the same refusal at the unit level; this run is the operator-visible half.)

- [x] **Step 8: Verify the feature-off build still works**

Run:
```bash
cargo check -p tinio-server --no-default-features
cargo check -p tinio-server --no-default-features --features multipart,copy,list-v1,list-v2,cors,select,acl
```
Expected: both succeed, **and the example is built by both** — it carries no `required-features`, which is what the `no-default` CI leg requires and what makes `main`'s `#[cfg(not(feature = "sandbox"))]` refusal reachable. A first run of this step that reports "the example is skipped" is running the file this step no longer asks for: the earlier spelling had `required-features = ["sandbox"]` and a `cargo check --workspace --no-default-features` would then skip the target silently, which is exactly how the CI breakage went unnoticed.

---

### Task 8: The readiness contract — the harness migration and the end-to-end test

**Files:**
- Create: `crates/tinio-server/tests/sandbox_e2e.rs`
- Modify: `crates/tinio-server/Cargo.toml` (the test target)
- Modify: `crates/tinio-e2e/Cargo.toml` (the `tinio-sandbox` dependency the reader needs)
- Modify: `crates/tinio-e2e/tests/steps/clients.rs` (the readiness scrape)
- Modify: `crates/tinio-server/tests/serve_read_only.rs` (the second readiness scrape — see below)
- Modify: `crates/tinio-server/examples/serve.rs` (the `emit_on_error` helper the earlier startup steps gain)
- Modify: any cucumber feature that asserts readiness

**Interfaces:**
- Consumes: the built `serve` example, resolved from the test binary's own path; `tinio_sandbox::Event`. **(The block below is that step's history: the helper it defines is `crates/tinio-server/tests/common/mod.rs`'s `tinio_bin` today — `serve_bin` until the example retired — and it resolves the `tinio` binary.)**
- Produces: nothing.

**This task touches shipped test code, and that is deliberate.** `crates/tinio-e2e/tests/steps/clients.rs` holds `const READY: &str = "listening on ";` and a `wait_for_ready` that reads the child's stdout until it finds it. Decision 19 replaces that scrape with the event stream, so the harness has to move with it — in a task that says so, rather than as a side effect of a sandbox change. The harness migration is not optional and not deferrable: leaving it would mean the e2e suite and the sandbox tests disagree about what readiness looks like, and the cucumber run would fail on every scenario the moment the endpoint line changes.

**There are two such readers, not one, and the second arrived with the read-only work.** `crates/tinio-server/tests/serve_read_only.rs` carries its own `const READY: &str = "listening on ";` at line 66 and a `Serve::ready()` that strips the prefix and parses the remainder as a `SocketAddr`. It is an auto-discovered integration test that CI's `test-port` leg runs (`run-test-leg` invokes `cargo nextest run --workspace --exclude tinio-e2e`), so the endpoint change breaks it exactly as it breaks `clients.rs` — and it is fixed in the same commit rather than discovered by a red CI run. This is the plan that changes the endpoint, so it is the plan that carries both.

- [x] **Step 1: Write the test**

```rust
//! The confined server, driven the way the interop harness drives it//! The confined server, driven the way the interop harness drives it: spawn
//! the example, read the endpoint it prints, and talk to it.
//!
//! The example is confined **in place** on Linux (design decision 2), so these
//! legs prove the end of the chain no unit test can: a process that built a
//! jail around itself still binds a listener, still reports it, and still
//! answers on it from outside. The second leg drives the same binary under
//! `--read-only` and asks the confinement's own question — whether a write
//! through the served root is refused — through the child's private mount
//! namespace, which is the one place the answer exists.

#![cfg(target_os = "linux")]
#![cfg(feature = "sandbox")]

use std::{
    env, fs,
    io::{self, BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

use tinio_sandbox::{Event, linux::Mechanism};
use tinio_util::testing::wait_timeout_secs;

/// The one way past a missing precondition, and it is a deliberate opt-in.
///
/// **A suite that certifies a confinement does not get to skip in silence**: a
/// precondition that is absent fails the run by default, so a host that cannot
/// confine can never report a pass for a server that was never jailed. An
/// operator who knows this machine cannot confine sets this variable and means
/// it. The rule and the variable are the jail test's (`tinio-sandbox-linux`'s
/// `tests/jail.rs`), spelled the same way so one habit covers both.
const ALLOW_SKIP: &str = "TINIO_SANDBOX_ALLOW_SKIP";

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
    let name = format!("serve{}", env::consts::EXE_SUFFIX);
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
///
/// `Err` carries the `unshare(1)` invocations that answered no, spelled as an
/// operator would retype them: the caller's message has to name the command it
/// tried, which is the whole difference between a legible skip and a shrug.
fn host_can_confine(model: Mechanism) -> Result<(), String> {
    let user: &[&str] = &["--user", "--map-root-user"];
    let mount: &[&str] = &["--mount"];
    let probes: &[&[&str]] = match model {
        Mechanism::UserNamespace => &[user],
        Mechanism::Root => &[mount],
        Mechanism::Auto => &[user, mount],
    };
    if probes.iter().any(|flags| unshare_probe(flags)) {
        return Ok(());
    }
    Err(probes
        .iter()
        .map(|flags| format!("unshare {} true", flags.join(" ")))
        .collect::<Vec<_>>()
        .join(" and "))
}

/// `unshare(1)` with `flags`, as a process of its own — the program the WSL2
/// script gates on, so the script and the test agree by construction.
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

/// How long the confined child gets to report `listening`, or the refused start
/// that took its place. The knob and default the crate's other spawned-server
/// wait honours, read through its own accessor: one policy for a starved runner,
/// not a third default here.
fn wait_timeout() -> Duration {
    Duration::from_secs(wait_timeout_secs())
}

/// Wait for the child's `listening` event and return the address it bound, or
/// fail with the child's own evidence.
///
/// **The read happens on a reader thread and the wait is bounded here.** The
/// earlier shape read two lines straight off the pipe with no deadline, so a
/// startup path that neither writes a frame nor exits — a `confine` that blocks,
/// a config read on a FIFO — hung the whole workspace leg until CI times it out,
/// with no test named: libtest has no per-test timeout, and both sibling waiters
/// bound theirs. The thread also keeps the pipe drained, so the child cannot
/// block on a full buffer.
///
/// **And it accumulates a frame rather than reading two lines.** A stray line
/// ahead of the frame is exactly the case the framing exists to survive —
/// `sse-core` ignores field lines it does not know and the sibling waiters
/// tolerate them — while a fixed two-line read reported it as an unexpected
/// frame. Lines accumulate to the blank line that closes a frame, the block is
/// decoded, and text that is not one of our frames is skipped rather than fatal.
///
/// A refusal is reported as the refusal it is: an `error` frame carries the
/// confinement's own rendering, which is worth more than any guess this side
/// could make about why no endpoint came.
fn wait_for_listening(child: &mut Child) -> SocketAddr {
    let stdout = child.stdout.take().expect("stdout is piped");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if line.trim_end().is_empty() {
                if let Some(event) = Event::decode(&frame) {
                    // A send that fails means the wait below already gave up and
                    // killed the child; there is nowhere left to report it.
                    if tx.send(event).is_err() {
                        return;
                    }
                }
                frame.clear();
            } else {
                frame.push_str(&line);
            }
            line.clear();
        }
    });
    let timeout = wait_timeout();
    match rx.recv_timeout(timeout) {
        Ok(Event::Listening { addr }) => addr,
        Ok(Event::Error { message }) => fail(child, &format!("serve refused to start: {message}")),
        Err(err) => fail(
            child,
            &format!(
                "serve reported no `listening` event within {} s ({err})",
                timeout.as_secs()
            ),
        ),
    }
}

/// Kill and reap the child, then fail with `message` and the stderr it left.
///
/// **Killing first is not tidiness.** A confined `serve` left alive holds its
/// listener, its jail and its state lock for the life of the test process — and
/// the leg that failed is the one whose child is least likely to exit on its
/// own. Reaping also ends the reader thread, whose pipe reaches EOF at the
/// child's death, so the stderr read below cannot block.
fn fail(child: &mut Child, message: &str) -> ! {
    let _ = child.kill();
    let _ = child.wait();
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    panic!("{message}; stderr:\n{stderr}");
}

/// The precondition every leg of this file asks before it spawns anything:
/// `false` when this host cannot confine under the model the run asked for and
/// [`ALLOW_SKIP`] was set deliberately, which is the only case in which a leg
/// returns unasserted.
///
/// **A skip printed through `eprintln!` alone is a silent one**: libtest
/// swallows it without `--nocapture`, so a host that cannot confine reported a
/// green run that certified nothing. The opt-in is what makes the skip a
/// statement an operator made rather than one the suite made for them.
///
/// One function rather than one copy per leg, because the skip rule is the
/// thing a second leg is most likely to get subtly wrong — a leg that skipped
/// without the opt-in would certify a server that was never jailed, which is
/// exactly what design §12 refuses.
fn confinement_available() -> bool {
    let model = requested_model();
    if let Err(tried) = host_can_confine(model) {
        assert!(
            env::var(ALLOW_SKIP).is_ok_and(|value| !value.is_empty()),
            "this host cannot confine under {model:?}: the precondition `{tried}` failed, so this \
             run would certify a server that was never jailed. WSL2 is where both legs are \
             verified (design §12). Set {ALLOW_SKIP}=1 to skip deliberately on a host that \
             genuinely cannot confine."
        );
        eprintln!(
            "skipping: this host cannot confine under {model:?} and {ALLOW_SKIP} is set (design \
             §12)"
        );
        return false;
    }
    true
}

/// A path **as a confined process sees it**: `/proc/<pid>/root` is that
/// process's root directory, so everything below it resolves through the
/// child's own private mount namespace rather than through the host's.
///
/// **This is the one probe that can see a jail from outside it**, and it is
/// what makes the read-only leg below an assertion about the confinement rather
/// than about the data plane: the S3 layer refuses a write in read-only mode
/// whether or not the served root was ever mounted read-only, so a signed `PUT`
/// would pass with the seam broken.
///
/// The leading separator is stripped rather than kept, because `Path::join`
/// with an absolute path *replaces* the base instead of extending it — which
/// would silently probe the host's own path and answer about a root this
/// process was never confined to.
fn confined_view(pid: u32, path: &Path) -> PathBuf {
    PathBuf::from(format!("/proc/{pid}/root")).join(path.strip_prefix("/").unwrap_or(path))
}

#[test]
fn a_confined_server_serves() {
    if !confinement_available() {
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

    // No precondition is left to blame here, so the wait's own failure is the
    // failure — with the child killed and its stderr quoted, because an `Error`
    // frame carries the confinement's rendering and the stderr behind it
    // carries the backend's own `error!` line for whatever refused.
    let addr = wait_for_listening(&mut child);

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

/// The read-only seam, end to end: `serve <root> --sandbox --read-only`.
///
/// **What this leg is for.** The phase's read-only assertions all ran against
/// `sandbox-child` (`crates/tinio-sandbox-linux/tests/jail.rs`), and its
/// `--read-only` assertions against an *unconfined* `serve`
/// (`serve_read_only.rs`) — so the line design §9 calls the seam, where `serve`
/// turns its **resolved** `access` into the exposure set, was never exercised
/// through the real entry point. The failure that hides there is the one the
/// phase's own §9 warns about: a server confined against a served root whose
/// access mode nobody asked the mode chain about.
///
/// **The assertions are the confinement's, not the data plane's.** The plane
/// refuses a write in read-only mode whether or not the root was ever mounted
/// read-only, so a signed `PUT` here would stay green with the seam broken —
/// which is why the probe is [`confined_view`]: the read that must succeed
/// proves the jail reaches the served root at the path it was told to serve,
/// the write that must fail is the kernel's `EROFS` about the mount the server
/// is living on, and the state directory's write is the other half of the same
/// exposure set — always read-write, which is what makes the relocation
/// (FR-023) survivable inside the jail.
#[test]
fn a_confined_read_only_server_refuses_writes_through_the_served_root() {
    if !confinement_available() {
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().join("root");
    let state = tmp.path().join("state");
    // **Seeded first, because read-only mode refuses to create the root it was
    // told to serve** — a missing one fails startup rather than being written,
    // and the confinement then has nothing to expose.
    fs::create_dir_all(&root).expect("the served root must already exist");
    fs::create_dir_all(&state).expect("the relocated state dir");
    fs::write(root.join("obj.txt"), "hello from the served root\n").expect("seed the root");

    let mut child = Command::new(serve_bin())
        .args([
            root.to_str().unwrap(),
            "--port",
            "0",
            "--sandbox",
            "--read-only",
        ])
        // The state has to leave the served root in read-only mode (FR-023),
        // and the derived `<home>/.tinio/roots/<hash>/` would write into the
        // real home of whoever runs this leg.
        .env("TINIO_STATE_DIR", &state)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn serve");

    // Still serving: the jail built with a read-only served root and a
    // writable state directory, the plane opened its database inside the
    // latter, and the listener is reachable from outside.
    let addr = wait_for_listening(&mut child);
    let mut stream = TcpStream::connect(addr).expect("connect to the confined server");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    assert!(response.starts_with("HTTP/1.1 "), "{response}");

    // The read first, through the confined process's own root: the planted
    // object is where this leg says it is, so the refusal below is about the
    // mount rather than about a path that does not exist.
    let body = fs::read_to_string(confined_view(child.id(), &root.join("obj.txt")))
        .expect("the served root must be readable inside the jail");
    assert_eq!(body, "hello from the served root\n");

    // The promise FR-023's kernel-level half makes, asked of the mount rather
    // than of the plane. `EROFS` and nothing else: any other errno is a
    // different failure — a permission accident, a path that is not there —
    // and a leg that accepted one would report a confinement it never
    // established.
    let refused = fs::write(
        confined_view(child.id(), &root.join("should-not-exist")),
        b"nope",
    );
    let err = refused.expect_err("a write through the served root must be refused");
    assert_eq!(err.kind(), io::ErrorKind::ReadOnlyFilesystem, "{err}");

    // The other exposure of the same set, and the one a read-only run cannot
    // do without: the relocated state directory is read-write inside the jail.
    fs::write(confined_view(child.id(), &state.join("probe.txt")), b"ok")
        .expect("the relocated state dir is read-write inside the jail");

    // The host's own root is untouched — the refusal was the jail's, and the
    // residue of a broken seam would be visible right here.
    assert!(
        !root.join("should-not-exist").exists(),
        "the host's served root must not carry the refused write"
    );

    child.kill().expect("kill");
    child.wait().expect("wait");
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-server --test sandbox_e2e`
Expected: FAIL to compile until the target is declared, then PASS — or a printed `skipping:` on a host that cannot confine.

- [x] **Step 3: Wire the test target**

`crates/tinio-server/Cargo.toml`:

```toml
[[test]]
name = "sandbox_e2e"
required-features = ["sandbox"]
```

- [x] **Step 4: Migrate the e2e harness to the event stream**

`crates/tinio-e2e/tests/steps/clients.rs` currently reads the child's stdout until it finds `listening on `. Replace the scrape with a decode, keeping the same shape — read the stream, stop when an event says the listener is up:

```rust
/// Read the child's stdout until it reports its first event.
///
/// The child speaks the sandbox's event stream (design decision 19): one SSE
/// frame per event, an `event:` line and a `data:` line, closed by a blank line.
/// **A frame is not a line**, so this accumulates until the blank line and
/// decodes the block — decoding line by line would hand `Event::decode` half a
/// frame, and searching for the old text would find nothing at all. Text that is
/// not a frame — a log a dependency wrote before the subscriber was installed,
/// say — does not decode and is skipped rather than fatal, which is the property
/// the framing buys and the reason this is not a `strip_prefix`.
///
/// **The event, not just the address**: a refusal arrives as an `Error` frame,
/// and the caller is the side that can name it. A reader thread keeps the pipe
/// drained so the child never blocks on a full buffer; the thread exits when the
/// child dies.
fn wait_for_ready(stdout: impl Read + Send + 'static) -> Option<Event> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut frame = String::new();
        while reader.read_line(&mut line).is_ok_and(|n| n > 0) {
            if line.trim_end().is_empty() {
                if let Some(event) = Event::decode(&frame) {
                    let _ = tx.send(event);
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

**The address is a `SocketAddr` now, not a `String`, and the reader hands back the whole `Event` rather than only an address.** That is the point of typing the field: `serve` gets it from `TcpListener::local_addr()` and the harness passes it straight to `TcpStream::connect`, with no text in between that could disagree with what was actually bound — `spawn_child`'s `.trim().strip_prefix(READY)` disappears. **The `Event` is the second half and it is not a convenience**: a refusal arrives as an `Error` frame, and discarding it and reporting the `None` below makes a server that refused to start read as the stale-binary guess, which sends the reader after the wrong thing. So the call site matches on the event — `Some(Event::Listening { addr })`, `Some(Event::Error { message })` (terminate the child, then panic with the frame's own message), `None` (the stale-`.exe` hint).

Delete `const READY` — a constant that exists to be searched for has no role once the line is structured. Add `tinio-sandbox` to `tinio-e2e`'s dependencies if it is not there — a `path` dependency, as the crate's manifest has it, not a `.workspace` one; the harness needs the event type, not a copy of its spelling.

Then find every feature and step that asserts readiness and confirm it still holds:

```bash
rg -n "listening on|READY" crates/tinio-e2e/ crates/tinio-server/tests/
```

The expected residue is the harness's own doc comments. Anything else — a step definition matching the old text, a `.feature` quoting it, `serve_read_only.rs`'s own constant and waiter — changes here too.

**One near-miss worth knowing about, so nobody "fixes" it**: `crates/tinio-server/src/data.rs` has `tracing::info!("S3 data plane listening on {}", …)`. It is the same phrase on the *tracing* channel, which writes to stderr; it is not the protocol and not a second readiness line. The grep above is scoped to the two test directories for exactly that reason — widening it to all of `crates/` will find this line and it should be left alone.

- [x] **Step 5: Run the e2e suite**

Run: `TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e --test cucumber`
Expected: PASS, **and the plain form of this command is not the check it was written as.** `tests/cucumber.rs` filters the external tags out unless `TINIO_E2E_EXTERNAL` is set, and the migrated reader is reached only through `External::start` — so `cargo test -p tinio-e2e --test cucumber` spawns no `serve` at all and **never calls `wait_for_ready`**. Both forms were run: 236 scenarios plain, 265 with the variable set, and only the second one exercises what this step exists to check. **CI does run the external form** — its `interop` and `interop-port` jobs pass `--tags '@interop'` with `TINIO_E2E_EXTERNAL`, and `@interop` scenarios take `External::start` — so the migration is proven in CI and by Task 9's script, and it is proven *outside* the local pre-push gate rather than nowhere. **`docs/tests.md` lists the external legs under "CI-only legs … out of gate"**, and this is why Task 9's script runs the external form explicitly: nothing else the *local* gate runs executes the migrated reader.

- [x] **Step 6: Verify the skip path is legible**

Run the test on a host that cannot confine (or with `TINIO_SANDBOX_MODEL=root` as an unprivileged user, which forces the refusal).
Expected: **a failure, not a pass, unless `TINIO_SANDBOX_ALLOW_SKIP` is set.** This is the correction to this step's earlier expectation, which said "a `skipping:` line on stderr and a pass": an `eprintln!` skip is swallowed by libtest without `--nocapture`, so a host that cannot confine reported a green run that certified nothing — design §12's "a suite of skips exits 0" arriving in the suite that exists to prevent it. So an absent precondition **fails by default** and names the `unshare` command it tried; setting `TINIO_SANDBOX_ALLOW_SKIP=1` is what takes the quiet path, and the skip then prints the line. The variable and the rule are the jail test's, spelled identically in both files so one habit covers them. **The skip happens at `host_can_confine`, before the server is spawned** — and the reason the opt-in exists rather than a plain `#[ignore]` is that an ignored test is a test nobody runs. **This has blast radius beyond the test suite**: a Linux CI runner that cannot create an unprivileged user namespace — Ubuntu 24.04 with AppArmor's restriction among them — will now fail this leg instead of passing it quietly, which is the intended reading and a change the workflow may need to know.

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

- [x] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# Verify the Linux sandbox inside WSL2, under both confinement models.
# Usage: bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh [cargo test args...]
#
# WHAT IT NEEDS. A Linux host (this is the WSL2 leg of `docs/tests.md`'s
# platform-gated verification) with an unprivileged user namespace and
# passwordless `sudo`, plus everything the external e2e leg at the end shells
# out to: `aws` (cli v2), `rclone` and `mc` on `PATH`, and the boto3 venv the
# `@boto3` scenarios run inside (`TINIO_BOTO3_PYTHON` overrides its location).
# Anything missing fails that leg; it is never a skip, because a suite that
# certifies the harness must not report success for a client it never ran.
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

# `RUST_TEST_NOCAPTURE=1` on both model legs, for one line: the jail test prints
# the read-only arm it read out of the child (`read-only-arm: remount` or
# `… overlay`), and the harness would otherwise swallow it along with the rest of
# a passing test's output. Which arm each model takes is what separates these two
# legs — a run that says `2 passed` twice cannot be told from one that exercised
# the same mechanism twice — so the run's own output has to carry it.
echo "== user-namespace model =="
TINIO_SANDBOX_MODEL=userns RUST_TEST_NOCAPTURE=1 \
  cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== root model =="
# A target directory of its own for the privileged leg. Cargo run under `sudo`
# leaves root-owned artifacts behind, and the user-owned legs that follow then
# fail to rebuild over them — so the two identities never share one.
#
# The environment is passed through `sudo env` rather than `sudo -E`: this
# host's sudoers says "preserving the environment is not supported, '-E' is
# ignored", so an `-E VAR=…` spelling runs the leg with the variable **unset**
# and the root model silently becomes the default one — which is exactly the
# two-leg reasoning this script exists for. The explicit PATH is also what
# finds cargo at all under `sudo`, whose `secure_path` rarely includes
# `~/.cargo/bin`.
SUDO_TARGET_DIR="${CARGO_TARGET_DIR:-$HOME/tinio-target}-root"
sudo env "PATH=$PATH" "HOME=$HOME" "CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}" \
  "CARGO_TARGET_DIR=$SUDO_TARGET_DIR" TINIO_SANDBOX_MODEL=root RUST_TEST_NOCAPTURE=1 \
  cargo test -p tinio-sandbox-linux -p tinio-sandbox "$@"
echo
echo "== the confined server, end to end =="
# The example is built first, and separately: a filtered `cargo test --test
# sandbox_e2e` builds test targets only, and the test resolves the serve binary
# from its own profile directory. The next leg needs the same example, built by
# the same profile, which is why this build sits in front of both.
cargo build -p tinio-server --example serve
cargo test -p tinio-server --test sandbox_e2e
echo
echo "== the external e2e suite =="
# **The external form, not the plain one.** `tests/cucumber.rs` filters the
# external tags out unless `TINIO_E2E_EXTERNAL` is set, and the migrated
# readiness reader (`wait_for_ready`, which decodes the `listening`/`error`
# event frame rather than scraping a printed line) is reached only through
# `External::start` — so `cargo test -p tinio-e2e --test cucumber` spawns no
# `serve` at all and cannot fail if that reader is wrong. CI proves the
# migration in its `interop` jobs; this is the leg that proves it here.
# Everything the external set reaches for is asked for **here**, not discovered
# two hundred scenarios in: the three client binaries its scenarios shell out to,
# and the venv the `@boto3` ones run inside (boto3 must never be the system
# python). The venv's conventional home is the target directory, which is what
# the harness's own `target_dir()` resolves, `TINIO_BOTO3_PYTHON` overriding it.
for tool in aws rclone mc; do
  command -v "$tool" >/dev/null || {
    echo "FATAL: '$tool' is not on PATH, so the external e2e suite cannot run." >&2
    echo "       Install it, or drop this leg deliberately — a skipped leg here" >&2
    echo "       would certify a harness that never ran." >&2
    exit 1
  }
done
BOTO3_VENV="${TINIO_BOTO3_PYTHON:-${CARGO_TARGET_DIR:-$PWD/target}/tinio-e2e-venv/bin/python3}"
if [[ ! -x "$BOTO3_VENV" ]]; then
  echo "FATAL: the boto3 venv python is not at $BOTO3_VENV, and the '@boto3'" >&2
  echo "       scenarios run inside it. Create the venv there, or point" >&2
  echo "       TINIO_BOTO3_PYTHON at your own:" >&2
  echo "       python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3" >&2
  exit 1
fi
TINIO_E2E_EXTERNAL=1 cargo test -p tinio-e2e --test cucumber
```

Make it executable: `chmod +x crates/tinio-sandbox-linux/scripts/wsl-chroot.sh`.

**Two WSL2 facts this script exists to work around**, both measured on this machine while writing the design: the distro idle-shuts-down between sessions, which silently discards anything under `/tmp` (a probe report was lost this way), and `/tmp` is itself a tmpfs. A long `cargo test` run keeps the distro alive by itself, but a run that pauses must not be trusted to have kept its scratch files.

- [x] **Step 2: Run it**

Run from the repository root inside WSL2:

```bash
wsl.exe -d Ubuntu-26.04 -- bash -c 'cd /mnt/e/GitHub/tinio/.claude/worktrees/sandbox && bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh'
```
**The path is the worktree this phase runs in, not the main checkout**, and the `cd` is the only part of the command that knows it: this plan was written before the phase started in a git worktree, and the main checkout is on a different branch without this work — running it there would verify another branch's tree and report a green suite for code that is not here. The script itself is repo-relative (`cd "$(dirname "$0")/../../.."`), so the command works from wherever the worktree is.

Expected, measured end to end twice at `EXIT=0`: the user-namespace leg and the root leg both PASS at 99 tests each with 0 failed and 0 skipped — so neither took the skip path, which the opt-in rule makes provable rather than assumed — plus `sandbox_e2e` **2 passed** — it gained a read-only leg in the phase's final review, and its second test is the one that exercises design §9's seam through the real entry point — and the external e2e at 265 scenarios / 2087 steps. **The two legs are told apart by the `read-only-arm:` line the jail test prints** under `RUST_TEST_NOCAPTURE=1`: the unprivileged leg reported `overlay` and the privileged leg `remount`. **(2026-09-24: pre-fix values — with the remount re-asserting the source mount's locked per-mount flags, the unprivileged leg reports `remount` too, so the two legs are no longer told apart by that line and the overlay arm has no end-to-end leg; design §4 records the change and the coverage gap.)** If `sudo` prompts for a password, the environment is not the passwordless-sudo WSL2 setup this script assumes — run `sudo -v` once first. The two legs deliberately build into different target directories (`…-root` for the privileged one), so the root leg's first run compiles the workspace again from scratch; that is the price of not handing root-owned artifacts to the user-owned legs.

- [x] **Step 3: Record the commands in `docs/tests.md`**

Under the existing `## Platform-gated verification (WSL2)` section, add a bullet in the file's compressed style:

```markdown
- Confinement (`--sandbox`): `bash crates/tinio-sandbox-linux/scripts/wsl-chroot.sh` — both models, the confined server, and the external e2e leg (`TINIO_E2E_EXTERNAL=1`; needs aws cli v2, rclone, mc and the e2e boto3 venv, and fails without them).
- The confinement tests **fail** rather than skip when the host cannot create an unprivileged user namespace — a skip through `eprintln!` is swallowed by libtest and would report a green suite that certified nothing. `TINIO_SANDBOX_ALLOW_SKIP=1` opts out deliberately, naming the `unshare` command that was tried. A CI runner on Ubuntu 24.04's AppArmor default is therefore red until unprivileged user namespaces are allowed or the binary carries a profile; that is the intended reading, not a flake.
```

**`docs/tests.md` gains one more line, in its CI-legs section rather than this one**: the `serve` example deliberately carries no `required-features`, and adding one breaks the `no-default` CI leg (an explicit `--example` selection with unmet features exits 101, while the workspace gate silently skips it). The rule landed there because a manifest comment stating it was removed from `crates/tinio-server/Cargo.toml`, which `docs/cargo.md` forbids, and because it is a fact about the CI matrix rather than about WSL2 — Task 7 Step 1 is where it is decided.

**The second bullet is the one this plan's own testing rule forced.** Ruling: an absent precondition fails by default, and the opt-in exists because a suite that certifies a confinement "does not get to skip in silence". It is recorded in `docs/tests.md` rather than only in the test, because the blast radius is a CI runner rather than a developer's machine.

- [x] **Step 4: Record the lint exception in `docs/cargo.md`**

Change the lint bullet so the exception is stated where the rule is, not only in the design:

```markdown
- `[lints.rust] unsafe_code = "forbid"` on every crate. The sandbox platform backends — `tinio-sandbox-linux`, `tinio-sandbox-macos` and `tinio-sandbox-windows` — become `"deny"` **as each backend lands**, with its `#[allow(unsafe_code)]` sites enumerated in the sandbox design's decision 14; a stub that has no `unsafe` site yet stays `forbid`, because the lint follows the implementation. `forbid` cannot be overridden, so `deny` is what makes the exception expressible (`docs/superpowers/specs/2026-09-18-sandbox-design.md`).
```

**This plan writes the rule once, for all three backends, and it is the only plan that writes it.** The design's Bookkeeping says the rule "gains the exception in the same commit as the first of them", and the first of them is this phase's `tinio-sandbox-linux`, so this is where the text changes. Naming `-macos` and `-windows` in the same line is what keeps the later phases from rewriting it — `2026-09-18-sandbox-p2-windows.md` and `-p3-macos.md` each used to carry their own version of this bullet, worded differently, and those were removed precisely because three documents writing one line is how they drift apart.

**And the line is written in the tense of the rule, not of the moment.** At this commit only `tinio-sandbox-linux` is `deny`; the macOS and Windows crates are still `forbid` stubs with no `unsafe` site to enumerate, which is decision 14(d)'s "the lint follows the implementation" applied to them. A bullet that asserted all three were already `deny` would be false for two crates until P2 and P3 land — so it says when each becomes `deny` instead, and each later phase **verifies** the line and flips its own manifest when its first site arrives rather than restating the rule.

**The bullet states the rule and not the sites, because `docs/cargo.md` is a Cargo.toml convention file and nothing else.** Its sections are Rules, Versions, Features, Groups and Example; a `deny`-versus-`forbid` lint choice is a `[lints.rust]` line and belongs there, while a list of source modules is not a Cargo.toml convention at all and does not. Enumerating which functions carry `#[allow(unsafe_code)]` would also require this phase to know the Windows modules P2 has not written yet and the macOS site P3 has not either; the enumeration's home is decision 14, which the bullet points at. A `docs/cargo.md` that listed sites would go stale in the same commit that added one.

- [x] **Step 5: Write the operator notes in `docs/sandbox.md`**

**`--sandbox` is a feature an operator switches on and then has to live with, and three of its consequences exist nowhere but in this plan's reasoning** — a reader would have to derive them from design §4's mount table, decision 13's filter and the capability policy to know why their objects started answering `AccessDenied`. None of the three is a defect; each is the confinement working as designed, and each reads as a bug the first time it is met without warning. Create the file with this content (one unbroken line per paragraph, the rule every document under `docs/` follows):

```markdown
# `--sandbox`: what an operator has to know

`--sandbox` confines the server to the paths it is given — a private mount namespace, `pivot_root`, and a seccomp filter over outbound connection setup. The notes below are Linux's, because Linux is the platform that has landed, and each is a consequence of the confinement working as designed rather than a defect: all three read as bugs the first time they are met without warning.

## The process has no capabilities, even when it runs as root

The confinement drops every capability before the server starts, `CAP_DAC_OVERRIDE` and `CAP_DAC_READ_SEARCH` among them, so the process reads files as the account it runs as rather than as the superuser it still is: under the root model it is uid 0 with an empty capability set apart from the single `CAP_CHOWN` a `local_uid` mapping needs. A file under the served root that the service account does not own and that is not readable by others is therefore refused, and an S3 client sees `AccessDenied` for objects that were readable before the flag was added. The user-namespace model states the same rule from the other side: its namespace maps exactly one uid — the invoking user's — so a file owned by any other host id is as unreadable inside the jail as a stranger's file is outside it. Run the server as the account that owns the data, or `chown -R` the served root to that account.

## Outbound connections are denied, and the filter cannot see the address family

The seccomp filter denies `connect`, `sendto`, `sendmsg` and `sendmmsg` by syscall number, so no address family can be let through and `connect(AF_UNIX)` is denied along with the TCP ones: inside the jail, `/dev/log` (syslog), a Docker socket and a D-Bus connection all fail with `EPERM`. This is deliberate — a filter that could see the family would be a different and larger program — and it is survivable because the server's own logs go to stderr rather than to syslog. There is no allowance to add: a Unix socket inside an exposed directory is unreachable too, because the refusal is at `connect`, not at the path.

## The host's path namespace does not exist inside the jail

The jail is a private mount namespace with `pivot_root`, and what it contains is the exposure set **plus the four device files every `serve` jail binds in — `/dev/null`, `/dev/zero`, `/dev/random` and `/dev/urandom`** — each entry at its original absolute path. **"The exposure set and nothing else" is literally untrue and is not what this note says**: the device files are bound by the construction `serve` makes (design decision 12), so a reader who checked the jail against that sentence would find four entries the sentence denies. Everything else is absent rather than refused: `/etc`, `/proc`, the host's `/tmp` and the operator's home do not exist in there, and a path that is neither an exposure nor one of those four fails as "no such file or directory" rather than as a permission error. This is where Linux differs from the other two platforms, which confine by policy — a Windows AppContainer's DACLs, a macOS Seatbelt profile — and in those the host's paths are visible and refused. So a configuration file, a helper binary or a data path the server needs on Linux has to be exposed, and what would have been a permission error at startup is a missing file instead.
```

Then one line in `README.md`, so the notes are reachable from where the flag is first met: the `Layout` section's closing sentence names the other `docs/` files (`` Workspace docs: [`docs/`](docs) — cargo conventions, code style, test guide. ``), and `docs/sandbox.md` is added to that list. Nothing else in the README changes: the flag's own documentation is the file, not the front page.

**Why a file of its own, and why this phase writes it.** Task 9 is already the task that records what this phase owes the repository — the WSL2 commands in `docs/tests.md` and the lint exception in `docs/cargo.md` — and these notes are the third of those. They live in `docs/` beside the other reference documents rather than in the README, which is a front page that links out; the README gets one line and no more. **This phase creates the file and owns it**: a launcher phase extends it with its own platform's notes instead of creating a sibling, which is the one-owner rule the shared document states for rules, names and code (shared §1, §4) applied to the one document in this feature that is written for an operator rather than for an implementer.

- [x] **Step 6: Run the full local gate**

Run the **Standard acceptance (pre-push gate)** block in `docs/tests.md` in full — it is the bar, and it is a longer list than the one this step used to carry (shared §1). The legs this phase adds to it are the two feature-off checks:

```bash
cargo check -p tinio-server --no-default-features
cargo check -p tinio-server --no-default-features --features multipart,copy,list-v1,list-v2,cors,select,acl
```

**Run the block inside WSL2, and this is the one part of it a phase cannot decide for itself.** Every per-task gate runs there, because the crate's body is `#![cfg(target_os = "linux")]` and a Windows `cargo test -p tinio-sandbox-linux` is a false green on an empty library — but **the workspace-level block is the one that quietly does the same thing at a larger scale**: `cargo check --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace --exclude tinio-e2e` all *succeed* on the Windows host while compiling none of the Linux crate's code, so a phase that ran its own acceptance block there would certify itself against a build that never happened — the exact failure class this design's fail-closed reasoning exists to refuse. `CARGO_TARGET_DIR` must be set explicitly in every invocation, and the tools the block needs (`cargo-hack`) are present there while the CI-only legs are not (`cargo-nextest`, `cargo-audit`, `cargo-semver-checks` — `docs/tests.md` puts all three under "CI-only legs … out of gate", and `semver-checks` is unrunnable locally besides, because `tinio` is `publish = false` and has no registry baseline).

Expected: all clean, and **17 of 19 legs green**. The two red ones are pre-existing and belong to another workstream: `cargo check -p tinio-server --features otel` fails `E0277 SdkTracer: opentelemetry::trace::tracer::Tracer is not satisfied`, reproduced with no sandbox crate in the graph and tracking an `opentelemetry` pin that landed before this phase. **One consequence of that failure belongs to this phase and was chased down**: `cargo hack check --each-feature` aborts on the failing combination, and cargo-hack orders `otel` immediately before `sandbox`, so `tinio-server`'s own `sandbox` combination was masked — never built, by the leg that exists to build every combination. Re-running with `--exclude-features otel` builds it (12/12, `sandbox` at 10/12), which is what makes the feature this phase added a built combination rather than an unbuilt one.

The clippy leg is what proves the `unsafe_code` exception stayed at three sites: `rg -n '^#\[allow\(unsafe_code\)\]' crates/tinio-sandbox-linux/src` must print exactly three — the anchored form, for the reason Task 4 Step 5 gives.

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
- Read-only enforced by remount, or overlay where the source's locked per-mount flags refuse it
- Outbound syscalls denied; filter cannot see address family, so AF_UNIX fails too
- Filter installed last: irreversible, and a mistake cannot be repaired at runtime
- Falls back to root when unprivileged namespaces are refused, else fails closed
- Bounding set swept before the set is narrowed: the reverse order drops nothing
- The example builds without the `sandbox` feature: gating it broke the no-default CI leg
- `serve` folds the resolved mode and the uid mapping into the backend's options
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
| decision 8 read-only is the absence of a write permission | 2, 5, 6 — §4's mechanism (the read-only remount, with the `MS_RDONLY` overlay as its fallback where the **source's locked per-mount flags** refuse it) is chosen by `apply_mount` rather than by the plan, and the nested-mount refusal beside it is Task 5's |
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
| `serve` reaches `Options`, `Backend` and `Seccomp` through the facade's cfg-gated `linux` alias, because the facade deliberately exports no platform options — **the target-gated dependency on `tinio-sandbox-linux` this row used to record was added and then removed**, because it contradicted the facade's own "consumers name this crate and nothing else" and would have grown one target-gated dependency per platform by P3 | Task 7 |
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
| `Mechanism` lives in `mechanism.rs` and is re-exported from `lib.rs` as `tinio_sandbox_linux::Mechanism`; in-crate modules keep `crate::Mechanism`. The rows that forbade that re-export assumed P0 still defined the enum at the crate root, which would have been `E0255`; the type moved, so the re-export is now the one public path. **The still-earlier spelling of this row said `mechanism.rs` re-exported it while P0 defined it, and that was the mechanism the first draft of Task 2 depended on — the import it produced had nothing to compile against until Task 3 existed** | Tasks 2, 3 |
| The child honours `TINIO_SANDBOX_MODEL` through `Options::from_env()`, so the two acceptance legs run different code; and both skip paths became a precondition asked as a capability (`unshare(1)`, before the child or the server starts) instead of a substring of a failure — **and then a precondition that fails by default**, with `TINIO_SANDBOX_ALLOW_SKIP=1` as the deliberate opt-out, because an `eprintln!` skip is swallowed by libtest | Tasks 6, 8 |
| `local_uid` is refused against the **resolved** model as well as the request, which is the half `Mechanism::Auto` can reach and the request cannot describe | Task 5 |
| The x32 ABI gap in the seccomp filter is recorded beside the filter and added to the design's Risks as this phase lands | Task 4 |
| `assert_cmd` is gone from the stack and from the manifest claim; `options.rs` is in Task 5's file list and the invented `new` is out of `Options`; the red-test filter is two substrings rather than one regex-looking literal that matched nothing and exited 0; `install() -> Result<Seccomp>` matches its implementation | Tasks 3, 4, 5 |
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
| `$TMPDIR` is stated as a precondition of the whole confinement rather than discovered as a stray errno: the jail is created inside it, so a read-only, missing or full temp directory fails at the first `create_dir_all`, and the step string names the resource — `create jail root in $TMPDIR` — for the operator whose exposures are not at fault | Task 5 (the entry sequence, the `create_dir_all` step, which moved ahead of the first `unshare`), `Options::with_tmpdir`, Task 6 Step 4 |
| `docs/sandbox.md`, the operator-facing section this feature had nowhere to put: the process has no capabilities even as root (an unowned, unreadable file under the served root answers `AccessDenied`), the seccomp filter cannot see an address family so `AF_UNIX` connects are denied with the rest (`/dev/log`, a Docker socket, D-Bus: `EPERM`), and the host's path namespace does not exist inside the jail at all, unlike the two policy-based platforms — with the README's doc index gaining the one line that points at it | Task 9 Steps 5-7 |
| The read-only mechanism is **not** chosen by the plan: every read-only exposure enters the mount plan as `Bind` under either model, `apply_mount` remounts `RDONLY\|BIND` and substitutes the `MS_RDONLY` overlay only on `EPERM`, and the model-based test is replaced by a both-models one, with the overlay's own test moved to the errno reading because the branch itself needs a filesystem (Task 6's unprivileged leg is where it runs). **The two rules this row has carried are both wrong and both are corrected here**: "the model decides" was the first, "the filesystem decides" the second — a `strace` of the real child falsified the second, and what refuses the remount is a **locked per-mount flag**, since `lock_mnt_tree()` runs in `copy_mnt_ns()` whenever the new mount namespace belongs to a different user namespace and the request must re-assert every locked flag. The child now reports which arm it landed on (`read-only-arm:`), and the parent asserts only that it is one of the two. **(2026-09-24: the remount re-asserts the source mount's locked per-mount flags, so `apply_mount`'s `EPERM` branch no longer runs on the unprivileged leg and the overlay arm has no end-to-end coverage; design §4 records the change and the gap.)** | Task 2 (prose, `MountKind`, `plan`, tests), Task 5 (`apply_mount`, `refused_remount_needs_the_overlay`, the nested-mount refusal), Task 6 |
| Task 7 Step 7's "a bad exposure fails closed" check replaced with one the validation actually reaches — a relative root — and the reason the old spelling could not fail (`create_dir_all` in front of the sandbox arm) is now in the step, so nobody re-adds it | Task 7 Step 7 |

**What the implementation changed (2026-09-20).** The phase was executed task by task, and every row below is a place where the code as landed differs from the blocks above or from a rule they stated. Each is corrected where it lands, as the earlier tables are, and this table is the index to them.

| Change | Where |
|---|---|
| `LinuxOptions` is `Options`, and a consumer reaches it through the facade's cfg-gated `linux` alias rather than by naming the platform crate: `tinio_sandbox_linux::LinuxOptions` stutters twice, and a per-platform dependency in `tinio-server` would become one edge per platform by P3. **This reaches Step 2's red-run expectation**, which names the missing struct | Task 5 (every block and the Interfaces), Task 6, Task 7 Step 1 (the target-gated table is removed, not added), `serve.rs`'s four imports |
| The read-only rule is corrected **twice**: "the model decides" was replaced by "the filesystem decides", and a `strace` of the real child falsified that too. What refuses the remount is a **locked per-mount flag** — `lock_mnt_tree()` in `copy_mnt_ns()` — so every statement of the old rule is rewritten, in both vocabularies it was stated in. **(2026-09-24: that locked-flag rule now cuts the other way for the overlay — the remount re-asserts the locked flags, so the unprivileged leg takes the remount arm and the `EPERM` overlay branch is no longer exercised end to end; design §4 owns the change and the coverage gap.)** | Task 2 (the section prose, `MountKind`, `plan` and its test comment), Task 5 (`apply_mount`, `refused_remount_needs_the_overlay`), Task 6 (the legs paragraph and the child), the Self-Review tables |
| A read-only exposure whose tree contains a **writable nested mount** is refused rather than silently under-enforced: `mount_bind_recursive` carries submounts in, a successful remount makes only the top read-only, and `mount_setattr(AT_RECURSIVE)` is not in `rustix` 1.1.4 while reaching the syscall would be a fourth `unsafe` site. The check runs *after* a successful remount and only when the remount succeeded — the overlay arm covers the whole view — and the predicate is writability, not existence | Task 5 (`writable_nested_mounts_in`, `decode_mountinfo`, `read_mountinfo`, `apply_mount`'s `Ok` arm, two new tests) |
| `backend()` becomes `Backend::default()`, because `Backend` stops being a unit struct; the trait impl clones its options because `self.enter(sandbox, &self.options)` is `E0502`; `pub mod mechanism` and not `pub mod model`; `Mechanism` is defined in `mechanism.rs` and re-exported from `lib.rs`, which is what makes `plan.rs`'s `crate::Mechanism` resolve | Task 3 (the enum, the module doc, the re-export), Task 5 Step 5 |
| `jail_root` hashes the **component sequence** rather than the path's bytes, so one root has one name however a shell spelled it, and the `!file.contains(&pid.to_string())` assertion is **deleted** — the digest is 16 hex characters, so any pid whose decimal spelling occurs inside it fails a correct implementation, and the length assertion beside it is the real pid proof. A tenth test pins the spelling property | Task 1 Steps 1 and 3 |
| The four fixes a review of the jail test found, each inherited from the brief: the outbound probe is a **listener the parent binds** rather than a fixed address on the network (so an offline host cannot skip the assertion); the child gains an `old-root-visible` probe (until then the `pivot_root` escape was unasserted); `host-home-visible` probes `$HOME` itself rather than `$HOME/.ssh`, which does not exist on every host and made the line unfalsifiable; and an absent precondition **fails** rather than printing a skip libtest would swallow, with `TINIO_SANDBOX_ALLOW_SKIP=1` as the deliberate opt-out | Task 6 Steps 1 and 2 |
| Invariant 2 is impossible as written: `Pipelines::build` starts multi-threaded runtimes and `unshare(CLONE_NEWUSER)` refuses a multi-threaded caller, so the lanes, the storage and the runtime run **after** the sandbox arm while the state directory is resolved as a *path* before it. `serve` consequently gains two parameters — `access` and `pipelines` — for values the body cannot reach for itself | Task 7 Step 5 (invariant 2, the `serve` signature, the `main` block) |
| The unsafe-site check is the **anchored** pattern, because the unanchored one prints a fourth line: `seccomp.rs`'s module-scope note quotes the attribute while explaining why the `prctl` declaration cannot live there. The probe's `#[allow(unsafe_code)]` is stated on `userns_available` itself. Three sites, and the count is unchanged by any of the fixes | Task 4 Step 5, Task 5 Step 7, Task 9 Step 6 |
| The `serve` helpers grow: `emit_on_error` (the rest of decision 19's "a startup failure is an event rather than an absence") and `sandbox_request`, which is the construction `main` calls — the device-files test built its **own** `Sandbox`, so flipping the flag would have left every test green while the shipped jail lost `/dev/null` (`emit_on_error` is the phase's spelling of `crates/tinio-cli/src/events.rs`'s `emit_error`, emitted once at the boundary in `run.rs`; that name has no definition in the tree) | Task 7 Steps 2 (the test), 4 and 5 |
| **The command line is `clap`'s, and help and version are disabled outright** (decision 19): a private `RawArgs` carries the flags, `Args` keeps its five field names and types, `parse_args` keeps its signature and its `Result`, and `usage()` is replaced by `refuse_command_line` — clap's rendering on stderr, the same text on an `error` frame, exit 2. **`no_binary_name` is the attribute a later phase trips on**: clap consumes the first iterator element as the program name, and every caller passes arguments with `argv[0]` already filtered off. **And the stdout-exclusivity grep cannot see into a dependency**, so the mechanical check moved to a test that counts the refused command line's real stdout lines | Task 7 Steps 1 (the `clap` pins), 2 (the parser's own tests), 4 and 5 (the parser, `refuse_command_line`, `main`), and the stdout paragraph in Step 5 |
| The privileged leg is invoked with `sudo env`, not `sudo -E`, which is inert where sudoers refuses to preserve the environment — the variable would be unset and the root leg would silently run the same code as the unprivileged one | Task 6 Step 5, Task 9 Step 1 |
| The acceptance block is run **in WSL2**, and the plan never said so: the workspace-level legs (`check`/`clippy`/`test --workspace`) succeed on the Windows host while compiling none of the Linux crate's code, so a phase that ran its own acceptance block there would certify itself against a build that never happened. The local gate also gains the external e2e form, because the plain cucumber run filters `@external` out and never reaches the migrated readiness reader | Task 9 Steps 1 and 6, Task 8 Step 5 |
| `create_dir_all(&jail)` moves **ahead of the first `unshare`** (the step string's whole point), `open_map` takes the map's name so a missing `/proc` is attributable, the `EINVAL` comment says that errno is reachable on a pre-5.9 kernel rather than impossible, `Options::from_env`'s doctest is environment-independent, and `from_env`'s example matches on the result | Task 5 Steps 3 and 4 |
| Every public item the blocks introduce without one carries a runnable `# Examples` block, and each was made true rather than decorative — the device-file and mount-plan examples assert the item's own behaviour | Global Constraints, Task 2, Task 3, Task 4, Task 5 |

**What the final whole-branch review changed (2026-09-20).** The phase's last review ran over the branch rather than over a task, and every row below is a defect no per-task review could see — each one is about the *seam* between two tasks' work, or about a build configuration no task builds.

| Change | Where |
|---|---|
| **The `[[example]] serve` block is gone, and `required-features` with it.** It broke a CI leg that was already there — `.github/actions/run-test-leg/action.yml`'s `no-default` mode builds `--example serve --no-default-features`, and an explicit `--example` selection with unmet `required-features` is a hard error (exit 101) — while the phase's own gate could not see it, because `cargo test --workspace --no-default-features` silently skips such a target. The removal is decisions 4 and 19 read literally: the example must build and serve in **every** feature configuration, and what the feature gates is the `--sandbox` arm's body. It also made `main`'s `#[cfg(not(feature = "sandbox"))]` refusal arm reachable for the first time, and `serve_read_only.rs` now has a test pinning it (exit 2 and an `event: error` frame) | Task 7 Step 1 (the manifest), Step 5 (the arm's comment), Step 8 (the expectation); `docs/tests.md`'s CI-legs section carries the rule |
| **`serve` folds the capability policy into `Options`** — `sandbox_options(base, config, access)` takes the **resolved** `access` and whether a `local_uid` mapping is configured, asked of `identity::owner_uids_for`. Handing `Options::from_env()` over raw meant `caps::retained` always got `(ReadWrite, false)`: design §8's single `CAP_CHOWN` was **never retained** for an operator with a `local_uid` mapping, so the first chown of a landed object failed `EPERM`, and a `--read-only` run exposed the served root read-write inside the jail. A unit test calls the production function for both fields | Task 7 Step 4 (the helper and its doc), Step 5 (the call site), Step 2 (the test) |
| **Three events are `info!` where the phase first wrote `debug!`** — the resolved model, the exposure set the jail applied, and the seccomp install (its *skip* was already a `warn!`). `serve` installs its subscriber at a fixed `Verbosity::Info`, so a **successful** `serve --sandbox` logged nothing at all through it, which is the failure shared §7 names as this feature's characteristic one; the per-item `debug!` events were left alone on purpose | Task 3 (`Mechanism::resolved`'s event), Task 5 (`enter`'s model and exposure-set events), Task 4 (the seccomp install), the subscriber paragraph in Task 7 Step 5 |
| **The crate has one failure path.** `refused`/`fail`/`step` are `mechanism.rs`'s — **the core's functions since the phase-basis correction, re-exported there with a `pub(crate) use`** — and `enter.rs` and `seccomp.rs` import them — including `install`'s refused `prctl`, which built its own `Error::Step` and its own `error!`. The duplication had already drifted on landing: `enter.rs` logged `sandbox step refused` where the other two logged `sandbox step failed`, so one failure was two events with two wordings. **This supersedes the plan's instruction to keep the copy** (Ruling 22's "the duplication stays") — a written-down prediction is worth exactly what the measurement says it is worth | Task 3 (the one definition), Task 5 (the copy removed, the import added), Task 4 (the inline `error!` routed) |
| **`sandbox_e2e` gained a read-only leg** — `serve --sandbox --read-only` against a pre-seeded root, asserting the server serves and that a write through the served root is refused, with `confinement_available()` as its precondition and `confined_view(pid, path)` (through `/proc/<pid>/root`) as the only probe that can see a jail from outside it. It goes red when the seam is deliberately broken to `ReadWrite`, which is the point: design §9's seam, where `serve` turns its resolved access into the exposure set, was never exercised through the real entry point. **`sandbox_e2e` is 2 passed, not 1**, and Task 9's expected results say so | Task 8 Step 1 (the file), Task 9 Step 2 (the measured result) |
| **The manifest comment is gone from `crates/tinio-server/Cargo.toml`**, and its rationale moved into `docs/tests.md`'s CI-legs section: `docs/cargo.md` §Rules forbids comments in a `Cargo.toml`, and the `[[test]] sandbox_e2e` block is the form the tree keeps | Task 8 Step 3 |
| **`Mechanism` lives in `mechanism.rs`, not `lib.rs`.** The crate root re-exports it (`pub use self::mechanism::{Mechanism, Resolved, userns_available}`), so callers keep `tinio_sandbox_linux::Mechanism` and in-crate modules keep `crate::Mechanism`. P0's stub no longer spells the enum; the compile-order split that put the variants in `lib.rs` and the behaviour in Task 3 is gone | Task 3 (the enum, the module doc, the re-export), Task 5 Step 5, P0 Step 4 |

**Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N", and **the one placeholder this plan did carry is now filled**: Task 5's `ALL_CAPABILITIES` was written as a "fill this in from rustix" instruction with two lines of the array, and it now lists all 41 `CapabilitySet` constants in kernel numbering order — the transcription happened, the review checked it constant by constant against `libcap.rs`, and a later phase reads a complete list rather than an instruction. Task 7 remains the step written against a file another branch is editing: it names seams and invariants instead of line numbers, and the landed file is the authority for the line numbers it used to quote.

**Type consistency.** `Sandbox`, `AccessMode`, `Exposure`, `Error` and `Result` come from P0 and are used verbatim. **So do the four failure builders — `refused`, `fail`, `step`, `launch`** (P0 Task 1's `error.rs`, landed by the phase-basis correction): this crate consumes the first three through `mechanism.rs`'s `pub(crate) use` of the core's — which is why `enter.rs`'s import and `seccomp.rs`'s `use crate::mechanism::fail;` are unchanged, and why no call site in this crate moved — and has no call for `launch`, because Linux confines in place and builds no `Error::Launch`. `Mount`, `MountKind`, `OverlayDirs`, `plan`, `DEVICE_FILES` (Task 2) are matched exhaustively in Task 5 — and `MountKind::OverlayReadOnly` is the one kind `plan` never emits, since the read-only mechanism is the filesystem's answer rather than the plan's choice (Task 5's `apply_mount` substitutes it, and `refused_remount_needs_the_overlay` is that decision in full). `jail_root`, `mirror_under` (Task 1) are used by Tasks 2 and 5. `Mechanism`, `Resolved`, `Cap`, `retained`, `Options` (Tasks 3, 5) are used by Tasks 2, 5, 6, 7 — **`Options` and not `LinuxOptions`**, and `Mechanism` is defined in `mechanism.rs` and reached as `crate::Mechanism` through the crate-root re-export; `Cap::set()` yields a single-bit `CapabilitySet` and never the deprecated `Capability`. `seccomp::{filter_program, install, Arch, Syscall, SockFilter}` (Task 4) is consumed by Task 5, and `Syscall` has four variants everywhere it is matched. `PROBE_TIMEOUT` (Task 3) is the probe's one deadline, shared by its read and its reap. Task 7 introduces the names a launcher phase must call rather than redeclare into the serving entry point — `Args` (`crates/tinio-cli/src/commands/start/args.rs`), `cli::parse` (`crates/tinio-cli/src/cli.rs`), `sandbox_request` (`crates/tinio-cli/src/sandbox/mod.rs`), `sandbox_options` (`crates/tinio-cli/src/sandbox/linux.rs`), `emit`, `emit_error` (`crates/tinio-cli/src/events.rs`) and `serve`'s signature (`crates/tinio-cli/src/commands/start/serve.rs`) — and they are each file's only definition of it: all three platforms' arms live in that one entry point, so a second `emit` beside this one is `E0428`. **`sandbox_exposures` is not among them** — Shared §4 forbids it and `sandbox_request` inlines the rule — and neither are the retired example's `parse_args`, private `RawArgs`, `refuse_command_line` and `emit_on_error`; the entry point has since moved out of the example entirely. Shared §4 owns that list. No name is introduced twice with different meanings, and nothing is defined here that belongs on P0's surface.

**What this plan deliberately does not do.** It adds no items to `tinio-sandbox-core`. It is tempting to put a Linux-flavoured convenience there while implementing — the crate is right there and the editor is open — but the whole reason this plan was split out of a larger one is that shared items accumulating in a platform phase is what produced three plans that disagreed about the same API. Anything shared goes to P0.

**One thing this plan changes outside the sandbox, and it is called out in its own task.** Task 8 rewrites `crates/tinio-e2e/tests/steps/clients.rs`, which is shipped test code with cucumber features behind it. It is not smuggled in as a side effect: the task says so, names the files, and requires the cucumber suite to pass as the proof the migration is complete.
