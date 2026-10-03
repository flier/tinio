//! End-to-end confinement. The jail is a one-way door, so the assertions run
//! here and the jail runs in `src/bin/sandbox-child.rs`.

#![cfg(target_os = "linux")]

use std::{
    env, fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Command,
};

// **The host precondition is the crate's** (`tinio_sandbox_linux::testing`), so
// this suite and `tinio-server/tests/sandbox_e2e.rs` ask the kernel the same way
// instead of keeping a copy each; what stays here is the part that is this
// suite's own — the child, the listener and the assertions.
use tinio_sandbox_linux::testing::{host_can_confine, requested_model};
use tinio_util::testing::host_cannot;

/// The environment every leg hands its child, in one place.
///
/// **`HOME` is set here rather than inherited**, so the child's
/// `host-home-visible` line has one answer on every run: the directory it probes
/// is the one the parent made for it, which exists on the host and is never
/// mirrored into the jail.
///
/// **Two legs spawn the child through different chains** — [`run_child`] execs it
/// directly and the opt-in leg below hands it to a wrapper's `exec` — so the
/// environment is set beside the chain rather than inside it, and a leg cannot
/// quietly probe a different `HOME` than its sibling.
///
/// **`extra` is the leg's own additions**, and it exists for the one hook that
/// is not a fact about the host: the forced `/proc/self/fd` fallback, which the
/// child resolves from its environment as it confines itself.
fn child_environment(
    command: &mut Command,
    root: &Path,
    state: Option<&Path>,
    home: &Path,
    access: &str,
    probe: SocketAddr,
    extra: &[(&str, &str)],
) {
    command
        .env("TINIO_SANDBOX_TEST_ROOT", root)
        .env("TINIO_SANDBOX_TEST_ACCESS", access)
        .env("TINIO_SANDBOX_TEST_PROBE", probe.to_string())
        .env("HOME", home);
    if let Some(state) = state {
        command.env("TINIO_SANDBOX_TEST_STATE", state);
    }
    for (key, value) in extra {
        command.env(key, value);
    }
}

/// Spawn the confined child and return its report.
///
/// **It fails rather than skips when this host cannot confine**, unless the
/// operator opted out through [`host_cannot`]'s variable — a skip printed by
/// libtest alone is swallowed without `--nocapture`, which is the silent green
/// run that rule exists to delete.
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
    run_child_with_env(root, state, home, access, probe, &[])
}

/// [`run_child`] with a leg's own environment added, for the leg whose subject
/// is a hook the child reads rather than a fact about the host. One body, so the
/// two entry points cannot drift into confining differently.
fn run_child_with_env(
    root: &Path,
    state: Option<&Path>,
    home: &Path,
    access: &str,
    probe: SocketAddr,
    extra: &[(&str, &str)],
) -> Option<String> {
    let model = requested_model();
    if let Err(tried) = host_can_confine(model) {
        host_cannot(
            &format!("a jail under {model:?}"),
            &format!("the precondition `{tried}` failed"),
            ". WSL2 is where both legs are verified (design §12)",
        );
        return None;
    }
    let mut command = Command::new(env!("CARGO_BIN_EXE_sandbox-child"));
    child_environment(&mut command, root, state, home, access, probe, extra);
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

/// The read-write legs' fixture: the served root with one object, a state
/// directory beside it, a scratch `HOME` the child probes, and the parent's
/// probe listener.
///
/// **The listener is returned rather than leaked**, and the caller binds it to
/// a name so it outlives the child ([`probe_listener`] carries the reason):
/// dropping it first would leave the child's `connect` nothing to reach, and
/// the assertion it feeds is the one that separates the filter's
/// `PermissionDenied` from `ConnectionRefused`.
fn jail_fixture(dir: &tempfile::TempDir) -> (PathBuf, PathBuf, SocketAddr, PathBuf, TcpListener) {
    let root = dir.path().join("root");
    let state = dir.path().join("state");
    let home = dir.path().join("home");
    fs::create_dir_all(root.join("bucket")).expect("create root");
    fs::create_dir_all(&state).expect("create state");
    fs::create_dir_all(&home).expect("create the home the child probes");
    fs::write(root.join("bucket/obj.txt"), "hello from the served root\n").expect("seed");

    let (listener, probe) = probe_listener();
    (root, home, probe, state, listener)
}

#[test]
fn the_jail_confines_the_process() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (root, home, probe, state, _listener) = jail_fixture(&tmp);

    let Some(log) = run_child(&root, Some(&state), &home, "read-write", probe) else {
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

/// The `/proc/self/fd` fallback forced, and the same invariants as the leg
/// above.
///
/// **The hook is the child's environment, not this process's**: the choice is
/// the confining process's own, so the variable is handed to the spawned child
/// through `child_environment`'s `extra`. **This is the only leg that reaches
/// the fallback branch** — measured
/// by making that branch answer `Unsupported`, which reddens this leg alone —
/// so what it proves is that the fallback confines identically, and that the
/// `open_tree` path the other legs take is not the only one that works.
#[test]
fn the_forced_proc_fallback_confines_identically() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (root, home, probe, state, _listener) = jail_fixture(&dir);

    let Some(log) = run_child_with_env(
        &root,
        Some(&state),
        &home,
        "read-write",
        probe,
        &[("TINIO_SANDBOX_FORCE_PROC_FALLBACK", "1")],
    ) else {
        return;
    };

    assert!(log.contains("read-ok: hello from the served root"), "{log}");
    assert!(log.contains("host-etc-visible: false"), "{log}");
    assert!(log.contains("old-root-visible: false"), "{log}");
    assert!(log.contains("host-home-visible: false"), "{log}");
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
    //
    // **Neither leg reaches the `EPERM` branch on this kernel, and this is the
    // place the follow-ups plan asked for that to be written down** (§7: "the
    // arm stays; the gap is documented where the test asserts 'one of the
    // two'"): design §4 records that the remount now re-asserts the source
    // mount's own locked flags, so the request is accepted where the partial one
    // was refused, and both legs of this run report `read-only-arm: remount`
    // (measured, `TINIO_SANDBOX_MODEL=userns` and `=root`). The `overlay`
    // alternative in the assertion above is therefore a mechanism this run does
    // not exercise end to end, and this test keeps accepting the arm so that it
    // stays covered rather than being deleted as dead.
    //
    // **§7's proposed way of closing it — a locked per-mount flag the request
    // does not name, `nosymfollow` on this kernel — was landed on 2026-09-28 as
    // the opt-in leg and measured false**
    // (`a_nosymfollow_source_is_served_read_only_under_a_nested_user_namespace`,
    // `TINIO_SANDBOX_TEST_NOSYMFOLLOW=1`; that leg's own doc carries the numbers):
    // the kernel locks `MNT_LOCK_ATIME` unconditionally on a namespace copy and
    // `MNT_LOCK_READONLY`/`NODEV`/`NOSUID`/`NOEXEC` for the flags a mount
    // carries, `can_change_locked_flags` tests exactly those five, and **no
    // `MNT_LOCK_NOSYMFOLLOW` exists at all** — while `MNT_NOSYMFOLLOW` sits
    // inside `MNT_USER_SETTABLE_MASK`, so a remount that does not name it
    // **clears** it instead of being refused. The nested-userns leg therefore
    // reaches `read-only-arm: remount` like the two default legs. What the
    // `EPERM` branch still needs is a locked flag, and every flag the kernel
    // locks is one `apply_mount` re-asserts or that a remount preserves — the
    // three of `source_mount_flags_in`'s four that have an `MNT_LOCK_*` behind
    // them (`nosuid`/`nodev`/`noexec`), `RDONLY`, and the atime bits the kernel
    // preserves on a remount that does not name them — so on this kernel the
    // branch has no end-to-end route through a flag a **correct** request fails
    // to name, which is not the same as being out of reach: it is **one
    // un-parsed locked flag away**, and `noexec` is that flag — deleting its
    // parse arm in the guest flips the opt-in leg to `read-only-arm: overlay`
    // (measured 2026-09-28) — while `nosymfollow` cannot be the trigger at all,
    // the kernel never locking it. That is this gap's measured state rather than
    // an assumption about the next host. **The set gained `nosymfollow` on
    // 2026-09-28 for the half of this that is a defect**: it is the member no
    // `EPERM` protects, so a request that does not name it clears it, and the
    // opt-in leg's `served-root-symlink` assertion is where that is pinned.
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

/// The opt-in switch for
/// [`a_nosymfollow_source_is_served_read_only_under_a_nested_user_namespace`].
///
/// **The leg is not part of the default gate, and the variable is not a skip
/// inside it.** Building its state needs a second `unshare(CLONE_NEWUSER)` nested
/// in a wrapper's own user namespace and a `mount -o nosymfollow` this host
/// accepts (`util-linux`'s `mount` translates the option into `MS_NOSYMFOLLOW`;
/// this kernel refuses the same name as a data string), so an unset variable
/// keeps the leg out of a run whose host never claimed either. A **set** variable
/// means the state must be built: the leg fails rather than skips, which is
/// `docs/tests.md`'s rule for confinement tests ("Confinement tests fail, never
/// skip") — the opt-in is the escape hatch for hosts that cannot do it, not a
/// skip on the way in.
const NOSYMFOLLOW: &str = "TINIO_SANDBOX_TEST_NOSYMFOLLOW";

/// What the wrapper runs **inside** user namespace A, before `exec`ing the child:
/// cover the served root with a `nosymfollow,noexec` tmpfs, seed it, and prove
/// both flags took.
///
/// **Each proof is the behaviour the flag governs, not a field parsed out of
/// `/proc/self/mountinfo`.** `nosymfollow` is a per-mount *flag* and this kernel
/// refuses the same name as a data string, so a `mount -t tmpfs -o nosymfollow` a
/// host silently downgraded would leave this leg asserting nothing; `test -e`
/// follows the link and is false exactly while the flag is on the mount that
/// carries it. `noexec` is proved the same way, by running the file it is
/// supposed to refuse — **inside the wrapper, where the interpreter that script
/// names does exist**, so the mount's flag is the only thing that can refuse it.
/// Either line the branch prints is asserted by the caller, so a host that cannot
/// is a failure rather than a quiet green run.
///
/// `exec` is the last line, so the child inherits A and its mount namespace —
/// and the child's own `unshare(CLONE_NEWUSER)` is what makes B. That nesting is
/// the whole construction: the copy of A's mount tree into B's mount namespace
/// belongs to a different user namespace, which is what makes `lock_mnt_tree`
/// lock the copy's per-mount flags.
const WRAPPER: &str = r#"set -eu
root="$TINIO_SANDBOX_TEST_ROOT"
mount -t tmpfs -o nosymfollow,noexec tmpfs "$root"
mkdir -p "$root/bucket"
printf 'hello from the nosymfollow mount\n' > "$root/bucket/obj.txt"
ln -s bucket/obj.txt "$root/alias"
if [ -e "$root/alias" ]; then
  echo 'nosymfollow-not-set'
  exit 1
fi
printf '#!/bin/sh\necho executable\n' > "$root/exec-probe"
chmod 0755 "$root/exec-probe"
if "$root/exec-probe" > /dev/null 2>&1; then
  echo 'noexec-not-set'
  exit 1
fi
exec "$TINIO_SANDBOX_TEST_CHILD"
"#;

/// Run the child **behind a wrapper's own user namespace**, on a served root the
/// wrapper covered with a `nosymfollow` tmpfs.
///
/// **The model is pinned rather than inherited.** This leg's subject is the
/// second `unshare(CLONE_NEWUSER)`, so a run started with
/// `TINIO_SANDBOX_MODEL=root` — the root leg of `scripts/wsl-chroot.sh` — must
/// not turn this leg into one with no nesting at all: the child would confine
/// with the root model inside A and never create B.
///
/// **`--propagation private`,** like the confinement's own `make / private`: the
/// mount belongs to the wrapper and must not reach the host's mount table.
fn run_child_under_a_nested_user_namespace(root: &Path, home: &Path, probe: SocketAddr) -> String {
    let mut command = Command::new("unshare");
    command
        .args([
            "--user",
            "--map-root-user",
            "--mount",
            "--propagation",
            "private",
            "sh",
            "-c",
            WRAPPER,
        ])
        // The child is the same binary every other leg runs, reached through the
        // wrapper's `exec` instead of a direct spawn.
        .env(
            "TINIO_SANDBOX_TEST_CHILD",
            env!("CARGO_BIN_EXE_sandbox-child"),
        )
        .env("TINIO_SANDBOX_MODEL", "userns");
    child_environment(&mut command, root, None, home, "read-only", probe, &[]);
    let output = command.output().expect("spawn the wrapper");
    let log = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !log.contains("bad-model"),
        "the child refused TINIO_SANDBOX_MODEL:\n{log}"
    );
    assert!(
        !log.contains("confine-failed"),
        "the host can nest a user namespace but the child did not confine under it:\n{log}\n\
         child stderr:\n{stderr}"
    );
    // **Fail, never skip.** Every way this host can refuse the construction — a
    // forbidden unprivileged user namespace, a `mount` that will not take the
    // flag, no `sh` — ends without the child reporting its arm, and that is this
    // leg's failure when the operator asked for it: the wrapper's own message is
    // on `stderr` and its `nosymfollow-not-set` line, if it got that far, is in
    // `stdout` beside this report.
    assert!(
        log.contains("read-only-arm: "),
        "the wrapper never reached the child's report, so the nested user namespace \
         or the `nosymfollow` mount was refused:\nstdout:\n{log}\nstderr:\n{stderr}"
    );
    log
}

/// The read-only arm **under a nested user namespace**, on a served root whose
/// mount carries `nosymfollow` — the state the follow-ups plan (§7) proposed for
/// reaching `enforce_bind_read_only`'s `EPERM` branch end to end.
///
/// **What the plan proposed, and what this leg measured instead.** The proposal
/// was that `nosymfollow` is a locked per-mount flag the request does not name,
/// so the remount would be refused with `EPERM` and the `MS_RDONLY` overlay would
/// be the arm that ran. On this kernel it is not, and the reason is in the
/// kernel rather than in this suite: `lock_mnt_tree` sets `MNT_LOCK_ATIME`
/// unconditionally and then `MNT_LOCK_READONLY`/`NODEV`/`NOSUID`/`NOEXEC` for the
/// flags a mount carries (`fs/namespace.c`; `can_change_locked_flags` tests
/// exactly those five), while **`MNT_LOCK_NOSYMFOLLOW` does not exist at all** —
/// and because `MNT_NOSYMFOLLOW` is inside `MNT_USER_SETTABLE_MASK`, a
/// successful bind remount that does not name it does not preserve it either:
/// the flag is **silently cleared**, not defended. Measured on the project's
/// kernel (`6.18.33.2-microsoft-standard-WSL2`) with a bare `mount(2)` from a
/// nested user namespace: a bind of a `rw,relatime,nosymfollow` tmpfs, remounted
/// `MS_REMOUNT|MS_BIND|MS_RDONLY` — this crate's request for that source, which
/// carries none of `source_mount_flags_in`'s four flags — returns `0` and reads
/// back `ro,relatime`, with the flag gone.
///
/// **The same construction then found a second defect in the same flag, and this
/// leg is its end-to-end half.** `source_mount_flags_in` named only
/// `nosuid`/`nodev`/`noexec`, so a served root on a `nosymfollow` mount was
/// served **without** that protection — cleared by the remount with no `EPERM`
/// to report, and never carried by the overlay arm's fresh mount either. The set
/// carries the flag now when the source does (the overlay included, so both arms
/// serve one root the same way), and the child's read of the symlink the wrapper
/// planted is the assertion: `ELOOP` while the flag survives, a successful read
/// without it — which is what the fix-reverted run in the guest measured.
///
/// **The same leg carries the `noexec` half, which is the same asymmetry seen
/// from the other side.** `noexec` *is* a flag the kernel locks, so the remount
/// arm has re-asserted the source's spelling since the 2026-09-24 fix — while
/// the overlay arm's fresh mount carried only `nosuid`/`nodev`, so one served
/// root on a `noexec` mount was exec-capable under the fallback and not under the
/// remount. `overlay_read_only_flags` now follows the source for `noexec` as it
/// does for `nosymfollow`, and **that rule is pinned by its own unit test rather
/// than here**: this leg reports `read-only-arm: remount` on this kernel (the
/// paragraph above), so the overlay's flag set is never reached end to end. What
/// this leg pins is the source-to-jail direction on the arm that does run — the
/// wrapper mounts its tmpfs `noexec` and proves the flag there by running the
/// file itself, and the child then tries to execute that same file through the
/// jail, where the refusal has to be the **mount's** and not the missing
/// interpreter's (`PermissionDenied` while the flag survives, the `NotFound` a
/// run without it produces).
///
/// **So this leg does not assert the overlay arm, and asserting it would be a red
/// test on every host rather than a covered branch.** What it does assert is the
/// construction itself — the child confines end to end inside a user namespace
/// nested in a wrapper's, serves the `nosymfollow,noexec` root read-only, cannot
/// write it, does not follow a symlink the host's mount would not, cannot
/// execute a file the host's mount would not, and reports which arm ran, which
/// is what the caller reads out of the run.
/// [`a_read_only_jail_keeps_the_served_root_unwritable`]'s `read-only-arm`
/// assertion carries the same gap in prose, and the two are the record that the
/// `EPERM` branch's end-to-end coverage is still owed — **and the debt is
/// narrower than "no locked flag can refuse this request"**: what has no route
/// on this kernel is a flag a **correct** request fails to name, because the
/// kernel locks exactly what the request re-asserts (the three of
/// `source_mount_flags_in`'s four that have an `MNT_LOCK_*` behind them,
/// `nosymfollow` having none) and preserves the atime bits a remount does not
/// name. **`nosymfollow` cannot be the trigger and `noexec` can**, which is
/// measured: deleting `noexec` from the parse arm in the guest flips this leg to
/// `read-only-arm: overlay`, its child's exec probe answering `NotFound` where
/// the shipped code answers `PermissionDenied`. The arm is therefore one
/// un-parsed locked flag away rather than out of reach, and what closes the
/// route is the request naming every flag the kernel *can* lock — not the
/// kernel's locked set.
#[test]
fn a_nosymfollow_source_is_served_read_only_under_a_nested_user_namespace() {
    // Set-and-non-empty rather than a literal `=1`, which is the habit
    // `TINIO_SANDBOX_ALLOW_SKIP` set; `=1` is what the plan and this doc write.
    if !env::var(NOSYMFOLLOW).is_ok_and(|value| !value.is_empty()) {
        eprintln!(
            "skipping: {NOSYMFOLLOW} is not set, so the nested-user-namespace leg did \
             not run (set it to 1 on a host that can run `unshare --user` and `mount -o \
             nosymfollow`)"
        );
        return;
    }

    let tmp = tempfile::tempdir().expect("tempdir");
    // **The directory exists on the host and stays empty there.** The wrapper
    // mounts its tmpfs over it inside the wrapper's own mount namespace, so the
    // seed the child reads is proof the mount happened and the emptiness of the
    // host's copy is proof it did not leak.
    let root = tmp.path().join("root");
    let home = tmp.path().join("home");
    fs::create_dir_all(&root).expect("create the mount point the wrapper covers");
    fs::create_dir_all(&home).expect("create the home the child probes");

    let (_listener, probe) = probe_listener();

    let log = run_child_under_a_nested_user_namespace(&root, &home, probe);

    assert!(
        !log.contains("nosymfollow-not-set"),
        "the wrapper's tmpfs did not carry the flag, so this leg asserts nothing about \
         a `nosymfollow` source: {log}"
    );
    assert!(
        log.contains("read-ok: hello from the nosymfollow mount"),
        "{log}"
    );
    assert!(log.contains("served-root-write: refused"), "{log}");
    assert!(log.contains("old-root-visible: false"), "{log}");
    assert!(
        log.contains("outbound-connect: refused (PermissionDenied"),
        "{log}"
    );
    // **The assertion the 2026-09-28 fix exists for, and the one that fails
    // without it.** The wrapper proved the source mount carries `nosymfollow`
    // (the symlink it made is not followed *there*); this proves the *jail* still
    // carries it, which it does only because the request names the flag: a bind
    // remount that omits it clears it — silently, with no `EPERM` to report,
    // because the kernel has no `MNT_LOCK_NOSYMFOLLOW` — and the child's read of
    // that symlink comes back `Ok` instead of `ELOOP`. Arm-independent by
    // construction: the overlay is a fresh mount, and it names the flag when the
    // source does so that both arms of `AccessMode::ReadOnly` serve one root the
    // same way.
    assert!(
        log.contains("served-root-symlink: refused (FilesystemLoop)"),
        "the served root followed a symlink the host's own mount refuses to follow, \
         so the remount did not re-assert `nosymfollow`:\n{log}"
    );
    assert!(
        !log.contains("noexec-not-set"),
        "the wrapper's tmpfs did not carry `noexec`, so the probe below asserts nothing \
         about a `noexec` source: {log}"
    );
    // **The `noexec` half of the same rule, and the kind is the assertion.** The
    // wrapper proved the source mount refuses to execute that file *there*, where
    // its interpreter exists; this proves the jail does not execute it either.
    // `PermissionDenied` is the mount's `EACCES`; a run whose arm dropped the
    // flag gets past that check and dies on the interpreter the jail does not
    // carry, which is `NotFound` — so an assertion on "the exec failed" alone
    // would pass in both states. Which arm carries it here is the arm that ran
    // (the remount re-asserts the source's spelling); the overlay's half of the
    // rule is `enter.rs`'s `the_overlay_carries_the_sources_noexec_and_nosymfollow`,
    // this leg being unable to reach that arm on this kernel (the doc above).
    assert!(
        log.contains("served-root-exec: refused (PermissionDenied)"),
        "the served root executed a file the host's own mount refuses to execute, \
         so the arm that ran did not carry the source's `noexec`:\n{log}"
    );
    // **Which arm ran, one of the two, for the reason the sibling leg states at
    // length** — and the doc above records what this host's answer is and why the
    // overlay arm is not the one this construction can be made to reach.
    assert!(
        log.contains("read-only-arm: remount") || log.contains("read-only-arm: overlay"),
        "{log}"
    );
    println!(
        "{}",
        log.lines()
            .find(|line| line.starts_with("read-only-arm: "))
            .unwrap_or("read-only-arm: ABSENT")
    );
    // The served root is untouched on the host: the tmpfs the wrapper seeded is
    // inside its own mount namespace, and the host's directory is still empty.
    assert!(
        !root.join("bucket").exists(),
        "the wrapper's tmpfs reached the host's mount table"
    );
}
