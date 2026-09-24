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
