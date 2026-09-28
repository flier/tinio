//! End-to-end confinement. The jail is a one-way door, so the assertions run
//! here and the jail runs in `src/bin/sandbox-child.rs`.

#![cfg(target_os = "linux")]

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::Path,
    process::Command,
};

// **The host precondition is the crate's** (`tinio_sandbox_linux::testing`), so
// this suite and `tinio-server/tests/sandbox_e2e.rs` ask the kernel the same way
// instead of keeping a copy each; what stays here is the part that is this
// suite's own — the child, the listener and the assertions.
use tinio_sandbox_linux::testing::{host_can_confine, requested_model};
use tinio_util::testing::host_cannot;

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
    //
    // **Neither leg reaches the `EPERM` branch on this kernel, and this is the
    // place the follow-ups plan asked for that to be written down** (§7: "the
    // arm stays; the gap is documented where the test asserts 'one of the
    // two'"): design §4 records that the remount now re-asserts the source
    // mount's own locked flags, so the request is accepted where the partial one
    // was refused, and both legs of this run report `read-only-arm: remount`
    // (measured, `TINIO_SANDBOX_MODEL=userns` and `=root`). The `overlay`
    // alternative in the assertion above is therefore a mechanism this run does
    // not exercise end to end; reaching the `EPERM` branch needs a locked
    // per-mount flag the request does not name, which is §7's own remaining
    // work, and this test keeps accepting the arm so that it stays covered when
    // that work lands rather than being deleted as dead.
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
