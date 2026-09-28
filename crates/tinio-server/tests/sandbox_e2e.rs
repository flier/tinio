//! The confined server, driven the way the interop harness drives it: spawn
//! `tinio server <root> --sandbox`, read the endpoint it reports, and talk to it.
//!
//! The server is confined **in place** on Linux (design decision 2), so these
//! legs prove the end of the chain no unit test can: a process that built a
//! jail around itself still binds a listener, still reports it, and still
//! answers on it from outside. The second leg drives the same binary under
//! `--read-only` and asks the confinement's own question — whether a write
//! through the served root is refused — through the child's private mount
//! namespace, which is the one place the answer exists.

#![cfg(target_os = "linux")]
#![cfg(feature = "sandbox")]

mod common;

use std::{
    fs,
    io::{self, Read as _},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use common::{answer, tinio_bin, wait_for_listening};
// **The precondition is `tinio-sandbox-linux`'s** — the same reader of
// `TINIO_SANDBOX_MODEL` the confined child uses, and the same `unshare(1)` probes
// the jail suite asks, so this leg and that suite cannot drift apart on what a
// host must grant. What stays here is the part that is this file's own: which
// failure each answer becomes.
use tinio_sandbox::linux::testing::{host_can_confine, requested_model};
use tinio_util::testing::{host_cannot, kill_and_reap};

/// Kill and reap the child, then fail with `message` and the stderr it left.
///
/// **Killing first is not tidiness.** A confined `tinio server` left alive holds its
/// listener, its jail and its state lock for the life of the test process — and
/// the leg that failed is the one whose child is least likely to exit on its
/// own. Reaping also ends the reader thread, whose pipe reaches EOF at the
/// child's death, so the stderr read below cannot block.
fn fail(child: &mut Child, message: &str) -> ! {
    kill_and_reap(child);
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    panic!("{message}; stderr:\n{stderr}");
}

/// The precondition every leg of this file asks before it spawns anything:
/// `false` when this host cannot confine under the model the run asked for and
/// the operator opted out through [`host_cannot`]'s variable, which is the only
/// case in which a leg returns unasserted.
///
/// **One function rather than one copy per leg**, because the skip rule is the
/// thing a second leg is most likely to get subtly wrong — a leg that skipped
/// without the opt-in would certify a server that was never jailed, which is
/// exactly what design §12 refuses. The order of the assertion and the print,
/// and the variable they name, are the shared device's.
fn confinement_available() -> bool {
    let model = requested_model();
    if let Err(tried) = host_can_confine(model) {
        host_cannot(
            &format!("a server confined under {model:?}"),
            &format!("the precondition `{tried}` failed"),
            ". WSL2 is where both legs are verified (design §12)",
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
    // **The CLI serves an existing `DIR` or refuses it**, where the retired example created the
    // root it was handed — so a leg that wants a serving server makes the directory first.
    fs::create_dir_all(&root).expect("the served root");

    let mut child = Command::new(tinio_bin())
        .args(["server", root.to_str().unwrap(), "--port", "0", "--sandbox"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn tinio server");

    // No precondition is left to blame here, so the wait's own failure is the
    // failure — with the child killed and its stderr quoted, because an `Error`
    // frame carries the confinement's rendering and the stderr behind it
    // carries the backend's own `error!` line for whatever refused.
    let addr = match wait_for_listening(&mut child) {
        Ok(addr) => addr,
        Err(message) => fail(&mut child, &message),
    };

    // A plain TCP connection proves the listener is reachable from outside the
    // jail. Anything more would re-test S3, which the other suites cover.
    // `addr` goes to `connect` as it is: the event's field is already a
    // `SocketAddr`, `SocketAddr` implements `ToSocketAddrs`, and that is the
    // point of the field being typed — there is no text in between that could
    // disagree with what was bound. (There is no `SocketAddr::as_str`.)
    // The probe itself is `common::answer`'s — the request bytes, the read and
    // the bound in one place — and its refusal goes through `fail`, which kills
    // the child before it reports, the rule the handshake above already follows.
    let response = match answer(addr) {
        Ok(response) => response,
        Err(message) => fail(&mut child, &message),
    };
    assert!(response.starts_with("HTTP/1.1 "), "{response}");

    child.kill().expect("kill");
    child.wait().expect("wait");
}

/// The read-only seam, end to end: `tinio server <root> --sandbox --read-only`.
///
/// **What this leg is for.** The phase's read-only assertions all ran against
/// `sandbox-child` (`crates/tinio-sandbox-linux/tests/jail.rs`), and its
/// `--read-only` assertions against an *unconfined* `tinio server`
/// (`serve_read_only.rs`) — so the line design §9 calls the seam, where the
/// serving path turns its **resolved** `access` into the exposure set, was never
/// exercised through the real entry point. The failure that hides there is the one the
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

    let mut child = Command::new(tinio_bin())
        .args([
            "server",
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
    let addr = match wait_for_listening(&mut child) {
        Ok(addr) => addr,
        Err(message) => fail(&mut child, &message),
    };
    let response = match answer(addr) {
        Ok(response) => response,
        Err(message) => fail(&mut child, &message),
    };
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
