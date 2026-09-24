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
    env, fs,
    io::{self, Read, Write},
    net::TcpStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use common::{tinio_bin, wait_for_listening};
use tinio_sandbox::linux::Mechanism;

/// The one way past a missing precondition, and it is a deliberate opt-in.
///
/// **A suite that certifies a confinement does not get to skip in silence**: a
/// precondition that is absent fails the run by default, so a host that cannot
/// confine can never report a pass for a server that was never jailed. An
/// operator who knows this machine cannot confine sets this variable and means
/// it. The rule and the variable are the jail test's (`tinio-sandbox-linux`'s
/// `tests/jail.rs`), spelled the same way so one habit covers both.
const ALLOW_SKIP: &str = "TINIO_SANDBOX_ALLOW_SKIP";

/// The model the run asked for, read from the same variable `tinio server` reads,
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
/// produce as easily as an unprivileged host: anything that makes `tinio server`
/// die early while mentioning permissions reports a green suite. The question has a
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

/// Kill and reap the child, then fail with `message` and the stderr it left.
///
/// **Killing first is not tidiness.** A confined `tinio server` left alive holds its
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
