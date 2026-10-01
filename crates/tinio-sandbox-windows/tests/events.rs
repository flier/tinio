//! The stdout relay: core's `Observer` driving the child's stdout, dropping
//! everything that is not one of our frames.
//!
//! The launcher is a separate process here, because what is being asserted is
//! what lands on *its* stdout — a relay running inside the test process would
//! write into libtest's own capture, where nothing can be read back.

#![cfg(target_os = "windows")]

// **The harness is the library's now** (`tinio_sandbox_windows::testing`), and
// this binary keeps its own top-level `child_role` and `launcher_role` shims —
// the names the harness spawns here, because the lib linked into an integration
// target is compiled without `cfg(test)`
// (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`).
use tinio_sandbox_windows::testing as common;

use std::{
    env, fs,
    io::{self, Read},
    net::SocketAddr,
    path::Path,
    process::{Child, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use tinio_sandbox_core::Event;
use tinio_sandbox_windows::launch;
use tinio_util::testing::kill_and_reap;

/// The launcher's own role: run the production launch path and supervise it,
/// with its stdout owned by the test.
///
/// It publishes the bound address, which is this suite's difference from the
/// signals one: the test that spawned this process has no other way to learn the
/// port — the listener belongs to this process and the child's `Listening` event
/// is the protocol's, not this launcher's — and it needs it because the child
/// role ends by accepting a connection.
#[test]
fn launcher_role() {
    common::launcher_role(true);
}

/// Wait for a launcher role to publish the address it bound.
///
/// **Bounded, so that a launcher which never got that far fails the test rather
/// than hanging it.** The alternative — reading the launcher's stdout, which
/// returns only when the launcher exits, and the launcher exits only when its
/// child does — is a wait with no edge at all when the launch fails.
fn wait_for_address(tree: &Path) -> SocketAddr {
    let file = tree.join("address");
    let deadline = Instant::now() + common::TIMEOUT;
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(&file)
            && let Ok(address) = text.trim().parse()
        {
            return address;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "the launcher never published an address at {}",
        file.display()
    );
}

/// The child role: one stray line, one real event, then the usual probes.
#[test]
fn child_role() {
    let Some(adopted) = launch::adopt(env::args_os(), io::stdin().lock()).expect("adopt") else {
        return;
    };
    let listener = launch::listener(&adopted).expect("a listener");
    // Deliberate protocol violations, to prove the relay is a decoder and not a
    // `copy`: a bare line, and a frame of somebody else's.
    println!("this line is not a frame");
    println!("data: {{}}");
    // **The blank line is what makes the second violation a *frame* rather than a
    // prefix that swallows the first real one** (measured on this host,
    // 2026-09-21). The SSE decoder accumulates a `data:` field across lines and
    // dispatches only at a blank line, so a `data: {}` with no terminator merges
    // into the next event's data: the frame below then decodes as
    // `{}\n{"message":"probe"}`, `serde_json` refuses the trailing bytes, and the
    // relay drops the one frame this test asserts on — which is what this child
    // role does without this line, and what the plan's text as written cannot see.
    println!();
    print!(
        "{}",
        Event::Error {
            message: "probe".into()
        }
        .encode()
    );
    common::child_role_body(adopted.config(), listener);
}

/// Wait for the launcher's own output to close, or fail the test and kill it.
///
/// **The read is bounded on a thread of its own, because the launcher's stdout
/// closes only when the launcher exits.** A launcher still supervising a child
/// that never exited would leave a `read_to_string` on this thread reading until
/// the suite itself was killed, which is the shape of hang this phase's harness
/// rule exists to refuse. The thread is the bound: `recv_timeout` returns without
/// it, and the launcher is killed before the panic so a failing test does not
/// leave a process behind.
fn read_launcher_output(launcher: &mut Child) -> String {
    let mut stdout = launcher
        .stdout
        .take()
        .expect("the launcher's stdout was piped");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut text = String::new();
        let read = stdout.read_to_string(&mut text);
        let _ = sender.send(read.map(|_| text));
    });
    match receiver.recv_timeout(common::TIMEOUT) {
        Ok(Ok(text)) => text,
        Ok(Err(err)) => panic!("reading the launcher's stdout failed: {err}"),
        Err(_) => {
            kill_and_reap(launcher);
            panic!(
                "the launcher wrote nothing and did not exit within {:?}: its child never \
                 answered the handshake, or never exited",
                common::TIMEOUT
            );
        }
    }
}

/// Whether `line` is the launcher process's own libtest output rather than
/// something the relay put on this stream.
///
/// **Measured on this host, 2026-09-21, and it is not decoration**: libtest
/// writes its banner, its progress character and its summary to *stdout* — the
/// very handle the relay re-emits through — so a strict "every line is a frame"
/// assertion would fail for a reason that has nothing to do with the relay. The
/// three shapes are what `--quiet` prints; the launcher exits from inside its own
/// test, so in practice only the banner arrives, and the other two are listed
/// because whether libtest prints them before or after the body is libtest's
/// business and not this test's to depend on.
fn is_libtest_output(line: &str) -> bool {
    line.starts_with("running ") || line == "." || line.starts_with("test result:")
}

#[test]
fn the_parent_re_emits_frames_and_drops_everything_else() {
    let (_dir, tree) = common::tree(&common::one_volume());
    // **Piped, and `--quiet`, which is why the harness's spawn takes both**: this
    // is the one suite that reads the launcher's stdout, so it cannot inherit the
    // test's, and `--quiet` is what narrows the libtest banner `is_libtest_output`
    // has to filter. The `Command` this used to be spelled as had also lost the
    // harness's `CREATE_NEW_PROCESS_GROUP`, which is the flag a stop's address
    // depends on.
    let mut launcher = common::spawn_launcher(&tree.path, Stdio::piped(), true);

    // **The child has to be answered, or this test hangs instead of failing.**
    // It shares `child_role_body` with the confinement suite, so its last act is
    // `listener.accept()`: with nothing connecting, the child blocks, the
    // launcher waits on the child, and the read below — which returns only when
    // the launcher exits — never returns. So the address comes first, with a
    // bound, and the connection is made before a single byte of the relayed
    // stream is read.
    let address = wait_for_address(&tree.path);
    common::answer(address);

    let relayed = read_launcher_output(&mut launcher);
    // The launcher answered its child's handshake and the relay reached
    // end-of-stream, so what is left is a process on its way out — and the
    // harness's wait is what bounds that rather than blocking on it.
    let status = common::wait_for_exit(
        &mut launcher,
        common::TIMEOUT,
        "the launcher, after it answered its child's handshake",
    );
    assert_eq!(
        status.code(),
        Some(0),
        "the launcher exits with its child's status:\n{relayed}"
    );

    // The frame's real shape, which is what the relay writes on: `Event` puts the
    // name in its own field, so an `event:` line comes first and the JSON object
    // on the `data:` line is the variant's *payload*, not the variant — a test
    // asserting the serde-enum shape (`{"Error":{…}}`) would be asserting a
    // protocol nobody implements (P0's encoding is normative).
    assert!(
        relayed.contains("event: error\n"),
        "the child's event reached the operator:\n{relayed}"
    );
    assert!(
        relayed.contains(r#"data: {"message":"probe"}"#),
        "the child's event payload reached the operator whole:\n{relayed}"
    );
    // **Exactly one frame, and that is the count that makes the relay a decoder.**
    // The child printed one frame, so a forwarded byte stream would carry the
    // child's own libtest banner and its summary besides, both of which contain
    // neither marker — the count is what catches them where a `contains` cannot.
    assert_eq!(
        relayed.matches("event: ").count(),
        1,
        "the child printed one event, so one `event:` line reaches the operator:\n{relayed}"
    );
    assert_eq!(
        relayed.matches("data: ").count(),
        1,
        "the child printed one event, so one `data:` line reaches the operator:\n{relayed}"
    );
    assert!(
        !relayed.contains("this line is not a frame"),
        "a bare line the child wrote reached the operator, so the relay forwarded bytes:\n{relayed}"
    );
    assert!(
        !relayed.contains("data: {}"),
        "a foreign frame reached the operator:\n{relayed}"
    );
    // The strongest negative, because it is the child's *own* machinery rather
    // than a line this suite planted: libtest names the test it ran, the child's
    // name is `child_role` and the launcher's is `launcher_role`, so a relay that
    // forwarded bytes would put the child's banner and per-test line — the first
    // thing that child writes — on the operator's stdout.
    assert!(
        !relayed.contains("test child_role"),
        "the child's own libtest output reached the operator:\n{relayed}"
    );
    // Every line that reached stdout belongs to one of our frames, or to the
    // launcher's own libtest output — see `is_libtest_output` for why that second
    // case exists. Every real frame starts with `event: `, which is the shape a
    // `data: `-only assertion would have missed.
    for line in relayed.lines() {
        assert!(
            line.is_empty()
                || line.starts_with("event: ")
                || line.starts_with("data: ")
                || is_libtest_output(line),
            "a line that is neither a frame nor the launcher's own libtest output reached the \
             operator: {line:?}\n{relayed}"
        );
    }
}
