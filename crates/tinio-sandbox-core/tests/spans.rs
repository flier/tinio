//! The launcher's readiness and exit spans, asserted from a dedicated binary.
//!
//! **A separate target rather than a module of the lib test binary**: the recorder is the process
//! default, and the lib binary already installs one of its own (`launcher/adopt.rs`'s), so two
//! global installers would leave one of them silent. This binary has one test.

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;
use std::{ffi::OsString, io, process::ExitStatus, sync::mpsc, thread, time::Duration};

use tinio_sandbox_core::{Attribution, Child, Readiness, Terminal, adopt};
use tinio_util::testing::{metrics::counter, spans::recording};
use tracing::Level;

/// A zero exit status without spawning a process, through the platform's raw constructor.
fn success() -> ExitStatus {
    ExitStatus::from_raw(0)
}

#[test]
fn the_launcher_opens_its_readiness_and_exit_spans() {
    let (capture, _gate) = recording();
    let readiness_before = counter("tinio_sandbox_readiness_total", &[("result", "reported")]);
    let exit_before = counter("tinio_sandbox_child_exit_total", &[("outcome", "success")]);

    // The child's half: no launcher element, so this process is the launcher and nothing is read
    // from stdin. A payload's configuration and token must never reach a span field.
    let adopted = adopt(Vec::<OsString>::new(), io::empty()).expect("no payload");
    assert!(adopted.is_none());

    let addr = "127.0.0.1:9000".parse().expect("addr");
    let (tx, rx) = mpsc::channel();
    tx.send(Terminal::Listening(addr))
        .expect("send the announcement");
    let relay = thread::spawn(move || Some(Terminal::Listening(addr)));
    let mut child = Child::new(|| Ok(success()))
        .with_relay(relay)
        .with_ready(rx);

    let readiness = child.wait_for_ready(Duration::from_secs(1));
    assert!(matches!(readiness, Readiness::Reported(_)), "{readiness:?}");

    let status = child.wait().expect("the child is reaped");
    assert!(matches!(child.attribution(&status), Attribution::Ran));

    let adopted_span = capture.named("adopt");
    assert_eq!(adopted_span.len(), 1, "one span per adoption");
    assert_eq!(adopted_span[0].target, "tinio::sandbox");
    assert_eq!(adopted_span[0].level, Level::INFO);
    assert!(
        adopted_span[0].fields.is_empty(),
        "the payload's contents are never a field"
    );

    let readiness_span = capture.named("wait_for_ready");
    assert_eq!(readiness_span.len(), 1, "one span per readiness wait");
    assert_eq!(readiness_span[0].target, "tinio::sandbox");
    assert_eq!(readiness_span[0].level, Level::INFO);

    let waits = capture.named("wait");
    assert_eq!(waits.len(), 1, "one span per wait");
    assert_eq!(waits[0].target, "tinio::sandbox");
    assert_eq!(waits[0].level, Level::INFO);
    assert_eq!(waits[0].field("outcome"), Some("success"));

    let attributions = capture.named("attribution");
    assert_eq!(attributions.len(), 1, "one span per exit classification");
    assert_eq!(attributions[0].target, "tinio::sandbox");
    assert_eq!(attributions[0].level, Level::INFO);

    assert_eq!(
        counter("tinio_sandbox_readiness_total", &[("result", "reported")]) - readiness_before,
        1.0,
        "the readiness family is recorded where the wait answers"
    );
    assert_eq!(
        counter("tinio_sandbox_child_exit_total", &[("outcome", "success")]) - exit_before,
        1.0,
        "the exit family is recorded where the status arrives"
    );
}
