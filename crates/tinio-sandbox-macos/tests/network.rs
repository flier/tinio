//! Design §7's uniform rule on macOS: the confined process cannot initiate an
//! outbound connection, and the profile never grants one. Inbound goes through
//! the listener the parent bound (decision 16) — and whether `accept` needs a
//! rule of its own is what this file settles.

#![cfg(target_os = "macos")]

mod harness;

use std::{
    fs,
    io::{BufRead, BufReader, Write},
};

use tinio_sandbox_core::{AccessMode, Sandbox};
use tinio_sandbox_macos::profile::{network_operation, profile};

/// Runs inside the confinement: accept on the inherited listener, and try to
/// reach out.
#[test]
fn child_network() {
    if !harness::is_child_process() {
        harness::launch();
        return;
    }
    let Some(child) = harness::child() else {
        return;
    };
    let mut log = String::new();

    // The listener arrived in the payload (design §3 step 7). Accepting on it is
    // the whole of the child's **data-plane** inbound surface — no bind, and so
    // no `network-bind` rule for serving (decision 16). The profile carries no
    // network rule of any kind, including for the *control* channel: the launcher
    // binds that unix socket too, so this probe's own process would be refused a
    // `bind()` exactly as the data plane's is.
    match child.listener() {
        Ok(listener) => match listener.accept() {
            Ok((mut stream, _)) => {
                let mut line = String::new();
                let read = BufReader::new(stream.try_clone().expect("clone")).read_line(&mut line);
                let _ = stream.write_all(b"pong-from-inside-the-profile\n");
                let _ = stream.flush();
                log.push_str(&harness::line(
                    "accept",
                    match read {
                        Ok(_) => format!("ok:{}", line.trim()),
                        Err(err) => format!("error({})", err.kind()),
                    },
                ));
            }
            Err(err) => log.push_str(&harness::line("accept", format!("error({})", err.kind()))),
        },
        Err(err) => log.push_str(&harness::line("accept", format!("no-listener({err})"))),
    }

    // Outbound, through the shared probe: the parent proved this exact address is
    // reachable from outside the confinement before the child existed
    // (`harness::reachable_target`), so a failure here is the confinement and not
    // a machine with no route (design §12).
    log.push_str(&harness::probe_outbound(&child));
    harness::finish(&child, &log);
}

#[test]
fn the_confined_process_cannot_reach_out() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_network` and nothing else.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("create root");

    // The baseline first: the target is reachable from *outside* the confinement,
    // so the assertion below cannot pass vacuously on a machine with no route or no
    // network (design §12). The listener it hands back stays alive for the whole
    // test, which is what the `_target` binding is for.
    let (_target, addr) = harness::reachable_target();

    let sandbox = Sandbox::new().expose(&root, AccessMode::ReadWrite);
    let report =
        harness::run_confined(&sandbox, &[("outbound", addr.to_string())], "child_network");

    report.expect("payload", "ok");
    // The harness's own connection to the inherited listener is what the
    // child's `accept` probe reads, so this line proves inbound works *and*
    // that the descriptor handover of design §3 step 7 reached the child.
    report.expect("accept", "ok:ping");
    assert_eq!(
        report.value("outbound"),
        Some("refused(permission denied)"),
        "{}",
        report.text()
    );
}

#[test]
fn the_profile_grants_no_network_operation() {
    // Design §7's macOS row is not "no outbound" — serving needs no network
    // capability of any kind: a profile carrying zero network rules accepted a
    // real connection from a listener the parent had handed in, and the kernel
    // logged no `accept` violation. The property is asserted through
    // `network_operation`, which owns the keyword table and the measurements
    // behind it — `profile.rs`'s unit leg reads the same one, and the two used to
    // carry the table twice.
    let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadWrite);
    let rendered = profile(&sandbox, tinio_sandbox_macos::Mechanism::SandboxExec).expect("profile");
    assert_eq!(
        network_operation(&rendered.text),
        None,
        "no rule in this profile may mention a network operation: {}",
        rendered.text
    );
}
