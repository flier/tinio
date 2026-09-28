//! The first question P3 asks: does a `(deny default)` profile leave enough for
//! a Rust process to start, size a multi-threaded runtime and read its data?
//!
//! The confined child is this same test binary, re-executed through the real
//! launcher — so the payload on stdin, the listener descriptor and the profile
//! are all exercised, not simulated. It is launched by a **launcher copy** of
//! this binary rather than by the test process, which is what puts this probe's
//! name in the child's argv: the harness module's docs have the whole argument.

#![cfg(target_os = "macos")]

mod harness;

use std::{fs, thread};

use harness::Report;
use tinio_sandbox_core::{AccessMode, Sandbox};

/// Runs inside the confinement and reports what a Rust process can still do.
#[test]
fn child_startup() {
    let Some(child) = harness::child() else {
        // Not the confined child: the test runner or the launcher copy, which
        // is the process this harness spawns to run `confine`. `launch` is a
        // no-op in the first and the whole of the launch in the second.
        harness::launch();
        return;
    };
    // `payload` is deliberately **not** pushed here: [`harness::finish`] writes
    // that discriminator into every report, so pushing it again would put one
    // key in the report twice and leave a reader unable to tell which line is
    // the handover's.
    let mut log = String::new();
    // **The error arm reports the `ErrorKind` token, not the `Display` string**,
    // and that is what makes the parent's assertion able to fail. `io::Error`'s
    // `Display` appends the errno — the confined child answers
    // `error(Operation not permitted (os error 1))` — so an assertion written
    // against `error(Operation not permitted)` was **always true** and this probe
    // passed while reporting a failure (measured on macOS 15.8, 2026-09-25). The
    // kind is the stable part: `ErrorKind::PermissionDenied` is one token on every
    // host and in every Rust release, and it is what the parent compares.
    log.push_str(&harness::line(
        "available_parallelism",
        match thread::available_parallelism() {
            Ok(n) => format!("ok:{}", n.get()),
            Err(err) => format!("error({:?})", err.kind()),
        },
    ));
    let threads = thread::scope(|scope| {
        let handles: Vec<_> = (0..4).map(|_| scope.spawn(|| 1u32)).collect();
        // `join().unwrap_or(0)`: a panicking scoped thread is 0 rather than a
        // second panic, and the identity `map` an earlier draft carried is what
        // `clippy::map_identity` refuses under `-D warnings`.
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or(0))
            .sum::<u32>()
    });
    log.push_str(&harness::line("threads", threads.to_string()));
    log.push_str(&harness::line(
        "read_marker",
        match fs::read_to_string(child.param("marker")) {
            Ok(body) => body.trim().to_string(),
            Err(err) => format!("error({})", err.kind()),
        },
    ));
    harness::finish(&child, &log);
}

/// The parent half: the process starts, threads and the runtime sizing work.
#[test]
fn a_confined_process_starts() {
    // The child is launched with this probe's name as its filter, so it runs
    // `child_startup` and nothing else; the guard below is what keeps the parent
    // half from running inside it.
    if harness::is_child_process() {
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("create root");
    let marker = root.join("marker.txt");
    fs::write(&marker, "confined\n").expect("seed");

    // **The fixture is the exposure set `serve` builds, and nothing else**: device
    // files are Linux's concept, so this platform's set carries no flag for them
    // and no rule is rendered for one (`profile.rs`'s module doc, and its
    // `no_device_literal_is_rendered_on_this_platform` leg — the literals this
    // profile used to carry were measured unnecessary on both paths and deleted).
    let sandbox = Sandbox::new().expose(&root, AccessMode::ReadWrite);
    let params = [("marker", marker.display().to_string())];

    // Two modes, one probe. Normally the child is launched through `confine`,
    // the product path (by a launcher copy, so its argv carries the filter
    // below); under `scripts/prune-profile.sh` the harness launches
    // `sandbox-exec` itself so that a single baseline rule can be dropped, and
    // that child has no payload — which is why the expectation below differs.
    //
    // **The pruned mode needs the variable to name a rule, and that is a
    // coverage rule rather than tidiness**: the arm used to read `is_some()`, so
    // an ambient set-but-empty `TINIO_SANDBOX_MACOS_PRUNE` took the pruned path,
    // dropped no rule (`remove_rule` answers the text unchanged for an empty id)
    // and passed with `payload=none` — the product handover silently
    // unexercised. `harness::pruned_rule` is the one definer of that value, and
    // it answers `None` for every value that names nothing, so the confined path
    // is the default unconditionally. A Cargo feature is not available to gate
    // this: the experiment's only caller is the script, whose single knob is
    // this variable's value.
    let pruning = harness::pruned_rule().is_some();
    let report: Report = if pruning {
        harness::run_pruned(&sandbox, &params, "child_startup")
    } else {
        harness::run_confined(&sandbox, &params, "child_startup")
    };

    report.expect("payload", if pruning { "none" } else { "ok" });
    report.expect("threads", "4");
    report.expect("read_marker", "confined");
    // **The parallel-value assertion is a comparison, not a tautology.** A report
    // of `missing` means nothing measured the runtime sizing at all; an error kind
    // other than `PermissionDenied` is a failure the confinement did not cause;
    // and `ok:0` is impossible (`available_parallelism` never answers zero), so it
    // is asserted against rather than assumed away. **Which of the two accepted
    // answers the child reports is not this leg's discriminator**, and it is wrong
    // to read `ok:<n>` as "the count is granted" or `error(PermissionDenied)` as
    // "the profile refuses it": the assertion's error arm admits *one* kind, so
    // both readings pass here — measured on the VM 2026-09-25, once under the
    // single-filter profile, which answered `available_parallelism=error(PermissionDenied)`
    // (the kernel logs `deny(1) sysctl-read hw.ncpu` beside the successful run),
    // and again under the shipped two-filter rule, which answers `ok:6`. What the
    // leg owns is that the child's CPU-count read is *answered*, by one of the two
    // legitimate outcomes; that the shipped rule grants the count is asserted where
    // the rule's text is, in `profile.rs`'s own test, and the abort that makes the
    // rule required at all still precedes `main` and so can never show up here.
    let parallelism = report.value("available_parallelism").unwrap_or("missing");
    assert_ne!(parallelism, "missing", "{}", report.text());
    assert_ne!(parallelism, "ok:0", "{}", report.text());
    assert!(
        !parallelism.starts_with("error(") || parallelism == "error(PermissionDenied)",
        "{}",
        report.text()
    );
}
