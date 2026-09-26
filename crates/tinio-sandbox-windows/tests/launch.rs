//! The AppContainer launch: a Rust binary runs inside it, the whitelist holds,
//! and a program that does not exist fails with the path in the message.
//!
//! **The child role and the probe file live in the harness** (`tests/common`)
//! since Task 9: the confinement, events, network and signal suites launch the
//! same role from their own binaries, and this file is the one that drives the
//! launch machinery by hand rather than through [`Backend::confine`].

#![cfg(windows)]

mod common;

use std::{env, ffi::OsString, net::TcpListener, path::Path, ptr};

use tinio_sandbox_core::{AccessMode, LaunchPayload};
use tinio_sandbox_windows::{
    acl::{self, Target},
    handles, launch,
    process::{self, Container, LaunchSpec},
    sid::CapabilitySids,
    socket,
};

/// The confined child. The body lives in the harness because every suite that
/// launches a child runs the same role, and this test binary is one of them.
#[test]
fn child_role() {
    common::child_role();
}

#[test]
fn a_rust_binary_runs_inside_the_container_and_the_whitelist_holds() {
    // The harness's tree, seeds and `Users` ACE, because they are the same ones
    // this test used to spell out: `denied` carries `Users` and no package-SID
    // ACE, which is the verified row this test reproduces.
    let (_dir, tree) = common::tree(&common::one_volume());
    let probe = tree.probe();

    // **This is where the launch-succeeds assertion lives.** Two failures reach
    // it with the same symptom — `CreateProcessW` refusing a program path that
    // plainly exists — and this is the order to check them in: the ACE on the
    // launcher's own directory first (the confined child's own reads are checked
    // against its package SID, and the binary's siblings are what it may need to
    // load), then the profile behind the SID, because a merely derived SID fails
    // `err=2 ERROR_FILE_NOT_FOUND` on a machine where this profile never existed
    // — which is every fresh machine — and it does so identically for this binary
    // and for `cmd.exe`, with or without the ACEs already granted.
    //
    // **The first of those two is not what made this test fail, and the run that
    // settled that is recorded here** (2026-09-21): with the launcher's directory
    // ungranted — `acl::has_ace(dir)` and `acl::has_ace(exe)` both `false` — the
    // confined child still launched and ran this test's assertions, because
    // `CreateProcessW` opens the image with the *launcher's* access rather than
    // the child's. What the grant buys is what the child opens for itself, which
    // is why decision 22 keeps it; what it does not buy is the launch.
    let sid = common::sid_for(&tree);
    // The launcher's own directory, read-only: Task 3's `exposures()` adds
    // exactly this path, first of all, and this test grants by hand because it
    // drives `process::launch` itself rather than going through `Backend::confine`,
    // which is what would have added that exposure for it.
    let program = env::current_exe().expect("current exe");
    let launcher_dir = program
        .parent()
        .expect("the launcher's directory")
        .to_path_buf();
    acl::grant(&tree.root, Target::Directory, AccessMode::ReadOnly, &sid).expect("grant the root");
    acl::grant(&launcher_dir, Target::Directory, AccessMode::ReadOnly, &sid)
        .expect("grant the launcher's own directory");
    acl::grant(&tree.report, Target::Directory, AccessMode::ReadWrite, &sid)
        .expect("grant the report dir");

    // The child's stdin carries the payload and its stdout carries the event
    // stream (decisions 17, 19); its listener is handed over by the argv element.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("the listener's address");
    socket::mark_inheritable(&listener).expect("mark the listener inheritable");
    let payload_pipe = handles::pipe().expect("the child's stdin");
    let event_pipe = handles::pipe().expect("the event pipe");
    // `command_args` puts the listener element first and the launcher's own
    // arguments after it (Task 7 fixes the rule, and Task 9's `confine` states
    // the same one from the launcher's end), so the child's argv is `<program>
    // tinio-sandbox-fd=<handle> child_role --exact`. libtest meets the element
    // exactly as a test runner meets any unrecognised argument — a second
    // positional filter, which with `--exact` matches no test — and `child_role`
    // is the one test that runs (measured, by running this binary with the
    // child's own argv).
    let args = launch::command_args(
        socket::handle(&listener),
        &[],
        &[OsString::from("child_role"), OsString::from("--exact")],
    );
    let frame = serde_json::to_vec(&LaunchPayload {
        config: probe.to_string_lossy().into_owned(),
        token: None,
        confinement: None,
    })
    .expect("the payload serializes");
    handles::write_payload(payload_pipe.write, &frame)
        .expect("write the frame into the child's stdin");
    let stderr = handles::inherited_stderr().expect("the launcher's stderr");

    let mut launched = process::launch(
        &LaunchSpec {
            program: &program,
            args: &args,
            stdin: payload_pipe.read,
            stdout: event_pipe.write,
            stderr,
            inherited: &[socket::handle(&listener)],
        },
        &Container {
            package_sid: sid.raw(),
            capabilities: CapabilitySids::resolve(&[]).expect("no capabilities"),
        },
    )
    .expect("launch inside the container");
    // The child's own ends are `CreateProcessW`'s to duplicate into the child,
    // and leaving this process's copies open is a hang rather than a leak: with
    // the event pipe's write end still open here, the child's stdout never
    // reaches end-of-stream (`handles::parent_end`'s rule, and what the launch
    // site of Task 9 does at this same point).
    handles::close(payload_pipe.read);
    handles::close(event_pipe.write);
    // **The child's last act is the `accept()` on the listener it inherited, so
    // answering it is what lets this test wait for the child at all**: a test
    // that only waited would be waiting for a process that is waiting for the
    // test. The connect and the read are bounded, like every other wait in this
    // suite (Task 6's rule), so a child that died before its handshake fails here
    // with that reason rather than hanging the harness.
    common::answer(address);
    let status = launched.wait().expect("wait");

    let report = tree.report_text();
    acl::revoke(&tree.root, &sid).expect("roll back");
    acl::revoke(&launcher_dir, &sid).expect("roll back");
    acl::revoke(&tree.report, &sid).expect("roll back");

    assert_eq!(
        status.code(),
        Some(0),
        "the confined child exited non-zero: {report}"
    );
    assert!(report.contains("granted-read: OK"), "{report}");
    // The harness renders the failure with `ErrorKind`'s `Display`, which is the
    // lowercase description: a read that the package SID does not hold is a
    // permission denial and nothing else.
    assert!(
        report.contains("denied-read: refused (permission denied)"),
        "{report}"
    );
}

#[test]
fn a_bad_program_path_is_an_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("temp dir");
    // The harness's derivation, over a directory that exists to make the package
    // name this run's own and nothing else: `Tree::at` is lexical, so no tree is
    // created under it.
    let sid = common::sid_for(&common::Tree::at(dir.path()));
    let err = process::launch(
        &LaunchSpec {
            program: Path::new(r"E:\does-not-exist\tinio-serve.exe"),
            args: &[],
            // The standard handles are never reached — `CreateProcessW` fails on
            // the program path first — and a null handle is the honest value for
            // a launch that never got that far.
            stdin: ptr::null_mut(),
            stdout: ptr::null_mut(),
            stderr: ptr::null_mut(),
            inherited: &[],
        },
        &Container {
            package_sid: sid.raw(),
            capabilities: CapabilitySids::resolve(&[]).expect("no capabilities"),
        },
    )
    .expect_err("a missing program must fail");
    let rendered = err.to_string();
    assert!(rendered.contains("CreateProcessW"), "{rendered}");
    assert!(rendered.contains("does-not-exist"), "{rendered}");
}

/// The default capability set — none at all — gets a test of its own, because it
/// is the configuration design §7's outbound guarantee rests on and because its
/// value in the attribute list is easy to get wrong in a way nothing else
/// catches: an empty `CapabilitySids` whose `as_ptr()` is passed as it stands is
/// a **dangling non-null** pointer, and `CreateProcessW` refuses the whole
/// launch with `err=87` (`ERROR_INVALID_PARAMETER`) rather than reporting
/// anything about capabilities (measured 2026-09-21; null is the value for no
/// capabilities). The child here is this test binary with a filter that matches
/// no test, so "the launch succeeded" is the entire assertion —
/// `a_rust_binary_runs_inside_the_container_and_the_whitelist_holds` uses the
/// empty set too, but its subject is the whitelist rather than the default.
#[test]
fn the_empty_capability_set_launches() {
    let dir = tempfile::tempdir().expect("temp dir");
    // As above: a name source, through the harness's derivation.
    let sid = common::sid_for(&common::Tree::at(dir.path()));
    let program = env::current_exe().expect("current exe");
    // The production exposure set's first entry, so this launches the way the
    // launcher does (decision 22, Task 3's `exposures()`).
    let launcher_dir = program
        .parent()
        .expect("the launcher's directory")
        .to_path_buf();
    acl::grant(&launcher_dir, Target::Directory, AccessMode::ReadOnly, &sid)
        .expect("grant the launcher's directory");
    let stdin = handles::pipe().expect("the child's stdin");
    let stdout = handles::pipe().expect("the child's stdout");
    let stderr = handles::inherited_stderr().expect("the launcher's stderr");

    let mut launched = process::launch(
        &LaunchSpec {
            program: &program,
            // No listener element, so `adopt` reads nothing in the child, and
            // libtest's filter matches no test: it exits 0 having run none.
            args: &[
                OsString::from("--exact"),
                OsString::from("no_such_test_runs_here"),
            ],
            stdin: stdin.read,
            stdout: stdout.write,
            stderr,
            inherited: &[],
        },
        &Container {
            package_sid: sid.raw(),
            capabilities: CapabilitySids::resolve(&[]).expect("the default: no capabilities"),
        },
    )
    .expect("the default capability set launches");
    handles::close(stdin.read);
    handles::close(stdout.write);
    let status = launched.wait().expect("wait");
    acl::revoke(&launcher_dir, &sid).expect("roll back");

    assert_eq!(
        status.code(),
        Some(0),
        "the confined child did not exit cleanly with no capability"
    );
}
