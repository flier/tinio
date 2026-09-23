//! The launch payload: a parent writes a frame to the child's stdin and hands
//! the listener over by naming its handle in argv.
//!
//! Both halves of that exchange are here, because neither is observable without
//! the other: the child role is a process that adopts the frame and rebuilds the
//! socket the element names, and the launcher side is the only place that can
//! see what the child made of it.

#![cfg(windows)]

mod common;

use std::{
    env,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read, Write},
    net::TcpListener,
    process::{Command, Stdio},
};

use tinio_sandbox_core::{
    Adopted, LaunchPayload, is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg,
};
use tinio_sandbox_windows::{launch, socket};
use windows_sys::Win32::Foundation::HANDLE;

/// The marker that makes [`child_role`] inert in this binary's own test run.
///
/// The spawned child runs `child_role --exact` with this variable set; in the
/// launcher's run of the same binary the same function is an ordinary test that
/// must return rather than adopt — adopting from a console this process does not
/// own is a hang, not a failure. The payload cannot carry the marker: adopting
/// the payload is what the role is *for*.
const CHILD: &str = "TINIO_SANDBOX_TEST_PAYLOAD_CHILD";

/// Where the child role records what it saw, for the parent to assert on.
const REPORT: &str = "TINIO_SANDBOX_TEST_REPORT";

#[test]
fn child_role() {
    if env::var_os(CHILD).is_none() {
        return;
    }
    let adopted = launch::adopt(env::args_os(), io::stdin().lock())
        .expect("adopt")
        .expect("a payload arrived on stdin");
    assert_eq!(adopted.config(), "version = 1\n");
    // The element named the listener's handle and `adopt` handed back the
    // descriptor; rebuilding the socket is this crate's half.
    assert!(
        adopted.listener_fd.is_some(),
        "the element named a listener"
    );
    let listener = launch::listener(&adopted).expect("rebuild the listener");
    assert!(listener.local_addr().expect("a bound socket").port() > 0);
    let report = env::var(REPORT).expect("report path");
    fs::write(report, "payload-ok\n").expect("write the report");
}

#[test]
fn a_child_reads_the_frame_the_parent_wrote() {
    if env::var_os(CHILD).is_some() {
        return;
    }
    let dir = tempfile::tempdir().expect("temp dir");
    let report = dir.path().join("report.txt");
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    socket::mark_inheritable(&listener).expect("mark the listener inheritable");
    let element = listener_fd_arg(socket::value(&listener) as i64);

    let mut child = Command::new(env::current_exe().expect("current exe"))
        .args(["child_role", "--exact", &element.to_string_lossy()])
        .env(CHILD, "1")
        .env(REPORT, &report)
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn the child");

    let frame = serde_json::to_vec(&LaunchPayload {
        config: "version = 1\n".into(),
    })
    .expect("the payload serializes");
    child
        .stdin
        .take()
        .expect("the child's stdin")
        .write_all(&frame)
        .expect("write the frame");
    // Dropping the handle closes the pipe, which is what makes the child's read
    // of stdin reach EOF (design §3 step 7).

    // The wait is the harness's, and it is the shape the phase's rule asks for:
    // the child here is a whole test binary, so "it exited" and "it is stuck
    // reading a stdin nobody closed" are the two outcomes the wait has to tell
    // apart — and a red one is killed and reaped before it panics.
    let status = common::wait_for_exit(&mut child, common::TIMEOUT, "the payload child");
    assert!(status.success(), "the child role failed: {status:?}");
    assert_eq!(
        fs::read_to_string(&report).expect("the child's report"),
        "payload-ok\n"
    );
}

#[test]
fn a_process_with_no_element_is_not_a_confined_child() {
    // In the child role this process's argv carries the element *by
    // construction* — that is what makes it the child — so the rule this test
    // states is not the rule that process is under.
    if env::var_os(CHILD).is_some() {
        return;
    }
    // The discriminator: no element means no launcher expected this process to be
    // confined, and stdin is never read — which is what keeps an ordinary
    // launcher from blocking on its own console.
    let adopted = launch::adopt(env::args_os(), Untouchable).unwrap();
    assert!(adopted.is_none());
    // `OsString`s and not `String`s: the rule takes a process's own arguments,
    // and a path that is not valid UTF-8 is a legal argument on Windows.
    let adopted = launch::adopt(
        [OsString::from("serve"), OsString::from("/srv/data")],
        Untouchable,
    )
    .unwrap();
    assert!(adopted.is_none());
}

#[test]
fn the_element_carries_a_listener_descriptor() {
    let element = listener_fd_arg(1040);
    assert_eq!(element, "tinio-sandbox-fd=1040");
    assert!(is_listener_fd_arg(&element));
    assert_eq!(listener_fd_from_arg(&element), Some(1040));
    assert!(!is_listener_fd_arg(OsStr::new("--port")));
    // The launcher's own arguments survive untouched, minus the element the
    // child consumes itself. `args` is what follows `argv[0]` — what
    // `std::env::args_os().skip(1)` hands over — and the cast stands in for the
    // listener's handle, whose value is the only thing the element carries.
    let args = launch::command_args(
        1040usize as HANDLE,
        &["/srv/data".into(), "--port".into(), "0".into()],
    );
    assert_eq!(args[0], element, "the element comes first");
    let kept: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|arg| !is_listener_fd_arg(arg))
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    assert_eq!(kept, ["/srv/data", "--port", "0"]);
}

#[test]
fn a_launch_that_named_no_listener_is_an_error_not_a_socket() {
    // `adopt` only ever hands back a descriptor alongside the element that named
    // it, so this is the hand-built `Adopted` a caller reaches by constructing
    // one itself: what the rule does about it is report it, rather than rebuild a
    // socket from a value no launch chose.
    let adopted = Adopted {
        config: String::new(),
        listener_fd: None,
    };
    let error = launch::listener(&adopted).expect_err("no descriptor, no socket");
    let rendered = error.to_string();
    assert!(rendered.contains("rebuild the listener"), "{rendered}");
    assert!(rendered.contains("argv"), "{rendered}");
}

/// A reader that fails the test if anything reads from it.
///
/// **An EOF-returning reader cannot express this rule**: `io::empty()` and a
/// `Cursor` over an empty buffer behave identically whether or not they are read,
/// so a test built on one passes for a rule that reads stdin and one that does
/// not. This reader is hostile to being read at all, which is the property under
/// test — the same shape the core's own test for this rule uses.
struct Untouchable;

impl Read for Untouchable {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        panic!("`adopt` read stdin without a listener element in argv");
    }
}
