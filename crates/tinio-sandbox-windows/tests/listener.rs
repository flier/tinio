//! The inherited listener: a child rebuilds the parent's socket from a handle
//! value and serves on it. No AppContainer here — this test pins the handle
//! plumbing on its own so a failure is unambiguous; the confined variant is in
//! `tests/confine.rs`.

#![cfg(target_os = "windows")]

mod common;

use std::{
    env,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    os::windows::io::IntoRawSocket,
    process::Command,
    sync::mpsc,
    thread,
    time::Duration,
};

use tinio_sandbox_core::Adopted;
use tinio_sandbox_windows::{launch, socket};

/// The socket a child serves on must not block: a `std` listener is created blocking on Windows,
/// and an inherited one keeps that mode unless the rebuild clears it.
#[test]
fn the_rebuilt_listener_accepts_without_blocking() {
    let bound = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = bound.local_addr().expect("address");
    let raw = bound.into_raw_socket();
    let adopted = Adopted::new(String::new(), Some(raw as i64), Vec::new(), None, None);
    let listener = launch::listener(&adopted).expect("rebuild the listener");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let kind = listener.accept().map(|_| ()).map_err(|err| err.kind());
        let _ = tx.send(kind);
    });
    match rx.recv_timeout(Duration::from_millis(300)) {
        Ok(Err(ErrorKind::WouldBlock)) => {}
        Ok(other) => panic!("accept returned {other:?}, expected WouldBlock"),
        Err(_) => {
            // A blocking accept is stuck until a client arrives. Connect so the thread can
            // finish, then fail: the harness must not hang on the bug this test exists to catch.
            let _ = TcpStream::connect(address);
            let _ = rx.recv_timeout(Duration::from_secs(2));
            panic!("accept blocked: the rebuilt listener is still a blocking socket");
        }
    }
}

#[test]
fn child_role() {
    if env::var_os("TINIO_SANDBOX_TEST_LISTENER_CHILD").is_none() {
        return;
    }
    let raw: u64 = env::var("TINIO_SANDBOX_TEST_LISTENER")
        .expect("handle value")
        .parse()
        .expect("a handle value");
    let listener = socket::from_value(raw).expect("rebuild the listener");
    let (mut stream, _) = listener.accept().expect("accept");
    let mut buffer = [0u8; 4];
    stream.read_exact(&mut buffer).expect("read the ping");
    assert_eq!(&buffer, b"ping");
    stream.write_all(b"pong").expect("write the pong");
}

#[test]
fn an_inherited_handle_is_the_same_socket_in_the_child() {
    if env::var_os("TINIO_SANDBOX_TEST_LISTENER_CHILD").is_some() {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("address");
    socket::mark_inheritable(&listener).expect("mark inheritable");

    let mut child = Command::new(env::current_exe().expect("current exe"))
        .args(["child_role", "--exact"])
        .env("TINIO_SANDBOX_TEST_LISTENER_CHILD", "1")
        .env(
            "TINIO_SANDBOX_TEST_LISTENER",
            socket::value(&listener).to_string(),
        )
        .spawn()
        .expect("spawn the child");

    let mut stream = TcpStream::connect(address).expect("connect through the parent's socket");
    // Bounded before anything reads: a child that failed to rebuild the listener
    // answers nothing, and an unbounded read would block the whole harness on a
    // failure that is already fully reported by the child's own output above.
    // The bound is the harness's (`common::TIMEOUT`, thirty seconds) because the
    // feature's own readiness budget is what it is: **a harness that hangs
    // instead of failing is a harness nobody can read**, and this project has paid
    // for that once already — P1's evidence table carries the row for an unbounded
    // `waitpid` after a fallback `kill`, where the first probe hung and had to be
    // killed by hand.
    stream
        .set_read_timeout(Some(common::TIMEOUT))
        .expect("bound the read");
    stream.write_all(b"ping").expect("write");
    let mut buffer = [0u8; 4];
    stream.read_exact(&mut buffer).expect(
        "the child never wrote its pong — whatever went wrong in it is in the test output above",
    );
    assert_eq!(&buffer, b"pong");

    // **This wait is the one the handshake has already justified**: the child
    // answered, so it is alive and past its own assertion, and all that is left
    // is the few milliseconds libtest takes to exit. It is bounded anyway,
    // because "answered and then stopped responding" is the other shape that
    // stalls a harness, and unlike the read above there is no second mechanism
    // that would end it.
    let status = common::wait_for_exit(&mut child, common::TIMEOUT, "the child role");
    assert!(status.success(), "the child role failed: {status:?}");
}
