//! The launcher's own plumbing: the child's adoption of the payload it was
//! started with, the inherited listener it rebuilds from that, and the argv
//! elements that name the inherited handles in the first place (design §3,
//! decisions 16 and 17).
//!
//! **The frame is the core's; the socket is this crate's.** [`adopt`] wraps the
//! core's rule — the whole decision (no element, an element with an undecodable
//! payload, an element with a good one) lives in `tinio-sandbox-core` and is
//! unit-tested there, because it takes the arguments and the reader as
//! parameters — and what this module adds to it is the two inputs that are *this*
//! process's: its argv and its stdin. The listener is the other way round:
//! turning a handle value back into a `TcpListener` is `unsafe`, so it belongs in
//! exactly one module ([`crate::socket`], decision 14), and all this module does
//! is unwrap the descriptor the core handed back. The **control channel's** handles
//! take the same route out of the core and a different one out of this crate: the
//! named pipes are adopted by `tinio-api` ([`adopt`]'s `Adopted::controls` names
//! them), because the crate that serves the channel is the crate that owns the raw
//! conversion. There is one per instance, and [`command_args`] writes them all.
//!
//! **What is deliberately not here.** The payload *write* is
//! [`crate::handles::write_payload`] with `serde_json` at the call site, so no
//! wrapper re-spells those two. The stop hook is the other way round: the
//! mechanism, the Win32 calls and their safety arguments are
//! [`crate::control`]'s (design decision 14), and [`on_stop`] and [`disarm`] are
//! the crate-level names the child side and a process that outlives one launch
//! reach for — the shape the shared surface fixes, so that `serve`'s child path
//! names one function on every platform.

use std::{
    ffi::OsString,
    io::{self, Read},
    net::TcpListener,
};

use windows_sys::Win32::Foundation::HANDLE;

// The core's `adopt` is aliased because this module's own wrapper owns that
// name. The core's `launch` needs no alias: no item here is called that, and a
// module's own name is not in its own scope.
use crate::_core::{Adopted, Result, adopt as core_adopt, launch};
/// Re-export the core's element helpers so the child side of the call site reads
/// symmetrically, consuming the core's names rather than aliasing them.
pub use crate::_core::{
    CONTROL_FD_PREFIX, LISTENER_FD_PREFIX, control_fd_arg, control_fd_from_arg, is_control_fd_arg,
    is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg,
};
use crate::{control, socket};

/// Adopt the launch payload, if this process inherited one.
///
/// Core's rule, called on this process's arguments and stdin: `Ok(None)` means
/// this process is the launcher. The rule itself — which element triggers a
/// read, what a bad frame means — is the core's and is unit-tested there; what
/// this wrapper adds is only that the two inputs are *this* process's.
///
/// # Errors
///
/// None of its own: the rule is the core's, and this wrapper only supplies *this*
/// process's argv and stdin. Core's `adopt` reads stdin only when the element is
/// present, and treats a read that fails or a frame that does not decode as the
/// launcher case — `Ok(None)` — which inside a confinement fails closed at the
/// next grant or bind.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// let adopted = launch::adopt(std::env::args_os(), std::io::stdin().lock())?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn adopt(
    args: impl IntoIterator<Item = OsString>,
    stdin: impl Read,
) -> Result<Option<Adopted>> {
    core_adopt(args, stdin)
}

/// Rebuild the listener the launch handed back as a descriptor.
///
/// **Once per process.** The rebuilt `TcpListener` is the sole owner of the
/// inherited handle ([`socket::from_value`]'s own safety argument), so a child
/// calls this once, on the descriptor its payload named: a second call with the
/// same value is two owners of one socket, and one of them will close it under
/// the other.
///
/// # Errors
///
/// [`Error::Launch`](tinio_sandbox_core::Error::Launch) when the launch named no
/// listener: a production launcher always binds one (design §10 step 6), so this
/// reports a launch that did not come from the launcher path rather than
/// fabricating a socket.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::Adopted;
/// use tinio_sandbox_windows::launch;
///
/// let adopted = Adopted::new(String::new(), Some(1040), Vec::new(), None, None);
/// let listener = launch::listener(&adopted)?;
/// // The child serves on the socket the launcher bound; it never binds one.
/// let _ = listener;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn listener(adopted: &Adopted) -> Result<TcpListener> {
    let fd = adopted.listener().ok_or_else(|| {
        launch(
            "rebuild the listener",
            "argv",
            io::Error::other("the launch named no listener descriptor"),
        )
    })?;
    let listener = socket::from_value(fd as u64)?;
    // A `std` socket is created blocking on Windows. The mode is inherited by every accepted
    // connection, and tokio cannot see it (`check_socket_for_blocking` is a no-op off unix), so
    // hyper's next read blocks a worker inside `recv`. The child's copy is the one that serves.
    listener
        .set_nonblocking(true)
        .map_err(|source| launch("mark the listener nonblocking", "listener", source))?;
    Ok(listener)
}

/// Run `handler` when this process is stopped: the child-side half of the signal
/// relay (design §3 step 9, decision 20's second half).
///
/// **The delegation is the whole of this function, and the name is the reason
/// it exists.** The mechanism is [`crate::control::on_stop`]'s — a console
/// control handler that signals an event a thread of this process waits on — and
/// what this wrapper adds is the crate-level path the child side reaches for, the
/// one the facade carries under the same name so that `serve`'s child path can be
/// written once and name one function on all three platforms.
///
/// # Errors
///
/// Whatever [`crate::control::on_stop`] reports: the handler was refused, or the
/// event its thread waits on could not be created.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// // A forwarded stop lands here. It runs on a thread of this process's own, so
/// // what it does may allocate and take locks — which is what makes it the
/// // place a graceful shutdown starts.
/// launch::on_stop(|| {
///     let _ = ();
/// })?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn on_stop(handler: impl Fn() + Send + 'static) -> Result<()> {
    control::on_stop(handler)
}

/// Put this process back on the default disposition, so a relay armed for one
/// launch does not go on swallowing console control events after it.
///
/// The process that needs this is the one that outlives a launch — the test
/// harness, and any future CLI that serves twice — because `serve`'s launcher arm
/// forwards its child's status and exits instead (design §3 step 9).
///
/// # Errors
///
/// Whatever [`crate::control::disarm`] reports: the handler could not be removed,
/// which means it is still installed and still swallowing.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_windows::launch;
///
/// launch::disarm()?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn disarm() -> Result<()> {
    control::disarm()
}

/// The child's arguments: the listener element first, the control elements next
/// when there are any, then the launcher's own arguments in order.
///
/// `args` is what follows `argv[0]` — `std::env::args_os().skip(1)` in
/// production, `Options::args` under test — and the program is not among them:
/// `command_line` writes this process's own executable as the child's `argv[0]`,
/// so the elements land at index 1 (and 2…) of that line and the launcher's
/// arguments keep their order after them (design §3 step 1: the child reaches the
/// same root and the same flags by the same path).
///
/// **The ordering is the contract, and the child parses its own arguments
/// against it.** The elements come first so that a child which drops `argv[0]`
/// and filters them is left with exactly the launcher's arguments — for a test
/// binary `child_role --exact` and nothing else — and so that neither launcher
/// plan nor the child's `own_args` has to guess where they sit. The listener's is
/// **always** present and always first: it is what `adopt` reads to decide this
/// process is a child at all.
///
/// **The launcher's side of that contract is `skip(1)` and nothing else.** A
/// caller that handed over its whole `args_os()` would put the launcher's own
/// executable path where the served root belongs — the child drops its `argv[0]`
/// and filters the element, so what is left is parsed as the launcher's
/// arguments.
///
/// `controls` is the inherited control channel's **instances**, for the launch that
/// carries them: the Windows launcher binds the pipes (only an unconfined
/// process may — a confined child's `CreateNamedPipeW` is refused) and the child serves on every
/// handle it inherits. The same element is written once per instance — one prefix,
/// repeated — so the child's parse is unchanged and only the count differs. An
/// empty slice is every other launch, and the child's argv is then byte-for-byte
/// what it was before the control channel existed.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_windows::launch;
/// use windows_sys::Win32::Foundation::HANDLE;
///
/// let instances = [1044usize as HANDLE, 1048usize as HANDLE];
/// let args = launch::command_args(
///     1040usize as HANDLE,
///     &instances,
///     &[OsString::from("/srv/data"), OsString::from("--port")],
/// );
/// assert_eq!(args[0], "tinio-sandbox-fd=1040");
/// assert_eq!(args[1], "tinio-sandbox-control-fd=1044");
/// assert_eq!(args[2], "tinio-sandbox-control-fd=1048");
/// assert_eq!(
///     args[3..],
///     [OsString::from("/srv/data"), OsString::from("--port")]
/// );
///
/// // No control channel: the listener element and the launcher's arguments alone.
/// let bare = launch::command_args(
///     1040usize as HANDLE,
///     &[],
///     &[OsString::from("/srv/data"), OsString::from("--port")],
/// );
/// assert_eq!(bare[0], "tinio-sandbox-fd=1040");
/// assert_eq!(
///     bare[1..],
///     [OsString::from("/srv/data"), OsString::from("--port")]
/// );
/// ```
pub fn command_args(listener: HANDLE, controls: &[HANDLE], args: &[OsString]) -> Vec<OsString> {
    let mut child = Vec::with_capacity(args.len() + 1 + controls.len());
    child.push(listener_fd_arg(listener as i64));
    child.extend(
        controls
            .iter()
            .map(|control| control_fd_arg(*control as i64)),
    );
    child.extend(args.iter().cloned());
    child
}

#[cfg(test)]
mod tests {
    //! The launch payload and the inherited listener: what [`adopt`] answers for
    //! a process's arguments, the argv [`command_args`] builds, the socket
    //! [`listener`] rebuilds from a descriptor, and the re-exec that puts a real
    //! child on the other end of the frame.
    //!
    //! **Both halves live here since the 2026-10-01 placement cleanup**
    //! (`docs/superpowers/plans/2026-10-01-test-placement-cleanup.md`): the legs
    //! that used to be `tests/payload.rs` moved beside their subjects, which is
    //! what `docs/tests.md`'s placement rule asks. The child's half is a role of
    //! *this* binary — a unit test's `env::current_exe()` is the libtest binary,
    //! where libtest names every test by its module, hence
    //! `launch::tests::child_role` — and the spawn still goes through
    //! `tinio_util::job`, the guard every harness spawn site takes.
    //!
    //! The split is still the point: the legs that observe the payload from
    //! *within* one process are here so a failure is unambiguous about which half
    //! broke, and the re-exec leg is here so the frame and the argv element are
    //! read by a process that really inherited them. The socket's own re-exec leg
    //! is `crate::socket`'s, where the rebuild it exercises lives.

    use std::{
        env,
        ffi::{OsStr, OsString},
        fs,
        io::{self, ErrorKind, Write},
        net::{TcpListener, TcpStream},
        os::windows::io::IntoRawSocket,
        process::{Command, Stdio},
        sync::mpsc,
        thread,
        time::Duration,
    };

    use tinio_sandbox_core::LaunchPayload;
    use tinio_util::job;
    // **The stdin stand-in is the shared one** (`tinio_util::testing`), which the core's own
    // leg for this rule takes too: what the two measure is the same "was stdin read at all",
    // and only a reader that fails on being read can tell that from a reader that returns
    // nothing.
    use tinio_util::testing::Untouchable;
    use windows_sys::Win32::Foundation::HANDLE;

    use super::*;
    use crate::{
        _core::is_launcher_fd_arg,
        testing::{TIMEOUT, wait_for_exit},
    };

    /// The marker that makes [`child_role`] inert in this binary's own test run.
    ///
    /// The spawned child runs `child_role --exact` with this variable set; in the
    /// launcher's run of the same binary the same function is an ordinary test that
    /// must return rather than adopt — adopting from a console this process does
    /// not own is a hang, not a failure. The payload cannot carry the marker:
    /// adopting the payload is what the role is *for*.
    const CHILD: &str = "TINIO_SANDBOX_TEST_PAYLOAD_CHILD";

    /// The libtest name of the child's role **in this binary**, which the spawn in
    /// [`a_child_reads_the_frame_the_parent_wrote`] passes as its `--exact` filter:
    /// the lib test binary, where a test is named by its module path.
    const CHILD_ROLE: &str = "launch::tests::child_role";

    /// Where the child role records what it saw, for the parent to assert on.
    const REPORT: &str = "TINIO_SANDBOX_TEST_REPORT";

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
        let adopted = adopt(env::args_os(), Untouchable).unwrap();
        assert!(adopted.is_none());
        // `OsString`s and not `String`s: the rule takes a process's own arguments,
        // and a path that is not valid UTF-8 is a legal argument on Windows.
        let adopted = adopt(
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
        let args = command_args(
            1040usize as HANDLE,
            &[],
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
    fn the_control_elements_ride_beside_the_listener_element() {
        // The inherited descriptors, and the shape the launcher's own argv takes when
        // its plan carries them: the listener element first (the child reads *it* to decide
        // this process is a child), one control element per instance next, then the launcher's
        // arguments untouched. All of them are the child's to consume, so all are filtered out
        // here — what `tinio_sandbox_core::filter_argv` does for the real child.
        let instances = [1044usize as HANDLE, 1048usize as HANDLE];
        let args = command_args(
            1040usize as HANDLE,
            &instances,
            &["/srv/data".into(), "--port".into(), "0".into()],
        );
        assert_eq!(args[0], "tinio-sandbox-fd=1040");
        assert_eq!(args[1], "tinio-sandbox-control-fd=1044");
        assert_eq!(args[2], "tinio-sandbox-control-fd=1048");
        let kept: Vec<String> = args
            .iter()
            .filter(|arg| !is_launcher_fd_arg(arg))
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            kept,
            ["/srv/data", "--port", "0"],
            "clap must never see any element"
        );
        // And with no control channel the argv is what it was before this seam existed.
        let bare = command_args(1040usize as HANDLE, &[], &["/srv/data".into()]);
        assert_eq!(bare, ["tinio-sandbox-fd=1040", "/srv/data"]);
    }

    #[test]
    fn a_launch_that_named_no_listener_is_an_error_not_a_socket() {
        // `adopt` only ever hands back a descriptor alongside the element that named
        // it, so this is the hand-built `Adopted` a caller reaches through
        // `Adopted::new`: what the rule does about it is report it, rather than rebuild
        // a socket from a value no launch chose.
        let adopted = Adopted::new(String::new(), None, Vec::new(), None, None);
        let error = listener(&adopted).expect_err("no descriptor, no socket");
        let rendered = error.to_string();
        assert!(rendered.contains("rebuild the listener"), "{rendered}");
        assert!(rendered.contains("argv"), "{rendered}");
    }

    /// The socket a child serves on must not block: a `std` listener is created blocking on Windows,
    /// and an inherited one keeps that mode unless the rebuild clears it.
    #[test]
    fn the_rebuilt_listener_accepts_without_blocking() {
        let bound = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = bound.local_addr().expect("address");
        let raw = bound.into_raw_socket();
        let adopted = Adopted::new(String::new(), Some(raw as i64), Vec::new(), None, None);
        let listener = listener(&adopted).expect("rebuild the listener");
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

    /// The child's half of the frame: adopt what the parent wrote, rebuild the
    /// socket its element named, and record what the adoption answered.
    #[test]
    fn child_role() {
        if env::var_os(CHILD).is_none() {
            return;
        }
        let adopted = adopt(env::args_os(), io::stdin().lock())
            .expect("adopt")
            .expect("a payload arrived on stdin");
        assert_eq!(adopted.config(), "version = 1\n");
        // The element named the listener's handle and `adopt` handed back the
        // descriptor; rebuilding the socket is this crate's half.
        assert!(adopted.listener().is_some(), "the element named a listener");
        let listener = listener(&adopted).expect("rebuild the listener");
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

        // **The child role is spawned through the guard** (`tinio_util::job`), like every other spawn
        // in this crate's tests: the frame is written after it is up, so a test binary that died
        // without unwinding would leave this copy of itself holding the inherited handle and the
        // target dir.
        let mut command = Command::new(env::current_exe().expect("current exe"));
        command
            .args([CHILD_ROLE, "--exact", &element.to_string_lossy()])
            .env(CHILD, "1")
            .env(REPORT, &report)
            .stdin(Stdio::piped());
        let mut child = job::spawn(&mut command).expect("spawn the child");

        // The frame comes from the core's one writer — the payload derives no
        // `Serialize` — so this is the byte string a launch really writes, config
        // text and all.
        let frame = LaunchPayload::encode_frame("version = 1\n", None, None)
            .expect("the frame is one JSON object");
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
        let status = wait_for_exit(&mut child, TIMEOUT, "the payload child");
        assert!(status.success(), "the child role failed: {status:?}");
        assert_eq!(
            fs::read_to_string(&report).expect("the child's report"),
            "payload-ok\n"
        );
    }
}
