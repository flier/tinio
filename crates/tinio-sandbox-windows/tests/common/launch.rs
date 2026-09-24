//! In-process confined launch, and the `Run` a suite reads back.

use std::{
    ffi::OsString,
    net::{SocketAddr, TcpListener},
    path::Path,
};

use tinio_sandbox_core::{
    AccessMode, Backend as BackendTrait, Confinement, LaunchPlan, Sandbox, supervise,
};
use tinio_sandbox_windows::{Backend, Options, sid};

use super::{tree::Tree, wait::answer};

/// Start one confined child over `tree` and hand back the supervision.
///
/// The in-process entry: the caller has a [`Tree`] because it built one, and the
/// probe it writes is the one that carries this tree's `target` when a test set
/// one (the network suite does).
///
/// **A launch that fails panics with the backend's own error**, which is the
/// whole of what this harness can do with it: the confinement suite has no
/// subject left after a launch that did not happen, and a `None` a caller could
/// return on would make "the backend refused to confine anything" look green.
pub fn launch_confined(
    tree: &Tree,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> (Confinement, SocketAddr) {
    let probe = tree.probe();
    start(tree, &probe, root_access, options, bind)
}

/// Start one confined child from a tree **path**, for a caller that is its own
/// process.
///
/// The launcher roles of the events and signal suites are separate processes,
/// handed a directory in `TINIO_SANDBOX_TEST_TREE`; they cannot be given a
/// [`Tree`], so this entry rebuilds one with [`Tree::at`] and writes the probe
/// under it. What a rebuilt tree cannot carry is a `target` — a launcher role is
/// not the test that set one — and nothing that calls this entry wants one.
pub fn launch_confined_at(
    path: &Path,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> (Confinement, SocketAddr) {
    let tree = Tree::at(path);
    let probe = tree.probe();
    start(&tree, &probe, root_access, options, bind)
}

/// The one body both entries above share: the exposure set, the listener, the
/// plan and the launch.
///
/// It takes the probe's path rather than writing it, because *where* the probe
/// goes is [`Tree::probe`]'s rule and *what* it says is the caller's — one entry
/// carries a target and the other cannot.
fn start(
    tree: &Tree,
    probe: &Path,
    root_access: AccessMode,
    options: Options,
    bind: Option<SocketAddr>,
) -> (Confinement, SocketAddr) {
    let sandbox = Sandbox::new()
        .expose(&tree.root, root_access)
        .expose(&tree.report, AccessMode::ReadWrite);
    let listener = TcpListener::bind(bind.unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 0))))
        .expect("bind the listener");
    let address = listener.local_addr().expect("the listener's address");
    // The listener is attached, not carried through the payload: a socket
    // cannot ride on a stream (decision 16).
    let plan = LaunchPlan::for_launch(probe.to_string_lossy().into_owned()).with_listener(listener);
    // The child is one role function of this test binary: `Options::args` is the
    // seam that says so, because a test binary's own argv belongs to libtest.
    //
    // **`--nocapture` is load-bearing, and what it prevents was measured on this
    // host (2026-09-21).** The child's stdout is the event pipe, and libtest
    // captures a test's `print!`/`println!` into a per-test buffer — discarded for
    // a passing test and written out only for a failing one. Without this flag the
    // child's frames never reach the pipe at all (the events suite failed exactly
    // so), and a child that panicked would have its diagnostics swallowed twice:
    // captured by libtest, and then dropped by the relay as non-frames, leaving
    // the operator a silent non-zero exit.
    let options = Options {
        args: Some(vec![
            OsString::from("child_role"),
            OsString::from("--exact"),
            OsString::from("--nocapture"),
        ]),
        ..options
    };
    let mut backend = Backend::with_options(options);
    let confinement = BackendTrait::confine(&mut backend, &sandbox, &plan)
        .expect("the Windows backend must confine the sandbox it was given");
    (confinement, address)
}

/// Start a confined child **with the options the caller needs**, answer one
/// loopback connection through the inherited listener, wait for it, and hand back
/// what it did.
///
/// **The one body, and [`run_confined`] below is its default.** The callers that
/// needed anything but `Options::default()` — the three that exercise a
/// capability — used to open-code this whole sequence, which put the harness's
/// order (launch, destructure, answer, supervise) and the `panic!` that reports a
/// backend which launched nothing in each of them.
///
/// The tree is still the caller's: what it wrote to its report file is read here,
/// and so is what the relay recorded — [`Run`] carries all three, because
/// [`Run::terminal`] is final only once this function's `supervise` has joined
/// the relay (decision 19).
pub fn run_confined_with(tree: &Tree, root_access: AccessMode, options: Options) -> Run {
    let (confinement, address) = launch_confined(tree, root_access, options, None);
    let Confinement::Supervise(mut child) = confinement else {
        panic!("the Windows backend must launch a child");
    };
    answer(address);
    // `supervise` **borrows** the child, which is what leaves the terminal slot
    // readable afterwards — and the wait inside it joins the relay, so that read
    // is final rather than a snapshot (decision 19).
    let status = supervise(&mut child, || ()).expect("supervise the confined child");
    Run {
        status: status.code().unwrap_or(-1),
        report: tree.report_text(),
        terminal: child.terminal().is_some(),
    }
}

/// Start a confined child with the backend's default options, answer one loopback
/// connection through the inherited listener, wait for it, and hand back what it
/// did.
pub fn run_confined(tree: &Tree, root_access: AccessMode) -> Run {
    run_confined_with(tree, root_access, Options::default())
}

/// A confined run's outcome.
pub struct Run {
    /// The child's exit code.
    pub status: i32,
    /// What the child wrote to its report file.
    pub report: String,
    /// **Whether the child reported a terminal event** — `Listening` or
    /// `Error` — on its way up.
    ///
    /// **`false` is a fact and not a missing value**: the child exited without
    /// emitting either frame, which is the state a launcher turns into its own
    /// `Event::Error` (decision 19). A `bool` rather than the `Option<Terminal>`
    /// this field used to carry, because the harness asks only whether the relay
    /// answered at all — the event's own contents are asserted where the frame is
    /// decoded, and a second reader wanting them is what would widen this back.
    /// Read **after** `supervise` — the wait joins the relay, so the answer is
    /// final rather than a snapshot.
    pub terminal: bool,
}

/// The package SID this run derives for `tree`.
pub fn sid_for(tree: &Tree) -> sid::PackageSid {
    sid::PackageSid::derive(&sid::package_name(&tree.root)).expect("derive the package SID")
}
