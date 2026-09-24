//! Bounded waits shared by every suite that spawns a process.

use std::{
    io::Read,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

/// How long any bounded wait in this harness may take before it fails the test.
///
/// The feature's own readiness budget rather than an arbitrary number:
/// `tinio-e2e`'s `START_TIMEOUT` is 30 s, and the launch has to fit inside it.
pub const TIMEOUT: Duration = Duration::from_secs(30);

/// Answer one loopback connection through the inherited listener.
///
/// **The read is bounded, and that is what makes this a test device rather than
/// a way to hang the suite**: a child that died before it reached its `accept`
/// answers nothing at all, and an unbounded read would keep the harness waiting
/// for a failure that has already been reported in full. The unwinding that
/// follows a failure drops the caller's supervision, and the job with it, which
/// is how the child is killed rather than left behind.
pub fn answer(address: SocketAddr) {
    let mut stream =
        TcpStream::connect_timeout(&address, TIMEOUT).expect("connect to the confined child");
    stream
        .set_read_timeout(Some(TIMEOUT))
        .expect("bound the read");
    let mut pong = [0u8; 4];
    stream
        .read_exact(&mut pong)
        .expect("the confined child answered the inherited listener");
    assert_eq!(&pong, b"pong", "the confined child answered something else");
}

/// Wait for `child` to exit, or kill it and fail the test rather than block on
/// it.
///
/// **The definition lives in the library** ([`tinio_sandbox_windows::testing`])
/// and this is a re-export, because `tinio-server`'s Windows e2e suite needs the
/// same function and cannot reach a `tests/` directory across a crate boundary.
/// Two copies of the rule "every spawned process is waited for or killed on
/// every path, red ones included" is the state that precedes them disagreeing —
/// and the disagreement is silent, because a suite that gave up on the deadline
/// without killing first simply leaks a child into the rest of the run. `within`
/// is [`TIMEOUT`] at every call site here; `what` names the process, because a
/// suite has more than one.
#[allow(unused_imports)] // a re-export: the binaries that call it are the readers
pub use tinio_sandbox_windows::testing::wait_for_exit;
