//! `tinio stop` — the thin client that asks a running server to stop and waits for its exit.
//!
//! The request is plan A's `client::stop` (the `POST /stop` the plane answers `202` to), and the
//! wait is `gone`'s poll over the candidate `probe_pair` chose (decision 19).
//! **Nothing here creates and nothing here deletes**: `state` and the socket are the plane's own,
//! removed by `Plane::shutdown` on its way out — a client that unlinked either could strip a live
//! server of its discovery, and this command has no way to know the server behind the file is not
//! still draining — and the resolution both client commands share *reads* a configuration rather
//! than writing one, so a root that was never started is never initialised by asking it to stop.
//!
//! The command is an `async fn` awaited on the process's **one** runtime: the probe and the
//! 100 ms poll are async work on that runtime, and this module has no runtime of its own to
//! build, enter or shut down (design decision 2).

use std::{path::Path, process::ExitCode, time::Duration};

use tokio::time::{Instant, sleep};

pub use super::DirArgs as Args;
use super::{client_resolution, probe_pair};
use crate::{
    _api::{State, client, state},
    Error,
    resolve::Located,
};

/// The contract's bound on the confirmation (`contracts/cli.md:68`: "polls the control channel
/// until the probe fails or `state` disappears (bounded ~15 s)").
const STOP_BOUND: Duration = Duration::from_secs(15);

/// One poll's step — the cadence the daemon's own readiness probe uses, short enough that a
/// server that goes away is noticed promptly and long enough not to spin on a dial.
const POLL: Duration = Duration::from_millis(100);

/// Asks the server for the root `args` names to stop, and polls until it has.
///
/// The request goes to the one of decision 19's two candidates that answers; a root with no
/// running server is `Error::Operational` — exit 1, with "no running server" in the message.
/// Success is the **probe** failing (or `state` disappearing, whichever comes first), because
/// the plane stops answering the moment it ceases accepting, while its `state` file lives until
/// the drain is over.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a state directory that cannot be derived, an
/// unreadable `state` file, a control channel that refuses the request, and an exit that was not
/// confirmed within `STOP_BOUND` (all exit 1).
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::stop::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn stop() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let code = run(args, located).await?;
/// // 0 once the server has gone; an unconfirmed exit is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// `located` and `args` are `status`'s pair for the same reason: one discovery resolution for
/// every non-launching command, and the authoritative parse's flags.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let (resolution, candidates) = client_resolution(args, located)?;
    // Decision 19's pair, tried in order, and the request goes only where a server answers:
    // `POST /stop` at whatever else holds a stale control name would report a transport failure
    // where the truth is "no running server".
    let answer = probe_pair(&candidates).await?;
    if !answer.running() {
        return Err(Error::Operational(format!(
            "no running server for {}",
            resolution.root.display()
        )));
    }
    let state_dir = answer.state_dir;
    client::stop(&state_dir).await?;

    let deadline = Instant::now() + STOP_BOUND;
    loop {
        if gone(&state_dir).await {
            println!("stopped");
            println!("state dir: {}", state_dir.display());
            return Ok(ExitCode::SUCCESS);
        }
        if Instant::now() >= deadline {
            // The contract's other outcome: the request was accepted and the server has not been
            // seen to leave. Reported as an error — the operator's next move depends on knowing
            // that the stop is unconfirmed, and exit 0 would say the opposite.
            return Err(Error::Operational(format!(
                "{} accepted the stop, but its exit was not confirmed within {} s",
                resolution.root.display(),
                STOP_BOUND.as_secs()
            )));
        }
        sleep(POLL).await;
    }
}

/// Whether the server behind `state_dir` has gone: its `state` file is no longer there, or
/// nothing answers on the control channel that file names.
///
/// **The probe is the signal that ends the wait**, and that is measured rather than tidy:
/// `Plane::shutdown` removes `state` only after both planes have drained, so a bound that waited
/// for the file alone would hold this command open for the whole drain window. The channel stops
/// answering the moment the plane ceases accepting — which is when the operator's "stopped" is
/// true — and every entry point to a shutdown (a signal, the supervisor, `POST /stop`) ends the
/// same way.
///
/// A file that cannot be read mid-poll is not an answer either way — a removal can transiently
/// look like one, and an unreadable file is `status`'s error, not this poll's — so a poll that
/// cannot ask the probe question leaves the caller's bound to decide.
async fn gone(state_dir: &Path) -> bool {
    if !state::path(state_dir).exists() {
        return true;
    }
    match State::read(state_dir) {
        Ok(Some(state)) => !client::probe(&state).await,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    /// The two shapes a poll's question has before a server is ever involved: a directory with no
    /// `state` file is gone, and there is nothing to ask the probe about.
    #[tokio::test]
    async fn a_directory_with_no_state_file_is_gone() {
        let dir = TempDir::new().expect("temp");
        assert!(gone(dir.path()).await);
    }

    /// A `state` file whose control channel cannot be dialled is gone too — that is the case the
    /// probe exists for, and the one a crashed server leaves behind.
    #[tokio::test]
    async fn a_state_file_nothing_answers_for_is_gone() {
        let dir = TempDir::new().expect("temp");
        State::new(
            "/nonexistent/tinio-0123456789abcdef.sock",
            Some(9000),
            Path::new("/srv/data"),
        )
        .expect("state")
        .write(dir.path())
        .expect("write");
        assert!(gone(dir.path()).await);
    }
}
