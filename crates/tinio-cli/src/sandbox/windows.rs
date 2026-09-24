//! Windows launcher confinement: design §10 step 6, the launcher's half.
//!
//! The launcher binds the data plane's port, hands the socket over, and launches this
//! executable again inside an AppContainer; [`crate::commands::start::run`] supervises the
//! child it gets back. Windows is the only arm that launches — Linux confines in place
//! ([`super::linux::confine_in_place`]) — which is why this returns the [`Child`] and that
//! one returns `()`.
//!
//! **The configuration travels as text, and the launcher never builds a `Config` from a host
//! path the child cannot reach** (decision 18): the child re-derives every answer from the
//! payload's text, the environment and its inherited argv, and the file itself may sit
//! outside every exposure.
//!
//! **The launcher also owns `state`, and that is this arm's other half.** The file's DACL is
//! current-user-only and `SE_DACL_PROTECTED`, and an AppContainer token's restricted-access
//! check does not match the user SID — measured: `GetNamedSecurityInfoW`/`SetNamedSecurityInfoW`
//! answer `ERROR_ACCESS_DENIED` for every path-based access a confined child makes to it. So the
//! child neither creates nor writes nor removes `state`: this process, which runs unconfined as
//! the user, generates the token, publishes the file once the child has reported that it is
//! listening, and takes it back when the child is gone
//! ([`crate::commands::start::run`]'s `remove_launcher_state`, on `supervise`'s teardown). The
//! strict DACL is unchanged — nothing here widens a grant — and the token travels to the child
//! in the launch payload, which is private launcher→child data on stdin rather than an argv
//! element or an environment variable any other process can read.

#[cfg(feature = "api")]
use std::time::Duration;
use std::{env, fs, net::TcpListener as StdTcpListener};

use super::{refuse_in_root_state_dir, sandbox_request};
#[cfg(feature = "api")]
use crate::_api::{
    bind::{Channel, bind_pipe},
    state::{self as api_state, State},
    transport::LocalForm,
};
use crate::{
    _core::mode::AccessMode,
    _sandbox::{Backend, Child, Confinement, LaunchPlan, Terminal, windows},
    Error,
    commands::start::{Args, api_options},
    resolve::Resolution,
};

/// How long the launcher waits for the child's readiness frame before it gives up on publishing
/// `state` and lets the supervision report what happened.
///
/// **A bound, not a promise about the child**: the child's own startup may take as long as it
/// takes, and a child that never reports has either failed (its `Error` frame — or its exit
/// status — is what the launcher reports) or is stuck. What the bound stops is a *stuck* child
/// holding the launcher in a wait with nothing at the end of it: past it, the launcher does not
/// publish and goes on to supervise, which is where the child's fate is decided anyway. The
/// number is the sandbox design's readiness budget (§6, 30 s), which a confined launch has to
/// fit inside and which the harnesses that wait on a launch time out against.
#[cfg(feature = "api")]
const PUBLISH_BOUND: Duration = Duration::from_secs(30);

/// Design §10 step 6: the exposure set, the payload, the bound listener, the bound control
/// channel and the launch.
///
/// **The mode is the resolved one**, not `[server] read_only`: `TINIO_READ_ONLY` and
/// `--read-only` are part of the chain that produced it, so reading the document here would
/// expose the served root read-write on a run the operator asked to be read-only.
///
/// **The listener is bound here rather than by the child** (decision 16): the child inherits
/// a socket instead of the right to bind, which is what lets the container's rules drop that
/// one, and the descriptor reaches it in argv.
///
/// **The control channel is bound here for a reason of its own, and it is the same handoff
/// with a different motive.** The pipe `Plane::bind` would create carries a
/// current-user-only DACL, and inside the AppContainer the child's identity is the package
/// SID: its own `CreateNamedPipeW` is refused `ERROR_ACCESS_DENIED`, which the bind reports
/// as "another server is already listening" — a message about a competing server for a
/// channel no one could create. This process runs unconfined, as the user, so *it* makes the
/// channel and the child adopts the handle; the DACL, and the single-instance claim, are
/// exactly what they were. The selection resolved here is [`Args`]'s and the configuration's,
/// through the CLI's one `api_options` rule, so the launcher's answer and the child's are the
/// same one; a selection with no local channel (`--api http://…`, or a build without `api`)
/// hands nothing over and the child binds its own transport as it always did.
///
/// **The token is generated with the channel and travels on the plan** (see the module doc):
/// the launcher publishes `state` with it, so it is the launcher's, and the child adopts it
/// rather than minting one — two tokens for one file would leave every client that read it
/// refused. Both live inside the `LocalForm::Pipe` arm because both are that arm's: the file
/// names the channel this process created, and a launch with no channel has no `state` for this
/// process to own or to name.
///
/// **A capability list is the operator's environment**, `TINIO_SANDBOX_CAPABILITIES`, parsed
/// by the backend's own [`windows::Capability::parse_list`] so the grammar has one owner; an
/// absent or empty value is no capability at all, which is the whole of the outbound
/// guarantee's Windows half (design §7).
///
/// # Errors
///
/// The read of the configuration the payload carries, the bind, the capability list's own
/// refusal, the token generation, the `state` publish, and every failure the backend reports —
/// each fatal, because a confinement that cannot be applied must not leave the process running
/// unconfined (design §11). A publish that fails after the child is running returns before the
/// child is handed over, and the `Child` dropped on that path closes the job that owns it —
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` — so a refused startup leaves no confined server behind.
pub(super) fn confine_or_launch(resolution: &Resolution, args: &Args) -> Result<Child, Error> {
    let access = AccessMode::from(resolution.read_only);
    // Refused **before** the exposure set is built: the entry it would add is the hole the
    // guard exists to close.
    refuse_in_root_state_dir(
        access.is_read_only(),
        &resolution.root,
        &resolution.state_dir,
    )
    .map_err(|err| Error::Operational(err.to_string()))?;
    let sandbox = sandbox_request(&resolution.root, &resolution.state_dir, access);
    // The launcher's resolution ran the serving chain, which created a missing file before
    // this line, so the text is the same document the child would have read.
    let config = fs::read_to_string(&resolution.config_path)?;
    let listener = StdTcpListener::bind(resolution.address()?)?;
    let plan = LaunchPlan::for_launch(config).with_listener(listener);
    // **The control channel, when this run has a local one.** Held to the end of this
    // function and no further: the child inherits a *duplicate* of the handle, which is what
    // keeps the pipe alive once this one is dropped — the launcher never serves on it, and a
    // `Channel` that outlived the launch would be a second live owner of the pipe it created.
    #[cfg(feature = "api")]
    let control = {
        let selection = api_options(resolution, args)?;
        match selection.local {
            Some(LocalForm::Pipe(name)) => {
                let channel = bind_pipe(&name)?;
                // The token the child will verify against, generated here because this is the
                // process that publishes the file it guards. `api_state` is the one definer of
                // what a token is, so the launcher does not spell a length or a source of
                // randomness of its own.
                let token = api_state::generate_token()?;
                // **Every instance of the pool, not just the first.** The child cannot create a
                // pipe at all (its identity is the package SID), so the handles it is handed are
                // the whole of what it can ever serve on: one handle means the name answers once
                // and then goes deaf, which is the defect the pool exists to close. The values
                // travel as the raw handles the child's `Plane::adopt` converts back.
                let handles = channel
                    .raw_handles()
                    .into_iter()
                    .map(|raw| raw as usize as i64);
                let plan = plan.with_controls(handles).with_token(token);
                (plan, Some(channel))
            }
            // No local channel: an `http://`/`https://` selection, or `--no-api-unix`. The
            // child binds nothing of its own here either — it serves no local channel at
            // all — and the launch is unchanged.
            _ => (plan, None),
        }
    };
    #[cfg(not(feature = "api"))]
    let control = (plan, None);
    let (plan, _channel) = control;
    let options = windows::Options {
        capabilities: windows::Capability::parse_list(
            env::var("TINIO_SANDBOX_CAPABILITIES").ok().as_deref(),
        )?,
        ..Default::default()
    };
    let mut backend = windows::Backend::with_options(options);
    let child = match backend.confine(&sandbox, &plan)? {
        Confinement::Supervise(child) => child,
        Confinement::InPlace => {
            return Err(Error::Operational(
                "the Windows backend must supervise its child rather than confine in place: \
                 nothing was confined, so the run is refused"
                    .into(),
            ));
        }
    };
    // **The publish, once the child says it is listening** — the readiness point, moved from the
    // child's `serve` to the process that can write the file. The token comes back off the plan
    // rather than out of a second copy, so the value in the payload the child adopted and the
    // value in the file are one value by construction.
    #[cfg(feature = "api")]
    if let (Some(channel), Some(token)) = (&_channel, plan.token()) {
        publish_state(&child, channel, token, resolution)?;
    }
    Ok(child)
}

/// Write the `state` file for the child this launcher confined, with the port the child reported.
///
/// **Nothing is published unless the child reported `Listening`.** `None` from
/// [`Child::wait_for_ready`] means no terminal event arrived inside [`PUBLISH_BOUND`] — a child
/// that failed, or one that is stuck — and `Terminal::Error` means it reported its own failure.
/// Neither is a server: `state` means "a server claims this root", and publishing it for a child
/// that is not serving would send `tinio status` and `tinio stop` after a channel nothing
/// answers. The launcher's report of what happened is the supervision's, which
/// [`crate::commands::start::run`] reaches either way.
///
/// **The three facts are the child's and the file is the launcher's.** The port is the address
/// the child relayed in its readiness frame (`Listening { addr }` — the socket this process
/// bound and handed over); the control name is the launcher's own channel's identity, which is
/// the name the child records too (`Plane::adopt` normalises the same selection through the same
/// `bind::pipe_name`); and the root is the resolved one, canonical, exactly as the child's
/// `ApiOptions` carries it.
///
/// **The pid is the launcher's** (`State::new`'s own `process::id()`), and that choice is
/// deliberate rather than incidental. It is the process the operator owns: it outlives the
/// child's whole serving life, killing it takes the child with it through the job object, and
/// the `--daemon` parent's own readiness check compares the pid in this file against the pid it
/// spawned — which *is* this launcher, so a file naming the confined grandchild would make the
/// daemon time out on a run that started perfectly. It is also what makes `Plane::shutdown`'s
/// removal guard hold in the child (`state.pid == process::id()` is false there), so the child
/// never even attempts the deletion its confinement would refuse.
#[cfg(feature = "api")]
fn publish_state(
    child: &Child,
    channel: &Channel,
    token: &str,
    resolution: &Resolution,
) -> Result<(), Error> {
    let Some(terminal) = child.wait_for_ready(PUBLISH_BOUND) else {
        return Ok(());
    };
    let Terminal::Listening(addr) = terminal else {
        return Ok(());
    };
    let state = State::with_token(
        channel.identity(),
        Some(addr.port()),
        &resolution.root,
        token.to_owned(),
    )?;
    state.write(&resolution.state_dir)?;
    Ok(())
}
