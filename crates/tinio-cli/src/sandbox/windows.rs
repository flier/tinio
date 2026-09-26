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

use std::env;

use super::launch::Handoff;
#[cfg(feature = "api")]
use crate::_api::{bind::bind_pipe, state as api_state, transport::LocalForm};
use crate::{
    _sandbox::{Backend, Child, ControlHandle, windows},
    Error,
    commands::start::{Args, api_options},
    resolve::Resolution,
};

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
/// The bind, the capability list's own refusal, the token generation, the publish's half-connected
/// pair, the `state` publish, and every failure the backend reports — each fatal, because a
/// confinement that cannot be applied must not leave the process running unconfined (design §11).
/// A publish that fails after the child is running returns before the child is handed over, and the
/// `Child` dropped on that path closes the job that owns it —
/// `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` — so a refused startup leaves no confined server behind.
pub(super) fn confine_or_launch(resolution: &Resolution, args: &Args) -> Result<Child, Error> {
    let (sandbox, plan) = super::launch::prepare(resolution)?;
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
                // travel as the raw handles the child's `Plane::adopt` converts back, and on this
                // platform the plan holds them as values rather than owning them — `ControlHandle`
                // carries the reason (`tinio-sandbox-core`'s `launcher::ControlHandle`: the
                // launcher's `Channel` is the owner, and `OwnedHandle` has no safe constructor
                // from a raw handle).
                let handles = channel
                    .raw_handles()
                    .into_iter()
                    .map(|raw| ControlHandle::inherited(raw as usize as i64))
                    .collect::<Vec<_>>();
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
    // **The publish's one value, built here and once** — `Handoff`'s own doc owns why the two halves
    // are one fact and what it refuses. It is built **before the launch** so a half-connected pair
    // is a startup refusal rather than a serving child whose `state` file was never written, and it
    // borrows the token off the plan rather than holding a second copy of it.
    #[cfg(feature = "api")]
    let handoff = Handoff::of(&plan, _channel.as_ref())?;
    let options = windows::Options {
        capabilities: windows::Capability::parse_list(
            env::var("TINIO_SANDBOX_CAPABILITIES").ok().as_deref(),
        )?,
        ..Default::default()
    };
    let mut backend = windows::Backend::with_options(options);
    let child = super::launch::supervised("Windows", backend.confine(&sandbox, &plan)?)?;
    // **The publish, once the child says it is listening** — the readiness point, moved from the
    // child's `serve` to the process that can write the file. The token comes back off the plan and
    // the name off the channel rather than out of second copies, so the value in the payload the
    // child adopted and the value in the file are one value by construction. A run that bound no
    // local channel has no handoff and owns no file: that is a state of the launch rather than a
    // half of the pair, and the failure path needs nothing extra here — the `Child` dropped on it
    // closes the job that owns the process (`JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`).
    #[cfg(feature = "api")]
    if let Some(handoff) = &handoff {
        super::launch::publish_state(&child, handoff, resolution)?;
    }
    Ok(child)
}

// The `state` publish — its readiness gate, the bound it waits on and the pid/name choice —
// is `super::launch`'s, shared with the macOS arm rather than spelled here.
