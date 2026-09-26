//! The launcher steps both platform arms run, in one place.
//!
//! **What is here is what the arms spelled identically.** The arms keep what is theirs: the
//! channel each binds, the handle form that carries it, the backend, and the platform's own extra
//! refusals — which is the whole of what differs. The `state` publish and the readiness budget it
//! waits on were two byte-identical copies before this module, and the copies had already drifted
//! by omission: the macOS arm ends a child whose publish failed, and the Windows arm did not.

use std::net::TcpListener as StdTcpListener;

#[cfg(feature = "api")]
use std::time::Duration;

use super::sandbox_request;
#[cfg(feature = "api")]
use crate::_api::{bind::Channel, state::State};
#[cfg(feature = "api")]
use crate::_sandbox::Terminal;
use crate::{
    _core::mode::AccessMode,
    _sandbox::{Child, Confinement, LaunchPlan, Sandbox},
    Error,
    resolve::Resolution,
};

/// The spine's shared prefix: the in-root refusal, the exposure set, the configuration the payload
/// carries, the bound listener, and the launch plan they make.
///
/// **The order is the contract.** The refusal runs *before* the exposure set is built — both are
/// [`sandbox_request`]'s, which is what makes that order structural rather than a rule a caller
/// keeps. The configuration is the **text the resolution read** and not a second read of the file
/// it came from (`Resolution::config_text`): that file was already read to resolve this run — once
/// by `resolve::mode_config` for the mode, once by the chain that produced the resolution — and
/// re-reading it here was both a third read of one start and a second answer to what the document
/// says, so a file edited between the resolution and the launch would have served a configuration
/// the resolution never saw. The text is the payload's because the file itself may sit outside
/// every exposure the confinement grants (design decision 18), and the launcher still never builds
/// a `Config`. The listener is a `std` one because this runs before the runtime exists. Every step
/// is fatal: a confinement that cannot be applied must not leave the process running unconfined
/// (design §11).
pub(super) fn prepare(resolution: &Resolution) -> Result<(Sandbox, LaunchPlan), Error> {
    let access = AccessMode::from(resolution.read_only);
    let sandbox = sandbox_request(resolution, access)?;
    let listener = StdTcpListener::bind(resolution.address()?)?;
    Ok((
        sandbox,
        LaunchPlan::for_launch(resolution.config_text.clone()).with_listener(listener),
    ))
}

/// How long the launcher waits for the child's readiness frame before it gives up on publishing
/// `state` and lets the supervision report what happened.
///
/// **A bound, not a promise about the child**: the child's own startup may take as long as it
/// takes, and a child that never reports has either failed (its `Error` frame — or its exit
/// status — is what the launcher reports) or is stuck. What the bound stops is a *stuck* child
/// holding the launcher in a wait with nothing at the end of it: past it, the launcher does not
/// publish and goes on to supervise, which is where the child's fate is decided anyway. The number
/// is the sandbox design's readiness budget (§6, 30 s), which a confined launch has to fit inside
/// and which the harnesses that wait on a launch time out against.
#[cfg(feature = "api")]
pub(super) const PUBLISH_BOUND: Duration = Duration::from_secs(30);

/// The `Confinement` an arm's backend answered, as the child it must be — and the refusal when it
/// is not.
///
/// **A launcher platform never confines in place, and the rule is the backend contract's rather
/// than either arm's sentence.** `Confinement::InPlace` from a launcher backend means the arm's
/// own process was confined instead of a child, so nothing was confined where the run asked and
/// the run must be refused: that is the one outcome the fail-closed rule forbids serving through.
/// The sentence names the platform because that is what an operator reads and what the e2e leg
/// asserts on, so the platform word is all an arm passes.
pub(super) fn supervised(platform: &str, confinement: Confinement) -> Result<Child, Error> {
    match confinement {
        Confinement::Supervise(child) => Ok(child),
        Confinement::InPlace => Err(Error::Operational(format!(
            "the {platform} backend must supervise its child rather than confine in place: \
             nothing was confined, so the run is refused"
        ))),
    }
}

/// What the `state` publish needs: **the control channel this launcher bound and the token that
/// guards it, as one value** (design decision 17's pair).
///
/// **The two halves are one fact, and this type is the only place they meet.** The arm that binds
/// the channel is the arm that mints the token — both live in one arm of one `match` over the
/// resolved selection — so a launch has either both or neither, and holding them apart was the
/// defect: a launcher that passed one without the other published **nothing** for a confined child
/// that was already serving, and a server with no `state` file is one no `tinio status` and no
/// `tinio stop` can find. That is the failure [`crate::sandbox::macos`]'s `retract` answers once it
/// has happened; [`Handoff::of`] is what keeps it from happening, because the publish takes this
/// value and has nothing left to re-test.
///
/// **The token is read off the plan rather than carried as a second copy**, which is
/// [`LaunchPlan::with_token`]'s own rule: the plan is where the value is set — it is what the
/// platform serializes into the payload the child adopts — so the token the child verifies against
/// and the token the `state` file holds are one value by construction rather than two that must
/// agree.
#[cfg(feature = "api")]
pub(super) struct Handoff<'a> {
    /// The channel this launcher bound; its `identity()` is what the `state` file records.
    channel: &'a Channel,
    /// The token the plan carries for it.
    token: &'a str,
}

#[cfg(feature = "api")]
impl<'a> Handoff<'a> {
    /// The handoff a launched plan and the channel that launch bound make, or `None` for a launch
    /// that bound **no** local channel at all.
    ///
    /// `Ok(None)` is every run with no local management channel — an `--api http://…` selection, a
    /// `--no-api-unix` run, an `api`-less build — where the child serves no such channel and this
    /// process therefore owns no `state` file. That is a state of the launch, not half of this
    /// pair.
    ///
    /// # Errors
    ///
    /// [`Error::Operational`] for a pair that is **half-connected**: a channel whose launch set no
    /// token, or a token whose launch bound no channel. Neither is a state any arm of this crate
    /// builds — the two are set by the same arm of the same `match` — so this is the refusal a
    /// future arm meets instead of the silent no-publish that separate arguments made possible:
    /// **a serving server with no `state` file is the failure**, and it is answered by a refusal
    /// naming it rather than by letting the run serve undiscoverable, which is the shape
    /// [`supervised`] keeps for the backend that confined in place.
    pub(super) fn of(
        plan: &'a LaunchPlan,
        channel: Option<&'a Channel>,
    ) -> Result<Option<Self>, Error> {
        match (channel, plan.token()) {
            (Some(channel), Some(token)) => Ok(Some(Self { channel, token })),
            (None, None) => Ok(None),
            (Some(_), None) => Err(Error::Operational(
                "this launcher bound a local control channel but the launch payload carries no \
                 token for it: the `state` file the run owes would name a channel nothing guards, \
                 so the run is refused rather than served undiscoverable"
                    .into(),
            )),
            (None, Some(_)) => Err(Error::Operational(
                "the launch payload carries a token but this launcher bound no local control \
                 channel: there is no channel for the `state` file to name, so the run is refused \
                 rather than served half-connected"
                    .into(),
            )),
        }
    }
}

/// Write the `state` file for the child this launcher confined, with the port the child reported.
///
/// **Nothing is published unless the child reported `Listening`.** `None` from
/// [`Child::wait_for_ready`] means no terminal event arrived inside [`PUBLISH_BOUND`] — a child
/// that failed, or one that is stuck — and `Terminal::Error` means it reported its own failure.
/// Neither is a server: `state` means "a server claims this root", and publishing it for a child
/// that is not serving would send `tinio status` and `tinio stop` after a channel nothing answers.
/// The launcher's report of what happened is the supervision's, which
/// [`crate::commands::start::run`] reaches either way.
///
/// **The control name is the bound channel's own identity**, which is the name the child actually
/// serves on: [`Channel::identity`] is what this launcher bound — the pipe name, or the socket
/// file — and it is the same name the child's `Plane::adopt` resolves from the same selection
/// through the same single definer, so the file and the child meet by construction rather than by
/// agreement, and nothing here re-derives a name that could disagree with the channel on disk.
///
/// **The pid is the launcher's** (`State::with_token`'s own `process::id()`), and that choice is
/// deliberate rather than incidental. It is the process the operator owns: it outlives the child's
/// whole serving life, and on Windows killing it takes the child with it through the job object.
/// The `--daemon` parent's readiness check compares the pid in this file against the pid it
/// spawned — which *is* this launcher, so a file naming the confined grandchild would make the
/// daemon time out on a run that started perfectly. It is also what makes `Plane::shutdown`'s
/// removal guard hold in the child (`state.pid == process::id()` is false there), so the child
/// never attempts a deletion that would take this launcher's file.
#[cfg(feature = "api")]
pub(super) fn publish_state(
    child: &Child,
    handoff: &Handoff<'_>,
    resolution: &Resolution,
) -> Result<(), Error> {
    let Some(terminal) = child.wait_for_ready(PUBLISH_BOUND) else {
        return Ok(());
    };
    let Terminal::Listening(addr) = terminal else {
        return Ok(());
    };
    let state = State::with_token(
        handoff.channel.identity(),
        Some(addr.port()),
        &resolution.root,
        handoff.token.to_owned(),
    )?;
    state.write(&resolution.state_dir)?;
    Ok(())
}

#[cfg(all(test, feature = "api"))]
mod tests {
    use super::*;

    /// **The publish's pair has three shapes, and only two of them are answers.** A launch with no
    /// local channel owns no `state` file, and that is `Ok(None)`; a launch that carries the
    /// token without the channel it guards is the **half-connected** state the publish must never
    /// skip over — the pair used to travel as two separate arguments, and an arm that passed one
    /// without the other wrote no file at all for a confined server that was already listening.
    /// The refusal names that outcome instead, so a future arm that assembles the halves apart
    /// hears about it at startup rather than leaving a server nothing can find.
    ///
    /// **The channel-carrying halves of the pair need a bound channel**, so what this leg drives is
    /// the two states a plan alone can be put in; the arm that binds one is exercised by the
    /// launch itself (`crates/tinio-e2e`'s confined runs) rather than by a fabricated channel here.
    #[test]
    fn a_handoff_is_absent_or_refused_and_never_half() {
        let plan = LaunchPlan::for_launch("port = 9000".into());
        assert!(
            matches!(Handoff::of(&plan, None), Ok(None)),
            "a launch with no local channel owns no `state` file"
        );
        let plan = plan.with_token("00".repeat(32));
        assert!(
            Handoff::of(&plan, None).is_err(),
            "a token no bound channel carries is refused, not published without a name"
        );
    }
}
