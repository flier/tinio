#[cfg(not(feature = "sandbox"))]
use std::env::consts::OS;
#[cfg(feature = "sandbox")]
use std::net::TcpListener as StdTcpListener;
// `Duration` is the launch's readiness bound, which is the `api` arm's — the arm that publishes
// `state` and so is the one that waits.
#[cfg(all(feature = "sandbox", feature = "api"))]
use std::time::Duration;

use super::plan::Request;
#[cfg(all(feature = "sandbox", feature = "api"))]
use super::product::refuse_unwritable_state_dir;
#[cfg(feature = "sandbox")]
use super::product::sandbox_request;
#[cfg(feature = "sandbox")]
use super::sandbox_err;
#[cfg(all(feature = "sandbox", feature = "api"))]
use crate::_api::{
    bind::Channel as ControlChannel,
    state::{self as api_state, State},
    transport::{LocalForm, bind_local, resolve_socket_path},
};
#[cfg(all(feature = "sandbox", target_os = "linux"))]
use crate::_sandbox::{SECCOMP_SKIP_WARNING, linux::Seccomp};
#[cfg(all(feature = "sandbox", feature = "api"))]
use crate::commands::start::api_options;
#[cfg(feature = "sandbox")]
use crate::{
    _config::Config,
    _core::mode::AccessMode,
    _sandbox::{self, Terminal, serve},
    _server::identity,
};
use crate::{_config::storage, _sandbox::Child, Error, commands::start::Args, resolve::Resolution};

/// Runs the confinement — §1 step i, the **dynamic** half, after the fork.
///
/// It contains no determination of its own: [`crate::sandbox::plan`] has already refused every request this
/// build cannot honour, so what is left is one [`tinio_sandbox::serve::Spec`].
/// [`tinio_sandbox::platform_facts`] picks the shape through `launches_child`: an in-place target
/// applies the spec in this process and gets `None`; a launcher target binds the listener and
/// requires `Some(child)`. Three values are assembled onto that spec:
///
/// - the `Sandbox`, from `sandbox_request` — the in-root refusal and the exposure set it stands
///   in front of, in one call. Nothing here rebuilds a set: the launcher platforms'
///   canonicalisation is their own crates' (`with_canonical_paths`, `canonical_exposures`), and
///   the device-file grant no longer rides the set at all — `Sandbox` dropped the flag, and the
///   Linux backend asks for the nodes itself, as it owns every other Linux-only knob
///   (`tinio_sandbox_linux::Options::with_device_files`). No builder here names `/dev`.
/// - the configuration's **text**, carried on the resolution that read it
///   ([`Resolution::config_text`]) and forwarded unparsed. The launcher never builds a `Config`:
///   it holds credential-bearing bytes for exactly as long as it takes to hand them over (sandbox
///   design decision 18), and the child re-derives its answers from them. **It is not read here**:
///   a launcher that opened the file for itself would be taking a second answer to what the
///   document says, from a file that may have changed since the resolution ran.
/// - the **listener**, bound here so the plan owns it (sandbox design decision 16). The child
///   inherits a socket instead of the right to bind, which is what lets the platform's profile
///   or capability set drop that rule; the descriptor reaches the child in argv, spelled by the
///   facade's `listener_fd_arg`.
/// - `args` is threaded through for one thing and nothing else: the **control channel's**
///   handoff. A launcher resolves the child's transport selection
///   from the configuration and the `--api*` flags (`start::api_options`, the one definer of that
///   rule), binds the local channel itself, and hands the child what it bound. **The two
///   platforms' reasons differ and the shape does not.** Windows' is identity: a process inside
///   the container cannot create a pipe (`CreateNamedPipeW` is refused there), and the child would otherwise
///   fail its own bind with a message about a second server. macOS' is the profile: the confined
///   child's unix `bind()` is a network operation the profile grants nothing for, so a child that
///   bound its own control socket would die `Operation not permitted` — and the grant that used
///   to cover it permitted every bind, which is the grant this handoff removes. In-place platforms
///   read nothing here: `launches_child` is false, this process confines itself, then binds its
///   own socket.
///
/// **The tracing subscriber goes in through `install`, because its position is a rule about this
/// call and not a choice the caller can make.** Two facts fix the two positions, and both are
/// facts about the confinement this function performs:
///
/// - A **launcher** target emits the events an operator reconstructs a failed launch with
///   *inside* the launch below — the backend's `step = "CreateProcessW"`, the profile's
///   `taken_over=`, the ACL grant the launcher reports before it makes it — so the subscriber has
///   to exist **before** it. A launcher's confinement is its child's, applied at process creation,
///   so nothing in this process is left outside anything.
/// - An **in-place** target applies the confinement to the **calling thread**: `unshare`, the
///   capability drop and `prctl(PR_SET_SECCOMP)` each bind this thread, so a thread alive at that
///   moment stays outside the confinement for the life of the process. `build_subscriber` starts
///   exactly such a thread when it is handed an OTLP endpoint in an `otel` build
///   (`opentelemetry_sdk`'s batch exporter spawns its own), so on that arm the subscriber is built
///   **after** the confinement — where its threads are inside the namespaces, the dropped
///   capability set and the seccomp filter rather than beside them.
///
/// The caller therefore hands over **one action** rather than deciding where to put it:
/// `install` is a `FnOnce`, so "installed twice" is not expressible, and every path out of this
/// function — the two that confine nothing included — calls it exactly once, so "installed never"
/// is not either. **What this replaced was a pair of `if platform_facts().launches_child` guards
/// around this call site**, which put the ordering rule in the caller and left both failures
/// expressible: a second `set_global_default` is silently discarded, taking the access-log handle
/// and the exporter's thread with it, and a run that installed none serves with no log at all.
///
/// The answer is `Option<Child>`: `None` is "this process is confined, carry on"; `Some(child)`
/// is a confined child the caller drives with `tinio_sandbox::supervise`.
///
/// # Errors
///
/// A configuration the payload cannot be read from, a bind that fails, and every failure the
/// platform's own call reports — each fatal, because a confinement failure must not leave the
/// process running unconfined (design §11). A failure of `install` itself — the second thing that
/// can be built here — is fatal for the same reason: a run that cannot log is refused rather than
/// served silently.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_cli::{
///     commands::start::Args,
///     resolve::Resolution,
///     sandbox::{Request, confine},
/// };
/// use tinio_config::Config;
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let resolution = Resolution {
///     pg_root: None,
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_text: String::new(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     state_dir_override: None,
///     config_error: None,
/// };
/// let args = Args::default();
/// // Nothing asked for the confinement: there is nothing to run.
/// let request = Request { channel: None };
/// // The subscriber the run owes, installed by the confinement at whichever position its
/// // target's shape requires — the example stands in for it with a no-op.
/// let child = confine(&resolution, &args, &request, || Ok(()))?;
/// assert!(child.is_none());
/// # Ok(())
/// # }
/// ```
pub fn confine(
    resolution: &Resolution,
    args: &Args,
    request: &Request,
    install: impl FnOnce() -> Result<(), Error>,
) -> Result<Option<Child>, Error> {
    if !request.enabled() {
        // Nothing is confined, so there is no ordering for the subscriber to respect: it goes
        // in where it always has, before the run does anything.
        install()?;
        return Ok(None);
    }
    // [`plan`] has already refused a request this build cannot honour. What is left is one
    // spec: in-place targets apply it here, launcher targets bind the sockets and supervise
    // the child. Which is which is [`tinio_sandbox::platform_facts`], not this file's `cfg`.
    #[cfg(not(feature = "sandbox"))]
    {
        let _ = (resolution, args);
        // The refusal below is a run that ends, and the log is where an operator reads why.
        install()?;
        Err(Error::Operational(format!(
            "a confinement was requested, but no platform backend is wired to apply it on `{OS}`: \
             nothing would be confined, so the run is refused rather than served unconfined"
        )))
    }
    #[cfg(feature = "sandbox")]
    {
        confine_enabled(resolution, args, install)
    }
}

/// One spec, applied in place or by launching a child — [`tinio_sandbox::platform_facts`] picks.
#[cfg(feature = "sandbox")]
fn confine_enabled(
    resolution: &Resolution,
    args: &Args,
    install: impl FnOnce() -> Result<(), Error>,
) -> Result<Option<Child>, Error> {
    let facts = _sandbox::platform_facts();
    let access = AccessMode::from(resolution.read_only);
    let sandbox = sandbox_request(resolution, access)?;
    // Read once, off the rule that owns it, and carried as a value: the spec takes it and the
    // confinement applies it.
    let local_uid = local_uid_configured(&resolution.config);
    let spec = serve::Spec::from_sandbox(sandbox).local_uid(local_uid);
    if !facts.launches_child {
        let _ = args;
        // **The confinement first, the subscriber second** — this thread is the one being
        // confined, so every thread the subscriber starts has to start inside it. A confinement
        // that failed returns through `?` with nothing installed, which is the same answer a
        // process that never reached the subscriber gave before this parameter existed.
        //
        // **The answer is the confinement's own** (`serve::Confined`): the resolved model, the
        // exposure facts (the set's size, the jail's mount count and the rendered path list) and
        // the filter's outcome come back on it, so nothing is pre-read from the spec this call
        // consumes and no request is restated — the re-emit below states what the confinement
        // applied rather than a second derivation of it.
        let confined = spec.apply().map_err(sandbox_err)?;
        install()?;
        // **The re-emit this arm's ordering owes** — `announce_in_place`'s doc owns what it says
        // and why, and the `cfg` is the target whose in-place arm can succeed.
        #[cfg(target_os = "linux")]
        announce_in_place(&confined);
        return Ok(confined.child);
    }
    // **The subscriber first, the launch second**: everything the launcher reports from here on
    // — the grants, the profile, `CreateProcessW` — is emitted by the calls below, and a
    // subscriber built after them would see none of it.
    install()?;
    let listener = StdTcpListener::bind(resolution.address()?)?;
    let spec = spec
        .config_text(resolution.config_text.clone())
        .listener(listener);
    #[cfg(feature = "api")]
    {
        let (spec, bound) = attach_control(spec, resolution, args)?;
        let handoff = match bound.as_ref() {
            Some((channel, token)) => Handoff::of(Some(token.as_str()), Some(channel))?,
            None => None,
        };
        let mut child = expect_launcher_child(spec)?;
        // **A publish failure ends the child and the run with it — one unconditional call.**
        // `Child::abort` is the core's ownership of "end this child": every launcher platform
        // installs a killer, so which mechanism it is — a `SIGKILL` to the pid on macOS, a
        // `TerminateJobObject` on the job the Windows launcher holds — is not a question this
        // call site asks, and the launch's failure is not one it has to reason about before
        // ending the child it left serving. **It used to be a question**, through a
        // `retract_on_publish_failure` fact and a `retract` helper spelled here; both are gone,
        // the fact having lost its last reader once the ending moved into the core beside the
        // killer. The `Err` below is what the caller gets, and `crate::run`'s serving arm frames
        // it on stdout exactly once.
        if let Some(handoff) = &handoff
            && let Err(error) = publish_state(&child, handoff, resolution)
        {
            child.abort();
            return Err(error);
        }
        Ok(Some(child))
    }
    #[cfg(not(feature = "api"))]
    {
        let _ = args;
        expect_launcher_child(spec).map(Some)
    }
}

/// Whether the resolved config asks for a `local_uid` mapping — the Linux capability knob.
///
/// **Asked of [`identity::owner_uids_for`]**, the same function the storage's chown map comes from,
/// so the capability `serve::Spec` keeps and the chowns storage performs cannot disagree about
/// whether a mapping exists.
#[cfg(feature = "sandbox")]
pub(super) fn local_uid_configured(config: &Config) -> bool {
    !identity::owner_uids_for(config).is_empty()
}

/// The `sandbox-shared.md` §7 record this process owes **after** confining itself in place — the
/// events the confinement emitted into nothing, said once the subscriber exists.
///
/// **Why a re-emit rather than a second install point.** The subscriber's position is a rule about
/// this confinement and not a choice the caller can make: on this arm every thread `build_subscriber`
/// starts has to start inside the namespaces, the dropped capability set and the filter, so the
/// subscriber is built *after* `apply`. `tracing` drops an event emitted before a subscriber
/// exists, so the events the confinement itself emits (`enter`'s `model` and `exposures`, the
/// capabilities' and `capset`'s lines, and `seccomp`'s install-or-skip) are written into nothing on
/// this arm, however real the facts behind them are.
///
/// **What is said again is §7's own record, and every value comes off the confinement's own
/// answer** ([`serve::Confined`], which `Spec::apply` carries back): the *resolved* mechanism
/// (never the request — `Auto` became one of the two against a live probe, and that is a
/// confinement-time decision this line must not take a second time), the exposure set's size, the
/// mount count and the rendered path list (the exposure event's whole field set), and the network
/// filter's outcome. Nothing here re-derives a fact, pre-reads a spec `apply` consumes, or reads
/// the environment.
///
/// **This replaces a line with a vocabulary of its own** — `step = "confined in place"` with
/// fields `requested`/`count`/`local_uid`. It could not name the resolved model (`apply` dropped
/// it with the backend), so it stated the operator's *request* under a name of its own; the
/// exposure count was read off the spec at the call site before `apply` consumed it; and
/// `local_uid` was a request fact the confinement had already answered. The step names below are
/// §7's, so an operator greps the in-place arm's record with the same words as a launcher arm's.
///
/// **This is the Linux arm's alone.** `platform_facts().launches_child` is false only here, and a
/// target with no backend is refused by `plan` before `confine` is ever entered — so a launcher
/// platform has no re-emit to make (its subscriber is installed *before* the launch, and sees
/// everything already).
///
/// **`pub(super)` because the call site is not a test's to reach**: the arm runs after a real
/// confinement, and the crate's own test drives the emission instead — asserting §7's fields, the
/// level `serve` enables and that nothing the run holds as a secret is among them.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
pub(super) fn announce_in_place(confined: &serve::Confined) {
    // §7's model event: the resolved mechanism, which is exactly what the replaced line could not
    // state.
    tracing::info!(
        step = "model",
        model = ?confined.model,
        "the model this process confined itself with"
    );
    // §7's exposure event, **in full**: `count` and `mounts` are the two numbers the backend's own
    // exposure event carries and the rendered path list is its third field — all three ride
    // [`serve::Confined`] back, so this line states the same facts the confinement's own event
    // stated into nothing rather than a count alone.
    tracing::info!(
        step = "exposures",
        count = confined.exposures,
        mounts = confined.mounts,
        exposures = %confined.rendered_exposures,
        "the exposure set this process confined itself to"
    );
    // §7's filter event: the install, or the one degradation this run must name — the backend's own
    // `warn!` was emitted before the subscriber existed, so this is the record that survives (and
    // the reason the values ride `Confined` back at all). **The sentence is the shared definition**
    // (`SECCOMP_SKIP_WARNING`), with this arm's own clause about what still holds appended, so the
    // two warning lines cannot drift apart.
    match confined.seccomp {
        Some(Seccomp::Installed) => tracing::info!(
            step = "seccomp",
            installed = true,
            "the network filter this process installed"
        ),
        Some(Seccomp::SkippedUnsupportedArch) => tracing::warn!(
            step = "seccomp",
            installed = false,
            "{SECCOMP_SKIP_WARNING}; the directory confinement still holds"
        ),
        None => {}
    }
}

/// How long the launcher waits for the child's readiness frame — **the bound the `state` publish
/// is decided by, in both directions**.
///
/// A child that announces `Listening` inside it is published for. A child still silent when it
/// expires is **not merely left unpublished: the run is refused** — the launcher answers an error,
/// the arm's caller ends the child, and this process exits non-zero. The bound is therefore the
/// decision `refuse_unwritable_state_dir` reads from the other side: a confined server left
/// serving with no `state` file is the run `tinio status` reports as stopped while it works, which
/// is the state that refusal exists to keep from ever being reached, and waiting is what would
/// reach it.
///
/// The number is the sandbox design's readiness budget (§6, 30 s): the e2e harness's start timeout
/// (`crates/tinio-e2e/tests/steps/clients.rs`'s `START_TIMEOUT`), which is the budget a confined
/// launch is measured against. A longer wait would answer past the bound the consumers of this
/// process's readiness already gave up on, and a shorter one would refuse a launch that is still
/// coming — the ACL grant alone is measured past 30 s on a large NTFS tree (design §6), which is
/// why the number is the budget's own rather than a smaller one's.
#[cfg(all(feature = "sandbox", feature = "api"))]
const PUBLISH_BOUND: Duration = Duration::from_secs(30);

/// The control channel this launcher bound and the token that guards it, as one value
/// (design decision 17's pair).
#[cfg(all(feature = "sandbox", feature = "api"))]
pub(super) struct Handoff<'a> {
    channel: &'a ControlChannel,
    token: &'a str,
}

#[cfg(all(feature = "sandbox", feature = "api"))]
impl<'a> Handoff<'a> {
    /// `Ok(None)` when there is no local channel; refuse a half-connected pair.
    pub(super) fn of(
        token: Option<&'a str>,
        channel: Option<&'a ControlChannel>,
    ) -> Result<Option<Self>, Error> {
        match (channel, token) {
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

/// Write the `state` file once the child reports `Listening` — **or refuse the run when the bound
/// expires with the child still silent**.
///
/// One answer per [`_sandbox::Readiness`]: the announcement is what the file is written from; a
/// child that reported something else, or whose relay ended, leaves the supervision as the run's
/// report; and the bound expiring is an [`Error::Operational`]. **The refusal is this arm's shape
/// rather than a new
/// one**: it travels the same path every other failure of this `if let` does — the caller ends the
/// child with `Child::abort`, which every launcher platform implements (macOS's `SIGKILL` to the
/// pid, the Windows launcher's `TerminateJobObject`), and returns the `Err` that `crate::run`'s
/// serving arm frames on stdout once. A launcher that instead supervised the silent child would
/// leave a server serving with nothing naming it, which is exactly the state
/// `refuse_unwritable_state_dir` exists to prevent.
#[cfg(all(feature = "sandbox", feature = "api"))]
fn publish_state(
    child: &Child,
    handoff: &Handoff<'_>,
    resolution: &Resolution,
) -> Result<(), Error> {
    // **The three answers come back named**, so nothing here re-derives them: the facade's
    // `Readiness` is read off the channel rather than off a clock this module would have to
    // supply.
    let address = match child.wait_for_ready(PUBLISH_BOUND) {
        _sandbox::Readiness::Reported(Terminal::Listening(address)) => address,
        // The child reported a failure of its own (an `Error` frame), or its relay ended because
        // it exited: **the child's own life, not the launcher's bound, is what ended the wait**,
        // and the supervision below is that run's report (design decision 19's parent frame). It
        // is also the arm a confined child that refuses its own root before ever reporting takes —
        // the case `tinio-server`'s Windows e2e pins as the parent's own frame and the child's
        // status.
        _sandbox::Readiness::Reported(Terminal::Error(_)) | _sandbox::Readiness::Gone => {
            return Ok(());
        }
        // **The bound expired with the child still there and silent** — the refusal.
        _sandbox::Readiness::Elapsed => {
            return Err(Error::Operational(format!(
                "the confined child did not report readiness within {bound:?}: the `state` file \
                 this launcher publishes so `tinio status` and `tinio stop` can find the run was \
                 never written, and a server left serving without one is the run that file exists \
                 to prevent — so the child is ended and the run is refused",
                bound = PUBLISH_BOUND,
            )));
        }
    };
    let state = State::with_token(
        handoff.channel.identity(),
        Some(address.port()),
        &resolution.root,
        handoff.token.to_owned(),
    )?;
    state.write(&resolution.state_dir)?;
    Ok(())
}

/// FR-036's trade as the pair the warning names — the channel that asked for the confinement and
/// the endpoint the confinement costs — or `None` when either half is absent.
///
/// **The two cannot both be honoured**, which is why the combination is a warning rather than two
/// grants: the confinement cuts the confined process's outbound connections, and the OTLP exporter
/// is an outbound path this product has. **It is not the only one any more**: the pg backend's
/// connection to its database is the other, and a pg run under a confinement is not warned about
/// but **refused up front** ([`refuse_pg_under_confinement`]) — losing an observer is not grounds
/// for refusing to serve, losing the store is. The operator's ruling for the OTLP case is that the
/// combination is **warned about, naming both, and the confinement proceeds** (design decision 25,
/// FR-036) —
/// losing an observer is not grounds for refusing to serve — so this answers the sentence's two
/// facts and `crate::commands::start::run` emits the event, where the log is live and stdout's
/// frames are `crate::events`' own.
///
/// **It answers rather than emits**, for the reason [`crate::commands::start::run`]'s
/// `launcher_exit` states: a function that both decided the message and wrote it would put the one
/// writer in two places — and could not be driven by a test at all. The channel is read off the
/// [`Request`] rather than recomputed from the three channels' sources: `plan` is the one reader
/// of that rule, and the request is where its answer travels.
pub(crate) fn lost_export<'a>(
    request: &Request,
    endpoint: Option<&'a str>,
) -> Option<(&'static str, &'a str)> {
    // `channel` is `Some` exactly when a confinement was asked for, so an unconfined run — the one
    // whose export nobody is about to cut — answers `None` here without a second test of
    // `enabled`.
    Some((request.channel?.name(), endpoint?))
}

/// The refusal a **pg backend under a requested confinement** answers with, or `None` when either
/// half is absent.
///
/// **A confinement and a postgres store cannot be honoured together.** The confinement cuts the
/// confined process's outbound connections, and since the pg backend exists the store's connection
/// to its database is such a path — so a jailed pg run has no route to its store and could never
/// serve. FR-036's own trade ([`lost_export`]) is a **warning** because what it costs is an
/// observer; here what is lost is the store itself, so the combination is **refused** up front,
/// naming both settings, in the shape the run's other static refusals use (it precedes the fork,
/// the directories and the confinement) — a silent unservable run is worse than a refusal.
///
/// **It answers rather than emits**, for `lost_export`'s reason: the caller (the pre-runtime half)
/// owns the refusal's rendering. The channel is read off the [`Request`] rather than recomputed —
/// `plan` is the one reader of that precedence rule.
pub(crate) fn refuse_pg_under_confinement(
    request: &Request,
    backend: storage::Backend,
) -> Option<Error> {
    if backend != storage::Backend::Pg {
        return None;
    }
    let channel = request.channel?;
    Some(Error::Usage(format!(
        "{} asks for a confinement, but `[storage] type = \"pg\"` cannot serve under one: the \
         confinement cuts the confined process's outbound connections, and the pg store's \
         connection to its database is exactly such a path — a jailed pg run has no route to its \
         store. Serve the filesystem backend under the confinement, or drop the confinement \
         ({}) to serve PostgreSQL",
        channel.name(),
        channel.name(),
    )))
}

/// `serve::Spec::apply` must hand back a child when this target launches one.
#[cfg(feature = "sandbox")]
fn expect_launcher_child(spec: serve::Spec) -> Result<Child, Error> {
    match spec.apply().map_err(sandbox_err)?.child {
        Some(child) => Ok(child),
        None => Err(Error::Operational(
            "the backend returned no child to supervise: nothing was confined, so the run is \
             refused"
                .into(),
        )),
    }
}

/// Bind the local control channel, when the selection has one, and put its handles on the spec.
#[cfg(all(feature = "sandbox", feature = "api"))]
fn attach_control(
    spec: serve::Spec,
    resolution: &Resolution,
    args: &Args,
) -> Result<(serve::Spec, Option<(ControlChannel, String)>), Error> {
    let selection = api_options(resolution, args)?;
    let Some(form) = selection.local else {
        return Ok((spec, None));
    };
    if _sandbox::platform_facts().probe_state_dir_writable {
        refuse_unwritable_state_dir(&resolution.state_dir)
            .map_err(|err| Error::Operational(err.to_string()))?;
    }
    let form = match form {
        LocalForm::Unix(path) => LocalForm::Unix(resolve_socket_path(&path, &resolution.state_dir)),
        LocalForm::Pipe(name) => LocalForm::Pipe(name),
    };
    let channel = bind_local(&form)?;
    let token = api_state::generate_token()?;
    // **What crosses is the platform's, so this call is `#[cfg]`-split rather than fed one
    // shape** (`tinio_sandbox::control_handles` carries the rule): a unix launcher hands the
    // descriptor itself, borrowed from the channel that owns it and outlives the call, so the
    // duplicate the plan takes is made inside that call; Windows hands the handle values its
    // channel keeps owning, which is the form `CreateProcessW` marks inheritable. Neither arm
    // is the other's shape bent — a unix `i64` would be a number no signature can check, and a
    // Windows `BorrowedFd` does not exist — which is why the facade publishes two signatures
    // where it used to publish one.
    #[cfg(unix)]
    let handles = _sandbox::control_handles(&[channel.socket()]).map_err(sandbox_err)?;
    #[cfg(not(unix))]
    let handles = _sandbox::control_handles(&channel.handoff_values()).map_err(sandbox_err)?;
    let spec = spec.controls(handles).with_token(token.clone());
    Ok((spec, Some((channel, token))))
}
