#[cfg(feature = "sandbox")]
use std::env::consts::OS;

use clap::parser::ValueSource;

#[cfg(feature = "sandbox")]
use super::product::refuse_underived_state_dir;
#[cfg(feature = "sandbox")]
use crate::_sandbox;
use crate::{
    Error,
    cli::Provenance,
    commands::start::Args,
    resolve::{Resolution, pick},
};

/// Which of the three channels asked for the confinement (design decision 10).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Channel;
///
/// // A refusal names the channel it came from, which is why the channel is a value and not a
/// // boolean.
/// assert!(matches!(Channel::Env, Channel::Env));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// `--sandbox`.
    Flag,
    /// `TINIO_SANDBOX`.
    Env,
    /// `[server] sandbox`.
    Config,
}

impl Channel {
    /// The channel as a refusal names it — the spelling the operator used, which is the first
    /// half of every message this module raises.
    ///
    /// `pub(super)` because [`Request::channel`] carries it past the decision: the FR-036 warning
    /// names the channel that asked, and `crate::sandbox::confine`'s `lost_export` is where that
    /// spelling is read. The second reader is a warning rather than a refusal, so this stays the
    /// one place the three spellings live.
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Flag => "`--sandbox`",
            Self::Env => "`TINIO_SANDBOX`",
            Self::Config => "`[server] sandbox`",
        }
    }
}

/// Where the run's state directory came from: named by the operator (`TINIO_STATE_DIR`), or
/// derived by the resolution from the home directory.
///
/// **A value and not a boolean**, for [`Channel`]'s reason: what the refusing guard below reads is
/// which of the two channels answered, and a bare `!state_dir_given` at the call site reads as a
/// property of a path rather than of the parse that produced it. The distinction is the whole
/// rule — a *named* directory is exposed read-write wherever it points, so it is served; a
/// *derived* one lands under `<home>/.tinio`, which is the case macOS has no grant for.
///
/// **One reader, and it derives this from the resolution rather than being handed it**:
/// `refuse_underived_state_dir` is macOS's read-only rule — reached from [`plan`] when
/// `platform_facts().refuses_derived_state_dir` is set — and it is built there out of
/// [`Resolution::state_dir_override`]'s presence,
/// which is the same fact one step earlier and the only place it is stored.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::StateDirSource;
///
/// // The discovery parse's answer travels as one of two names, and the derived one is the case
/// // a read-only macOS confinement refuses.
/// assert!(StateDirSource::Derived.is_derived());
/// assert!(!StateDirSource::Named.is_derived());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateDirSource {
    /// An explicit `TINIO_STATE_DIR` named it.
    Named,
    /// No explicit variable: the resolution derived `<home>/.tinio/roots/<hash>/`.
    Derived,
}

impl StateDirSource {
    /// Whether the directory was derived rather than named — the case macOS cannot grant a
    /// parent for.
    pub fn is_derived(self) -> bool {
        matches!(self, Self::Derived)
    }
}

/// What the three channels resolved to.
///
/// The exposure set and the device-file grant land with the platform backend
/// [`crate::sandbox::confine`] hands the request to, while **FR-036's OTLP trade is that same
/// module's `lost_export`** — a configured `[telemetry] otlp_endpoint` and a confinement cannot
/// both be honoured, and the warning that names both is emitted by
/// [`crate::commands::start::run`], which is where the log is live. [`plan`] answers a disabled
/// one when no channel asked, and an enabled one only after `refuse_unhonourable` has cleared the
/// feature, the target and the host — so an enabled `Request` is one this build is committed to applying,
/// and the exposure set is rebuilt in the platform arm rather than carried here.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::sandbox::Request;
///
/// // No channel set: nothing is asked for, and no channel is named.
/// let request = Request { channel: None };
/// assert!(!request.enabled());
/// ```
#[derive(Debug)]
pub struct Request {
    /// **Which channel asked**, when one did — the FR-036 warning's first fact, and `Some`
    /// exactly when a confinement was asked for at all. It travels on the request rather than
    /// being recomputed where the warning is emitted for the reason every other value here does:
    /// [`plan`] is the one reader of the three channels' precedence, and a second reader would
    /// be a second answer to which of them won.
    pub channel: Option<Channel>,
}

impl Request {
    /// Whether a confinement was asked for at all.
    ///
    /// **Derived rather than stored.** The channel is `Some` exactly when one asked, so a
    /// separate field would be a second answer to one question — and the two constructors in
    /// [`plan`] are the only places that could ever have disagreed.
    pub fn enabled(&self) -> bool {
        self.channel.is_some()
    }
}

/// The sandbox channels, collapsed with FR-016's precedence: the flag and the variable (one
/// clap answer, `.env` included — the parse is two-pass), then the config file.
///
/// `source` is that answer's [`ValueSource`], and it is load-bearing rather than decorative:
/// `TINIO_SANDBOX=0` with `[server] sandbox = true` resolves **off**, which a plain `||` over a
/// `bool` could not express. Only clap's own default leaves the file's key in place.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::sandbox::resolve_flag;
///
/// assert!(
///     resolve_flag(ValueSource::CommandLine, true, false),
///     "the flag wins"
/// );
/// assert!(
///     resolve_flag(ValueSource::EnvVariable, true, false),
///     "the variable answers next"
/// );
/// assert!(
///     resolve_flag(ValueSource::DefaultValue, false, true),
///     "the config file is the last level"
/// );
/// assert!(
///     !resolve_flag(ValueSource::EnvVariable, false, true),
///     "an explicit `false` wins over the file"
/// );
/// ```
pub fn resolve_flag(source: ValueSource, requested: bool, file: bool) -> bool {
    pick(source, Some(requested), Some(file)).unwrap_or(false)
}

/// The three **static** determinations, as §1 step f runs them — before the daemon fork, so
/// every refusal reaches the operator's shell.
///
/// No channel set answers "disabled" with an empty exposure set, and a channel that **is** set
/// is resolved in one place and then measured against the three facts this build cannot change:
///
/// - **the feature.** With `sandbox` off, `--sandbox` is not in the parser at all — so the
///   answer can only have come from `TINIO_SANDBOX` or `[server] sandbox`, and either is a
///   **usage** error (exit 2) naming the build: a stripped capability refuses the request rather
///   than silently ignoring it, because silently running unconfined is the failure the whole
///   feature exists to prevent (design decision 5).
/// - **the platform's backend.** With the feature on, [`tinio_sandbox::backend_available`] is the
///   one place that records whether this target can confine anything — the target list lives in
///   the facade, beside [`tinio_sandbox::platform_facts`], so no crate keeps a table of its own —
///   and while it answers `false`, the answer is a **startup** error (exit 1) naming
///   `std::env::consts::OS`. [`plan`] is that gate, and it is the only one: [`crate::sandbox::confine`] applies
///   one [`tinio_sandbox::serve::Spec`] and does not ask the question again. The determination
///   runs before the fork, on every target, which is what makes it reachable and observable in
///   every build.
/// - **the host's own answer.** With the feature on and the target backed, the token may still be
///   refused a profile: [`tinio_sandbox::can_confine`] is the run-time half of the same question
///   [`tinio_sandbox::backend_available`] answers at build time, and while it answers `Err`, the
///   answer is a **startup** error (exit 1). It is asked before this build writes anything, so the
///   operator reads a cause and two actions instead of a `PermissionDenied` on a temp directory.
///
/// Only a build, target and host that clear all three facts are allowed to answer an **enabled**
/// [`Request`]; the exposure set at
/// the mode's access, plus the platform's own extras, is built where [`crate::sandbox::confine`]
/// applies the spec rather than carried here. **The channel rides on the request**
/// ([`Request::channel`]) and the exposure set does not: the channel is what FR-036's warning names
/// where the log is live, so it has to travel past this decision, while the set is rebuilt from the
/// resolution by the arm that applies it.
///
/// **The state directory's *channel* is read off the resolution's own answer rather than taken as
/// an argument**: [`crate::resolve::Located::state_dir_override`]'s presence is what a
/// [`StateDirSource`] names, and the one rule that needs it (macOS's, below) derives it at its
/// call site — from the [`Resolution`] that parse produced, so no caller has to hand it across
/// the directory's consumption and the resolution does not carry the same fact twice. A read-only
/// confinement derives the directory from the home when it was not named, and that derivation is
/// one macOS cannot grant — `refuse_underived_state_dir` states the rule and why the launcher is
/// where it belongs.
///
/// # Errors
///
/// A usage error (2) for a channel this build cannot honour, a startup error (1) for a platform
/// whose backend is still a stub or a host that cannot confine, and whatever reading the
/// platform's own extras needs.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::Args,
///     resolve::{Resolution, locate, server},
///     sandbox::plan,
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution: Resolution = server(located, &args, &Provenance::default())?;
/// let request = plan(&resolution, &args, &Provenance::default())?;
/// assert!(!request.enabled());
/// # Ok(())
/// # }
/// ```
pub fn plan(
    resolution: &Resolution,
    args: &Args,
    provenance: &Provenance,
) -> Result<Request, Error> {
    // FR-016's precedence, and nothing else rides on it. The flag and `TINIO_SANDBOX` are one
    // clap answer — the switch carries its variable through `env`, and a value `.env` supplied
    // arrives on the authoritative parse as an ordinary `EnvVariable` answer, which is what
    // makes `.env` the same channel one level lower — so the `ValueSource` is what names the
    // channel a refusal reports, and `[server] sandbox` is read only when clap answered nothing.
    let requested = resolve_flag(
        provenance.sandbox,
        args.sandbox_flag(),
        resolution.config.server.sandbox,
    );
    let channel = requested.then_some(match provenance.sandbox {
        ValueSource::CommandLine => Channel::Flag,
        ValueSource::EnvVariable => Channel::Env,
        _ => Channel::Config,
    });
    let Some(channel) = channel else {
        return Ok(Request { channel: None });
    };
    // A request this build cannot honour is refused here, before any exposure set exists and
    // before the fork, so the operator's shell sees it. **What is left after the refusal is a
    // request this build, this target and this host can honour**: the feature is on,
    // [`tinio_sandbox::backend_available`] answers `true`, [`_sandbox::can_confine`] answers `Ok`,
    // and [`crate::sandbox::confine`] applies
    // it — a request that reached `confine` with nothing to do would serve unconfined.
    refuse_unhonourable(channel)?;
    // A request this target **can** honour is still refused where the confinement it would build
    // cannot reach the state directory's parent — read-only with the directory derived rather than
    // named. It sits here, after the channel is known and before any exposure set or sandbox step,
    // for the same reason every refusal in this function does: the operator's shell sees it and
    // nothing is granted or launched. **The state directory itself may already exist** — the
    // resolution that produced this `Resolution` creates it — which is why the guard decides on
    // whether it was named rather than on whether it is there, and why it reads that fact off the
    // resolution: the parse that resolved the directory is the one that knew whether it was
    // named.
    //
    // The platform fact is the facade's. macOS Seatbelt exposes the state directory and grants
    // nothing for its parent; other targets create that tree before confinement and are not asked.
    #[cfg(feature = "sandbox")]
    if _sandbox::platform_facts().refuses_derived_state_dir {
        let source = if resolution.state_dir_override.is_some() {
            StateDirSource::Named
        } else {
            StateDirSource::Derived
        };
        refuse_underived_state_dir(
            resolution.read_only,
            &resolution.root,
            &resolution.state_dir,
            source,
        )
        .map_err(|err| Error::Operational(err.to_string()))?;
    }
    Ok(Request {
        channel: Some(channel),
    })
}

/// The three facts this build cannot change, refused in one place so none can be answered by
/// accident — the fail-closed half of [`plan`], and the only reader of
/// [`tinio_sandbox::backend_available`].
///
/// The messages are a matched pair and are written to be told apart by a test: the build's names
/// the build and never the platform, the platform's names `std::env::consts::OS` and never the
/// build. A leg that accepted either would pass just as well if the two were swapped, which is
/// why `confinement.feature`'s variable scenario asserts each one's own words and the absence
/// of the other's (`crates/tinio-e2e/tests/features/confinement.feature`).
fn refuse_unhonourable(channel: Channel) -> Result<(), Error> {
    // A channel this build has no feature for is a **usage** error: the request was well formed
    // and the answer is "not in this build" (design decision 5). This arm and the next are the
    // function's whole body, one cfg'd away at compile time — the same shape
    // `Args::sandbox_flag` uses, so no build has an unreachable tail.
    #[cfg(not(feature = "sandbox"))]
    {
        Err(Error::Usage(format!(
            "{} asked for a confinement, but this build has no `sandbox` feature: nothing would be confined, so the run is refused rather than served unconfined",
            channel.name()
        )))
    }
    // The platform's backend is a startup error: the argument was fine, the target cannot honour
    // it, and the message names which platform so the operator knows what the remedy is.
    #[cfg(feature = "sandbox")]
    {
        // **The question is the facade's, and this is the CLI asking it rather than deciding.**
        // The target list lives beside the `platform()` arms that implement it
        // (`tinio_sandbox::backend_available`), so a new backend's target is added in one crate
        // rather than to a second table here that only this file would notice going stale.
        if !_sandbox::backend_available() {
            Err(Error::Operational(format!(
                "{} asked for a confinement, but this platform's `sandbox` backend (`{}`) is still a stub: nothing would be confined, so the run is refused rather than served unconfined",
                channel.name(),
                OS
            )))
        } else {
            // **The third fact, and the last this build can answer before it writes anything.**
            // The target has a backend; the token may still be refused one, and on a host where
            // it is, every later step — the state directory, the served root, the listener, the
            // stale-profile sweep, the ACL salt file and the profile itself — is work done for a
            // run that cannot serve. Asking here is what makes the operator read a cause and two
            // actions instead of a `PermissionDenied` on a temp directory.
            //
            // **The ask is the facade's, exactly as `backend_available` is**: the platform that
            // can answer it is the one that owns the probe.
            _sandbox::can_confine().map_err(super::sandbox_err)?;
            Ok(())
        }
    }
}
