//! Business-agnostic confine entry: paths, bound sockets, token — no product types.

use std::{net::TcpListener, path::PathBuf, time::Instant};

use secrecy::ExposeSecret;
use tracing::{Span, field};

// **The platform vocabulary `Confined` carries on Linux** — the resolved mechanism and the
// network filter's outcome, the two facts the in-place re-emit states after the confinement.
#[cfg(target_os = "linux")]
use crate::_linux::{Mechanism, Seccomp};
// **Every arm of this module can build a refusal** — the launcher platforms' `InPlace` arm, the
// no-backend arm, and the Linux arm's unresolved-mechanism guard — so the error type is imported
// on every target rather than under a `cfg` no arm can be trusted to keep true.
use crate::Error;
use crate::{
    _core::{Secret, SecretKey},
    AccessMode, Child, Confinement, ControlHandle, LaunchPlan, Result, Sandbox, describe_exposures,
    metrics,
};

/// How a caller asks this process (or a child) to be confined — builder in, [`Confined`] out.
///
/// **Inputs are sandbox vocabulary only**: exposures, config text, an already-bound listener,
/// control handles, a token string, and the Linux `local_uid` knob. No clap, config keys, or
/// management-plane types (design `2026-09-27-sandbox-servespec-seam-design.md`).
///
/// **What the spec does not carry is the Linux model.** It used to (a `model` field fed by the
/// caller), because the CLI had to state the *request* after a confinement that consumed the
/// spec — and the value rode a facade re-export so the CLI could name its type. The resolved
/// model now travels back in [`Confined`] instead, read from the backend that applied it, so the
/// request is stated by the one reader that owns it (`Options::from_env`) and the answer is the
/// confinement's own.
///
/// **The token is a [`Secret`]** — the shared `SecretBox` layer; redacted from `Debug` by
/// construction. A `Spec` is the value a caller holds while a launch is assembled, so a failing
/// launch's `?`, a `dbg!` or a `tracing::debug!` would otherwise print the bearer credential
/// that guards `/status` and `/stop`. A set token still appears as `Some(…)`; absence stays
/// `None`, so a reader sees whether one was set without ever seeing the secret. Core's
/// [`LaunchPayload::token`] and `Adopted::token` use `#[debug(skip)]` and omit the field; here
/// the field stays in the output.
///
/// [`LaunchPayload::token`]: tinio_sandbox_core::LaunchPayload::token
///
/// `Secret` itself is `tinio_util::secret::Secret` — spelled rather than linked because this
/// crate does not depend on `tinio-util`, and a link it cannot resolve would fail the doc gate.
///
/// # Examples
///
/// ```
/// use tinio_sandbox::{AccessMode, serve};
///
/// let spec = serve::Spec::new()
///     .expose("/srv/data", AccessMode::ReadWrite)
///     .config_text("port = 9000")
///     .local_uid(false);
/// assert_eq!(spec.sandbox().exposures().len(), 1);
///
/// let token = "00".repeat(32);
/// let printed = format!("{:?}", serve::Spec::new().with_token(token.clone()));
/// assert!(printed.contains("REDACTED"), "{printed}");
/// assert!(!printed.contains(&token), "{printed}");
/// ```
#[derive(Default, Debug)]
pub struct Spec {
    sandbox: Sandbox,
    config: Option<String>,
    listener: Option<TcpListener>,
    controls: Vec<ControlHandle>,
    /// Bearer credential for `/status` and `/stop`; [`Secret`] redacts in `Debug` while
    /// `Option` still shows whether a token was set.
    token: Option<Secret>,
    local_uid: bool,
}

impl Spec {
    /// Empty spec: no exposures, no plan facts, Linux knobs off.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert!(serve::Spec::new().sandbox().exposures().is_empty());
    /// ```
    pub fn new() -> Self {
        Self::default()
    }

    /// Start from an exposure set already built.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::{AccessMode, Sandbox, serve};
    ///
    /// let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
    /// let spec = serve::Spec::from_sandbox(sandbox);
    /// assert_eq!(spec.sandbox().exposures().len(), 1);
    /// ```
    pub fn from_sandbox(sandbox: Sandbox) -> Self {
        Self {
            sandbox,
            ..Self::default()
        }
    }

    /// Expose `host` with `access` — same rule as [`Sandbox::expose`].
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::{AccessMode, serve};
    ///
    /// let spec = serve::Spec::new().expose("/srv/data", AccessMode::ReadWrite);
    /// assert_eq!(spec.sandbox().exposures().len(), 1);
    /// ```
    pub fn expose(mut self, host: impl Into<PathBuf>, access: AccessMode) -> Self {
        self.sandbox = self.sandbox.expose(host, access);
        self
    }

    /// Configuration text the launcher forwards unparsed (design decision 18).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// let spec = serve::Spec::new().config_text("port = 9000");
    /// assert_eq!(spec.config(), Some("port = 9000"));
    /// ```
    pub fn config_text(mut self, text: impl Into<String>) -> Self {
        self.config = Some(text.into());
        self
    }

    /// The config text, when set.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert_eq!(serve::Spec::new().config(), None);
    /// ```
    pub fn config(&self) -> Option<&str> {
        self.config.as_deref()
    }

    /// Own the bound listener the child will serve on (design decision 16).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::net::TcpListener;
    ///
    /// use tinio_sandbox::serve;
    ///
    /// let listener = TcpListener::bind("127.0.0.1:0")?;
    /// let spec = serve::Spec::new().listener(listener);
    /// assert!(spec.has_listener());
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn listener(mut self, listener: TcpListener) -> Self {
        self.listener = Some(listener);
        self
    }

    /// Whether a listener was set.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert!(!serve::Spec::new().has_listener());
    /// ```
    pub fn has_listener(&self) -> bool {
        self.listener.is_some()
    }

    /// Carry inherited control-channel handles.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(windows)]
    /// # fn main() {
    /// use tinio_sandbox::{ControlHandle, serve};
    ///
    /// let spec = serve::Spec::new().controls([ControlHandle::inherited(1040)]);
    /// assert_eq!(spec.control_count(), 1);
    /// # }
    /// # #[cfg(not(windows))]
    /// # fn main() {}
    /// ```
    pub fn controls(mut self, handles: impl IntoIterator<Item = ControlHandle>) -> Self {
        self.controls = handles.into_iter().collect();
        self
    }

    /// Carry one control-channel handle.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(windows)]
    /// # fn main() {
    /// use tinio_sandbox::{ControlHandle, serve};
    ///
    /// let spec = serve::Spec::new().control(ControlHandle::inherited(1040));
    /// assert_eq!(spec.control_count(), 1);
    /// # }
    /// # #[cfg(not(windows))]
    /// # fn main() {}
    /// ```
    pub fn control(self, handle: ControlHandle) -> Self {
        self.controls([handle])
    }

    /// How many control handles the spec carries.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert_eq!(serve::Spec::new().control_count(), 0);
    /// ```
    pub fn control_count(&self) -> usize {
        self.controls.len()
    }

    /// Management-plane token the child verifies and the launcher may publish.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// let token = "00".repeat(32);
    /// let spec = serve::Spec::new().with_token(token.clone());
    /// assert_eq!(spec.token(), Some(token.as_str()));
    /// ```
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(SecretKey::from(token.into()).into());
        self
    }

    /// The token, when set.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert_eq!(serve::Spec::new().token(), None);
    /// ```
    pub fn token(&self) -> Option<&str> {
        self.token
            .as_ref()
            .map(|token| token.expose_secret().as_str())
    }

    /// Whether FR-033's `local_uid` mapping is configured — Linux capability retention only.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert!(serve::Spec::new().local_uid(true).local_uid_configured());
    /// ```
    pub fn local_uid(mut self, configured: bool) -> Self {
        self.local_uid = configured;
        self
    }

    /// Whether [`Self::local_uid`] asked for a mapping.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::serve;
    ///
    /// assert!(!serve::Spec::new().local_uid_configured());
    /// ```
    pub fn local_uid_configured(&self) -> bool {
        self.local_uid
    }

    /// The exposure set the spec will confine.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox::{AccessMode, serve};
    ///
    /// let spec = serve::Spec::new().expose("/srv/data", AccessMode::ReadOnly);
    /// assert_eq!(spec.sandbox().exposures().len(), 1);
    /// ```
    pub fn sandbox(&self) -> &Sandbox {
        &self.sandbox
    }

    /// Build platform Options from the environment, confine, and answer with **what the
    /// confinement applied** ([`Confined`]).
    ///
    /// - **Linux** `InPlace` → a [`Confined`] with no child, carrying the resolved model, the
    ///   exposure count and the network filter's outcome.
    /// - **macOS / Windows** `Supervise` → a [`Confined`] carrying the child; `InPlace` → refuse.
    /// - **Other targets** → [`Error::Unsupported`].
    ///
    /// # Errors
    ///
    /// Platform Options parsing, `Backend::confine`, and a launcher that answered `InPlace`.
    ///
    /// [`Error::Unsupported`]: crate::Error::Unsupported
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_sandbox::{AccessMode, serve};
    ///
    /// // Empty exposure set is refused by the backend, not by this builder.
    /// let _ = serve::Spec::new()
    ///     .expose("/srv/data", AccessMode::ReadWrite)
    ///     .apply();
    /// ```
    #[tracing::instrument(
        name = "tinio.sandbox.apply",
        target = "tinio::sandbox",
        level = "info",
        skip_all,
        fields(
            count = self.sandbox.exposures().len(),
            mechanism = field::Empty,
            outcome = field::Empty
        )
    )]
    pub fn apply(self) -> Result<Confined> {
        let result = apply_platform(self);
        // **One close line at default verbosity, and the outcome on it**: the plan's T0 rule
        // asks a refused launch to be visible as a refusal, and the error is the caller's to
        // print. The fields are a closed pair — an exposure count and `refused` — never a path.
        if result.is_err() {
            Span::current().record("outcome", "refused");
        }
        result
    }

    /// [`LaunchPlan`] from the optional facts this spec carries.
    fn into_plan(self) -> (Sandbox, LaunchPlan) {
        let mut plan = match self.config {
            Some(text) => LaunchPlan::for_launch(text),
            None => LaunchPlan::new(),
        };
        if let Some(listener) = self.listener {
            plan = plan.with_listener(listener);
        }
        if !self.controls.is_empty() {
            plan = plan.with_controls(self.controls);
        }
        if let Some(token) = self.token {
            plan = plan.with_token(token.expose_secret().to_string());
        }
        (self.sandbox, plan)
    }
}

/// What a [`Spec::apply`] confined — **the facts the confinement itself answered with**, so a
/// caller that must state them after the call (the CLI's in-place re-emit, whose subscriber is
/// installed *after* the confinement) reads them off this value rather than re-deriving them.
///
/// **Every fact here has one owner, and it is not the caller**: `child` is the launch, `model`
/// and `seccomp` are the backend's post-apply state (`LinuxBackend::resolved`/`seccomp` — the
/// same accessors `enter` writes), `mounts` is the mount plan that backend applied, and
/// `exposures`/`rendered_exposures` are the exposure set the confinement was handed and its one
/// rendering ([`describe_exposures`], composed while the set was in hand). The alternative shape
/// — a caller carrying a *request* in and re-stating it afterwards — is what made the CLI resolve
/// `TINIO_SANDBOX_MODEL` twice and name a platform type of its own.
///
/// **No field can carry a secret, and the one string is why that is argued rather than
/// asserted.** `rendered_exposures` is the exposure set's own rendering — host paths and their
/// access, composed by the core's renderer from the same value the confinement applied — so the
/// `Spec`'s token has no route into it: the string is not the spec's, and no other field is a
/// string at all.
///
/// # Examples
///
/// ```
/// # #[cfg(target_os = "linux")]
/// # fn main() {
/// use tinio_sandbox::{linux::Mechanism, serve::Confined};
///
/// // What an in-place confinement answers with: this process is the confined one.
/// let confined = Confined {
///     child: None,
///     exposures: 2,
///     mounts: 6,
///     rendered_exposures: String::from("/srv/data (read-write), /srv/state (read-write)"),
///     model: Mechanism::Root,
///     seccomp: None,
/// };
/// assert_eq!(confined.mounts, 6);
/// assert!(confined.rendered_exposures.contains("/srv/data"));
/// # }
/// # #[cfg(not(target_os = "linux"))]
/// # fn main() {
/// use tinio_sandbox::serve::Confined;
///
/// // A launcher arm answers with the child it spawned; its confinement applies the exposure set
/// // as grants rather than mounts, so the mount count is the jail's own zero.
/// let confined = Confined {
///     child: None,
///     exposures: 2,
///     mounts: 0,
///     rendered_exposures: String::from("/srv/data (read-write), /srv/state (read-write)"),
/// };
/// assert_eq!(confined.exposures, 2);
/// # }
/// ```
#[derive(Debug)]
pub struct Confined {
    /// The confined child, where this target launches one. `None` is the in-place arm: the
    /// calling process is the one that was confined, so there is no child to drive.
    pub child: Option<Child>,
    /// How many exposures the confinement applied — the [`Sandbox`]'s own set size, read where
    /// the confinement ran because `apply` consumed the spec that held it.
    pub exposures: usize,
    /// How many mounts the jail applied — the exposure binds plus the jail's own entries (its
    /// root, and the four device files on Linux), recorded by the backend that performed them
    /// (`LinuxBackend::mounts`) rather than re-planned here. **Zero on a launcher platform**,
    /// which applies its set as profile or ACL grants and mounts nothing.
    pub mounts: usize,
    /// The exposure set, rendered by the core's [`describe_exposures`] (`host (access)`,
    /// comma-joined) — §7's exposure event carries it under this name, and the in-place re-emit
    /// states it from here because that event is emitted before the arm's subscriber exists.
    /// Read off the same set the confinement was handed rather than re-derived afterwards.
    pub rendered_exposures: String,
    /// The mechanism the confinement resolved and applied — **`Auto` resolved against this
    /// kernel, never the request** (`LinuxBackend::resolved`'s answer, which the run-time gate
    /// asked the same question of).
    #[cfg(target_os = "linux")]
    pub model: Mechanism,
    /// What the network filter did (`LinuxBackend::seccomp`'s answer), or `None` where this
    /// target installs none.
    #[cfg(target_os = "linux")]
    pub seccomp: Option<Seccomp>,
}

/// Map a backend's [`Confinement`] into the facade's `Option<Child>` answer.
///
/// **Launcher platforms must supervise**; `InPlace` there means nothing was confined where the
/// run asked — fail closed (design §11). In-place platforms answer `None` for `InPlace` and
/// still surface a surprising `Supervise` as `Some(child)` so the caller can drive it.
pub(crate) fn map_confinement(confinement: Confinement) -> Result<Option<Child>> {
    match confinement {
        Confinement::Supervise(child) => Ok(Some(child)),
        Confinement::InPlace => {
            #[cfg(any(target_os = "macos", target_os = "windows"))]
            {
                Err(Error::Unsupported {
                    detail: LAUNCHER_IN_PLACE,
                })
            }
            #[cfg(not(any(target_os = "macos", target_os = "windows")))]
            {
                Ok(None)
            }
        }
    }
}

/// The `outcome` label for a backend's `confine` answer, read before the caller's `?` consumes
/// it so the family is recorded on both paths.
#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
fn outcome<T>(result: &Result<T>) -> metrics::Outcome {
    match result {
        Ok(_) => metrics::Outcome::Applied,
        Err(_) => metrics::Outcome::Refused,
    }
}

/// Record a `confine` call's outcome metric and the span's mechanism field together, so a
/// platform arm cannot write one and forget the other.
fn record_confinement(mechanism: metrics::Mechanism, outcome: metrics::Outcome, started: Instant) {
    metrics::record(mechanism, outcome, started.elapsed());
    Span::current().record("mechanism", mechanism.label());
}

/// Static detail for the launcher `InPlace` refusal — `Error::Unsupported` needs `'static`.
///
/// **One sentence, and the platform is not in it**: which platforms answer this is the `cfg` on
/// the arm that raises it, so a per-platform spelling was a second home for the same fact — and
/// since each `cfg` only ever compiles alone, a divergence between the two was invisible to
/// either build.
#[cfg(any(target_os = "macos", target_os = "windows"))]
const LAUNCHER_IN_PLACE: &str = "this platform's backend must supervise its child rather than \
                                 confine in place: nothing was confined, so the run is refused";

#[cfg(target_os = "linux")]
fn apply_platform(spec: Spec) -> Result<Confined> {
    use crate::{
        _linux::{Backend as LinuxBackend, Options},
        Backend,
    };

    let local_uid = spec.local_uid;
    let (sandbox, plan) = spec.into_plan();
    // **The device nodes are this platform's own, not the caller's choice**: alongside
    // `local_uid`, they are what a Linux jail needs to be a jail, so they are asked for here
    // rather than carried on the business-agnostic builder — where the only caller that could
    // set them always did, and a caller that did not got a silent no-op.
    //
    // **The model is the environment's own read, and the only one.** `Options::from_env` is the
    // variable's one reader (`TINIO_SANDBOX_MODEL`); the spec carries no model and no caller
    // states a request, because the *resolved* mechanism travels back on the answer instead
    // (`Confined::model`, read off the backend below).
    let options = Options::from_env()?
        .with_device_files(true)
        .with_local_uid(local_uid);
    // **The request, kept for the refusal taken before a model is resolved.** `Auto` becomes one
    // of the two models inside `enter`, so the mechanism a refusal names is the resolved one when
    // the resolution happened and the request when validation refused first.
    let requested = options.model();
    let mut backend = LinuxBackend::with_options(options);
    // **The one call families 19-20 time, and the one place this span learns its mechanism**: the
    // answer is read after the call because `Auto` resolves inside it, and the record cannot
    // depend on a subscriber — the confinement is the first thing a run does.
    let started = Instant::now();
    let confinement = backend.confine(&sandbox, &plan);
    let mechanism = metrics::Mechanism::from(backend.resolved().unwrap_or(requested));
    record_confinement(mechanism, outcome(&confinement), started);
    let child = map_confinement(confinement?)?;
    // **The facts are the backend's, read after the confinement — never re-derived here.** The
    // resolved mechanism is `Some` by construction after a successful confine (`enter` records it
    // the moment the resolution answers, and an unavailable one is refused before that), so a
    // `None` is an answer this facade cannot carry and is refused rather than assumed. The
    // seccomp skip that this arm used to `eprintln!` now travels on the answer: the caller that
    // installs a subscriber *after* this call is the one that reports §7's record, and it reports
    // the degradation once, from this outcome.
    //
    // **The exposure facts travel the same way, and the set is the one this call acted on**: the
    // mount count is the plan the backend applied (`LinuxBackend::mounts`), and the rendered list
    // is [`describe_exposures`] over `sandbox.exposures()` — the very value handed to `confine`
    // above, so the re-emit states the paths that were mounted rather than a second answer
    // derived after the spec is gone.
    let Some(model) = backend.resolved() else {
        return Err(Error::Unsupported {
            detail: "the confinement answered without resolving a mechanism: nothing was \
                     confined, so the run is refused",
        });
    };
    Ok(Confined {
        child,
        exposures: sandbox.exposures().len(),
        mounts: backend.mounts(),
        rendered_exposures: describe_exposures(sandbox.exposures()),
        model,
        seccomp: backend.seccomp(),
    })
}

#[cfg(target_os = "macos")]
fn apply_platform(spec: Spec) -> Result<Confined> {
    use crate::{
        _macos::{Backend as MacosBackend, Options},
        Backend,
    };

    let (sandbox, plan) = spec.into_plan();
    let options = Options::from_env()?;
    // The one line that records the choice before anything acts on it: an operator reading a
    // failed startup has to tell which mechanism ran, and a `debug!` would be invisible under
    // `serve`'s fixed Info level.
    tracing::info!(
        step = "mechanism",
        mechanism = %options.mechanism(),
        "the macOS confinement mechanism is resolved"
    );
    // The mechanism this launch applies is the one `from_env` resolved: this platform has no
    // `Auto`, so the request and the answer are one value.
    let requested = options.mechanism();
    let mut backend = MacosBackend::with_options(options);
    // Families 19-20, timed around the one call that confines — the Linux arm's comment states
    // why the mechanism is known on both paths.
    let started = Instant::now();
    let confinement = backend.confine(&sandbox, &plan);
    let mechanism = metrics::Mechanism::from(requested);
    record_confinement(mechanism, outcome(&confinement), started);
    Ok(Confined {
        child: map_confinement(confinement?)?,
        exposures: sandbox.exposures().len(),
        // **No mounts on this arm**: the profile grants the set rather than binding it, so the
        // count is the jail's own zero while the rendered list is the value the confinement was
        // handed — the set the launch granted, in the one rendering `describe_exposures` owns.
        mounts: 0,
        rendered_exposures: describe_exposures(sandbox.exposures()),
    })
}

#[cfg(target_os = "windows")]
fn apply_platform(spec: Spec) -> Result<Confined> {
    use std::env;

    use crate::{
        _windows::{Backend as WindowsBackend, Capability, Options},
        Backend,
    };

    let (sandbox, plan) = spec.into_plan();
    let options = Options {
        capabilities: Capability::parse_list(
            env::var("TINIO_SANDBOX_CAPABILITIES").ok().as_deref(),
        )?,
        ..Default::default()
    };
    let mut backend = WindowsBackend::with_options(options);
    // Families 19-20, timed around the one call that launches — the AppContainer is this
    // platform's only mechanism, so both paths name it.
    let started = Instant::now();
    let confinement = backend.confine(&sandbox, &plan);
    let mechanism = metrics::Mechanism::AppContainer;
    record_confinement(mechanism, outcome(&confinement), started);
    Ok(Confined {
        child: map_confinement(confinement?)?,
        exposures: sandbox.exposures().len(),
        // The same two facts as the macOS arm's, for the same reason: the capability set's ACL
        // grants are not mounts, and the rendered list is the set this launch was handed.
        mounts: 0,
        rendered_exposures: describe_exposures(sandbox.exposures()),
    })
}

/// No backend at all: a target this crate was built for without one is a refusal, not a
/// silently unconfined run.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn apply_platform(spec: Spec) -> Result<Confined> {
    let _ = spec;
    // The refusal is still an attempt, so the families and the span say so: a target with no
    // backend has no mechanism to name, which is the `none` label.
    let started = Instant::now();
    let mechanism = metrics::Mechanism::None;
    record_confinement(mechanism, metrics::Outcome::Refused, started);
    Err(Error::Unsupported {
        detail: "no confinement backend is wired for this target",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_builder_records_exposures_and_knobs() {
        let spec = Spec::new()
            .expose("/srv/data", AccessMode::ReadWrite)
            .expose("/srv/state", AccessMode::ReadWrite)
            .config_text("port = 9000")
            .local_uid(true);
        assert_eq!(spec.sandbox().exposures().len(), 2);
        assert_eq!(spec.config(), Some("port = 9000"));
        assert!(spec.local_uid_configured());
    }

    /// **The apply answer carries the platform's own facts, and this leg is where the deleted
    /// model knob's contract moved.** The old leg asserted that a *carried request* answered back
    /// out; nothing carries a request any more — [`Confined`] is the confinement's own answer —
    /// so what this pins is that the Linux answer is the whole of what a caller states the run
    /// with: the exposure count and its rendering, the mount count, the resolved mechanism and
    /// the filter's outcome, none of them a value the caller fed in.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_apply_answer_carries_the_platforms_own_facts() {
        let confined = Confined {
            child: None,
            exposures: 2,
            mounts: 6,
            rendered_exposures: String::from("/srv/data (read-write), /srv/state (read-write)"),
            model: Mechanism::UserNamespace,
            seccomp: Some(Seccomp::Installed),
        };
        assert_eq!(confined.exposures, 2);
        assert_eq!(confined.mounts, 6);
        assert!(
            confined
                .rendered_exposures
                .contains("/srv/data (read-write)")
        );
        assert_eq!(confined.model, Mechanism::UserNamespace);
        assert_eq!(confined.seccomp, Some(Seccomp::Installed));
    }

    #[test]
    fn from_sandbox_keeps_the_exposure_set() {
        let sandbox = Sandbox::new().expose("/srv/data", AccessMode::ReadOnly);
        let spec = Spec::from_sandbox(sandbox).config_text("");
        assert_eq!(spec.sandbox().exposures().len(), 1);
        assert_eq!(spec.config(), Some(""));
    }

    #[test]
    fn map_confinement_answers_none_for_in_place_on_this_host() {
        #[cfg(any(target_os = "macos", target_os = "windows"))]
        {
            let err = map_confinement(Confinement::InPlace).expect_err("launcher");
            assert!(err.to_string().contains("must supervise"), "{err}");
        }
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        {
            assert!(matches!(map_confinement(Confinement::InPlace), Ok(None)));
        }
    }
}
