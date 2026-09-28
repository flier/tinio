//! Business-agnostic confine entry: paths, bound sockets, token — no product types.

use std::{net::TcpListener, path::PathBuf};

use secrecy::ExposeSecret;

// **`Error` is named only by the arms that build a refusal**, and every one of them is
// non-Linux: `apply_platform`'s other-target arm and `map_confinement`'s launcher-platform arm.
// A Linux build reaches its failures through `?` and never spells the type, so the import
// carries their cfg — and `Spec::apply`'s doc links the variant by full path, the way it names
// an item no target imports.
#[cfg(not(target_os = "linux"))]
use crate::Error;
use crate::{
    _core::{Secret, SecretKey},
    AccessMode, Child, Confinement, ControlHandle, LaunchPlan, Result, Sandbox,
};

/// How a caller asks this process (or a child) to be confined — builder in, [`Option<Child>`] out.
///
/// **Inputs are sandbox vocabulary only**: exposures, config text, an already-bound listener,
/// control handles, a token string, and Linux knobs. No clap, config keys, or management-plane
/// types (design `2026-09-27-sandbox-servespec-seam-design.md`).
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

    /// Build platform Options from the environment, confine, and map the shape.
    ///
    /// - **Linux** `InPlace` → `Ok(None)`; prints a stderr warning when seccomp was skipped.
    /// - **macOS / Windows** `Supervise` → `Ok(Some(child))`; `InPlace` → refuse.
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
    pub fn apply(self) -> Result<Option<Child>> {
        apply_platform(self)
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
fn apply_platform(spec: Spec) -> Result<Option<Child>> {
    use crate::{
        _linux::{Backend as LinuxBackend, Options, Seccomp},
        Backend,
    };

    let local_uid = spec.local_uid;
    let (sandbox, plan) = spec.into_plan();
    // **The device nodes are this platform's own, not the caller's choice**: alongside
    // `local_uid`, they are what a Linux jail needs to be a jail, so they are asked for here
    // rather than carried on the business-agnostic builder — where the only caller that could
    // set them always did, and a caller that did not got a silent no-op.
    let options = Options::from_env()?
        .with_device_files(true)
        .with_local_uid(local_uid);
    let mut backend = LinuxBackend::with_options(options);
    let confinement = backend.confine(&sandbox, &plan)?;
    let child = map_confinement(confinement)?;
    if child.is_none() && backend.seccomp() == Some(Seccomp::SkippedUnsupportedArch) {
        // The backend also emitted a `warn!` through the subscriber; this line is for whoever
        // watches a startup rather than a log (stdout is the event channel).
        eprintln!(
            "warning: this architecture has no seccomp table, so outbound \
             connections are NOT blocked; the directory confinement still holds"
        );
    }
    Ok(child)
}

#[cfg(target_os = "macos")]
fn apply_platform(spec: Spec) -> Result<Option<Child>> {
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
    let mut backend = MacosBackend::with_options(options);
    let confinement = backend.confine(&sandbox, &plan)?;
    map_confinement(confinement)
}

#[cfg(target_os = "windows")]
fn apply_platform(spec: Spec) -> Result<Option<Child>> {
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
    let confinement = backend.confine(&sandbox, &plan)?;
    map_confinement(confinement)
}

/// No backend at all: a target this crate was built for without one is a refusal, not a
/// silently unconfined run.
#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
fn apply_platform(spec: Spec) -> Result<Option<Child>> {
    let _ = spec;
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
