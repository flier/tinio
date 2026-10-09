//! Linux confinement.
//!
//! The process is confined in place: `unshare` plus `pivot_root` apply to the
//! calling process, so there is no child, no payload and no exit-status
//! forwarding (design decision 6).
//!
//! **The process must be single-threaded when it confines itself, and the
//! backend refuses one that is not rather than assuming the entry point got the
//! ordering right.** Every operation of the sequence — `unshare`,
//! `no_new_privs`, the capability drop and the seccomp filter — applies to the
//! calling task alone, so a second thread would keep the host mount namespace,
//! the full credentials and an unfiltered network for the life of the process
//! while the run reported itself confined (design §8, §10; the design's own
//! §10 wiring is what used to carry this, and a subscriber built before
//! `confine` is the shape that breaks it). [`Backend`](tinio_sandbox_core::Backend)'s `confine` reads
//! `/proc/self/task` and refuses with the count before the first irreversible
//! step, and the refusal is fail-closed: a `/proc` that cannot be read refuses
//! too. The remedy belongs to the entry point — build the subscriber and the
//! runtime *after* the confinement — and not to
//! `SECCOMP_FILTER_FLAG_TSYNC`, which would synchronise the filter while the
//! namespaces and the credentials stayed per-thread.
//!
//! **The crate is behind `#![cfg(target_os = "linux")]`** so the workspace
//! builds on every host. `rustix` itself compiles everywhere — it ships no
//! crate-level gate and a Windows build simply links a different backend — but
//! its `mount`/`fs`/`process`/`thread` modules are configured out on Windows,
//! so the first line of *this* crate's code that names one of them is an
//! `E0433: cannot find … in rustix`. The gate makes the crate an empty library
//! elsewhere, which is what the facade already assumes when it re-exports this
//! crate under `#[cfg(target_os = "linux")]`. **Every separate target in the
//! crate repeats the gate** (`src/bin/`, `tests/`, `examples/`): a crate-level
//! `#![cfg]` does not cover them.

#![cfg(target_os = "linux")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use crate::_core::{
    Backend as BackendTrait, Confinement, Diagnostics, Enforcement, LaunchPlan, PlatformFacts,
    Result, Sandbox,
};

pub mod caps;
pub mod enter;
pub mod jail;
pub mod mechanism;
pub mod options;
pub mod plan;
pub mod seccomp;

/// The two families this crate records into the process-wide prometheus default registry — the
/// read-only arm an exposure took and the seccomp skip — **internal to the crate**: nothing
/// outside it reads or writes them, and the exposition they land in is the management plane's
/// (`/metrics` gathers the same default registry this process registered into; the module's own
/// doc carries that argument).
pub(crate) mod metrics;

/// The crate's test-side support, in one place: this platform's host
/// precondition and the two shared recording devices.
///
/// **Behind the non-default `testing` feature** (`docs/tests.md`'s rule for a
/// crate's own harness module, and the shape `tinio-sandbox-windows` takes): the
/// module has to be lib code to cross the `tests/` boundary at all, and the
/// feature is what keeps it out of a build that is not a test build. The crate's
/// own test targets turn it on through the self dev-dependency in `Cargo.toml`,
/// and `tinio-server`'s Linux e2e target asks for it from its own edge.
#[cfg(any(test, feature = "testing"))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: behind `testing` (docs/style.md §Panics, design 2026-10-09 D7)"
)]
pub mod testing;

pub use self::{
    caps::{Cap, retained},
    jail::{jail_root, mirror_under},
    mechanism::{Mechanism, Resolved, userns_available},
    options::Options,
    plan::{DEVICE_FILES, Mount, MountKind, OverlayDirs, plan},
    seccomp::Seccomp,
};

/// What this mechanism does, for a caller that branches on the fact rather than the OS.
///
/// **The in-place answer**: this backend confines the calling process (design decision 6), so it
/// launches nothing and none of the two facts a *launch* owns are its to state.
pub const FACTS: PlatformFacts = PlatformFacts::NONE;

/// How much of its guarantee this backend's mechanism applies — **the strength the mechanism
/// answers for**, published beside [`FACTS`] so the crate that owns the mechanism is the crate
/// that answers for it (the rule [`PlatformFacts`]'s own doc states).
///
/// **`Full`, and what that covers**: the user namespace and the mount namespace this process
/// `unshare`s for itself, the mirror plus `pivot_root` that is the mount view, the capability
/// drop, and the seccomp filter on top (design decisions 6, 12, 13) — the same set
/// [`Enforcement::Full`]'s own doc names, and the whole of it. **There is no PID, IPC or UTS
/// namespace here**, which is design decision 12 rather than a gap in this list — v1 mounts no
/// `/proc` in the jail, a `/proc` without a PID namespace of its own being the host's process
/// table handed in. It is not a claim that every syscall a confined process could make is
/// filtered either, which is `docs/sandbox.md`'s to state.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Enforcement;
///
/// assert_eq!(
///     tinio_sandbox_linux::DIAGNOSTICS.enforcement,
///     Enforcement::Full
/// );
/// ```
pub const DIAGNOSTICS: Diagnostics = Diagnostics {
    enforcement: Enforcement::Full,
};

/// The Linux backend.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Backend;
///
/// let backend = Backend::default();
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: Options,
    /// What the last `confine` did about the network filter, written through the
    /// trait's `&mut self` receiver once the filter has been attempted — the
    /// value cannot be known before that.
    ///
    /// The alternative was to carry it in `Confinement`, which is P0's type and
    /// would then drag a Linux-only fact into both launcher platforms; or to
    /// return it from `confine`, whose return type the trait fixes.
    /// A caller that wants the answer — and `serve` does, to warn — reads it
    /// here after confining, which the `&mut self` receiver makes ordinary
    /// rather than something to work around with a `Cell`.
    seccomp: Option<Seccomp>,
    /// The mechanism the last `confine` resolved, written the moment the
    /// resolution answers and read the same way [`Backend::seccomp`] is —
    /// through the `&mut self` receiver, after the call.
    ///
    /// **`Auto` is why this is stored rather than re-derived**: the resolution
    /// probes the kernel inside `confine`, so a caller that asked the table
    /// again afterwards would take a second confinement-time decision outside
    /// the confinement. `tinio-sandbox`'s `serve::Spec::apply` carries it back
    /// as part of the confinement's own answer (the §7 record a caller re-emits
    /// where its subscriber is installed after the confinement), and
    /// `Resolved::Unavailable` never reaches this field — the resolution is
    /// refused before it would be recorded.
    resolved: Option<Mechanism>,
    /// How many mounts the last `confine` applied, written where they landed and
    /// read the same way the two fields above are.
    ///
    /// **The plan's own length, recorded rather than re-derived.** The table is
    /// built inside the confinement ([`crate::plan`]) and applying it is what
    /// makes the jail; a caller that wanted the count afterwards — the facade's
    /// `Confined::mounts`, which the CLI's in-place re-emit states beside the
    /// exposure set's size — would otherwise have to re-plan the jail, a second
    /// answer to what was mounted. It is the number `announce_exposures` logs.
    mounts: usize,
}

impl Backend {
    /// A backend with explicit options.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::{Backend, Mechanism, Options};
    ///
    /// let backend = Backend::with_options(Options::default().with_model(Mechanism::Root));
    /// let _ = backend;
    /// ```
    pub fn with_options(options: Options) -> Self {
        Self {
            options,
            seccomp: None,
            resolved: None,
            mounts: 0,
        }
    }

    /// What the last [`Backend::confine`] did about the seccomp filter, or
    /// `None` if it has not been called.
    ///
    /// The one degradation in the Linux backend is an architecture with no
    /// syscall table, and this is where the caller finds out (design §11): a
    /// directory boundary with no network cut is not the guarantee that was
    /// asked for, so it is *returned* rather than left for someone to notice.
    /// It is also **logged**, with a `warn!` at the point `install` decides it,
    /// because a process that has a subscriber should not have to read a field
    /// to learn that its network is open — the two are not in tension, and
    /// `Seccomp`'s own doc says why.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Backend;
    ///
    /// assert!(
    ///     Backend::default().seccomp().is_none(),
    ///     "nothing confined yet"
    /// );
    /// ```
    pub fn seccomp(&self) -> Option<Seccomp> {
        self.seccomp
    }

    /// The mechanism the last [`Backend::confine`] resolved and applied, or
    /// `None` if it has not been called — the [`Backend::seccomp`] shape for
    /// the model the resolution chose.
    ///
    /// **The resolved answer, never a second derivation of it.** `Auto` becomes
    /// one of the two mechanisms against a live probe of this kernel (and, for
    /// a forced model, may be refused outright); asking the table again after
    /// the call would be a second confinement-time decision taken outside the
    /// confinement, on a host state that may have moved. The value is written
    /// when the resolution answers, so a caller reads the model the run really
    /// applied.
    ///
    /// **What it carries: the serve-spec seam's §7 record**
    /// (`docs/superpowers/specs/2026-09-27-sandbox-servespec-seam-design.md`) —
    /// the *resolved* mechanism `tinio-sandbox`'s `serve::Spec::apply` hands back
    /// on `Confined::model`, so the caller that re-emits the confinement's own
    /// record after the subscriber exists states the model that ran.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Backend;
    ///
    /// assert!(
    ///     Backend::default().resolved().is_none(),
    ///     "nothing confined yet"
    /// );
    /// ```
    pub fn resolved(&self) -> Option<Mechanism> {
        self.resolved
    }

    /// How many mounts the last [`Backend::confine`] applied, or `0` if it has
    /// not been called — the mount plan's own length, recorded where the mounts
    /// landed.
    ///
    /// **What it carries: the serve-spec seam's §7 record**
    /// (`docs/superpowers/specs/2026-09-27-sandbox-servespec-seam-design.md`) —
    /// the `mounts` field of the exposure event that record re-emits, which
    /// `tinio-sandbox`'s `serve::Spec::apply` hands back on `Confined::mounts`.
    /// A caller that re-planned the jail for the number would be answering what
    /// was mounted a second time.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Backend;
    ///
    /// assert_eq!(Backend::default().mounts(), 0, "nothing confined yet");
    /// ```
    pub fn mounts(&self) -> usize {
        self.mounts
    }
}

impl BackendTrait for Backend {
    fn confine(&mut self, sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        // Linux confines in place: it never launches a child, so the launch
        // plan is unused here (design decision 6).
        //
        // **The clone is the borrow checker's answer, not a preference.**
        // `self.enter(sandbox, &self.options)` is `E0502`: `enter` takes
        // `&mut self`, and the shared borrow of the field is still live when
        // the receiver's two-phase reservation is activated.
        let options = self.options.clone();
        self.enter(sandbox, &options)
    }
}

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_linux::backend();
/// let _ = backend;
/// ```
pub fn backend() -> Backend {
    Backend::default()
}
