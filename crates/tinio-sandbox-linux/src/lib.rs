//! Linux confinement.
//!
//! The process is confined in place: `unshare` plus `pivot_root` apply to the
//! calling process, so there is no child, no payload and no exit-status
//! forwarding (design decision 6).
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

use crate::_core::{Backend as BackendTrait, Confinement, LaunchPlan, Result, Sandbox};

pub mod caps;
pub mod enter;
pub mod jail;
pub mod mechanism;
pub mod options;
pub mod plan;
pub mod seccomp;

pub use self::{
    caps::{Cap, retained},
    jail::{jail_root, mirror_under},
    mechanism::{Mechanism, Resolved, userns_available},
    options::Options,
    plan::{DEVICE_FILES, Mount, MountKind, OverlayDirs, plan},
    seccomp::Seccomp,
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
    /// What the last `confine` did about the network filter. Interior
    /// mutability because `Backend::confine` takes `&self` (P0's trait) and the
    /// value can only be known once the filter has been attempted.
    ///
    /// The alternative was to carry it in `Confinement`, which is P0's type and
    /// would then drag a Linux-only fact into both launcher platforms; or to
    /// return it from `confine`, whose return type the trait fixes.
    /// A caller that wants the answer — and `serve` does, to warn — reads it
    /// here after confining, which the `&mut self` receiver makes ordinary
    /// rather than something to work around with a `Cell`.
    seccomp: Option<Seccomp>,
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
