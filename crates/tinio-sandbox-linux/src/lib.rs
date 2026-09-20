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

use std::io;

use crate::_core::{Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox};

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
pub struct Backend;

impl BackendTrait for Backend {
    fn confine(&mut self, sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        if sandbox.exposures().is_empty() {
            // A request with nothing in it is the caller's bug, not the
            // platform's shortcoming: `Unsupported` would tell the operator to
            // go look at their kernel, which is the wrong place (design §11).
            return Err(Error::Step {
                step: "confine",
                source: io::Error::other("nothing exposed to confine to"),
            });
        }
        // Filled in by P1 (`2026-09-18-sandbox-p1-linux.md`), which replaces
        // this struct with one carrying `LinuxOptions`.
        Err(Error::Unsupported {
            detail: "the Linux jail is not built yet",
        })
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
    Backend
}

/// Which confinement mechanism Linux uses (design decision 7).
///
/// **Complete here, not in P1.** P1's mount-plan unit tests build a plan for
/// every variant, so a one-variant placeholder would leave that task's tests
/// referencing names that do not compile until a later task defines them — a
/// compile-order defect across this plan's own boundary. Nothing outside
/// `tinio-sandbox-linux` reads this type; it is a platform crate's vocabulary
/// that simply happens to be cheap to spell in full. What P1 adds is the
/// *behaviour* — `Resolved`, `from_env`, `resolved()` and the availability
/// probe — not the variants.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Mechanism;
///
/// assert_eq!(Mechanism::default(), Mechanism::Auto);
/// assert_ne!(Mechanism::Root, Mechanism::UserNamespace);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mechanism {
    /// Prefer the user namespace; resolve the fallback at confinement time.
    #[default]
    Auto,
    /// Force `unshare(CLONE_NEWUSER | CLONE_NEWNS)`.
    UserNamespace,
    /// Force `unshare(CLONE_NEWNS)` with `CAP_SYS_ADMIN`.
    Root,
}
