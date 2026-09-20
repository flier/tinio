//! macOS confinement via Seatbelt. The stub exists so the launcher-shaped half
//! of the `Backend` contract is exercised from here onward; the implementation
//! lands with P3 (`docs/superpowers/plans/2026-09-18-sandbox-p3-macos.md`).
//!
//! Behind `#![cfg(target_os = "macos")]` so the workspace builds on every host:
//! `members = ["crates/*"]` has no per-target form, so this crate compiles — and
//! lints — on targets it does not serve. **P3 replaces this stub and may drop
//! the line**: that plan's shape is a crate which compiles everywhere with each
//! macOS-specific item gated internally, and both shapes satisfy the same rule.
//! Separate targets repeat whichever gate they need.

#![cfg(target_os = "macos")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use crate::_core::{Backend as BackendTrait, Confinement, Error, LaunchPlan, Result, Sandbox};

/// The macOS backend.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::Backend;
///
/// let backend = Backend::default();
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend;

impl BackendTrait for Backend {
    fn confine(&mut self, _sandbox: &Sandbox, _plan: &LaunchPlan) -> Result<Confinement> {
        Err(Error::Unsupported {
            detail: "the macOS backend is not implemented yet",
        })
    }
}

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_macos::backend();
/// let _ = backend;
/// ```
pub fn backend() -> Backend {
    Backend
}
