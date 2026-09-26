//! The confinement contract for tinio.
//!
//! [`Sandbox`] collects the host paths a confined process must reach and the
//! access each gets. Turning that into a jail is a platform's job: this crate
//! holds only what more than one platform needs — the vocabulary, the
//! [`Backend`] trait, and the launcher machinery the two child-launching
//! platforms share. The platform implementations live in
//! `tinio-sandbox-linux`, `-macos` and `-windows`, and the `tinio-sandbox`
//! facade re-exports the one that applies.
//!
//! See `design §1` in `docs/superpowers/specs/2026-09-18-sandbox-design.md`.
//!
//! # Examples
//!
//! ```
//! use tinio_sandbox_core::{AccessMode, Sandbox};
//!
//! let sandbox = Sandbox::new()
//!     .expose("/srv/tinio", AccessMode::ReadWrite)
//!     .expose("/srv/tinio/static", AccessMode::ReadOnly);
//!
//! assert_eq!(sandbox.exposures().len(), 2);
//! ```

#[doc(hidden)]
pub extern crate tinio_core as _core;

/// The test-support half of `tinio-util`, reached through an alias for the same
/// reason [`_core`](crate::_core) has one: `tests.md`'s convention is that
/// conformance helpers live in that crate's `testing` feature rather than in a
/// copy per suite, and this is the dev-dependency that makes it a name here
/// (`error.rs`'s capture sink is the one user — F32's `SharedBuf`, whose three
/// removed copies this crate held one of).
#[cfg(test)]
#[doc(hidden)]
pub extern crate tinio_util as _util;

pub mod backend;
mod error;
pub mod event;
pub mod launcher;
pub mod sandbox;
#[cfg(test)]
pub(crate) mod testing;

// **The whole public surface, grouped by where each name comes from.**
// Grouping is not decoration: a flat list is what let `AccessMode` drift into the
// `launcher` group in an earlier draft, where it does not exist — the failure
// reads as a missing name three phases later rather than as a wrong line here.
//
// `AccessMode` is the one name here that is not this crate's own: it is
// `tinio-core`'s write policy, re-exported so a consumer of the sandbox never
// has to know which crate the vocabulary came from, and so that a value the
// read-only work already produced is the value an exposure takes. It is
// therefore reached through the dependency's alias declared at the top of this
// file — `pub use crate::_core::AccessMode`, on its own line after the
// `self::{…}` group because `tinio-core` is a dependency and not one of this
// crate's modules.
//
// The facade re-exports exactly this list, name for name, so that a consumer
// naming `tinio_sandbox::` can reach everything the core publishes. Keeping
// that equality is a rule this file is edited under rather than something a
// test proves: `the_launcher_surface_is_re_exported` checks the names it lists,
// so a name added here still has to be added there by hand — see the comment on
// that test.
pub use self::{
    backend::{Backend, Confinement},
    error::{Error, Result, fail, launch, refused, step},
    event::Event,
    launcher::{
        Adopted, CONTROL_FD_PREFIX, Child, ControlHandle, LISTENER_FD_PREFIX, LaunchPayload,
        LaunchPlan, Observer, SandboxSpec, Terminal, adopt, control_fd_arg, control_fd_from_arg,
        is_control_fd_arg, is_launcher_fd_arg, is_listener_fd_arg, listener_fd_arg,
        listener_fd_from_arg, supervise,
    },
    sandbox::{Exposure, Sandbox},
};
pub use crate::_core::AccessMode;
