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
#[doc(hidden)]
pub extern crate tinio_util as _util;

pub mod backend;
pub mod enforcement;
mod error;
pub mod event;
pub mod launcher;
/// The launcher's two families — the readiness answer and the child's exit outcome — recorded
/// where the wait answers and read from the process-global registry the management plane renders.
mod metrics;
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
// **The fork probe's three names, published only where a probe can exist.** The
// handshake is a unix one — it is built on a `fork`, a pipe and a `waitpid` — so
// the list above, which every build has to compile, cannot carry them. This is
// the same rule the facade is edited under: its re-export of these three names
// is gated the same way, so "exactly this surface, name for name" stays true on
// each target rather than only on the one this file was edited on.
#[cfg(unix)]
pub use self::launcher::{ProbeSteps, await_verdict, report_verdict};
pub use self::{
    backend::{Backend, Confinement, PlatformFacts},
    enforcement::{Attribution, Diagnostics, Enforcement, Refused, SECCOMP_SKIP_WARNING},
    error::{
        Error, LINUX_MODEL_USAGE, MACOS_MECHANISM_USAGE, Result, WINDOWS_CAPABILITY_USAGE, fail,
        launch, refused, step,
    },
    event::Event,
    launcher::{
        Adopted, CONTROL_FD_PREFIX, Child, ControlHandle, LISTENER_FD_PREFIX, LaunchPayload,
        LaunchPlan, Observer, Readiness, SandboxSpec, Terminal, adopt, control_fd_arg,
        control_fd_from_arg, filter_argv, is_control_fd_arg, is_launcher_fd_arg,
        is_listener_fd_arg, listener_fd_arg, listener_fd_from_arg, supervise,
    },
    sandbox::{Exposure, Sandbox, describe_exposures, describe_exposures_iter},
};
pub use crate::_core::AccessMode;
/// Shared secret box — used by [`crate`]'s consumers for token fields; not part of the
/// confinement vocabulary re-exported by the facade.
#[doc(hidden)]
pub use crate::_util::secret::{Secret, SecretKey};
