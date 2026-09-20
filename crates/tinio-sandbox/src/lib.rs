//! Process confinement for tinio — the facade.
//!
//! Re-exports [`tinio_sandbox_core`] and, for the platform this build targets,
//! the one backend that implements it. Consumers name this crate and nothing
//! else; the platform split stays behind the selection below.
//!
//! The three platform crates depend on the core, so a facade that the core or
//! a platform crate depended on would be a cycle — which is why the layering
//! is three deep and not two (design decision 3).
//!
//! # Examples
//!
//! ```
//! use tinio_sandbox::{AccessMode, Sandbox};
//!
//! let sandbox = Sandbox::new().expose("/srv/tinio", AccessMode::ReadWrite);
//! assert_eq!(sandbox.exposures().len(), 1);
//! ```

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub extern crate tinio_sandbox_linux as _linux;
#[cfg(target_os = "macos")]
#[doc(hidden)]
pub extern crate tinio_sandbox_macos as _macos;
#[cfg(target_os = "windows")]
#[doc(hidden)]
pub extern crate tinio_sandbox_windows as _windows;

// **Exactly the core's public surface, name for name, and that equality is the
// rule rather than an accident.** A consumer of this facade is the `serve`
// example and the platform crates; none of them implements `Backend` or
// constructs a platform's `Options`, so "what the consumer needs" and "what the
// core publishes" are the same set. Anything narrower would have to be
// re-derived every time a consumer appears, and would drift silently until a
// platform crate failed to build.
//
// The list is kept in the core's grouping (vocabulary, then launcher) so the two
// can be compared by eye, and Task 3's `the_launcher_surface_is_re_exported`
// binds every name it lists — so a name that stops resolving, or that changes
// shape, is a compile error in that test rather than a discovery three phases
// later. A name *added* to the core is still this list's to gain by hand: see
// the comment on that test.
pub use crate::_core::{
    AccessMode, Adopted, Backend, Child, Confinement, Error, Event, Exposure, LISTENER_FD_PREFIX,
    LaunchPayload, LaunchPlan, Observer, Result, Sandbox, Terminal, adopt, is_listener_fd_arg,
    listener_fd_arg, listener_fd_from_arg, supervise,
};
/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "linux")]
pub use crate::_linux::{Backend as Platform, backend as platform};
/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "macos")]
pub use crate::_macos::{Backend as Platform, backend as platform};
/// The backend for the platform this build targets, and the constructor for it.
///
/// # Examples
///
/// ```
/// let backend: tinio_sandbox::Platform = tinio_sandbox::platform();
/// let _ = backend;
/// ```
#[cfg(target_os = "windows")]
pub use crate::_windows::{Backend as Platform, backend as platform};
