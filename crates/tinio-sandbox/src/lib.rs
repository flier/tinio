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
// rule rather than an accident.** The consumers are `crates/tinio-cli` (the
// serving process, which reaches this crate through the `sandbox` feature its
// manifest declares) and `crates/tinio-server`; neither implements `Backend` or
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
/// The child-side listener rebuild, under the platform-neutral name the shared
/// child path drives.
///
/// **The name is what the `#[cfg]` is for**: the CLI's serving path gates its
/// child arm by `feature = "sandbox"` alone, so it names this one name on every
/// platform and the facade selects the implementation (design decision 2, §10). Nothing else
/// from this platform's `launch` module rides along — the launcher's own argv
/// construction and `disarm` have no shared call site — and the core's names,
/// `adopt` among them, are already re-exported above rather than here.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::{Adopted, Error, adopted_listener};
///
/// # fn main() -> Result<(), Error> {
/// let adopted = Adopted::new(String::new(), Some(1040), Vec::new(), None);
/// // The child serves on the socket the launcher bound; it never binds one.
/// let listener = adopted_listener(&adopted)?;
/// # let _ = listener;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "windows")]
pub use tinio_sandbox_windows::launch::listener as adopted_listener;
/// The child-side stop hook: where a stop the launcher forwards lands, under the
/// platform-neutral name the shared child path drives (design §3 step 9,
/// decision 20's second half).
///
/// **This is the second of the two names the child side needs, and the pair is
/// the whole of it.** `adopted_listener` above reconstructs the socket the
/// launcher bound; this is what the child arms so that a forwarded stop gives it
/// somewhere to drain from rather than the default disposition, which is to die
/// where it stands. Both are named by the CLI's confined child, which is gated by the
/// feature alone and can name no platform crate — so the facade is where each
/// call that path makes gets one `#[cfg]`-selected spelling.
///
/// **`disarm` deliberately does not ride along.** It is for a process that
/// outlives one launch — the test harness, a CLI that serves twice — and no
/// shared call site reaches it, which is the line the shared surface draws
/// between the two names: `tinio_sandbox::windows::control::disarm` is where a
/// caller that has already written a Windows-only branch finds it.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::on_stop;
///
/// # fn main() -> Result<(), tinio_sandbox::Error> {
/// // The hook runs on a thread of its own, so what it does may allocate and
/// // take locks: starting the runtime's shutdown here is what it is for.
/// on_stop(|| {
///     let _ = ();
/// })?;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "windows")]
pub use tinio_sandbox_windows::launch::on_stop;

pub use crate::_core::{
    AccessMode, Adopted, Backend, CONTROL_FD_PREFIX, Child, Confinement, Error, Event, Exposure,
    LISTENER_FD_PREFIX, LaunchPayload, LaunchPlan, Observer, Result, Sandbox, Terminal, adopt,
    control_fd_arg, control_fd_from_arg, fail, is_control_fd_arg, is_listener_fd_arg, launch,
    listener_fd_arg, listener_fd_from_arg, refused, step, supervise,
};
/// The Linux backend's own crate, for the settings the shared surface does not
/// carry — its `Options` (the model and the `tmpdir` knob) and its `Seccomp`
/// answer.
///
/// **A consumer names this facade and nothing else**, which is the rule the
/// module doc above states and which a consumer that reached
/// `tinio-sandbox-linux` by name would break: by the time all three arms of
/// the CLI's serving path exist, such a consumer would need one target-gated
/// dependency per platform while every arm lives in the same function. The
/// re-export does not widen the *shared* surface — these are still the
/// platform's own types in the platform's own crate, reached through a
/// `#[cfg]` the consumer would have written anyway.
///
/// # Examples
///
/// ```
/// let options = tinio_sandbox::linux::Options::default();
/// assert!(!options.local_uid_configured());
/// ```
#[cfg(target_os = "linux")]
pub use crate::_linux as linux;
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
/// The Windows backend's own crate, for the settings the shared surface does not
/// carry — its `Options`, and the capabilities that value holds — which a
/// consumer writes inside the `#[cfg(target_os = "windows")]` block it would have
/// written anyway.
///
/// **The re-export does not widen the shared surface**: these are still the
/// platform's own types in the platform's own crate, reached through a `#[cfg]`
/// the consumer already carries (design decision 2, §10) — the same shape the
/// Linux arm above takes for `linux`.
///
/// # Examples
///
/// ```
/// let options = tinio_sandbox::windows::Options::default();
/// assert!(options.capabilities.is_empty());
/// ```
#[cfg(target_os = "windows")]
pub use crate::_windows as windows;
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
