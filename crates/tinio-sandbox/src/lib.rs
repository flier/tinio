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

// The platform items each launcher crate answers, bound under one alias per platform so the
// facade's own bodies call a two-segment path: `clippy::absolute_paths` is `warn` for this
// crate, and a path written straight from the crate root is three segments or more. The
// `disarm` pair is not one alias for one body, because their answers differ — see
// [`disarm_signal_relay`]. The rest are the adopted names' platform halves, which the facade
// maps into the shared shape — see [`adopted_listener`].
#[cfg(target_os = "macos")]
use std::{net::TcpListener, os::unix::net::UnixListener};

#[cfg(target_os = "macos")]
use crate::_macos::launch::{control as macos_control, listener as macos_listener};
#[cfg(target_os = "macos")]
use crate::_macos::relay::on_forwarded_stop as macos_on_stop;

/// The child-side **control-channel** rebuild: the unix socket the launcher bound,
/// under the platform-neutral name the shared child path drives.
///
/// **The second of the two handoffs a macOS launch performs, and the same shape as
/// [`adopted_listener`]**: the profile grants the confined child no `network-bind`
/// at all, so a unix `bind()` inside the confinement is refused — the launcher
/// binds the control socket itself, clears its `FD_CLOEXEC`, and writes the
/// descriptor number into the child's argv, and this is the rebuild on the other
/// side. One socket, so one inherited descriptor; the channel is the child's
/// whole control plane, and the macOS wrapper in `tinio-cli` is the caller.
///
/// The name is the platform-neutral one for the same reason the listener's is
/// (design decision 2, §10): the child path can name no platform crate, so the
/// facade selects the implementation. Nothing here is re-exported on a target
/// that does not launch a child.
///
/// **The facade is where the platforms' shapes meet**, and this name carries the
/// same one return type as [`adopted_listener`] and [`on_stop`] for the reason
/// stated there: the platform crate answers its own error type (macOS's
/// `std::io::Result`, here and for the listener and the hook), and the facade maps
/// it into the shared [`Result`] so that `?` in a caller's function converts the
/// same way on every platform the caller builds for.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::{Adopted, Error, adopted_control};
///
/// let adopted = Adopted::new(String::new(), Some(3), vec![4], None, None);
/// let socket = adopted_control(&adopted)?;
/// # let _ = socket;
/// # Ok::<(), Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn adopted_control(adopted: &Adopted) -> Result<UnixListener> {
    macos_control(adopted).map_err(step("rebuild the control channel"))
}
/// The child-side listener rebuild, under the platform-neutral name the shared
/// child path drives.
///
/// **The macOS half of the pair above, and the same name for the same reason**:
/// each launcher platform publishes this one name, so the child path writes it
/// once and the implementation is selected by the build rather than by a branch
/// at the call site (design decision 2, §10). **The selection is a `#[cfg]` and
/// the call site that absorbs it is the CLI's**, so this name is *not* reachable
/// from a feature-only gate: it does not exist on Linux, where the confinement is
/// in place and no launcher hands a listener over. The name the child path
/// actually writes is `crates/tinio-cli/src/sandbox/mod.rs`'s own
/// `adopted_listener`, which exists on **every** target and in every build — that
/// module is compiled everywhere, because a capability that does not exist cannot
/// refuse — delegates here on a launcher platform, and refuses on a target whose
/// confinement never hands one over. Which of the two a given process is comes
/// from its own shape rather than from a feature: the CLI asks whether it was
/// launched at all (`adopted.is_some()`), and a child of one of these two
/// platforms is the only process for which that answer is yes.
/// What differs between the two launcher arms is where the socket
/// comes from. The macOS launcher binds with `std`, clears the descriptor's
/// close-on-exec flag so it survives `execve`, and this rebuild is what turns the
/// inherited descriptor back into a `TcpListener`; **on unix a `std` socket is
/// already non-blocking**, so unlike the Windows arm there is no mode to
/// correct after the conversion.
///
/// **The facade is where the platforms' shapes meet, and the three adopted names
/// are one rule because of it.** The two platform crates answer what their
/// mechanism returns — macOS's rebuild is a `BorrowedFd`/`OwnedFd` conversion and
/// answers `std::io::Result`, the Windows crate's descriptor reconstruction
/// answers the core's [`Result`] — so a caller that writes the child path once
/// and builds it for both would convert one error at its `?` and not the other,
/// and a leg on one target could not see the disagreement. Neither platform's
/// signature changes here (each is its mechanism's own); the mapping is the
/// facade's, and this arm's `io::Error` becomes the core's [`Error`] through
/// [`step`] — a refusal that names the step and keeps the platform's error as its
/// source. A single [`Result`] is the whole surface of the rule: [`adopted_control`],
/// [`adopted_listener`] and [`on_stop`] each answer `Result<T>` on every platform
/// that publishes them.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::{Adopted, Error, adopted_listener};
///
/// let adopted = Adopted::new(String::new(), Some(3), Vec::new(), None, None);
/// // The child serves on the socket the launcher bound; it never binds one.
/// let listener = adopted_listener(&adopted)?;
/// # let _ = listener;
/// # Ok::<(), Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn adopted_listener(adopted: &Adopted) -> Result<TcpListener> {
    macos_listener(adopted).map_err(step("rebuild the adopted listener"))
}
/// The child-side stop hook: where a stop the launcher forwards lands, under the
/// platform-neutral name the shared child path drives (design §3 step 9,
/// decision 20's second half).
///
/// **The macOS half of the pair above, and the same name for the same reason**:
/// the child path names one name and the build selects the implementation, which
/// is why this block is `#[cfg]`-selected for the platform that has a launcher
/// ([`adopted_listener`] states the rule; the CLI's own wrapper is the call site,
/// and it carries the same two-platform `#[cfg]`, because a target that confines
/// in place has no launcher to forward a stop). What travels behind it differs — the macOS
/// relay delivers a real signal into the confined child, so the child needs its
/// own registration for the forwarded stop to land anywhere — and what the child
/// sees does not: a closure that runs on a thread of its own and starts the
/// shutdown.
///
/// **One return type with the other two, mapped here rather than in the platform
/// crate** ([`adopted_listener`] states the rule): macOS's `io::Result` is what
/// the signal registration answers, and the facade turns its refusal into the
/// core's [`Error`] so that a caller's `?` is one conversion on every platform.
///
/// **`disarm` deliberately does not ride along.** It is for a process that
/// outlives one launch — the test harness, a CLI that serves twice — so the
/// launcher's own teardown is the one call site that wants it, and no shared path
/// names it today; [`disarm_signal_relay`] is that name, and it is a function
/// rather than a re-export because the platforms do not agree on a return type.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::{Error, on_stop};
///
/// // The hook runs on a thread of its own, so what it does may allocate and
/// // take locks: starting the runtime's shutdown here is what it is for.
/// on_stop(|| {
///     let _ = ();
/// })?;
/// # Ok::<(), Error>(())
/// ```
#[cfg(target_os = "macos")]
pub fn on_stop(hook: impl Fn() + Send + 'static) -> Result<()> {
    macos_on_stop(hook).map_err(step("install stop-signal handling"))
}
/// The child-side listener rebuild, under the platform-neutral name the shared
/// child path drives.
///
/// **The name is what the `#[cfg]` is for, and the gate it answers sits one layer
/// up** (design decision 2, §10): the re-export exists where a launcher does, so a
/// call site that names it is `#[cfg]`-selected for those platforms already — which
/// is exactly what the CLI's own wrapper is,
/// `crates/tinio-cli/src/sandbox/mod.rs`'s `adopted_listener`. **That wrapper is
/// not gated by the feature**: `sandbox/mod.rs` is compiled into every build
/// (its own module doc says why — a capability that does not exist cannot refuse),
/// and its platform arm is `#[cfg(any(target_os = "macos", target_os = "windows"))]`
/// with a refusal in the other direction, while the shape that decides whether a
/// wrapper is called at all is the call site's `adopted.is_some()`
/// (`commands/start/serve.rs`'s listener arm, the other caller). What it does is
/// delegate here on a launcher platform and refuse on a target whose confinement
/// never hands one over. Nothing else
/// from this platform's `launch` module rides along — the launcher's own argv
/// construction and `disarm` have no shared call site — and the core's names,
/// `adopt` among them, are already re-exported above rather than here.
///
/// **This arm is the identity of the shared shape**, and that is the only
/// difference between the two: the Windows launcher's descriptor reconstruction
/// already answers the core's [`Result`], so the facade has nothing to map — where
/// the macOS arm has to carry its `io::Error` across. [`adopted_listener`] there
/// states the rule the three adopted names share, and the mapping is why the
/// facade exists at all: one name, one return type, whichever platform builds it.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox::{Adopted, Error, adopted_listener};
///
/// # fn main() -> Result<(), Error> {
/// let adopted = Adopted::new(String::new(), Some(1040), Vec::new(), None, None);
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
/// where it stands. Both are reached by the CLI's confined child through **its own**
/// wrappers rather than through these re-exports directly:
/// `crates/tinio-cli/src/sandbox/mod.rs` carries an `on_stop` beside its
/// `adopted_listener`, `#[cfg]`-selected for the two platforms that forward a stop,
/// and the call site asks the process's own shape first — `adopted.is_some()`, the
/// same question that decides whether this child was launched at all.
///
/// **One return type with the other two, and here it is the platform's already**
/// ([`adopted_listener`] states the rule): the Windows hook answers the core's
/// [`Result`] natively, so this arm is the identity while the macOS arm maps its
/// `io::Result` — and a caller's `?` is one conversion either way.
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

#[cfg(target_os = "macos")]
use crate::_macos::relay::disarm as macos_relay_disarm;
#[cfg(target_os = "windows")]
use crate::_windows::launch::disarm as windows_relay_disarm;

/// Disarm the launcher's signal relay, if this platform has one.
///
/// **A function rather than a third re-export, because the two platforms do not
/// agree on a shape**: the Windows launcher's `launch::disarm` reports a failure
/// through the core's `Result`, while the macOS relay's `disarm` is the teardown
/// `supervise` already takes and answers `()`. A caller that outlives one launch
/// — the test harness, and any CLI that serves twice — wants one name and no
/// branch, so the facade is where the disagreement stops (design decision 2).
///
/// **This is the rule [`adopted_listener`] states, in the one shape a mapping
/// cannot fix.** There the disagreement is the error type and the facade maps it
/// into one [`Result`]; here the platforms disagree about the *success* shape
/// itself — one answers `()`, the other an error — and the one error is one this
/// teardown has no reporter for, so the shared name answers `()` on every
/// platform and the Windows arm discards what it was told (the next paragraphs
/// give the reason). A caller reads the same name and writes no `#[cfg]` either
/// way.
///
/// **The in-place platform has nothing to disarm**: Linux confines this process
/// and relays no signal, so its arm is the empty body and the call is a no-op
/// rather than a `#[cfg]` at the call site.
///
/// **A failure is not reported**, and that is deliberate rather than an omission:
/// the one caller is a teardown, the relay is the launcher's own, and a process on
/// its way out has no one to tell.
///
/// # Examples
///
/// ```
/// // Nothing to disarm on a host whose platform confines in place, and on a
/// // launcher platform this puts the process back on the default disposition.
/// tinio_sandbox::disarm_signal_relay();
/// ```
pub fn disarm_signal_relay() {
    // **Two statements rather than one expression both platforms share**, because their `disarm`
    // answers different things: the macOS relay answers `()`, the Windows launcher a `Result`
    // this teardown has no reporter for. Each alias is bound by the `#[cfg]` above.
    #[cfg(target_os = "macos")]
    macos_relay_disarm();
    #[cfg(target_os = "windows")]
    let _ = windows_relay_disarm();
}

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
pub use crate::_core::{
    AccessMode, Adopted, Backend, CONTROL_FD_PREFIX, Child, Confinement, ControlHandle, Error,
    Event, Exposure, LISTENER_FD_PREFIX, LaunchPayload, LaunchPlan, Observer, Result, Sandbox,
    SandboxSpec, Terminal, adopt, control_fd_arg, control_fd_from_arg, fail, is_control_fd_arg,
    is_launcher_fd_arg, is_listener_fd_arg, launch, listener_fd_arg, listener_fd_from_arg, refused,
    step, supervise,
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
/// The backend for the platform this build targets, as a **value**.
///
/// **The trait has one public name here, and it is [`Backend`]** — the core's, re-exported
/// with the rest of that surface rather than republished under a name of this arm's own.
/// The alias this arm used to add (`Platform`) was one fact with two public spellings,
/// which is the rule the list above is written under: a consumer that wants the trait
/// names `tinio_sandbox::Backend` on every target, and the only thing a target has to
/// answer for itself is *which type* implements it.
///
/// # Examples
///
/// ```
/// let mut backend = tinio_sandbox::platform();
/// // The constructor's answer is this build's backend, reached through the one name the
/// // facade publishes for the trait.
/// let _: &mut dyn tinio_sandbox::Backend = &mut backend;
/// ```
#[cfg(target_os = "linux")]
pub use crate::_linux::backend as platform;
/// The macOS backend's own crate, for the settings the shared surface does not
/// carry — its `Options`, which holds the mechanism (`Mechanism`), and the
/// `apply` module the confined child reaches its own confinement through.
///
/// **The same shape the Linux arm above takes, and the same reason**: the
/// mechanism is macOS's own vocabulary, so a consumer writes
/// `tinio_sandbox::macos::Options` inside the `#[cfg(target_os = "macos")]` block
/// it would have written anyway — and names **one** crate rather than one per
/// platform. The re-export does not widen the *shared* surface: these are still
/// the platform's own types in the platform's own crate (design decision 2, §10).
///
/// # Examples
///
/// ```
/// # #[cfg(target_os = "macos")]
/// # fn main() {
/// use tinio_sandbox::macos::{Mechanism, Options};
///
/// assert_eq!(Options::default().mechanism(), Mechanism::SandboxExec);
/// # }
/// # #[cfg(not(target_os = "macos"))]
/// # fn main() {}
/// ```
#[cfg(target_os = "macos")]
pub use crate::_macos as macos;
/// The backend for the platform this build targets, as a **value**.
///
/// [`Backend`] is the one public name for the trait (the Linux arm above carries the
/// rule); this is the constructor, and it differs per target only in the type it answers.
///
/// # Examples
///
/// ```
/// let mut backend = tinio_sandbox::platform();
/// let _: &mut dyn tinio_sandbox::Backend = &mut backend;
/// ```
#[cfg(target_os = "macos")]
pub use crate::_macos::backend as platform;
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
/// The backend for the platform this build targets, as a **value**.
///
/// [`Backend`] is the one public name for the trait (the Linux arm above carries the
/// rule); this is the constructor, and it differs per target only in the type it answers.
///
/// # Examples
///
/// ```
/// let mut backend = tinio_sandbox::platform();
/// let _: &mut dyn tinio_sandbox::Backend = &mut backend;
/// ```
#[cfg(target_os = "windows")]
pub use crate::_windows::backend as platform;
