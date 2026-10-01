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

pub mod serve;

// The platform items each launcher crate answers, bound under one alias per platform so the
// facade's own bodies call a two-segment path: `clippy::absolute_paths` is `warn` for this
// crate, and a path written straight from the crate root is three segments or more. The
// `disarm` pair is not one alias for one body, because their answers differ — see
// [`disarm_signal_relay`]. The rest are the adopted names' platform halves, which the facade
// maps into the shared shape — see [`adopted_listener`]; `macos_apply` is the child-side apply
// [`apply_self_confinement`] delegates to, under the same rule.
//
// `io` is the one standard-library name of the facade's own (the errors it builds from the
// platforms' refusals), so it is imported on every target rather than with the platform items
// below.
#[cfg(unix)]
use std::os::{fd::BorrowedFd, unix::net::UnixListener};
use std::{io, net::TcpListener};

#[cfg(target_os = "macos")]
use crate::_macos::apply::self_confinement as macos_apply;
#[cfg(target_os = "macos")]
use crate::_macos::launch::{control as macos_control, listener as macos_listener};
#[cfg(target_os = "macos")]
use crate::_macos::relay::on_forwarded_stop as macos_on_stop;
#[cfg(target_os = "windows")]
use crate::_windows::launch::{listener as windows_listener, on_stop as windows_on_stop};

/// The socket [`adopted_control`] rebuilds. A unix listener where the platform has one.
#[cfg(unix)]
pub type ControlSocket = UnixListener;

/// Success type of [`adopted_control`] where this platform has no unix control socket.
#[cfg(not(unix))]
#[derive(Debug)]
pub struct ControlSocket {
    _private: (),
}

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
pub fn adopted_control(adopted: &Adopted) -> Result<ControlSocket> {
    #[cfg(target_os = "macos")]
    {
        macos_control(adopted).map_err(step("rebuild the control channel"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err(missing_handoff(
            STEP_ADOPTED_CONTROL,
            format!(
                "this process inherited control descriptor {:?}, but this platform's confinement \
                 never hands a control socket over: refusing rather than serving on a channel \
                 this process did not bind",
                adopted.controls()
            ),
        ))
    }
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
pub fn adopted_listener(adopted: &Adopted) -> Result<TcpListener> {
    #[cfg(target_os = "macos")]
    {
        macos_listener(adopted).map_err(step("rebuild the adopted listener"))
    }
    #[cfg(target_os = "windows")]
    {
        windows_listener(adopted)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        Err(missing_handoff(
            STEP_ADOPTED_LISTENER,
            format!(
                "this process inherited listener descriptor {:?}, but this platform's confinement \
                 never hands one over: refusing rather than serving on a socket this process did \
                 not bind",
                adopted.listener()
            ),
        ))
    }
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
pub fn on_stop(hook: impl Fn() + Send + 'static) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        macos_on_stop(hook).map_err(step("install stop-signal handling"))
    }
    #[cfg(target_os = "windows")]
    {
        windows_on_stop(hook)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        let _ = hook;
        Err(missing_handoff(
            STEP_ON_STOP,
            "this platform's confinement never forwards a stop: refusing rather than installing a \
             hook nothing will call"
                .to_owned(),
        ))
    }
}

/// The `step` a platform without a child-side handoff reports for it.
///
/// **Published because a caller classifies them.** `tinio-cli`'s `sandbox_err` maps exactly
/// these three onto the operational sentence the [`Error::Step`] source already carries, so a
/// caller matching the literals would have the CLI's answer change silently under a rename
/// here. Handing the caller the names makes that a compile error instead.
pub const STEP_ADOPTED_LISTENER: &str = "adopted-listener";
/// See [`STEP_ADOPTED_LISTENER`].
pub const STEP_ADOPTED_CONTROL: &str = "adopted-control";
/// See [`STEP_ADOPTED_LISTENER`].
pub const STEP_ON_STOP: &str = "on-stop";

#[cfg(not(target_os = "macos"))]
fn missing_handoff(step_name: &'static str, detail: String) -> Error {
    Error::Step {
        step: step_name,
        source: io::Error::other(detail),
    }
}

/// A confinement a confined child applied to **itself**, as its launcher's frame asked for it.
///
/// **The three facts the child's own `step = "sandbox-init"` event reports** (`tinio-cli`'s
/// `sandbox::apply_self_confinement` is the caller that emits it): which mechanism the payload
/// named, how many rules the text it applied grants, and how many parameters it referenced.
///
/// **The platform's own value, projected onto the one shape a caller can name on every target.**
/// macOS's `tinio_sandbox_macos::apply::Applied` carries that crate's `Mechanism` enum, which
/// exists on that target alone; the mechanism travels here as its wire spelling — the string the
/// launch payload and `Mechanism::as_str` already share — so a caller reads the same three
/// fields whichever platform built the value, and a target whose mechanism applies no
/// confinement of its own never builds one at all.
///
/// # Examples
///
/// ```
/// use tinio_sandbox::Applied;
///
/// // What a child confined by the in-process mechanism reports: the mechanism its launcher
/// // resolved, and the shape of the text it applied.
/// let applied = Applied {
///     mechanism: "sandbox-init",
///     rules: 4,
///     params: 1,
/// };
/// assert_eq!(applied.mechanism, "sandbox-init");
/// assert_eq!(applied.rules, 4);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// The mechanism the frame named, as the wire spelling both processes share.
    pub mechanism: &'static str,
    /// How many rules the applied text grants.
    pub rules: usize,
    /// How many `(param "…")` values the profile referenced.
    pub params: usize,
}

/// The step name this apply's failures carry — the mechanism's own wire spelling, and the
/// `step` the child's own event is keyed by (shared §7's "the log's `step` and the error's are
/// the same string").
const SELF_CONFINEMENT_STEP: &str = "sandbox-init";

/// Apply the confinement a launch payload carries to **this** process — the child side of the
/// in-process mechanism, under one platform-neutral name.
///
/// **The rule is the contract's rather than one backend's, and that is what this name is for.**
/// Which mechanisms a process may apply to itself is the platform's
/// (`tinio_sandbox_macos::apply`, whose own doc owns that vocabulary); what a frame means on a
/// platform whose confinement is applied **at process creation or in place** is the contract's,
/// because a frame that asked for a confinement must never be served without one (design §11).
/// The disagreement stops here, the way it does in [`disarm_signal_relay`]: macOS applies the
/// payload's spec and answers what it applied, every other target answers the refusal below when
/// the frame carries a spec, and a target that grows an in-process mechanism adds an arm here
/// rather than a call site of its own.
///
/// **`Ok(None)` is every process that is not the child of one of these mechanisms** — a
/// launcher, a `status` client, an unconfined run, the child of the mechanism that confines by
/// wrapping its `exec` (whose confinement was applied to it at that exec) — **and a frame that
/// carries no confinement at all**, on every target.
///
/// **It fails closed, and that is the whole contract.** The apply is one-shot and irreversible,
/// so a process that continued past a refusal would be a server with **no** confinement under a
/// launcher that reported one. The environment is never re-read: the launcher already resolved
/// the mechanism, and a second reader would be a second answer.
///
/// # Errors
///
/// The platform's own refusal, carried as the diagnostic it is — libSystem's for a profile the
/// kernel would not compile, and the frame's own for a mechanism this build does not know or one
/// that cannot have written a spec — and, on a target whose confinement is applied when the
/// process is created or in place, **any** frame that carries a spec.
///
/// # Examples
///
/// ```
/// use tinio_sandbox::{Adopted, apply_self_confinement};
///
/// // A launch that handed this process no confinement: nothing to apply, on every platform.
/// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
/// assert_eq!(
///     apply_self_confinement(&adopted).expect("no confinement"),
///     None
/// );
/// ```
pub fn apply_self_confinement(adopted: &Adopted) -> Result<Option<Applied>> {
    // **Two arms, and the `#[cfg]` is the platform fact** — the shape [`disarm_signal_relay`]
    // uses for its own disagreement. macOS is the one target whose confinement a child applies
    // to a *running* process, so it is the one arm that reads the payload's spec; the other arm
    // is the contract's answer for every target that cannot, and it is compiled on the three
    // targets this build serves as well as on a target with no backend at all.
    #[cfg(target_os = "macos")]
    {
        macos_apply(adopted)
            .map(|applied| {
                applied.map(|applied| Applied {
                    // The platform's enum as the wire spelling, which is the name the launch
                    // payload carried and the one an operator reads in the event.
                    mechanism: applied.mechanism.as_str(),
                    rules: applied.rules,
                    params: applied.params,
                })
            })
            .map_err(|refused| fail(SELF_CONFINEMENT_STEP, io::Error::other(refused)))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let Some(spec) = adopted.confinement() else {
            // The ordinary case of every platform: a launch whose confinement was already
            // applied, or none at all. Nothing is read and nothing happens.
            return Ok(None);
        };
        // **A spec here is a frame this process cannot honour, and serving it would be the
        // failure the contract exists to prevent.** Windows' container and Linux's in-place
        // jail are both established around a process this one is not, so there is no mechanism
        // here to apply the spec with; the refusal names the mechanism the frame asked for,
        // because a frame carrying one is either a launcher this build does not agree with or
        // an argv someone assembled by hand.
        Err(fail(
            SELF_CONFINEMENT_STEP,
            io::Error::other(format!(
                "the launch payload asks this process to apply the confinement `{}` to itself, \
                 but this platform's confinement is applied when the process is created or in \
                 place — never by the process it confines: nothing was applied, so the run is \
                 refused rather than served without the confinement the frame asked for",
                spec.mechanism
            )),
        ))
    }
}

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

/// What a target's confinement does, so a caller branches on the fact and not on the OS.
///
/// **The type is the core's** ([`tinio_sandbox_core::PlatformFacts`]) and each backend answers
/// for itself, beside its own mechanism: `tinio_sandbox_macos::FACTS`,
/// `tinio_sandbox_windows::FACTS`, `tinio_sandbox_linux::FACTS`.
pub use crate::_core::PlatformFacts;

/// This target's [`PlatformFacts`] — **the one OS split a caller writes**.
///
/// The value is the target backend's own constant, so every field is a sentence the crate that
/// owns the mechanism wrote. Which constant is the only `cfg` here.
///
/// # Examples
///
/// ```
/// let facts = tinio_sandbox::platform_facts();
/// assert_eq!(
///     facts.launches_child,
///     cfg!(any(target_os = "macos", target_os = "windows"))
/// );
/// ```
///
/// ```
/// let facts = tinio_sandbox::platform_facts();
/// assert_eq!(facts.refuses_derived_state_dir, cfg!(target_os = "macos"));
/// ```
pub const fn platform_facts() -> PlatformFacts {
    #[cfg(target_os = "linux")]
    let facts = _linux::FACTS;
    #[cfg(target_os = "macos")]
    let facts = _macos::FACTS;
    #[cfg(target_os = "windows")]
    let facts = _windows::FACTS;
    // No backend is wired for this target, which is the same answer an in-place backend gives:
    // nothing is launched, so the two facts a launch owns are unstated rather than false.
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let facts = PlatformFacts::NONE;
    facts
}

/// How much of its guarantee this target's confinement applies — **the strength the backend
/// answers for, reached the way [`platform_facts`] reaches the facts**.
///
/// The value is the target backend's own constant, so it is a sentence the crate that owns the
/// mechanism wrote: `tinio_sandbox_macos::DIAGNOSTICS`, `tinio_sandbox_windows::DIAGNOSTICS`,
/// `tinio_sandbox_linux::DIAGNOSTICS`. Which constant is the only `cfg` here, and a target this
/// build wires no backend for answers [`Diagnostics::NOT_REPORTED`] — not a claim that a
/// mechanism which is not there applies nothing.
///
/// **A caller that needs the strength reads this rather than assuming one**: the value a
/// backend that has not answered returns is `NotReported`, and treating that as "nothing is
/// applied" is the one reading this function exists to prevent.
///
/// # Examples
///
/// ```
/// use tinio_sandbox::{Enforcement, backend_available, diagnostics};
///
/// // The three targets whose backend has landed answer `Full`; any other target answers the
/// // honest `NotReported` rather than failing to name a value at all.
/// let expected = if backend_available() {
///     Enforcement::Full
/// } else {
///     Enforcement::NotReported
/// };
/// assert_eq!(diagnostics().enforcement, expected);
/// ```
pub const fn diagnostics() -> Diagnostics {
    #[cfg(target_os = "linux")]
    let diagnostics = _linux::DIAGNOSTICS;
    #[cfg(target_os = "macos")]
    let diagnostics = _macos::DIAGNOSTICS;
    #[cfg(target_os = "windows")]
    let diagnostics = _windows::DIAGNOSTICS;
    // No backend is wired for this target, so nothing has answered the strength question: the
    // value is "not reported" rather than a claim that no mechanism is applied.
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
    let diagnostics = Diagnostics::NOT_REPORTED;
    diagnostics
}

/// The control descriptors a launcher hands to the plan — **on unix, the borrowed
/// descriptors themselves**, so the type system carries the one fact the handoff is
/// made of.
///
/// **A descriptor reaches this call as an object, because only an object proves it is
/// open in this process.** [`ControlHandle::duplicate`] — the core's safe constructor,
/// which duplicates with `F_DUPFD_CLOEXEC` — is what turns each borrow into the plan's
/// own handle, so the launcher's own socket keeps every flag it had and the descriptor
/// the child rebuilds is one the launcher really holds. **The shape used to be one
/// `&[i64]` for every platform, and the change is the point rather than a tidy-up**: a
/// number carries no evidence of anything, so the macOS crate had to rebuild a
/// descriptor from a caller-supplied integer before it could duplicate one
/// (`launch::duplicate_control`, deleted 2026-09-27) — an `unsafe` site where design
/// decision 14 enumerates three for that crate, and one whose failure mode was a
/// duplicate of an unrelated open descriptor that the child would adopt as its control
/// channel. The integer exists only where the platform's own handoff form is an
/// integer, which is the Windows half below.
///
/// **The borrow ends with the call, and nothing in the answer points back at the
/// caller.** Every handle returned here owns its duplicate, so the caller must keep
/// the borrowed descriptors alive for the call itself — which the signature enforces,
/// since the slice's lifetime is anonymous and unconstrained — and the plan built from
/// the answer may outlive the object the descriptors were borrowed from. That is the
/// CLI's shape (`crates/tinio-cli/src/sandbox/confine.rs`): the channel borrows the
/// socket it bound, one duplicate goes on the spec, and the channel itself lives for
/// as long as the launch needs it.
///
/// **macOS is the one unix platform that hands a control channel over**, because it is
/// the one whose confined child cannot bind a socket itself: its profile grants no
/// `network-bind` at all. Every other unix target answers [`Error::Unsupported`] — a
/// target whose confinement is applied in place starts no child, so there is no channel
/// to hand over — which is the same fact [`platform_facts`] states from the other side.
///
/// # Errors
///
/// A descriptor that cannot be duplicated (a closed socket, or a full descriptor table),
/// and [`Error::Unsupported`] on a platform whose confinement never hands a control
/// channel to a child.
///
/// # Examples
///
/// ```no_run
/// use std::{fs::File, os::fd::AsFd as _};
///
/// use tinio_sandbox::control_handles;
///
/// // The launcher's channel, borrowed from the object that owns it: the duplicate the
/// // plan takes is made inside the call, so the socket may be closed after it returns.
/// let channel = File::open("/dev/null")?;
/// let handles = control_handles(&[channel.as_fd()])?;
/// # let _ = handles;
/// # Ok::<(), tinio_sandbox::Error>(())
/// ```
#[cfg(unix)]
pub fn control_handles(controls: &[BorrowedFd<'_>]) -> Result<Vec<ControlHandle>> {
    #[cfg(target_os = "macos")]
    {
        controls
            .iter()
            .map(|control| {
                ControlHandle::duplicate(control).map_err(step("duplicate the control channel"))
            })
            .collect()
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = controls;
        Err(Error::Unsupported {
            detail: "this platform does not hand a control channel to a child",
        })
    }
}

/// The control handle values a launcher hands to the plan — **the handoff's Windows
/// form, where a value is what the platform passes and the channel stays the owner**.
///
/// **Nothing is duplicated here, and that is the platform's own rule**: a Windows handle
/// is made inheritable by value (`SetHandleInformation`, taken immediately before
/// `CreateProcessW`), so the plan carries the values the launcher's channel already
/// holds — `tinio-api`'s `Channel::handoff_values` — and the channel remains their one
/// owner, because a duplicate would be a second owner of a pipe the child still serves
/// on. [`ControlHandle::inherited`] copies the value for the plan and takes no ownership,
/// which is why this arm cannot fail; the core's own doc states the same argument for why
/// its Windows form is a value where the unix one is an owned object.
///
/// **The values are unchecked, and the one call site is what makes that affordable.**
/// What reaches this arm is what a launcher read off its own channel in the same process
/// (`crates/tinio-cli/src/sandbox/confine.rs`), and what acts on the copied value is that
/// same launcher's process creation — so no public step turns an arbitrary integer into a
/// live handle. The unix arm above is where the type system can carry that fact, and it
/// now does: there, an integer would have to be rebuilt into a descriptor before it could
/// be duplicated, which is why the two platforms no longer share one signature.
///
/// # Errors
///
/// [`Error::Unsupported`] on a platform whose confinement never hands a control channel
/// to a child.
///
/// # Examples
///
/// ```
/// use tinio_sandbox::control_handles;
///
/// // The handle values the launcher's channel keeps owning, copied for the plan.
/// let handles = control_handles(&[1040])?;
/// assert_eq!(handles[0].value(), 1040);
/// # Ok::<(), tinio_sandbox::Error>(())
/// ```
#[cfg(not(unix))]
pub fn control_handles(handles: &[i64]) -> Result<Vec<ControlHandle>> {
    #[cfg(target_os = "windows")]
    {
        Ok(handles
            .iter()
            .copied()
            .map(ControlHandle::inherited)
            .collect())
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = handles;
        Err(Error::Unsupported {
            detail: "this platform does not hand a control channel to a child",
        })
    }
}

/// Whether this build's platform has a confinement backend at all — the one answer a caller
/// asks **before** it chooses a backend.
///
/// **The target list lives here because the selection lives here.** `platform()` is defined per
/// target, one `#[cfg]`-gated arm per platform crate, and this answer names the same targets
/// those arms are written for — so a new backend's target is added to the two in the same edit
/// and to no other crate. A caller with a list of its own would be a second table, in a crate
/// that owns neither the arms nor the selection, and the two would have to be kept in step by
/// hand (`tinio-cli`'s `sandbox::BACKEND_IS_STUB` was that table, and is gone for that reason).
///
/// **It cannot be a method on [`Backend`]**, and the next reader should not have to work out
/// why: a caller needs this answer *before* an arm chooses a backend — to refuse a request
/// rather than reach an arm — and on a target with no backend there is no `Backend`
/// implementation for a method to be called on at all. The trait answers "confine this"; this
/// answers "is there anything here to confine with".
///
/// **`false` is a target this build refuses confinement on, never a silent no-op**: the caller
/// that gets it must refuse the request rather than proceed unconfined (design §11). The three
/// targets whose backends have landed — Linux, macOS and Windows — all answer `true`.
///
/// # Examples
///
/// ```rust
/// // The answer is this host's, so the assertion is written as the platform fact it is: the
/// // three targets whose backend has landed answer `true`, and any other target answers `false`
/// // rather than failing to name a value at all.
/// # #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
/// assert!(tinio_sandbox::backend_available());
/// # #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
/// assert!(!tinio_sandbox::backend_available());
/// ```
pub const fn backend_available() -> bool {
    // The list is `platform()`'s arms' own gate, one screen below: the three `#[cfg]`-selected
    // re-exports, each of which resolves to a real backend. An unsupported target has neither an
    // arm nor an implementation, which is the `false` above — a value rather than a failure to
    // compile.
    cfg!(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows"
    ))
}

/// Whether **this host** can confine a process, as the platform answers it.
///
/// **The run-time half of the question [`backend_available`] answers at build time.** That one
/// is a `const fn` over the target list and says the backend has landed; this one asks the
/// token this process is actually running under, and on Windows that means creating a
/// throwaway AppContainer profile and deleting it.
///
/// **It cannot be `const`**, unlike its two siblings (`platform_facts`, `diagnostics`): the
/// answer is the machine's, not the build's. **The non-Windows arm is `Ok(())` rather than an
/// absence**: macOS's Seatbelt is an SBPL profile applied to this process and Linux's jail
/// applies in place, so neither has a host-level precondition to fail. The name exists on
/// every target so a consumer compiles and answers one question rather than two.
///
/// # Errors
///
/// `Error::HostCannotConfine` on Windows when this token is refused. The fact's owner is
/// `docs/superpowers/specs/2026-10-01-host-confinement-capability-design.md`.
///
/// # Examples
///
/// ```rust
/// // The answer is this host's, and the profile the probe creates is deleted again before it
/// // returns: a refusal is a value to read here, never a panic.
/// match tinio_sandbox::can_confine() {
///     Ok(()) => {}
///     Err(err) => eprintln!("{err}"),
/// }
/// ```
#[cfg(target_os = "windows")]
pub fn can_confine() -> Result<()> {
    use crate::_windows::capability::can_confine as windows_can_confine;

    windows_can_confine()
}

/// Whether **this host** can confine a process — see the Windows arm's doc.
///
/// # Examples
///
/// ```rust
/// // A non-Windows host has no host-level precondition to fail: macOS's profile applies to
/// // this process and Linux's jail applies in place, so this arm answers `Ok` on every
/// // target it is compiled for — including one this build wires no backend for, whose
/// // refusal is [`backend_available`]'s to make.
/// assert!(tinio_sandbox::can_confine().is_ok());
/// ```
#[cfg(not(target_os = "windows"))]
pub fn can_confine() -> Result<()> {
    Ok(())
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
    AccessMode, Adopted, Attribution, Backend, CONTROL_FD_PREFIX, Child, Confinement,
    ControlHandle, Diagnostics, Enforcement, Error, Event, Exposure, LINUX_MODEL_USAGE,
    LISTENER_FD_PREFIX, LaunchPayload, LaunchPlan, MACOS_MECHANISM_USAGE, Observer, Readiness,
    Refused, Result, Sandbox, SandboxSpec, Terminal, WINDOWS_CAPABILITY_USAGE, adopt,
    control_fd_arg, control_fd_from_arg, describe_exposures, fail, filter_argv, is_control_fd_arg,
    is_launcher_fd_arg, is_listener_fd_arg, launch, listener_fd_arg, listener_fd_from_arg, refused,
    step, supervise,
};
// **The fork probe's names, published only where a probe can exist** — a `#[cfg]` the core's own
// list carries for the same reason, and one this list has to repeat because the equality between
// them is the rule: a probe is built on a fork, a pipe and a `waitpid`, so neither list can name
// it on a target that has none of the three.
#[cfg(unix)]
pub use crate::_core::{ProbeSteps, await_verdict, report_verdict};
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
/// The Linux model `TINIO_SANDBOX_MODEL` asks for — **one read, on the rule that owns the
/// variable**, so that a caller can carry the answer instead of spelling the variable again.
///
/// **[`serve::Spec::model`] is where the answer goes**, and the reason it is read here rather
/// than there: the CLI has to state the request *after* the confinement has run (its in-place
/// re-emit), where a second read of `TINIO_SANDBOX_MODEL` would be a second answer to what the
/// confinement applied. The value therefore travels as a value, read once through the Linux
/// backend's own parser (`tinio_sandbox_linux::Options::from_env` — the derivation the Linux arm
/// of `Spec::apply` builds its options from).
///
/// **A refusal is the reader's own and is not swallowed**: a value that is neither `userns` nor
/// `root` answers the backend's [`Error::Usage`] carrying [`LINUX_MODEL_USAGE`] — a caller maps it
/// the way it maps `Spec::apply`'s, so a typo still ends the run as a usage error rather than being
/// downgraded to the default.
///
/// # Errors
///
/// [`Error::Usage`] ([`LINUX_MODEL_USAGE`]) for a value outside the two model names.
///
/// # Examples
///
/// ```
/// # #[cfg(target_os = "linux")]
/// # fn main() {
/// use tinio_sandbox::{requested_model, serve};
///
/// // Both arms, so the example turns on the call rather than on how the test process happened
/// // to be started: the model the environment spelled, or the refusal that names the variable.
/// match requested_model() {
///     Ok(model) => {
///         let _ = serve::Spec::new().model(model);
///     }
///     Err(error) => assert!(error.to_string().contains("TINIO_SANDBOX_MODEL")),
/// }
/// # }
/// # #[cfg(not(target_os = "linux"))]
/// # fn main() {}
/// ```
#[cfg(target_os = "linux")]
pub fn requested_model() -> Result<Mechanism> {
    use crate::_linux::Options;

    Ok(Options::from_env()?.model())
}
/// The Linux model vocabulary [`requested_model`] answers and [`serve::Spec::model`] carries —
/// the platform's own type, published under a `#[cfg]`-selected facade name so that a caller
/// that must name it in a signature of its own (the CLI's re-emit parameter) writes no `linux`
/// module path for it.
///
/// **This is the module alias's rule in the one shape a path cannot serve**: the alias is for a
/// caller spelling platform vocabulary at a call site, while this name exists because the shared
/// signatures that carry a model — `Spec::model`, `requested_model` — must be nameable by a
/// caller that never opens the platform module.
///
/// # Examples
///
/// ```
/// # #[cfg(target_os = "linux")]
/// # fn main() {
/// use tinio_sandbox::{Mechanism, serve};
///
/// let spec = serve::Spec::new().model(Mechanism::Auto);
/// assert_eq!(spec.model_configured(), Some(Mechanism::Auto));
/// # }
/// # #[cfg(not(target_os = "linux"))]
/// # fn main() {}
/// ```
#[cfg(target_os = "linux")]
pub use crate::_linux::Mechanism;
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

#[cfg(test)]
mod tests {
    //! The facade's own logic, tested at the file that defines it: the backend this
    //! build selects resolves to a value, the platform selection resolves to a real
    //! backend, the target backend names its own enforcement, the child-side
    //! self-confinement step answers what the platform can and cannot apply, and the
    //! host predicate answers on every target.
    //!
    //! **The re-export list stays in `tests/facade.rs`**, and that is the boundary's
    //! whole point: it binds the public `tinio_sandbox::` paths on purpose, so a name
    //! that stopped being re-exported — or that was narrowed from `pub` to
    //! crate-visibility — fails there, while a module inside this file could not
    //! catch it.

    use super::*;
    /// The mechanism's own vocabulary through the facade's `macos` alias — the same
    /// module `tinio_sandbox::macos::…` names from outside, bound here for the
    /// default-mechanism leg below.
    #[cfg(target_os = "macos")]
    use crate::macos::{Mechanism, Options};

    #[test]
    fn the_facade_resolves_exactly_one_backend() {
        let mut backend = platform();
        // Confining an empty sandbox is allowed to fail for any platform-specific
        // reason; what this asserts is that the call reaches a real backend rather
        // than failing to compile or panicking on a missing selection.
        let result = backend.confine(&Sandbox::new(), &LaunchPlan::new());
        match result {
            Ok(_) => {}
            Err(err) => assert!(!err.to_string().is_empty()),
        }
    }

    /// **Every backend names the strength of its own mechanism, and this is where the one this
    /// build selected is pinned** — `diagnostics()` is the target backend's constant, reached the
    /// way `platform_facts()` reaches the facts.
    ///
    /// **Why the value is `Full` on all three, and why the assertion is written against the
    /// selection rather than a list of platforms**: Linux's in-place jail, macOS's Seatbelt and
    /// Windows' AppContainer each apply the whole guarantee this build's contract claims for them,
    /// so the expected answer is the same on every target a backend exists for. A backend that
    /// stopped answering would read `NotReported` here — the value that is no claim about a
    /// mechanism — and this test is where that would fail. The per-platform half — that *this*
    /// crate's constant is `Full` — is asserted in each crate's own doctest, which only its own
    /// platform compiles.
    #[test]
    fn the_target_backend_names_its_enforcement() {
        let expected = if backend_available() {
            Enforcement::Full
        } else {
            // A target this build wires no backend for has nothing to answer with, and
            // `NotReported` is that answer rather than a claim about a mechanism.
            Enforcement::NotReported
        };
        assert_eq!(
            diagnostics().enforcement,
            expected,
            "the selected backend answers for its own mechanism"
        );
    }

    /// **The Linux selection resolves to the in-process backend, and this leg is where that is
    /// pinned** — `platform()` is the alias the crate root binds to `tinio_sandbox_linux`'s own
    /// `backend` on this target alone.
    ///
    /// **Not executed on the host this move was made on (Windows)**: the `#[cfg]` below is the
    /// whole reason — a `--target <platform> --tests` check type-checks it, and nothing here runs
    /// it (`plans/2026-10-01-test-placement-cleanup.md`).
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_selects_the_in_process_backend() {
        // An empty sandbox has nothing to confine to, so Linux refuses — which is
        // itself the proof that the Linux backend is the one wired in.
        let mut backend = platform();
        let err = backend
            .confine(&Sandbox::new(), &LaunchPlan::new())
            .expect_err("an empty sandbox must be refused");
        assert!(err.to_string().contains("nothing exposed"), "{err}");
    }

    /// **macOS alone, until P3's backend landed — and the assertion is now the opposite one.** The
    /// Windows composition landed in P2's Task 9 and this test left that platform the moment its
    /// backend stopped answering `Unsupported`; P3's macOS composition has now landed, so the same
    /// thing is true here. What is asserted is the *composition* rather than a launch: a backend that
    /// reaches `confine` at all has replaced the stub, and an exposure it cannot express is the one
    /// refusal this call can produce without spawning `sandbox-exec` (a tree with no `sandbox-exec`
    /// answers `Unsupported`, which would make the assertion below vacuous).
    ///
    /// **Not executed on the host this move was made on (Windows)**: the `#[cfg]` below is the
    /// whole reason — a `--target <platform> --tests` check type-checks it, and nothing here runs
    /// it (`plans/2026-10-01-test-placement-cleanup.md`).
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_selects_a_launcher_backend_rather_than_the_stub() {
        let mut backend = platform();
        let err = backend
            .confine(
                &Sandbox::new().expose("relative/path", AccessMode::ReadOnly),
                &LaunchPlan::new(),
            )
            .expect_err("a relative exposure cannot be expressed in a profile");
        assert!(
            matches!(err, Error::BadPath { .. }),
            "the macOS backend validates its exposures: {err}"
        );
        assert!(
            !err.to_string().contains("not implemented"),
            "the stub is gone: {err}"
        );
    }

    /// **The backend this facade builds is the default one, and the default mechanism is
    /// `sandbox-exec`** — the one every allowance was measured against and the one a run that
    /// sets no variable gets. An `Options::default`, or a `Mechanism::from_env` whose absent
    /// answer, that named the other mechanism fails here rather than in a launch.
    ///
    /// **The other half of the pair stays in `tests/facade.rs`**: reaching `Options`/`Mechanism`
    /// through the published `tinio_sandbox::macos::…` path is that boundary's subject, while the
    /// defaults asserted here are this module's. The alias is spelled `crate::macos` above, which
    /// is the same re-export seen from inside.
    ///
    /// **Not executed on the host this move was made on (Windows)**: the `#[cfg]` below is the
    /// whole reason — a `--target <platform> --tests` check type-checks it, and nothing here runs
    /// it (`plans/2026-10-01-test-placement-cleanup.md`).
    #[cfg(target_os = "macos")]
    #[test]
    fn macos_publishes_the_mechanism_and_defaults_to_the_helper() {
        assert_eq!(Options::default().mechanism(), Mechanism::SandboxExec);
        assert_eq!(
            Mechanism::from_env(None).expect("absent means the default"),
            Mechanism::SandboxExec
        );
        assert_eq!(
            Mechanism::from_env(Some("sandbox-init")).expect("the alternative"),
            Mechanism::SandboxInit
        );
    }

    /// **The self-confinement step answers one shape on every platform, and refuses what the platform
    /// cannot apply** — the child-side rule the contract owns, pinned the way
    /// `the_adopted_names_answer_one_error_type` (in `tests/facade.rs`) pins the adopted names: a
    /// `fn`-pointer binding at the shared [`Result`], so a platform arm that went back to its own
    /// error type stops compiling here rather than in a launch nobody runs on this host.
    ///
    /// **The frame is one macOS cannot have been handed, and that is deliberate.** `sandbox-exec`
    /// confines the child's `exec`, so a launcher that resolved it never puts a profile on the frame:
    /// the refusal is that disagreement between the two processes, and the apply is never reached. A
    /// frame this build would really apply is not a case this test can hold — the apply is one-shot
    /// and irreversible, so driving it here would confine the test binary and every leg after it.
    ///
    /// **The no-op half is the other side of the rule**, and it is what every ordinary run takes: a
    /// frame carrying no confinement is nothing to apply on any platform.
    #[test]
    fn the_self_confinement_step_refuses_what_the_platform_cannot_apply() {
        let _: fn(&Adopted) -> Result<Option<Applied>> = apply_self_confinement;

        let plain = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
        assert_eq!(
            apply_self_confinement(&plain).expect("no confinement to apply"),
            None,
            "a frame that carries none is a no-op everywhere"
        );

        let framed = Adopted::new(
            String::new(),
            Some(7),
            Vec::new(),
            None,
            Some(SandboxSpec {
                mechanism: String::from("sandbox-exec"),
                profile: String::new(),
                params: Vec::new(),
            }),
        );
        let err = apply_self_confinement(&framed)
            .expect_err("a frame this process cannot apply is refused, never served");
        assert!(!err.to_string().is_empty(), "{err}");
    }

    #[test]
    fn the_host_predicate_answers_on_every_target() {
        // The facade's surface has to exist where no AppContainer does: a consumer that
        // compiles only on Windows is not a facade.
        let answer = crate::can_confine();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(
            answer.is_ok(),
            "unix platforms confine in place or by profile of their own"
        );
        #[cfg(target_os = "windows")]
        let _ = answer;
    }
}
