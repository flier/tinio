//! Windows confinement for tinio: an AppContainer whose package SID holds an
//! ACE on exactly the exposed paths, launched by a supervising parent.
//!
//! [`Backend`] implements [`crate::_core::Backend`]. It can only confine a
//! process it creates — AppContainer membership is fixed at process creation —
//! so it binds nothing itself, grants the ACLs, launches a confined child and
//! supervises it through [`Confinement::Supervise`] (design decisions 2, 6).
//!
//! The confinement is three mechanisms, not one: the DACL whitelist decides
//! which paths exist for the confined process (design §6), the AppContainer's
//! capability set decides what it can reach over the network (design §7), and
//! the relay in `control` forwards the stop the launcher receives, because a
//! Windows container signals its main process and nothing else (design §3 step
//! 9). The parent hands the child a listener it never created, which is a
//! convenience for the port, **not** a network-isolation mechanism (design
//! decision 16).
//!
//! Three consequences are stated rather than hidden: the launcher writes ACEs
//! onto the operator's directories, they are standing, and a hard kill leaves
//! them where they are (design §6); the program's own directory is exposed read-only,
//! **without inheritance, together with the `*.dll` files directly beside the
//! program** — the child's own loader reads what it imports under its own token,
//! the image itself is opened with the launcher's, and what is *not* reached is a
//! DLL in a subdirectory or one loaded from elsewhere at runtime (design decision
//! 22 as Ruling 78 reshaped it); and the relay delivers a stop the child must still
//! know what to do with — the drain that follows is the CLI contract's
//! (T066/T067), and `launch::on_stop` is the hook it lands on.
//!
//! Behind `#![cfg(target_os = "windows")]` so the workspace builds on every
//! host: `members = ["crates/*"]` has no per-target form, so this crate
//! compiles — and lints — on targets it does not serve. **The modules of the
//! Windows implementation land across this phase's tasks**, so the list below
//! grows one name per task: a module joins it in the task that creates it.
//!
//! **The NUL-terminated UTF-16 rule is [`tinio_winsec::to_wide`]'s, not this
//! crate's.** Every name this crate hands the kernel — `sid`'s profile names and
//! capability strings, `process`'s image path, `acl`'s descriptor read — goes
//! through that one function, which the workspace's other Windows crates share,
//! so the encoding has one spelling here rather than one per module. What stays
//! at this root is the inverse: `from_wide_units` and `from_wide` below, which
//! are this crate's own FFI because no other crate reads a name back out of the
//! kernel.

#![cfg(target_os = "windows")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use std::slice::from_raw_parts;

use crate::_core::{
    Backend as BackendTrait, Confinement, LaunchPlan, PlatformFacts, Result, Sandbox,
};

pub mod acl;
pub mod backend;
pub mod control;
pub mod handles;
pub mod job;
pub mod launch;
pub mod process;
pub mod sid;
pub mod socket;
// **Behind this crate's `testing` feature, which no production build turns on.**
// The module is the conformance device `tinio-server`'s Windows e2e suite and this
// crate's own suites compose, and `docs/tests.md` puts conformance support in
// `[dev-dependencies]` rather than in a library a consumer links — the feature is
// what makes that true here, because the module has to be lib code to cross the
// `tests/` boundary at all. What turns it on:
//
// - this crate's own suites, through the self dev-dependency in `Cargo.toml` (a
//   `#[cfg(test)]` module is invisible to `tests/`, so a test-only module has to be
//   a feature, and the feature has to be asked for on this crate's test builds);
// - `tinio-server`'s Windows e2e targets, through their dev-dependency on this
//   crate with `features = ["testing"]`.
//
// It pulls `tinio-util`'s `testing` feature with it, which is the other half of the
// same rule: the shared device is not compiled into the launcher the CLI starts.
#[cfg(feature = "testing")]
pub mod testing;

/// This crate's own names, reached from the crate root.
///
/// The same shape the facade's platform aliases take: a consumer naming
/// `tinio_sandbox::windows` reaches the [`backend`](mod@backend) module here, and
/// the three names the launcher and its configuration need — the backend, its
/// `Options` and the `Capability` list those options carry — are at this root as
/// well, so `tinio_sandbox_windows::Options` resolves without the module path.
pub use self::backend::{Backend, Capability, Options};

/// What this mechanism does, for a caller that branches on the fact rather than the OS.
///
/// **The launcher answer, minus the macOS facts**: this backend starts a supervised child, and
/// the three facts a *launch* owns are Seatbelt's rather than its — it refuses no derived state
/// directory, proves nothing writable before launching, and reads handle values rather than
/// rebuilding a socket.
///
/// **Ending the child is not among these facts**, and this backend does end one: like every
/// launcher platform it installs a killer (`TerminateJobObject` on the job it holds), so a launch
/// that fails after `confine` returns ends its child through `Child::abort` rather than through a
/// fact a call site would have to consult.
pub const FACTS: PlatformFacts = PlatformFacts {
    launches_child: true,
    ..PlatformFacts::NONE
};

/// The backend for this platform, with its defaults.
///
/// # Examples
///
/// ```
/// let backend = tinio_sandbox_windows::backend();
/// let _ = backend;
/// ```
pub fn backend() -> Backend {
    Backend::default()
}

/// Confine through this platform's backend, with its defaults.
///
/// The trait-shaped entry for a caller that is content with the backend's
/// defaults, so the call site does not name the `Backend` type (design decision 2).
///
/// # Errors
///
/// Whatever [`Backend::confine`](crate::_core::Backend::confine) reports.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_core::{LaunchPlan, Sandbox};
///
/// let confined = tinio_sandbox_windows::confine(&Sandbox::new(), &LaunchPlan::new());
/// assert!(confined.is_err(), "nothing was exposed");
/// ```
pub fn confine(sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement> {
    backend().confine(sandbox, plan)
}

/// Read a NUL-terminated UTF-16 buffer as the units before its terminator.
///
/// **The reader to `tinio_winsec::to_wide`'s writer, and the half of the rule
/// that stays here**: the encoding is shared (the crate doc names the one
/// function that applies it), while this direction is this crate's own FFI — the
/// inverse of a rule is not something the writer's crate has to own, and
/// `tinio-winsec` reads no name back out of the kernel.
///
/// **One of the crate root's two unsafe sites, and the reason each exception
/// sits on its own function rather than on the file**: the manifest denies
/// `unsafe_code`, a crate-level allow would lift that for every line of this
/// module — including the modules it declares and every item added later — and
/// the constitution asks for the smallest scope in which the exception is
/// expressible (design decision 14). **Two, and a reader counting them should
/// count both**, because that is what the lint sees: this function and
/// [`from_wide`] below, each with its own `allow`.
///
/// # Safety
///
/// `text` must point at a NUL-terminated UTF-16 buffer that stays valid for the
/// call.
#[allow(unsafe_code)]
pub(crate) unsafe fn from_wide_units<'a>(text: *const u16) -> &'a [u16] {
    // SAFETY: the pointer is NUL-terminated by the caller's contract, so the
    // scan stops inside the buffer and yields the length below it.
    let end = unsafe { (0..).position(|offset| *text.add(offset) == 0).unwrap_or(0) };
    // SAFETY: `end` counts the units before that terminator, all of which the
    // caller's contract says are readable.
    unsafe { from_raw_parts(text, end) }
}

/// The same units as a `String`, **for the inputs that are text rather than
/// paths**: a SID's rendered form and a command line's arguments.
///
/// `from_utf16_lossy` is right for those and wrong for a path — a Windows path is
/// arbitrary UTF-16, so the lossy read replaces any unit that is not valid UTF-8
/// with U+FFFD and a caller then names a different file from the one it was given
/// — and [`system_directory`] is the one caller that has a path, so it goes
/// through [`from_wide_units`] and `OsString::from_wide` instead.
///
/// [`system_directory`]: crate::handles::system_directory
///
/// # Safety
///
/// `text` must point at a NUL-terminated UTF-16 buffer that stays valid for the
/// call.
#[allow(unsafe_code)]
pub(crate) unsafe fn from_wide(text: *const u16) -> String {
    // SAFETY: the caller's contract is this function's, forwarded unchanged.
    String::from_utf16_lossy(unsafe { from_wide_units(text) })
}
