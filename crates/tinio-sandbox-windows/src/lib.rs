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
//! onto the operator's directories and a hard kill between grant and rollback
//! leaves them (design §6); the program's own directory is exposed read-only,
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

#![cfg(target_os = "windows")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use std::{ffi::OsStr, iter::once, os::windows::ffi::OsStrExt, slice::from_raw_parts};

use crate::_core::{Backend as BackendTrait, Confinement, LaunchPlan, Result, Sandbox};

pub mod acl;
pub mod backend;
pub mod control;
pub mod handles;
pub mod job;
pub mod launch;
pub mod process;
pub mod sid;
pub mod socket;
// **Unconditional, and that is the whole point rather than an oversight.** A
// `#[cfg(test)]` module is invisible to `tests/`, and the crate cannot enable a
// feature of its own for them — a dev-dependency on itself is a cycle — so a
// gated `testing` module would be reachable by `tinio-server`'s suites and not
// by this crate's, which is the duplication the module exists to remove. What
// it costs is one small module compiled into a library nothing outside the
// workspace builds (`publish = false`), and what it buys is that the
// "waited for or killed on every path" rule is spelled once for both callers.
pub mod testing;

/// This crate's own names, reached from the crate root.
///
/// The same shape the facade's platform aliases take: a consumer naming
/// `tinio_sandbox::windows` reaches the [`backend`](mod@backend) module here, and
/// the three names the launcher and its configuration need — the backend, its
/// `Options` and the `Capability` list those options carry — are at this root as
/// well, so `tinio_sandbox_windows::Options` resolves without the module path.
pub use self::backend::{Backend, Capability, Options};

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

/// A NUL-terminated UTF-16 copy of `text`.
///
/// **The crate root owns the termination rule, and every wide string this crate
/// builds goes through here or through `wide_os` below rather than spelling it
/// itself**: `sid`'s profile names and capability strings take this one, and the
/// system directory [`handles`](mod@handles) reads back from the kernel takes
/// [`from_wide_units`].
///
/// **`process`'s command line is the one buffer that takes neither, and the reason
/// is that it is not text**: every argument in it may be arbitrary UTF-16, so it is
/// built and terminated as code units rather than as a string
/// ([`process::command_line`]'s doc states it), while the program path — an
/// `OsStr` and not a `str` — takes `wide_os`.
pub(crate) fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(once(0)).collect()
}

/// A NUL-terminated UTF-16 copy of `path`, for the input that cannot go through
/// `wide`'s `&str`.
///
/// A Windows path is arbitrary UTF-16 rather than UTF-8, so `to_string_lossy`
/// is not a conversion this crate may make on one: it replaces a unit that is
/// not valid UTF-8 with U+FFFD and the kernel then opens a different file from
/// the one the caller named. `encode_wide` is the lossless copy — it is what
/// `OsStr` is for on this platform — and the termination below is the same rule
/// `wide` applies.
pub(crate) fn wide_os(path: &OsStr) -> Vec<u16> {
    path.encode_wide().chain(once(0)).collect()
}

/// Read a NUL-terminated UTF-16 string into a `String`.
///
/// **The crate root's one unsafe site**, and the reason the exception sits on
/// this function rather than on the file: the manifest denies `unsafe_code`, a
/// crate-level allow would lift that for every line of this module — including
/// the modules it declares and every item added later — and the constitution
/// asks for the smallest scope in which the exception is expressible (design
/// decision 14).
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
/// `from_utf16_lossy` is right for those and wrong for a path — [`wide_os`]'s
/// doc states why a path may not take it, and [`system_directory`] is the one
/// caller that has a path, so it goes through [`from_wide_units`] and
/// `OsString::from_wide` instead.
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
