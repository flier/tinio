//! macOS confinement for tinio: a Seatbelt (SBPL) profile, applied by
//! `sandbox-exec` or by the confined process itself.
//!
//! **Two mechanisms, one profile, and `sandbox-exec` is the default** (design
//! decision 6). `sandbox-exec` applies a profile to a process it **execs**, so
//! macOS confines a child: the parent keeps the listener, hands it over with the
//! launch payload and supervises. `sandbox-init` applies the same text **in the
//! process it confines**, so the launcher spawns the child directly and the child
//! calls `sandbox_init_with_parameters` on itself before it serves. The mechanism
//! is read from `TINIO_SANDBOX_MACOS_MECHANISM` by `Options::from_env`, and any
//! other value is a refusal naming the variable and its two values — never a
//! silent fallback to the default.
//!
//! The profile is `profile::profile`, a pure function of the exposure set and the
//! mechanism that will apply it: one `profile::Profile` holding the text — which
//! names every exposure as `(param "E<i>")` — and the `-D` values those references
//! resolve to. **The mechanism decides exactly one entry**: `process-exec` is
//! rendered for `sandbox-exec`, whose helper cannot `exec` the child without it,
//! and not for `sandbox-init`, whose exec and dynamic loader have already happened
//! by the time the profile lands (`Mechanism::wraps_the_exec`).
//!
//! `sandbox-exec` is deprecated by Apple and still shipped. It is not a
//! supported or versioned API, and nothing in that mechanism treats it as one:
//! when Apple removes it, `sandbox-exec` degrades to "no sandbox available, fail
//! closed", which is the error this backend returns when the binary is missing
//! (design, Risks) — while `sandbox-init` keeps working, its one dependency being
//! `libSystem`.
//!
//! **The platform gate is taken once, at the crate root, and that is the
//! operator's ruling rather than a preference**: every file in this crate is
//! macOS-only, so `#![cfg(target_os = "macos")]` below replaces the per-item
//! `#[cfg(target_os = "macos")]` this crate used to carry. The consequence is
//! accepted knowingly: on Linux and Windows the crate now compiles to an
//! **empty library**, so this crate's unit tests and doctests do not run on a
//! non-Mac host and every verification of it moves to a Mac — the same shape
//! `tinio-sandbox-linux` already has in the other direction (`members = ["crates/*"]` is a flat
//! glob with no per-target form, which is why the gate can only be per crate). `bash crates/tinio-sandbox-macos/scripts/macos-sandbox-verify.sh` is the run.
//!
//! **A doctest written in this crate's *module* docs is not gated with the items
//! beside it**, and that is the one sharp edge the ruling leaves: the gate removes
//! every item on a host that is not macOS, so an item's example disappears with it
//! — but a `//!` example survives, compiles against an empty library and fails with
//! `E0432` (measured: `cargo test -p tinio-sandbox-macos` on Windows, 2026-09-25,
//! which is a leg of `cargo test --workspace`). The crate-level example below is
//! therefore written with its body behind a `#[cfg(target_os = "macos")]`: it is
//! the whole example where the crate exists and an empty `main` where it does not.
//! That gate lives in a doctest rather than on an item, so it is not the per-item
//! noise the ruling removed — it is what keeps a `//!` example honest.
//!
//! # Logging
//!
//! This crate emits `tracing` events and installs no subscriber (shared §7):
//! the process that owns the runtime decides where they go, and in `serve` that
//! is a stderr subscriber. stdout carries the event protocol and nothing else
//! (decision 19), so nothing in this crate writes a log line to it.
//!
//! The levels follow shared §7's split, which exists because `serve`'s
//! subscriber is a fixed `Verbosity::Info`: `info!` for what a confinement was —
//! the rendered profile's rule and parameter counts, the pre-flight and its
//! outcome, the spawn with the child's pid, and the exit — and `debug!` for the
//! detail behind it, which is not printed until the `[log]` wiring lands. A
//! failure is an `error!` emitted where its `Error` is built, so the log's
//! `step` and the error's are the same string.
//!
//! # Examples
//!
//! ```
//! # #[cfg(target_os = "macos")]
//! # fn main() {
//! use std::ffi::OsString;
//!
//! use tinio_sandbox_core::{AccessMode, Sandbox};
//! use tinio_sandbox_macos::{Mechanism, profile::profile};
//!
//! let rendered = profile(
//!     &Sandbox::new().expose("/srv/data", AccessMode::ReadWrite),
//!     Mechanism::SandboxExec,
//! )
//! .unwrap();
//! assert!(
//!     rendered
//!         .text
//!         .contains("(allow file-read* file-write* (subpath (param \"E0\")))")
//! );
//! assert_eq!(
//!     rendered.params[0],
//!     (String::from("E0"), OsString::from("/srv/data"))
//! );
//! # }
//! # #[cfg(not(target_os = "macos"))]
//! # fn main() {}
//! ```

#![cfg(target_os = "macos")]

#[doc(hidden)]
pub extern crate tinio_sandbox_core as _core;

use crate::_core::{
    Backend as BackendTrait, Confinement, Diagnostics, Enforcement, LaunchPlan, PlatformFacts,
    Result, Sandbox,
};

pub mod apply;
pub mod launch;
pub mod mechanism;
pub mod profile;
pub mod relay;

pub use self::{
    launch::{SANDBOX_EXEC, command_args, confine, self_args},
    mechanism::{Mechanism, Options},
};

/// What this mechanism does, for a caller that branches on the fact rather than the OS.
///
/// **Every field is true for Seatbelt**, and each is a sentence about this crate's own launch:
/// it starts a supervised child; it grants the state directory without its parent (the `home`
/// paragraph of design §5); and it proves that directory can take a file before it launches.
///
/// **Ending the child of a failed launch is this crate's act and not a fact it publishes.**
/// [`launch::confine`] installs a killer on the child — a `SIGKILL` to the pid it spawned — and
/// the core's `Child::abort` is what calls it, on every launcher platform alike. No caller
/// consults a fact before ending a child, so none is answered here.
pub const FACTS: PlatformFacts = PlatformFacts {
    launches_child: true,
    refuses_derived_state_dir: true,
    probe_state_dir_writable: true,
};

/// How much of its guarantee this backend's mechanism applies — **the strength the mechanism
/// answers for**, published beside [`FACTS`] so the crate that owns the mechanism is the crate
/// that answers for it (the rule [`PlatformFacts`]'s own doc states).
///
/// **`Full`, and what that covers**: every exposure the profile names is granted at the access
/// it was exposed with, and everything else is refused — the `(deny default)` profile
/// `profile::render` writes, applied either by `sandbox-exec` around the child's `exec` or by
/// the child itself through `sandbox_init` (design decision 6, `Mechanism`). It is the whole
/// guarantee this build's contract claims for this mechanism; the one shape it does not span is
/// macOS's own — a profile is textual, so a parameter travels as a rendered path rather than
/// as a structural grant — and that is `docs/sandbox.md`'s to state, not a gap in what is
/// applied.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::Enforcement;
///
/// assert_eq!(
///     tinio_sandbox_macos::DIAGNOSTICS.enforcement,
///     Enforcement::Full
/// );
/// ```
pub const DIAGNOSTICS: Diagnostics = Diagnostics {
    enforcement: Enforcement::Full,
};

/// The macOS backend — the type the facade's `platform()` selects.
///
/// The struct and the `backend()` constructor below are this crate's contract
/// with the facade: `crates/tinio-sandbox/src/lib.rs`'s `platform()` calls
/// `backend()`, so losing either would leave `serve --sandbox` unable to leave
/// `Unsupported` on macOS no matter what the profile module does.
///
/// **The backend carries its [`Options`] and the facade's `platform()` builds the
/// default ones** — `Mechanism::SandboxExec`, which is what this crate did before
/// there was a choice. A caller that wants the other mechanism builds the backend
/// through [`Backend::with_options`], which is the shape `tinio-sandbox-linux`'s
/// backend takes for its own model.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::{Backend, Mechanism, Options};
///
/// let backend = Backend::with_options(Options::default().with_mechanism(Mechanism::SandboxInit));
/// let _ = backend;
/// ```
#[derive(Debug, Clone, Default)]
pub struct Backend {
    options: Options,
}

impl Backend {
    /// A backend with explicit options.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::{Backend, Mechanism, Options};
    ///
    /// let backend = Backend::with_options(Options::default().with_mechanism(Mechanism::SandboxInit));
    /// let _ = backend;
    /// ```
    pub fn with_options(options: Options) -> Self {
        Self { options }
    }

    /// The mechanism this backend will confine with.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_macos::{Backend, Mechanism, Options};
    ///
    /// let backend = Backend::with_options(Options::default().with_mechanism(Mechanism::SandboxInit));
    /// assert_eq!(backend.options().mechanism(), Mechanism::SandboxInit);
    /// ```
    pub fn options(&self) -> Options {
        self.options
    }
}

impl BackendTrait for Backend {
    fn confine(&mut self, sandbox: &Sandbox, plan: &LaunchPlan) -> Result<Confinement> {
        // `Copy`, so handing the options on costs nothing and leaves the backend
        // able to answer [`Self::options`] afterwards — the shape the Linux
        // backend reaches for with a clone, which it needs because its options
        // carry a `PathBuf`.
        launch::confine(sandbox, plan, self.options)
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
    Backend::default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::{AccessMode, Error, Sandbox};

    #[test]
    fn the_backend_delegates_to_the_launch() {
        // The one line `impl Backend` is, pinned on the target the crate is built
        // for: the delegation reaches `launch::confine` rather than a second copy
        // of anything. The fixture is a sandbox the launch refuses **before it
        // spawns**, so the assertion costs one profile render and no
        // `sandbox-exec` — and an exposure the profile cannot express is the
        // refusal both paths must render identically.
        let sandbox = Sandbox::new().expose("relative/path", AccessMode::ReadOnly);
        let plan = LaunchPlan::new();

        let via_backend = backend()
            .confine(&sandbox, &plan)
            .expect_err("a relative exposure is refused");
        let direct = confine(&sandbox, &plan, Options::default()).expect_err("the same refusal");
        assert!(
            matches!(via_backend, Error::BadPath { .. }),
            "{via_backend:?}"
        );
        // Equal renderings, not just the same variant: a stub arm answering
        // `Unsupported` for this target is exactly what this test is here to
        // catch, and it would not carry the launch module's own values (`path`
        // and `reason`).
        assert_eq!(format!("{via_backend:?}"), format!("{direct:?}"));
    }

    #[test]
    fn the_backend_carries_the_mechanism_it_was_built_with() {
        // **The one thing the CLI's arm has to get right**: the options it read
        // from the environment reach the launch, so a run selects a mechanism and
        // the whole confinement follows it. A backend that dropped them would run
        // `sandbox-exec` for a `sandbox-init` request — a confinement the operator
        // asked for, delivered by a mechanism they did not choose.
        // A distinct binding name rather than `backend`: the free function
        // `backend()` is in scope here, and a local of the same name shadows it —
        // which is how the assertion below would stop compiling rather than fail.
        let configured =
            Backend::with_options(Options::default().with_mechanism(Mechanism::SandboxInit));
        assert_eq!(configured.options().mechanism(), Mechanism::SandboxInit);
        // The default backend is the helper, unchanged: this is the arm the
        // facade's `platform()` builds and the one every existing run takes.
        assert_eq!(backend().options().mechanism(), Mechanism::SandboxExec);
    }
}
