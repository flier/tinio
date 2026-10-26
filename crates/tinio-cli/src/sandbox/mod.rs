//! The `--sandbox` seam: three channels, one decision, one confinement (design §7, decision 10).
//!
//! The three channels are the `--sandbox` flag, `TINIO_SANDBOX` and `[server] sandbox`, and they
//! collapse to one answer in [`plan`], which is also where every *static* refusal is raised —
//! before the fork, so the operator's shell sees it. [`confine`] is the *dynamic* half, after
//! the fork and before the runtime exists: on Linux this process enters the jail **in place**;
//! on Windows and macOS the listener is bound here and a confined child is launched to serve
//! on it.
//!
//! **This module is compiled in every build**, because the feature gates behaviour and not
//! linkage: `TINIO_SANDBOX` and `[server] sandbox` cannot be compiled away, and code that does
//! not exist cannot refuse. The feature takes the `--sandbox` *flag* and nothing else: the
//! variable keeps an argument of its own in that build (hidden, `start::Args::sandbox`'s
//! feature-off declaration), so the channel still reaches this module and still refuses.
//!
//! **Confinement mechanics are [`tinio_sandbox::serve::Spec`]** — this module owns enablement,
//! product refusals, launcher API handoff/`state` publish, and FR-036's OTLP trade
//! (`lost_export`, whose event `crate::commands::start::run` emits); it names no platform
//! `Backend` / `Options` type. The confinement's own report — the resolved model, the exposure
//! set and the network filter's outcome — is read through `serve::Confined`, not restated here.
//!
//! **Two of this module's names are how a caller reads a confinement's own report**, and they
//! are the entry points an operator-facing classification travels through: [`classify_io`] is
//! the io failure a **confined child** met, which is the confinement's refusal when the
//! mechanism is what denied it; and [`describe`] renders the exit a launcher forwards for its
//! child — the child's own
//! [`Attribution`](crate::_sandbox::Child::attribution), which the launcher's call reaches as
//! `Broken` (the one arm a frame is built from) or `Ran` (the arm no frame is built from), and
//! whose doc is where that limit lives. **The strength a backend answers for
//! is not one of them**: that fact is `tinio_sandbox::diagnostics`', read through the facade as
//! `platform_facts` is, and a re-export here would be a second name for a value with one home.
//! Neither name reaches a platform backend: the vocabulary is the core's, the value is the
//! platform's, and this module is where the CLI meets both.

mod child;
mod confine;
mod plan;
mod product;

#[cfg(test)]
mod tests;

use std::{env::consts::OS, io};

pub(crate) use child::on_stop;
pub use child::{adopted_control, adopted_listener, apply_self_confinement, filter_argv};
pub use confine::confine;
pub(crate) use confine::{lost_export, refuse_store_under_confinement};
pub use plan::{Channel, Request, StateDirSource, plan, resolve_flag};
#[cfg(all(test, feature = "sandbox"))]
pub(crate) use product::{
    refuse_in_root_state_dir, refuse_underived_state_dir, refuse_unwritable_state_dir,
    sandbox_request,
};

use crate::{
    _sandbox::{self, Attribution, Refused},
    Error,
};

/// What an [`Attribution`] reads as — **one rendering for the value a call site classifies
/// with and the sentence it reports**.
///
/// `Broken`'s text is its `detail`, so a caller that reports a broken confinement prints the
/// same words whether it went through this function or read the field.
///
/// **`Ran`'s text is the one no frame is built from, and `launcher_exit` withholds it by
/// matching the arm rather than by rendering it**: a child that reported readiness and then
/// ended owes stdout nothing (its stderr is the operator's, unmediated — decision 19), so
/// what this arm renders is what a caller that asks anyway reads — the fact named, with no
/// classification invented over an exit this side has no evidence to read one from.
///
/// # Examples
///
/// ```
/// use tinio_cli::sandbox::describe;
/// use tinio_sandbox::Attribution;
///
/// // The arm no frame is built from renders the fact rather than a refusal: the child
/// // reported readiness and then ended, and nothing on this side can say more than that.
/// assert_eq!(
///     describe(&Attribution::Ran),
///     "the confined child reported readiness and then exited"
/// );
/// ```
pub fn describe(attribution: &Attribution) -> String {
    match attribution {
        Attribution::Broken { detail } => detail.clone(),
        // **The arm that classifies nothing still renders what happened rather than an empty
        // string**: no run frames this one — `launcher_exit` emits nothing for a child that
        // reported and then ended (decision 19 leaves its stderr the operator's) — and the
        // sentence is here so that a caller rendering it anyway reads a fact instead of a hole.
        Attribution::Ran => "the confined child reported readiness and then exited".to_owned(),
    }
}

/// The confinement's own answer to an io failure this process met — **or the failure itself,
/// unchanged, when it is not one**.
///
/// **The gate is "this process is a confined child", and it has to be.** A permission refusal
/// outside a confinement is an ordinary io failure — the operator's own filesystem said no —
/// and calling it a denial would put a sentence about a mechanism on a failure that mechanism
/// never saw. The call sites pass `adopted.is_some()`, the same discriminator the process entry
/// uses to identify a confined child at all (design decision 17): a launcher's own io errors
/// are its own.
///
/// **Only two kinds are classified, and the reason is that only two are distinguishable.**
/// `PermissionDenied` is how all three mechanisms refuse — Seatbelt's `EPERM`, an
/// AppContainer's `ERROR_ACCESS_DENIED`, the Linux jail's refused write — and
/// `ReadOnlyFilesystem` is the same fact from a filesystem that answers `EROFS` instead
/// (a read-only mount). Everything else stays what it was: an `ENOENT` under a refused parent
/// is indistinguishable from a missing file at the syscall boundary, so reading it as a denial
/// would turn a genuinely absent path into a sentence about the confinement.
///
/// **The subject is its own argument** rather than a path, because a refusal is not always
/// about one: the child's control channel is a pipe name on Windows and a socket on macOS, so
/// the classifier takes what the caller knows it was about.
///
/// # Examples
///
/// ```
/// use std::{io, path::Path};
///
/// use tinio_cli::{Error, sandbox::classify_io};
/// use tinio_sandbox::Refused;
///
/// let refused = || Refused::Path(Path::new("/srv/data/.tinio").to_path_buf());
/// let denied = io::Error::from(io::ErrorKind::PermissionDenied);
///
/// // Inside the confinement, the mechanism is what refused — and the sentence names the path.
/// let err = classify_io(denied, refused(), true);
/// assert!(matches!(err, Error::SandboxDenied { .. }), "{err}");
/// assert!(err.to_string().contains("/srv/data/.tinio"), "{err}");
///
/// // Outside it, the same syscall failure is the filesystem's own.
/// let plain = classify_io(
///     io::Error::from(io::ErrorKind::PermissionDenied),
///     refused(),
///     false,
/// );
/// assert!(matches!(plain, Error::Io(_)), "{plain}");
/// ```
pub fn classify_io(err: io::Error, refused: Refused, confined: bool) -> Error {
    if confined && refused_by_confinement(err.kind()) {
        return Error::SandboxDenied {
            platform: OS,
            refused: describe_refused(&refused),
        };
    }
    Error::Io(err)
}

/// The two io kinds a confinement's own refusals arrive as; see [`classify_io`].
fn refused_by_confinement(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::PermissionDenied | io::ErrorKind::ReadOnlyFilesystem
    )
}

/// A refusal's subject, as this crate's sentences spell it: the core's vocabulary rendered
/// rather than re-derived, so the word for "a path" and the word for a channel have one home
/// each. It is the subject half of `Error::SandboxDenied`'s frame, and that variant owns the
/// operator-facing sentence around it.
fn describe_refused(refused: &Refused) -> String {
    match refused {
        Refused::Path(path) => format!("`{}`", path.display()),
        Refused::Channel(name) => format!("the `{name}` channel"),
    }
}

/// Map a facade confinement error into the CLI's exit-code vocabulary.
///
/// An operator typo ([`tinio_sandbox::Error::Usage`]) is usage (exit 2); its message is the
/// operator-facing sentence the platform built, rendered by the core error it arrived as. A
/// child-side handoff this platform does not perform stays the operational sentence the facade's
/// source carries. Every other sandbox failure stays operational (exit 1).
fn sandbox_err(err: _sandbox::Error) -> Error {
    match err {
        // The class is the core's variant, not a sentence a caller matches: one operator mistake
        // answers one exit code whichever platform's selector raised it. The `@` binding renders
        // the core error itself, so the operator's text stays byte-identical to what the
        // `Unsupported` days printed.
        usage @ _sandbox::Error::Usage { .. } => Error::Usage(usage.to_string()),
        // The three handoff steps, named by the crate that raises them: a caller matching the
        // literals here would have this answer (exit 1, operational) change silently under a
        // rename in the facade.
        _sandbox::Error::Step {
            step:
                _sandbox::STEP_ADOPTED_LISTENER
                | _sandbox::STEP_ADOPTED_CONTROL
                | _sandbox::STEP_ON_STOP,
            source,
        } => Error::Operational(source.to_string()),
        // The host's own answer, kept as a value rather than flattened into the generic
        // `Sandbox`: what a caller does about it is not what it does about a step failure.
        // **The core error travels whole** — its own `#[error]` renders the sentence the
        // operator reads — so this arm moves the value instead of taking it apart and rebuilding
        // it, and the raw HRESULT rides along rather than being dropped here.
        host @ _sandbox::Error::HostCannotConfine { .. } => Error::SandboxHostCannotConfine(host),
        other => Error::from(other),
    }
}
