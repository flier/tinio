//! Linux in-place confinement: design §10 step 6.

use std::process;

use super::sandbox_request;
use crate::{
    _config::Config,
    _core::mode::AccessMode,
    _sandbox::{Backend, Confinement, LaunchPlan, linux, supervise},
    _server::identity,
    Error,
    resolve::Resolution,
};

/// The one name for the construction of the Linux confinement: the backend's options, with the
/// environment's model and the two facts `enter` reads carried onto them.
///
/// **Both facts are resolved here rather than at the call site, because `enter` reads them off
/// `Options` and neither default is right for a server.** `enter` takes the exposure set from
/// `served_root_access` and the retained capability set from `(served_root_access,
/// local_uid_configured)`; with the defaults, a `--read-only` run handed the jail a **read-write**
/// served root — the kernel-level half of FR-023, absent with nothing to say so — and
/// `caps::retained` always returned `&[]`, so the single `CAP_CHOWN` design §8 promises for an
/// FR-033 `local_uid` mapping was never retained and the first `chown` of a landed object failed
/// `EPERM` with no capability to fall back on. `access` is the same **resolved** value the
/// exposure set uses — the config file's `[server] read_only`, `TINIO_READ_ONLY` and the flag
/// already folded together — and the mapping is asked of `identity::owner_uids_for`, the same
/// function the storage's chown map comes from, so the capability this keeps and the chowns it
/// performs cannot disagree about whether a mapping exists.
///
/// `base` rather than a fresh `Options` so the caller's `from_env` — the model, and the refusal
/// that names a bad `TINIO_SANDBOX_MODEL` — is not re-derived or lost here.
pub(super) fn sandbox_options(
    base: linux::Options,
    config: &Config,
    access: AccessMode,
) -> linux::Options {
    let mapping_configured = !identity::owner_uids_for(config).is_empty();
    base.with_served_root_access(access)
        .with_local_uid(mapping_configured)
}

/// Design §10 step 6 for the platform that confines **in place**: build the request, enter the
/// jail, report what the confinement could not deliver, and return so the caller's sequence can
/// continue at step 8.
///
/// **This process is the one that binds and the one that serves** (decision 6): it confines
/// itself, then returns to a `start::run` that continues with its own signal handlers, its own
/// storage, its own listener and its own `Listening` event. Nothing is handed back, which is why
/// this arm returns `()` rather than a `Child` — the launcher platform's arm is the other
/// function, and the design's own §10 draws the same line.
///
/// **Every refusal this arm produces reaches stdout as an `Error` frame, in the same shape both
/// launcher platforms use** (decision 19's "a startup failure is an event rather than an
/// absence"), and it reaches it **through the caller**: `start::run` returns the error, and the
/// server arm of `run` frames it. A frame emitted here as well would put one sentence on stdout
/// twice — measured on 2026-09-23 in the example this arm came from, which framed at the call
/// site *and* through its wrapper. **One refusal, one frame**, which is what every leg that
/// counts stdout lines asserts. A consumer of stdout must not have to know which platform it is
/// on: a Linux refusal that arrived as nothing at all would read as a timeout rather than as a
/// cause.
pub(super) fn confine_in_place(resolution: &Resolution, access: AccessMode) -> Result<(), Error> {
    // The **resolved** mode and not the configuration file's own field: `TINIO_READ_ONLY` and
    // `--read-only` are part of the chain that produced `access`, so reading
    // `config.server.read_only` here would expose the served root `ReadWrite` on a run the
    // operator asked to be read-only — losing the kernel-level half of FR-023 that design §9
    // promises, and doing it silently.
    //
    // **Design §9's refused combination and the exposure set it stands in front of, in one call**:
    // the guard the arm used to spell here is now `sandbox_request`'s own first step — the refusal
    // runs *before* the set is built, which is the order that matters, because the entry the guard
    // exists to close is one the set would otherwise carry. With no caller on this platform, an
    // in-root state directory travelled into the confinement and was answered by `FsStorage` a
    // process later — the "a process too late" the guard's own doc names as the reason the refusal
    // lives in the arm rather than inside the confinement. The rule was never Windows-specific;
    // only its first caller was.
    //
    // **The pair the guard decides about is the pair the exposure set is built from**, because
    // they are one argument now: the resolution the run produced, which is the root the operator's
    // shell spelled (canonicalised by `resolve::resolve_root`) and the state directory the
    // resolution produced. `state_dir` is the directory this arm both exposes and hands on to the
    // storage; in the only runs the guard can refuse (read-only ones) it is always the resolved
    // relocation, because `resolve` substitutes the `<root>/.tinio` default only in read-write
    // mode, where the mode's own conjunct has already answered.
    let sandbox = sandbox_request(resolution, access)?;

    // **Neither refusal below frames itself, and that is the caller's job** — see this
    // function's doc.
    //
    // The model comes from the environment (`TINIO_SANDBOX_MODEL`); it reaches the backend
    // through `Options`, which is why this arm names the Linux crate directly instead of going
    // through the facade's `platform()` — the facade returns whatever backend the build targets,
    // and an options value only one of them has cannot be handed to it. Each launcher platform's
    // own arm does the same with its own options type.
    let options = sandbox_options(linux::Options::from_env()?, &resolution.config, access);
    let mut backend = linux::Backend::with_options(options);
    let confinement = backend.confine(&sandbox, &LaunchPlan::new())?;
    match confinement {
        Confinement::InPlace => {
            // The one degradable step says so rather than passing quietly (design §11). **Both
            // statements are kept, because they are not the same statement**: the backend emitted
            // a `warn!` event for the degradation through the subscriber the caller installed,
            // which is what puts it in the log, and this line is for whoever is watching a
            // startup rather than a log. Every log goes to stderr; stdout is the event channel.
            if backend.seccomp() == Some(linux::Seccomp::SkippedUnsupportedArch) {
                eprintln!(
                    "warning: this architecture has no seccomp table, so outbound \
                     connections are NOT blocked; the directory confinement still holds"
                );
            }
        }
        Confinement::Supervise(child) => {
            // Unreachable on Linux by construction, kept so the call site is platform-neutral;
            // the launcher plans own its real behaviour. The shape is theirs too: `supervise`
            // borrows the child, and the terminal read that follows is what tells a child which
            // never reported from one that is merely quiet (decision 19).
            let mut child = child;
            let status = supervise(&mut child, || ())?;
            if child.terminal().is_none() {
                eprintln!("sandbox: the confined child exited without reporting readiness");
            }
            process::exit(status.code().unwrap_or(1));
        }
    }
    Ok(())
}
