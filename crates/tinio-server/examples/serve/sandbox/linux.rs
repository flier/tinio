//! Linux in-place confinement: design §10 step 6.

use std::{error::Error, path::Path, process};

use tinio_config::Config;
use tinio_core::mode::AccessMode;
use tinio_sandbox::{Backend, Confinement, LaunchPlan, linux, supervise};
use tinio_server::identity;

use super::{refuse_in_root_state_dir, sandbox_request};
use crate::args::Args;

/// The one name for the construction of the Linux confinement: the backend's
/// options, with the environment's model and the two facts `enter` reads
/// carried onto them.
///
/// **Both facts are resolved here rather than at the call site, because
/// `enter` reads them off `Options` and neither default is right for a
/// server.** `enter` takes the exposure set from `served_root_access` and the
/// retained capability set from `(served_root_access, local_uid_configured)`;
/// with the defaults, a `--read-only` run handed the jail a **read-write**
/// served root — the kernel-level half of FR-023, absent with nothing to say
/// so — and `caps::retained` always returned `&[]`, so the single `CAP_CHOWN`
/// design §8 promises for an FR-033 `local_uid` mapping was never retained and
/// the first `chown` of a landed object failed `EPERM` with no capability to
/// fall back on. `access` is the same **resolved** value the exposure set uses
/// — the config file's `[server] read_only`, `TINIO_READ_ONLY` and the flag
/// already folded together — and the mapping is asked of
/// `identity::owner_uids_for`, the same function the storage's chown map comes
/// from, so the capability this keeps and the chowns it performs cannot
/// disagree about whether a mapping exists.
///
/// `base` rather than a fresh `Options` so the caller's `from_env` — the
/// model, and the refusal that names a bad `TINIO_SANDBOX_MODEL` — is not
/// re-derived or lost here.
#[cfg(all(feature = "sandbox", target_os = "linux"))]
pub(super) fn sandbox_options(
    base: linux::Options,
    config: &Option<Config>,
    access: AccessMode,
) -> linux::Options {
    let mapping_configured =
        !identity::owner_uids_for(config.as_ref().unwrap_or(&Config::default())).is_empty();
    base.with_served_root_access(access)
        .with_local_uid(mapping_configured)
}

/// Design §10 step 6 for the platform that confines **in place**: build the
/// request, enter the jail, report what the confinement could not deliver, and
/// return so the caller's sequence can continue at step 8.
///
/// **This process is the one that binds and the one that serves** (decision 6):
/// it confines itself, then goes back to a `main` that continues with its own
/// listener, its own `Listening` event and its own storage. Nothing is handed
/// back, which is why this arm is a function returning `()` rather than a
/// `Child` — the launcher platform's arm is the other function below, and the
/// design's own §10 draws the same line.
///
/// **Every refusal this arm produces goes to stdout as an `Error` frame, in the
/// same shape both launcher platforms use** (decision 19's "a startup failure is
/// an event rather than an absence"). P1 had none of them until the review pass,
/// for a reason that turned out to be wrong: Linux confines **in place**, so
/// there is no child whose frames a parent relays, and the `Error` frame read as
/// the launcher platforms' business. It is not theirs. A consumer of stdout must
/// not have to know which platform it is on — on a launcher a failure arrives as
/// a frame and on Linux it arrived as nothing at all, which a harness reads as a
/// timeout rather than as a cause. The refusals below are the ones this arm can
/// produce: **design §9's in-root state directory**, the guard both arms run and
/// a `?` here because this arm's failures return to the caller's
/// `emit_on_error`; the model variable naming something that is not a model; and
/// `confine` refusing — the unavailable-model refusal among its reasons, which is
/// the one an operator is most likely to meet. (The `Supervise` arm's own `?` is
/// the fourth call that can fail, and it is unreachable on this platform by
/// construction.) Each also logs where its `Error` is built, so stderr carries
/// the same message through the subscriber the caller installed (shared §7).
#[cfg(all(feature = "sandbox", target_os = "linux"))]
pub fn confine_in_place(
    args: &Args,
    config: &Option<Config>,
    state_dir_path: &Path,
    access: AccessMode,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // The **resolved** mode and not the configuration file's own field:
    // `TINIO_READ_ONLY` and `--read-only` are part of the chain that produced
    // `access`, so reading `config.server.read_only` here would expose
    // the served root `ReadWrite` on a run the operator asked to be
    // read-only — losing the kernel-level half of FR-023 that design §9
    // promises, and doing it silently. `access` **is** the type the exposure
    // set takes, so it is handed over as it stands.
    //
    // **Design §9's refused combination, refused here for the launcher's own
    // reason and in the same position: before the exposure set is built,
    // because the entry it would add is the hole the guard exists to close**
    // (the launcher's call site states the ordering; the Windows arm had the
    // only non-test caller until this line existed). Linux did not reach the
    // guard, and what that cost is the whole of the argument for calling it
    // here rather than leaving it to the storage: with no caller on this
    // platform, an in-root state directory travelled into the confinement and
    // was answered by `FsStorage` a process later — the "a process too late"
    // the guard's own doc names as the reason the refusal lives in the arm
    // rather than inside the confinement, and the reason its gate was widened
    // past `target_os` in the first place. The rule was never Windows-specific;
    // only its first caller was.
    //
    // **The three arguments are the pair `sandbox_request` is handed on the next
    // line, plus the mode that built it**, so the guard decides about the paths
    // the exposure set will actually carry. `&args.root` is the root **as
    // written** — the string the exposure set holds rather than a canonical form,
    // which is the spelling the launcher's arm passes and the one the guard's own
    // doc argues for. `state_dir_path` is the state directory this arm both
    // exposes and hands on to the storage, and in the only runs the guard can
    // refuse (read-only ones) it is always the resolved relocation —
    // `TINIO_STATE_DIR` as written, or the derived home layout — because `main`
    // substitutes the `<root>/.tinio` default only in read-write mode, where the
    // mode's own conjunct has already answered. The mode is
    // `access.is_read_only()` and not `args.read_only`, for the reason the
    // comment above gives: the flag is one of the three sources the **resolved**
    // mode was folded from, and `main` derives the launcher's `read_only`
    // parameter from that same chain, so this is that value rather than a second
    // derivation of it.
    //
    // A `?` rather than an `emit_error` of this arm's own, which is the shape
    // the launcher's guard refusal has too: this function's failures return to
    // the caller, where `emit_on_error` frames them, so framing it here as well
    // would put one sentence on stdout twice.
    refuse_in_root_state_dir(access.is_read_only(), &args.root, state_dir_path)?;
    let sandbox = sandbox_request(&args.root, state_dir_path, access);

    // The model comes from the environment in P1 (`TINIO_SANDBOX_MODEL`);
    // the CLI carries it as a flag when it owns `serve` (design §1). It
    // reaches the backend through `Options`, which is why this arm names the
    // Linux crate directly instead of going through the facade's
    // `platform()` — the facade returns whatever backend the build targets,
    // and an options value only one of them has cannot be handed to it.
    // Each launcher platform's own arm does the same with its own options
    // type.
    //
    // The two facts `enter` reads that the environment does not carry — the
    // resolved `access` and whether an FR-033 `local_uid` mapping is
    // configured — are folded in by [`sandbox_options`], which is also where
    // they are tested. Left at their defaults they would build a confinement
    // this run did not ask for; see that function.
    // **Neither refusal below frames itself, and that is the caller's job.**
    // `main` wraps this whole function in `emit_on_error`, so a frame emitted at
    // a call site here would be a second one for the same message — measured on
    // 2026-09-23: a run hitting the first of these carried two identical `error`
    // frames, because this arm framed the error *and* the wrapper framed the
    // returned `Err`. The launcher arm states the same division from the other
    // side (`confine_or_launch` frames, its caller returns the error unchanged),
    // and the guard above follows it. **One refusal, one frame**, which is what
    // every leg that counts stdout lines is asserting.
    let options = sandbox_options(linux::Options::from_env()?, config, access);
    let mut backend = linux::Backend::with_options(options);
    // A refusal here is a startup failure like the launcher platforms', and the
    // frame the consumer needs goes out at the call site above this one — a
    // caller that returned without a frame would leave its consumer waiting for
    // a `Listening` that can never come.
    let confinement = backend.confine(&sandbox, &LaunchPlan::new())?;
    match confinement {
        Confinement::InPlace => {
            // The one degradable step says so rather than passing quietly
            // (design §11). **Both statements are kept, because they are
            // not the same statement**: the backend emitted a `warn!` event
            // for the degradation through the subscriber the caller installed,
            // which is what puts it in the log, and this line is the example's
            // own — read by whoever is watching a startup rather than a
            // log, written here because a library cannot assume a
            // subscriber exists. Every log goes to stderr; stdout is the
            // event channel.
            if backend.seccomp() == Some(linux::Seccomp::SkippedUnsupportedArch) {
                eprintln!(
                    "warning: this architecture has no seccomp table, so outbound \
                     connections are NOT blocked; the directory confinement still holds"
                );
            }
        }
        Confinement::Supervise(child) => {
            // Unreachable on Linux by construction, kept so the call site is
            // platform-neutral; the launcher plans own its real behaviour.
            // The shape is theirs too: `supervise` borrows the child, and
            // the terminal read that follows is what tells a child which
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
