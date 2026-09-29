use std::{ffi::OsString, io};

use tokio::net::TcpListener;

use super::sandbox_err;
use crate::{
    _sandbox::{self, Adopted},
    Error,
};

/// Reads the launch payload out of `argv`, or decides this process is not a confined child.
///
/// The split and the adoption are `tinio_sandbox::filter_argv`:
/// this process's stdin is the reader, and a launcher element is the only thing that makes that
/// read happen.
///
/// # Examples
///
/// ```rust
/// use std::ffi::OsString;
///
/// use tinio_cli::sandbox::filter_argv;
///
/// // No listener element: this process is the launcher, and its stdin is never touched.
/// let argv = vec![OsString::from("server"), OsString::from("/srv/data")];
/// let (argv, adopted) = filter_argv(argv).expect("argv");
/// assert_eq!(argv.len(), 2);
/// assert!(adopted.is_none(), "no launcher put an element in this argv");
/// ```
pub fn filter_argv(argv: Vec<OsString>) -> Result<(Vec<OsString>, Option<Adopted>), Error> {
    Ok(_sandbox::filter_argv(argv, io::stdin())?)
}

/// Confine **this** process, when its launcher's mechanism handed it that job.
///
/// **The child side of the in-process mechanism** (design decision 6): a process
/// whose launch payload carries a `SandboxSpec` applies it to itself here, before it
/// serves anything and before the runtime exists (decision 26). `Ok(())` is every
/// other process — a launcher, a `status` client, an unconfined run, and the child
/// of the mechanism that confines by wrapping its `exec`, whose confinement was
/// already applied to it at that exec.
///
/// **The rule is the contract's** ([`crate::_sandbox::apply_self_confinement`]), and that is
/// what this wrapper delegates to on every target: *which* mechanisms a process may apply to
/// itself is the platform's, and what a frame means on a platform whose confinement is applied
/// when the process is created or in place is the facade's — one home, so a Windows or Linux
/// process handed a frame that carries a spec refuses it instead of serving without the
/// confinement it asked for, and a target that grows an in-process mechanism adds an arm there
/// rather than a call site here. What is this function's is the **framing** (design decision 2,
/// §10, the shape the facade's other child-side names have): the `step = "sandbox-init"` event an
/// operator reads, and the refusal sentence that says what a frame this process could not honour
/// means for the run.
///
/// **It fails closed, and that is the whole contract.** A child under a mechanism
/// that handed it a profile and could not apply it must not serve: the apply is
/// one-shot and irreversible, so a run that continued past a refusal would be a
/// server with **no** confinement under a launcher that reported one — the failure
/// this entire seam exists to prevent.
///
/// **The environment is never re-read** — the launcher already resolved the
/// mechanism, and a second reader would be a second answer.
///
/// **It is not `#[cfg]`-gated, on the platform or on the feature**, because the frame's own
/// presence is what decides whether there is anything to apply: `crate::run` calls it before the
/// parse on every target, and a frame carrying a confinement is refused wherever this build
/// cannot apply one. A `#[cfg]` here would be a target whose forgery serves.
///
/// # Errors
///
/// [`Error::SandboxBroken`] when the apply is refused — carrying the platform's own diagnostic —
/// and when the frame asks for a confinement this build cannot apply to a running process. The
/// variant is the classification and not decoration: the confinement never applied, so nothing
/// ran confined.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::apply_self_confinement;
/// use tinio_sandbox::Adopted;
///
/// // A launch that handed this process no confinement: nothing to apply.
/// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
/// apply_self_confinement(&adopted)?;
/// # Ok::<(), tinio_cli::Error>(())
/// ```
pub fn apply_self_confinement(adopted: &Adopted) -> Result<(), Error> {
    match _sandbox::apply_self_confinement(adopted) {
        // The launcher's own launch, and every process that is not a confined child.
        Ok(None) => Ok(()),
        // **What this process did, in the operator's own vocabulary** — the event the
        // platform cannot emit for it, because the log's `step` is this crate's contract (shared
        // §7) — with the counts the platform's apply is the one that knows.
        Ok(Some(applied)) => {
            tracing::info!(
                step = "sandbox-init",
                mechanism = %applied.mechanism,
                rules = applied.rules,
                params = applied.params,
                "this process confined itself"
            );
            Ok(())
        }
        // **The sentence keeps both halves, and the seam is the colon**: the contract's error
        // says what is wrong with the frame — the platform's diagnosis of it, or the platform's
        // own inability to apply one — and the tail is this crate's, because what a refusal means
        // for the *run* is the CLI's to say. One tail rather than one per class: the diagnosis
        // already names which case it is, and a second sentence per variant would be a copy of a
        // classification the contract owns.
        //
        // **The classification is the error's variant, and this is the one site that raises
        // `SandboxBroken`** (follow-up §4): a frame this process could not apply is a confinement
        // that never applied, so *nothing* ran confined — the failure a caller must be able to
        // tell from a refusal that happened *inside* an applied confinement
        // (`Error::SandboxDenied`, which names its subject). The text is unchanged from the
        // `Operational` refusal this replaced, because the detail is what the variant's sentence
        // is (`Error::SandboxBroken` renders it verbatim) and every existing refusal sentence
        // stays as it was.
        Err(err) => Err(Error::SandboxBroken {
            detail: format!("{err}: the run is refused rather than served unconfined"),
        }),
    }
}

/// The listener a confined child was handed by its launcher.
///
/// **A descriptor is not a socket**: turning one back into a `TcpListener` is the platform
/// crate's conversion — `from_raw_socket` on Windows, `BorrowedFd`/`OwnedFd` on macOS — which is
/// the one place that work may live, because it is that crate's enumerated `unsafe` site and this
/// crate adds none. On both launcher platforms that is the facade's
/// [`crate::_sandbox::adopted_listener`], called here; the child of a launcher is the only process
/// that ever reaches this with a real descriptor.
///
/// **On a target with no launcher the inherited element is a forgery, and the answer is a
/// refusal** rather than a fabricated listener. Linux confines its server **in place**, so no
/// process legitimately hands one over, and an argv carrying the listener element there is
/// something the operator or another process put there. Binding the configured address instead
/// would be the one silent answer available and the wrong one: that socket is not the one the
/// confinement's rules were written against (sandbox design decision 16), so a process that was
/// handed a descriptor is refused rather than served.
///
/// # Errors
///
/// The platform's own conversion failure, and — on a target with no launcher — always.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::adopted_listener;
/// use tinio_sandbox::Adopted;
///
/// # fn listener(adopted: &Adopted) -> Result<(), tinio_cli::Error> {
/// let listener = adopted_listener(adopted)?;
/// # let _ = listener;
/// # Ok(())
/// # }
/// ```
pub fn adopted_listener(adopted: &Adopted) -> Result<TcpListener, Error> {
    // The platform listener already returns a non-blocking `std` socket. This wrapper only
    // crosses into tokio. A target that never hands a listener over is the facade's refusal.
    let listener = _sandbox::adopted_listener(adopted).map_err(sandbox_err)?;
    Ok(TcpListener::from_std(listener)?)
}

/// The control socket a launcher handed this confined child, rebuilt from the descriptor in its
/// argv.
///
/// **The child cannot bind its own control channel, and that is what this name is for.** A unix
/// `bind()` is a network operation the macOS profile grants nothing for — the grant it used to
/// carry was a bind grant rather than a filter, which is exactly what a `(deny default)` profile
/// exists to avoid — so the launcher binds the socket outside the sandbox, clears `FD_CLOEXEC` on
/// it, and puts the descriptor in argv; `tinio_api::Plane::adopt` is where the child serves on
/// what this returns. The rebuild is the platform's, and it is the inherited-descriptor rule
/// [`adopted_listener`] applies, so the facade publishes it under a platform-neutral name for the
/// same reason the listener's is (design decision 2, §10).
///
/// **One socket, and the platform's own refusal for anything else**: a unix control channel is one
/// socket file, so a handoff that carries no descriptor — or more than one — is refused rather
/// than served.
///
/// # Errors
///
/// The platform's own conversion failure: no inherited control descriptor, more than one, or a
/// number that does not fit the kernel's `RawFd`.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::sandbox::adopted_control;
/// use tinio_sandbox::Adopted;
///
/// # fn socket(adopted: &Adopted) -> Result<(), tinio_cli::Error> {
/// let socket = adopted_control(adopted)?;
/// # let _ = socket;
/// # Ok(())
/// # }
/// ```
pub fn adopted_control(adopted: &Adopted) -> Result<_sandbox::ControlSocket, Error> {
    _sandbox::adopted_control(adopted).map_err(sandbox_err)
}

/// The child-side stop hook a confined child arms: where a stop its launcher forwards lands.
///
/// **The second of the two names the child side needs**, beside [`adopted_listener`] (design §3
/// step 9, decision 20's second half). Without it a forwarded stop meets the default
/// disposition, which is to die where the process stands instead of draining — and the launcher
/// forwards, rather than kills, precisely so a stop is a drain. The closure runs on a thread of
/// its own, so what it does may allocate and take locks: sending on the shutdown channel is what
/// it is for.
///
/// **Both launcher platforms arm it**, because both forward the stop rather than killing the
/// child: Windows through its console-control handler, macOS through the signal relay. The two
/// differ in what arrives — a `CTRL_*` event there, a real signal here — and not in what the
/// child does with it.
///
/// # Errors
///
/// The platform's own failure to install the relay.
pub(crate) fn on_stop(hook: impl Fn() + Send + 'static) -> Result<(), Error> {
    _sandbox::on_stop(hook).map_err(sandbox_err)
}
