//! Which confinement mechanism to use, and whether it is available.
//!
//! `Mechanism` (the request) lives here with `Resolved`, `from_env`,
//! `resolved` and the availability probe that makes `Auto` mean something.
//! The crate root re-exports the public names, so callers keep writing
//! `tinio_sandbox_linux::Mechanism`.

use std::{os::fd::OwnedFd, time::Duration};

use rustix::{
    pipe::pipe,
    runtime::{Fork, exit_group, kernel_fork},
    thread::{UnshareFlags, unshare_unsafe},
};
use tracing::{debug, error};

use crate::_core::{Error, LINUX_MODEL_USAGE, ProbeSteps, Result, await_verdict, report_verdict};
// **The failure builders are the core's, and they are re-exported here rather
// than re-imported by each module.** This file defined all three until the
// phase-basis correction moved them to `tinio-sandbox-core`, beside the other
// platforms' shapes — three copies of one builder being the state that precedes
// three wordings of one log line, which this crate had already reached
// (`enter.rs` said `sandbox step refused` where this file and `seccomp.rs` said
// `sandbox step failed`). `enter.rs` and `seccomp.rs` go on naming them as
// `crate::mechanism::{fail, refused, step}`, so no call site in this crate
// changes with the move; the doc that explained the `refused` decision moved to
// the core's function, trimmed of this crate's history.
pub(crate) use crate::_core::{fail, refused, step};

/// Which confinement mechanism Linux uses (design decision 7).
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Mechanism;
///
/// assert_eq!(Mechanism::default(), Mechanism::Auto);
/// assert_ne!(Mechanism::Root, Mechanism::UserNamespace);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Mechanism {
    /// Prefer the user namespace; resolve the fallback at confinement time.
    #[default]
    Auto,
    /// Force `unshare(CLONE_NEWUSER | CLONE_NEWNS)`.
    UserNamespace,
    /// Force `unshare(CLONE_NEWNS)` with `CAP_SYS_ADMIN`.
    Root,
}

/// What [`Mechanism::resolved`] decided.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::Resolved;
///
/// // The refusal carries the operator-facing reason; a usable model does not.
/// let refused = Resolved::Unavailable("no user namespaces and not root");
/// assert!(matches!(refused, Resolved::Unavailable(reason) if reason.contains("root")));
/// assert_ne!(Resolved::Root, Resolved::UserNamespace);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolved {
    /// Confine with an unprivileged user namespace.
    UserNamespace,
    /// Confine with real root's `CAP_SYS_ADMIN`.
    Root,
    /// Neither is available; startup must fail.
    Unavailable(&'static str),
}

impl Mechanism {
    /// Read `TINIO_SANDBOX_MODEL`. Absent or empty means [`Mechanism::Auto`].
    ///
    /// # Errors
    ///
    /// [`Error::Usage`] for any other value, so a typo cannot silently
    /// select a different mechanism.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::Mechanism;
    ///
    /// assert_eq!(Mechanism::from_env(Some("root")).unwrap(), Mechanism::Root);
    /// // Absent or empty means the adaptive default.
    /// assert_eq!(Mechanism::from_env(None).unwrap(), Mechanism::Auto);
    /// ```
    pub fn from_env(raw: Option<&str>) -> Result<Self> {
        match raw {
            None | Some("") => Ok(Self::Auto),
            Some("userns") => Ok(Self::UserNamespace),
            Some("root") => Ok(Self::Root),
            Some(wrong) => {
                // Logged where the `Error` is built (shared §7): this refusal
                // is otherwise silent, because the caller's `?` only prints it
                // on the way out, and the value that was wrong is the whole
                // diagnosis.
                error!(
                    step = "TINIO_SANDBOX_MODEL",
                    value = wrong,
                    "unsupported model"
                );
                Err(Error::Usage {
                    detail: LINUX_MODEL_USAGE,
                })
            }
        }
    }

    /// Resolve the request against the probe's answer and the effective uid.
    ///
    /// The user namespace is preferred because it needs no privilege at all:
    /// the process maps itself to root inside a namespace it owns, so a bug in
    /// the jailed server cannot reach anything the invoking user could not.
    /// Root is the fallback for hosts that forbid unprivileged user
    /// namespaces.
    ///
    /// `userns` comes from [`userns_available`] rather than from a sysctl,
    /// because the syscall is the authority: a container can report that
    /// unprivileged user namespaces are enabled and still refuse the call. The
    /// probe is a separate function so this decision table stays pure and
    /// exhaustively unit-testable (design decision 7).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_linux::{Mechanism, Resolved};
    ///
    /// // Both inputs are injected, so the table needs no privileges to exercise.
    /// assert_eq!(
    ///     Mechanism::Auto.resolved(1000, true),
    ///     Resolved::UserNamespace
    /// );
    /// assert_eq!(Mechanism::Auto.resolved(0, false), Resolved::Root);
    /// ```
    pub fn resolved(self, euid: u32, userns: bool) -> Resolved {
        match self {
            Self::UserNamespace if userns => Resolved::UserNamespace,
            Self::UserNamespace => Resolved::Unavailable(
                "TINIO_SANDBOX_MODEL=userns was forced but this kernel refused the user \
                 namespace: on Ubuntu 24.04+ set kernel.apparmor_restrict_unprivileged_userns=0 \
                 or give this binary an AppArmor profile; on Debian-family kernels check \
                 kernel.unprivileged_userns_clone; or run as root; or unset TINIO_SANDBOX_MODEL",
            ),
            Self::Auto if userns => Resolved::UserNamespace,
            Self::Auto | Self::Root if euid == 0 => Resolved::Root,
            Self::Root => Resolved::Unavailable(
                "the root model was forced but the process is not root: run with sudo, \
                 or use TINIO_SANDBOX_MODEL=userns",
            ),
            Self::Auto => Resolved::Unavailable(
                "neither confinement model is available: this kernel refused an unprivileged \
                 user namespace and the process is not root — allow unprivileged user \
                 namespaces (Ubuntu 24.04+ blocks them: kernel.apparmor_restrict_unprivileged_userns=0 \
                 or an AppArmor profile for this binary; Debian-family kernels: \
                 kernel.unprivileged_userns_clone) or run with sudo",
            ),
        }
    }
}

/// How long [`userns_available`] waits for its forked child, at each end of the
/// handshake — the read and the reap share the one deadline, which
/// [`await_verdict`] applies to both.
///
/// Generous on purpose rather than tight: the child's whole job is one syscall,
/// one byte and an exit, so a healthy run answers in microseconds and anything
/// this far out is a child that is gone or stuck rather than one that is slow.
/// The value is a bound on a hang, not a performance budget.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The names this probe's own failures carry: the flag, the read and the reap,
/// one step each.
///
/// **Three names rather than one, and that is the half of the handshake that
/// stays this crate's** — [`ProbeSteps`] takes its words from the caller for
/// exactly this reason, since what a failure is *called* belongs to the site that
/// knows what was being probed. A probe that could not be made non-blocking, one
/// whose child never answered, and one whose child could not be reaped are three
/// different diagnoses, and `userns probe read` is the string an operator greps
/// the log for when the model table was decided on a timeout.
const PROBE_STEPS: ProbeSteps = ProbeSteps {
    nonblocking: "userns probe nonblocking",
    read: "userns probe read",
    reap: "userns probe reap",
};

/// Whether this kernel grants an unprivileged `unshare(CLONE_NEWUSER)`.
///
/// **Answered by forking, not by reading a sysctl.** `unshare` changes the
/// calling process for good, so the parent cannot attempt it and then fall back
/// — but a child can attempt it and report. The child unshares, writes one byte
/// to a pipe and exits; the parent reads the pipe and is itself untouched. A
/// sysctl would be cheaper and is the wrong instrument: containers routinely
/// report unprivileged user namespaces as enabled while the seccomp or AppArmor
/// policy refuses the call, and the design's rule is that the syscall is the
/// authority (design decision 7).
///
/// **The parent's half of that handshake is `tinio_sandbox_core`'s now, and this
/// probe gained the shared helper's semantics with it.** The pipe, the fork and
/// the child's body stay here — the fork is this crate's `unsafe` site, and the
/// child's `unshare`, its one byte and its `exit_group` are the half that knows
/// what was being probed — while the non-blocking flag, the bounded read, the
/// `SIGKILL` and the bounded reap are [`await_verdict`]'s. **Two things change
/// with it, and both were this probe's own shape**: it used to leave its child
/// unsignalled on *every* path — the deadline's included, so a probe that timed
/// out left a child running — and it turned any `waitpid` error into the probe's
/// refusal even when the byte it had already read was a sound verdict. The child
/// is now ended on every path, an `ESRCH` from the kill is a `debug!` rather
/// than a refusal, and a reap that fails while the read produced a verdict is
/// tolerated: the verdict is what the model table is decided on, and the reap is
/// cleanup.
///
/// # Preconditions
///
/// **It forks, so no thread may exist yet** (design §10): the child is a copy of
/// the calling thread, which is the thing `kernel_fork`'s signature cannot
/// promise, and `serve` confines before it builds its runtime — and, on this
/// platform, before it installs its subscriber — for exactly that reason. **This crate's own test does not meet the precondition** — libtest
/// runs it on a worker thread with the others beside it, and one of those may be
/// holding the tracing lock when the fork happens, leaving the child blocked if
/// its one event reaches that lock. The failure is bounded and loud rather than
/// a hang: the read end is non-blocking and shares the reap's deadline, so a
/// child that never answers expires as a [`Error::Step`] and the test goes red.
/// The test stays, because it is the only pin on the property the two-tier
/// fallback rests on — the probe does not confine its caller.
///
/// **The confinement's own thread check does not cover this call, and that is
/// the half of the precondition worth stating plainly**: the probe runs
/// inside `enter`'s validation, because the model table cannot be resolved
/// without its answer, while the check that refuses a threaded process sits
/// *after* validation, in front of the first irreversible step — so a process
/// that arrived with a second thread reaches this fork and is refused a few
/// instructions later. That ordering is deliberate (`validate_and_plan` is
/// where an operator's own mistake is meant to be reported as itself), and it
/// means a threaded process's probe is the one fork this crate still makes in a
/// thread group it has not checked. It is bounded and loud for the reason the
/// test's paragraph above gives: a child that deadlocks on a lock the fork
/// copied expires as a [`Error::Step`] and the run refuses.
///
/// # Errors
///
/// [`Error::Step`] when the fork or the pipe itself fails, **or when either end
/// of the handshake runs past `PROBE_TIMEOUT`** — the private constant above,
/// named rather than linked because a private item is not a link target —
/// distinct from a probe that ran and answered "no", which is `Ok(false)` and
/// an ordinary outcome. The read and the reap raise that refusal through
/// [`await_verdict`], which owns the bound.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_linux::userns_available;
///
/// // The answer is a property of the host, not of the call: both are legal.
/// let available = userns_available().unwrap();
/// let _ = available;
/// ```
#[expect(
    unsafe_code,
    reason = "an `unsafe` site of its own: the probe forks with `kernel_fork`, and the child exits having done nothing else of ours"
)]
pub fn userns_available() -> Result<bool> {
    // A child that reaches the unshare and a parent that never does: the answer
    // travels as one byte, and the child always exits without running anything
    // else of ours.
    let (reader, writer): (OwnedFd, OwnedFd) = pipe().map_err(step("userns probe pipe"))?;
    // **`kernel_fork`, and it is the crate's third `unsafe` site.** `rustix` has
    // no safe `fork`: `rustix::process` exports none at all, and the one it does
    // ship is `rustix::runtime::kernel_fork`, which is `unsafe fn` because a
    // fork in a multi-threaded process is not something a signature can promise.
    // The invariant it relies on is design §10's ordering rather than anything
    // this call can check: every production caller confines from a `fn main`
    // that has spawned nothing, so the child is a copy of a single-threaded
    // process and touches nothing but its own pipe end (the one byte
    // `report_verdict` writes through it), the `unshare` wrapper's one event and
    // `exit_group`. **The order the check and this fork stand in is deliberate,
    // and the `# Preconditions` section above states it in full**: `enter`
    // refuses a threaded process *after* `validate_and_plan`, which is where
    // this call sits, so a process that arrived with a second thread is forked
    // from first and refused immediately afterwards.
    // **On this platform's production path that event now has no subscriber,
    // and the ordering that removed it is deliberate.** `serve` installs its
    // subscriber *after* `confine` on the in-place arm — a subscriber built with
    // an OTLP endpoint starts a thread, and a thread alive at the confinement
    // stays outside it for the life of the process — so this child's `error!`,
    // like the confinement's own events, is emitted where nothing is listening.
    // **That is a diagnostic loss and not a silent one**: the parent reports the
    // same fact as its own answer — `Ok(false)` and the fallback to root, or the
    // `Resolved::Unavailable` this module's table answers with, which
    // `enter`'s `resolve_model` turns into an `Error::Unsupported` — and a
    // caller that installs a subscriber before confining still reads the line. The line is left standing rather than silenced: the unshare
    // genuinely did fail, and silencing it would mean the child calling
    // `unshare_unsafe` itself — a fourth `unsafe` site, which decision 14
    // forbids. It also needs rustix's `runtime` feature, on top of the `pipe`
    // one `pipe()` above already uses — both are in the crate's manifest, and
    // both are stated in this plan's tech stack.
    let fork = unsafe { kernel_fork() }.map_err(step("userns probe fork"))?;
    match fork {
        Fork::Child(_) => {
            drop(reader);
            // Through the crate's one wrapper, not a second `unsafe` block: it
            // is the site that carries the safety argument, and this call is
            // the easy half of it — `NEWUSER` alone, in a child that is about
            // to exit.
            let granted = unshare(UnshareFlags::NEWUSER, "userns probe").is_ok();
            // The one byte, through the core's writer, and then the exit. **The
            // child never returns into its own Rust**: the helper says why that
            // is the caller's to do rather than the helper's, and a failed write
            // — a parent that is already gone — is not a reason to do anything
            // else.
            let _ = report_verdict(writer, granted);
            // **`exit_group`, because `rustix::process` has no `exit`**: the
            // exit syscall is `rustix::runtime::exit_group`, which terminates
            // the child's whole thread group — a distinction that costs nothing
            // here, because a fork child is a copy of the calling thread alone
            // and so has exactly one, whatever the parent's count was.
            exit_group(0);
        }
        Fork::ParentOf(child) => {
            drop(writer);
            // **The bound is the parent's at both ends, and the shared helper is
            // where it lives now.** The design's own evidence row — "an
            // unbounded `waitpid` after a fallback `kill` is a harness hazard …
            // a supervisor that reaps a child it may not be able to signal must
            // bound that wait" — describes a probe bounded at both ends by an
            // `alarm` in the child and a `NOHANG` loop in the parent, and this
            // one can only have the second: `rustix` ships no `alarm` (nothing
            // in it sets a timer), and a hand-rolled FFI declaration for one
            // would be a fourth `unsafe` site, which this crate's ledger does
            // not have. So the deadline is the parent's — applied to the read
            // through the non-blocking flag [`await_verdict`] sets and to the
            // reap through its `NOHANG` loop — and the child is ended on every
            // path, this one included.
            let available = await_verdict(reader, child, PROBE_STEPS, PROBE_TIMEOUT)?;
            debug!(
                step = "userns probe",
                available, "unprivileged user namespace probed"
            );
            Ok(available)
        }
    }
}

/// Enter one namespace, and say which one failed when it does.
///
/// **This is the crate's one `unshare` wrapper and one of its three `unsafe`
/// sites**, and it lives here rather than beside `confine` because the probe
/// above is its first caller: two wrappers around one syscall would be two
/// safety arguments to keep in step, and the second could only ever be an
/// accident. **It is `pub(crate)` for the same reason** — Task 5's `enter`
/// imports it rather than writing a second body, and a private item cannot be
/// imported at all.
///
/// The two unshares of the confinement go through this one function because
/// they are one decision — `NEWUSER` and then `NEWNS`, with the map writes
/// between them — and splitting it in half would quietly make the `unsafe`
/// surface three.
///
/// # Safety
///
/// rustix marks `unshare_unsafe` unsafe because `CLONE_FILES` would break a
/// multi-threaded process. This crate never passes it: the confinement calls
/// this twice with `NEWUSER` and `NEWNS`, and the probe passes `NEWUSER` in a
/// forked child that does nothing else and exits.
///
/// **The confinement's half of "no thread exists" is now checked rather than
/// promised**: `enter` refuses a process with more than one task before its
/// first irreversible step, so neither of its two calls here can be made in a
/// process the kernel would refuse `CLONE_NEWUSER` in (design §10). The probe's
/// half is the precondition [`userns_available`] documents and does not
/// enforce — it forks inside a libtest worker in this crate's own test — and
/// that function's doc says so rather than restating it here.
#[expect(
    unsafe_code,
    reason = "an `unsafe` site of its own: `unshare_unsafe` runs with the caller's namespace flags, and `enter` has already refused a process with more than one task"
)]
pub(crate) fn unshare(flags: UnshareFlags, step_name: &'static str) -> Result<()> {
    // SAFETY: the flags are the caller's, and every caller in this crate passes
    // a namespace flag — never `CLONE_FILES` (see above).
    unsafe { unshare_unsafe(flags) }
        .map_err(step(step_name))
        // One event per `unshare`, named by the caller's own step string, so
        // the log says which namespace was entered and a failure and its
        // success are the same operation (shared §7).
        .inspect(|_| debug!(step = step_name, flags = ?flags, "namespace unshared"))
}

#[cfg(test)]
mod tests {
    use rustix::process::geteuid;

    use super::*;

    #[test]
    fn env_selects_the_model() {
        assert_eq!(
            Mechanism::from_env(Some("userns")).unwrap(),
            Mechanism::UserNamespace
        );
        assert_eq!(Mechanism::from_env(Some("root")).unwrap(), Mechanism::Root);
        assert_eq!(Mechanism::from_env(None).unwrap(), Mechanism::Auto);
        assert_eq!(Mechanism::from_env(Some("")).unwrap(), Mechanism::Auto);
        assert!(
            Mechanism::from_env(Some("bogus")).is_err(),
            "a typo must not pick a model"
        );
    }

    #[test]
    fn auto_prefers_the_user_namespace_when_the_probe_grants_it() {
        assert_eq!(
            Mechanism::Auto.resolved(1000, true),
            Resolved::UserNamespace
        );
        assert_eq!(Mechanism::Auto.resolved(0, true), Resolved::UserNamespace);
    }

    #[test]
    fn auto_falls_back_to_root_when_the_probe_fails_and_we_are_root() {
        // Design decision 7's second tier: a hardened host that forbids
        // unprivileged user namespaces still works when started as root.
        assert_eq!(Mechanism::Auto.resolved(0, false), Resolved::Root);
    }

    #[test]
    fn auto_fails_closed_when_neither_tier_is_available() {
        let Resolved::Unavailable(detail) = Mechanism::Auto.resolved(1000, false) else {
            panic!("expected Unavailable");
        };
        assert!(detail.contains("sudo"), "{detail}");
        assert!(detail.contains("unprivileged user namespace"), "{detail}");
    }

    #[test]
    fn forcing_the_user_namespace_never_falls_back_to_root() {
        // Even as root: forcing a model means that model or nothing, so a
        // host with no user namespaces gets a refusal rather than a silent
        // root confinement the operator did not ask for.
        assert_eq!(
            Mechanism::UserNamespace.resolved(0, true),
            Resolved::UserNamespace
        );
        assert!(matches!(
            Mechanism::UserNamespace.resolved(0, false),
            Resolved::Unavailable(_)
        ));
    }

    #[test]
    fn a_forced_root_model_is_refused_when_not_root() {
        assert_eq!(Mechanism::Root.resolved(0, false), Resolved::Root);
        assert!(matches!(
            Mechanism::Root.resolved(1000, false),
            Resolved::Unavailable(_)
        ));
    }

    #[test]
    fn the_unavailable_reason_names_a_remedy() {
        let Resolved::Unavailable(detail) = Mechanism::Root.resolved(1000, false) else {
            panic!("expected Unavailable");
        };
        assert!(
            detail.contains("sudo") || detail.contains("root"),
            "{detail}"
        );
        assert!(detail.contains("userns"), "{detail}");
    }

    #[test]
    fn the_probe_answers_without_privileges_and_leaves_us_unshared() {
        // The probe forks, so the parent is never inside a namespace whatever
        // the answer is. This is the property the two-tier fallback rests on:
        // if the probe leaked its unshare into the parent, `Auto` could never
        // back out of a failed attempt.
        let before = geteuid();
        let _ = userns_available().unwrap();
        assert_eq!(geteuid(), before, "the probe must not confine us");
    }
}
