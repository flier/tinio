//! Which confinement mechanism to use, and whether it is available.
//!
//! `Mechanism` (the request) lives here with `Resolved`, `from_env`,
//! `resolved` and the availability probe that makes `Auto` mean something.
//! The crate root re-exports the public names, so callers keep writing
//! `tinio_sandbox_linux::Mechanism`.

use std::{
    fs::File,
    io::{self, Read as _, Write as _},
    os::fd::OwnedFd,
    thread::sleep,
    time::{Duration, Instant},
};

use rustix::{
    fs::{OFlags, fcntl_setfl},
    pipe::pipe,
    process::{WaitOptions, waitpid},
    runtime::{Fork, exit_group, kernel_fork},
    thread::{UnshareFlags, unshare_unsafe},
};
use tracing::{debug, error};

use crate::_core::{Error, Result};
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
    /// [`Error::Unsupported`] for any other value, so a typo cannot silently
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
                Err(Error::Unsupported {
                    detail: "TINIO_SANDBOX_MODEL must be `userns` or `root`",
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
/// handshake — the read and the reap share the one deadline.
///
/// Generous on purpose rather than tight: the child's whole job is one syscall,
/// one byte and an exit, so a healthy run answers in microseconds and anything
/// this far out is a child that is gone or stuck rather than one that is slow.
/// The value is a bound on a hang, not a performance budget.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

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
/// # Preconditions
///
/// **It forks, so no thread may exist yet** (design §10): the child is a copy of
/// the calling thread, which is the thing `kernel_fork`'s signature cannot
/// promise, and `serve` confines before it builds its runtime for exactly that
/// reason. **This crate's own test does not meet the precondition** — libtest
/// runs it on a worker thread with the others beside it, and one of those may be
/// holding the tracing lock when the fork happens, leaving the child blocked if
/// its one event reaches that lock. The failure is bounded and loud rather than
/// a hang: the read end is non-blocking and shares the reap's deadline, so a
/// child that never answers expires as a [`Error::Step`] and the test goes red.
/// The test stays, because it is the only pin on the property the two-tier
/// fallback rests on — the probe does not confine its caller.
///
/// # Errors
///
/// [`Error::Step`] when the fork or the pipe itself fails, **or when either end
/// of the handshake runs past `PROBE_TIMEOUT`** — the private constant above,
/// named rather than linked because a private item is not a link target —
/// distinct from a probe that ran and answered "no", which is `Ok(false)` and
/// an ordinary outcome.
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
#[allow(unsafe_code)]
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
    // this call can check: the probe runs before any thread exists (decision 26),
    // so the child is a copy of a single-threaded process and touches nothing but
    // its own pipe end, the `unshare` wrapper's one event and `exit_group`.
    // **That event does fire in production.** `serve` installs its subscriber
    // before `confine` — deliberately, so the confinement's own events are not
    // dropped — and the child inherits it, so a host that refuses the namespace
    // gets an `error!` from the child's failed `unshare` for what the parent
    // reports as an ordinary negative answer. The line is left standing rather
    // than silenced: the unshare genuinely did fail, it costs one entry beside
    // the `Error::Unsupported` the parent logs immediately afterwards, and
    // silencing it would mean the child calling `unshare_unsafe` itself — a
    // fourth `unsafe` site, which decision 14 forbids. It also needs rustix's
    // `runtime` feature, on top of the `pipe` one `pipe()` above already uses —
    // both are in the crate's manifest, and both are stated in this plan's tech
    // stack.
    let fork = unsafe { kernel_fork() }.map_err(step("userns probe fork"))?;
    match fork {
        Fork::Child(_) => {
            drop(reader);
            // Through the crate's one wrapper, not a second `unsafe` block: it
            // is the site that carries the safety argument, and this call is
            // the easy half of it — `NEWUSER` alone, in a child that is about
            // to exit.
            let granted = unshare(UnshareFlags::NEWUSER, "userns probe").is_ok();
            let mut writer = File::from(writer);
            // A failed write means the parent is gone; exiting is the whole job.
            let _ = writer.write_all(&[u8::from(granted)]);
            // **`exit_group`, because `rustix::process` has no `exit`**: the
            // exit syscall is `rustix::runtime::exit_group`, which terminates
            // the child's whole thread group — a distinction that costs nothing
            // here, because the child has exactly one thread, and the same
            // ordering argument (design §10) is what says so.
            exit_group(0);
        }
        Fork::ParentOf(child) => {
            drop(writer);
            // **Both halves of this handshake are bounded, and the reason is the
            // design's own evidence table**: "An unbounded `waitpid` after a
            // fallback `kill` is a harness hazard … A supervisor that reaps a
            // child it may not be able to signal must bound that wait." The
            // probe that row describes is bounded at both ends — an `alarm` in
            // the child and a `NOHANG` loop in the parent — and this one can
            // only have the second: `rustix` ships no `alarm` (nothing in it
            // sets a timer), and a hand-rolled FFI declaration for one would be
            // a fourth `unsafe` site, which this crate's ledger does not have.
            // So the parent carries both, and the read end is made
            // non-blocking — which is what lets the read share the reap's
            // deadline instead of blocking past it.
            let mut reader = File::from(reader);
            fcntl_setfl(&reader, OFlags::NONBLOCK).map_err(step("userns probe nonblocking"))?;
            let deadline = Instant::now() + PROBE_TIMEOUT;
            let mut answer = [0u8; 1];
            let available = loop {
                match reader.read(&mut answer) {
                    // EOF: the child died without answering. A failed probe,
                    // not a "no" — `Ok(false)` is a child that ran and reported
                    // a refusal, which is this probe's ordinary negative.
                    Ok(0) => {
                        return Err(fail(
                            "userns probe read",
                            io::Error::from(io::ErrorKind::UnexpectedEof),
                        ));
                    }
                    Ok(_) => break answer[0] != 0,
                    // `Interrupted` is retried for the same reason `read_exact`
                    // used to retry it internally — a resting pipe is not a
                    // reason to call a probe failed — and `WouldBlock` is what
                    // the non-blocking end reports between the child's write and
                    // the deadline.
                    Err(err)
                        if matches!(
                            err.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                        ) =>
                    {
                        // **Expiry is a refusal, never a false `Ok`.** A probe
                        // that answered "unavailable" on a timeout would send
                        // `Auto` to the root model on a host that never refused
                        // anything — a silent downgrade produced by a hang
                        // (design §11's fail-closed, applied to the question the
                        // model table is built on).
                        if Instant::now() >= deadline {
                            return Err(fail(
                                "userns probe read",
                                io::Error::from(io::ErrorKind::TimedOut),
                            ));
                        }
                        sleep(Duration::from_millis(1));
                    }
                    Err(source) => return Err(fail("userns probe read", source)),
                }
            };
            // The reap, on the same deadline and refusing the same way. The
            // child has written and exited before the read returned on every
            // run that answers, so a `NOHANG` miss that persists to the
            // deadline is a child that is gone or stuck rather than one that is
            // slow. (`WaitOptions::NOHANG` is the kernel's `WNOHANG`; rustix
            // spells the constant without the leading `W`.)
            loop {
                match waitpid(Some(child), WaitOptions::NOHANG) {
                    Ok(Some(_)) => break,
                    Ok(None) if Instant::now() < deadline => {
                        sleep(Duration::from_millis(1));
                    }
                    Ok(None) => {
                        return Err(fail(
                            "userns probe reap",
                            io::Error::from(io::ErrorKind::TimedOut),
                        ));
                    }
                    Err(source) => return Err(fail("userns probe reap", source)),
                }
            }
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
/// forked child that does nothing else and exits. The entry point guarantees no
/// thread exists before either of them (design §10).
#[allow(unsafe_code)]
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
