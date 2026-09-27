//! Applying the profile to **this** process, with `sandbox_init_with_parameters`.
//!
//! The second macOS mechanism (design decision 6): where `sandbox-exec` confines
//! a process it `exec`s, this applies a Seatbelt profile to the process that asks
//! for it. It carries **two** of the crate's three enumerated `unsafe` sites
//! (decision 14): the `libSystem` declarations ([`libsystem`]) and [`validator`]'s
//! `fork`, which is `unsafe` for a reason of its own — a child of a threaded
//! process is not something a signature can promise.
//!
//! # What the route is, and what it costs
//!
//! **Measured on macOS 15.8 (24H23, x86_64), in-process, with the real rendered
//! text and the real `-D` pairs.** The call returns `rc=0`; afterwards threads —
//! `std::thread::spawn`, named spawn, mpsc, `Condvar`, and **every** tokio
//! `new_multi_thread` shape — build and join, `stat` and `readdir` work, and a
//! read, a write and an `accept` on an inherited listener all work, while
//! `fork`/`subprocess` (`deny(1) process-fork`), `exec` of anything but the
//! profile's literal, `bind` and `connect` are refused `EPERM`.
//!
//! **A second apply is refused** — `rc=-1` with `Operation not permitted`, kernel
//! `deny(1) forbidden-sandbox-reinit` — so the call is **one-shot and
//! irreversible**, and a process it has been applied to can apply nothing else.
//! That is why this is a mechanism the *child* selects rather than something a
//! launcher could do on its own behalf: a launcher that applied a profile to
//! itself could no longer create the child's process at all. It is also why
//! [`apply`]'s failure is fatal and not retried or downgraded — there is no
//! second attempt to make.
//!
//! **What the route saves is the `process-exec` rule**, and it is a real saving
//! rather than a tidy one: the exec and the dynamic loader have already happened
//! when the profile lands, so the confined process needs no permission to exec
//! anything, which is the one allowance the other mechanism cannot do without
//! ([`crate::Mechanism::wraps_the_exec`]).
//!
//! # The validator, and why the launcher cannot check a profile itself
//!
//! `sandbox-init` has no pre-flight equivalent: the launcher cannot apply the
//! profile in its own process (it would confine itself and lose the ability to
//! spawn the child) and cannot ask a helper to compile it either — the whole point
//! of the mechanism is that it invokes no `sandbox-exec`. So the check is made
//! **in a forked child**: the parent forks an unconfined child that applies the
//! profile to itself, reports through a pipe, and exits; the parent reaps it and
//! refuses the launch on anything but a report of success. The parent is untouched
//! by the child's apply — `sandbox_init` is per-process — and a profile the kernel
//! refuses is a startup error carrying the same diagnostic the real child would
//! have produced, which is design §11's rule applied to the second mechanism.
//!
//! **The fork is bounded at both ends and reports through a pipe rather than an
//! exit status**, for the reason `tinio-sandbox-linux`'s user-namespace probe is:
//! the child's whole job is one call, one byte and an exit, and a child that never
//! answers must be a refusal rather than a launcher that waits forever.

use std::{
    error::Error as StdError,
    ffi::OsString,
    fmt,
    fs::File,
    io::{self, Read as _, Write as _},
    os::fd::OwnedFd,
    result,
    thread::sleep,
    time::{Duration, Instant},
};

use rustix::{
    fs::{OFlags, fcntl_setfl},
    io::Errno,
    pipe::pipe,
    process::{Pid, Signal, WaitOptions, kill_process, waitpid},
};
use signal_hook::consts::signal::SIGKILL;
use tracing::{debug, error, info};

use crate::{
    _core::{Adopted, Error, Result, fail, step},
    launch::PREFLIGHT_TIMEOUT,
    mechanism::Mechanism,
    profile::rule_count,
};

/// The `libSystem` declarations, and the safe call that wraps them.
///
/// A submodule rather than a block in this file, and the reason is the same one
/// decision 14 gives the Windows crate: the exception belongs on the smallest
/// scope that expresses it, so what is `unsafe` here is one module whose whole job
/// is meeting Apple's ABI, and everything else in this file is safe.
pub mod libsystem;

/// How long [`validator`] waits for its forked child, at each end of the
/// handshake — the read and the reap share the one deadline.
///
/// **A bound on a hang, not a performance budget.** The child's whole job is one
/// `sandbox_init` call on a profile of a few kilobytes, a one-byte write and an
/// exit, so a healthy run answers in microseconds and anything this far out is a
/// child that is gone or stuck rather than one that is slow. The number is the
/// pre-flight's own, because the two mechanisms' checks are the same step of one
/// startup and an operator should not have to learn two of them.
const VALIDATOR_TIMEOUT: Duration = PREFLIGHT_TIMEOUT;

/// The `step` every validator failure names — one spelling, because the log's
/// field and the error's field are the same sentence (shared §7).
const VALIDATOR_STEP: &str = "sandbox-init validator";

/// The `step` every apply record names — one spelling, because the log's field and
/// the error's field are the same sentence (shared §7).
///
/// **The mechanism's name rather than a phrase about the operation**, and the
/// operator's grep is why — the same ruling that made `launch::PREFLIGHT_STEP` a
/// step's name rather than the command's. This moment has a second record on the other
/// side of the seam: the CLI reports the apply under `step = "sandbox-init"`
/// (`tinio-cli`'s `apply_self_confinement`), so one string covers the refusal and the
/// run it refused. The site used to log that name while building its `Error` under
/// `"apply the SBPL profile"` — one failure, two events, two names, and the second one
/// spelled at exactly one site in the crate, which is the drift §7 forbids.
const APPLY_STEP: &str = "sandbox-init";

/// The byte a child writes to answer "the kernel accepted the profile".
const ACCEPTED: u8 = 1;

/// A parameter list, as the flat name/value pairs [`apply`] takes — the shape
/// [`Profile::params`](crate::profile::Profile::params) holds, with each value as
/// the UTF-8 text the launch payload can carry.
///
/// **One conversion, at the boundary the launch payload creates.** A profile's
/// values are `OsString`s because an exposure is a path, and a path on macOS is
/// bytes that need not be UTF-8 — which is why `profile` refuses a non-UTF-8
/// exposure rather than mangling it, and why this conversion cannot fail for a
/// list that reached a launch at all. It is still a `Result`, because the refusal
/// belongs where the conversion is rather than in an `expect`.
///
/// # Errors
///
/// [`Error::Unsupported`] for a value that is not UTF-8, which the profile
/// generator refuses long before a launch.
///
/// # Examples
///
/// ```
/// use std::ffi::OsString;
///
/// use tinio_sandbox_macos::apply::pairs;
///
/// // One exposure's pair, in the shape the render declares it: the name the text
/// // references, and the path it resolves to.
/// let params = vec![(String::from("E0"), OsString::from("/srv/data"))];
/// assert_eq!(
///     pairs(&params).unwrap(),
///     vec![(String::from("E0"), String::from("/srv/data"))]
/// );
/// ```
pub fn pairs(params: &[(String, OsString)]) -> Result<Vec<(String, String)>> {
    params
        .iter()
        .map(|(name, value)| {
            value
                .to_str()
                .map(|value| (name.clone(), value.to_owned()))
                .ok_or(Error::Unsupported {
                    detail: "a profile parameter is not valid UTF-8",
                })
        })
        .collect()
}

/// Apply `profile` to **this process**, resolving its `(param "…")` references
/// from `params`.
///
/// **One-shot and irreversible**: a second call fails, and the process that
/// called it can no longer `fork` or `exec`. Callers apply their confinement and
/// then serve; nothing here may be used to ask a question about a profile, which
/// is what [`validator`] forks for.
///
/// **A parameter a rule references but the launcher never wrote is a refusal**,
/// not an empty value: the kernel's own diagnostic for an undefined `(param …)`
/// is `invalid data type of path filter; expected pattern, got boolean`, and this
/// function's error carries whatever the kernel said rather than inventing a
/// message for it.
///
/// # Errors
///
/// [`Error::Step`] for a profile or a parameter that cannot be a C string (an
/// interior NUL, which the exposure validator refuses earlier — this is the
/// belt) and for the kernel's refusal, which carries libSystem's own diagnostic.
///
/// # Examples
///
/// ```no_run
/// // The **function**, named through the module: this crate has a module called
/// // `apply` as well, so a bare `use tinio_sandbox_macos::apply;` binds the
/// // module and the call below would fail to resolve.
/// use tinio_sandbox_macos::apply::apply;
///
/// // Confines *this* process for good: a caller does this once, before it serves.
/// apply("(version 1)\n(deny default)\n", &[])?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
pub fn apply(profile: &str, params: &[(String, String)]) -> Result<()> {
    // **The `unsafe` is entirely inside [`libsystem`]**, which is why this
    // function carries no allow: what is left here is the mapping from the
    // kernel's answer to this crate's error, and a profile whose refusal must be
    // diagnosed from the kernel's own words.
    libsystem::init(profile, params).map_err(refused_by_the_kernel)?;
    debug!(
        step = APPLY_STEP,
        params = params.len(),
        "the profile was applied to this process"
    );
    Ok(())
}

/// The refusal `libsystem`'s diagnostic becomes: **logged where the [`Error`] is
/// built, under the one step this site names** ([`APPLY_STEP`], shared §7).
///
/// A function rather than the closure this used to be written as, for the reason the
/// const exists at all: the event and the error are one sentence about one failure,
/// and a body that builds both is what makes that a property rather than a habit —
/// the leg below reads the event and the returned error and fails if either stops
/// naming [`APPLY_STEP`].
fn refused_by_the_kernel(diagnostic: String) -> Error {
    error!(
        step = APPLY_STEP,
        diagnostic = %diagnostic,
        "the kernel refused the profile"
    );
    step(APPLY_STEP)(io::Error::other(format!(
        "sandbox_init_with_parameters refused: {diagnostic}"
    )))
}

/// The confinement a child applied to itself, as its launcher's frame asked for it.
///
/// **The three facts the caller's own event reports** (`tinio-cli`'s `step = "sandbox-init"` line):
/// which mechanism the payload named, how many rules the text it applied grants and how many
/// parameters it referenced. The counts are this crate's rather than a caller's because the render
/// is — [`rule_count`](crate::profile::rule_count) reads the SBPL text's own spelling, and a caller
/// counting for itself would be a second reader of one profile.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::{Mechanism, apply::Applied};
///
/// // What a child confined by the in-process mechanism reports to its launcher's
/// // event: which mechanism the frame named, and the shape of the text it applied.
/// let applied = Applied {
///     mechanism: Mechanism::SandboxInit,
///     rules: 4,
///     params: 1,
/// };
/// assert_eq!(applied.mechanism.as_str(), "sandbox-init");
/// assert_eq!(applied.rules, 4);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    /// The mechanism the frame named, resolved and applied by [`self_confinement`].
    pub mechanism: Mechanism,
    /// How many rules the applied text grants — [`rule_count`](crate::profile::rule_count)'s
    /// number.
    pub rules: usize,
    /// How many `(param "…")` values the profile referenced, which is the frame's own list length.
    pub params: usize,
}

/// Why a child did **not** confine itself, when its launcher's frame asked it to.
///
/// **The two payload refusals are one sentence, and this type is its first half**: what the frame
/// asks for and why this process may not do it. What the caller does about it — a run refused
/// rather than served, a probe that fails — is the caller's half, and the two meet at a colon
/// (`tinio-cli`'s `self_confinement_refused` says so from its side). The classes are variants
/// rather than one rendered string so that a caller can answer each in its own words without
/// parsing this type's.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_macos::apply::Refused;
///
/// // The sentence a frame naming the exec-wrapping mechanism gets: this process
/// // cannot apply a confinement the launcher's helper already applied.
/// let refused = Refused::WrapsTheExec {
///     mechanism: String::from("sandbox-exec"),
/// };
/// assert!(refused.to_string().contains("confines the exec"));
/// ```
#[derive(Debug)]
pub enum Refused {
    /// The frame names a mechanism this build does not know.
    ///
    /// **A refusal rather than a fallback to the default**, which is the whole reason
    /// [`Mechanism::from_env`] is a parser: the field is the launcher's word for what it resolved,
    /// and a value this build cannot read is a disagreement between the two processes rather than
    /// a mechanism to guess at.
    UnknownMechanism {
        /// The spelling the frame carried, which is the whole diagnosis.
        mechanism: String,
    },
    /// The frame names the mechanism that confines by **wrapping the child's `exec`**.
    ///
    /// A launcher that resolved that mechanism cannot have put a profile on the frame at all — it
    /// hands the profile to the helper instead — so this is a disagreement between the two
    /// processes rather than a confinement to apply, and applying it would confine a child whose
    /// `exec`-time profile is already in force.
    WrapsTheExec {
        /// The spelling the frame carried.
        mechanism: String,
    },
    /// The frame's own confinement: the profile this process applied was refused.
    ///
    /// Carries [`apply`]'s own [`Error`] — the value any caller of that function gets — so the
    /// diagnostic is libSystem's rather than a second sentence about it.
    Apply(Error),
}

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMechanism { mechanism } => write!(
                formatter,
                "the launch payload asks for confinement by `{mechanism}`, which this build does \
                 not know"
            ),
            Self::WrapsTheExec { mechanism } => write!(
                formatter,
                "the launch payload asks this process to apply `{mechanism}`, but that mechanism \
                 confines the exec rather than the child"
            ),
            // The apply's `Error` is already the whole sentence — its `step` and the kernel's own
            // words — so this arm adds nothing to it.
            Self::Apply(error) => write!(formatter, "{error}"),
        }
    }
}

impl StdError for Refused {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            // The one variant with a cause of its own; the two payload refusals are their own
            // whole diagnosis, which is what their `Display` renders.
            Self::Apply(error) => Some(error),
            Self::UnknownMechanism { .. } | Self::WrapsTheExec { .. } => None,
        }
    }
}

/// Apply the confinement **the launch payload carries** to this process — the child side of the
/// in-process mechanism, and the one home of *which mechanisms a child may apply*.
///
/// **One step, two callers, two framings.** The product's child (`tinio-cli`'s
/// `apply_self_confinement`) reports what it did as a tracing event and turns a refusal into its
/// own `Error`; the harness's child (`tests/harness`) asserts. Both reach the rule through this
/// function, so a mechanism added to [`Mechanism`], or a change to what
/// [`Mechanism::wraps_the_exec`] answers, is one edit here rather than one per consumer.
///
/// **`Ok(None)` is every process that is not the child of this mechanism**: a launcher, a `status`
/// client, an unconfined run, and the child of the mechanism that confines by wrapping its `exec`,
/// whose confinement was applied to it at that exec. `Ok(Some(_))` is a confinement this process
/// has just applied and must now serve under.
///
/// **The mechanism comes off the payload and is checked, not assumed**: the field is the
/// launcher's own word for what it resolved, and a value this build does not know is a refusal
/// naming it rather than a confinement applied by guesswork. The environment is never re-read —
/// the launcher already resolved it, and a second reader would be a second answer.
///
/// **It fails closed, and that is the whole contract.** The apply is one-shot and irreversible, so
/// a process that continued past a refusal would be a server with **no** confinement under a
/// launcher that reported one — the failure this entire seam exists to prevent.
///
/// # Errors
///
/// [`Refused::UnknownMechanism`] for a spelling this build does not know, [`Refused::WrapsTheExec`]
/// for the mechanism that confines at the `exec` — a frame it cannot have written — and
/// [`Refused::Apply`], carrying libSystem's own diagnostic, for the kernel's refusal.
///
/// # Examples
///
/// ```no_run
/// // Both names are written through their modules: this crate has an `apply` module as well as
/// // the `apply` function, and `Adopted`/`SandboxSpec` are the core's.
/// use tinio_sandbox_core::{Adopted, SandboxSpec};
/// use tinio_sandbox_macos::apply::self_confinement;
///
/// // A launch that handed this process no confinement: nothing to apply.
/// let adopted = Adopted::new(String::new(), Some(7), Vec::new(), None, None);
/// assert_eq!(self_confinement(&adopted).expect("no confinement"), None);
///
/// // A frame naming the mechanism that confines at the `exec`: refused, never applied.
/// let helper = Adopted::new(
///     String::new(),
///     Some(7),
///     Vec::new(),
///     None,
///     Some(SandboxSpec {
///         mechanism: String::from("sandbox-exec"),
///         profile: String::new(),
///         params: Vec::new(),
///     }),
/// );
/// assert!(self_confinement(&helper).is_err());
/// ```
pub fn self_confinement(adopted: &Adopted) -> result::Result<Option<Applied>, Refused> {
    let Some(spec) = adopted.confinement() else {
        // The helper's launch, and every process that is not a confined child.
        return Ok(None);
    };
    // **The check parses the payload's spelling through the platform's own parser**, so this
    // build's mechanism list and the wire spelling are one list: a mechanism added later cannot be
    // applied by guesswork here, because the parser does not know it either. What the parser's own
    // refusal names is the *environment* variable, which is not this process's question — the
    // frame is — so the diagnosis below is built from the frame's spelling instead.
    let mechanism = Mechanism::from_env(Some(spec.mechanism.as_str())).map_err(|_| {
        Refused::UnknownMechanism {
            mechanism: spec.mechanism.clone(),
        }
    })?;
    if mechanism.wraps_the_exec() {
        return Err(Refused::WrapsTheExec {
            mechanism: spec.mechanism.clone(),
        });
    }
    apply(&spec.profile, &spec.params).map_err(Refused::Apply)?;
    Ok(Some(Applied {
        mechanism,
        rules: rule_count(&spec.profile),
        params: spec.params.len(),
    }))
}

/// Prove `profile` before the real child is spawned, by forking an unconfined
/// child that applies it to itself.
///
/// **This is `sandbox-init`'s replacement for the `sandbox-exec` pre-flight**
/// (design §11): the helper that would otherwise compile the profile is
/// deliberately not invoked by this mechanism, so the check is made the only other
/// way it can be — a process that applies the very text the child will apply, and
/// reports whether the kernel accepted it.
///
/// **The child's apply is exactly the real child's**, text and parameters
/// included, which is what makes the answer transferable: an undefined
/// `(param …)` is a compile error under this call, so a validator that dropped the
/// pairs would refuse every profile with an exposure.
///
/// **The child reports through a pipe and the parent waits on a deadline.**
/// `_exit` after the apply, so the child runs no `atexit` handler and flushes no
/// stdio buffer of a process whose confinement has just changed underneath it. Its
/// stderr is inherited, so the operator sees the kernel's own line beside the
/// launcher's error.
///
/// **The fork's precondition is the caller's and it is stated rather than
/// assumed**: the child of a multi-threaded process may only call what is
/// async-signal-safe, and this child calls `sandbox_init_with_parameters` — a
/// libSystem function that allocates and takes locks — plus one `write` and
/// `_exit`. The caller is `launch::confine`, which runs before the launcher
/// installs its signal relay or builds any runtime, so the parent is single
/// threaded at that moment; a second thread would make the child's call a hazard
/// rather than a check, and no signature can promise otherwise.
///
/// # Errors
///
/// [`Error::Step`] when the pipe, the fork or the reap fails, when the child
/// outlives [`VALIDATOR_TIMEOUT`], and when the child reports that the kernel
/// refused the profile.
///
/// # Examples
///
/// ```no_run
/// use tinio_sandbox_macos::apply::validator;
///
/// // A profile the kernel compiles: `Ok(())`, and this process is left unconfined —
/// // the apply happened in the forked child. `no_run` because this forks a process,
/// // which is the whole of what it does.
/// validator("(version 1)\n(deny default)\n", &[])?;
/// # Ok::<(), tinio_sandbox_core::Error>(())
/// ```
#[allow(unsafe_code)]
pub fn validator(profile: &str, params: &[(String, String)]) -> Result<()> {
    // A child that applies the profile and a parent that never does: the answer
    // travels as one byte, and the child exits without running anything else of
    // ours.
    let (reader, writer): (OwnedFd, OwnedFd) = pipe().map_err(step(VALIDATOR_STEP))?;
    // SAFETY: see this function's precondition. The child touches its own frame,
    // one pipe and libSystem, and reaches no lock of the parent's that this file
    // can name; it never returns into the parent's Rust, because both of its arms
    // end in `_exit`. The `unsafe` is `fork`'s own contract — that a child of a
    // threaded process is not something a signature can promise — and the
    // alternative is unavailable: this mechanism invokes no helper, so a process
    // that applies the text is the only way to prove it, and the launcher cannot
    // be that process (the apply is irreversible and would cost it the spawn).
    let fork = unsafe { libc::fork() };
    match fork {
        -1 => Err(fail(VALIDATOR_STEP, io::Error::last_os_error())),
        // The child. Its whole job is the apply, the byte and the exit.
        0 => {
            drop(reader);
            let accepted = apply(profile, params).is_ok();
            let mut writer = File::from(writer);
            // A failed write means the parent is gone; exiting is the whole job.
            if writer.write_all(&[u8::from(accepted)]).is_err() {
                debug!(step = VALIDATOR_STEP, "the validator's parent is gone");
            }
            // **`libc::_exit` rather than `std::process::exit`**: a forked child
            // must not run the `atexit` handlers, the stdio flushes or the
            // destructors of a process whose confinement has just changed
            // underneath it, and `_exit` is the call that skips all three. It does
            // not return, so it *is* the arm's value.
            //
            // SAFETY: `_exit` is async-signal-safe, never returns, and takes an
            // `int` this arm owns.
            unsafe { libc::_exit(i32::from(!accepted)) }
        }
        child => {
            drop(writer);
            // **The verdict is read first, and the ending is what comes after it,
            // so a pid this crate cannot name is not a reason to leave a byte
            // unread.** `Pid::from_raw` answers `None` for a value that is not a
            // process, which a kernel that just forked cannot produce — and if it
            // somehow did, the refusal that follows is the honest answer rather
            // than a `Pid` this code invented.
            let verdict = read_verdict(File::from(reader));
            let Some(pid) = Pid::from_raw(child) else {
                return Err(fail(
                    VALIDATOR_STEP,
                    io::Error::other("the forked child's pid is not a process id"),
                ));
            };
            // The child is ended and reaped before the verdict is reported, on
            // every path: a validator whose child is left running, or left as a
            // zombie, would outlive the check it was for — and the one path that
            // needs the ending rather than the wait is the deadline's.
            end(pid, verdict.is_ok())?;
            let accepted = verdict?;
            if accepted {
                info!(step = VALIDATOR_STEP, "the kernel accepted the profile");
                return Ok(());
            }
            error!(
                step = VALIDATOR_STEP,
                "the kernel refused the profile; the launch is refused"
            );
            Err(step(VALIDATOR_STEP)(io::Error::other(
                "the profile was refused by the kernel: see the diagnostic above",
            )))
        }
    }
}

/// The child's one-byte answer, or a refusal when it does not arrive in time.
///
/// **A timeout is a refusal and never an acceptance.** A validator that read a
/// hang as "the kernel was happy" would start a child whose profile was never
/// proved — design §11's fail-closed, applied to the question this function asks.
fn read_verdict(reader: File) -> Result<bool> {
    // The read end is made non-blocking — `F_SETFL`/`O_NONBLOCK`, which is a
    // different flag on a different `fcntl` command than the descriptor flags
    // `launch::make_inheritable` clears — so the read can share the reap's
    // deadline instead of blocking past it.
    fcntl_setfl(&reader, OFlags::NONBLOCK).map_err(step(VALIDATOR_STEP))?;
    let mut reader = reader;
    let deadline = Instant::now() + VALIDATOR_TIMEOUT;
    let mut answer = [0u8; 1];
    loop {
        match reader.read(&mut answer) {
            // EOF: the child died without answering, which is a failed check
            // rather than a "no" — `Ok(false)` is a child that ran and reported
            // the kernel's refusal.
            Ok(0) => {
                return Err(fail(
                    VALIDATOR_STEP,
                    io::Error::from(io::ErrorKind::UnexpectedEof),
                ));
            }
            Ok(_) => return Ok(answer[0] == ACCEPTED),
            // `Interrupted` is retried because a resting pipe is not a reason to
            // call the check failed, and `WouldBlock` is what the non-blocking end
            // reports between the child's write and the deadline.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                // **Expiry is a refusal, never a false `Ok`**: a check that
                // answered "accepted" on a timeout would launch a child against a
                // profile nothing compiled.
                if Instant::now() >= deadline {
                    return Err(fail(
                        VALIDATOR_STEP,
                        io::Error::from(io::ErrorKind::TimedOut),
                    ));
                }
                sleep(Duration::from_millis(1));
            }
            Err(source) => return Err(fail(VALIDATOR_STEP, source)),
        }
    }
}

/// End and reap the validator's child: a `SIGKILL` and a bounded `NOHANG` reap,
/// whichever way the read ended.
///
/// **`SIGKILL` rather than the child's own exit**, because the path that needs
/// this most is the deadline's: a child that has not written its byte inside
/// [`VALIDATOR_TIMEOUT`] is one this process must not wait for, and the kill is
/// what turns a wedged check into an ordinary failure. The reap is bounded for
/// the same reason the read was — a process this one may not be able to signal
/// (it is not this process's to signal once it is a zombie, and the kernel owns
/// the timing) must not become an unbounded wait, which is the harness hazard the
/// design's evidence table records.
///
/// `happy` is whether the verdict itself was sound; the reap's own failures are
/// returned only when it was not, so a healthy check is not turned into a failure
/// by a refused `kill` on a child that had already exited.
fn end(child: Pid, happy: bool) -> Result<()> {
    let deadline = Instant::now() + VALIDATOR_TIMEOUT;
    // A child that has already exited is the ordinary case here, so the signal is
    // attempted and its answer reported in the log rather than returned: `ESRCH`
    // is "it is gone", which is what this call wants.
    if let Some(signal) = Signal::from_named_raw(SIGKILL)
        && let Err(errno) = kill_process(child, signal)
    {
        debug!(
            step = VALIDATOR_STEP,
            ?errno,
            "the validator's child was already gone"
        );
    }
    loop {
        match waitpid(Some(child), WaitOptions::NOHANG) {
            Ok(Some(_)) => return Ok(()),
            Ok(None) if Instant::now() < deadline => sleep(Duration::from_millis(1)),
            Ok(None) => {
                return Err(fail(
                    VALIDATOR_STEP,
                    io::Error::from(io::ErrorKind::TimedOut),
                ));
            }
            // A signal that arrived while this process was in `waitpid` is not a
            // failure of the reap; the poll is retried against the same deadline.
            Err(Errno::INTR) => {}
            // A child this process already reaped, or one it cannot wait for, is
            // reported only when the verdict was unsound anyway: the caller's own
            // refusal is the operator's diagnosis, and this is the second thing
            // that went wrong.
            Err(source) if happy => {
                debug!(step = VALIDATOR_STEP, error = %source, "the validator's child was not reaped");
                return Ok(());
            }
            Err(source) => return Err(fail(VALIDATOR_STEP, source)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, ffi::OsStr, os::unix::ffi::OsStrExt as _};

    use tinio_util::testing::SharedBuf;
    use tracing::subscriber::with_default;

    use super::*;

    /// A refused apply logs the `step` it returns — shared §7's checking rule at the
    /// one site that had two names for it.
    ///
    /// **The refusal path without the refusal**: [`apply`] confines the process that
    /// calls it, one-shot and irreversibly, so a test may not call it — but the
    /// sentence a refusal becomes is a function of the kernel's own diagnostic, and
    /// calling that function under a recording subscriber is what holds the event's
    /// `step` and the error's to one string. A rename at either site fails here rather
    /// than in a log nobody reads.
    ///
    /// **`with_ansi(false)` is load-bearing for a field assertion** (measured in the
    /// core's recorder test): without it the subscriber interleaves escapes into the
    /// field *syntax*, so `step="…"` never appears as such. The level is the one
    /// `serve`'s subscriber enables rather than `TRACE`, so a failure moved to
    /// `debug!` would stop being recorded and this leg would lose the property instead
    /// of keeping it.
    #[test]
    fn the_apply_refusal_logs_the_step_it_returns() {
        let capture = SharedBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let capture = capture.clone();
                move || capture.clone()
            })
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();

        let error = with_default(subscriber, || {
            refused_by_the_kernel(String::from("syntax error: expecting ')'"))
        });

        let Error::Step { step, .. } = &error else {
            panic!("the apply's refusal is the core's `step`: {error:?}");
        };
        assert_eq!(*step, APPLY_STEP);
        let logged = String::from_utf8(capture.0.lock().expect("the capture mutex").clone())
            .expect("the capture is UTF-8");
        assert!(
            logged.contains(&format!("step=\"{APPLY_STEP}\"")),
            "{logged}"
        );
        assert!(logged.contains("diagnostic=syntax error"), "{logged}");
    }

    #[test]
    fn the_validator_accepts_a_profile_the_kernel_compiles() {
        // **The validator's decision, and it is the whole point of the fork**: a
        // profile the kernel accepts answers `Ok`, so the launch proceeds — and
        // the *parent* is left unconfined, which this test asserts implicitly by
        // reaching its own assertions at all. A validator that applied the profile
        // to the wrong process would confine the test binary, and the second half
        // of this test is a file read that the confinement would refuse.
        validator("(version 1)\n(deny default)\n", &[]).expect("the kernel compiles this profile");
        assert!(
            env::current_exe().is_ok(),
            "the parent must still be unconfined after a validator run"
        );
    }

    #[test]
    fn the_validator_refuses_a_profile_the_kernel_rejects() {
        // The refusal class with a *diagnostic*: measured on macOS 15.8, an
        // unbalanced profile is answered with `syntax error: expecting ')'`. The
        // refusal must be an `Err` naming the validator's step, so the launcher
        // reports a startup error rather than spawning a child under a profile
        // nothing compiled.
        let error = validator("(version 1\n(deny default)\n", &[])
            .expect_err("an unbalanced profile must be refused");
        assert!(format!("{error:?}").contains(VALIDATOR_STEP), "{error:?}");
    }

    #[test]
    fn the_validator_refuses_a_profile_whose_parameter_is_undefined() {
        // The other refusal class, and the one the parameter list exists for:
        // measured on macOS 15.8, an undefined `(param …)` is `invalid data type of
        // path filter; expected pattern, got boolean`. A validator that dropped the
        // pairs would accept this profile and the real child would then die on it.
        let error = validator(
            "(version 1)\n(deny default)\n(allow file-read* (subpath (param \"E0\")))\n",
            &[],
        )
        .expect_err("an undefined parameter must be refused");
        assert!(format!("{error:?}").contains(VALIDATOR_STEP), "{error:?}");
    }

    #[test]
    fn the_payload_refusals_are_the_sentence_their_callers_complete() {
        // **The first half of one sentence, pinned as text.** `tinio-cli` appends its own
        // fail-closed tail to exactly this string, and an operator reads the two as one line —
        // so a reworded refusal here silently rewrites the CLI's message and the harness's panic
        // message with it. The third variant is the platform's own `Error`, which is a whole
        // sentence by itself and is asserted where it is built.
        assert_eq!(
            Refused::UnknownMechanism {
                mechanism: String::from("sandbox_init")
            }
            .to_string(),
            "the launch payload asks for confinement by `sandbox_init`, which this build does not \
             know"
        );
        assert_eq!(
            Refused::WrapsTheExec {
                mechanism: String::from("sandbox-exec")
            }
            .to_string(),
            "the launch payload asks this process to apply `sandbox-exec`, but that mechanism \
             confines the exec rather than the child"
        );
    }

    #[test]
    fn a_parameter_that_is_not_utf8_is_refused_rather_than_lossily_converted() {
        // The conversion the payload forces, and the refusal at the other end: a
        // lossy conversion would name a *different* path than the one the operator
        // exposed, which is worse than refusing to launch.
        let params = vec![(
            String::from("E0"),
            OsString::from(OsStr::from_bytes(b"/srv/\xff")),
        )];
        assert!(pairs(&params).is_err());
    }

    #[test]
    fn the_pairs_conversion_is_one_to_one() {
        // The payload's shape and the profile's shape are the same list, so the
        // conversion must not drop, reorder or rewrite an entry: a parameter the
        // text references is resolved from exactly this list.
        let params = vec![
            (String::from("E0"), OsString::from("/srv/data")),
            (String::from("E1"), OsString::from("/srv/state")),
        ];
        assert_eq!(
            pairs(&params).unwrap(),
            vec![
                (String::from("E0"), String::from("/srv/data")),
                (String::from("E1"), String::from("/srv/state")),
            ]
        );
    }
}
