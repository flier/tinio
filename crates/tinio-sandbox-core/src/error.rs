//! The confinement error vocabulary, shared by every backend.

use std::{io, path::PathBuf, result};

use tracing::error;

/// A confinement failure. Every variant is fatal to startup: design §11
/// requires the process to refuse to run rather than continue unconfined.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Error, fail};
///
/// // What a syscall site builds: the operation's own name, and the platform's error as the
/// // source a caller can still reach.
/// let err = fail("mount", std::io::Error::from_raw_os_error(1));
/// assert!(matches!(err, Error::Step { step: "mount", .. }), "{err}");
/// assert!(std::error::Error::source(&err).is_some(), "{err}");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A syscall or platform call refused. `step` names the operation, never
    /// a path, so the message stays stable when a temporary jail path is
    /// involved.
    #[error("sandbox step `{step}` failed: {source}")]
    Step {
        /// A short operation name (`unshare`, `mount`, `CreateProcess`, …).
        step: &'static str,
        /// The underlying platform error.
        #[source]
        source: io::Error,
    },
    /// The platform cannot confine this process at all.
    ///
    /// Reserved for exactly that: no user namespaces and not root, a stub whose
    /// mechanism is not written yet, a kernel with no seccomp table. **A request
    /// this process cannot satisfy is not this variant** — an exposure set with
    /// nothing in it is reported as [`Self::Step`] with the rejected step named,
    /// so that "unsupported here" keeps pointing at the platform rather than at
    /// the operator's argument.
    ///
    /// **One value of `detail` is an operator typo rather than a missing
    /// platform**, and [`Error::is_usage`] is how a caller tells them apart
    /// without matching the sentence: [`MACOS_MECHANISM_USAGE`].
    #[error("sandbox unsupported here: {detail}")]
    Unsupported {
        /// Why, in operator-facing terms, with the remedy where one exists.
        detail: &'static str,
    },
    /// A path handed to [`crate::Sandbox::expose`] cannot be expressed inside
    /// the confinement — it is not absolute, it does not exist, it is the
    /// confinement's own root, or it is not a directory on a platform whose
    /// grant is per exposed directory (Windows refuses a caller's exposed file
    /// this way rather than granting it nothing; design §6 owns that rule).
    #[error("path `{path}` cannot be exposed: {reason}")]
    BadPath {
        /// The offending path.
        path: PathBuf,
        /// Why it was rejected.
        reason: &'static str,
    },
    /// Something done *to a path* failed.
    ///
    /// Distinct from [`Self::Step`], which names an operation but no path, and
    /// from [`Self::BadPath`], which names a path but carries a static reason
    /// rather than a source. Design §11's Windows rows need both halves at
    /// once: "error naming the directory **and** the underlying failure", where
    /// that failure may be a Win32 code or a tool's exit status and text.
    #[error("sandbox could not {operation} `{path}`: {source}")]
    Launch {
        /// The operation, in operator-facing words (`grant ACL on`, `open`).
        operation: &'static str,
        /// The path it was done to.
        path: PathBuf,
        /// The underlying failure, whichever kind it was.
        #[source]
        source: io::Error,
    },
    /// Waiting for the confined child failed.
    ///
    /// **This is the `#[from]` target for [`std::io::Error`], and the only
    /// `From` impl on this type.** It exists so a launcher can write its wait
    /// closure the way the platform's own API reads —
    /// `Child::new(move || Ok(child.wait()?))` — without naming a step
    /// the core cannot know. A platform that *does* know the operation's name
    /// builds [`Self::Step`] instead, which is why the two never disagree: only
    /// one of them happens by accident.
    #[error("sandbox could not wait for the confined child: {0}")]
    Wait(#[from] io::Error),
}

/// The macOS mechanism selector's refusal when the operator typed neither
/// accepted value.
///
/// **This is the one [`Error::Unsupported`] a caller treats as usage** (exit 2).
/// The sentence stays here so the platform that builds the error and
/// [`Error::is_usage`] cannot drift.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Error, MACOS_MECHANISM_USAGE};
///
/// let err = Error::Unsupported {
///     detail: MACOS_MECHANISM_USAGE,
/// };
/// assert!(err.is_usage());
/// ```
pub const MACOS_MECHANISM_USAGE: &str =
    "TINIO_SANDBOX_MACOS_MECHANISM must be `sandbox-exec` or `sandbox-init`";

impl Error {
    /// Whether this failure is an operator typo (exit 2) rather than a platform
    /// that cannot confine (exit 1).
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_sandbox_core::{Error, MACOS_MECHANISM_USAGE};
    ///
    /// let typo = Error::Unsupported {
    ///     detail: MACOS_MECHANISM_USAGE,
    /// };
    /// let missing = Error::Unsupported {
    ///     detail: "no user namespaces",
    /// };
    /// assert!(typo.is_usage());
    /// assert!(!missing.is_usage());
    /// ```
    pub fn is_usage(&self) -> bool {
        matches!(self, Self::Unsupported { detail } if *detail == MACOS_MECHANISM_USAGE)
    }
}

/// Convenience alias.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Error, Result, fail};
///
/// // Every backend's fallible step is written against this alias, so a `?` in a platform
/// // crate names no type the core does not publish.
/// fn enter() -> Result<()> {
///     Err(fail("unshare", std::io::Error::from_raw_os_error(1)))
/// }
///
/// assert!(matches!(
///     enter(),
///     Err(Error::Step {
///         step: "unshare",
///         ..
///     })
/// ));
/// ```
pub type Result<T> = result::Result<T, Error>;

/// Log a failure and hand it back, so a refusing `return` in any backend is one
/// call and the log cannot drift from the error (shared §7 of the `--sandbox`
/// plans).
///
/// **This is the primitive [`fail`] and [`step`] are written on, and it is
/// here rather than in each backend because §7's rule is one sentence.** The
/// rule asks for the event's `step` and the returned error's `step` to be the
/// same string and to stay that way; what the three backends had instead was
/// three copies of one function — the state that precedes a disagreement, not a
/// rule kept. Linux had already grown two wordings of the event (`sandbox step
/// refused` in one module, `sandbox step failed` in two others) before the
/// copies collapsed, which is what one failure with two log lines looks like
/// from the outside.
///
/// **It is not the primitive [`launch`] is written on, and that is §7 taken
/// literally rather than a second funnel left open.** §7 names `path` as one of
/// the fields an event carries; `launch` is the one constructor that holds a
/// path, and an arbitrary [`Error`] handed to `refused` may be a `Step`, a
/// `BadPath` or an `Unsupported` — so extracting a path here would be guessing
/// at which of those it is. `launch` therefore emits its own event, with the
/// same message, and there are exactly two logging sites for four builders
/// instead of one site that cannot see what it is logging.
///
/// The parameter is a built [`Error`] rather than an [`io::Error`] because not
/// every refusal is a syscall's: Linux's validation refuses with
/// [`Error::BadPath`] and [`Error::Unsupported`], and those are the failures an
/// operator most needs in the log.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::{Error, refused};
///
/// let err = refused(
///     "local_uid",
///     Error::Unsupported {
///         detail: "no user namespaces",
///     },
/// );
///
/// assert!(err.to_string().contains("no user namespaces"), "{err}");
/// ```
pub fn refused(step: &'static str, error: Error) -> Error {
    error!(step, error = %error, "sandbox step failed");
    error
}

/// [`refused`] for a syscall's own error, which is what nearly every call site
/// has.
///
/// Generic over the error type because the callers are not all `std`: `rustix`
/// returns an `Errno`, which converts into [`io::Error`] through its own `From`
/// impl but is not that type. Without the bound every syscall site in a backend
/// would need its own closure — and, worse, a `|err: io::Error| …` written at
/// one of them does not compile at all, because nothing coerces `Errno` to
/// `io::Error` on its own.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::fail;
///
/// let err = fail("mount", std::io::Error::from_raw_os_error(1));
///
/// assert!(err.to_string().contains("mount"), "{err}");
/// ```
pub fn fail(step: &'static str, source: impl Into<io::Error>) -> Error {
    let source = source.into();
    refused(step, Error::Step { step, source })
}

/// The `.map_err` shape of [`fail`], for the call sites that read better as a
/// closure.
///
/// **The bound is [`fail`]'s and it is load-bearing for the same reason**: one
/// helper serves `std`'s errors and `rustix`'s alike, so the call site writes
/// `.map_err(step("mount --bind"))` without a closure that names a type the
/// syscall does not return.
///
/// # Examples
///
/// ```
/// use std::io;
///
/// use tinio_sandbox_core::step;
///
/// let map = step::<io::Error>("mount --bind");
/// let err = map(io::Error::from_raw_os_error(1));
///
/// assert!(err.to_string().contains("mount --bind"), "{err}");
/// ```
pub fn step<Source: Into<io::Error>>(step_name: &'static str) -> impl Fn(Source) -> Error {
    move |source: Source| fail(step_name, source)
}

/// The builder for a failure *about a path*, which is what [`Error::Launch`]
/// records.
///
/// **Named `launch` because the name `step` is taken by the curried form**: Rust
/// has one arity per name, so the other builder — the one that builds
/// [`Error::Launch`] rather than [`Error::Step`] — needs the other name. Both
/// launcher platforms have this shape, because design §11's Windows rows want an
/// error naming the directory *and* the underlying failure together.
///
/// **It emits its own event rather than routing through [`refused`], because it
/// is the one constructor that has a path and shared §7 names `path` as a field
/// an event carries.** The message is [`refused`]'s, so a failure still reads as
/// one wording, and so is the field the payload travels in — `error = %source`,
/// the spelling [`refused`] already emits — so a query for one failure field
/// does not miss half the failures. The other two fields are this constructor's
/// own arguments, `step` and `path`, so a log is greppable by the directory a
/// refusal was about rather than by prose inside the rendered error. `refused`,
/// `fail` and `step` are unchanged and stay the shapes for a failure with no
/// path of its own.
///
/// # Examples
///
/// ```
/// use tinio_sandbox_core::launch;
///
/// let err = launch(
///     "CreateProcessW",
///     "program.exe",
///     std::io::Error::other("boom"),
/// );
///
/// assert!(err.to_string().contains("CreateProcessW"), "{err}");
/// assert!(err.to_string().contains("program.exe"), "{err}");
/// ```
pub fn launch(
    operation: &'static str,
    path: impl Into<PathBuf>,
    source: impl Into<io::Error>,
) -> Error {
    let path = path.into();
    let source = source.into();
    error!(
        step = operation,
        path = %path.display(),
        error = %source,
        "sandbox step failed"
    );
    Error::Launch {
        operation,
        path,
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tracing::subscriber::with_default;

    use super::*;
    use crate::_util::testing::{SharedBuf, assert_step};

    #[test]
    fn a_step_error_names_the_operation() {
        let err = Error::Step {
            step: "mount",
            source: io::Error::from_raw_os_error(1),
        };
        assert!(err.to_string().contains("mount"), "{err}");
    }

    #[test]
    fn unsupported_carries_an_operator_facing_reason() {
        let err = Error::Unsupported {
            detail: "no user namespaces",
        };
        assert!(err.to_string().contains("no user namespaces"));
    }

    #[test]
    fn an_io_error_converts_so_a_wait_closure_can_use_the_question_mark() {
        // The one conversion the launcher platforms' wait closures are written
        // against: `Child::new(move || Ok(child.wait()?))`. Without it
        // there is no `?` that reaches this crate's `Result` from a platform's
        // `std::process` call.
        let err: Error = io::Error::other("boom").into();
        assert!(matches!(err, Error::Wait(_)), "{err}");
    }

    /// Shared §7's "the log's `step` and the error's `step` are the same
    /// string", made a property a test can lose: [`launch`] both builds the
    /// `Error` and emits the event, so removing one of the two breaks this test
    /// and nothing else.
    ///
    /// **It sits beside the helper because both halves are the core's now.** It
    /// was `handles`' recorder test in `tinio-sandbox-windows` until the three
    /// platform copies of the builder collapsed into this one; a platform crate
    /// no longer builds a failure event of its own, so a property pinned there
    /// would have been failing for code that crate does not own.
    ///
    /// It is deliberately one test and not one per operation: the other events
    /// a backend emits are read where they land (shared §7), while this one pins
    /// the rule that the failure path cannot drift from the value the caller
    /// sees.
    ///
    /// **The field assertion is [`assert_step`]'s**, which owns the rule and the
    /// `with_ansi(false)` prerequisite it rests on; what this test adds is the
    /// other half of shared §7's claim — that the `step` the event carries is the
    /// `step` the returned error names.
    ///
    /// **What Ruling 45 changed is the line's shape, not the assertions, and
    /// P3 is told to cite this paragraph.** Until that ruling, the event
    /// rendered the operation a second time inside the error's own text —
    /// `Error::Launch` carries the operation as well as emitting it — so
    /// `CreateProcessW` appeared twice; `launch` now emits `error = %source`,
    /// the plain [`io::Error`], and the name appears once, on the `step` field
    /// alone. A rationale that still said "twice" would be describing a line
    /// nobody writes any more, while the reason for asserting the field rather
    /// than the value is unchanged: the field is the half the log owns.
    /// `path=` is asserted beside it because shared §7 names `path` as one of
    /// the fields an event carries and [`launch`] is the constructor that has
    /// one: a `launch` that went back to routing through [`refused`] keeps
    /// `step` and drops `path`, and this line is what fails when it does.
    ///
    /// The level is the one `serve`'s subscriber enables (`Verbosity::Info`) and
    /// not `TRACE`, so a failure that was moved to `debug!` would stop being
    /// recorded and this test would lose the property rather than keep it.
    ///
    /// **Why this test is still thread-scoped while a shared device exists.**
    /// `tinio_util::testing` carries one recording device for the workspace, and it installs a
    /// **process** default — which this test binary already has: `launcher/adopt.rs`'s tests install a
    /// structured recorder of their own with `set_global_default`, and two global installers in one
    /// binary cannot both hold the default. Whoever installs first wins, the other stays empty, and its
    /// assertions fail for a recording that never happened; that is the hazard the device's own doc
    /// warns about. `with_default` is the one shape that composes with it — it overrides the default on
    /// the thread this test runs on, and leaves adopt's recorder the process's.
    ///
    /// **What that costs is the callsite-cache hazard the device exists to remove**: while no process
    /// default is installed, a parallel test that hits this callsite first can cache it `never` and drop
    /// the event. That is the price of sharing a binary with the one recorder that cannot join the
    /// device, so this is a documented exception rather than the shape to copy — a test in a binary
    /// with no default of its own takes `tinio_util::testing::record_events` or the `capture` device.
    #[test]
    fn a_launch_failure_logs_the_step_and_path_it_reports() {
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
            launch("CreateProcessW", "program", io::Error::other("boom"))
        });

        let Error::Launch { operation, .. } = &error else {
            panic!("launch builds Error::Launch: {error:?}");
        };
        assert_eq!(*operation, "CreateProcessW");
        let logged = String::from_utf8(capture.0.lock().expect("the capture mutex").clone())
            .expect("the capture is UTF-8");
        assert_step(&logged, "CreateProcessW");
        assert!(logged.contains("path=program"), "{logged}");
    }
}
