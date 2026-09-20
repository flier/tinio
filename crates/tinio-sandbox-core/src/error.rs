//! The confinement error vocabulary, shared by every backend.

use std::{io, path::PathBuf, result};

/// A confinement failure. Every variant is fatal to startup: design §11
/// requires the process to refuse to run rather than continue unconfined.
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
    #[error("sandbox unsupported here: {detail}")]
    Unsupported {
        /// Why, in operator-facing terms, with the remedy where one exists.
        detail: &'static str,
    },
    /// A path handed to [`crate::Sandbox::expose`] cannot be expressed inside
    /// the confinement — it is not absolute, it does not exist, or it is the
    /// confinement's own root.
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

/// Convenience alias.
pub type Result<T> = result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

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
}
