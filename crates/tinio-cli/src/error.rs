//! CLI errors (task T022).
//!
//! User-facing messages with the documented exit codes (contracts/cli.md):
//! `0` success, `1` operational error, `2` usage error.

use std::io;

use handlebars::RenderError;
#[cfg(test)]
use tinio_util::testing::{bucket_name, object_key};

#[cfg(feature = "api")]
use crate::_api;
use crate::{
    _core::storage, _fs::Error as FsError, _sandbox::Error as SandboxError,
    _server::Error as ServerError, _tier::Error as TierError, _util::state::Error as StateError,
    config,
};

/// A CLI failure carrying its exit code.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::Error;
///
/// let err = Error::Usage("unknown command".into());
/// assert_eq!(err.exit_code(), 2);
/// let err = Error::Operational("port in use".into());
/// assert_eq!(err.exit_code(), 1);
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A usage error (bad arguments) — exit code 2.
    #[error("{0}")]
    Usage(String),
    /// An operational error (startup, runtime) — exit code 1.
    #[error("{0}")]
    Operational(String),
    /// An I/O failure — exit code 1.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A configuration failure — exit code 1.
    #[error("configuration error: {0}")]
    Config(#[from] config::Error),
    /// A state-directory failure — exit code 1.
    #[error("state error: {0}")]
    State(#[from] StateError),
    /// A storage-contract failure — exit code 1.
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),
    /// A storage-backend failure — exit code 1.
    #[error("storage error: {0}")]
    Fs(#[from] FsError),
    /// A tier-layer failure — exit code 1. The layer's own sentence is kept:
    /// its bind refusals (`--adopt`, a root-ID mismatch) are exactly what the
    /// operator has to act on.
    #[error("tier error: {0}")]
    Tier(#[from] TierError),
    /// A data-plane failure — exit code 1.
    #[error("server error: {0}")]
    Server(#[from] ServerError),
    /// A confinement failure — exit code 1.
    #[error("sandbox error: {0}")]
    Sandbox(#[from] SandboxError),
    /// The confinement never applied, so nothing ran confined — exit code 1.
    ///
    /// **The half a sentence cannot carry is the classification.** `Broken` is the answer a
    /// caller needs when a confined process died on the way up: it says the mechanism is what
    /// failed, so nothing was ever run under it — the opposite of [`Self::SandboxDenied`],
    /// which says the mechanism applied and refused an operation. A caller that had only the
    /// sentence could not tell them apart (follow-up §4).
    ///
    /// **The operator's text is the detail's**, so a refusal that moved here from
    /// [`Self::Operational`] reads exactly as it did: the classification is a value, not a
    /// rewrite. **There is no `platform` field**, unlike [`Self::SandboxDenied`]: the process
    /// raising this is the one `std::env::consts::OS` already names, and its sentence names no
    /// platform — so the field would be a second answer nothing reads.
    #[error("{detail}")]
    SandboxBroken {
        /// What was observed instead, in the refusing call site's own words.
        detail: String,
    },
    /// The confinement applied and refused an operation — exit code 1.
    ///
    /// **The sentence is the operator-facing one** — `the confinement on <platform> refused
    /// <subject>`, the `#[error]` format string below — and **`refused` is its subject, already
    /// rendered by `crate::sandbox`'s `describe_refused`**: the core's `Refused` rendered rather
    /// than re-derived, a path in backticks or a channel by name. Naming the subject is the
    /// point of the variant: `Permissions denied` alone leaves a confined operator with nothing
    /// to expose.
    #[error("the confinement on `{platform}` refused {refused}")]
    SandboxDenied {
        /// The platform whose confinement refused.
        platform: &'static str,
        /// What it refused, as the core's `Refused` renders it.
        refused: String,
    },
    /// This host's token cannot confine this process, though the platform can — exit code 1.
    ///
    /// **The sentence is the core error's, rendered rather than re-spelled.** The variant holds
    /// the `tinio-sandbox-core` error the platform built, so `{cause} — {remedy}` is the join
    /// that error's own `#[error]` carries: the text the operator reads, the text a skipped
    /// suite prints and the text the platform measured have one author **and one spelling**. An
    /// earlier shape carried the two halves here and re-typed the join, held equal to the core's
    /// only by a test whose whole job was comparing the two strings; that test is gone with the
    /// second spelling, and the raw HRESULT now rides along instead of being dropped on the way
    /// through (the sentence still carries it in prose).
    ///
    /// **It does not join [`Self::SandboxBroken`] or [`Self::SandboxDenied`]**: `Broken` says
    /// the mechanism never applied and `Denied` says it applied and refused an operation,
    /// while this says the mechanism **could not be applied on this host** — the operator's to
    /// fix rather than the request's. That distinction is why the variant exists at all, and why
    /// a host refusal is not simply carried by [`Self::Sandbox`].
    #[error("{0}")]
    SandboxHostCannotConfine(_sandbox::Error),
    /// A logging-subscriber failure — exit code 1. The subscriber's own error is a boxed
    /// `dyn Error` the crate's `Error` cannot convert (`tinio_server::log::build_subscriber`'s
    /// return), so it is carried as its message rather than wrapped.
    #[error("logging error: {0}")]
    Log(String),
    /// A template failed to render — exit code 1. The handlebars error stays the
    /// [`source`](std::error::Error::source), including its reason and location.
    #[error("{context}: {source}")]
    Render {
        context: &'static str,
        source: RenderError,
    },
    /// A management-plane failure — exit code 1.
    #[cfg(feature = "api")]
    #[error("management error: {0}")]
    Api(#[from] _api::Error),
}

impl Error {
    /// The process exit code for this error (1 operational / 2 usage).
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) => 2,
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use io::Error as IoError;

    use super::*;
    use crate::{
        _core::storage::Error::*,
        _util::{state::Error as StateError, testing::assert_send_sync},
        config::Error as ConfigError,
    };

    #[test]
    fn exit_codes_follow_contract() {
        assert_eq!(Error::Usage("bad flag".into()).exit_code(), 2);
        assert_eq!(Error::Operational("bind failed".into()).exit_code(), 1);
        assert_eq!(Error::Io(IoError::other("x")).exit_code(), 1);
        assert_eq!(
            Error::Config(ConfigError::Missing("api".into())).exit_code(),
            1
        );
        assert_eq!(
            Error::State(StateError::Missing("home directory".into())).exit_code(),
            1
        );
        assert_eq!(
            Error::Storage(NoSuchBucket(bucket_name("data"))).exit_code(),
            1
        );
    }

    #[test]
    fn displays_variants() {
        let cases = [
            (Error::Usage("unknown flag".into()), "unknown flag"),
            (Error::Operational("port in use".into()), "port in use"),
            (Error::Io(IoError::other("gone")), "I/O error: gone"),
            (
                Error::State(StateError::Missing("home directory".into())),
                "state error: missing required value: home directory",
            ),
            (
                Error::Storage(NoSuchKey(object_key("k"))),
                "storage error: no such object: `k`",
            ),
            // The two confinement classifications, and the difference between them is the
            // point: a mechanism that never applied says so in its own detail, while a refusal
            // that happened names the platform and the subject (`crate::sandbox`'s rendering of
            // the core's `Refused`).
            (
                Error::SandboxBroken {
                    detail: "the frame was not applied: the run is refused rather than served \
                             unconfined"
                        .into(),
                },
                "the frame was not applied: the run is refused rather than served unconfined",
            ),
            (
                Error::SandboxDenied {
                    platform: "windows",
                    refused: "`C:\\srv\\data`".into(),
                },
                "the confinement on `windows` refused `C:\\srv\\data`",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn a_host_that_cannot_confine_is_operational_and_renders_the_cores_sentence() {
        // **The pair this crate owns, and only that**: the exit code, beside the rendering the
        // core's own `#[error]` produces. There is no second spelling left to compare.
        let err = Error::SandboxHostCannotConfine(_sandbox::Error::HostCannotConfine {
            cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
            remedy: "run in an ordinary user session",
            hr: 0x8007_0005u32 as i32,
        });
        assert_eq!(err.exit_code(), 1);
        assert_eq!(
            err.to_string(),
            "this token cannot create an AppContainer profile (hr=0x80070005) — run in an ordinary user session"
        );
    }

    #[test]
    fn errors_are_send_sync_and_static() {
        assert_send_sync::<Error>();
    }

    #[test]
    fn a_render_error_keeps_its_source() {
        let missing = "work".to_string();
        let source = handlebars::RenderError::strict_error(Some(&missing));
        let err = Error::Render {
            context: "the systemd unit could not be rendered",
            source,
        };
        assert_eq!(err.exit_code(), 1);
        let kept = StdError::source(&err)
            .and_then(|err| err.downcast_ref::<handlebars::RenderError>())
            .expect("the handlebars error");
        assert_eq!(
            kept.to_string(),
            handlebars::RenderError::strict_error(Some(&missing)).to_string()
        );
    }
}
