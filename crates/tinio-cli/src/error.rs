//! CLI errors (task T022).
//!
//! User-facing messages with the documented exit codes (contracts/cli.md):
//! `0` success, `1` operational error, `2` usage error.

use std::io;

use handlebars::RenderError;

#[cfg(feature = "api")]
use crate::_api;
use crate::{
    _config, _core::storage, _fs::Error as FsError, _sandbox::Error as SandboxError,
    _server::Error as ServerError, sandbox::denied_sentence_for,
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
    Config(#[from] _config::Error),
    /// A storage-contract failure — exit code 1.
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),
    /// A storage-backend failure — exit code 1.
    #[error("storage error: {0}")]
    Fs(#[from] FsError),
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
    /// **`refused` is the subject, rendered**: a path in backticks, a channel by name, or `an
    /// operation the platform did not name` where the platform named nothing (the rendering is
    /// `crate::sandbox`'s, from the core's `Refused`, so the sentence and the value cannot
    /// drift). Naming the subject is the point of the variant: `Permissions denied` alone
    /// leaves a confined operator with nothing to expose.
    ///
    /// **The frame around that subject is `crate::sandbox`'s too**, so this file carries no
    /// format string of its own: the attribute below hands the two fields to the module's own
    /// renderer (`denied_sentence_for`, the type behind it), which is the definer
    /// [`describe`](crate::sandbox::describe) builds a launcher's denial sentence on — a
    /// rewording cannot reach one of the two renderings and not the other.
    #[error("{}", denied_sentence_for(platform, refused))]
    SandboxDenied {
        /// The platform whose confinement refused.
        platform: &'static str,
        /// What it refused, as the core's `Refused` renders it.
        refused: String,
    },
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
        _config::Error as ConfigError, _core::storage::Error::*, _util::testing::assert_send_sync,
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
        assert_eq!(Error::Storage(NoSuchBucket("data".into())).exit_code(), 1);
    }

    #[test]
    fn displays_variants() {
        let cases = [
            (Error::Usage("unknown flag".into()), "unknown flag"),
            (Error::Operational("port in use".into()), "port in use"),
            (Error::Io(IoError::other("gone")), "I/O error: gone"),
            (
                Error::Storage(NoSuchKey("k".into())),
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
