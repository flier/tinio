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
    _server::Error as ServerError,
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
