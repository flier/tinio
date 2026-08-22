//! CLI errors (task T022).
//!
//! User-facing messages with the documented exit codes (contracts/cli.md):
//! `0` success, `1` operational error, `2` usage error.

use std::io;

use tinio_core::storage;

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
    Config(#[from] tinio_config::Error),
    /// A storage-contract failure — exit code 1.
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),
    /// A management-plane failure — exit code 1.
    #[cfg(feature = "api")]
    #[error("management error: {0}")]
    Api(#[from] tinio_api::Error),
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
    use super::*;
    use tinio_core::storage::Error::*;
    use tinio_core::testing::assert_send_sync;

    #[test]
    fn exit_codes_follow_contract() {
        assert_eq!(Error::Usage("bad flag".into()).exit_code(), 2);
        assert_eq!(Error::Operational("bind failed".into()).exit_code(), 1);
        assert_eq!(Error::Io(io::Error::other("x")).exit_code(), 1);
        assert_eq!(
            Error::Config(tinio_config::Error::Missing("api".into())).exit_code(),
            1
        );
        assert_eq!(Error::Storage(NoSuchBucket("data".into())).exit_code(), 1);
    }

    #[test]
    fn displays_variants() {
        let cases = [
            (Error::Usage("unknown flag".into()), "unknown flag"),
            (Error::Operational("port in use".into()), "port in use"),
            (Error::Io(io::Error::other("gone")), "I/O error: gone"),
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
}
