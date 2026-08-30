//! Server errors (task T020).
//!
//! Startup failures (bind, config, storage construction) and S3-mapping
//! failures of the compatibility layer. The S3 protocol layer itself reports
//! standard S3 error codes; this type covers the server's own failures.

use std::io;

use crate::_core::storage;

/// A server failure: startup or S3-mapping.
///
/// # Examples
///
/// ```rust
/// use tinio_server::Error;
///
/// let err = Error::Mapping("unsupported operation".into());
/// assert!(err.to_string().contains("unsupported"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A filesystem/network I/O failure (bind, read, write).
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// A configuration failure (invalid config, missing sections).
    #[error("configuration error: {0}")]
    Config(#[from] crate::_config::Error),
    /// A storage-contract failure surfaced during startup or mapping.
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),
    /// A protocol-mapping failure (operation unsupported by the surface).
    #[error("mapping error: {0}")]
    Mapping(String),
    /// A metrics registration failure.
    #[error("metrics error: {0}")]
    Metrics(#[from] prometheus::Error),
}

#[cfg(test)]
mod tests {
    use io::Error as IoError;
    use prometheus::Error as PrometheusError;

    use super::*;
    use crate::{
        _config::Error as ConfigError, _core::storage::Error::*, _util::testing::assert_send_sync,
    };

    #[test]
    fn displays_variants() {
        let cases = [
            (
                Error::Io(IoError::other("port in use")),
                "I/O error: port in use",
            ),
            (
                Error::Config(ConfigError::Missing("api".into())),
                "configuration error: missing required configuration: api",
            ),
            (
                Error::Storage(NoSuchBucket("data".into())),
                "storage error: no such bucket: `data`",
            ),
            (
                Error::Mapping("no multipart".into()),
                "mapping error: no multipart",
            ),
            (
                Error::Metrics(PrometheusError::Msg("duplicate registration".into())),
                "metrics error: Error: duplicate registration",
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
