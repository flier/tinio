//! Configuration errors (task T015).
//!
//! The single error type of `tinio-config`: parse failures (I/O, TOML
//! syntax), validation failures (unknown keys, invalid values, missing
//! requirements), and environment-file loading failures. Every failure is a
//! startup error — the configuration is fail-fast (FR-016/FR-021).

use std::{io, path::PathBuf};

/// A configuration failure: parse, validation, or environment loading.
///
/// # Examples
///
/// ```rust
/// use tinio_config::Error;
///
/// let err = Error::Missing("api.https.cert".into());
/// assert_eq!(err.to_string(), "missing required configuration: api.https.cert");
///
/// let err = Error::InvalidValue {
///     key: "server.port".into(),
///     reason: "not a number".into(),
/// };
/// assert!(err.to_string().contains("server.port"));
/// ```
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The config file could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    Io {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// The config file is not valid TOML.
    #[error("failed to parse config file `{path}`: {message}")]
    Parse {
        /// The file being parsed (or `(inline)`).
        path: PathBuf,
        /// The parser message, including the location when available.
        message: String,
    },
    /// An unknown key or section was found (fail-fast, FR-016).
    #[error("unknown configuration key or section: {0}")]
    UnknownKey(String),
    /// A known key holds an invalid value (type, format, or rule violation).
    #[error("invalid value for `{key}`: {reason}")]
    InvalidValue {
        /// The offending key path (e.g. `api.https.cert`).
        key: String,
        /// Why the value is invalid.
        reason: String,
    },
    /// A required key is absent.
    #[error("missing required configuration: {0}")]
    Missing(String),
    /// The `.env` file could not be loaded.
    #[error("failed to load environment file `{path}`: {source}")]
    Env {
        /// The `.env` path.
        path: PathBuf,
        /// The underlying dotenvy error.
        #[source]
        source: dotenvy::Error,
    },
}

/// Config file could not be read.
#[inline]
pub(crate) fn io(path: impl Into<PathBuf>, source: io::Error) -> Error {
    Error::Io {
        path: path.into(),
        source,
    }
}

/// TOML parse failure.
#[inline]
pub(crate) fn parse(path: impl Into<PathBuf>, message: impl Into<String>) -> Error {
    Error::Parse {
        path: path.into(),
        message: message.into(),
    }
}

/// Unknown key or section.
#[inline]
pub(crate) fn unknown_key(message: impl Into<String>) -> Error {
    Error::UnknownKey(message.into())
}

/// `.env` load failure.
#[inline]
pub(crate) fn env(path: PathBuf, source: dotenvy::Error) -> Error {
    Error::Env { path, source }
}

impl Error {
    /// Build an [`Error::InvalidValue`] for a key with a reason.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_config::Error;
    ///
    /// let err = Error::invalid_value("server.port", "not a number");
    /// assert!(err.to_string().contains("server.port"));
    /// ```
    pub fn invalid_value(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidValue {
            key: key.into(),
            reason: reason.into(),
        }
    }

    /// Map a garde validation report onto [`Error::InvalidValue`]
    /// (the first violation, fail-fast).
    pub(crate) fn from_report(report: garde::Report) -> Self {
        match report.iter().next() {
            Some((path, error)) => {
                Self::invalid_value(path.to_string(), error.message().to_string())
            }
            None => parse(PathBuf::from("(validation)"), "validation failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{io, path::PathBuf};

    use super::*;

    #[test]
    fn displays_variants() {
        let cases = [
            (
                Error::Missing("api.https.cert".into()),
                "missing required configuration: api.https.cert",
            ),
            (
                Error::InvalidValue {
                    key: "server.port".into(),
                    reason: "not a number".into(),
                },
                "invalid value for `server.port`: not a number",
            ),
            (
                Error::UnknownKey("unknown field `foo`".into()),
                "unknown configuration key or section: unknown field `foo`",
            ),
            (
                Error::Io {
                    path: PathBuf::from("config.toml"),
                    source: io::Error::other("denied"),
                },
                "failed to read config file `config.toml`: denied",
            ),
            (
                Error::Parse {
                    path: PathBuf::from("config.toml"),
                    message: "bad toml".into(),
                },
                "failed to parse config file `config.toml`: bad toml",
            ),
        ];
        for (err, expected) in cases {
            assert_eq!(err.to_string(), expected);
        }
    }

    #[test]
    fn env_constructor_labels_path_and_source() {
        let err = env(
            PathBuf::from(".tinio/.env"),
            dotenvy::Error::Io(io::Error::other("boom")),
        );
        assert!(matches!(err, Error::Env { .. }));
        assert!(err.to_string().contains(".tinio/.env"));
    }

    #[test]
    fn empty_report_maps_to_parse_error() {
        let err = Error::from_report(garde::Report::new());
        assert!(matches!(err, Error::Parse { .. }));
    }
}
