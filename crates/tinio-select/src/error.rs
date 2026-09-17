use std::io;

use derive_more::{Deref, From, PartialEq};
use thiserror::Error;

use crate::json::error::Error as JsonError;

/// `std::io::Error` compared by `kind()` — the payload is not `PartialEq`,
/// and whole-event-sequence assertions in the events adapter only need the
/// kind.
#[derive(Debug, Deref, From, Error)]
#[error(transparent)]
pub struct IoError(io::Error);

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        self.kind() == other.kind()
    }
}

/// A JSON input failure, nameable from outside the crate.
///
/// `json::Error` stays `pub(crate)`: its variants carry `struson` types
/// (`ValueType`, `JsonReaderPosition`, `UnexpectedStructureKind`), and a public
/// variant holding them would make `struson` a de-facto public dependency. This
/// newtype is the public face `Error::Json` exposes instead — downstream can
/// name and match the variant — and it forwards Display verbatim, so the
/// rendered crate-level message is byte-identical.
#[derive(Debug, Error, From, PartialEq)]
#[error(transparent)]
pub struct JsonInputError(pub(crate) JsonError);

impl JsonInputError {
    /// The 1 MB per-root record cap — the only JSON failure the S3 Select layer
    /// reports specially.
    pub fn is_cap(&self) -> bool {
        matches!(self.0, JsonError::Cap)
    }
}

/// Errors raised by the select pipeline. `Parse`/`Unsupported` are
/// request-level (the server maps them to 400 before streaming);
/// the rest surface as error items inside the 200 event stream
/// (spec §3) under `Custom("S3QueryError")` (grilling Q2).
#[derive(Debug, Error, From, PartialEq)]
pub enum Error {
    #[error("S3 select: {0}")]
    Parse(String),
    #[error("S3 select: unsupported: {0}")]
    Unsupported(String),
    #[error("S3 select: value error: {0}")]
    Value(String),
    #[error("S3 select: ambiguous field: {0}")]
    Ambiguous(String), // -> S3ErrorCode::AmbiguousFieldName (real s3s variant) in-stream
    #[error("S3 select: missing header: {0}")]
    MissingHeader(String), // -> Custom("MissingHeaderName") in-stream
    #[error("S3 select: input error: {0}")]
    Format(String),
    /// JSON input failures: every mode keeps its own value (see json::error).
    #[error("S3 select: input error: {0}")]
    Json(#[from] JsonInputError),
    #[error("S3 select: io error: {0}")]
    #[from(forward)]
    Io(IoError),
    #[error("S3 select: output record exceeds 1 MB limit")]
    TooLarge,
    #[error("S3 select: nested column not supported for CSV output")]
    NestedCsv,
    #[error("S3 select: parquet object exceeds the memory bound")]
    ParquetTooLarge,
}

/// Failure-mode classification for consumers outside the crate, which cannot
/// name `json::Error` (`json::error` is `pub(crate)`) and would otherwise have
/// to match on the Display string. `is_cap` delegates to the classifier on the
/// `Json` payload, so both entry points give one verdict.
impl Error {
    /// The 1 MB per-root record cap — the failure the S3 Select layer reports
    /// specially, and the only one it tells apart from a malformed document.
    pub fn is_cap(&self) -> bool {
        matches!(self, Self::Json(e) if e.is_cap())
    }

    /// Any JSON input failure, the cap included.
    pub fn is_json_input(&self) -> bool {
        matches!(self, Self::Json(_))
    }
}

#[cfg(test)]
mod tests {
    use std::{io as std_io, io::ErrorKind::NotFound};

    use super::{Error, JsonError, JsonInputError};

    #[test]
    fn display_messages() {
        assert_eq!(Error::Parse("p".into()).to_string(), "S3 select: p");
        assert_eq!(
            Error::Unsupported("u".into()).to_string(),
            "S3 select: unsupported: u"
        );
        assert_eq!(
            Error::Value("v".into()).to_string(),
            "S3 select: value error: v"
        );
        assert_eq!(
            Error::Ambiguous("a".into()).to_string(),
            "S3 select: ambiguous field: a"
        );
        assert_eq!(
            Error::MissingHeader("m".into()).to_string(),
            "S3 select: missing header: m"
        );
        assert_eq!(
            Error::Format("f".into()).to_string(),
            "S3 select: input error: f"
        );
        assert_eq!(
            Error::Json(JsonError::Cap.into()).to_string(),
            "S3 select: input error: input record exceeds 1 MB"
        );
        // std::io::Error Display wraps the custom message; assert the framing
        // and payload rather than the exact io formatting.
        let io = Error::from(std_io::Error::other("boom"));
        let msg = io.to_string();
        assert!(msg.starts_with("S3 select: io error:"), "{msg}");
        assert!(msg.contains("boom"), "{msg}");
        assert_eq!(
            Error::TooLarge.to_string(),
            "S3 select: output record exceeds 1 MB limit"
        );
        assert_eq!(
            Error::NestedCsv.to_string(),
            "S3 select: nested column not supported for CSV output"
        );
        assert_eq!(
            Error::ParquetTooLarge.to_string(),
            "S3 select: parquet object exceeds the memory bound"
        );
    }

    #[test]
    fn from_io_error() {
        let io: std_io::Error = std_io::Error::new(NotFound, "missing");
        let e = Error::from(io);
        // Io compares by kind() only, not payload.
        assert_eq!(e, Error::from(std_io::Error::new(NotFound, "other")));
        assert_ne!(e, Error::from(std_io::Error::other("missing")));
    }

    #[test]
    fn partial_eq_same_and_cross() {
        assert_eq!(Error::Parse("x".into()), Error::Parse("x".into()));
        assert_ne!(Error::Parse("x".into()), Error::Parse("y".into()));
        assert_eq!(
            Error::Unsupported("u".into()),
            Error::Unsupported("u".into())
        );
        assert_eq!(Error::Value("v".into()), Error::Value("v".into()));
        assert_eq!(Error::Ambiguous("a".into()), Error::Ambiguous("a".into()));
        assert_eq!(
            Error::MissingHeader("m".into()),
            Error::MissingHeader("m".into())
        );
        assert_eq!(Error::Format("f".into()), Error::Format("f".into()));
        // Unit variants equal themselves only.
        assert_eq!(Error::TooLarge, Error::TooLarge);
        assert_eq!(Error::NestedCsv, Error::NestedCsv);
        assert_eq!(Error::ParquetTooLarge, Error::ParquetTooLarge);
        // Cross-variant never equal, payload ignored across variants.
        assert_ne!(Error::Parse("x".into()), Error::Value("x".into()));
        assert_ne!(Error::TooLarge, Error::NestedCsv);
        assert_ne!(Error::TooLarge, Error::Parse("".into()));
        assert_ne!(Error::Parse("".into()), Error::TooLarge);
    }

    #[test]
    fn is_cap_recognizes_only_the_cap() {
        assert!(Error::Json(JsonError::Cap.into()).is_cap());
        // The payload's classifier and the crate-level one give one verdict.
        assert!(Error::Json(JsonError::Cap.into()).is_json_input());
        assert!(JsonInputError::from(JsonError::Cap).is_cap());
        // Another JSON failure is an input error, but not the cap.
        assert!(!Error::Json(JsonError::Other("json input: boom".into()).into()).is_cap());
        assert!(!JsonInputError::from(JsonError::Other("json input: boom".into())).is_cap());
        assert!(!Error::from(std_io::Error::other("boom")).is_cap());
        assert!(!Error::Format("f".into()).is_cap());
    }

    #[test]
    fn is_json_input_covers_every_json_failure() {
        assert!(Error::Json(JsonError::Cap.into()).is_json_input());
        assert!(Error::Json(JsonError::Other("json input: boom".into()).into()).is_json_input());
        // The CSV-ish format error is a different input failure.
        assert!(!Error::Format("f".into()).is_json_input());
        assert!(!Error::from(std_io::Error::other("boom")).is_json_input());
    }

    #[test]
    fn non_json_errors_are_false_for_every_accessor() {
        for e in [
            Error::Format("f".into()),
            Error::Parse("p".into()),
            Error::TooLarge,
        ] {
            assert!(!e.is_cap(), "{e:?}");
            assert!(!e.is_json_input(), "{e:?}");
        }
    }
}
