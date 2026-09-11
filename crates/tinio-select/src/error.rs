use derive_more::{Deref, From, PartialEq};
use thiserror::Error;

/// `std::io::Error` compared by `kind()` — the payload is not `PartialEq`,
/// and whole-event-sequence assertions in the events adapter only need the
/// kind.
#[derive(Debug, Deref, From, Error)]
#[error(transparent)]
pub struct IoError(std::io::Error);

impl PartialEq for IoError {
    fn eq(&self, other: &Self) -> bool {
        self.kind() == other.kind()
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

#[cfg(test)]
mod tests {
    use super::Error;

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
        // std::io::Error Display wraps the custom message; assert the framing
        // and payload rather than the exact io formatting.
        let io = Error::from(std::io::Error::other("boom"));
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
        let io: std::io::Error = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let e = Error::from(io);
        // Io compares by kind() only, not payload.
        assert_eq!(
            e,
            Error::from(std::io::Error::new(std::io::ErrorKind::NotFound, "other"))
        );
        assert_ne!(e, Error::from(std::io::Error::other("missing")));
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
}
