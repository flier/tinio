use thiserror::Error;

/// Errors raised by the select pipeline. `Parse`/`Unsupported` are
/// request-level (the server maps them to 400 before streaming);
/// the rest surface as error items inside the 200 event stream
/// (spec §3) under `Custom("S3QueryError")` (grilling Q2).
#[derive(Debug, Error)]
pub enum SelectError {
    #[error("S3 select: {0}")]
    Parse(String),
    #[error("S3 select: unsupported: {0}")]
    Unsupported(String),
    #[error("S3 select: value error: {0}")]
    Value(String),
    #[error("S3 select: ambiguous field: {0}")]
    Ambiguous(String),      // -> S3ErrorCode::AmbiguousFieldName (real s3s variant) in-stream
    #[error("S3 select: missing header: {0}")]
    MissingHeader(String),  // -> Custom("MissingHeaderName") in-stream
    #[error("S3 select: input error: {0}")]
    Format(String),
    #[error("S3 select: io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("S3 select: output record exceeds 1 MB limit")]
    TooLarge,
    #[error("S3 select: nested column not supported for CSV output")]
    NestedCsv,
    #[error("S3 select: parquet object exceeds the memory bound")]
    ParquetTooLarge,
}

/// Manual `PartialEq` — `std::io::Error` is not comparable, so `Io` compares
/// by `kind()`; the rest compare payloads. Purpose: whole-event-sequence
/// assertions in the events adapter's tests.
impl PartialEq for SelectError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Parse(a), Self::Parse(b)) => a == b,
            (Self::Unsupported(a), Self::Unsupported(b)) => a == b,
            (Self::Value(a), Self::Value(b)) => a == b,
            (Self::Ambiguous(a), Self::Ambiguous(b)) => a == b,
            (Self::MissingHeader(a), Self::MissingHeader(b)) => a == b,
            (Self::Format(a), Self::Format(b)) => a == b,
            (Self::Io(a), Self::Io(b)) => a.kind() == b.kind(),
            (Self::TooLarge, Self::TooLarge) => true,
            (Self::NestedCsv, Self::NestedCsv) => true,
            (Self::ParquetTooLarge, Self::ParquetTooLarge) => true,
            _ => false,
        }
    }
}
