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
