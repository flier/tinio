//! Streaming SQL SELECT engine for S3 objects (S3 Select).
//!
//! Synchronous pipeline: decompress, read records per the input format,
//! evaluate the SQL plan, serialize rows, and pack whole records into
//! events capped at 1 MB (the async bridge lives in tinio-server).
//! `Parse`/`Unsupported` are request-level errors the server maps to 400
//! before streaming; the rest surface as in-stream error items — see
//! `error::SelectError` and
//! `docs/superpowers/specs/2026-09-04-select-object-content-design.md`.

pub mod error;
pub mod row;

pub use error::SelectError;
