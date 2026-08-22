//! Facade error re-exports.
//!
//! The facade exposes exactly one public error type per implementation crate
//! (per plan.md Project Structure): `tinio-core`'s storage error, tinio-fs,
//! tinio-config, tinio-server, tinio-api, and tinio-cli errors. The
//! conversion chain runs one way — fs → core → S3 error codes → HTTP
//! statuses → CLI exit codes — so no crate leaks another crate's error type.
//!
//! The re-exports land as each crate's error module is implemented
//! (Phase 2 foundational tasks); nothing is public yet.
