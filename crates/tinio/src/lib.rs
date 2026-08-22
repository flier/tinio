//! Tinio: an S3-compatible local storage server.
//!
//! This facade crate is the only public API surface of the project (the
//! semver-checks target and rustdoc-example contract, per the constitution).
//! It curates re-exports from the implementation crates — the storage
//! contract from tinio-core, the configuration type from tinio-config, the
//! S3 compatibility layer from tinio-server, the management plane from
//! tinio-api, and the CLI entry from tinio-cli.
//!
//! The curated re-exports (with rustdoc examples per constitution III) land
//! as the underlying modules are implemented: the storage contract arrives
//! with Phase 2, the server/CLI surfaces with US1/US2, and the facade
//! integration tests in `tests/` with task T096. Nothing is public yet.
//!
//! The binary is built from `main.rs`, a two-line delegate to
//! `tinio_cli::run`.
