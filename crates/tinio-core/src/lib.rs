//! Storage backend contract for the tinio S3 server.
//!
//! This crate is the extension seam of the project: it defines the
//! backend-agnostic domain errors, the async `Storage` contract, the `Cleanup`
//! contract, and key validation — all without any HTTP or filesystem
//! implementation. Concrete backends (tinio-fs is the v1 one) implement the
//! contract and must pass the conformance test harness behind the `testing`
//! feature.
//!
//! The module layout (error, domain, storage, cleanup, keys, testing) is
//! populated by the Phase 2 foundational tasks; nothing is public yet.
