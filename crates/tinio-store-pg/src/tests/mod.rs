//! The database-backed tests: the conformance suite and the store's own legs.
//!
//! These are ordinary `#[cfg(test)]` legs: each one skips when `TINIO_PG_DSN`
//! is unset, so `cargo test --workspace` stays database-free (PG8, PG15).

pub mod conformance;
pub use crate::test_support as db;
pub mod fixtures;
