//! Shared runtime utilities for tinio: small backend-agnostic helpers
//! built on by both the storage backends and the server.
//!
//! - [`lockmap`] — the evicting per-key lock map (multipart part-write
//!   locks, per-object conditional-PUT locks, per-bucket directory
//!   mutation locks).
//! - `testing` — the conformance test harness (behind the `testing`
//!   feature: requires `tinio-core` types).

pub mod lockmap;

#[cfg(feature = "testing")]
pub mod testing;
