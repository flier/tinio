//! Shared unit-test fixtures (`#[cfg(test)]` only): the in-memory store
//! handle and the ETag helper the row-table suites re-declared after they
//! moved inline from `tests/rows.rs` (2026-10-01) — one home, so a fixture
//! change lands once.

use redb::{Database, backends::InMemoryBackend};

use crate::{_core::etag::ETag, ensure_all, store::Handle};

/// A ready store handle over a fresh in-memory redb database — the
/// byte-format guard's home: no tempdir, no fs `Handle`, no tokio.
pub(crate) fn handle() -> Handle {
    let db = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let handle = Handle::new(db);
    handle.write(ensure_all).unwrap();
    handle
}

/// A short single-upload ETag.
pub(crate) fn etag(hex: &str) -> ETag {
    ETag::new(hex).unwrap()
}
