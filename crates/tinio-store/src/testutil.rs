//! Shared unit-test fixtures (`#[cfg(test)]` only): the in-memory store
//! handle and the ETag helper the row-table suites re-declared after they
//! moved inline from `tests/rows.rs` (2026-10-01) — one home, so a fixture
//! change lands once.

use crate::{
    _core::etag::ETag,
    ensure_all,
    store::{Handle, open_in_memory},
};

/// A ready store handle over a fresh in-memory redb database — the
/// byte-format guard's home: no tempdir, no fs `Handle`, no tokio.
pub(crate) fn handle() -> Handle {
    let handle = Handle::new(open_in_memory().unwrap());
    handle.write(ensure_all).unwrap();
    handle
}

/// A short single-upload ETag.
pub(crate) fn etag(hex: &str) -> ETag {
    ETag::new(hex).unwrap()
}
