//! Shared unit-test fixtures (`#[cfg(test)]` only): the in-memory store
//! handle and the ETag helper the row-table suites re-declared after they
//! moved inline from `tests/rows.rs` (2026-10-01) — one home, so a fixture
//! change lands once; and the process's span recorder, the shared harness's.

use std::time::{Duration, SystemTime};

use tinio_util::testing::spans;

use crate::{
    _core::{
        etag::ETag,
        object_lock::{LegalHold, Lock, Mode, Retention},
    },
    ensure_all,
    store::{Handle, open_in_memory},
};

/// A lock whose retention ends `secs` after the Unix epoch — the row tables'
/// round-trip fixture. The instant is absolute, so a re-read row compares
/// equal without depending on when the test ran.
pub(crate) fn lock(mode: Mode, secs: u64, hold: LegalHold) -> Lock {
    Lock {
        retention: Some(Retention {
            mode,
            retain_until: SystemTime::UNIX_EPOCH + Duration::from_secs(secs),
        }),
        legal_hold: hold,
    }
}

/// A ready store handle over a fresh in-memory redb database — the
/// byte-format guard's home: no tempdir, no fs `Handle`, no tokio.
pub(crate) fn handle() -> Handle {
    let handle = Handle::new(open_in_memory().unwrap());
    handle.write(ensure_all).unwrap();
    handle
}

/// A short single-upload ETag.
pub(crate) fn etag(hex: &str) -> ETag {
    hex.parse::<ETag>().unwrap()
}

/// The process's shared span recorder, installed on first use.
///
/// **Process-global and installed once**: a thread-scoped subscriber leaves a
/// callsite's interest cache cold for every other test thread, so a span a leg
/// waits for can be dropped in silence. A leg isolates itself by snapshotting
/// `watermark()` and reading `spans_since()` — the recorded thread keeps a
/// parallel test's spans out.
pub(crate) fn capture_spans() -> spans::Capture {
    spans::install();
    spans::capture()
}
