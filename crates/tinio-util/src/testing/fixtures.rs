//! The fixtures the metadata and Object Lock conformance suites share.
//!
//! [`super::metadata`] and [`super::object_lock`] plant the same rows and read
//! the same clock; the definitions live here so neither conformance module
//! owns a fixture the other reaches for.

use std::time::{Duration, SystemTime};

use super::unique_bucket;
use crate::_core::{
    ETag,
    acl::Acl,
    blob::{BlobRef, BlockHash},
    bucket,
    meta::{
        bucket as bucket_row,
        version::{Kind, Stored},
    },
    object, object_lock, version,
};

/// The time every leg records: a whole second, so a backend whose clock
/// resolves to 100 ns stores it exactly.
const T0: Duration = Duration::from_secs(1_700_000_000);

/// Two valid ETag literals (32 hex digits).
pub(super) const ETAG_A: &str = "d41d8cd98f00b204e9800998ecf8427e";
pub(super) const ETAG_B: &str = "0cc175b9c0f1b6a831c399e269772661";

pub(super) fn t0() -> SystemTime {
    SystemTime::UNIX_EPOCH + T0
}

pub(super) fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid key literal")
}

pub(super) fn bucket_name(prefix: &str) -> bucket::Name {
    bucket::name(unique_bucket(prefix)).expect("a valid bucket name")
}

pub(super) fn etag(value: &str) -> ETag {
    value.parse::<ETag>().expect("a valid ETag literal")
}

/// A fresh bucket row at `created`.
pub(super) fn bucket_at(created: SystemTime) -> bucket_row::Row {
    bucket_row::Row::at(created)
}

/// A retention of `mode` ending `from_now` from this process's clock — the
/// backends' lock-test fixture. A positive span is an active lock; each
/// backend's own clock may read later than this one, so the spans the suite
/// uses leave a safety margin.
pub fn reservation(mode: object_lock::Mode, from_now: Duration) -> object_lock::Retention {
    object_lock::Retention {
        mode,
        retain_until: SystemTime::now() + from_now,
    }
}

/// A governance retention ending far enough ahead that even a backend whose
/// own clock reads later than this process's sees it unexpired.
pub(super) fn governance_reservation() -> object_lock::Retention {
    reservation(object_lock::Mode::Governance, Duration::from_secs(3_600))
}

/// One decoded version row: an object version of five bytes, or a delete
/// marker.
pub(super) fn version_row(seq: u64, id: Option<version::Id>, kind: Kind) -> Stored {
    let size = if kind.is_marker() { 0 } else { 5 };
    Stored {
        id,
        blob: if kind.is_marker() {
            BlobRef::empty()
        } else {
            BlobRef::single(BlockHash::of(b"conf"), size)
        },
        size,
        etag: etag(ETAG_A),
        created: t0(),
        became_noncurrent: 0,
        kind,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        seq,
    }
}
