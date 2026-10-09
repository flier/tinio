//! D41 (design 2026-10-03 §6): the **pre-change database** still opens and
//! serves its rows through the relocated implementations.
//!
//! The fixture `tests/fixtures/pre-change-meta.redb` was written by the
//! pre-change build (a `git archive HEAD` export of the commit before this
//! task, built in a scratch tree outside the repository and driven through
//! the pre-change `FsStorage` write path — the exact commands and their
//! output are in `.superpowers/sdd/2026-10-03-metadata-store-contract-p1-contract-and-fs/task-4-report.md`).
//! This test copies the fixture out of the repository before opening it, so
//! the committed file stays byte-identical; every read below goes through
//! the public surface (`tinio_store_redb`), never the fixture's bytes.

// This target is test context, but its file-level helper (`open_fixture`) is
// not a `#[test]` function, and clippy's `allow-*-in-tests` keys reach
// `#[test]` functions (and `#[cfg(test)]` modules) only — outside one,
// test-support code reads as production to the lint (docs/style.md §Panics;
// design 2026-10-09 D7: test contexts stay free).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helper (docs/style.md §Panics)"
)]

use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tinio_store_redb::{
    Driver, Error, bucket, object_meta, open,
    state::{self, FORMAT_VERSION},
};

/// The fixture's bucket.
const BUCKET: &str = "fixture";
/// The fixture object's key.
const KEY: &str = "pre-change.txt";
/// The fixture object's payload (the pre-change write's body).
const PAYLOAD: &[u8] = b"written before the change";
/// The ETag the pre-change build recorded for [`PAYLOAD`] — its MD5.
const ETAG: &str = "907b4ff97e4ae11be7fe3c999d709681";

/// The committed fixture's path.
fn fixture() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/pre-change-meta.redb"
    ))
}

/// A copy of the fixture in a fresh state dir, opened by the relocated
/// [`open`] (the format gate runs there) and wrapped in the relocated
/// [`Driver`].
fn open_fixture() -> (tempfile::TempDir, Arc<Driver>) {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join("state");
    fs::create_dir_all(&state_dir).unwrap();
    fs::copy(fixture(), state_dir.join("meta.redb")).unwrap();
    let opened = open::open(&state_dir).unwrap();
    (dir, Driver::new(opened.db))
}

#[test]
fn the_pre_change_database_opens_and_serves_its_rows() {
    let (_root, driver) = open_fixture();

    let version = driver
        .read(|txn| -> Result<_, Error> { state::Table::open_readonly(txn)?.version() })
        .unwrap();
    assert_eq!(
        version,
        Some(FORMAT_VERSION),
        "the pre-change file's state version"
    );

    let created = driver
        .read(|txn| -> Result<_, Error> { bucket::Table::open_readonly(txn)?.get(BUCKET) })
        .unwrap()
        .expect("the pre-change BUCKETS row");
    assert!(
        created > SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000),
        "the recorded creation time reads back as {created:?}"
    );

    let stored = driver
        .read(|txn| -> Result<_, Error> {
            object_meta::Table::open_readonly(txn)?.get(BUCKET, KEY)
        })
        .unwrap()
        .expect("the pre-change OBJECT_META row");
    assert_eq!(stored.etag.as_str(), ETAG, "the recorded ETag");
    assert_eq!(stored.size, PAYLOAD.len() as u64, "the recorded size");
    assert!(!stored.is_delete_marker, "a current object, not a marker");
    assert_eq!(stored.version_id, None, "written while unversioned");
}

#[tokio::test]
async fn the_pre_change_database_accepts_a_write() {
    let (_root, driver) = open_fixture();
    let added = SystemTime::UNIX_EPOCH + Duration::from_secs(1_234_567);

    driver
        .write(move |txn| -> Result<(), Error> {
            bucket::Table::open(txn)?.put("added-after-the-change", added)?;
            Ok(())
        })
        .await
        .unwrap();

    let read_back = driver
        .read(|txn| -> Result<_, Error> {
            bucket::Table::open_readonly(txn)?.get("added-after-the-change")
        })
        .unwrap();
    assert_eq!(read_back, Some(added));
}
