//! The block plane's spans: one capture per instrumented unit, asserting the
//! name, target, level and recorded fields the plan fixes.

#![expect(
    clippy::await_holding_lock,
    reason = "test support: the recording gate is a std mutex held for one test's duration; each test runs on its own current-thread runtime, so no task that could need it is ever on the guarded thread"
)]
#![expect(
    clippy::unwrap_used,
    reason = "test support: the capture harness and its publish helper state their own invariants with unwrap"
)]

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use tinio_blob::{LiveSet, LocalBlocks, Store};
use tinio_core::{
    ByteRange,
    blob::{BlockHash, BlockStore, Published, StagedBlock},
    storage::collect_body,
};
use tinio_util::testing::{body, spans::recording};
use tracing::Level;

/// Stage these bytes and publish them, the way Store::put does.
async fn publish(blocks: &LocalBlocks, bytes: &[u8]) -> Published {
    let hash = BlockHash::of(bytes);
    let mut staged = blocks.stage().await.unwrap();
    staged.write(Bytes::copy_from_slice(bytes)).await.unwrap();
    blocks.publish(staged, &hash).await.unwrap()
}

#[tokio::test]
async fn a_put_records_one_debug_span_carrying_its_bytes() {
    let (capture, _gate) = recording();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LocalBlocks::new(dir.path()));

    store.put(body(b"hello")).await.unwrap();

    let puts = capture.named("put");
    assert_eq!(puts.len(), 1, "one T1 span per put");
    let span = &puts[0];
    assert_eq!(span.target, "tinio::storage");
    assert_eq!(span.level, Level::DEBUG);
    assert_eq!(span.field("bytes"), Some("5"));
    assert_eq!(
        span.fields.len(),
        1,
        "bytes only: no body, path or hash is a field"
    );
}

#[tokio::test]
async fn a_publish_tells_stored_from_existing() {
    let (capture, _gate) = recording();
    let dir = tempfile::tempdir().unwrap();
    let blocks = LocalBlocks::new(dir.path());

    assert_eq!(
        publish(&blocks, b"hello").await,
        Published::Stored,
        "the first publish stores the block"
    );
    assert_eq!(
        publish(&blocks, b"hello").await,
        Published::Existing,
        "the second publish is a dedup hit"
    );

    let mut publishes = capture.named("tinio.storage.block_publish");
    publishes.sort_by(|left, right| left.field("result").cmp(&right.field("result")));
    assert_eq!(publishes.len(), 2, "one T1 span per publish");
    let results: Vec<&str> = publishes
        .iter()
        .map(|span| span.field("result").unwrap_or(""))
        .collect();
    assert_eq!(results, ["existing", "stored"]);
    for span in &publishes {
        assert_eq!(span.target, "tinio::storage");
        assert_eq!(span.level, Level::DEBUG);
        assert_eq!(span.fields.len(), 1, "result only: no path or hash");
    }
}

#[tokio::test]
async fn an_open_records_one_trace_span() {
    let (capture, _gate) = recording();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LocalBlocks::new(dir.path()));
    let blob = store.put(body(b"hello")).await.unwrap();

    let opened = store
        .open(&blob, Some(ByteRange::Inclusive(1, 3)))
        .await
        .unwrap();
    assert_eq!(collect_body(opened).await.unwrap(), b"ell");

    let opens = capture.named("tinio.storage.block_open");
    assert_eq!(opens.len(), 1, "one T2 span per opened block");
    let span = &opens[0];
    assert_eq!(span.target, "tinio::storage");
    assert_eq!(span.level, Level::TRACE);
    assert!(span.fields.is_empty(), "the open records no field");
}

#[tokio::test]
async fn a_sweep_that_reclaims_nothing_records_one_info_span() {
    let (capture, _gate) = recording();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LocalBlocks::new(dir.path()));

    let report = store
        .sweep(
            &LiveSet::from_hashes([]),
            Duration::from_secs(600),
            SystemTime::now(),
        )
        .await
        .unwrap();

    assert_eq!(report.removed, 0);
    let sweeps = capture.named("tinio.storage.block_sweep");
    assert_eq!(sweeps.len(), 1, "one T0 span per sweep, even an empty one");
    let span = &sweeps[0];
    assert_eq!(span.target, "tinio::storage");
    assert_eq!(span.level, Level::INFO);
    assert_eq!(span.field("pass"), Some("blob_sweep"));
    assert_eq!(span.fields.len(), 1, "the pass label only");
}

#[tokio::test]
async fn a_sweep_that_reclaims_records_one_remove_span_below_the_walk() {
    let (capture, _gate) = recording();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::new(LocalBlocks::new(dir.path()));
    store.put(body(b"dead")).await.unwrap();
    capture.clear();

    let report = store
        .sweep(
            &LiveSet::from_hashes([]),
            Duration::from_secs(600),
            SystemTime::now() + Duration::from_secs(3600),
        )
        .await
        .unwrap();

    assert_eq!(report.removed, 1);
    assert_eq!(
        capture.named("tinio.storage.block_sweep").len(),
        1,
        "the walk is one span"
    );
    let removes = capture.named("tinio.storage.block_remove");
    assert_eq!(removes.len(), 1, "one removal phase per sweep");
    let span = &removes[0];
    assert_eq!(span.target, "tinio::storage");
    assert_eq!(span.level, Level::DEBUG);
    assert_eq!(span.field("pass"), Some("blob_sweep"));
}
