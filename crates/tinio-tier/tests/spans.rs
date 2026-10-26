//! The tier layer's span conventions, asserted from a dedicated binary.
//!
//! **A separate target rather than a module of the lib test binary**, for the
//! reason `tinio-cli`'s span test carries: the recorder is the process
//! default, so a capture opened beside other tests would read their spans.
//! This binary has one test, so nothing else races the instrumented callsites.
//!
//! The conventions pinned here are the ones a served `/metrics` operator reads:
//! a per-object move (`tier.demote`, `tier.promote`) is debug, a pass-level
//! unit (`tier.pass`, `tier.stats`, `tier.flush`) is info, every span targets
//! `tinio::tier`, and `skip_all` keeps every recorded field empty so no
//! bucket, key, tag or ACL identity can reach a span. `tier.flush` is opened
//! only when the drain selected an entry, so an empty flush adds none.

// This target's file-level helpers are not `#[test]` functions, and clippy's
// `allow-*-in-tests` reach `#[test]` functions only (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helpers (docs/style.md §Panics)"
)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::testing;
use tinio_store_redb::RedbContent;
use tinio_tier::{Config, FlushMode, MoveOutcome, Mover, TierStorage, VolumeCapacity};
use tinio_util::testing::{body, spans};
use tracing::Level;

/// A tier configuration with the requested flush mode and no background moves.
fn config(flush_mode: FlushMode) -> Config {
    Config {
        capacity_bytes: 1024 * 1024,
        max_object_bytes: 64 * 1024,
        flush_mode,
        batch_bytes: 8 * 1024,
        batch_interval: Duration::from_secs(1),
        heat_window: Duration::from_secs(60),
        promote_reads: 3,
        stats_flush: Duration::from_secs(10),
        high_water_percent: 90,
        low_water_percent: 80,
        demote_idle: Duration::from_secs(3600),
        dwell: Duration::from_secs(3600),
        scan_interval: Duration::from_secs(60),
        move_concurrency: 1,
        index_compact_percent: 30,
        gc_grace: Duration::from_secs(300),
    }
}

fn data() -> bucket::Name {
    bucket::name("data").expect("a valid bucket name")
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

/// A tier over a temporary root with its one bucket already created.
async fn open_tier(flush_mode: FlushMode) -> (tempfile::TempDir, TierStorage<RedbContent>) {
    let root = tempfile::tempdir().expect("a temporary root");
    let tier = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(flush_mode),
        RedbContent::in_memory().expect("an in-memory cold plane"),
    )
    .expect("the tier layer opens");
    tier.create_bucket(&data(), None, &Acl::default_private(None))
        .await
        .expect("the bucket is created");
    (root, tier)
}

#[tokio::test]
async fn the_tier_spans_carry_their_conventional_levels() {
    spans::install();

    // One per-object move in each direction: both are debug spans.
    let (sync_root, sync_tier) = open_tier(FlushMode::Sync).await;
    sync_tier
        .put_object(&data(), &key("moved.bin"), body(b"hi"))
        .await
        .expect("the object is written");
    let demoted = sync_tier
        .demote(&data(), &key("moved.bin"), SystemTime::now())
        .await
        .expect("the object demotes");
    assert!(
        matches!(demoted, MoveOutcome::Moved { .. }),
        "the fixture object changed plane"
    );
    let promoted = sync_tier
        .promote(&data(), &key("moved.bin"))
        .await
        .expect("the object promotes");
    assert!(
        matches!(promoted, MoveOutcome::Moved { .. }),
        "the fixture object changed plane again"
    );

    // One stats flush, and one pass (which also calls the stats flush).
    let mover = Mover::new(
        sync_tier.clone(),
        Arc::new(VolumeCapacity::new(sync_root.path())),
        1,
    );
    mover.flush_stats().expect("the heat counters flush");
    mover.pass().await.expect("the pass runs");

    // A flush that selected a buffered entry is an info span; the empty flush
    // that follows must not open a second one.
    let (_batch_root, batch_tier) = open_tier(FlushMode::Batch).await;
    batch_tier
        .put_object(&data(), &key("buffered.bin"), body(b"hi"))
        .await
        .expect("the write is buffered");
    batch_tier
        .flush()
        .await
        .expect("the buffered write publishes");
    batch_tier.flush().await.expect("an empty flush is a no-op");

    let recorded = spans::spans_on_this_thread();

    for (name, level) in [
        ("tier.demote", Level::DEBUG),
        ("tier.promote", Level::DEBUG),
        ("tier.flush", Level::INFO),
        ("tier.pass", Level::INFO),
    ] {
        let span = spans::one_named(&recorded, name);
        assert_eq!(span.target, "tinio::tier", "{name}");
        assert_eq!(span.level, level, "{name}");
        assert!(
            span.fields.is_empty(),
            "{name} records no identity field: {span:?}"
        );
    }

    // `tier.stats` opens on every call, including the pass's own gated one, so
    // assert the level and target on each rather than once.
    let stats: Vec<_> = recorded
        .iter()
        .filter(|span| span.name == "tier.stats")
        .collect();
    assert!(
        !stats.is_empty(),
        "the stats flush opens its span: {recorded:?}"
    );
    for span in stats {
        assert_eq!(span.target, "tinio::tier", "tier.stats");
        assert_eq!(span.level, Level::INFO, "tier.stats");
        assert!(span.fields.is_empty(), "tier.stats: {span:?}");
    }
}
