//! Delta assertions for the tier families' emitters (T23).
//!
//! Each leg reads the process-global family before and after the code under
//! test and asserts the difference, never an absolute value: the registry is
//! shared with every other test in this binary. The window serializes the
//! exact reads against the producers' writer gate.

use std::time::{Duration, SystemTime};

use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BucketOps, ObjectOps, VersionOps},
    version,
};
use tinio_fs::testing;
use tinio_store_redb::RedbContent;
use tinio_util::testing::{
    body,
    metrics::{counter, histogram_count, test_lock},
};
use tokio::runtime::{Builder, Runtime};

use crate::{Config, FlushMode, MoveOutcome, TierStorage};

/// The runtime the layer's futures run on.
fn runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a test runtime")
}

/// A small runtime configuration with the requested flush mode.
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

/// A tier over a temporary root with an in-memory cold plane.
fn tier(flush_mode: FlushMode) -> (tempfile::TempDir, TierStorage<RedbContent>) {
    let root = tempfile::tempdir().expect("a temporary root");
    let tier = TierStorage::open(
        root.path(),
        testing::fs_options(),
        config(flush_mode),
        RedbContent::in_memory().expect("an in-memory cold plane"),
    )
    .expect("the tier layer opens");
    (root, tier)
}

fn data() -> bucket::Name {
    bucket::name("data").expect("a valid bucket name")
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

async fn create_bucket(tier: &TierStorage<RedbContent>) {
    tier.create_bucket(&data(), None, &Acl::default_private(None))
        .await
        .expect("the bucket is created");
}

async fn put(tier: &TierStorage<RedbContent>, name: &str, bytes: &'static [u8]) {
    tier.put_object(&data(), &key(name), body(bytes))
        .await
        .expect("the object is written");
}

fn hits(plane: &str) -> f64 {
    counter("tinio_tier_hits_total", &[("tier", plane)])
}

fn misses(plane: &str) -> f64 {
    counter("tinio_tier_misses_total", &[("tier", plane)])
}

fn bytes(plane: &str) -> f64 {
    counter("tinio_tier_bytes_total", &[("tier", plane)])
}

fn moves(direction: &str) -> f64 {
    counter("tinio_tier_moves_total", &[("direction", direction)])
}

fn move_durations(direction: &str) -> f64 {
    histogram_count(
        "tinio_tier_move_duration_seconds",
        &[("direction", direction)],
    )
}

fn flush_durations() -> f64 {
    histogram_count("tinio_tier_flush_duration_seconds", &[])
}

/// A warm object's GET is served by the warm plane: one warm hit, no warm
/// miss, and the served bytes.
#[test]
fn a_warm_get_records_the_warm_plane() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "warm.bin", b"hello").await; // 5 bytes

        let hits_before = hits("warm");
        let misses_before = misses("warm");
        let bytes_before = bytes("warm");
        let served = tier
            .get_object(&data(), &key("warm.bin"), None)
            .await
            .expect("the object reads");

        assert_eq!(served.info.size, 5, "the fixture serves five bytes");
        assert_eq!(hits("warm") - hits_before, 1.0, "one warm hit is booked");
        assert_eq!(
            misses("warm") - misses_before,
            0.0,
            "the warm plane served it, so it did not miss"
        );
        assert_eq!(
            bytes("warm") - bytes_before,
            5.0,
            "the full GET books the object's five bytes"
        );
    });
}

/// A cold object's GET falls through the warm plane: one warm miss, one cold
/// hit, and the cold plane's served bytes.
#[test]
fn a_cold_get_records_a_warm_miss_and_a_cold_hit() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "cold.bin", b"seven!!").await; // 7 bytes
        let moved = tier
            .demote(&data(), &key("cold.bin"), SystemTime::now())
            .await
            .expect("the demote runs");
        assert!(
            matches!(moved, MoveOutcome::Moved { .. }),
            "the fixture object is cold"
        );

        let misses_before = misses("warm");
        let hits_before = hits("cold");
        let bytes_before = bytes("cold");
        tier.get_object(&data(), &key("cold.bin"), None)
            .await
            .expect("the cold object reads");

        assert_eq!(
            misses("warm") - misses_before,
            1.0,
            "the read fell through the warm plane"
        );
        assert_eq!(hits("cold") - hits_before, 1.0, "the cold plane served it");
        assert_eq!(
            bytes("cold") - bytes_before,
            7.0,
            "the cold GET books the object's seven bytes"
        );
    });
}

/// A whole-object cache hit is the warm plane's memory tier: it books a warm
/// hit, not another cold read.
#[test]
fn a_mem_cache_hit_counts_as_a_warm_hit() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "cached.bin", b"elevenbytes").await; // 11 bytes
        tier.demote(&data(), &key("cached.bin"), SystemTime::now())
            .await
            .expect("the demote runs");
        // The first full GET fills the cache; the second is the memory hit.
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        tier.get_object(&data(), &key("cached.bin"), None)
            .await
            .expect("the first read serves and caches");
        assert_eq!(
            bytes("cold") - cold_bytes_before,
            11.0,
            "the first, cold read books eleven bytes"
        );
        assert_eq!(
            bytes("warm") - warm_bytes_before,
            0.0,
            "the first read is not a warm serve"
        );

        let warm_before = hits("warm");
        let cold_before = hits("cold");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        tier.get_object(&data(), &key("cached.bin"), None)
            .await
            .expect("the second read is a memory hit");

        assert_eq!(
            hits("warm") - warm_before,
            1.0,
            "a mem hit counts as a warm hit"
        );
        assert_eq!(
            hits("cold") - cold_before,
            0.0,
            "the cold plane did not serve the cached bytes"
        );
        assert_eq!(
            bytes("warm") - warm_bytes_before,
            11.0,
            "the mem hit books the cached eleven bytes warm"
        );
        assert_eq!(
            bytes("cold") - cold_bytes_before,
            0.0,
            "the mem hit books no cold bytes"
        );
    });
}

/// A demotion books its direction and one duration sample.
#[test]
fn a_demotion_records_its_direction_and_duration() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "demote.bin", b"hello").await;

        let moves_before = moves("demote");
        let durations_before = move_durations("demote");
        let summary = tier
            .demote(&data(), &key("demote.bin"), SystemTime::now())
            .await
            .expect("the demote runs");

        assert!(
            matches!(summary, MoveOutcome::Moved { .. }),
            "the object changed plane"
        );
        assert_eq!(moves("demote") - moves_before, 1.0, "one demote is booked");
        assert_eq!(
            move_durations("demote") - durations_before,
            1.0,
            "the demote's duration is booked"
        );
    });
}

/// A promotion books its direction and one duration sample.
#[test]
fn a_promotion_records_its_direction_and_duration() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "promote.bin", b"hello").await;
        tier.demote(&data(), &key("promote.bin"), SystemTime::now())
            .await
            .expect("the demote runs");

        let moves_before = moves("promote");
        let durations_before = move_durations("promote");
        let summary = tier
            .promote(&data(), &key("promote.bin"))
            .await
            .expect("the promote runs");

        assert!(
            matches!(summary, MoveOutcome::Moved { .. }),
            "the object changed plane"
        );
        assert_eq!(
            moves("promote") - moves_before,
            1.0,
            "one promote is booked"
        );
        assert_eq!(
            move_durations("promote") - durations_before,
            1.0,
            "the promote's duration is booked"
        );
    });
}

/// Draining the batch buffer books one flush duration.
#[test]
fn a_batch_flush_records_its_duration() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Batch);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "buffered.bin", b"hello").await;
        assert!(
            tier.buffer().contains(&data(), &key("buffered.bin")),
            "the write waits in the buffer"
        );

        let before = flush_durations();
        tier.shutdown().await.expect("the buffer drains");

        assert_eq!(
            flush_durations() - before,
            1.0,
            "the drain books exactly one flush sample"
        );
    });
}

/// A drain that selected no entry is not a flush: it books no duration
/// sample, so an idle shutdown cannot pad the histogram.
#[test]
fn an_empty_batch_flush_records_no_duration() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Batch);
    runtime().block_on(async {
        create_bucket(&tier).await;
        assert!(tier.buffer().is_empty(), "the fixture buffered nothing");

        let before = flush_durations();
        tier.shutdown().await.expect("the empty buffer drains");

        assert_eq!(
            flush_durations() - before,
            0.0,
            "an empty drain books no flush sample"
        );
    });
}

/// A subresource read is a metadata read: a warm object's tags book one warm
/// hit and no bytes, exactly as a HEAD does.
#[test]
fn a_warm_tag_read_books_one_hit_and_no_bytes() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "tagged.bin", b"hello").await;

        let warm_hits_before = hits("warm");
        let cold_hits_before = hits("cold");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        let tags = tier
            .get_object_tags(&data(), &key("tagged.bin"))
            .await
            .expect("the warm tags read");

        assert!(tags.is_empty(), "the fixture object has no tags");
        assert_eq!(
            hits("warm") - warm_hits_before,
            1.0,
            "the warm plane served the tag read"
        );
        assert_eq!(
            hits("cold") - cold_hits_before,
            0.0,
            "a warm object never reaches the cold plane"
        );
        assert_eq!(
            bytes("warm") - warm_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
        assert_eq!(
            bytes("cold") - cold_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
    });
}

/// A cold object's ACL is the cold authority's: the read books one warm miss
/// and one cold hit, and still no bytes.
#[test]
fn a_cold_acl_read_books_a_warm_miss_and_a_cold_hit_with_no_bytes() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "cold-acl.bin", b"hello").await;
        tier.demote(&data(), &key("cold-acl.bin"), SystemTime::now())
            .await
            .expect("the demote runs");

        let warm_hits_before = hits("warm");
        let warm_misses_before = misses("warm");
        let cold_hits_before = hits("cold");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        tier.get_object_acl(&data(), &key("cold-acl.bin"))
            .await
            .expect("the cold ACL read");

        assert_eq!(
            misses("warm") - warm_misses_before,
            1.0,
            "the read fell through the warm plane"
        );
        assert_eq!(
            hits("cold") - cold_hits_before,
            1.0,
            "the cold authority answered the ACL"
        );
        assert_eq!(
            hits("warm") - warm_hits_before,
            0.0,
            "the warm plane did not hold the object"
        );
        assert_eq!(
            bytes("warm") - warm_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
        assert_eq!(
            bytes("cold") - cold_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
    });
}

/// Retention and legal hold are filesystem-row reads classified by the
/// placement: each cold placement read books one warm miss and one cold hit,
/// with no bytes.
#[test]
fn cold_retention_and_legal_hold_reads_each_book_one_cold_hit_with_no_bytes() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "locked.bin", b"hello").await;
        let cold_key = key("locked.bin");
        tier.demote(&data(), &cold_key, SystemTime::now())
            .await
            .expect("the demote runs");

        let warm_hits_before = hits("warm");
        let warm_misses_before = misses("warm");
        let cold_hits_before = hits("cold");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        tier.get_object_retention(&data(), &cold_key, None)
            .await
            .expect("the retention read");
        tier.get_object_legal_hold(&data(), &cold_key, None)
            .await
            .expect("the legal-hold read");

        assert_eq!(
            misses("warm") - warm_misses_before,
            2.0,
            "each read fell through the warm plane"
        );
        assert_eq!(
            hits("cold") - cold_hits_before,
            2.0,
            "each cold placement read is a cold hit"
        );
        assert_eq!(
            hits("warm") - warm_hits_before,
            0.0,
            "the warm plane did not serve either read"
        );
        assert_eq!(bytes("warm") - warm_bytes_before, 0.0, "no warm bytes");
        assert_eq!(bytes("cold") - cold_bytes_before, 0.0, "no cold bytes");
    });
}

/// An explicit-version HEAD is a metadata read against a warm object: one warm
/// hit and no bytes.
#[test]
fn an_explicit_version_head_books_a_warm_hit_with_no_bytes() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        tier.set_bucket_versioning(&data(), version::State::Enabled)
            .await
            .expect("versioning is enabled");
        put(&tier, "versioned.bin", b"hello").await;
        let versioned = tier
            .head_object(&data(), &key("versioned.bin"))
            .await
            .expect("the versioned head");
        let version_id = versioned
            .version_id
            .clone()
            .expect("a versioned bucket records an id");

        let warm_hits_before = hits("warm");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        let result = tier
            .head_object_version(&data(), &key("versioned.bin"), Some(&version_id))
            .await
            .expect("the explicit-version head");

        assert!(
            matches!(result, version::HeadResult::Object(_)),
            "the explicit version is an object, not a marker"
        );
        assert_eq!(
            hits("warm") - warm_hits_before,
            1.0,
            "the versioned object is served warm"
        );
        assert_eq!(
            bytes("warm") - warm_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
        assert_eq!(
            bytes("cold") - cold_bytes_before,
            0.0,
            "a metadata read serves no body"
        );
    });
}

/// A `Deleted` resolve books one miss, for the warm plane the read attempted:
/// the cold plane is never reached, so it books neither a miss nor a hit.
#[test]
fn a_deleted_resolve_books_only_the_attempted_tier_miss() {
    let _window = test_lock::window();
    let (_root, tier) = tier(FlushMode::Sync);
    runtime().block_on(async {
        create_bucket(&tier).await;
        put(&tier, "vanished.bin", b"hello").await;
        let vanished = key("vanished.bin");
        tier.demote(&data(), &vanished, SystemTime::now())
            .await
            .expect("the demote runs");
        // The row goes, so the live cold placement resolves to `Deleted`.
        tier.fs()
            .delete_object(&data(), &vanished)
            .await
            .expect("the row is deleted under the placement");

        let warm_hits_before = hits("warm");
        let warm_misses_before = misses("warm");
        let cold_hits_before = hits("cold");
        let cold_misses_before = misses("cold");
        let warm_bytes_before = bytes("warm");
        let cold_bytes_before = bytes("cold");
        tier.head_object(&data(), &vanished)
            .await
            .expect_err("the object is gone");

        assert_eq!(
            misses("warm") - warm_misses_before,
            1.0,
            "the attempted warm plane books the one miss"
        );
        assert_eq!(
            hits("warm") - warm_hits_before,
            0.0,
            "the warm plane held nothing"
        );
        assert_eq!(
            misses("cold") - cold_misses_before,
            0.0,
            "the cold plane was never attempted"
        );
        assert_eq!(
            hits("cold") - cold_hits_before,
            0.0,
            "the cold plane was never attempted"
        );
        assert_eq!(bytes("warm") - warm_bytes_before, 0.0, "no warm bytes");
        assert_eq!(bytes("cold") - cold_bytes_before, 0.0, "no cold bytes");
    });
}
