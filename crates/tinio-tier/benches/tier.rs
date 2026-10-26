//! The tier layer's criterion screen: warm GET, cold GET and PUT.
//!
//! **A screen, never a verdict.** The decision-grade comparison runs through
//! `cargo xtask bench-ab` with two revision legs (`src/bin/tier-leg.rs`), one
//! named operation per process, following `docs/benches.md`. This target only
//! catches the large regressions a candidate change might introduce; its
//! numbers are comparable only within a logged flush regime.
//!
//! The cold plane is `RedbContent` in memory, not PostgreSQL: the tier paths
//! are what this screen prices, and a live database's round-trip latency would
//! drown the difference under test. The cells live under
//! `tinio_util::bench`'s pinned root (never `TMP`), and each run logs the
//! flush control beside its numbers.
//!
//! `max_object_bytes` stays below the object size, so a cold GET never fills
//! the object cache and every sample measures the cold path.

// A `harness = false` bench is not production code: its unwraps are fixture
// assertions (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a `harness = false` bench's fixture assertions (docs/style.md §Panics)"
)]

use std::{
    fs,
    hint::black_box,
    time::{Duration, SystemTime},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use tinio_core::{
    acl::Acl,
    bucket, object,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::testing;
use tinio_store_redb::RedbContent;
use tinio_tier::{Config, FlushMode, MoveOutcome, TierStorage};
use tinio_util::{
    bench,
    testing::{body, read_body},
};
use tokio::runtime::Runtime;

/// The object every operation measures: 1 KiB, one cached-file-chunk read.
const OBJECT_BYTES: usize = 1024;

/// The runtime configuration the cells share. The cache cap is deliberately
/// below the object size: a full cold GET then streams from the cold plane on
/// every sample instead of answering from the object cache.
fn config() -> Config {
    Config {
        capacity_bytes: 16 * 1024 * 1024,
        max_object_bytes: 512,
        flush_mode: FlushMode::Sync,
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

/// One cell's prepared lookup: the tier, its bucket and its key.
struct Cell {
    /// The tier under test.
    tier: TierStorage<RedbContent>,
    /// The bucket the object lives in.
    bucket: bucket::Name,
    /// The object's key.
    key: object::Key,
    /// The runtime the async calls run on.
    runtime: Runtime,
}

impl Cell {
    /// Open a fresh cell named `name` and write the 1 KiB object.
    ///
    /// With `cold`, the object is demoted and its warm file removed, so the
    /// measurements read the cold plane.
    fn new(name: &str, cold: bool) -> Self {
        let root = bench::cell_dir(name);
        let runtime = Runtime::new().expect("a bench runtime");
        let tier = TierStorage::open(
            &root,
            testing::fs_options(),
            config(),
            RedbContent::in_memory().expect("an in-memory cold plane"),
        )
        .expect("the tier opens");
        let bucket = bucket::name("data").expect("a valid bucket name");
        let key = object::key("bench.bin").expect("a valid key");
        runtime.block_on(async {
            tier.create_bucket(&bucket, None, &Acl::default_private(None))
                .await
                .expect("the bucket is created");
            tier.put_object(&bucket, &key, body(vec![b'x'; OBJECT_BYTES]))
                .await
                .expect("the fixture object is written");
            if cold {
                let moved = tier
                    .demote(&bucket, &key, SystemTime::now())
                    .await
                    .expect("the demote runs");
                assert!(
                    matches!(moved, MoveOutcome::Moved { .. }),
                    "the fixture object reaches the cold plane"
                );
                fs::remove_file(root.join("data").join("bench.bin"))
                    .expect("the leftover warm file is removed");
            }
        });
        Self {
            tier,
            bucket,
            key,
            runtime,
        }
    }

    /// Read the object whole and answer its bytes.
    fn get(&self) -> Vec<u8> {
        self.runtime.block_on(async {
            let got = self
                .tier
                .get_object(&self.bucket, &self.key, None)
                .await
                .expect("the object is served");
            read_body(got.body).await.expect("the body reads")
        })
    }

    /// Overwrite the object with the fixture's 1 KiB body.
    fn put(&self) {
        self.runtime.block_on(async {
            self.tier
                .put_object(&self.bucket, &self.key, body(vec![b'x'; OBJECT_BYTES]))
                .await
                .expect("the object is written");
        });
    }
}

/// A warm GET: the filesystem plane, with no object-cache entry to answer it.
fn warm_get(criterion: &mut Criterion) {
    bench::flush_control(&bench::data_root());
    let cell = Cell::new("tier-warm_get", false);
    let mut group = criterion.benchmark_group("tier");
    group.throughput(Throughput::Bytes(OBJECT_BYTES as u64));
    group.bench_function("warm_get", |b| {
        b.iter(|| black_box(cell.get()));
    });
    group.finish();
}

/// A cold GET: the placement resolves to the cold plane and the bytes stream
/// from `RedbContent` (the cache cap excludes the object).
fn cold_get(criterion: &mut Criterion) {
    bench::flush_control(&bench::data_root());
    let cell = Cell::new("tier-cold_get", true);
    let mut group = criterion.benchmark_group("tier");
    group.throughput(Throughput::Bytes(OBJECT_BYTES as u64));
    group.bench_function("cold_get", |b| {
        b.iter(|| black_box(cell.get()));
    });
    group.finish();
}

/// A PUT: stage, commit to the filesystem plane and reset the placement.
fn put(criterion: &mut Criterion) {
    bench::flush_control(&bench::data_root());
    let cell = Cell::new("tier-put", false);
    let mut group = criterion.benchmark_group("tier");
    group.throughput(Throughput::Bytes(OBJECT_BYTES as u64));
    group.bench_function("put", |b| b.iter(|| cell.put()));
    group.finish();
}

criterion_group!(tier, warm_get, cold_get, put);
criterion_main!(tier);
