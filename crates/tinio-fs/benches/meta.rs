//! Meta-hit latency benchmarks (meta-redb-spec task 7, optional T030-style
//! item): the `OBJECT_META` B+tree lookup paths — single-entry `get` /
//! `etag_matching` hits and a bucket `walk` range scan — at realistic
//! entry counts. Guards the redb lookup against regression vs the old
//! fan-out JSON layout. Baselines are recorded in Phase 6 (T088) and
//! regression-gated.

use std::hint::black_box;
use std::time::{Duration, SystemTime};

use criterion::{Criterion, criterion_group, criterion_main};
use tinio_core::{bucket, object};
use tinio_fs::{database, meta};

/// Entries in the populated `OBJECT_META` table (inserted in one bulk
/// write transaction — the bench measures lookups, not setup).
const ENTRIES: u32 = 100_000;

/// A store over a freshly populated database of `ENTRIES` entries in one
/// bucket (`data`). The table is opened through the crate's own
/// `ObjectMetaTable` handle (the same `TableDefinition` the backend uses —
/// no bench-local schema that can drift); the populate handle is dropped
/// before the store opens the file (redb's file lock is exclusive per
/// handle).
fn populated_store() -> (tempfile::TempDir, meta::Store) {
    let state = tempfile::tempdir().unwrap();
    {
        let db = database::open(state.path()).unwrap().db;
        {
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = database::ObjectMetaTable::open(&mut txn).unwrap();
                for i in 0..ENTRIES {
                    let key = format!("dir/obj-{i:06}");
                    table
                        .insert(
                            ("data", key.as_str()),
                            (
                                "d41d8cd98f00b204e9800998ecf8427e",
                                u64::from(i),
                                u64::from(i),
                                0,
                            ),
                        )
                        .unwrap();
                }
            }
            txn.commit().unwrap();
        }
        drop(db);
    }
    let store = meta::store(state.path());
    (state, store.unwrap())
}

/// The hit key: `dir/obj-050000` (present at `ENTRIES` scale).
fn hit_key() -> object::Key {
    object::key("dir/obj-050000").unwrap()
}

fn meta_hits(c: &mut Criterion) {
    let (_state, store) = populated_store();
    let bucket = bucket::name("data").unwrap();
    let key = hit_key();

    let mut group = c.benchmark_group("meta_hits");
    group.throughput(criterion::Throughput::Elements(1));

    group.bench_function("get_hit_100k", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = store.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        b.to_async(rt).iter(|| {
            let store = store.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            async move {
                let record = store.get(&bucket, &key).await.unwrap().unwrap();
                black_box(record.etag);
            }
        });
    });

    // The full gate (FR-022): size + mtime must match the entry (i = 50000
    // recorded both as 50000).
    group.bench_function("etag_matching_hit_100k", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = store.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let size = 50_000u64;
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_nanos(50_000);
        b.to_async(rt).iter(|| {
            let store = store.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            async move {
                // The bench runs the size+mtime pair (identity 0 — the
                // walk's identity lookup is not on this path).
                let etag = store
                    .etag_matching(&bucket, &key, size, mtime, 0)
                    .await
                    .unwrap();
                black_box(etag);
            }
        });
    });

    group.finish();
}

/// The whole-bucket range scan (walk/listing/remove_bucket path).
fn meta_walk(c: &mut Criterion) {
    let (_state, store) = populated_store();
    let bucket = bucket::name("data").unwrap();

    let mut group = c.benchmark_group("meta_walk");
    group.throughput(criterion::Throughput::Elements(u64::from(ENTRIES)));
    group.bench_function("walk_100k", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store = store.clone();
        let bucket = bucket.clone();
        b.to_async(rt).iter(|| {
            let store = store.clone();
            let bucket = bucket.clone();
            async move {
                let records = store.walk(&bucket).await.unwrap();
                black_box(records.len());
            }
        });
    });
    group.finish();
}

criterion_group!(benches, meta_hits, meta_walk);
criterion_main!(benches);
