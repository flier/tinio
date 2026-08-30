//! `set_batch` write-throughput benchmark (pipeline-spec.md task 2.5, Q6).
//!
//! Write throughput vs batch size × entry byte size for the
//! `Store::set_batch` primitive the write pipeline commits through.
//! One iteration = one write transaction of `batch` entries with redb's
//! default `Durability::Immediate` — every commit flushes to disk, so the
//! measurement includes real durability cost (the single writer
//! serializes). Only the store primitive is exercised: no pipeline, no
//! server runtime (the full-pipeline bench is task 5; `InlineRunner` is
//! not involved because `set_batch` is a direct store method).
//!
//! Cells: batch sizes 1/16/64/128/256/512/1024 × two entry sizes — short
//! key (≈ 70 B entry) and long nested key (≈ 564 B entry); a stored entry
//! is ≈ 56 B of value plus the key bytes (spec §3.3). Each cell runs on a
//! FRESH database in a temp dir; iterations rotate through a pool of
//! distinct keys, so the database grows during the run and, once the pool
//! wraps, rows update in place (a steady-state insert/update mix).
//!
//! The knee of the entries/sec curve determines the recommended
//! `meta_batch_size` default and calibrates `meta_batch_bytes`; the
//! numbers and reasoning are recorded in task-2.5-report.md (Q6 — the
//! spec default is wired by task 4, not here).

use std::{
    hint::black_box,
    time::{Duration, SystemTime},
};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use meta::Store;
use tinio_core::{ETag, bucket, object};
use tinio_fs::meta;
use tokio::runtime::Runtime;

/// The stored value of one entry: etag hex (32 B) + size/mtime/identity
/// u64s — the spec's ≈ 56 B before the key bytes (pipeline-spec.md §3.3).
const ENTRY_VALUE_BYTES: usize = 56;
/// Short realistic key (`dir/obj-000001`, 14 B): entry ≈ 70 B.
const SHORT_KEY_BYTES: usize = 14;
/// Long nested key (26 path segments + object name, 508 B): entry ≈ 564 B.
/// S3 keys may reach 1024 bytes; 508 B is a realistic deep path.
const LONG_KEY_BYTES: usize = 508;

/// Batch sizes measured: 1 (today's one transaction per entry) through
/// 1024, past the expected knee (pipeline-spec.md task 2.5).
const BATCH_SIZES: [usize; 7] = [1, 16, 64, 128, 256, 512, 1024];

/// Distinct keys in the rotating pool — a multiple of every batch size,
/// so a batch never straddles the wrap-around.
const POOL: usize = 1 << 16; // 65 536

/// One `set_batch` row (the exact slice element of `Store::set_batch`).
type Entry = meta::BatchEntry;

/// The digest every entry carries (content equality is irrelevant to the
/// write path — rows are distinct by key).
const ETAG: ETag = ETag::Single([0x42; 16]);

/// A short realistic key (`dir/obj-000001`).
fn short_key(i: usize) -> String {
    format!("dir/obj-{i:06}")
}

/// A long realistic key: 26 nested `segment-0123456789/` segments plus the
/// object name, exactly `LONG_KEY_BYTES` bytes.
fn long_key(i: usize) -> String {
    let mut key = String::with_capacity(LONG_KEY_BYTES + 8);
    key.push_str("dir/");
    for _ in 0..26 {
        key.push_str("segment-0123456789/");
    }
    key.push_str(&format!("obj-{i:06}"));
    key
}

/// A pool of `POOL` distinct entries: key (per `key_fn`), etag, size,
/// mtime (unix nanos), identity. `key_bytes` is the key's nominal size —
/// asserted unconditionally (the pool is built outside the timed region,
/// so the check is free) so a key-shape edit cannot silently drift the
/// measured entry size — `debug_assert` would be compiled out of the
/// release bench profile.
fn entry_pool(key_fn: fn(usize) -> String, key_bytes: usize) -> Vec<Entry> {
    (0..POOL)
        .map(|i| {
            let key = object::key(key_fn(i)).unwrap();
            assert_eq!(key.len(), key_bytes, "key drifted from nominal size");
            Entry {
                key,
                etag: ETAG,
                size: (i % 4096) as u64,
                mtime: SystemTime::UNIX_EPOCH + Duration::from_nanos(i as u64),
                identity: (i % 2) as u64,
            }
        })
        .collect()
}

/// One entry-size axis: a fresh database per batch size, iterations write
/// one transaction of `batch` entries from the rotating pool. The group
/// name carries the entry size (value + key) so the reported cells are
/// self-describing.
fn set_batch_cell(
    c: &mut Criterion,
    group_name: &str,
    key_fn: fn(usize) -> String,
    key_bytes: usize,
) {
    let entry_bytes = ENTRY_VALUE_BYTES + key_bytes;
    let mut group = c.benchmark_group(format!("{group_name}_entry_{entry_bytes}B"));
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));
    for batch in BATCH_SIZES {
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_function(format!("batch_{batch}"), |b| {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let bucket = bucket::name("data").unwrap();
            let pool = entry_pool(key_fn, key_bytes);
            let rt = Runtime::new().unwrap();
            let mut i = 0usize;
            b.iter(|| {
                let start = (i * batch) % POOL;
                i += 1;
                black_box(rt.block_on(async {
                    store
                        .set_batch(&bucket, &pool[start..start + batch])
                        .await
                        .unwrap();
                    start
                }));
            });
        });
    }
    group.finish();
}

fn set_batch_short_key(c: &mut Criterion) {
    set_batch_cell(c, "set_batch_short_key", short_key, SHORT_KEY_BYTES);
}

fn set_batch_long_key(c: &mut Criterion) {
    set_batch_cell(c, "set_batch_long_key", long_key, LONG_KEY_BYTES);
}

criterion_group!(benches, set_batch_short_key, set_batch_long_key);
criterion_main!(benches);
