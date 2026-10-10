//! Streaming throughput benchmarks for the fs backend (task T030).
//!
//! The bounded-buffer hot paths: streaming write (temp file + atomic
//! rename, ETag MD5 computed inline) and streaming read (full-object
//! drain). Baselines are recorded in Phase 6 (T088) and regression-gated.
//!
//! The cells' data root and the run's flush control come from
//! `tinio_util::bench` (the `bench` feature): its module docs carry why the
//! root is pinned away from `TMP` and why every run records a flush
//! measurement.

// This bench declares `harness = false`, so clippy's `allow-*-in-tests` keys do
// not reach it (design 2026-10-09 D7), and a bench is not production code
// (style.md §Panics covers production sources, D1): its unwraps are fixture
// assertions, kept as written.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a `harness = false` bench's fixture assertions (docs/style.md §Panics)"
)]

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::stream;
use tinio_core::{
    BodyStream,
    acl::Acl,
    bucket, object, storage,
    storage::{BucketOps, ObjectOps, VersionOps},
    version,
};
use tinio_fs::{AtomicWriter, FsStorage, testing::fs_options};
use tinio_util::{bench, testing::body};
use tokio::runtime::Runtime;

/// Total bytes per streaming round-trip (64 MiB — large enough to measure
/// throughput, small enough for CI smoke runs).
const TOTAL: u64 = 64 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;

/// A repeating chunk stream of `TOTAL` bytes (bounded buffers; no
/// pre-materialized payload).
fn chunk_stream() -> BodyStream {
    let total = TOTAL as usize;
    Box::pin(stream::unfold(0usize, move |pos| async move {
        if pos >= total {
            return None;
        }
        let n = (total - pos).min(CHUNK);
        let chunk = vec![b'x'; n];
        Some((Ok(Bytes::from(chunk)), pos + n))
    }))
}

fn streaming_write(c: &mut Criterion) {
    // The run's flush regime, beside its numbers (see `bench`).
    bench::flush_control(&bench::data_root());
    let mut group = c.benchmark_group("streaming_write");
    group.throughput(Throughput::Bytes(TOTAL));
    group.bench_function("atomic_write_64MiB", |b| {
        let state = bench::cell_dir("streaming_write");
        let writer = AtomicWriter::new(&state);
        let target = state.join("obj.bin");
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let writer = writer.clone();
            let target = target.clone();
            async move {
                let (etag, size) = writer.write(&target, chunk_stream()).await.unwrap();
                black_box((etag, size));
            }
        });
    });
    group.finish();
}

fn streaming_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_read");
    group.throughput(Throughput::Bytes(TOTAL));
    group.bench_function("get_object_drain_64MiB", |b| {
        let root = bench::cell_dir("streaming_read");
        let storage = FsStorage::new(&root, fs_options()).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let b = bucket::name("data").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .put_object(&b, &object::key("big.bin").unwrap(), chunk_stream())
                .await
                .unwrap();
        });
        b.to_async(rt).iter(|| {
            let storage = storage.clone();
            async move {
                let get = storage
                    .get_object(
                        &bucket::name("data").unwrap(),
                        &object::key("big.bin").unwrap(),
                        None,
                    )
                    .await
                    .unwrap();
                let drained = storage::collect_body(get.body).await.unwrap();
                black_box(drained.len());
            }
        });
    });
    group.finish();
}

/// Small-object throughput (thousands of objects per second on the
/// write path).
fn small_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("small_write");
    group.throughput(Throughput::Elements(1));
    group.bench_function("put_1KiB", |b| {
        let root = bench::cell_dir("small_write");
        let storage = FsStorage::new(&root, fs_options()).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            storage
                .create_bucket(
                    &bucket::name("data").unwrap(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
        });
        let payload: Vec<u8> = vec![b'y'; 1024];
        let mut i = 0u64;
        b.to_async(rt).iter(|| {
            let storage = storage.clone();
            let payload = payload.clone();
            i += 1;
            let key = format!("obj-{i}");
            async move {
                let put = storage
                    .put_object(
                        &bucket::name("data").unwrap(),
                        &object::key(key.clone()).unwrap(),
                        body(payload),
                    )
                    .await
                    .unwrap();
                black_box(put.etag);
            }
        });
    });
    group.finish();
}

/// The small-read cost legs (filed at close-out, 2026-10-03 ops-engine
/// design): the plain read, the current read of a version-recording bucket,
/// and the version-addressed read, each over a 1 KiB object.
///
/// Every read path now runs the engine's resolution stack — the plain read
/// heals through `healed_row`/`version_facts` and the version-addressed read
/// resolves the address several times where the pre-engine read resolved
/// once — and **no bench covered it**: the close-out's profile round measured
/// `head_object` once at +50% (163 → 245 µs) with nothing to compare against,
/// so a regression there would be invisible to the acceptance. These legs are
/// that comparison, and they stay after the pinned root so their numbers are
/// regime-comparable.
///
/// `head_object_plain` is the sighting's own shape (an unversioned bucket, the
/// fixture the profile harness used); the versioned pair prices the stack the
/// design's "version-read cost stack" bullet names.
fn read_meta(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_meta");
    group.throughput(Throughput::Elements(1));

    group.bench_function("head_object_plain", |b| {
        let (storage, bkt, key) = plain_object("read_meta/head_object_plain");
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let storage = storage.clone();
            let bkt = bkt.clone();
            let key = key.clone();
            async move {
                black_box(storage.head_object(&bkt, &key).await.unwrap());
            }
        });
    });

    group.bench_function("head_object_current", |b| {
        let (storage, bkt, key, _id) = versioned_object("read_meta/head_object_current");
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let storage = storage.clone();
            let bkt = bkt.clone();
            let key = key.clone();
            async move {
                black_box(storage.head_object(&bkt, &key).await.unwrap());
            }
        });
    });

    group.bench_function("head_object_version_addressed", |b| {
        let (storage, bkt, key, id) = versioned_object("read_meta/head_object_version");
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let storage = storage.clone();
            let bkt = bkt.clone();
            let key = key.clone();
            let id = id.clone();
            async move {
                black_box(
                    storage
                        .head_object_version(&bkt, &key, Some(&id))
                        .await
                        .unwrap(),
                );
            }
        });
    });
    group.finish();
}

/// One 1 KiB object on an unversioned bucket — the plain read's own shape.
fn plain_object(cell: &str) -> (FsStorage, bucket::Name, object::Key) {
    let root = bench::cell_dir(cell);
    let storage = FsStorage::new(&root, fs_options()).unwrap();
    Runtime::new().unwrap().block_on(async {
        let bkt = bucket::name("data").unwrap();
        storage
            .create_bucket(&bkt, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("small.bin").unwrap();
        storage
            .put_object(&bkt, &key, body(vec![b'y'; 1024]))
            .await
            .unwrap();
        (storage, bkt, key)
    })
}

/// One 1 KiB object on a version-recording bucket — the fixture both read
/// legs measure over (the write's own paths are `small_write`'s).
fn versioned_object(cell: &str) -> (FsStorage, bucket::Name, object::Key, version::Id) {
    let root = bench::cell_dir(cell);
    let storage = FsStorage::new(&root, fs_options()).unwrap();
    Runtime::new().unwrap().block_on(async {
        let bkt = bucket::name("data").unwrap();
        storage
            .create_bucket(&bkt, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .set_bucket_versioning(&bkt, version::State::Enabled)
            .await
            .unwrap();
        let key = object::key("small.bin").unwrap();
        storage
            .put_object(&bkt, &key, body(vec![b'y'; 1024]))
            .await
            .unwrap();
        // The identity the version-addressed leg names: the plain read's own
        // answer, so the leg measures addressing by id and not the mint.
        let id = storage
            .head_object(&bkt, &key)
            .await
            .unwrap()
            .version_id
            .expect("an enabled write mints an identity");
        (storage, bkt, key, id)
    })
}

criterion_group!(
    benches,
    streaming_write,
    streaming_read,
    small_write,
    read_meta
);
criterion_main!(benches);
