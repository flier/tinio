//! Streaming throughput benchmarks for the fs backend (task T030).
//!
//! The bounded-buffer hot paths: streaming write (temp file + atomic
//! rename, ETag MD5 computed inline) and streaming read (full-object
//! drain). Baselines are recorded in Phase 6 (T088) and regression-gated.

use std::hint::black_box;

use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures::stream;
use tinio_core::{
    BodyStream, bucket, object, storage,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::{AtomicWriter, FsStorage, testing::fs_options};
use tinio_util::testing::body;
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
    let mut group = c.benchmark_group("streaming_write");
    group.throughput(Throughput::Bytes(TOTAL));
    group.bench_function("atomic_write_64MiB", |b| {
        let state = tempfile::tempdir().unwrap();
        let writer = AtomicWriter::new(state.path());
        let target = state.path().join("obj.bin");
        b.to_async(Runtime::new().unwrap()).iter(|| {
            let writer = writer.clone();
            let target = target.clone();
            async move {
                let etag = writer.write(&target, chunk_stream()).await.unwrap();
                black_box(etag);
            }
        });
    });
    group.finish();
}

fn streaming_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("streaming_read");
    group.throughput(Throughput::Bytes(TOTAL));
    group.bench_function("get_object_drain_64MiB", |b| {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let b = bucket::name("data").unwrap();
            storage.create_bucket(&b).await.unwrap();
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
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            storage
                .create_bucket(&bucket::name("data").unwrap())
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

criterion_group!(benches, streaming_write, streaming_read, small_write);
criterion_main!(benches);
