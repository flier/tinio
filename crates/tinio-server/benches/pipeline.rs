//! End-to-end cold-list pipeline benchmark (pipeline-spec.md task 5).
//!
//! Measures the cold-list throughput of the REAL concurrent pipeline
//! runtimes ([`tinio_server::pipeline::Pipelines`] — tokio runtimes,
//! blocking-task model, `catch_unwind` workers) over a generated object
//! tree. Every entry is missing/stale by construction (the producer
//! skips the match gate — a cold list), so each iteration enqueues
//! `TASKS` compute tasks into the IO pipeline, streams the results into
//! batches, commits each batch through the DB write pipeline
//! ([`meta::Store::set_batch`] — one write transaction + fsync), and
//! drains the write completions before the "response" (Q2).
//!
//! The IO task mirrors the tinio-fs `etag::ComputeTask` compute core
//! (blocking `std::fs` open + 64 KiB bounded streaming MD5, no internal
//! `.await` — one task occupies one worker thread, Q4); the DB task
//! mirrors `MetaWriteBatchTask` over the real `set_batch` primitive. The
//! real task types are `pub(crate)` to tinio-fs, so the bench carries
//! representative mirrors — the runtimes themselves are the genuine
//! tinio-server ones. The IO output type is the real `tinio_fs::etag::Result`.
//!
//! Cells: IO workers 1/2/4/8 × batch sizes 32/128/256/512 × two entry
//! sizes (short ≈ 70 B and long ≈ 564 B per the spec's ≈ 56 B value +
//! key bytes, pipeline-spec.md §3.3), plus the db-workers verification
//! cell (1 vs 2 at io=4 — redb is single-writer, so >1 adds no write
//! throughput). The knee of the entries/sec vs workers curve determines
//! the `DEFAULT_IO_WORKERS` default; the numbers and reasoning are
//! recorded in task-5-report.md.
//!
//! Redb-file growth: the store is created once per cell and each
//! iteration inserts 2048 rows, so the redb file grows monotonically
//! across the measurement window — a within-cell drift, disclosed in the
//! report.

use std::{
    hint::black_box,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use md5::{Digest, Md5};
use tinio_core::{
    ETag, bucket, object,
    pipeline::{Completion, Runner, Task},
};
use tinio_fs::{Error as FsError, meta};
use tinio_server::pipeline::Pipeline;

/// Entries per iteration (one cold-list workload).
const TASKS: usize = 2048;
/// The IO enqueue/await chunk — the streaming-flush granularity (the
/// queue capacity 1024 keeps the workers busy across chunk boundaries).
const IO_CHUNK: usize = 256;
/// Object content bytes (64 KiB — a realistic small-object size that
/// keeps the compute phase on the critical path).
const FILE_BYTES: usize = 64 * 1024;
/// The blocking read buffer (the spec's 64 KiB bounded streaming).
const CHUNK: usize = 64 * 1024;
/// IO worker counts measured (the `[pipeline.io] workers` axis).
const IO_WORKERS: [u8; 4] = [1, 2, 4, 8];
/// Batch sizes measured (around the 128 default; 256 re-checks the T2.5
/// run-3 inverted-step flag on the write axis).
const BATCH_SIZES: [usize; 4] = [32, 128, 256, 512];
/// Short realistic key (`dir/obj-000000`, 14 B): entry ≈ 70 B.
const SHORT_KEY_BYTES: usize = 14;
/// Long nested key (26 path segments + object name, 508 B): entry ≈ 564 B.
const LONG_KEY_BYTES: usize = 508;

/// The write pipeline's task output (the real `set_batch` error type).
type WriteResult = Result<(), FsError>;
/// The bench's pipeline pair, typed to the real tinio-fs task outputs
/// (P4/P7 — the server wiring uses the same types).
type Pipelines =
    tinio_server::pipeline::Pipelines<tinio_fs::etag::Result, WriteResult, WriteResult>;
/// One write-batch row (the `set_batch` slice element).
type Entry = meta::BatchEntry;

/// Walk-time data of one object — what the task-4 producers hand the IO
/// pipeline (the bench's producer carries it per task). The size/mtime
/// the producer would record come back in the task's own outcome (the
/// batch entries mirror the real producer's hash-time metadata), so the
/// walk data here is just the enqueue context.
struct Object {
    key: object::Key,
    path: PathBuf,
}

/// A short realistic key (`dir/obj-000000`).
fn short_key(i: usize) -> String {
    format!("dir/obj-{i:06}")
}

/// A long realistic key: 26 nested `segment-0123456789/` segments plus
/// the object name, exactly `LONG_KEY_BYTES` bytes (the tree shares the
/// segment directories, only the leaf differs).
fn long_key(i: usize) -> String {
    let mut key = String::with_capacity(LONG_KEY_BYTES + 8);
    key.push_str("dir/");
    for _ in 0..26 {
        key.push_str("segment-0123456789/");
    }
    key.push_str(&format!("obj-{i:06}"));
    key
}

/// Build the object tree of one entry-size axis: `TASKS` files of
/// `FILE_BYTES` under `root`, named per `key_fn`, with walk-time
/// metadata read back from disk.
fn build_tree(root: &Path, key_fn: fn(usize) -> String) -> Vec<Object> {
    // Every key of one axis shares its parent directory chain.
    let first_key = key_fn(0);
    let parent = root.join(first_key.rsplit_once('/').unwrap().0);
    std::fs::create_dir_all(&parent).unwrap();
    let content = vec![b'x'; FILE_BYTES];
    (0..TASKS)
        .map(|i| {
            let key = key_fn(i);
            let path = root.join(&key);
            std::fs::write(&path, &content).unwrap();
            Object {
                key: object::key(&key).unwrap(),
                path,
            }
        })
        .collect()
}

/// The IO-pipeline task: mirrors the tinio-fs `etag::ComputeTask` compute
/// core — blocking open + 64 KiB bounded streaming MD5, no internal
/// `.await` (Q4: one task occupies one worker thread). The real task is
/// `pub(crate)` in tinio-fs, so the bench mirrors it; the output type is
/// the real `tinio_fs::etag::Result`. The file
/// identity is left at 0 — the write path accepts an unavailable
/// identity, and identity lookup is not on the throughput path.
struct BenchEtagTask {
    key: object::Key,
    path: PathBuf,
}

impl BenchEtagTask {
    fn new(object: &Object) -> Self {
        Self {
            key: object.key.clone(),
            path: object.path.clone(),
        }
    }
}

#[async_trait]
impl Task for BenchEtagTask {
    type Output = tinio_fs::etag::Result;

    fn kind(&self) -> &'static str {
        "etag"
    }

    async fn run(&mut self) -> tinio_fs::etag::Result {
        let mut file = std::fs::File::open(&self.path).map_err(FsError::Io)?;
        let mut hasher = Md5::new();
        let mut buf = vec![0u8; CHUNK];
        loop {
            let n = std::io::Read::read(&mut file, &mut buf).map_err(FsError::Io)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let digest: [u8; 16] = hasher.finalize().into();
        let metadata = std::fs::metadata(&self.path).map_err(FsError::Io)?;
        Ok(tinio_fs::etag::Outcome {
            key: self.key.clone(),
            etag: ETag::Single(digest),
            size: metadata.len(),
            mtime: metadata.modified().map_err(FsError::Io)?,
            identity: 0,
            kept: false,
        })
    }
}

/// The DB write-pipeline task: mirrors `MetaWriteBatchTask` — one batch
/// through the real `meta::Store::set_batch` (one write transaction +
/// fsync; the redb single writer serializes commits).
struct BenchWriteBatchTask {
    meta: meta::Store,
    bucket: bucket::Name,
    entries: Vec<Entry>,
}

#[async_trait]
impl Task for BenchWriteBatchTask {
    type Output = WriteResult;

    fn kind(&self) -> &'static str {
        "meta_write"
    }

    async fn run(&mut self) -> WriteResult {
        self.meta.set_batch(&self.bucket, &self.entries).await
    }
}

/// Build both pipelines from a worker configuration (queue capacity =
/// the 1024 default).
fn build_pipelines(io_workers: u8, db_workers: u8) -> Pipelines {
    Pipelines::build(&tinio_config::pipeline::Config {
        io: tinio_config::pipeline::Io {
            workers: io_workers,
            capacity: tinio_core::pipeline::DEFAULT_CAPACITY,
            ..Default::default()
        },
        remove: Default::default(),
        db: tinio_config::pipeline::Db {
            workers: db_workers,
            capacity: tinio_core::pipeline::DEFAULT_CAPACITY,
            ..Default::default()
        },
    })
    .expect("pipeline runtimes build")
}

/// One cold-list iteration: `objects` through the IO pipeline in chunks
/// (enqueue, await the chunk's completions), results streamed into
/// batches of `batch_size`, each full batch committed through the DB
/// pipeline (enqueue — backpressure bounds the producer), then all
/// write completions drained (Q2 — the response waits for the final
/// drain, like the list producer).
async fn cold_list(
    io: &Arc<Pipeline<tinio_fs::etag::Result>>,
    db: &Arc<Pipeline<WriteResult>>,
    store: &meta::Store,
    bucket: &bucket::Name,
    objects: &[Object],
    batch_size: usize,
) -> usize {
    let mut processed = 0usize;
    let mut pending_db: Vec<Completion<WriteResult>> = Vec::new();
    let mut batch: Vec<Entry> = Vec::with_capacity(batch_size);
    for chunk in objects.chunks(IO_CHUNK) {
        let mut completions = Vec::with_capacity(chunk.len());
        for object in chunk {
            completions.push(
                io.enqueue(Box::new(BenchEtagTask::new(object)))
                    .await
                    .unwrap(),
            );
        }
        for done in completions {
            let outcome = done.await.unwrap().unwrap();
            batch.push(Entry {
                key: outcome.key,
                etag: outcome.etag,
                size: outcome.size,
                mtime: outcome.mtime,
                identity: outcome.identity,
            });
        }
        while batch.len() >= batch_size {
            let entries: Vec<Entry> = batch.drain(..batch_size).collect();
            processed += entries.len();
            pending_db.push(
                db.enqueue(Box::new(BenchWriteBatchTask {
                    meta: store.clone(),
                    bucket: bucket.clone(),
                    entries,
                }))
                .await
                .unwrap(),
            );
        }
    }
    if !batch.is_empty() {
        processed += batch.len();
        pending_db.push(
            db.enqueue(Box::new(BenchWriteBatchTask {
                meta: store.clone(),
                bucket: bucket.clone(),
                entries: batch,
            }))
            .await
            .unwrap(),
        );
    }
    for done in pending_db {
        done.await.unwrap().unwrap();
    }
    processed
}

/// One cell: a fresh meta database and fresh pipeline runtimes, then the
/// timed cold-list iterations.
fn cold_list_cell(
    c: &mut Criterion,
    group_name: &str,
    objects: &[Object],
    io_workers: u8,
    db_workers: u8,
    batch_size: usize,
) {
    let mut group = c.benchmark_group(group_name);
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(TASKS as u64));
    group.bench_function(
        format!("workers_{io_workers}_batch_{batch_size}_db_{db_workers}"),
        |b| {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let pipelines = build_pipelines(io_workers, db_workers);
            let bucket = bucket::name("data").unwrap();
            let rt = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                black_box(rt.block_on(cold_list(
                    &pipelines.io(),
                    &pipelines.db(),
                    &store,
                    &bucket,
                    objects,
                    batch_size,
                )))
            });
        },
    );
    group.finish();
}

/// One entry-size axis: the shared tree (leaked — it must outlive the
/// whole run), then the workers × batch cells.
fn pipeline_axis(
    c: &mut Criterion,
    group_name: &str,
    key_fn: fn(usize) -> String,
    key_bytes: usize,
) {
    let root = tempfile::tempdir().unwrap();
    let objects = build_tree(root.path(), key_fn);
    // The nominal key size is asserted unconditionally (the tree is
    // built outside the timed region, so the check is free) — a key-shape
    // edit cannot silently drift the measured entry size.
    for object in &objects {
        assert_eq!(object.key.len(), key_bytes, "key drifted from nominal size");
    }
    // Leak the tempdir guard: the tree must outlive the whole benchmark
    // run (criterion exits the process afterwards).
    std::mem::forget(root);
    for io_workers in IO_WORKERS {
        for batch in BATCH_SIZES {
            cold_list_cell(c, group_name, &objects, io_workers, 1, batch);
        }
    }
}

fn pipeline_short_key(c: &mut Criterion) {
    pipeline_axis(c, "pipeline_short_key", short_key, SHORT_KEY_BYTES);
}

fn pipeline_long_key(c: &mut Criterion) {
    pipeline_axis(c, "pipeline_long_key", long_key, LONG_KEY_BYTES);
}

/// The db-workers verification cell: io=4, batch=128, short keys — redb
/// is single-writer, so 2 db workers must not beat 1 (pipeline-spec.md
/// task 5).
fn pipeline_db_workers(c: &mut Criterion) {
    let root = tempfile::tempdir().unwrap();
    let objects = build_tree(root.path(), short_key);
    std::mem::forget(root);
    for db_workers in [1, 2] {
        cold_list_cell(c, "pipeline_db_workers", &objects, 4, db_workers, 128);
    }
}

criterion_group!(
    benches,
    pipeline_short_key,
    pipeline_long_key,
    pipeline_db_workers
);
criterion_main!(benches);
