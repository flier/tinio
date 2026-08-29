//! Prefix/delimiter listing benchmarks (task T031).
//!
//! The S3 `ListObjectsV2` path over a generated tree: a flat listing, a
//! delimiter-rolled listing, and a prefixed listing. Baselines are
//! recorded in Phase 6 (T088) and regression-gated.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use tinio_core::{
    bucket, object,
    storage::{BucketOps, ListObjectsParams, ObjectOps},
};
use tinio_fs::FsStorage;
use tinio_fs::testing::fs_options;

use tinio_util::testing::body;

/// Tree shape: 4 prefixes × 1000 objects each + 500 flat objects.
const PREFIXES: usize = 4;
const PER_PREFIX: usize = 1000;
const FLAT: usize = 500;

fn listing(c: &mut Criterion) {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bname = bucket::name("data").unwrap();
    rt.block_on(async {
        storage.create_bucket(&bname).await.unwrap();
        for i in 0..FLAT {
            storage
                .put_object(
                    &bname,
                    &object::key(format!("flat-{i:04}.bin")).unwrap(),
                    body(b"x"),
                )
                .await
                .unwrap();
        }
        for p in 0..PREFIXES {
            for i in 0..PER_PREFIX {
                storage
                    .put_object(
                        &bname,
                        &object::key(format!("dir-{p}/obj-{i:04}.bin")).unwrap(),
                        body(b"x"),
                    )
                    .await
                    .unwrap();
            }
        }
    });

    let mut group = c.benchmark_group("listing");
    group.throughput(criterion::Throughput::Elements(
        (PREFIXES * PER_PREFIX + FLAT) as u64,
    ));
    group.bench_function("flat_full", |b| {
        b.to_async(rt.handle().clone()).iter(|| {
            let storage = storage.clone();
            let bname = bname.clone();
            async move {
                let page = storage
                    .list_objects(ListObjectsParams {
                        bucket: bname,
                        prefix: String::new(),
                        delimiter: None,
                        start_after: None,
                        max_keys: 1000,
                    })
                    .await
                    .unwrap();
                black_box(page.objects.len());
            }
        });
    });
    group.bench_function("delimiter_rollup", |b| {
        b.to_async(rt.handle().clone()).iter(|| {
            let storage = storage.clone();
            let bname = bname.clone();
            async move {
                let page = storage
                    .list_objects(ListObjectsParams {
                        bucket: bname,
                        prefix: String::new(),
                        delimiter: Some("/".into()),
                        start_after: None,
                        max_keys: 1000,
                    })
                    .await
                    .unwrap();
                black_box((page.objects.len(), page.common_prefixes.len()));
            }
        });
    });
    group.bench_function("prefixed", |b| {
        b.to_async(rt.handle().clone()).iter(|| {
            let storage = storage.clone();
            let bname = bname.clone();
            async move {
                let page = storage
                    .list_objects(ListObjectsParams {
                        bucket: bname,
                        prefix: "dir-2/".into(),
                        delimiter: None,
                        start_after: None,
                        max_keys: 1000,
                    })
                    .await
                    .unwrap();
                black_box(page.objects.len());
            }
        });
    });
    group.finish();
}

criterion_group!(benches, listing);
criterion_main!(benches);
