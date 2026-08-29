//! Multipart assembly benchmarks (task T031).
//!
//! The composed-object path the S3 mapping layer drives: upload N parts,
//! then `complete_multipart_upload` assembles them byte-exact into the
//! object. Baselines are recorded in Phase 6 (T088) and regression-gated.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use tinio_core::{
    bucket,
    multipart::CompletedPart,
    object,
    storage::{BucketOps, MultipartOps, ObjectOps},
};
use tinio_fs::FsStorage;
use tinio_fs::testing::fs_options;

use tinio_util::testing::body;

/// Part count of the assembly benchmark (each part 256 KiB → 64 MiB
/// composed object).
const PARTS: usize = 256;
const PART_SIZE: usize = 256 * 1024;

fn multipart_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("multipart_assembly");
    group.throughput(criterion::Throughput::Bytes((PARTS * PART_SIZE) as u64));
    group.bench_function("complete_64MiB_256_parts", |b| {
        let root = tempfile::tempdir().unwrap();
        let storage = FsStorage::new(root.path(), fs_options()).unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let bname = bucket::name("data").unwrap();
        let key = object::key("big.bin").unwrap();
        rt.block_on(async {
            storage.create_bucket(&bname).await.unwrap();
        });
        b.to_async(rt).iter(|| {
            let storage = storage.clone();
            let bname = bname.clone();
            let key = key.clone();
            async move {
                // Re-create the upload each iteration (a completed upload is
                // consumed).
                let upload = storage.create_multipart_upload(&bname, &key).await.unwrap();
                let part_data: Vec<u8> = vec![b'p'; PART_SIZE];
                let mut completed = Vec::with_capacity(PARTS);
                for i in 1..=PARTS {
                    let part = storage
                        .upload_part(
                            &bname,
                            &key,
                            &upload.upload_id,
                            (i as u32).into(),
                            body(part_data.clone()),
                        )
                        .await
                        .unwrap();
                    completed.push(CompletedPart {
                        part_number: part.part_number,
                        etag: part.etag,
                    });
                }
                let info = storage
                    .complete_multipart_upload(&bname, &key, &upload.upload_id, &completed)
                    .await
                    .unwrap();
                black_box(info.size);
                storage.delete_object(&bname, &key).await.unwrap();
            }
        });
    });
    group.finish();
}

criterion_group!(benches, multipart_assembly);
criterion_main!(benches);
