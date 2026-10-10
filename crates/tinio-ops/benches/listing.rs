//! The listing benchmark (spec §3): a **paginated full listing** over 10⁴–10⁶
//! entries, the cost pin the rewritten listing path owes (round 38's finding on
//! benchmarks: the path is new, so nothing else measures it).
//!
//! What is measured is one complete walk of the bucket in `max_keys` pages —
//! the shape an S3 client's sweep has, and the shape the engine's two cost
//! rules bind: the enrichment is **one `ObjectMeta::load` per page** (never a
//! point read per key) and the scan carries the prefix bound. The rate is
//! reported in entries, so a regression in either rule shows up as a collapse
//! of throughput, not as a constant offset.
//!
//! The 10⁶ leg is opt-in (`TINIO_OPS_BENCH_LARGE=1`): its *fixture* costs more
//! than the measurement, and the default run is meant to stay short enough for
//! the acceptance gate. The three legs share one fixture per size for the same
//! reason: a fixture is built once per size, never once per leg.

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

use std::{
    collections::HashMap,
    env,
    sync::{Arc, LazyLock, Mutex},
    time::SystemTime,
};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tinio_core::{
    ETag, ListObjectsParams,
    acl::Acl,
    bucket,
    content::{ContentKey, ObjectContent, StagedObject},
    meta::{ObjectMeta, object::Row as MetaObjectRow},
    object,
};
use tinio_ops::{BucketOps, BucketRow, MetadataFailure, ObjectOps};
use tinio_store_redb::{RedbContent, RedbMetadata};
use tokio::runtime::Runtime;

/// The page size the walk pages with (S3's own default `max_keys`).
const PAGE: usize = 1000;

/// The fixture sizes: the spec's 10⁴–10⁵ legs, plus 10⁶ behind the opt-in.
fn sizes() -> Vec<usize> {
    let mut sizes = vec![10_000, 100_000];
    if env::var("TINIO_OPS_BENCH_LARGE").is_ok_and(|value| value == "1") {
        sizes.push(1_000_000);
    }
    sizes
}

/// One built fixture: the runtime the legs run on, the bucket's name, and the
/// object group's receiver.
struct Fixture {
    runtime: Runtime,
    name: String,
    ops: ObjectOps<RedbMetadata, RedbContent>,
}

/// The built fixtures, one per size: the three legs measure the same bucket,
/// and building it (a stage/publish round trip per entry) costs more than the
/// measurement does.
static FIXTURES: LazyLock<Mutex<HashMap<usize, Fixture>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Build `entries`'s fixture on first use; every leg over that size takes the
/// same one.
fn with_fixture<R>(entries: usize, f: impl FnOnce(&Fixture) -> R) -> R {
    let mut fixtures = FIXTURES.lock().unwrap();
    let fixture = fixtures
        .entry(entries)
        .or_insert_with(|| populated(entries));
    f(fixture)
}

/// A bucket holding `entries` one-byte objects, half of them under `dir/`.
///
/// The bucket goes in through the engine's own `create_bucket` receiver (the
/// real create path, content-plane namespace included). The fixture writes the
/// content and the rows separately: the content through the content plane's
/// stage/publish pair (what a real writer pays) and the rows through one
/// batched `put_batch` per thousand keys — the rows are not what this bench
/// measures, and a per-key row write would dominate the setup. The object
/// group's receiver carries no hooks, so the seam the create reaches is
/// [`tinio_ops::NoHooks`].
fn populated(entries: usize) -> Fixture {
    let runtime = Runtime::new().unwrap();
    let meta = Arc::new(RedbMetadata::in_memory().unwrap());
    let content = Arc::new(RedbContent::from_driver(meta.driver()));
    let name = bucket::name("data").unwrap();
    let buckets = BucketOps {
        meta: Arc::clone(&meta),
        content: Arc::clone(&content),
        hooks: tinio_ops::NoHooks,
        metadata_failure: MetadataFailure::Fail,
        bucket_row: BucketRow::Guaranteed,
    };
    let objects = ObjectOps {
        meta: Arc::clone(&meta),
        content: Arc::clone(&content),
        metadata_failure: MetadataFailure::Fail,
        bucket_row: BucketRow::Guaranteed,
    };
    runtime.block_on(async {
        buckets
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        let mut rows = Vec::with_capacity(1024);
        for index in 0..entries {
            // Three shapes: flat keys, one key per its own directory, and a
            // quarter of them under one shared prefix — the group a delimited
            // page has to walk (and the case a listing that remembered what it
            // walked would pay for twice).
            let key = match index % 4 {
                0 => object::key(format!("flat-{index:07}")).unwrap(),
                2 => object::key(format!("dir-{index:07}/leaf")).unwrap(),
                _ => object::key(format!("big/{index:07}")).unwrap(),
            };
            let content_key = ContentKey::Object(key.clone());
            let mut staged = content.stage(&name, &content_key).await.unwrap();
            staged.write(Bytes::from_static(b"x")).await.unwrap();
            content.publish(&name, staged, &content_key).await.unwrap();
            rows.push((
                key,
                MetaObjectRow {
                    etag: ETag::from_content(b"x"),
                    size: 1,
                    mtime: SystemTime::now(),
                    identity: 0,
                    tags: object::Tags::empty(),
                    checksum: None,
                    owner: None,
                    acl: Acl::default_private(None),
                },
            ));
            if rows.len() == 1024 {
                meta.put_batch(&name, &rows).await.unwrap();
                rows.clear();
            }
        }
        if !rows.is_empty() {
            meta.put_batch(&name, &rows).await.unwrap();
        }
    });
    Fixture {
        runtime,
        name: name.to_string(),
        ops: objects,
    }
}

/// Walk the whole bucket in `max_keys` pages, returning the object count.
async fn walk(ops: &ObjectOps<RedbMetadata, RedbContent>, name: &bucket::Name) -> usize {
    let mut seen = 0usize;
    let mut start_after: Option<String> = None;
    loop {
        let page = ops
            .list_objects(
                ListObjectsParams {
                    bucket: name.clone(),
                    prefix: String::new(),
                    delimiter: None,
                    start_after: start_after.clone(),
                    max_keys: PAGE,
                },
                false,
            )
            .await
            .unwrap();
        seen += page.objects.len();
        match page.next_start_after {
            Some(marker) if page.truncated => start_after = Some(marker),
            _ => break,
        }
    }
    seen
}

/// The paginated full listing over each fixture size.
fn paginated_listing(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("listing_full_walk");
    for entries in sizes() {
        with_fixture(entries, |fixture| {
            let Fixture { runtime, ops, .. } = fixture;
            let name = bucket::name(fixture.name.as_str()).unwrap();
            // The fixture is asserted, not assumed: a walk that does not see
            // every entry would measure the wrong thing.
            assert_eq!(
                runtime.block_on(walk(ops, &name)),
                entries,
                "the fixture lists whole"
            );
            group.throughput(Throughput::Elements(entries as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(entries),
                &entries,
                |bencher, _| {
                    bencher.iter(|| runtime.block_on(walk(ops, &name)));
                },
            );
        });
    }
    group.finish();
}

/// The cost of a **delimited** page whose second entry sits behind a group a
/// quarter of the bucket wide.
///
/// This is the leg a listing that remembered what it scanned would pay for
/// twice: the page is two entries (`big/` and the first flat key), but the
/// engine has to walk the whole group to know the group is one entry — and it
/// holds one scan window of it, never the group.
fn delimited_group_page(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("listing_delimited_group");
    for entries in sizes() {
        with_fixture(entries, |fixture| {
            let Fixture { runtime, ops, .. } = fixture;
            let name = bucket::name(fixture.name.as_str()).unwrap();
            // The entries this leg walks: the group, not the bucket.
            group.throughput(Throughput::Elements((entries / 4) as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(entries),
                &entries,
                |bencher, _| {
                    bencher.iter(|| {
                        runtime.block_on(async {
                            ops.list_objects(
                                ListObjectsParams {
                                    bucket: name.clone(),
                                    prefix: String::new(),
                                    delimiter: Some("/".to_owned()),
                                    start_after: None,
                                    max_keys: 1,
                                },
                                false,
                            )
                            .await
                            .unwrap()
                        })
                    });
                },
            );
        });
    }
    group.finish();
}

/// The per-page cost of a **prefixed** listing: the scan's prefix bound is what
/// keeps a prefixed page from rescanning the bucket, so this leg must not grow
/// with the bucket.
fn prefixed_page(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("listing_prefixed_page");
    for entries in sizes() {
        with_fixture(entries, |fixture| {
            let Fixture { runtime, ops, .. } = fixture;
            let name = bucket::name(fixture.name.as_str()).unwrap();
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(
                BenchmarkId::from_parameter(entries),
                &entries,
                |bencher, _| {
                    bencher.iter(|| {
                        runtime.block_on(async {
                            ops.list_objects(
                                ListObjectsParams {
                                    bucket: name.clone(),
                                    prefix: "flat-00000".to_owned(),
                                    delimiter: None,
                                    start_after: None,
                                    max_keys: PAGE,
                                },
                                false,
                            )
                            .await
                            .unwrap()
                        })
                    });
                },
            );
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    paginated_listing,
    delimited_group_page,
    prefixed_page
);
criterion_main!(benches);
