//! Planted orphans: a dry run names them and a fix reclaims them. A live chunk
//! and a publish still in flight stay. The removal re-checks the scan's own
//! predicate, the grace window is pinned, and a pass crosses the scan's page
//! boundary.

#![expect(
    clippy::expect_used,
    reason = "test support: a leg asserts its preconditions by panicking"
)]

use std::time::Duration;

use futures::StreamExt;
use tinio_core::{
    blob::{BlobRef, BlockHash},
    bucket,
    cleanup::{ActionStream, Cleanup, CleanupOptions, RepairAction, RepairKind},
    content::{ContentKey, ObjectContent, StagedObject},
    lifecycle,
    meta::version::encode_blob,
    object,
    storage::{ListObjectsParams, ObjectOps},
};
use tinio_pg::{
    _store_pg::{
        _sqlx::{query, query_scalar},
        PgStore,
        cleanup::{CLEANUP_BATCH_ROWS, OrphanChunk},
        pool::Pool,
        test_support::{TestDb, require_dsn},
    },
    Access, Error, PgStorage,
    cleanup::PgCleanup,
};
use tinio_util::testing::hex;

/// Drain an action stream into the actions it reported.
async fn collect(actions: ActionStream<Error>) -> Vec<RepairAction> {
    let mut out = Vec::new();
    let mut actions = actions;
    while let Some(action) = actions.next().await {
        out.push(action.expect("a repair action"));
    }
    out
}

/// One scalar count, bound to the fixture bucket.
async fn count(pool: &Pool, bucket: &str, sql: &'static str) -> i64 {
    query_scalar::<_, i64>(sql)
        .bind(bucket)
        .fetch_one(pool)
        .await
        .expect("a count")
}

/// One scalar count over a block address.
async fn block_count(pool: &Pool, hash: &str) -> i64 {
    query_scalar::<_, i64>("SELECT count(*) FROM blocks WHERE hash = $1")
        .bind(hash)
        .fetch_one(pool)
        .await
        .expect("a block count")
}

/// The first page of the orphan-chunk scan, at `grace`. The stream ends on the
/// first short page, so a scan with no candidates answers an empty page.
async fn first_chunk_page(store: &PgStore, grace: Duration) -> Vec<OrphanChunk> {
    let mut pages = store.orphan_chunk_pages(grace);
    match pages.next().await {
        Some(page) => page.expect("the scan runs"),
        None => Vec::new(),
    }
}

/// The planted-orphan leg (PG7, PG23): a chunk no head names, a staged part whose upload
/// is gone, an expired version row and a block no version row references are all reported
/// in a dry run and reclaimed by the fix, while a live chunk and a **publish still in
/// flight** are never reported and a listing never shows residue.
#[tokio::test]
async fn planted_orphans_report_in_a_dry_run_and_the_fix_reclaims_them() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");

    let bucket = bucket::name("cleanup-leg").expect("a valid bucket name");
    let wire = "cleanup-leg";
    let part_ckey = format!("p:{}:0001", hex(b"gone-upload"));
    let dead_block = BlockHash::of(b"dead-block").to_hex();

    // The fixture, planted through SQL: a bucket, one **live** object (`live.txt`), and
    // the four orphan shapes the scans are about.
    query(
        "INSERT INTO buckets (name, created, tags, acl, cors) \
         VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the bucket row");
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
         VALUES ($1, 'object', 'o:live.txt', 1, 1, 4)",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the live head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
         VALUES ($1, 'object', 'o:live.txt', 1, 0, $2)",
    )
    .bind(wire)
    .bind(b"live".as_slice())
    .execute(&pool)
    .await
    .expect("the live chunk");
    // The row's ETag is a **domain-valid** wire (32 hex digits): the listing is a read path
    // that heals a cold or stale row, and a hand-planted row an ETag parse refuses would
    // read as absent — a heal on a read-only reader bypasses the entry guard and is refused
    // by the read-only *session* instead of degrading (an open cross-lane defect), so the
    // fixture plants a row the listing serves as it stands.
    query(
        "INSERT INTO object_meta (bucket, key, etag, size, mtime, identity, tags, checksum, acl) \
         VALUES ($1, 'live.txt', 'd41d8cd98f00b204e9800998ecf8427e', 4, now(), 0, '{}'::jsonb, \
         '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the live object row");
    // (a) an orphan chunk: a generation no head names, its row stamped past the grace
    // (an absent/current stamp would read as in flight under the sweep's age gate).
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         VALUES ($1, 'object', 'o:orphan.txt', 7, 0, $2, now() - interval '2 hours')",
    )
    .bind(wire)
    .bind(b"dead".as_slice())
    .execute(&pool)
    .await
    .expect("the orphan chunk");
    // (b) a staged part whose upload is gone: a head and a chunk, no `uploads` row.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
         VALUES ($1, 'part', $2, 1, 1, 1)",
    )
    .bind(wire)
    .bind(part_ckey.as_str())
    .execute(&pool)
    .await
    .expect("the part head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
         VALUES ($1, 'part', $2, 1, 0, $3)",
    )
    .bind(wire)
    .bind(part_ckey.as_str())
    .bind(b"p".as_slice())
    .execute(&pool)
    .await
    .expect("the part chunk");
    // (c) two history version rows at a key with no current slot; the keep budget of one
    // expires the older.
    for (seq, id) in [(1_i64, "planted-v1"), (2_i64, "planted-v2")] {
        query(
            "INSERT INTO version_rows (bucket, key, seq, id, blob, size, etag, created, \
             became_noncurrent, kind, tags, checksum, acl) \
             VALUES ($1, 'v.txt', $2, $3, '', 0, 'planted', now(), 1, 'object', \
             '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
        )
        .bind(wire)
        .bind(seq)
        .bind(id)
        .execute(&pool)
        .await
        .expect("a planted version row");
    }
    // (d) a block no version row references, published past the grace.
    query(
        "INSERT INTO blocks (hash, seq, chunk, len, published) \
         VALUES ($1, 0, $2, 1, now() - interval '1 hour')",
    )
    .bind(dead_block.as_str())
    .bind(b"b".as_slice())
    .execute(&pool)
    .await
    .expect("the dead block");

    // C1's fixture: a publish **still in flight**. `stage` allocates the generation and
    // commits its chunks while the head's `live_seq` is still NULL — exactly what the
    // orphan predicate must not read as residue.
    let inflight = object::key("inflight.txt").expect("a valid key");
    let mut staged = store
        .stage(&bucket, &ContentKey::Object(inflight.clone()))
        .await
        .expect("a staged generation");
    staged
        .write(vec![0_u8; 4 * 1024 * 1024].into())
        .await
        .expect("the staged body");
    drop(staged);

    // PG23: a listing is row-backed, so both the residue and the in-flight object are
    // invisible before any cleanup runs.
    let reader = PgStorage::connect_with_pool(pool.clone(), Access::ReadOnly)
        .await
        .expect("a read-only reader");
    let listed: Vec<String> = reader
        .list_objects(ListObjectsParams {
            bucket: bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            start_after: None,
            max_keys: 100,
        })
        .await
        .expect("a listing")
        .objects
        .iter()
        .map(|info| info.key.to_string())
        .collect();
    assert!(
        listed.iter().any(|key| key == "live.txt"),
        "the live object is listed: {listed:?}"
    );
    assert!(
        !listed.iter().any(|key| key == "orphan.txt"),
        "a listing never shows residue: {listed:?}"
    );
    assert!(
        !listed.iter().any(|key| key == "inflight.txt"),
        "a listing never shows an unflipped publish: {listed:?}"
    );

    // I2: the store's own mode decides. A read-only store's report still diagnoses, and
    // its real run writes nothing at all.
    let read_only = PgStore::connect_with_pool(pool.clone(), Access::ReadOnly)
        .await
        .expect("a read-only store");
    let ro_report = PgCleanup::new(
        read_only.clone(),
        CleanupOptions {
            dry_run: true,
            ..Default::default()
        },
    );
    let actions = collect(ro_report.repair(RepairKind::Full).await.expect("a report")).await;
    assert!(
        actions
            .iter()
            .any(|action| action.description.contains("orphan.txt")),
        "a read-only dry run still reports: {actions:?}"
    );
    let ro_fix = PgCleanup::new(read_only.clone(), CleanupOptions::default());
    let _ = collect(ro_fix.repair(RepairKind::Full).await.expect("a run")).await;
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:orphan.txt'"
        )
        .await,
        1,
        "a read-only store's cleanup removes nothing, whatever the options say"
    );

    // The dry run: every orphan is named, the live chunk and the in-flight publish are
    // not, and nothing is removed.
    let dry = PgCleanup::new(
        store.clone(),
        CleanupOptions {
            dry_run: true,
            ..Default::default()
        },
    )
    .with_retention(1, 0);
    let actions = collect(dry.repair(RepairKind::Full).await.expect("a dry run")).await;
    let text = actions
        .iter()
        .map(|action| action.description.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("orphan.txt"), "the orphan chunk: {text}");
    assert!(text.contains(&part_ckey), "the upload-gone part: {text}");
    assert!(text.contains("v.txt"), "the expired version row: {text}");
    assert!(
        text.contains("unreferenced block"),
        "the dead block: {text}"
    );
    assert!(
        !text.contains("live.txt"),
        "a live chunk is never reported: {text}"
    );
    assert!(
        !text.contains("inflight.txt"),
        "a publish still in flight is never reported: {text}"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1"
        )
        .await,
        7,
        "a dry run removes nothing"
    );

    // The fix: the same handle in write mode reclaims exactly those rows.
    let fix = PgCleanup::new(store.clone(), CleanupOptions::default()).with_retention(1, 0);
    let actions = collect(fix.repair(RepairKind::Full).await.expect("a fix")).await;
    assert!(
        actions
            .iter()
            .any(|action| action.description.contains("reclaimed")),
        "the fix reports what it reclaimed: {actions:?}"
    );

    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:orphan.txt'"
        )
        .await,
        0,
        "the orphan chunk is reclaimed"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content_heads WHERE bucket = $1 AND kind = 'part'"
        )
        .await,
        0,
        "the upload-gone part's head is reclaimed"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND kind = 'part'"
        )
        .await,
        0,
        "the upload-gone part's chunks are reclaimed"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM version_rows WHERE bucket = $1 AND seq = 1"
        )
        .await,
        0,
        "the older history row is expired"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM version_rows WHERE bucket = $1 AND seq = 2"
        )
        .await,
        1,
        "the keep budget spares the newest history row"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:live.txt'"
        )
        .await,
        1,
        "a live chunk is never reclaimed"
    );
    assert_eq!(
        query_scalar::<_, i64>("SELECT count(*) FROM blocks WHERE hash = $1")
            .bind(dead_block.as_str())
            .fetch_one(&pool)
            .await
            .expect("a count"),
        0,
        "the dead block is removed, not merely reported"
    );
    assert!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:inflight.txt'"
        )
        .await
            > 0,
        "a publish still in flight keeps its chunks"
    );

    // The grace is a window, not a permanent shield: aged past it on **both** gates —
    // the head's allocation and the rows' own write stamp — a crashed partial publish's
    // chunks are reclaimed like any other residue.
    query(
        "UPDATE content_heads SET allocated_at = now() - interval '2 hours' \
         WHERE bucket = $1 AND ckey = 'o:inflight.txt'",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the aged allocation");
    query(
        "UPDATE content SET published = now() - interval '2 hours' \
         WHERE bucket = $1 AND ckey = 'o:inflight.txt'",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the aged rows");
    let aged = PgCleanup::new(store.clone(), CleanupOptions::default());
    let _ = collect(aged.repair(RepairKind::Full).await.expect("a control run")).await;
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o:inflight.txt'"
        )
        .await,
        0,
        "past the grace, a crashed generation's chunks are reclaimed"
    );

    drop(reader);
    drop(read_only);
    drop(store);
    db.drop_db().await;
}

/// The removal re-checks the scan's own predicate (finding 1). The scan and the
/// removal are two statements and the store keeps serving between them, so a
/// generation the flip made **live** after the scan, and one a retried publish
/// re-stamped inside the grace after the scan, are both spared: the scan's key
/// alone is not proof of residue, and deleting either one would take bytes a
/// row and its ETag still name.
#[tokio::test]
async fn the_removal_rechecks_the_scan_predicate_on_its_own() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");
    let wire = "cleanup-phases";
    let grace = Duration::from_secs(600);

    query(
        "INSERT INTO buckets (name, created, tags, acl, cors) \
         VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the bucket row");
    // (a) A generation the head has not flipped: its allocation has aged, so the
    // head gate protects it only while `live_seq` still does not name it.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len, allocated_at) \
         VALUES ($1, 'object', 'o/flip.txt', 3, NULL, 0, now() - interval '2 hours')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the flipping head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         VALUES ($1, 'object', 'o/flip.txt', 3, 0, $2, now() - interval '2 hours')",
    )
    .bind(wire)
    .bind(b"flip".as_slice())
    .execute(&pool)
    .await
    .expect("the flipping chunk");
    // (b) A generation a later allocation superseded — so the head gate does not
    // protect it at all — whose rows a retried publish re-stamps in place.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len, allocated_at) \
         VALUES ($1, 'object', 'o/restamp.txt', 7, NULL, 0, now() - interval '2 hours')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the re-stamped head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         VALUES ($1, 'object', 'o/restamp.txt', 5, 0, $2, now() - interval '2 hours')",
    )
    .bind(wire)
    .bind(b"restamp".as_slice())
    .execute(&pool)
    .await
    .expect("the re-stamped chunk");

    // The scan, then the two writers' commits, then the removal.
    let page = first_chunk_page(&store, grace).await;
    assert!(
        page.iter()
            .any(|row| row.ckey == "o/flip.txt" && row.seq == 3),
        "the unflipped generation is a candidate: {page:?}"
    );
    assert!(
        page.iter()
            .any(|row| row.ckey == "o/restamp.txt" && row.seq == 5),
        "the superseded generation is a candidate: {page:?}"
    );
    query(
        "UPDATE content_heads SET live_seq = 3, live_len = 4 \
         WHERE bucket = $1 AND ckey = 'o/flip.txt'",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the flip commits");
    query(
        "UPDATE content SET published = now() \
         WHERE bucket = $1 AND ckey = 'o/restamp.txt'",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the retried publish re-stamps the same generation");

    let gone = store
        .reclaim_orphan_chunks(&page, grace)
        .await
        .expect("the removal runs");
    assert_eq!(
        gone, 0,
        "the removal spares every row the scan no longer calls residue"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o/flip.txt'"
        )
        .await,
        1,
        "a generation the flip made live keeps its bytes"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o/restamp.txt'"
        )
        .await,
        1,
        "a generation re-stamped inside the grace keeps its bytes"
    );

    // A whole policy pass leaves both as well: one is live, the other is inside
    // the window on both of its predicates.
    let fix = PgCleanup::new(store.clone(), CleanupOptions::default());
    let actions = collect(fix.repair(RepairKind::Full).await.expect("a pass")).await;
    assert!(
        actions
            .iter()
            .all(|action| !action.description.contains("flip.txt")
                && !action.description.contains("restamp.txt")),
        "the sweep leaves both generations alone: {actions:?}"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1 AND ckey = 'o/flip.txt'"
        )
        .await,
        1,
        "the pass removes nothing of the live generation"
    );

    db.drop_db().await;
}

/// The grace window is pinned (finding 3) and the rewrite keeps it: a generation
/// whose rows are stamped and whose allocation is made **this instant** is
/// residue only under a zero grace, which is no window at all. The two
/// predicates that carry the window are the row's own `published` stamp and the
/// head's `allocated_at`, and both phases read both of them.
#[tokio::test]
async fn a_zero_grace_turns_a_just_stamped_generation_into_an_orphan() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");
    let wire = "cleanup-grace";

    query(
        "INSERT INTO buckets (name, created, tags, acl, cors) \
         VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the bucket row");
    // (a) A generation a later allocation superseded — the head gate cannot
    // protect it — whose rows are stamped **this instant** by the column's
    // default: only the row's own `published` carries the window.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
         VALUES ($1, 'object', 'o/restamped.txt', 2, NULL, 0)",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the superseding head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
         VALUES ($1, 'object', 'o/restamped.txt', 1, 0, $2)",
    )
    .bind(wire)
    .bind(b"restamped".as_slice())
    .execute(&pool)
    .await
    .expect("the just-stamped chunk");
    // (b) The head's current allocation, allocated this instant, whose rows have
    // aged: only the head's `allocated_at` carries the window for it.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
         VALUES ($1, 'object', 'o/allocated.txt', 1, NULL, 0)",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the freshly allocated head");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         VALUES ($1, 'object', 'o/allocated.txt', 1, 0, $2, now() - interval '2 hours')",
    )
    .bind(wire)
    .bind(b"allocated".as_slice())
    .execute(&pool)
    .await
    .expect("the aged chunk of a fresh allocation");

    // The window spares both, each on its own bearer.
    let page = first_chunk_page(&store, Duration::from_secs(600)).await;
    assert!(
        page.is_empty(),
        "a just-stamped row and a fresh allocation are not residue: {page:?}"
    );
    // A zero grace is no window at all: both predicates fall out at once.
    let page = first_chunk_page(&store, Duration::ZERO).await;
    assert!(
        page.iter().any(|row| row.ckey == "o/restamped.txt")
            && page.iter().any(|row| row.ckey == "o/allocated.txt"),
        "a zero grace turns both generations into orphans: {page:?}"
    );
    let gone = store
        .reclaim_orphan_chunks(&page, Duration::ZERO)
        .await
        .expect("the removal runs");
    assert_eq!(gone, 2, "the zero-grace removal takes exactly those rows");

    db.drop_db().await;
}

/// A pass crosses the scan's page boundary (finding 2): the scans are keyset
/// pages of [`CLEANUP_BATCH_ROWS`] rows, so a store holding one more candidate
/// than a page is still swept whole — every candidate named, every candidate
/// removed, and none read twice.
#[tokio::test]
async fn a_pass_reclaims_every_candidate_past_the_first_page() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");
    let wire = "cleanup-pages";
    let many = CLEANUP_BATCH_ROWS + 1;

    query(
        "INSERT INTO buckets (name, created, tags, acl, cors) \
         VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
    )
    .bind(wire)
    .execute(&pool)
    .await
    .expect("the bucket row");
    // One more orphan generation than a page holds, each its own key.
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         SELECT $1, 'object', 'o/page-' || lpad(g::text, 5, '0'), 1, 0, $2, \
         now() - interval '2 hours' \
         FROM generate_series(1, $3) AS g",
    )
    .bind(wire)
    .bind(b"p".as_slice())
    .bind(many)
    .execute(&pool)
    .await
    .expect("the orphan generations");
    // And the same count of upload-gone parts, heads and chunks alike. The part
    // key is the wire the scan's own pattern matches: hex of the upload id.
    query(
        "INSERT INTO content_heads (bucket, kind, ckey, next_seq, live_seq, live_len) \
         SELECT $1, 'part', 'p:' || encode(('gone-' || lpad(g::text, 5, '0'))::bytea, \
         'hex') || ':0001', 1, 1, 1 FROM generate_series(1, $2) AS g",
    )
    .bind(wire)
    .bind(many)
    .execute(&pool)
    .await
    .expect("the orphan part heads");
    query(
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk) \
         SELECT $1, 'part', 'p:' || encode(('gone-' || lpad(g::text, 5, '0'))::bytea, \
         'hex') || ':0001', 1, 0, $2 FROM generate_series(1, $3) AS g",
    )
    .bind(wire)
    .bind(b"q".as_slice())
    .bind(many)
    .execute(&pool)
    .await
    .expect("the orphan part chunks");

    // The bound itself: one page is one batch, the remainder is the next page,
    // and the scan ends on the page after that — never on a full one.
    let mut pages = store.orphan_chunk_pages(Duration::from_secs(600));
    let first = pages.next().await.expect("a page").expect("the first page");
    assert_eq!(
        first.len(),
        CLEANUP_BATCH_ROWS as usize,
        "one page is one batch, not the whole scan"
    );
    let second = pages
        .next()
        .await
        .expect("a page")
        .expect("the second page");
    assert_eq!(
        second.len(),
        (many - CLEANUP_BATCH_ROWS) as usize,
        "the candidates past the first page are the next page"
    );
    assert!(pages.next().await.is_none(), "and the scan ends there");
    drop(pages);

    let dry = PgCleanup::new(
        store.clone(),
        CleanupOptions {
            dry_run: true,
            ..Default::default()
        },
    );
    let actions = collect(dry.repair(RepairKind::Full).await.expect("a report")).await;
    let chunks = actions
        .iter()
        .filter(|action| {
            action
                .description
                .contains("would reclaim orphan object chunk")
        })
        .count();
    let parts = actions
        .iter()
        .filter(|action| action.description.contains("whose upload is gone"))
        .count();
    assert_eq!(chunks, many as usize, "every chunk candidate is named");
    assert_eq!(parts, many as usize, "every part candidate is named");
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1"
        )
        .await,
        many * 2,
        "a report removes nothing, however many pages it reads"
    );

    let fix = PgCleanup::new(store.clone(), CleanupOptions::default());
    let actions = collect(fix.repair(RepairKind::Full).await.expect("a fix")).await;
    assert!(
        actions
            .iter()
            .any(|action| action.description.contains("reclaimed")),
        "the fix reports what it reclaimed: {actions:?}"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content WHERE bucket = $1"
        )
        .await,
        0,
        "every page's rows are reclaimed"
    );
    assert_eq!(
        count(
            &pool,
            wire,
            "SELECT count(*) FROM content_heads WHERE bucket = $1 AND kind = 'part'"
        )
        .await,
        0,
        "every page's part head is reclaimed with its chunks"
    );

    db.drop_db().await;
}

/// The block sweep's live set spans every page of the version scan (finding 2):
/// a block only a version row past the first page names is still live, while a
/// block no row names is reclaimed.
#[tokio::test]
async fn the_live_set_spans_every_page_of_the_version_scan() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");
    let wire = "cleanup-live-pages";
    let many = CLEANUP_BATCH_ROWS + 1;

    let head_block = BlockHash::of(b"page-head").to_hex();
    let tail_block = BlockHash::of(b"page-tail").to_hex();
    let dead_block = BlockHash::of(b"page-dead").to_hex();
    let head_blob = encode_blob(&BlobRef::single(BlockHash::of(b"page-head"), 1));
    let tail_blob = encode_blob(&BlobRef::single(BlockHash::of(b"page-tail"), 1));
    for hash in [
        head_block.as_str(),
        tail_block.as_str(),
        dead_block.as_str(),
    ] {
        query(
            "INSERT INTO blocks (hash, seq, chunk, len, published) \
             VALUES ($1, 0, $2, 1, now() - interval '1 hour')",
        )
        .bind(hash)
        .bind(b"b".as_slice())
        .execute(&pool)
        .await
        .expect("a planted block");
    }
    // The version rows are read in key order, so the **largest** key is the last
    // page's row: the block only it names is the one a dropped page would
    // reclaim.
    query(
        "INSERT INTO version_rows (bucket, key, seq, id, blob, size, etag, created, \
         became_noncurrent, kind, tags, checksum, acl) \
         SELECT $1, 'k-' || lpad(g::text, 5, '0'), 1, 'id-' || g, \
         CASE WHEN g = $4 THEN $2 ELSE $3 END, 1, 'e', now(), 0, 'object', \
         '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl \
         FROM generate_series(1, $4) AS g",
    )
    .bind(wire)
    .bind(tail_blob.as_str())
    .bind(head_blob.as_str())
    .bind(many)
    .execute(&pool)
    .await
    .expect("the version pages");

    let fix = PgCleanup::new(store.clone(), CleanupOptions::default());
    let _ = collect(fix.repair(RepairKind::Full).await.expect("a sweep")).await;
    assert_eq!(
        block_count(&pool, head_block.as_str()).await,
        1,
        "a block the first page names is live"
    );
    assert_eq!(
        block_count(&pool, tail_block.as_str()).await,
        1,
        "a block only the last page names is live"
    );
    assert_eq!(
        block_count(&pool, dead_block.as_str()).await,
        0,
        "a block no version row names is reclaimed"
    );

    db.drop_db().await;
}

/// LC21: a bucket whose stored configuration carries
/// `NoncurrentVersionExpiration` is governed by its rule for that action, so
/// `[s3] noncurrent_keep` does not apply to it — while a bucket that carries no
/// such rule, and one whose rule is `Disabled`, retire by the knob in the same
/// pass.
#[tokio::test]
async fn a_bucket_with_a_noncurrent_rule_keeps_its_versions_past_the_server_knob() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let store = PgStore::connect_with_pool(pool.clone(), Access::ReadWrite)
        .await
        .expect("a writable store (migrating the scratch database)");

    let governed = "cleanup-governed";
    let ungoverned = "cleanup-ungoverned";
    let disabled = "cleanup-disabled";
    for wire in [governed, ungoverned, disabled] {
        query(
            "INSERT INTO buckets (name, created, tags, acl, cors) \
             VALUES ($1, now(), '{}'::jsonb, ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
        )
        .bind(wire)
        .execute(&pool)
        .await
        .expect("the bucket row");
        // Two history rows at a key with no current slot: a keep budget of one
        // retires the older, which is what the server's knob says.
        for (seq, id) in [(1_i64, "v1"), (2_i64, "v2")] {
            query(
                "INSERT INTO version_rows (bucket, key, seq, id, blob, size, etag, created, \
                 became_noncurrent, kind, tags, checksum, acl) \
                 VALUES ($1, 'v.txt', $2, $3, '', 0, 'planted', now(), 1, 'object', \
                 '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
            )
            .bind(wire)
            .bind(seq)
            .bind(format!("{wire}-{id}"))
            .execute(&pool)
            .await
            .expect("a planted version row");
        }
    }

    // The governed bucket's rule, at a threshold the server's knob would beat —
    // and the same rule `Disabled` on the third bucket, which governs nothing.
    let config = |status| lifecycle::Config {
        rules: vec![lifecycle::Rule {
            id: None,
            status,
            filter: lifecycle::Filter::default(),
            expiration: None,
            noncurrent_version_expiration: Some(lifecycle::NoncurrentVersionExpiration {
                noncurrent_days: Some(30),
                newer_noncurrent_versions: None,
            }),
            abort_incomplete_multipart_upload: None,
        }],
    };
    for (wire, config) in [
        (governed, config(lifecycle::Status::Enabled)),
        (disabled, config(lifecycle::Status::Disabled)),
    ] {
        query("INSERT INTO bucket_lifecycle (bucket, config, updated_at) VALUES ($1, $2, now())")
            .bind(wire)
            .bind(config.to_wire())
            .execute(&pool)
            .await
            .expect("the lifecycle row");
    }

    let cleanup = PgCleanup::new(store.clone(), CleanupOptions::default()).with_retention(1, 0);
    let actions = collect(cleanup.repair(RepairKind::Full).await.expect("a pass")).await;

    assert_eq!(
        count(
            &pool,
            governed,
            "SELECT count(*) FROM version_rows WHERE bucket = $1"
        )
        .await,
        2,
        "the governed bucket's rule owns noncurrent retirement: {actions:?}"
    );
    for wire in [ungoverned, disabled] {
        assert_eq!(
            count(
                &pool,
                wire,
                "SELECT count(*) FROM version_rows WHERE bucket = $1"
            )
            .await,
            1,
            "{wire} retires by the knob in the same pass"
        );
    }

    db.drop_db().await;
}
