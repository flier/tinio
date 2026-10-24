//! The abandoned-upload stage: `[s3] multipart_expire_days` on pg
//! ([`PgCleanup::abort_stale_uploads`]).
//!
//! Three facts are the filesystem sweeper's own legs' assert, restated on pg: an
//! upload past the timeout is aborted, one inside it survives, and a bucket whose
//! stored lifecycle configuration carries `AbortIncompleteMultipartUpload` is
//! governed by its rule for that action (LC21), so the server's timeout does not
//! apply to it. The same bucket is swept by the timeout in a run that builds no
//! lifecycle pass: nothing is governed then, so turning the capability off never
//! stops retention (LC23). A fourth is the two backends' shared age rule: the timeout is
//! measured from the upload's **latest activity**, `max(initiated_at, latest part
//! write)`, so an upload a client is still feeding survives.
//!
//! The database is read from `TINIO_PG_DSN`; with the variable unset every leg
//! prints the skip notice and returns, which is what keeps `cargo test
//! --workspace` database-free (PG8). Each leg creates its **own scratch
//! database** through `tinio-store-pg`'s `testing` harness (`TestDb`).

#![expect(
    clippy::expect_used,
    reason = "test support: a leg asserts its preconditions by panicking"
)]

use std::time::Duration;

use tinio_core::{
    acl::Acl,
    bucket,
    cleanup::CleanupOptions,
    lifecycle,
    meta::BucketMeta,
    object,
    storage::{BucketOps, ListUploadsParams, MultipartOps},
};
use tinio_pg::{
    _store_pg::{
        _sqlx::query,
        pool::Pool,
        test_support::{TestDb, require_dsn},
    },
    Access, PgStorage,
    cleanup::PgCleanup,
};
use tinio_util::testing::{body, bucket_name, part_number};

/// One hour: the legs' own timeout, so an upload two hours old is past it and a
/// freshly created one is not.
const TTL: Duration = Duration::from_secs(3600);

/// A writable backend over a scratch database, with `bucket` created.
async fn store(db: &TestDb, bucket: &bucket::Name) -> PgStorage {
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    let storage = PgStorage::connect_with_pool(pool, Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");
    storage
        .create_bucket(bucket, None, &Acl::default_private(None))
        .await
        .expect("the bucket");
    storage
}

/// One upload of `key`, as an in-progress S3 upload would leave it.
async fn upload(storage: &PgStorage, bucket: &bucket::Name, key: &str) -> String {
    storage
        .create_multipart_upload(
            bucket,
            &object::key(key).expect("a valid key"),
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .expect("the upload is created")
        .upload_id
}

/// The bucket's in-progress uploads, by identifier.
async fn listed(storage: &PgStorage, bucket: &bucket::Name) -> Vec<String> {
    storage
        .list_multipart_uploads(ListUploadsParams {
            bucket: bucket.clone(),
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 100,
        })
        .await
        .expect("the upload listing")
        .uploads
        .into_iter()
        .map(|upload| upload.upload_id)
        .collect()
}

/// Move one upload's initiation instant `age` into the past: the stage reads the
/// stored column, and every fixture upload was created just now.
async fn backdate(db: &TestDb, bucket: &bucket::Name, upload_id: &str, age: Duration) {
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    query(
        "UPDATE uploads SET initiated_at = now() - make_interval(secs => $2) \
         WHERE bucket = $1 AND upload_id = $3",
    )
    .bind(bucket.as_ref())
    .bind(age.as_secs_f64())
    .bind(upload_id)
    .execute(&pool)
    .await
    .expect("the upload is backdated");
}

/// Move one part's write stamp `age` into the past: the stage reads that column
/// too, and every fixture part is written just now.
async fn backdate_part(db: &TestDb, bucket: &bucket::Name, upload_id: &str, age: Duration) {
    let pool = Pool::open(&db.dsn, 4).expect("a pool over the scratch database");
    query(
        "UPDATE parts SET modified_at = \
         (extract(EPOCH FROM now() - make_interval(secs => $2)) * 1000)::bigint \
         WHERE bucket = $1 AND upload_id = $3",
    )
    .bind(bucket.as_ref())
    .bind(age.as_secs_f64())
    .bind(upload_id)
    .execute(&pool)
    .await
    .expect("the part is backdated");
}

/// A one-rule lifecycle configuration carrying exactly
/// `AbortIncompleteMultipartUpload`, at a threshold the server's timeout would
/// beat — the shape the filesystem sweeper's own LC21 leg stores.
fn abort_rule() -> lifecycle::Config {
    lifecycle::Config {
        rules: vec![lifecycle::Rule {
            id: None,
            status: lifecycle::Status::Enabled,
            filter: lifecycle::Filter::default(),
            expiration: None,
            noncurrent_version_expiration: None,
            abort_incomplete_multipart_upload: Some(lifecycle::AbortIncompleteMultipartUpload {
                days_after_initiation: 7,
            }),
        }],
    }
}

/// The stage's handle over `storage`, on the leg's own timeout.
fn cleanup(storage: &PgStorage) -> PgCleanup {
    PgCleanup::new(storage.store().clone(), CleanupOptions::default()).with_multipart_ttl(Some(TTL))
}

/// The same handle for a run that builds no lifecycle pass (`[s3] lifecycle = false`).
fn cleanup_without_the_pass(storage: &PgStorage) -> PgCleanup {
    cleanup(storage).with_lifecycle_pass(false)
}

/// An upload initiated before the timeout is aborted; one initiated inside it
/// survives — the age boundary the stage exists for.
#[tokio::test]
async fn an_upload_past_the_timeout_is_aborted_and_a_young_one_survives() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let data = bucket_name("sweep-leg");
    let storage = store(&db, &data).await;
    let stale = upload(&storage, &data, "stale.bin").await;
    let young = upload(&storage, &data, "young.bin").await;
    backdate(&db, &data, &stale, Duration::from_secs(2 * 3600)).await;

    let aborted = cleanup(&storage)
        .abort_stale_uploads()
        .await
        .expect("the stage runs");

    assert_eq!(aborted, 1, "only the upload past the timeout is aborted");
    assert_eq!(
        listed(&storage, &data).await,
        vec![young],
        "the one inside the timeout survives"
    );
    db.drop_db().await;
}

/// LC21: a bucket whose stored configuration carries
/// `AbortIncompleteMultipartUpload` owns the abandoned-upload action, so the
/// server's timeout does not apply to its uploads — however old they are.
#[tokio::test]
async fn a_bucket_with_an_abort_rule_is_left_alone() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let data = bucket_name("governed-leg");
    let storage = store(&db, &data).await;
    let stale = upload(&storage, &data, "stale.bin").await;
    backdate(&db, &data, &stale, Duration::from_secs(2 * 3600)).await;
    BucketMeta::set_lifecycle(storage.store(), &data, &abort_rule())
        .await
        .expect("the bucket's rules are stored");

    let aborted = cleanup(&storage)
        .abort_stale_uploads()
        .await
        .expect("the stage runs");

    assert_eq!(aborted, 0, "the bucket's own rule owns the abort for it");
    assert_eq!(
        listed(&storage, &data).await,
        vec![stale],
        "the upload survives the server's timeout"
    );
    db.drop_db().await;
}

/// LC23: with the capability off nothing is governed, so the same bucket's upload is
/// aborted by the server's timeout — turning the pass off must not stop retention.
#[tokio::test]
async fn a_bucket_with_an_abort_rule_is_swept_when_the_pass_is_off() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let data = bucket_name("ungoverned-leg");
    let storage = store(&db, &data).await;
    let stale = upload(&storage, &data, "stale.bin").await;
    backdate(&db, &data, &stale, Duration::from_secs(2 * 3600)).await;
    BucketMeta::set_lifecycle(storage.store(), &data, &abort_rule())
        .await
        .expect("the bucket's rules are stored");

    let aborted = cleanup_without_the_pass(&storage)
        .abort_stale_uploads()
        .await
        .expect("the stage runs");

    assert_eq!(
        aborted, 1,
        "no pass, so no rule takes the abort away from the timeout"
    );
    assert!(
        listed(&storage, &data).await.is_empty(),
        "the idle upload is aborted by the server's timeout"
    );
    db.drop_db().await;
}

/// The stage judges an upload by its **latest activity**, not by its
/// initiation: an upload begun long ago whose latest part landed just now is
/// being written, so it survives — the filesystem sweeper's
/// `max(initiated_at, latest part mtime)` rule, which pg has to answer the same
/// way or one timeout cuts off an upload on one backend and spares it on the
/// other.
#[tokio::test]
async fn an_upload_still_receiving_parts_survives_a_long_ago_initiation() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let data = bucket_name("live-leg");
    let storage = store(&db, &data).await;
    let live_key = object::key("live.bin").expect("a valid key");
    let live = upload(&storage, &data, "live.bin").await;
    let idle_key = object::key("idle.bin").expect("a valid key");
    let idle = upload(&storage, &data, "idle.bin").await;
    for (key, upload_id) in [(&live_key, &live), (&idle_key, &idle)] {
        storage
            .upload_part(&data, key, upload_id, part_number(1), body("part"), None)
            .await
            .expect("the part is written just now");
        backdate(&db, &data, upload_id, Duration::from_secs(2 * 3600)).await;
    }
    // Both uploads now have an initiation two hours old and a part written just
    // now; backdating only `idle`'s part leaves it the one whose latest activity
    // is its initiation, so it is the one the timeout may reach.
    backdate_part(&db, &data, &idle, Duration::from_secs(2 * 3600)).await;

    let aborted = cleanup(&storage)
        .abort_stale_uploads()
        .await
        .expect("the stage runs");

    assert_eq!(
        aborted, 1,
        "only the upload whose latest part is also past the timeout is aborted"
    );
    assert_eq!(
        listed(&storage, &data).await,
        vec![live],
        "the upload a client is still writing survives"
    );
    db.drop_db().await;
}
