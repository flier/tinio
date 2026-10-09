//! The database-backed tests' shared fixtures.
//!
//! `tests::db` owns the scratch database and the dsn; this module owns the
//! domain values a leg builds on top of them. The object and the upload plane's
//! legs start from the same handful, so they live here rather than in either
//! module's copy.

use crate::{
    _core::{
        acl::OwnerId,
        bucket,
        meta::{BucketMeta, bucket::Row as BucketRow},
        object,
    },
    PgStore,
    tests::db::{TestDb, base_dsn},
    timestamp::unix_seconds,
};

/// The empty body's MD5: the etag literal the legs share.
pub const ETAG_A: &str = "d41d8cd98f00b204e9800998ecf8427e";

/// A scratch database and a writable store over it, or `None` when
/// `TINIO_PG_DSN` is unset, so the leg skips rather than fails.
pub async fn scratch() -> Option<(TestDb, PgStore)> {
    if base_dsn().is_none() {
        eprintln!("skipping: TINIO_PG_DSN is not set");
        return None;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let store = db.store().await.expect("a writable store");
    Some((db, store))
}

/// A validated bucket name.
pub fn name(value: &str) -> bucket::Name {
    bucket::name(value).expect("a valid bucket name")
}

/// A validated object key.
pub fn key(value: &str) -> object::Key {
    object::key(value).expect("a valid object key")
}

/// A bucket row for `bucket`: the row every guarded writer asserts before it
/// lands, seeded at one second past the epoch so the legs share one creation
/// time.
pub async fn bucket_row(store: &PgStore, bucket: &bucket::Name) {
    BucketMeta::create(store, bucket, &BucketRow::at(unix_seconds(1)))
        .await
        .expect("a bucket row");
}

/// A validated object tag set under the object cap.
pub fn tags(pairs: &[(&str, &str)]) -> object::Tags {
    object::Tags::from_pairs_limited(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        object::OBJECT_TAGS_MAX,
    )
    .expect("a valid object tag set")
}

/// A validated bucket tag set under the bucket cap, which is the object cap's
/// own rule with the bucket's bound.
pub fn bucket_tags(pairs: &[(&str, &str)]) -> object::Tags {
    object::Tags::from_pairs_limited(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        object::BUCKET_TAGS_MAX,
    )
    .expect("a valid bucket tag set")
}

/// The canonical owner id the legs share.
pub fn owner() -> OwnerId {
    OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        .expect("a valid owner id")
}
