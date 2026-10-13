//! A bucket whose only contents are delete markers is not empty, and a refused
//! delete does not drop the version history.

#![cfg(feature = "versioning")]
#![expect(
    clippy::absolute_paths,
    reason = "test support: a test proves the public paths"
)]

mod support;

use support::{require_dsn, scratch_config};
use tinio_core::{
    acl::Acl,
    bucket,
    meta::VersionMeta,
    object,
    storage::{self, BucketOps, ObjectOps, VersionOps},
    version::State,
};
use tinio_pg::{Access, PgStorage};
use tinio_util::testing::body;

/// Delete markers have no live bytes, so the content-plane probe calls the
/// bucket empty. The delete must still refuse, and the version row the marker
/// superseded must still be there afterwards.
#[tokio::test]
async fn delete_bucket_with_only_delete_markers_keeps_the_version_history() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket::name("marker-bucket").expect("a valid bucket name");
    let key = object::key("marked.txt").expect("a valid key");
    let acl = Acl::default_private(None);
    storage
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the bucket");
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning enabled");
    let staged = storage
        .stage_body(&bucket, &key, body("seed content"), None, None)
        .await
        .expect("the object is staged");
    let committed = storage
        .commit_object(&bucket, &key, staged, object::Tags::empty(), None, &acl)
        .await
        .expect("the versioned commit");
    let superseded = committed
        .version_id
        .clone()
        .expect("a versioned commit mints an identity");
    storage
        .delete_object(&bucket, &key)
        .await
        .expect("the versioned delete");

    let err = storage
        .delete_bucket(&bucket)
        .await
        .expect_err("a delete marker keeps the bucket");
    assert!(
        matches!(storage::Error::from(err), storage::Error::NotEmpty(_)),
        "the refusal is NotEmpty"
    );

    let rows = VersionMeta::list_versions(storage.store(), &bucket, &key)
        .await
        .expect("the key's version rows");
    assert!(
        rows.iter().any(|row| row.id.as_ref() == Some(&superseded)),
        "a refused delete leaves the superseded version"
    );
    assert_eq!(
        VersionMeta::version_seq_of(storage.store(), &bucket, &key, Some(&superseded))
            .await
            .expect("the index read"),
        Some(1),
        "a refused delete leaves the version index entry"
    );
    assert!(
        VersionMeta::current_slot(storage.store(), &bucket, &key)
            .await
            .expect("the slot read")
            .is_some(),
        "a refused delete leaves the key's slot"
    );

    db.drop_db().await;
}

/// A bucket's versioning state does not outlive the bucket: the teardown retires
/// the `bucket_versioning` row with the rest of the namespace, so a re-created
/// bucket starts `Unversioned` rather than inheriting the old state.
#[tokio::test]
async fn a_recreated_bucket_is_unversioned() {
    if !require_dsn() {
        return;
    }
    let db = tinio_pg::_store_pg::test_support::TestDb::create()
        .await
        .expect("a scratch database");
    let storage = PgStorage::connect(&scratch_config(&db), Access::ReadWrite)
        .await
        .expect("a writable store over the scratch database");

    let bucket = bucket::name("recreated-bucket").expect("a valid bucket name");
    let acl = Acl::default_private(None);
    storage
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the bucket");
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .expect("versioning enabled");
    assert_eq!(
        storage
            .bucket_versioning(&bucket)
            .await
            .expect("the state read"),
        State::Enabled
    );

    storage
        .delete_bucket(&bucket)
        .await
        .expect("an empty bucket deletes");

    storage
        .create_bucket(&bucket, None, &acl)
        .await
        .expect("the bucket is re-created");
    assert_eq!(
        storage
            .bucket_versioning(&bucket)
            .await
            .expect("the state read"),
        State::Unversioned,
        "the versioning row left with the bucket"
    );

    db.drop_db().await;
}
