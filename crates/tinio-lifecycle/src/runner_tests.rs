use std::{
    io,
    time::{Duration, SystemTime},
};

use futures::stream;
use tokio::sync::watch;

use super::*;
use crate::{
    _core::{BucketOps, ObjectOps, acl::Acl, object, storage::VersionOps, version::State},
    _mem::MemoryStorage,
    clock::FixedClock,
};

/// A versioned bucket holding one key.
async fn marked() -> (Arc<MemoryStorage>, bucket::Name, object::Key) {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("data").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    storage.delete_object(&bucket, &key).await.unwrap();
    (storage, bucket, key)
}

fn runner(storage: &Arc<MemoryStorage>) -> Runner<MemoryStorage> {
    let (_stop, stopped) = watch::channel(false);
    Runner::new(
        Arc::clone(storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    )
}

/// LC2's guard on its own: the object listing can name a due key whose
/// current slot is a marker (a store that lists its content, not its
/// rows), and the pass must not expire that key again.
#[tokio::test]
async fn a_marked_key_is_not_due() {
    let (storage, bucket, key) = marked().await;
    let runner = runner(&storage);
    assert!(
        !runner.due(&bucket, &key).await.unwrap(),
        "a key whose current slot is a delete marker has no current version to expire"
    );
    // The same key put back is due again: the guard reads the slot, not a
    // remembered key.
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    assert!(runner.due(&bucket, &key).await.unwrap());
}

/// The pass's accounting seam, pinned where it lives: a delete is
/// idempotent, so an item that vanished first is the outcome the action
/// wanted and counts as done; anything else is one error.
///
/// Pinned here rather than through an engine leg because no `tinio-mem`
/// path reaches the branch — its deletes answer `Ok` for a missing row,
/// which is exactly why the seam exists.
#[test]
fn a_key_that_vanished_is_counted_not_failed() {
    let mut summary = Summary::default();
    for gone in [
        storage::Error::NoSuchKey(object::key("a.txt").unwrap()),
        storage::Error::NoSuchVersion("v1".into()),
        storage::Error::NoSuchUpload("u1".into()),
        storage::Error::NoSuchBucket(bucket::name("data").unwrap()),
    ] {
        assert!(
            counted(Err::<(), _>(gone), &mut summary),
            "an item already gone is the outcome the action wanted"
        );
    }
    assert_eq!(summary.errors, 0);

    let broken: storage::Error = storage::Error::Io(io::Error::other("boom"));
    assert!(
        !counted(Err::<(), _>(broken), &mut summary),
        "a real failure is not the action's outcome"
    );
    assert_eq!(summary.errors, 1);
}

/// The read seam's one classification (R41): a bucket that vanished
/// mid-pass is gone, not broken, so it costs no `errors`; every other read
/// failure does.
#[test]
fn a_vanished_bucket_is_gone_not_broken() {
    let bucket = bucket::name("data").unwrap();
    let mut summary = Summary::default();
    read_failed(
        storage::Error::NoSuchBucket(bucket.clone()),
        &bucket,
        &mut summary,
        "list a bucket's objects",
    );
    assert_eq!(summary.errors, 0);
    read_failed(
        storage::Error::Io(io::Error::other("boom")),
        &bucket,
        &mut summary,
        "list a bucket's objects",
    );
    assert_eq!(summary.errors, 1);
}

/// A stop covers the chain the walk ends on as well as the entry the
/// listing loop last saw. A marker that is its key's only version is the
/// chain only the trailing call can reach: the walk's own per-entry check
/// guards a version row, and this chain has none.
#[tokio::test]
async fn a_stopping_chain_is_left_whole() {
    let (storage, bucket, key) = marked().await;
    // Remove the version behind the marker: the marker is now the key's
    // only version, which `ExpiredObjectDeleteMarker` would remove.
    let hidden = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries[1]
        .id
        .clone();
    storage
        .delete_object_version(&bucket, &key, hidden.as_ref())
        .await
        .unwrap();
    let (stop, stopped) = watch::channel(false);
    let runner = Runner::new(
        Arc::clone(&storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    );
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: lifecycle::Rule {
                id: None,
                status: lifecycle::Status::Enabled,
                filter: lifecycle::Filter::default(),
                expiration: Some(lifecycle::Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: Some(true),
                }),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: None,
            },
        }])
        .await;
    let config = storage
        .get_bucket_lifecycle(&bucket)
        .await
        .unwrap()
        .unwrap();
    let listing = || ListParams {
        max_keys: PAGE,
        ..ListParams::default()
    };
    let chain = storage
        .list_object_versions(&bucket, listing())
        .await
        .unwrap()
        .entries;
    stop.send(true).unwrap();

    let mut summary = Summary::default();
    let mut sweep = Sweep {
        bucket: &bucket,
        config: &config,
        now: SystemTime::now(),
        reads_tags: false,
        summary: &mut summary,
    };
    assert!(
        !runner.expire_chain(&mut sweep, &chain).await,
        "a chain reached after the stop does not carry on"
    );
    assert_eq!(summary.expired_versions, 0);
    assert_eq!(
        storage
            .list_object_versions(&bucket, listing())
            .await
            .unwrap()
            .entries
            .len(),
        1,
        "the marker is still there: nothing was acted on"
    );
}

#[test]
fn a_day_count_rounds_forward_to_midnight() {
    let noon = SystemTime::UNIX_EPOCH + Duration::from_secs(43_200);
    let due = lifecycle::midnight_utc(noon, 1);
    assert!(
        !due_on_midnight(noon, 1, due - Duration::from_secs(1)),
        "one raw day is not yet the next midnight"
    );
    assert!(due_on_midnight(noon, 1, due));
    assert!(
        due_on_midnight(noon, 0, noon),
        "a zero day count is due at the anchor"
    );
    assert!(
        !due_on_midnight(due, 1, noon),
        "a clock that stepped back expires nothing"
    );
}
