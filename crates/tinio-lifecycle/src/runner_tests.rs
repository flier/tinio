use std::{
    io,
    time::{Duration, SystemTime},
};

use futures::stream;
use tinio_util::testing::{
    metrics::{protected_deletions, test_lock},
    spans,
};
use tokio::{runtime, sync::watch, time::sleep};
use tracing::{Level, subscriber::with_default};
use tracing_subscriber::prelude::*;

use super::*;
use crate::{
    _core::{
        BucketOps, MultipartOps, ObjectOps, acl::Acl, object, object_lock, storage::VersionOps,
        version::State,
    },
    _mem::MemoryStorage,
    clock::FixedClock,
    metrics::action_count,
};

/// A noncurrent rule by version count rather than age: deterministic
/// regardless of the wall clock (a `Days` rule counts to the next midnight).
fn noncurrent_budget(keep: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        noncurrent_version_expiration: Some(lifecycle::NoncurrentVersionExpiration {
            noncurrent_days: None,
            newer_noncurrent_versions: Some(keep),
        }),
        // `NewerNoncurrentVersions` requires its own filter.
        filter: lifecycle::Filter {
            prefix: Some("a".into()),
            ..lifecycle::Filter::default()
        },
        ..rule()
    }
}

/// A lock-enabled versioned bucket holding three versions of one key: the two
/// older ones are history, the newest is current. Answers the oldest version's
/// id.
async fn locked_history(
    mode: object_lock::Mode,
) -> (Arc<MemoryStorage>, bucket::Name, object::Key, version::Id) {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    for _ in 0..3 {
        storage
            .put_object(&bucket, &key, Box::pin(stream::empty()))
            .await
            .unwrap();
    }
    let entries = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries;
    assert_eq!(entries.len(), 3, "three versions of one key");
    let old = entries[2]
        .id
        .clone()
        .expect("a versioned write mints an id");
    storage
        .set_retention(
            &bucket,
            &key,
            &version::Address::Id(old.clone()),
            Some(&object_lock::Retention {
                mode,
                retain_until: SystemTime::now() + Duration::from_secs(86_400),
            }),
            object_lock::Bypass::None,
            SystemTime::now(),
        )
        .await
        .unwrap();
    (storage, bucket, key, old)
}

/// A clock a minute on, so a zero-day noncurrent threshold is already due.
fn due_runner(storage: &Arc<MemoryStorage>) -> Runner<MemoryStorage> {
    let (_stop, stopped) = watch::channel(false);
    Runner::new(
        Arc::clone(storage),
        Options {
            delay: Duration::ZERO,
            ..Options::default()
        },
        Arc::new(FixedClock(
            SystemTime::now() + Duration::from_secs(100 * 86_400),
        )),
        stopped,
    )
}

/// OL19: a `Locked` noncurrent expiration is a counted skip, not an error. The
/// version stays, nothing is counted as expired or failed, and the metric
/// records the lifecycle operation with the refusal reason.
#[tokio::test]
async fn a_locked_noncurrent_version_is_a_counted_skip() {
    let (storage, bucket, _key, _old) = locked_history(object_lock::Mode::Governance).await;
    let before = protected_deletions("lifecycle", "governance");
    let runner = due_runner(&storage);
    let seeded = runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: noncurrent_budget(1),
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(seeded.buckets_with_rules, 1, "the rule was stored");
    assert_eq!(summary.protected_versions, 1, "the skip is counted");
    assert_eq!(summary.expired_versions, 0, "nothing was removed");
    assert_eq!(summary.errors, 0, "a protected version is not an error");
    assert_eq!(
        storage
            .list_object_versions(
                &bucket,
                ListParams {
                    max_keys: PAGE,
                    ..ListParams::default()
                },
            )
            .await
            .unwrap()
            .entries
            .len(),
        3,
        "the protected history row survives"
    );
    assert_eq!(
        protected_deletions("lifecycle", "governance") - before,
        1.0,
        "the refusal is recorded under operation=lifecycle, reason=governance"
    );
}

/// OL19: current-version expiration is not lock-protected — a DELETE without a
/// version id writes a marker even on a lock-enabled bucket.
#[tokio::test]
async fn the_current_version_still_expires_to_a_marker_on_a_locked_bucket() {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();

    let runner = due_runner(&storage);
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: expires(1),
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1, "the current version expires");
    assert_eq!(summary.protected_versions, 0, "no lock refuses a marker");
    assert_eq!(
        storage.current_version(&bucket, &key).await.unwrap(),
        CurrentVersion::Marker,
        "a versioned bucket's expiration writes a marker"
    );
}

/// OL19: `ExpiredObjectDeleteMarker` removes a marker (not protected), even on
/// a lock-enabled bucket.
#[tokio::test]
async fn an_expired_object_delete_marker_is_removed_on_a_locked_bucket() {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let bucket = bucket::name("locked").unwrap();
    let key = object::key("a.txt").unwrap();
    storage
        .create_bucket(&bucket, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&bucket, State::Enabled)
        .await
        .unwrap();
    storage
        .put_bucket_object_lock(
            &bucket,
            &object_lock::Config {
                enabled: true,
                rule: None,
            },
        )
        .await
        .unwrap();
    storage
        .put_object(&bucket, &key, Box::pin(stream::empty()))
        .await
        .unwrap();
    let content = storage
        .list_object_versions(
            &bucket,
            ListParams {
                max_keys: PAGE,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries[0]
        .id
        .clone();
    storage.delete_object(&bucket, &key).await.unwrap();
    storage
        .delete_object_version(&bucket, &key, content.as_ref(), object_lock::Bypass::None)
        .await
        .unwrap();

    let runner = due_runner(&storage);
    runner
        .seed(&[Seed {
            bucket: bucket.clone(),
            rule: lifecycle::Rule {
                expiration: Some(lifecycle::Expiration {
                    days: None,
                    date: None,
                    expired_object_delete_marker: Some(true),
                }),
                ..rule()
            },
        }])
        .await;
    let summary = runner.pass().await;

    assert_eq!(summary.expired_versions, 1, "the marker is removed");
    assert_eq!(summary.protected_versions, 0, "a marker is never protected");
    assert!(
        storage
            .list_object_versions(
                &bucket,
                ListParams {
                    max_keys: PAGE,
                    ..ListParams::default()
                },
            )
            .await
            .unwrap()
            .entries
            .is_empty(),
        "the key's only version (the marker) is gone"
    );
}

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
    let _guard = test_lock::window();
    let before = action_count(Kind::Object, "error");
    let mut summary = Summary::default();
    for gone in [
        storage::Error::NoSuchKey(object::key("a.txt").unwrap()),
        storage::Error::NoSuchVersion("v1".into()),
        storage::Error::NoSuchUpload("u1".into()),
        storage::Error::NoSuchBucket(bucket::name("data").unwrap()),
    ] {
        assert!(
            counted(Err::<(), _>(gone), &mut summary, Kind::Object),
            "an item already gone is the outcome the action wanted"
        );
    }
    assert_eq!(summary.errors, 0);

    let broken: storage::Error = storage::Error::Io(io::Error::other("boom"));
    assert!(
        !counted(Err::<(), _>(broken), &mut summary, Kind::Object),
        "a real failure is not the action's outcome"
    );
    assert_eq!(summary.errors, 1);
    assert_eq!(
        action_count(Kind::Object, "error") - before,
        1.0,
        "a failed action records one outcome=\"error\" sample"
    );
}

/// The read seam's one classification (R41): a bucket that vanished
/// mid-pass is gone, not broken, so it costs no `errors`; every other read
/// failure does.
#[test]
fn a_vanished_bucket_is_gone_not_broken() {
    let _guard = test_lock::window();
    let before = action_count(Kind::Object, "error");
    let bucket = bucket::name("data").unwrap();
    let mut summary = Summary::default();
    read_failed(
        storage::Error::NoSuchBucket(bucket.clone()),
        &bucket,
        &mut summary,
        "list a bucket's objects",
        Kind::Object,
    );
    assert_eq!(summary.errors, 0);
    read_failed(
        storage::Error::Io(io::Error::other("boom")),
        &bucket,
        &mut summary,
        "list a bucket's objects",
        Kind::Object,
    );
    assert_eq!(summary.errors, 1);
    assert_eq!(
        action_count(Kind::Object, "error") - before,
        1.0,
        "a failed action records one outcome=\"error\" sample"
    );
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
        .delete_object_version(&bucket, &key, hidden.as_ref(), object_lock::Bypass::None)
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

/// An enabled rule on every object.
fn rule() -> lifecycle::Rule {
    lifecycle::Rule {
        id: None,
        status: lifecycle::Status::Enabled,
        filter: lifecycle::Filter::default(),
        expiration: None,
        noncurrent_version_expiration: None,
        abort_incomplete_multipart_upload: None,
    }
}

/// Expire the current version `days` after it was created.
fn expires(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        expiration: Some(lifecycle::Expiration {
            days: Some(days),
            date: None,
            expired_object_delete_marker: None,
        }),
        ..rule()
    }
}

/// Expire a noncurrent version `days` after it became noncurrent.
fn noncurrent(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        noncurrent_version_expiration: Some(lifecycle::NoncurrentVersionExpiration {
            noncurrent_days: Some(days),
            newer_noncurrent_versions: None,
        }),
        ..rule()
    }
}

/// Abort an incomplete upload `days` after it was initiated.
fn aborts(days: i32) -> lifecycle::Rule {
    lifecycle::Rule {
        abort_incomplete_multipart_upload: Some(lifecycle::AbortIncompleteMultipartUpload {
            days_after_initiation: days,
        }),
        ..rule()
    }
}

/// The pass is the T0 unit: one `info` span, one `debug` child per action,
/// the chain walk under its version action, and the summary event inside it.
#[test]
fn a_pass_spans_its_cycle_and_its_actions() {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let runner = runtime.block_on(async {
        let storage = Arc::new(MemoryStorage::new().unwrap());
        let data = bucket::name("data").unwrap();
        let archive = bucket::name("archive").unwrap();
        for name in [&data, &archive] {
            storage
                .create_bucket(name, None, &Acl::default_private(None))
                .await
                .unwrap();
        }
        storage
            .set_bucket_versioning(&archive, State::Enabled)
            .await
            .unwrap();
        storage
            .put_object(
                &data,
                &object::key("a.txt").unwrap(),
                Box::pin(stream::empty()),
            )
            .await
            .unwrap();
        let logs = object::key("logs/b.txt").unwrap();
        for _ in 0..2 {
            storage
                .put_object(&archive, &logs, Box::pin(stream::empty()))
                .await
                .unwrap();
        }
        storage
            .create_multipart_upload(
                &archive,
                &object::key("big.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap();
        let (_stop, stopped) = watch::channel(false);
        let runner = Runner::new(
            Arc::clone(&storage),
            Options {
                delay: Duration::ZERO,
                ..Options::default()
            },
            Arc::new(FixedClock(
                SystemTime::now() + Duration::from_secs(100 * 86_400),
            )),
            stopped,
        );
        // The seed merges by rule id: two id-less rules would replace each
        // other, so the archive's two actions need their own ids.
        let mut versioned = noncurrent(1);
        versioned.id = Some("noncurrent".into());
        let mut uploads = aborts(1);
        uploads.id = Some("abort".into());
        runner
            .seed(&[
                Seed {
                    bucket: data,
                    rule: expires(1),
                },
                Seed {
                    bucket: archive.clone(),
                    rule: versioned,
                },
                Seed {
                    bucket: archive,
                    rule: uploads,
                },
            ])
            .await;
        runner
    });

    let capture = spans::Capture::new();
    with_default(tracing_subscriber::registry().with(capture.clone()), || {
        runtime.block_on(async {
            let summary = runner.pass().await;
            assert_eq!(summary.buckets_with_rules, 2, "both buckets were swept");
        });
    });

    let recorded = capture.spans_on_this_thread();
    let spans = &recorded;
    let pass = spans
        .iter()
        .find(|span| span.name == "tinio.lifecycle.pass")
        .expect("the pass opens its info span");
    assert_eq!(pass.target, "tinio::lifecycle");
    assert_eq!(pass.level, Level::INFO);
    assert!(
        pass.parent.is_none(),
        "the pass is the root of its own tree"
    );
    let field = |name: &str| {
        pass.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    };
    // The whole summary rides on the span — every field the close line prints.
    for name in [
        "buckets_with_rules",
        "expired_objects",
        "expired_versions",
        "protected_versions",
        "aborted_uploads",
        "errors",
    ] {
        assert!(
            field(name).is_some(),
            "the summary's {name} is recorded: {:?}",
            pass.fields
        );
    }
    assert_eq!(
        field("buckets_with_rules"),
        Some("2"),
        "the summary rides on the span, so the close line prints it: {:?}",
        pass.fields
    );
    assert_eq!(
        field("errors"),
        Some("0"),
        "no action failed in this pass: {:?}",
        pass.fields
    );

    for action in ["expire_objects", "expire_versions", "abort_uploads"] {
        let span = spans
            .iter()
            .find(|span| span.name == action)
            .unwrap_or_else(|| panic!("the pass opened {action}: {spans:?}"));
        assert_eq!(span.target, "tinio::lifecycle");
        assert_eq!(span.level, Level::DEBUG);
        assert_eq!(span.parent, Some(pass.id), "{action} is a pass child");
    }
    let versions = spans
        .iter()
        .find(|span| span.name == "expire_versions")
        .expect("the version action ran");
    assert!(
        spans
            .iter()
            .any(|span| span.name == "expire_chain" && span.parent == Some(versions.id)),
        "the chain walk is a child of the version action: {spans:?}"
    );
}

/// The run loop's body is the unit, so each cycle opens its own `info` span
/// above that cycle's pass.
#[test]
fn a_run_cycle_owns_a_span_above_its_pass() {
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let capture = spans::Capture::new();
    let (stop, stopped) = watch::channel(false);
    let runner = Runner::new(
        Arc::new(MemoryStorage::new().unwrap()),
        Options {
            delay: Duration::ZERO,
            cycle: Duration::from_millis(10),
        },
        Arc::new(FixedClock(SystemTime::now())),
        stopped,
    );
    with_default(tracing_subscriber::registry().with(capture.clone()), || {
        runtime.block_on(async move {
            let run = tokio::spawn(async move { runner.run().await });
            sleep(Duration::from_millis(50)).await;
            stop.send(true).unwrap();
            run.await.unwrap();
        });
    });

    let recorded = capture.spans_on_this_thread();
    let spans = &recorded;
    let runs: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "tinio.lifecycle.run")
        .collect();
    assert!(!runs.is_empty(), "the loop opens one span per cycle");
    assert!(
        runs.iter()
            .all(|span| span.target == "tinio::lifecycle" && span.level == Level::INFO),
        "the cycle span is the lifecycle layer's info span: {runs:?}"
    );
    let pass = spans
        .iter()
        .find(|span| span.name == "tinio.lifecycle.pass")
        .expect("a cycle ran its pass");
    assert!(
        runs.iter().any(|run| pass.parent == Some(run.id)),
        "the pass is a child of the cycle that drove it: {spans:?}"
    );
}
