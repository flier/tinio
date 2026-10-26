//! The engine over `tinio-mem`, with a pinned clock: what each of the three
//! actions does to a real store, and what it must refuse to do.
//!
//! No test reads the wall clock for an expiry: the store stamps objects with
//! the real "now", so a clock set ahead of it is what makes a `Days` rule due,
//! and a clock at "now" is what keeps one out of the way.

// The fixtures are helpers rather than test functions, so clippy's in-tests
// keys do not reach them: each asserts its preconditions by panicking
// (docs/style.md §Test-code exceptions).
#![expect(
    clippy::unwrap_used,
    reason = "test support: a fixture asserts its preconditions by panicking"
)]

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::stream;
use tinio_core::{
    BodyStream, ListUploadsParams,
    acl::Acl,
    bucket::{self, Name},
    lifecycle::{
        self, AbortIncompleteMultipartUpload, Expiration, Filter, NoncurrentVersionExpiration,
        Rule, Status,
    },
    object, object_lock,
    storage::{BucketOps, MultipartOps, ObjectOps, VersionOps},
    version::{self, CurrentVersion, ListParams, State},
};
use tinio_lifecycle::{FixedClock, Options, Runner, Seed};
use tinio_mem::MemoryStorage;
use tinio_util::testing::part_number;
use tokio::sync::watch;

/// An empty body: these tests are about which rows the pass touches, never
/// what an object holds.
fn body() -> BodyStream {
    Box::pin(stream::empty())
}

/// A store holding one bucket in `state`.
async fn store(state: State) -> (Arc<MemoryStorage>, Name) {
    let storage = Arc::new(MemoryStorage::new().unwrap());
    let name = bucket::name("data").unwrap();
    storage
        .create_bucket(&name, None, &Acl::default_private(None))
        .await
        .unwrap();
    if state != State::Unversioned {
        storage.set_bucket_versioning(&name, state).await.unwrap();
    }
    (storage, name)
}

/// Put one empty object, so a key has a current version.
async fn put(storage: &MemoryStorage, name: &Name, key: &str) {
    storage
        .put_object(name, &object::key(key).unwrap(), body())
        .await
        .unwrap();
}

/// A clock `days` days after the real "now" the store stamped its objects
/// with, so a `Days`-based rule on them is due.
fn ahead(days: u64) -> Arc<FixedClock> {
    Arc::new(FixedClock(
        SystemTime::now() + Duration::from_secs(days * 86_400),
    ))
}

/// The wall clock, so nothing is due.
fn now() -> Arc<FixedClock> {
    Arc::new(FixedClock(SystemTime::now()))
}

/// A runner whose clock is `clock`, and the stop channel's sender — held so
/// the pass never sees a signal whose sender is gone.
fn engine(
    storage: &Arc<MemoryStorage>,
    clock: Arc<FixedClock>,
    options: Options,
) -> (Runner<MemoryStorage>, watch::Sender<bool>) {
    let (stop, stopped) = watch::channel(false);
    (
        Runner::new(Arc::clone(storage), options, clock, stopped),
        stop,
    )
}

/// The defaults with the pause between actions off, so a test never sleeps.
fn no_pause() -> Options {
    Options {
        delay: Duration::ZERO,
        ..Options::default()
    }
}

/// Every key's version chain, newest first within each key.
async fn versions(storage: &MemoryStorage, name: &Name) -> Vec<version::Entry> {
    storage
        .list_object_versions(
            name,
            ListParams {
                max_keys: 1000,
                ..ListParams::default()
            },
        )
        .await
        .unwrap()
        .entries
}

/// The rule everything else is a variation on: enabled, every object.
fn rule() -> Rule {
    Rule {
        id: None,
        status: Status::Enabled,
        filter: Filter::default(),
        expiration: None,
        transitions: Vec::new(),
        noncurrent_version_expiration: None,
        abort_incomplete_multipart_upload: None,
    }
}

/// Expire the current version `days` after it was created.
fn expires(days: i32) -> Rule {
    Rule {
        expiration: Some(Expiration {
            days: Some(days),
            date: None,
            expired_object_delete_marker: None,
        }),
        ..rule()
    }
}

/// Remove a delete marker that is its key's only version.
fn keeps_markers() -> Rule {
    Rule {
        expiration: Some(Expiration {
            days: None,
            date: None,
            expired_object_delete_marker: Some(true),
        }),
        ..rule()
    }
}

/// Expire noncurrent versions: `days` down the chain, past `keep` newer ones,
/// or both.
fn noncurrent(days: Option<i32>, keep: Option<i32>, prefix: &str) -> Rule {
    Rule {
        filter: Filter {
            prefix: Some(prefix.to_owned()),
            ..Filter::default()
        },
        noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
            noncurrent_days: days,
            newer_noncurrent_versions: keep,
        }),
        ..rule()
    }
}

/// Abort an incomplete upload `days` after it was initiated.
fn aborts(days: i32) -> Rule {
    Rule {
        abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
            days_after_initiation: days,
        }),
        ..rule()
    }
}

/// Store `rule` on `name` through the seed, which is the same write path the
/// configuration file uses.
async fn seeded(runner: &Runner<MemoryStorage>, name: &Name, rule: Rule) {
    let summary = runner
        .seed(&[Seed {
            bucket: name.clone(),
            rule,
        }])
        .await;
    assert_eq!(summary.buckets_with_rules, 1, "the seed reached the bucket");
    assert_eq!(summary.errors, 0, "the seed wrote the rule");
}

/// The seed upserts into the stored configuration. A merge that would pass
/// the 1000-rule cap is refused and the stored rules stay as they were.
#[tokio::test]
async fn a_seed_past_the_rule_cap_writes_nothing() {
    let (storage, name) = store(State::Unversioned).await;
    let rules = (0..1000)
        .map(|index| {
            let mut rule = expires(30);
            rule.id = Some(format!("r{index}"));
            rule
        })
        .collect();
    storage
        .put_bucket_lifecycle(&name, &lifecycle::Config { rules })
        .await
        .unwrap();
    let (runner, _stop) = engine(&storage, now(), no_pause());
    let mut extra = expires(1);
    extra.id = Some("extra".into());

    let summary = runner
        .seed(&[Seed {
            bucket: name.clone(),
            rule: extra,
        }])
        .await;

    assert_eq!(summary.buckets_with_rules, 0);
    assert_eq!(summary.errors, 1, "the merged configuration is refused");
    assert_eq!(
        storage
            .get_bucket_lifecycle(&name)
            .await
            .unwrap()
            .expect("the stored configuration is untouched")
            .rules
            .len(),
        1000
    );
}

/// A seeded rule is the stored rule: the seed's write is the same contract
/// write `GET ?lifecycle` reads, with no second copy of the file's state.
#[tokio::test]
async fn a_seeded_rule_is_visible_through_the_stored_configuration() {
    let (storage, name) = store(State::Unversioned).await;
    let (runner, _stop) = engine(&storage, now(), no_pause());
    let mut rule = expires(30);
    rule.id = Some("seeded".into());

    seeded(&runner, &name, rule.clone()).await;

    assert_eq!(
        storage.get_bucket_lifecycle(&name).await.unwrap(),
        Some(lifecycle::Config { rules: vec![rule] })
    );
}

/// LC13's one-way rule: the file upserts its own IDs and deletes nothing, so a
/// rule a client added — or one the operator removed from the file — stays
/// stored.
#[tokio::test]
async fn the_seed_replaces_its_id_and_leaves_every_other_rule_stored() {
    let (storage, name) = store(State::Unversioned).await;
    let (runner, _stop) = engine(&storage, now(), no_pause());
    let mut from_file = expires(30);
    from_file.id = Some("from-file".into());
    let mut from_client = expires(90);
    from_client.id = Some("from-client".into());
    storage
        .put_bucket_lifecycle(
            &name,
            &lifecycle::Config {
                rules: vec![from_file.clone(), from_client.clone()],
            },
        )
        .await
        .unwrap();

    // The file's next start carries the same ID with a new threshold; the
    // rule the file never mentions must survive untouched.
    let mut changed = expires(7);
    changed.id = Some("from-file".into());
    seeded(&runner, &name, changed.clone()).await;

    let stored = storage
        .get_bucket_lifecycle(&name)
        .await
        .unwrap()
        .expect("the configuration is stored");
    assert_eq!(stored.rules.len(), 2, "the seed deleted no rule");
    assert_eq!(stored.rules[0], changed, "the file's ID was replaced");
    assert_eq!(
        stored.rules[1], from_client,
        "a rule the file does not mention stays stored (LC13)"
    );
}

/// LC13: a rule naming a bucket that does not exist is counted and skipped,
/// never a startup failure, and it writes nothing.
#[tokio::test]
async fn a_seed_for_a_missing_bucket_is_skipped_not_fatal() {
    let (storage, _name) = store(State::Unversioned).await;
    let (runner, _stop) = engine(&storage, now(), no_pause());
    let missing = bucket::name("missing").unwrap();
    let mut rule = expires(30);
    rule.id = Some("gone".into());

    let summary = runner
        .seed(&[Seed {
            bucket: missing.clone(),
            rule,
        }])
        .await;

    assert_eq!(summary.buckets_with_rules, 0, "nothing was written");
    assert_eq!(summary.errors, 1, "the refusal is counted, not returned");
    assert!(
        storage.get_bucket_lifecycle(&missing).await.is_err(),
        "the missing bucket is still missing"
    );
}

#[tokio::test]
async fn an_expired_object_is_deleted() {
    let (storage, name) = store(State::Unversioned).await;
    put(&storage, &name, "a.txt").await;
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.buckets_with_rules, 1);
    assert_eq!(summary.expired_objects, 1);
    assert_eq!(summary.errors, 0);
    assert!(
        storage
            .head_object(&name, &object::key("a.txt").unwrap())
            .await
            .is_err(),
        "the object is gone from an unversioned bucket"
    );
}

#[tokio::test]
async fn a_versioned_object_gains_a_marker_not_a_deletion() {
    let (storage, name) = store(State::Enabled).await;
    let key = object::key("a.txt").unwrap();
    put(&storage, &name, "a.txt").await;
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1);
    assert_eq!(
        storage.current_version(&name, &key).await.unwrap(),
        CurrentVersion::Marker,
        "expiration on a versioned bucket is the API's delete: a marker"
    );
    let chain = versions(&storage, &name).await;
    assert_eq!(chain.len(), 2, "the content version survives the marker");
    assert!(chain[0].is_delete_marker);
    assert!(!chain[1].is_delete_marker);
}

#[tokio::test]
async fn a_due_key_already_marked_is_skipped() {
    let (storage, name) = store(State::Enabled).await;
    put(&storage, &name, "a.txt").await;
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;
    runner.pass().await;

    // The rule is still due, and the key is still governed: a pass that acted
    // on the marker would add one marker per cycle.
    let second = runner.pass().await;

    assert_eq!(second.expired_objects, 0);
    let chain = versions(&storage, &name).await;
    assert_eq!(chain.len(), 2, "one marker, one version: {chain:?}");
    assert_eq!(
        chain.iter().filter(|entry| entry.is_delete_marker).count(),
        1
    );
}

#[tokio::test]
async fn a_noncurrent_version_past_both_thresholds_is_deleted() {
    let (storage, name) = store(State::Enabled).await;
    for _ in 0..4 {
        put(&storage, &name, "logs/a.txt").await;
    }
    // Both thresholds: one day noncurrent, and two newer versions kept.
    let rule = noncurrent(Some(1), Some(2), "logs/");

    // A day count that has not passed keeps every version, however many the
    // keep count would allow.
    let (young, _stop) = engine(&storage, now(), no_pause());
    seeded(&young, &name, rule.clone()).await;
    assert_eq!(young.pass().await.expired_versions, 0);
    assert_eq!(versions(&storage, &name).await.len(), 4);

    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    let summary = runner.pass().await;

    assert_eq!(summary.expired_versions, 1, "only the oldest is past both");
    let chain = versions(&storage, &name).await;
    assert_eq!(chain.len(), 3);
    assert!(chain.iter().all(|entry| !entry.is_delete_marker));
}

#[tokio::test]
async fn two_rules_are_never_merged() {
    // The review's A/B pair: A allows 10 days and keeps 5, B allows 30 days and
    // keeps 2. A version 20 days old with three newer noncurrent versions is
    // past A's age but within A's keep count, and short of B's age, so no rule
    // authorizes it — while a merged `{10 days, keep 2}` would delete it.
    let (storage, name) = store(State::Enabled).await;
    for _ in 0..5 {
        put(&storage, &name, "logs/a.txt").await;
    }
    let (runner, _stop) = engine(&storage, ahead(20), no_pause());
    seeded(&runner, &name, noncurrent(Some(10), Some(5), "logs/")).await;
    seeded(&runner, &name, noncurrent(Some(30), Some(2), "logs/")).await;

    // A merged `{10 days, keep 2}` would delete the two oldest of the four
    // noncurrent versions; no rule allows it.
    assert_eq!(runner.pass().await.expired_versions, 0);
    assert_eq!(versions(&storage, &name).await.len(), 5);

    // Past B's age, B's own pair authorizes exactly the two past its keep count.
    let (later, _stop) = engine(&storage, ahead(31), no_pause());
    assert_eq!(later.pass().await.expired_versions, 2);
    assert_eq!(versions(&storage, &name).await.len(), 3);
}

#[tokio::test]
async fn the_count_only_form_keeps_the_count() {
    let (storage, name) = store(State::Enabled).await;
    for _ in 0..5 {
        put(&storage, &name, "logs/a.txt").await;
    }
    let (runner, _stop) = engine(&storage, now(), no_pause());
    // MinIO's documented extension: keep two noncurrent versions and no day
    // count, so a version past the count is due at once.
    seeded(&runner, &name, noncurrent(None, Some(2), "logs/")).await;

    let summary = runner.pass().await;

    assert_eq!(summary.expired_versions, 2);
    assert_eq!(
        versions(&storage, &name).await.len(),
        3,
        "the current version and the two newest noncurrent ones"
    );
}

/// A null version carries no tags (LC10): the version path spends no read on
/// it, so a tag-filtered rule never matches it and it is retained.
#[tokio::test]
async fn a_tag_filtered_rule_retains_the_null_version() {
    let (storage, name) = store(State::Suspended).await;
    let key = object::key("logs/a.txt").unwrap();
    put(&storage, &name, "logs/a.txt").await;
    let tags = object::Tags::from_pairs([("k".to_owned(), "v".to_owned())]).unwrap();
    storage
        .set_version_tags(&name, &key, None, tags)
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&name, State::Enabled)
        .await
        .unwrap();
    put(&storage, &name, "logs/a.txt").await;
    let tagged = Rule {
        filter: Filter {
            tag: Some(lifecycle::Tag {
                key: "k".into(),
                value: "v".into(),
            }),
            ..Filter::default()
        },
        noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
            noncurrent_days: Some(1),
            newer_noncurrent_versions: None,
        }),
        ..rule()
    };
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, tagged).await;

    assert_eq!(
        runner.pass().await.expired_versions,
        0,
        "the null version carries none of its own tags here"
    );
    assert!(
        versions(&storage, &name)
            .await
            .iter()
            .any(|entry| entry.id.is_none()),
        "the tagged null version is retained"
    );
}

#[tokio::test]
async fn a_tag_filtered_noncurrent_rule_expires_only_its_versions() {
    // The tag is a version's own property: the rule matches the versions
    // carrying it and retains every other version — including the oldest,
    // which the keep count alone would let go.
    let (storage, name) = store(State::Enabled).await;
    for _ in 0..5 {
        put(&storage, &name, "logs/a.txt").await;
    }
    let before = versions(&storage, &name).await;
    let tags = object::Tags::from_pairs([("k".to_owned(), "v".to_owned())]).unwrap();
    // The two middle noncurrent versions carry the tag; the oldest and the
    // newest noncurrent one do not.
    for entry in &before[2..4] {
        storage
            .set_version_tags(&name, &entry.key, entry.id.as_ref(), tags.clone())
            .await
            .unwrap();
    }
    let tagged = Rule {
        filter: Filter {
            tag: Some(lifecycle::Tag {
                key: "k".into(),
                value: "v".into(),
            }),
            ..Filter::default()
        },
        noncurrent_version_expiration: Some(NoncurrentVersionExpiration {
            noncurrent_days: None,
            newer_noncurrent_versions: Some(1),
        }),
        ..rule()
    };
    let (runner, _stop) = engine(&storage, now(), no_pause());
    seeded(&runner, &name, tagged).await;

    let summary = runner.pass().await;

    assert_eq!(
        summary.expired_versions, 2,
        "only the tagged noncurrent ones"
    );
    assert_eq!(summary.errors, 0);
    let after: Vec<_> = versions(&storage, &name)
        .await
        .iter()
        .map(|entry| entry.id.clone())
        .collect();
    assert_eq!(
        after,
        [&before[0], &before[1], &before[4]].map(|entry| entry.id.clone()),
        "the current version, the newest noncurrent one and the untagged oldest survive"
    );
}

#[tokio::test]
async fn a_stopping_seed_writes_nothing() {
    let (storage, name) = store(State::Unversioned).await;
    let (stop, stopped) = watch::channel(true);
    let runner = Runner::new(Arc::clone(&storage), no_pause(), now(), stopped);

    let summary = runner
        .seed(&[Seed {
            bucket: name.clone(),
            rule: expires(1),
        }])
        .await;

    assert_eq!(
        summary.buckets_with_rules, 0,
        "a stopping seed writes nothing"
    );
    assert_eq!(summary.errors, 0, "and reports no failure");
    assert!(
        storage.get_bucket_lifecycle(&name).await.unwrap().is_none(),
        "the bucket keeps no configuration"
    );
    assert!(*stop.borrow());
}

#[tokio::test]
async fn an_only_version_marker_is_removed() {
    let (storage, name) = store(State::Enabled).await;
    let key = object::key("a.txt").unwrap();
    put(&storage, &name, "a.txt").await;
    storage.delete_object(&name, &key).await.unwrap();
    let (runner, _stop) = engine(&storage, now(), no_pause());
    seeded(&runner, &name, keeps_markers()).await;

    // The marker hides a content version, so it is not the only version left:
    // `ExpiredObjectDeleteMarker` never removes it.
    assert_eq!(runner.pass().await.expired_versions, 0);
    assert_eq!(versions(&storage, &name).await.len(), 2);

    // Remove the version behind the marker: now the marker is all that is left.
    let hidden = versions(&storage, &name).await[1].id.clone();
    storage
        .delete_object_version(&name, &key, hidden.as_ref(), object_lock::Bypass::None)
        .await
        .unwrap();

    let summary = runner.pass().await;

    assert_eq!(summary.expired_versions, 1);
    assert!(
        versions(&storage, &name).await.is_empty(),
        "the marker was the key's only version, so it leaves permanently"
    );
}

/// `NoncurrentDays` rounds forward to the next midnight UTC, the same clock
/// as `Expiration.Days`. One raw day after the successor was written is still
/// short of that midnight, so the version stays.
#[tokio::test]
async fn noncurrent_days_wait_for_the_next_midnight() {
    let (storage, name) = store(State::Enabled).await;
    put(&storage, &name, "logs/a.txt").await;
    put(&storage, &name, "logs/a.txt").await;
    let (runner, _stop) = engine(&storage, ahead(1), no_pause());
    seeded(&runner, &name, noncurrent(Some(1), None, "logs/")).await;

    assert_eq!(
        runner.pass().await.expired_versions,
        0,
        "one raw day is not yet the midnight NoncurrentDays names"
    );
    assert_eq!(versions(&storage, &name).await.len(), 2);
}

/// `DaysAfterInitiation` uses that same midnight. An upload one raw day old
/// is not yet due under a one-day rule.
#[tokio::test]
async fn an_upload_one_raw_day_old_is_not_aborted() {
    let (storage, name) = store(State::Unversioned).await;
    let key = object::key("big.bin").unwrap();
    storage
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    let (runner, _stop) = engine(&storage, ahead(1), no_pause());
    seeded(&runner, &name, aborts(1)).await;

    assert_eq!(runner.pass().await.aborted_uploads, 0);
    assert_eq!(
        storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: name,
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 10,
            })
            .await
            .unwrap()
            .uploads
            .len(),
        1,
        "the upload stays until the midnight the rule names"
    );
}

/// An abort rule's size predicate matches the bytes already uploaded, not
/// zero. A part larger than `ObjectSizeLessThan` is kept.
#[tokio::test]
async fn an_upload_larger_than_the_size_bound_is_kept() {
    let (storage, name) = store(State::Unversioned).await;
    let key = object::key("big.bin").unwrap();
    let upload = storage
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    storage
        .upload_part(
            &name,
            &key,
            &upload.upload_id,
            part_number(1),
            Box::pin(stream::once(async { Ok(Bytes::from_static(b"abc")) })),
            None,
        )
        .await
        .unwrap();
    let rule = Rule {
        filter: Filter {
            size_less_than: Some(2),
            ..Filter::default()
        },
        ..aborts(1)
    };
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, rule).await;

    assert_eq!(
        runner.pass().await.aborted_uploads,
        0,
        "three uploaded bytes are not under a bound of 2"
    );
}

#[tokio::test]
async fn an_upload_past_the_clock_is_aborted() {
    let (storage, name) = store(State::Unversioned).await;
    let key = object::key("big.bin").unwrap();
    storage
        .create_multipart_upload(
            &name,
            &key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, aborts(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.aborted_uploads, 1);
    assert_eq!(summary.errors, 0);
    let uploads = storage
        .list_multipart_uploads(ListUploadsParams {
            bucket: name.clone(),
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .await
        .unwrap();
    assert!(uploads.uploads.is_empty(), "the upload is aborted");
}

#[tokio::test]
async fn a_stopping_pass_acts_on_nothing() {
    // The stop is checked per item, before the item: with the channel already
    // set, a due key is left alone instead of being deleted and noticed after.
    let (storage, name) = store(State::Unversioned).await;
    put(&storage, &name, "a.txt").await;
    // Seeded before the stop is signalled: a stopping seed writes nothing.
    let (seeder, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&seeder, &name, expires(1)).await;
    let (stop, stopped) = watch::channel(true);
    let runner = Runner::new(Arc::clone(&storage), no_pause(), ahead(100), stopped);

    let summary = runner.pass().await;

    assert_eq!(
        summary.expired_objects, 0,
        "a stopping pass touches nothing"
    );
    assert!(
        storage
            .head_object(&name, &object::key("a.txt").unwrap())
            .await
            .is_ok(),
        "the due key survives the stopping pass"
    );
    assert!(*stop.borrow());
}

#[tokio::test]
async fn a_suspended_bucket_writes_a_null_marker() {
    let (storage, name) = store(State::Enabled).await;
    put(&storage, &name, "a.txt").await;
    storage
        .set_bucket_versioning(&name, State::Suspended)
        .await
        .unwrap();
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1);
    let chain = versions(&storage, &name).await;
    assert_eq!(chain.len(), 2);
    assert!(chain[0].is_delete_marker);
    assert_eq!(
        chain[0].id, None,
        "a suspended bucket's marker takes the null identity"
    );
    assert!(chain[1].id.is_some(), "the content version keeps its own");
}

/// When the current version is the null version, expiration deletes it
/// permanently: a suspended bucket's `delete_object` semantics, not a marker.
#[tokio::test]
async fn a_null_current_version_is_deleted_permanently() {
    let (storage, name) = store(State::Suspended).await;
    put(&storage, &name, "a.txt").await;
    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.expired_objects, 1);
    let chain = versions(&storage, &name).await;
    assert_eq!(
        chain.len(),
        1,
        "the null content version is permanently deleted, not superseded into history"
    );
    assert!(chain[0].is_delete_marker);
    assert_eq!(
        chain[0].id, None,
        "the suspended delete writes a null marker"
    );
}

#[tokio::test]
async fn a_bucket_without_an_expiration_rule_is_never_listed() {
    let (storage, name) = store(State::Unversioned).await;
    put(&storage, &name, "a.txt").await;
    let archive = bucket::name("archive").unwrap();
    storage
        .create_bucket(&archive, None, &Acl::default_private(None))
        .await
        .unwrap();
    put(&storage, &archive, "b.txt").await;

    let (runner, _stop) = engine(&storage, ahead(100), no_pause());
    seeded(&runner, &name, expires(1)).await;
    seeded(&runner, &archive, aborts(1)).await;

    let summary = runner.pass().await;

    assert_eq!(summary.buckets_with_rules, 2);
    assert_eq!(summary.expired_objects, 1, "the expiring bucket is walked");
    assert_eq!(
        storage.object_listings(&name),
        1,
        "an expiring bucket is listed once"
    );
    assert_eq!(
        storage.object_listings(&archive),
        0,
        "an abort-only bucket pays no object walk"
    );
}
