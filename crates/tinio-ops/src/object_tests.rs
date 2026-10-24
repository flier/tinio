use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tinio_core::{
    acl::{Grant, Grantee, OwnerId, Permission},
    content::StagedObject,
    meta::object::PartRow,
    storage::{Error as StorageError, collect_body},
    version::State,
};

use super::*;
use crate::{
    _core::meta::{BucketMeta, VersionMeta},
    _store_redb::{Driver, RedbContent, RedbMetadata},
    _version::Versions,
    BucketRow, MetadataFailure, VersionPlane,
    testutil::{
        Base, BlockMem, ContentCall, PublishSight, RecordingContent, RecordingHooks, TestMeta, body,
    },
};

/// A canonical owner id.
fn owner() -> OwnerId {
    OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
}

/// A second canonical principal (the grant a put replaces the set with).
fn other() -> OwnerId {
    OwnerId::new("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210").unwrap()
}

/// A tag set a write records.
fn tags() -> object::Tags {
    object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap()
}

/// A grant set a write replaces the object's with.
fn grants() -> AclGrants {
    vec![Grant {
        grantee: Grantee::Canonical(other()),
        permission: Permission::FullControl,
    }]
}

/// Read a served body whole.
async fn read(stream: BodyStream) -> Vec<u8> {
    collect_body(stream).await.unwrap().to_vec()
}

/// A bucket with a recorded row — the guarded policy's requirement.
async fn bucket(base: &Base, name: &str) -> bucket::Name {
    let name = bucket::name(name).unwrap();
    base.ops(BucketRow::Guaranteed)
        .create_bucket(&name, None, &Acl::default_private(None))
        .await
        .unwrap();
    name
}

/// Plant content at a key **without any row** — the cold start a read or a
/// listing must heal.
async fn plant(base: &Base, bucket: &bucket::Name, key: &object::Key, bytes: &[u8]) {
    let content_key = ContentKey::Object(key.clone());
    let mut staged = base.content.stage(bucket, &content_key).await.unwrap();
    staged.write(Bytes::copy_from_slice(bytes)).await.unwrap();
    base.content
        .publish(bucket, staged, &content_key)
        .await
        .unwrap();
}

/// A served body's bytes.
async fn served(result: GetObjectResult) -> Vec<u8> {
    read(result.body).await
}

#[tokio::test]
async fn the_stage_streams_the_body_and_fixes_its_etag_and_checksum() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);

    let staged = ops
        .stage_body(&name, &key, body(&[b"hel", b"lo"]), None, None)
        .await
        .unwrap();
    assert_eq!(staged.key(), &key);
    assert_eq!(staged.len(), 5, "the landed byte count");
    assert!(!staged.is_empty());
    assert_eq!(
        staged.etag(),
        &ETag::from_content(b"hello"),
        "the ETag is computed while the bytes stream"
    );
    assert_eq!(staged.checksum(), None, "the stage fixed no checksum");

    // Nothing is visible before the commit: the stage is not the publish.
    assert!(base.content.stat(&name, &key).await.unwrap().is_none());

    let info = ops
        .commit_object(
            &name,
            &key,
            staged,
            tags(),
            Some(&owner()),
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(info.etag, ETag::from_content(b"hello"));
    assert_eq!(info.size, 5);
    assert_eq!(info.tags, tags(), "the tags ride the commit");
    assert_eq!(info.owner.as_ref(), Some(&owner()));
    assert_eq!(
        ops.get_object(&name, &key, None).await.unwrap().info.etag,
        info.etag
    );
}

#[tokio::test]
async fn a_carried_checksum_wins_over_the_stages_streamed_digest() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("a.bin").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let carried = checksum::Recorded {
        part: checksum::Part {
            algorithm: checksum::Algorithm::Crc32,
            value: checksum::Value("NhCmhg==".into()),
        },
        kind: checksum::Type::Composite,
    };

    let staged = ops
        .stage_body(
            &name,
            &key,
            body(&[b"payload"]),
            None,
            Some(carried.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        staged.checksum().cloned(),
        Some(carried.clone()),
        "the carried value is the stage's checksum"
    );
    let info = ops
        .commit_object(
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(info.checksum, Some(carried));
}

#[tokio::test]
async fn the_commit_writes_the_row_under_the_receivers_bucket_row_policy() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("a.txt").unwrap();

    // The row-backed store: the guarded form. The core trait's default
    // `put_object` is exactly this composition — the stage beside the commit
    // with empty tags, no owner and the default private ACL — and both
    // backends take that default.
    let guarded = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let staged = guarded
        .stage_body(&name, &key, body(&[b"one"]), None, None)
        .await
        .unwrap();
    guarded
        .commit_object(
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        base.counts().put_guarded,
        1,
        "the guarded form is the write"
    );
    assert_eq!(base.counts().puts, 0, "and the plain upsert is not");
    assert!(base.meta.get(&name, &key).await.unwrap().is_some());

    // A store whose buckets outlive their rows: the plain upsert.
    let lazy = base.objects(BucketRow::Lazy, MetadataFailure::Fail);
    let key = object::key("b.txt").unwrap();
    let staged = lazy
        .stage_body(&name, &key, body(&[b"two"]), None, None)
        .await
        .unwrap();
    lazy.commit_object(
        &name,
        &key,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();
    assert_eq!(base.counts().puts, 1, "the lazy store writes plainly");
    assert_eq!(base.counts().put_guarded, 1, "and guarded nothing new");
}

#[tokio::test]
async fn a_failed_row_write_is_read_through_the_metadata_failure_policy() {
    // A bucket with no row: the guarded write refuses (`NoSuchBucket`) —
    // the content is published, and the policy decides what that means.
    let base = Base::new();
    let name = bucket::name("ghost").unwrap();
    let key = object::key("a.txt").unwrap();

    // The `Fail` arm: the refused row write is the operation's answer, and
    // the engine takes its own publish back — the bytes this call published
    // are removed, and the caller sees the row write's refusal.
    let (failing, recording) = base.recording_objects(BucketRow::Guaranteed);
    let staged = failing
        .stage_body(&name, &key, body(&[b"lost"]), None, None)
        .await
        .unwrap();
    let err = failing
        .commit_object(
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
    assert!(
        recording.saw(|call| matches!(call, ContentCall::RemoveIfTagged(_, _, true))),
        "the compensation took back exactly this call's own publish: {:?}",
        recording.calls()
    );
    assert!(
        base.content.stat(&name, &key).await.unwrap().is_none(),
        "the refused write left no bytes behind"
    );

    // The warn policy: the object is committed, the row is not, and the
    // next read heals it.
    let warning = base.objects(BucketRow::Guaranteed, MetadataFailure::Warn);
    let key = object::key("kept.txt").unwrap();
    let staged = warning
        .stage_body(&name, &key, body(&[b"kept"]), None, None)
        .await
        .unwrap();
    let put = warning
        .commit_object(
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(put.etag, ETag::from_content(b"kept"));
    assert!(base.meta.get(&name, &key).await.unwrap().is_none());
}

#[tokio::test]
async fn the_fail_policys_compensation_leaves_a_newer_writers_bytes_alone() {
    // A newer writer publishes **byte-identical** content between this
    // call's publish and its refused row write. Only the recorded publish
    // identity separates the two publishes, so the compensation must match
    // the identity and not the bytes.
    let base = Base::new();
    let name = bucket::name("ghost").unwrap();
    let key = object::key("raced.txt").unwrap();
    let content =
        RecordingContent::new(Arc::clone(&base.content), Arc::new(base.counted().clone()))
            .with_follower(b"mine");
    let (ops, recording) = base.recording_objects_with(BucketRow::Guaranteed, content);

    let staged = ops
        .stage_body(&name, &key, body(&[b"mine"]), None, None)
        .await
        .unwrap();
    let err = ops
        .commit_object(
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
    assert!(
        recording.saw(|call| matches!(call, ContentCall::RemoveIfTagged(_, _, false))),
        "the compensation matched the identity and left the newer publish: {:?}",
        recording.calls()
    );
    assert_eq!(
        base.content
            .stat(&name, &key)
            .await
            .unwrap()
            .map(|entry| entry.len),
        Some(4),
        "the newer writer's byte-identical content stays"
    );
}

#[tokio::test]
async fn a_read_serves_a_rowless_object_and_heals_the_row() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("dropped.txt").unwrap();
    plant(&base, &name, &key, b"dropped").await;
    assert!(base.meta.get(&name, &key).await.unwrap().is_none());

    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let head = ops.head_object(&name, &key).await.unwrap();
    assert_eq!(head.etag, ETag::from_content(b"dropped"));
    assert_eq!(head.size, 7, "the served size is the content's");
    assert_eq!(base.counts().put_preserving, 1, "one healing write");
    let row = base.meta.get(&name, &key).await.unwrap().expect("the row");
    assert_eq!(row.etag, ETag::from_content(b"dropped"));
    assert_eq!(row.size, 7);

    // The second read is a hit: the row now matches, so nothing is written.
    ops.head_object(&name, &key).await.unwrap();
    assert_eq!(base.counts().put_preserving, 1, "a fresh row heals nothing");

    // The get path heals too, and serves the bytes.
    let key = object::key("dropped2.txt").unwrap();
    plant(&base, &name, &key, b"more").await;
    let got = ops.get_object(&name, &key, None).await.unwrap();
    assert_eq!(got.info.etag, ETag::from_content(b"more"));
    assert_eq!(served(got).await, b"more");
    assert_eq!(base.counts().put_preserving, 2);
}

#[tokio::test]
async fn a_lazy_store_heals_without_requiring_a_bucket_row() {
    // A bucket that was never sighted: no row anywhere, content in the
    // namespace. A read must serve it — the heal records the bucket row it
    // needs lazily, then the preserving write that keeps the key's parts.
    let base = Base::new();
    let name = bucket::name("handmade").unwrap();
    let key = object::key("dropped.txt").unwrap();
    plant(&base, &name, &key, b"dropped").await;

    let ops = base.objects(BucketRow::Lazy, MetadataFailure::Fail);
    let head = ops.head_object(&name, &key).await.unwrap();
    assert_eq!(head.etag, ETag::from_content(b"dropped"));
    assert_eq!(
        base.counts().put_preserving,
        1,
        "the preserving write is the heal"
    );
    assert_eq!(base.counts().puts, 0, "and nothing is written unguarded");
    assert!(
        base.meta.created_at(&name).await.unwrap().is_some(),
        "the lazy heal recorded the bucket row its guarded write needs"
    );
    assert!(base.meta.get(&name, &key).await.unwrap().is_some());
}

#[tokio::test]
async fn a_lazy_heal_keeps_the_keys_retained_part_list() {
    // The heal of a multipart-completed object must not take its retained
    // parts: the preserving write keeps them, while the unguarded upsert
    // would clear them (it is an overwrite by contract).
    let base = Base::new();
    let name = bucket::name("handmade").unwrap();
    let key = object::key("parted.bin").unwrap();
    plant(&base, &name, &key, b"second").await;
    // The bucket row a parts-bearing object implies (a lazy first sight),
    // then the stale row and the retained part list it was completed with.
    base.meta
        .record_if_absent(&name, SystemTime::now())
        .await
        .unwrap();
    base.meta
        .put_with_parts(
            &name,
            &key,
            &Row {
                etag: ETag::from_content(b"first"),
                size: 5,
                mtime: SystemTime::now(),
                identity: 0,
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            },
            &[PartRow {
                number: 1,
                size: 5,
                checksum: None,
            }],
        )
        .await
        .unwrap();

    let ops = base.objects(BucketRow::Lazy, MetadataFailure::Fail);
    let head = ops.head_object(&name, &key).await.unwrap();
    assert_eq!(
        head.etag,
        ETag::from_content(b"second"),
        "the stale row healed"
    );
    assert_eq!(
        ops.list_object_parts(&name, &key).await.unwrap().len(),
        1,
        "the retained part list survives the heal"
    );
}

#[tokio::test]
async fn a_stale_row_is_recomputed_and_rewritten() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("a.txt").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    ops.commit_object(
        &name,
        &key,
        ops.stage_body(&name, &key, body(&[b"first"]), None, None)
            .await
            .unwrap(),
        tags(),
        Some(&owner()),
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();

    // An out-of-band rewrite: the row still describes the old bytes.
    plant(&base, &name, &key, b"second").await;
    assert_eq!(base.meta.get(&name, &key).await.unwrap().unwrap().size, 5);

    let head = ops.head_object(&name, &key).await.unwrap();
    assert_eq!(
        head.etag,
        ETag::from_content(b"second"),
        "the content is the truth"
    );
    assert_eq!(head.size, 6);
    let row = base.meta.get(&name, &key).await.unwrap().expect("the row");
    assert_eq!(row.etag, ETag::from_content(b"second"));
    assert_eq!(row.tags, tags(), "the recorded elements ride the heal");
    assert_eq!(row.owner.as_ref(), Some(&owner()));
}

#[tokio::test]
async fn the_staleness_gate_and_the_composed_keep_rule_are_read_off_the_contracts() {
    let key = object::key("a.txt").unwrap();
    let entry = ContentEntry {
        key: key.clone(),
        len: 10,
        identity: 7,
        mtime: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000)),
    };
    let row = |etag: ETag, size: u64, mtime: u64, identity: u64| Row {
        etag,
        size,
        mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(mtime),
        identity,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    };

    // The gate: the size, the mtime and (when both sides have one) the
    // identity must agree.
    assert!(row_matches(&row(ETag::EMPTY, 10, 1_000, 7), &entry));
    assert!(!row_matches(&row(ETag::EMPTY, 11, 1_000, 7), &entry));
    assert!(!row_matches(&row(ETag::EMPTY, 10, 999, 7), &entry));
    assert!(!row_matches(&row(ETag::EMPTY, 10, 1_000, 9), &entry));
    assert!(
        row_matches(&row(ETag::EMPTY, 10, 1_000, 0), &entry),
        "an identity-less store falls back to size and mtime"
    );
    // A store with no timestamp at all cannot see an mtime drift.
    let timeless = ContentEntry {
        mtime: None,
        ..entry.clone()
    };
    assert!(row_matches(&row(ETag::EMPTY, 10, 999, 7), &timeless));

    // The composed keep rule: a `-N` form on the same size, confirmed by
    // the identity and the unchanged mtime — or by the jitter window when
    // no identity exists.
    let composed = "5d41402abc4b2a76b9719d911017c592-2"
        .parse::<ETag>()
        .unwrap();
    assert!(composed_keep(&row(composed.clone(), 10, 1_000, 7), &entry));
    assert!(
        !composed_keep(&row(composed.clone(), 10, 1_000, 9), &entry),
        "a changed identity re-hashes"
    );
    assert!(
        !composed_keep(&row(composed.clone(), 10, 999, 7), &entry),
        "a drifted mtime re-hashes where an identity exists"
    );
    let touched = ContentEntry {
        mtime: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_030)),
        ..entry.clone()
    };
    assert!(
        composed_keep(&row(composed.clone(), 10, 1_000, 0), &touched),
        "an identity-less store keeps the form inside the jitter window"
    );
    let drifted = ContentEntry {
        mtime: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000 + 300)),
        ..entry.clone()
    };
    assert!(
        !composed_keep(&row(composed.clone(), 10, 1_000, 0), &drifted),
        "a larger drift re-hashes"
    );
    assert!(
        !composed_keep(&row(ETag::EMPTY, 10, 1_000, 7), &entry),
        "a single-form ETag has a content-derived equivalent"
    );
    assert!(
        !composed_keep(&row(composed, 11, 1_000, 7), &entry),
        "a size change is a content change"
    );
}

#[tokio::test]
async fn a_read_only_store_serves_the_heal_without_writing_it() {
    // Both persist arms degrade: the guarded row-backed one, and the lazy
    // one whose heal records the bucket row it needs first — a read-only
    // run serves the values either write would have produced and persists
    // nothing at all, the bucket row included.
    let dir = tempfile::tempdir().unwrap();
    let key = object::key("dropped.txt").unwrap();
    let name = bucket::name("data").unwrap();
    {
        let meta = Arc::new(RedbMetadata::open(dir.path()).unwrap());
        let content = Arc::new(RedbContent::from_driver(meta.driver()));
        let base = Base::over(Arc::clone(&meta), content, RecordingHooks::new());
        let namespace = base.ops(BucketRow::Guaranteed);
        namespace
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        plant(&base, &name, &key, b"dropped").await;
    }
    let driver = Driver::open_read_only(dir.path()).unwrap();
    let meta = Arc::new(RedbMetadata::from_driver(Arc::clone(&driver)));
    let content = Arc::new(RedbContent::from_driver(driver));

    for bucket_row in [BucketRow::Guaranteed, BucketRow::Lazy] {
        let ops = ObjectOps {
            meta: Arc::clone(&meta),
            content: Arc::clone(&content),
            metadata_failure: MetadataFailure::Fail,
            bucket_row,
        };
        let head = ops.head_object(&name, &key).await.unwrap();
        assert_eq!(
            head.etag,
            ETag::from_content(b"dropped"),
            "the read is served, not failed ({bucket_row:?})"
        );
        assert!(
            meta.get(&name, &key).await.unwrap().is_none(),
            "nothing was written back ({bucket_row:?})"
        );
    }
}

#[tokio::test]
async fn ranges_resolve_against_the_served_length() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("range.txt").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let staged = ops
        .stage_body(&name, &key, body(&[b"0123456789"]), None, None)
        .await
        .unwrap();
    ops.commit_object(
        &name,
        &key,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();

    let full = ops.get_object(&name, &key, None).await.unwrap();
    assert_eq!(full.served_range, None, "a full read answers no range");
    assert_eq!(served(full).await, b"0123456789");

    let partial = ops
        .get_object(&name, &key, Some(ByteRange::Inclusive(2, 5)))
        .await
        .unwrap();
    assert_eq!(partial.served_range, Some((2, 5)));
    assert_eq!(served(partial).await, b"2345");

    let open_ended = ops
        .get_object(&name, &key, Some(ByteRange::From(8)))
        .await
        .unwrap();
    assert_eq!(open_ended.served_range, Some((8, 9)));
    assert_eq!(served(open_ended).await, b"89");

    let unsatisfiable = ops
        .get_object(&name, &key, Some(ByteRange::From(10)))
        .await
        .unwrap_err();
    assert!(
        matches!(unsatisfiable, StorageError::InvalidRange { .. }),
        "{unsatisfiable:?}"
    );
}

#[tokio::test]
async fn a_rename_moves_the_bytes_and_the_record_in_one_call_each() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let src = object::key("src.txt").unwrap();
    let dst = object::key("dst.txt").unwrap();
    let (ops, recording) = base.recording_objects(BucketRow::Guaranteed);
    ops.commit_object(
        &name,
        &src,
        ops.stage_body(&name, &src, body(&[b"moved"]), None, None)
            .await
            .unwrap(),
        tags(),
        Some(&owner()),
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();
    base.meta
        .put_with_parts(
            &name,
            &src,
            &base.meta.get(&name, &src).await.unwrap().unwrap(),
            &[PartRow {
                number: 1,
                size: 5,
                checksum: None,
            }],
        )
        .await
        .unwrap();

    let moved = ops.rename_object(&name, &src, &dst).await.unwrap();

    assert!(
        recording.saw(
            |call| matches!(call, ContentCall::Rename(from, to) if from == &src && to == &dst)
        ),
        "the bytes move by the content plane's rename: {:?}",
        recording.calls()
    );
    assert_eq!(base.counts().moves, 1, "the metadata moves by `move_`");
    assert_eq!(moved.key, dst);
    assert_eq!(moved.etag, ETag::from_content(b"moved"));
    assert_eq!(moved.tags, tags(), "a rename is not a fresh object");
    assert_eq!(moved.owner.as_ref(), Some(&owner()));
    assert!(base.meta.get(&name, &src).await.unwrap().is_none());
    let row = base
        .meta
        .get(&name, &dst)
        .await
        .unwrap()
        .expect("the dst row");
    assert_eq!(row.tags, tags());
    assert_eq!(
        base.meta.parts(&name, &dst).await.unwrap().len(),
        1,
        "the retained parts move with the record"
    );
    assert!(base.content.stat(&name, &src).await.unwrap().is_none());
    assert_eq!(
        served(ops.get_object(&name, &dst, None).await.unwrap()).await,
        b"moved"
    );

    // A self-rename is the identity: the bytes stay, the record stays.
    let again = ops.rename_object(&name, &dst, &dst).await.unwrap();
    assert_eq!(again.etag, ETag::from_content(b"moved"));
    assert_eq!(base.counts().moves, 1, "a self-move is not recorded");
}

#[tokio::test]
async fn the_tag_pairs_replace_clear_and_follow_the_row_policy() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("a.txt").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let staged = ops
        .stage_body(&name, &key, body(&[b"x"]), None, None)
        .await
        .unwrap();
    ops.commit_object(
        &name,
        &key,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();

    assert!(ops.get_object_tags(&name, &key).await.unwrap().is_empty());
    let written = ops.put_object_tags(&name, &key, &tags()).await.unwrap();
    assert_eq!(written, TagWriteResult::untouched());
    assert_eq!(ops.get_object_tags(&name, &key).await.unwrap(), tags());
    let replaced = object::Tags::from_pairs([("team".to_owned(), "edge".to_owned())]).unwrap();
    ops.put_object_tags(&name, &key, &replaced).await.unwrap();
    assert_eq!(
        ops.get_object_tags(&name, &key).await.unwrap(),
        replaced,
        "a tag put replaces the set, it never merges"
    );
    ops.delete_object_tags(&name, &key).await.unwrap();
    assert!(ops.get_object_tags(&name, &key).await.unwrap().is_empty());
    ops.delete_object_tags(&name, &key).await.unwrap(); // idempotent

    // A missing object: the reads refuse, the delete is lenient.
    let ghost = object::key("ghost.txt").unwrap();
    let no_such = ops.get_object_tags(&name, &ghost).await.unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
    let no_such = ops
        .put_object_tags(&name, &ghost, &tags())
        .await
        .unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
    assert_eq!(
        ops.delete_object_tags(&name, &ghost).await.unwrap(),
        TagWriteResult::untouched()
    );

    // The lazy store's existence is the content side: a hand-dropped
    // object is tagged, and the tags land on the healed row.
    let lazy = base.objects(BucketRow::Lazy, MetadataFailure::Fail);
    let dropped = object::key("dropped.txt").unwrap();
    plant(&base, &name, &dropped, b"dropped").await;
    lazy.put_object_tags(&name, &dropped, &tags())
        .await
        .unwrap();
    assert_eq!(lazy.get_object_tags(&name, &dropped).await.unwrap(), tags());
    assert!(base.meta.get(&name, &dropped).await.unwrap().is_some());
    let no_such = lazy
        .get_object_tags(&name, &object::key("never.txt").unwrap())
        .await
        .unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
}

#[tokio::test]
async fn the_acl_pair_replaces_the_grants_and_preserves_the_owner() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("a.txt").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    ops.commit_object(
        &name,
        &key,
        ops.stage_body(&name, &key, body(&[b"x"]), None, None)
            .await
            .unwrap(),
        tags(),
        Some(&owner()),
        &Acl::default_private(Some(owner())),
        None,
    )
    .await
    .unwrap();

    let created = ops.get_object_acl(&name, &key).await.unwrap();
    assert_eq!(created.owner.as_ref(), Some(&owner()));
    ops.put_object_tags(&name, &key, &tags()).await.unwrap();

    ops.put_object_acl(&name, &key, &grants()).await.unwrap();
    let replaced = ops.get_object_acl(&name, &key).await.unwrap();
    assert_eq!(replaced.owner.as_ref(), Some(&owner()), "the owner rides");
    assert_eq!(replaced.grants, grants(), "the grants are replaced");
    let row = base.meta.get(&name, &key).await.unwrap().unwrap();
    assert_eq!(row.tags, tags(), "the ACL write left the tags alone");

    let ghost = object::key("ghost.txt").unwrap();
    let no_such = ops.get_object_acl(&name, &ghost).await.unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
    let no_such = ops
        .put_object_acl(&name, &ghost, &grants())
        .await
        .unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
}

#[tokio::test]
async fn the_retained_part_list_is_served_and_projected() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("parted.bin").unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    ops.commit_object(
        &name,
        &key,
        ops.stage_body(&name, &key, body(&[b"ab"]), None, None)
            .await
            .unwrap(),
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
    )
    .await
    .unwrap();
    assert!(
        ops.list_object_parts(&name, &key).await.unwrap().is_empty(),
        "a plain put has no parts"
    );

    base.meta
        .put_with_parts(
            &name,
            &key,
            &base.meta.get(&name, &key).await.unwrap().unwrap(),
            &[
                PartRow {
                    number: 2,
                    size: 5,
                    checksum: None,
                },
                PartRow {
                    number: 1,
                    size: 5,
                    checksum: None,
                },
            ],
        )
        .await
        .unwrap();
    let parts = ops.list_object_parts(&name, &key).await.unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(u32::from(parts[0].part_number), 1, "part numbers order");
    assert_eq!(parts[1].size, 5);

    let ghost = object::key("ghost.txt").unwrap();
    let no_such = ops.list_object_parts(&name, &ghost).await.unwrap_err();
    assert!(matches!(no_such, StorageError::NoSuchKey(_)), "{no_such:?}");
}

/// Put one version of a key through the version-creating body.
async fn put_versioned<C: ObjectContent>(
    ops: &ObjectOps<TestMeta, C>,
    plane: &VersionPlane<Versions<TestMeta, C, BlockMem>, TestMeta, BlockMem>,
    name: &bucket::Name,
    key: &object::Key,
    chunks: &'static [&'static [u8]],
) -> object::Info {
    let staged = ops
        .stage_body(name, key, body(chunks), None, None)
        .await
        .unwrap();
    ops.put_object_versioned(
        plane,
        name,
        key,
        staged,
        object::Tags::empty(),
        None,
        &Acl::default_private(None),
        None,
        plane.version_rows.as_ref(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_versioned_put_supersedes_before_it_publishes() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    // The first version, through the recording plane so the publish
    // observes the key's history at that moment.
    let (recorded, recording) = base.recording_objects(BucketRow::Guaranteed);
    let plane = base.plane_with(recording.clone());
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let staged = recorded
        .stage_body(&name, &key, body(&[b"first"]), None, None)
        .await
        .unwrap();
    let first = recorded
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap();
    assert!(
        first.version_id.is_some(),
        "an enabled bucket mints identities"
    );
    assert!(first.bucket_records_versions);
    assert_eq!(
        recorded
            .get_object(&name, &key, None)
            .await
            .unwrap()
            .info
            .etag,
        ETag::from_content(b"first")
    );

    // The second version: at publish time the outgoing version's row must
    // already exist — the supersede runs first, because the publish
    // overwrites the bytes it needs.
    let second = put_versioned(&recorded, &plane, &name, &key, &[b"second"]).await;
    let publishes: Vec<PublishSight> = recording
        .calls()
        .iter()
        .filter_map(|call| match call {
            ContentCall::Publish(_, sight) => Some(sight.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        publishes
            .iter()
            .map(|sight| sight.history)
            .collect::<Vec<usize>>(),
        vec![0, 1],
        "the first publish found no history, the second found the superseded row"
    );
    // The **row last** leg: at each publish the key's row still describes
    // the outgoing content (the first write had none yet), so the incoming
    // row is written after the publish, never before it.
    assert_eq!(
        publishes
            .iter()
            .map(|sight| sight.row.clone())
            .collect::<Vec<Option<ETag>>>(),
        vec![None, Some(ETag::from_content(b"first"))],
        "the publish never sees the row the write is about to record"
    );
    assert_ne!(second.version_id, first.version_id);

    // What landed: the outgoing content in the block plane, the incoming
    // one at the key, and the slot naming the incoming version.
    let (seq, row) = plane
        .versions
        .latest(&name, &key)
        .await
        .unwrap()
        .expect("the history row");
    assert_eq!(
        row.etag,
        ETag::from_content(b"first"),
        "the outgoing version's ETag"
    );
    assert_eq!(row.size, 5);
    let mut blob = plane.blobs.open(&row.blob, None).await.unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = blob.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(
        bytes, b"first",
        "the superseded bytes moved into the block plane"
    );
    assert_eq!(seq, 1);
    assert_eq!(
        served(recorded.get_object(&name, &key, None).await.unwrap()).await,
        b"second"
    );
    let slot = plane
        .version_rows
        .current_slot(&name, &key)
        .await
        .unwrap()
        .expect("a slot");
    assert_eq!(slot.version, second.version_id);
}

#[tokio::test]
async fn a_versioned_put_on_an_unversioned_bucket_takes_the_version_free_shape() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("plain.txt").unwrap();
    let plane = base.version_plane();

    let plain_ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let info = put_versioned(&plain_ops, &plane, &name, &key, &[b"plain"]).await;
    assert_eq!(
        info.version_id, None,
        "an unversioned write names no version"
    );
    assert!(!info.bucket_records_versions);
    assert!(base.meta.get(&name, &key).await.unwrap().is_some());
    assert!(
        plane
            .version_rows
            .current_slot(&name, &key)
            .await
            .unwrap()
            .is_none_or(|slot| slot.version.is_none()),
        "no identity is recorded for the key"
    );
    assert!(
        plane.versions.latest(&name, &key).await.unwrap().is_none(),
        "no version row is written"
    );
}

#[tokio::test]
async fn a_refused_versioned_row_write_leaves_the_current_version_in_place() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let first = put_versioned(&ops, &plane, &name, &key, &[b"first"]).await;
    let first_id = first
        .version_id
        .expect("an enabled write mints a version id");

    // The second write supersedes, publishes, then the row plane refuses.
    // The refusal is the call's answer, and the version it replaced stays
    // current: its bytes, its slot, and no history row of its own.
    base.counted().refuse_next_put_with_parts();
    let staged = ops
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let err = ops
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");

    assert_eq!(
        served(ops.get_object(&name, &key, None).await.unwrap()).await,
        b"first"
    );
    let slot = plane
        .version_rows
        .current_slot(&name, &key)
        .await
        .unwrap()
        .expect("the first version's slot");
    assert_eq!(slot.version.as_ref(), Some(&first_id));
    assert!(
        plane.versions.latest(&name, &key).await.unwrap().is_none(),
        "the superseded row is taken back with the refusal"
    );
}

#[tokio::test]
async fn a_refused_versioned_row_write_keeps_the_retained_parts() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let first = put_versioned(&ops, &plane, &name, &key, &[b"first"]).await;
    let first_id = first
        .version_id
        .expect("an enabled write mints a version id");
    let row = base.meta.get(&name, &key).await.unwrap().unwrap();
    base.meta
        .put_with_parts(
            &name,
            &key,
            &row,
            &[PartRow {
                number: 1,
                size: row.size,
                checksum: None,
            }],
        )
        .await
        .unwrap();

    base.counted().refuse_next_put_with_parts();
    let staged = ops
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let err = ops
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");
    assert_eq!(
        served(ops.get_object(&name, &key, None).await.unwrap()).await,
        b"first"
    );
    assert_eq!(
        ops.list_object_parts(&name, &key).await.unwrap().len(),
        1,
        "the retained parts survive the refused row write"
    );
    let slot = plane
        .version_rows
        .current_slot(&name, &key)
        .await
        .unwrap()
        .expect("the first version's slot");
    assert_eq!(slot.version.as_ref(), Some(&first_id));
}

#[tokio::test]
async fn a_refused_versioned_slot_advance_leaves_the_current_version_in_place() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let ops = base.objects(BucketRow::Guaranteed, MetadataFailure::Fail);
    let first = put_versioned(&ops, &plane, &name, &key, &[b"first"]).await;
    let first_id = first
        .version_id
        .expect("an enabled write mints a version id");

    // The row write lands and the slot advance is refused. The refusal is
    // the call's answer, and the version it replaced stays current.
    base.counted().refuse_next_advance_slot();
    let staged = ops
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let err = ops
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");

    assert_eq!(
        served(ops.get_object(&name, &key, None).await.unwrap()).await,
        b"first"
    );
    let slot = plane
        .version_rows
        .current_slot(&name, &key)
        .await
        .unwrap()
        .expect("the first version's slot");
    assert_eq!(slot.version.as_ref(), Some(&first_id));
    assert!(
        plane.versions.latest(&name, &key).await.unwrap().is_none(),
        "the superseded row is taken back with the refusal"
    );
}

#[test]
fn publish_ids_in_one_second_differ_in_the_sequence_half() {
    let first = u64::from(mint_publish_id());
    let second = u64::from(mint_publish_id());
    assert_ne!(first, second);
    if first >> 32 == second >> 32 {
        assert_eq!(second & 0xFFFF_FFFF, (first & 0xFFFF_FFFF).wrapping_add(1));
    }
}
