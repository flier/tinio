//! The fs version paths, end to end: the four journeys the plan pins (an
//! overwrite keeps the previous version readable, a suspended write replaces
//! the null version in place, deleting the last version removes the key, a
//! simple delete inserts a marker), the rename refusal, and the shared
//! conformance leg — the same oracle the memory backend runs.

use std::{
    fs,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures::stream;

use crate::{
    _core::{
        BodyStream, ETag,
        acl::Acl,
        bucket, collect_body, from_nanos,
        meta::VersionMeta,
        object, object_lock,
        storage::{self, BucketOps, ObjectOps, VersionOps},
        version::{self, ListParams},
    },
    _store_redb::{
        object_meta::{self, Stored},
        version as rows, version_object_lock,
    },
    _util::testing::{conformance_versioning, metadata::reservation},
    FsStorage,
    database::Error as DatabaseError,
    meta::{Content, Verdict, gate},
    sweep::{Options as SweepOptions, Sweeper},
    testutil,
    version::list::RESOLVE_CONCURRENCY,
};

/// A storage with a `data` bucket — unversioned, the state the round-trip
/// test starts from; each journey sets the state it wants on top.
async fn fixture() -> (tempfile::TempDir, FsStorage, bucket::Name) {
    let (root, storage) = testutil::storage();
    let (b, _) = testutil::versioned_bucket(&storage, version::State::Unversioned, &[]).await;
    (root, storage, b)
}

/// Stage and commit one body, answering the committed metadata.
async fn put(
    storage: &FsStorage,
    b: &bucket::Name,
    key: &object::Key,
    bytes: &[u8],
) -> object::Info {
    let chunk = Bytes::copy_from_slice(bytes);
    let body: BodyStream = Box::pin(stream::once(async move { Ok(chunk) }));
    let staged = storage.stage_body(b, key, body, None, None).await.unwrap();
    storage
        .commit_object(
            b,
            key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap()
}

/// One full page of the bucket's versions (the tests' own listing shape).
async fn list(storage: &FsStorage, b: &bucket::Name) -> version::Listing {
    page(
        storage,
        b,
        ListParams {
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await
}

/// One page with explicit parameters (the journey's delimiter and
/// pagination legs).
async fn page(storage: &FsStorage, b: &bucket::Name, params: ListParams) -> version::Listing {
    storage.list_object_versions(b, params).await.unwrap()
}

/// The `(key, version id)` pairs of a page, in page order — the shape the
/// pagination reassembly compares.
fn listed(entries: &[version::Entry]) -> Vec<(String, Option<String>)> {
    entries
        .iter()
        .map(|entry| {
            (
                entry.key.as_ref().to_owned(),
                entry.id.as_ref().map(|id| id.as_str().to_owned()),
            )
        })
        .collect()
}

/// The key's version-table rows, oldest first (the store's own walk).
fn version_rows(storage: &FsStorage, b: &bucket::Name, key: &object::Key) -> Vec<rows::Row> {
    let (b, key) = (b.clone(), key.clone());
    storage
        .handle()
        .read(move |txn| {
            let table = rows::Table::open_readonly(txn)?;
            let mut out = Vec::new();
            table.for_key(b.as_ref().as_str(), key.as_ref().as_str(), |row| {
                out.push(row);
                Ok(())
            })?;
            Ok(out)
        })
        .unwrap()
}

/// The key's file exists in the bucket tree.
fn file_exists(root: &tempfile::TempDir, key: &str) -> bool {
    fs::metadata(root.path().join("data").join(key)).is_ok()
}

/// Whether the bucket holds any `object_meta` row — the shared table probe
/// ([`object_meta::Table::has_bucket`], the one `ObjectMeta::has_objects` and
/// the teardown's `remove_if_empty` both answer from), read through the
/// store's own handle like [`version_rows`] is. It **decodes nothing**, so a
/// delete marker's row counts; the store's own `walk` is the wrong oracle (it
/// drops a marker's row and materializes the bucket).
fn has_object_rows(storage: &FsStorage, b: &bucket::Name) -> bool {
    let b = b.clone();
    storage
        .handle()
        .read(move |txn| {
            Ok(object_meta::Table::open_readonly(txn)?.has_bucket(b.as_ref().as_str())?)
        })
        .unwrap()
}

/// The key's recorded `object_meta` row — the pair the staleness gate judges.
async fn stored_row(storage: &FsStorage, b: &bucket::Name, key: &object::Key) -> Stored {
    storage
        .meta_store()
        .stored_entry(b, key)
        .await
        .unwrap()
        .expect("the key's meta row")
}

/// The gate's verdict for one row/file pair, from its one home.
fn gate_verdict(row: &Stored, size: u64, mtime: SystemTime, identity: u64) -> Verdict {
    gate(
        row,
        Content::Present {
            size,
            mtime,
            identity,
        },
    )
}

#[tokio::test]
async fn overwriting_a_versioned_object_keeps_the_previous_version_readable() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let first = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");
    let second = put(&storage, &b, &key, b"second")
        .await
        .version_id
        .expect("an enabled write mints a version id");
    assert_ne!(first, second, "each version has its own identity");

    let current = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(current.body).await.unwrap(), b"second");
    assert_eq!(
        current.info.version_id.as_ref(),
        Some(&second),
        "the current read names the newest version"
    );

    match storage
        .get_object_version(&b, &key, Some(&first), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(old) => {
            assert_eq!(collect_body(old.body).await.unwrap(), b"first");
            assert_eq!(
                old.info.version_id.as_ref(),
                Some(&first),
                "the read answers the version it named"
            );
        }
        version::ReadResult::Marker { .. } => panic!("a superseded version is content"),
    }

    let page = list(&storage, &b).await;
    assert_eq!(page.entries.len(), 2, "both versions list");
    assert_eq!(page.entries[0].id.as_ref(), Some(&second), "newest first");
    assert_eq!(page.entries[1].id.as_ref(), Some(&first));
    assert_eq!(page.entries[0].size, 6);
    assert_eq!(page.entries[1].size, 5);
    assert!(page.entries.iter().all(|entry| !entry.is_delete_marker));
}

#[tokio::test]
async fn a_suspended_write_replacing_the_null_version_creates_no_history() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&b, version::State::Suspended)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    assert!(
        put(&storage, &b, &key, b"first").await.version_id.is_none(),
        "a suspended write records the null version"
    );
    assert!(
        put(&storage, &b, &key, b"second")
            .await
            .version_id
            .is_none(),
        "the replacement stays the null version"
    );

    assert!(
        version_rows(&storage, &b, &key).is_empty(),
        "an in-place replacement creates no version row"
    );
    let page = list(&storage, &b).await;
    assert_eq!(page.entries.len(), 1, "one version: the null version");
    assert!(page.entries[0].id.is_none());
    assert_eq!(page.entries[0].size, 6, "the replacement's content");

    match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(null) => {
            assert_eq!(collect_body(null.body).await.unwrap(), b"second");
            assert!(null.info.version_id.is_none());
        }
        version::ReadResult::Marker { .. } => panic!("the null version is content"),
    }
}

#[tokio::test]
async fn deleting_the_last_version_removes_the_key_from_the_tree() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    let removed = storage
        .delete_object_version(&b, &key, Some(&only), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(!removed.is_delete_marker);
    assert_eq!(
        removed.id.as_ref(),
        Some(&only),
        "the answer names the removed version"
    );

    assert!(!file_exists(&root, "notes.txt"), "the file is gone");
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "the key is unreadable"
    );
    assert!(
        version_rows(&storage, &b, &key).is_empty(),
        "no rows remain"
    );
    assert!(
        list(&storage, &b).await.entries.is_empty(),
        "the key left the listing"
    );
}

#[tokio::test]
async fn a_simple_delete_on_a_versioned_bucket_inserts_a_marker_and_keeps_the_content() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    let marker = storage.delete_object(&b, &key).await.unwrap();
    let first_marker = marker
        .id
        .clone()
        .expect("an enabled bucket's marker carries an identity");
    assert!(marker.is_delete_marker);
    assert!(!file_exists(&root, "notes.txt"), "the file is gone");
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "a marker is not content to a plain read"
    );

    let page = list(&storage, &b).await;
    assert_eq!(
        page.entries[0].id.as_ref(),
        Some(&first_marker),
        "the marker is the latest version"
    );
    assert!(page.entries[0].is_delete_marker);

    // The superseded version still reads by its id.
    match storage
        .get_object_version(&b, &key, Some(&only), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(old) => {
            assert_eq!(collect_body(old.body).await.unwrap(), b"first");
        }
        version::ReadResult::Marker { .. } => panic!("a superseded version is content"),
    }
    // The marker is a successful version-addressed read, not an error.
    match storage
        .get_object_version(&b, &key, Some(&first_marker), None)
        .await
        .unwrap()
    {
        version::ReadResult::Marker { id, .. } => assert_eq!(id.as_ref(), Some(&first_marker)),
        version::ReadResult::Object(_) => panic!("the marker is not content"),
    }

    // A second delete stacks a second marker (AWS's documented behavior).
    let second_marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("the second marker carries an identity");
    assert_ne!(first_marker, second_marker);
    let page = list(&storage, &b).await;
    assert_eq!(page.entries[0].id.as_ref(), Some(&second_marker));
    assert_eq!(page.entries[1].id.as_ref(), Some(&first_marker));
    assert!(page.entries[0].is_delete_marker && page.entries[1].is_delete_marker);
}

#[tokio::test]
async fn removing_a_marker_restores_the_version_it_hid() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"first").await.version_id.unwrap();
    let marker = storage.delete_object(&b, &key).await.unwrap().id.unwrap();

    let removed = storage
        .delete_object_version(&b, &key, Some(&marker), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(removed.is_delete_marker);
    assert_eq!(removed.id.as_ref(), Some(&marker));

    let restored = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(restored.body).await.unwrap(), b"first");
    assert_eq!(restored.info.version_id.as_ref(), Some(&only));
    assert_eq!(
        restored.info.size, 5,
        "the restore records the published byte count, not a re-stat"
    );
    assert_eq!(
        restored.info.etag,
        ETag::from_content(b"first"),
        "and the ETag the publish computed while streaming, not a re-hash"
    );
    let page = list(&storage, &b).await;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].id.as_ref(), Some(&only));
}

/// The restore's **marker-successor** arm: the newest remaining version is a
/// delete marker, so the restore makes the marker current again — its own
/// identity and stamp re-stamped onto the slot, nothing published.
///
/// The chain the steps below leave: a version, the marker a plain DELETE put
/// over it, and a newer version that superseded both. Deleting the newest
/// version leaves the marker as the newest row, which is the arm
/// `removing_a_marker_restores_the_version_it_hid` (the object successor's
/// journey) does not reach.
#[tokio::test]
async fn removing_a_version_restores_the_marker_it_superseded() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"first").await;

    // A plain DELETE hides the version behind a marker; a later write
    // supersedes the marker, so the marker becomes history with a version
    // row of its own — the row the restore below re-stamps the slot from.
    let marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    // The marker's own stamp: what its object row records while it is current,
    // and the stamp its later history row must carry.
    let stamp = stored_row(&storage, &b, &key).await.mtime;
    let newest = put(&storage, &b, &key, b"second").await.version_id.unwrap();
    let recorded = version_rows(&storage, &b, &key)
        .into_iter()
        .find(|row| row.kind == rows::Kind::Marker)
        .expect("the superseded marker has a history row");
    assert_eq!(recorded.id(), Some(marker.clone()));
    assert_eq!(
        recorded.created, stamp,
        "the marker's row carries the stamp its object row recorded"
    );

    // Deleting the newest version makes the marker current again.
    let removed = storage
        .delete_object_version(&b, &key, Some(&newest), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(!removed.is_delete_marker);
    assert_eq!(removed.id.as_ref(), Some(&newest));

    let restored = stored_row(&storage, &b, &key).await;
    assert!(restored.is_delete_marker, "the marker is current again");
    assert_eq!(
        restored.version_id.as_ref(),
        Some(&marker),
        "with the marker's own identity"
    );
    assert_eq!(
        restored.version_seq, recorded.seq,
        "and the marker's own sequence"
    );
    assert_eq!(
        restored.mtime, stamp,
        "and the marker's own stamp, never a fresh one"
    );
    assert_eq!(restored.size, 0, "a marker holds no content");
    assert_eq!(restored.etag, ETag::EMPTY);
    assert_eq!(restored.file_identity, 0, "a marker matches no file");
    assert!(
        !file_exists(&root, "notes.txt"),
        "the marker publishes nothing onto the key path"
    );
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "a marker is not content to a plain read"
    );

    // The restored marker consumes its own row (a current version has no row
    // of its own), and the listing answers the marker as the newest version.
    assert!(
        version_rows(&storage, &b, &key)
            .iter()
            .all(|row| row.id() != Some(marker.clone())),
        "the restored marker's history row is consumed"
    );
    let page = list(&storage, &b).await;
    assert_eq!(page.entries[0].id.as_ref(), Some(&marker));
    assert!(page.entries[0].is_delete_marker);
}

#[tokio::test]
async fn an_out_of_band_edit_degrades_the_version_addressed_reads_to_the_null_version() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The edit: a different length, written behind the backend's back.
    let edited = b"an out-of-band edit, longer than the original";
    fs::write(root.path().join("data").join("notes.txt"), edited).unwrap();

    // The stale identity no longer resolves — it never names the edited
    // bytes.
    assert!(
        storage
            .get_object_version(&b, &key, Some(&old), None)
            .await
            .is_err(),
        "a recorded identity the content no longer matches must not serve"
    );
    assert!(
        storage
            .head_object_version(&b, &key, Some(&old))
            .await
            .is_err(),
        "the head agrees with the get"
    );

    // The null version is the gated truth, and it serves the current bytes.
    match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(null) => {
            assert_eq!(collect_body(null.body).await.unwrap(), edited);
            assert!(
                null.info.version_id.is_none(),
                "the degraded current version carries no identity"
            );
            assert_eq!(null.info.size, edited.len() as u64);
        }
        version::ReadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    }
    // The head and the plain read agree with the get, on size and identity.
    let head = match storage.head_object_version(&b, &key, None).await.unwrap() {
        version::HeadResult::Object(info) => info,
        version::HeadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    };
    assert_eq!(
        head.size,
        edited.len() as u64,
        "the head's size is the get's"
    );
    assert!(head.version_id.is_none());
    let plain = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(plain.info.size, head.size);
    assert!(plain.info.version_id.is_none());
    // The plain read's row is already correct: the identity its gate judged
    // stale was cleared inside the same write transaction that healed the
    // row, so the verdict is durable without a second write — and a second
    // read cannot resurrect the identifier the first refused to serve.
    let healed = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        healed.version_id.is_none(),
        "the heal and the clear are one write: the row never names the stale version"
    );
    let again = storage.get_object(&b, &key, None).await.unwrap();
    assert!(
        again.info.version_id.is_none(),
        "the second read agrees with the first"
    );

    // The listing agrees too: one current entry, no identity, and the same
    // content-derived ETag the read serves (not the row's superseded one).
    let page = list(&storage, &b).await;
    assert_eq!(page.entries.len(), 1);
    assert!(page.entries[0].id.is_none());
    assert!(!page.entries[0].is_delete_marker);
    assert_eq!(page.entries[0].size, edited.len() as u64);
    assert_eq!(
        page.entries[0].etag, plain.info.etag,
        "the listing and the read serve one ETag for the current version"
    );
}

#[tokio::test]
async fn an_out_of_band_edit_then_tagging_or_acl_does_not_keep_the_stale_version_id() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The edit: a different length, written behind the backend's back. The
    // row still names `old` until a reader that knows the gate clears it.
    let edited = b"an out-of-band edit, longer than the original";
    fs::write(root.path().join("data").join("notes.txt"), edited).unwrap();

    // An element read heals the row. It must clear the identity the bytes no
    // longer match, the same verdict the plain read records.
    storage.get_object_tags(&b, &key).await.unwrap();
    storage.get_object_acl(&b, &key).await.unwrap();
    let tagged = object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap();
    let answer = storage.put_object_tags(&b, &key, &tagged).await.unwrap();
    assert!(
        answer.version_id.is_none(),
        "the tag write must not answer {old} for the edited bytes"
    );

    let plain = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(plain.body).await.unwrap(), edited);
    assert!(
        plain.info.version_id.is_none(),
        "tagging must not weld {old} onto the edited bytes"
    );
    let healed = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .unwrap();
    assert!(
        healed.version_id.is_none(),
        "the heal leaves the row with no stale version id"
    );
    assert!(
        storage
            .get_object_version(&b, &key, Some(&old), None)
            .await
            .is_err(),
        "the retired id does not resolve"
    );
}

#[tokio::test]
async fn an_out_of_band_edit_leaves_the_key_writable_and_its_history_readable() {
    // The journey: an unversioned write records the null version, the enabled
    // overwrite moves it into history at sequence 0, the file is then edited
    // out of band — the gate degrades the current slot to the null version
    // while the row at 0 still names the superseded null's content — and the
    // next write must succeed. The degraded slot records no identity, so
    // there is no version to move into history: the write takes the key's
    // next sequence instead of writing a row at 0, where the adapter's row
    // guard would (rightly) refuse to overwrite a row naming different
    // content. A write that superseded the degraded null at 0 would fail
    // here, and every later write to the key would fail the same way.
    let (root, storage, b) = fixture().await;
    let key = object::key("notes.txt").unwrap();

    let one = put(&storage, &b, &key, b"one").await.version_id;
    assert!(
        one.is_none(),
        "the write before versioning records the null version"
    );
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let two = put(&storage, &b, &key, b"two")
        .await
        .version_id
        .expect("the enabled write mints an identity");
    assert!(
        version_rows(&storage, &b, &key)
            .iter()
            .any(|row| row.seq == 0 && row.id().is_none()),
        "the superseded null version sits at sequence 0"
    );

    // The edit: written behind the backend's back, and read by nothing before
    // the write (a read would heal the recorded identity), so the plan still
    // sees the recorded identity the content no longer matches.
    let edited = b"edited out of band, and longer than the original";
    fs::write(root.path().join("data").join("notes.txt"), edited).unwrap();
    let current = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(
        current.is_null(),
        "the gate reports the edited slot as the null version: {current:?}"
    );

    // The write must succeed.
    let three = put(&storage, &b, &key, b"three")
        .await
        .version_id
        .expect("the write after an out-of-band edit must mint an identity");
    assert_ne!(three, two, "the write created a new version");
    assert_eq!(
        storage.get_object(&b, &key, None).await.unwrap().info.size,
        5,
        "the new version replaced the edited bytes at the key path"
    );

    // The history is readable and intact: the superseded null version lists
    // at its own sequence 0 with its own content, and stays addressable as
    // `versionId=null`.
    let page = list(&storage, &b).await;
    let entries = listed(&page.entries);
    assert_eq!(
        entries,
        vec![
            (
                key.as_ref().as_str().to_owned(),
                Some(three.as_str().to_owned())
            ),
            (key.as_ref().as_str().to_owned(), None),
        ],
        "the new version is current and the superseded null version still lists"
    );
    assert_eq!(
        (
            page.entries[1].seq,
            page.entries[1].size,
            page.entries[1].etag.clone()
        ),
        (0, 3, ETag::from_content(b"one")),
        "the superseded null version keeps its own sequence, size and ETag"
    );
    match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(null) => {
            assert_eq!(collect_body(null.body).await.unwrap(), b"one");
        }
        version::ReadResult::Marker { .. } => {
            panic!("the surviving null version is content, not a marker")
        }
    }
}

#[tokio::test]
async fn an_out_of_band_removal_degrades_the_key_to_the_null_version_and_retires_the_id() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The removal: the file goes behind the backend's back, while the
    // `object_meta` row still describes the content it held.
    fs::remove_file(root.path().join("data").join("notes.txt")).unwrap();

    // The recorded id stops resolving — it names content that no longer
    // exists — and the key reads as the null version, never as a delete
    // marker the API did not create.
    assert!(
        storage
            .get_object_version(&b, &key, Some(&old), None)
            .await
            .is_err(),
        "a recorded identity whose content is gone must not serve"
    );
    assert!(
        storage
            .head_object_version(&b, &key, Some(&old))
            .await
            .is_err(),
        "the head agrees with the get"
    );
    let current = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(
        current.exists && current.is_null() && !current.is_delete_marker,
        "the mirror reports the removal as the null version: {current:?}"
    );
    // The plain read is the mirror's truth: no file, no object.
    assert!(storage.get_object(&b, &key, None).await.is_err());
    // And the listing shows no marker the API never created.
    assert!(
        list(&storage, &b).await.entries.is_empty(),
        "neither a current entry nor history: the key has nothing to list"
    );
}

#[tokio::test]
async fn an_out_of_band_removal_under_a_marker_history_discloses_no_marker() {
    // The plain read's 404 discloses a marker only when the key's *current*
    // version is one (the design's delete-marker rule). A key whose current
    // file is removed out of band while a marker sits in its version table
    // is not a marked key: the disclosure's probe — `VersionOps::
    // current_version`, the server's `miss_hides_a_marker` input — reads
    // the current slot, and that slot is the gone content version, never
    // the marker the history holds. The retired listing-based probe would
    // have answered the newest history row (the marker) and put
    // `x-amz-delete-marker: true` on a 404 about content that is merely
    // gone; this test pins the design's rule instead.
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"first").await;

    // Two plain deletes stack two markers: the second takes the slot, the
    // first becomes a marker row in the version table (the history).
    let history_marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    storage.delete_object(&b, &key).await.unwrap();

    // The write over the current marker supersedes it like any other
    // version — a marker has no content to move, but its row still enters
    // the version table — so the slot describes content while the stack's
    // marker rows sit in the history.
    let current = put(&storage, &b, &key, b"second")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let page = list(&storage, &b).await;
    assert!(
        page.entries
            .iter()
            .any(|entry| entry.is_delete_marker && entry.id.as_ref() == Some(&history_marker)),
        "the premise: a marker sits in the key's history: {:?}",
        listed(&page.entries)
    );
    assert_eq!(
        page.entries[0].id.as_ref(),
        Some(&current),
        "and the current version is the object the last write put there"
    );

    // Out of band: the current file goes, the row still describes content.
    fs::remove_file(root.path().join("data").join("notes.txt")).unwrap();
    // The retired probe's answer, for contrast: the listing drops the gone
    // current slot and leads with a marker of the key's history, so a
    // `ListObjectVersions(prefix=key, max_keys=1)` page would have
    // disclosed the marker on this 404.
    let after = list(&storage, &b).await;
    assert!(
        after.entries[0].is_delete_marker,
        "the listing leads with a marker of the key's history: {:?}",
        listed(&after.entries)
    );
    // Nothing about that marker makes the slot the marker: the probe reads
    // the current slot, which the mirror reports as the gone content
    // version, not a marker.
    assert_eq!(
        storage.current_version(&b, &key).await.unwrap(),
        version::CurrentVersion::Object,
        "the probe reads the current slot, never the newest history row"
    );
    let err = storage
        .get_object(&b, &key, None)
        .await
        .expect_err("the plain read of the gone file misses");
    assert!(
        matches!(err, crate::Error::Storage(storage::Error::NoSuchKey(_))),
        "the plain read answers NoSuchKey, and the 404 discloses no marker: {err:?}"
    );
}

#[tokio::test]
async fn a_stale_reader_never_clears_a_version_identity_a_concurrent_write_installed() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The identity the reader's gate judged, and the open file it judges it
    // against (the five values a read path hands the ensure).
    let judged = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(judged.version_id.as_ref(), Some(&old));
    let (path, _file, size, mtime, identity) = storage.resolve_object_file(&b, &key).await.unwrap();

    // The reader observes a drifted file...
    fs::write(root.path().join("data").join("notes.txt"), b"edited").unwrap();
    // ...and before its ensure runs, a concurrent write lands.
    let fresh = put(&storage, &b, &key, b"second")
        .await
        .version_id
        .expect("an enabled write mints a version id");
    assert_ne!(old, fresh);

    // The reader's stale verdict now travels WITH the rewrite: it hands the
    // ensure the identity its gate judged, and the rewrite's own transaction
    // reads the row the write installed — a different identity, so the clear
    // refuses. One write, and the live version's only home (a current version
    // has no version-table row) survives the stale reader.
    storage
        .meta_store()
        .ensure_row(&b, &key, &path, size, mtime, identity, false, Some(&old))
        .await
        .unwrap();

    let after = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.version_id.as_ref(),
        Some(&fresh),
        "a live version's identity is never cleared by a stale reader"
    );
    match storage
        .get_object_version(&b, &key, Some(&fresh), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(object) => {
            assert_eq!(collect_body(object.body).await.unwrap(), b"second");
            assert_eq!(object.info.version_id.as_ref(), Some(&fresh));
        }
        version::ReadResult::Marker { .. } => panic!("the live version is content"),
    }
}

#[tokio::test]
async fn deleting_a_noncurrent_version_leaves_the_current_one_alone() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first").await.version_id.unwrap();
    let fresh = put(&storage, &b, &key, b"second").await.version_id.unwrap();

    let removed = storage
        .delete_object_version(&b, &key, Some(&old), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(!removed.is_delete_marker);
    assert_eq!(removed.id.as_ref(), Some(&old));

    let current = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(current.body).await.unwrap(), b"second");
    assert_eq!(current.info.version_id.as_ref(), Some(&fresh));
    let page = list(&storage, &b).await;
    assert_eq!(page.entries.len(), 1, "only the current version remains");
    assert_eq!(page.entries[0].id.as_ref(), Some(&fresh));
    assert!(
        storage
            .get_object_version(&b, &key, Some(&old), None)
            .await
            .is_err(),
        "the removed version is gone"
    );
}

#[tokio::test]
async fn the_supersede_crash_window_lists_one_current_version_and_deletes_cleanly() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");
    // The engine's crash window: the outgoing version's row is committed
    // while the version is still current (the rename has not happened yet).
    storage.versions(&b).supersede(&b, &key).await.unwrap();
    assert_eq!(
        version_rows(&storage, &b, &key).len(),
        1,
        "the adopted row exists"
    );

    let page = list(&storage, &b).await;
    assert_eq!(
        page.entries.len(),
        1,
        "the adopted row must not list the key's current version twice"
    );
    assert_eq!(page.entries[0].id.as_ref(), Some(&only));
    let current = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(current.body).await.unwrap(), b"first");
    assert_eq!(current.info.version_id.as_ref(), Some(&only));

    let removed = storage
        .delete_object_version(&b, &key, Some(&only), object_lock::Bypass::None)
        .await
        .unwrap();
    assert_eq!(removed.id.as_ref(), Some(&only));
    assert!(!file_exists(&root, "notes.txt"));
    assert!(
        version_rows(&storage, &b, &key).is_empty(),
        "the version's own adopted row dies with it"
    );
    assert!(list(&storage, &b).await.entries.is_empty());
}

#[tokio::test]
async fn the_publish_stamps_the_superseded_row_as_history() {
    // The state the crash window does not leave behind: a completed
    // overwrite's outgoing row is stamped noncurrent by the very state
    // transaction that moved the current slot off it, so a reader asks the
    // row what it is instead of inferring it from the slot's sequence. Drop
    // the stamp (never stamp the row, or write it stamped only at supersede
    // time) and these assertions fail.
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let first = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let second = put(&storage, &b, &key, b"second")
        .await
        .version_id
        .expect("an enabled write mints an identity");

    let rows = version_rows(&storage, &b, &key);
    assert_eq!(rows.len(), 1, "the superseded version's row");
    assert_eq!(rows[0].id.as_str(), first.as_str());
    assert_ne!(
        rows[0].became_noncurrent, 0,
        "the publish's state transaction stamps the superseded row noncurrent"
    );
    assert!(
        rows[0].is_history(rows::Slot::At(rows[0].seq)),
        "a stamped row is history even at a sequence a slot occupies"
    );
    assert!(
        rows[0].is_history(rows::Slot::Unknown),
        "and the retention walk's view (no slot in hand) keeps it"
    );

    // The crash window's adopted row is the opposite: the engine's supersede
    // of the still-current version commits an unstamped row at the slot's
    // sequence. It lists no second "current" and retention must not touch it.
    storage.versions(&b).supersede(&b, &key).await.unwrap();
    let rows = version_rows(&storage, &b, &key);
    assert_eq!(rows.len(), 2, "the adopted row joins the stamped history");
    let adopted = rows
        .iter()
        .find(|row| row.seq == 2)
        .expect("the adopted row sits at the current slot's sequence");
    assert_eq!(adopted.id.as_str(), second.as_str());
    assert_eq!(adopted.became_noncurrent, 0);
    assert!(
        !adopted.is_history(rows::Slot::At(2)),
        "the adopted row is the current version's own row"
    );
    assert!(
        !adopted.is_history(rows::Slot::Unknown),
        "no slot in hand: an unstamped row is never history"
    );
    let page = list(&storage, &b).await;
    assert_eq!(
        listed(&page.entries),
        vec![
            (
                key.as_ref().as_str().to_owned(),
                Some(second.as_str().to_owned())
            ),
            (
                key.as_ref().as_str().to_owned(),
                Some(first.as_str().to_owned())
            ),
        ],
        "the adopted row must not list the key's current version twice"
    );
}

#[tokio::test]
async fn an_unstamped_row_left_by_a_failed_publish_is_visible_to_retention_and_the_listing() {
    // The residue of a publish that never finished moving the slot: the
    // supersede committed the outgoing version's row, and the state
    // transaction that would have stamped it never ran (the unix
    // chown-failure path removes the file and errors before it; an
    // out-of-band file removal inside the crash window leaves the same
    // shape). Two reachable forms of it must not fall between the readers:
    //
    // - a row the slot has moved off (the slot records another sequence): it
    //   is history whatever its stamp says, so the listing shows it AND
    //   retention may retire it;
    // - a row the slot still records while the key serves nothing (the file
    //   went out of band): no current version exists, so it is history too,
    //   and the listing shows it.
    //
    // A reader that leans on the stamp alone hides the first from the listing
    // and from retention; one that reads "no current entry" as "no slot fact"
    // hides the second from the listing.
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let first = put(&storage, &b, &key, b"one")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let second = put(&storage, &b, &key, b"two")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let third = put(&storage, &b, &key, b"three")
        .await
        .version_id
        .expect("an enabled write mints an identity");

    // Plant the residue: the oldest version's row, its stamp cleared, at the
    // sequence it was superseded from.
    let history = version_rows(&storage, &b, &key);
    let planted = history
        .iter()
        .find(|row| row.id.as_str() == first.as_str())
        .expect("the first version's row");
    assert_ne!(planted.became_noncurrent, 0);
    let (planted_seq, cleared) = (planted.seq, planted.clone());
    let second_seq = history
        .iter()
        .find(|row| row.id.as_str() == second.as_str())
        .expect("the second version's row")
        .seq;
    let slot_seq = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .expect("the current slot's row")
        .version_seq;
    assert_ne!(
        planted_seq, slot_seq,
        "the planted row is not at the current slot's sequence"
    );
    let (bucket, key_ref) = (b.clone(), key.clone());
    storage
        .handle()
        .write(move |txn| -> Result<(), DatabaseError> {
            let mut table = rows::Table::open(txn)?;
            let mut row = cleared;
            row.became_noncurrent = 0;
            table.put(bucket.as_ref().as_str(), key_ref.as_ref().as_str(), &row)?;
            Ok(())
        })
        .await
        .unwrap();

    // The listing shows it: the current version first, then both rows of
    // history, the unstamped one included.
    let page = list(&storage, &b).await;
    assert_eq!(
        listed(&page.entries),
        vec![
            (
                key.as_ref().as_str().to_owned(),
                Some(third.as_str().to_owned())
            ),
            (
                key.as_ref().as_str().to_owned(),
                Some(second.as_str().to_owned())
            ),
            (
                key.as_ref().as_str().to_owned(),
                Some(first.as_str().to_owned())
            ),
        ],
        "an unstamped row the slot has moved off is history, and it lists"
    );

    // Retention retires it: the budget keeps the newest noncurrent version,
    // so the planted row — the older one — is past it.
    let sweeper = Sweeper::new(
        storage.clone(),
        SweepOptions {
            noncurrent_keep: 1,
            ..SweepOptions::default()
        },
    );
    let summary = sweeper.sweep_once(SystemTime::now()).await.unwrap();
    assert_eq!(
        summary.expired_versions, 1,
        "the unstamped row is retirable"
    );
    let left = version_rows(&storage, &b, &key);
    assert_eq!(
        left.iter().map(|row| row.seq).collect::<Vec<_>>(),
        vec![second_seq],
        "the budget kept the newest noncurrent version (seq {second_seq}) and retired the residue"
    );
    assert!(
        left.iter().all(|row| row.id.as_str() != first.as_str()),
        "the retired row is the planted one"
    );

    // The second form: the row is the one the slot still records — the crash
    // window's adopted row — and the file goes out of band, so the key serves
    // nothing and the page derives no current entry. The row is history
    // (nothing is current), so the listing shows the version alone.
    let dead = object::key("dead.txt").unwrap();
    let gone = put(&storage, &b, &dead, b"gone")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    storage.versions(&b).supersede(&b, &dead).await.unwrap();
    let dead_rows = version_rows(&storage, &b, &dead);
    assert_eq!(dead_rows.len(), 1, "the adopted row exists");
    assert_eq!(
        dead_rows[0].became_noncurrent, 0,
        "and the slot never moved off it"
    );
    fs::remove_file(root.path().join("data").join("dead.txt")).unwrap();

    let page = list(&storage, &b).await;
    let dead_entries: Vec<&version::Entry> = page
        .entries
        .iter()
        .filter(|entry| entry.key.as_ref() == dead.as_ref())
        .collect();
    assert_eq!(
        dead_entries.len(),
        1,
        "the residue lists: {:?}",
        listed(&page.entries)
    );
    assert_eq!(
        dead_entries[0].id.as_ref(),
        Some(&gone),
        "with the version the row names"
    );
    assert!(!dead_entries[0].is_delete_marker);
}

#[tokio::test]
async fn the_plain_read_and_the_version_slot_take_one_gate_verdict() {
    // The staleness gate's decision has one home (`meta::gate`). The two
    // readers that need it — the plain read's version facts
    // (`resolve_object_slot`) and the engine's current-slot view,
    // `current_with_row` — must answer the same
    // verdict for the same row/file pair in every state the gate
    // distinguishes: matching, drifted, missing, marker. A path that
    // re-derived the decision its own way answers differently here.
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let id = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let path = root.path().join("data").join("notes.txt");

    // Matching: the row still describes the file. The gate says live, the
    // plain read serves the recorded identity, and the engine's slot is that
    // version — not a degraded null.
    let row = stored_row(&storage, &b, &key).await;
    assert_eq!(row.version_id.as_ref(), Some(&id));
    let (_path, _file, size, mtime, identity) =
        storage.resolve_object_file(&b, &key).await.unwrap();
    assert_eq!(gate_verdict(&row, size, mtime, identity), Verdict::Live);
    let head = storage.head_object(&b, &key).await.unwrap();
    assert_eq!(head.version_id.as_ref(), Some(&id));
    assert_eq!(head.size, row.size, "the plain read's size is the row's");
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert_eq!(slot.version.as_ref(), Some(&id));
    assert_eq!(slot.seq, row.version_seq, "the slot's own sequence");
    assert!(!slot.degraded);
    // The version-addressed read answers the same object: it resolves the
    // slot through the engine's own gate and serves the current version
    // through the plain read's body — the same size, stamp, ETag and identity
    // the plain head serves.
    match storage
        .get_object_version(&b, &key, Some(&id), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(served) => {
            assert_eq!(served.info.size, head.size);
            assert_eq!(served.info.last_modified, head.last_modified);
            assert_eq!(served.info.etag, head.etag);
            assert_eq!(served.info.version_id, head.version_id);
            assert_eq!(
                collect_body(served.body).await.unwrap(),
                b"first",
                "and the bytes the handle holds"
            );
        }
        version::ReadResult::Marker { .. } => panic!("content, not a marker"),
    }

    // Drifted: an out-of-band edit the row no longer matches.
    fs::write(&path, b"an out-of-band edit, longer than the original").unwrap();
    let row = stored_row(&storage, &b, &key).await;
    assert_eq!(
        row.version_id.as_ref(),
        Some(&id),
        "the row is still stale-named"
    );
    let (_, _, size, mtime, identity) = storage.resolve_object_file(&b, &key).await.unwrap();
    assert_eq!(gate_verdict(&row, size, mtime, identity), Verdict::Degraded);
    // The engine's slot answers the degraded verdict (before any read heals
    // the row): the recorded identity is gone, the row still records one.
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(slot.exists && slot.is_null() && !slot.is_delete_marker);
    assert!(slot.degraded, "the row still records an identity: {slot:?}");
    assert_eq!(slot.seq, 0, "the null version's own sequence");
    // The plain read agrees: no identity, and the heal clears the stale
    // element in the same write that rewrites the row.
    let head = storage.head_object(&b, &key).await.unwrap();
    assert!(head.version_id.is_none());
    // The version-addressed read answers the same (the gated identity is the
    // null version's `None`), and the pre-step retires the stale element the
    // same way — before its own serve.
    match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(served) => {
            assert_eq!(served.info.size, head.size);
            assert_eq!(served.info.etag, head.etag);
            assert!(served.info.version_id.is_none());
        }
        version::ReadResult::Marker { .. } => panic!("content, not a marker"),
    }
    assert_eq!(head.size, size, "the edited file's size");
    assert!(
        stored_row(&storage, &b, &key).await.version_id.is_none(),
        "the heal and the clear are one write"
    );
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(
        slot.is_null() && !slot.degraded,
        "the healed slot: {slot:?}"
    );

    // Missing: the file goes out of band while the row still describes
    // content. The gate degrades it, the plain read has no object, and the
    // engine knows the key as the null version, never as a marker the API
    // did not create.
    fs::remove_file(&path).unwrap();
    let row = stored_row(&storage, &b, &key).await;
    assert!(!row.is_delete_marker);
    assert_eq!(gate(&row, Content::Missing), Verdict::Degraded);
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "the plain read is the mirror's truth: no file, no object"
    );
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(slot.exists && slot.is_null() && !slot.is_delete_marker);
    assert_eq!(slot.seq, 0);

    // Marker: a key of its own — a marker holds no content, so the gate's
    // missing-file arm keeps its identity live, and both readers agree that
    // is the current version.
    let marked = object::key("marked.txt").unwrap();
    let hidden = put(&storage, &b, &marked, b"hidden")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    let marker = storage
        .delete_object(&b, &marked)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    let row = stored_row(&storage, &b, &marked).await;
    assert!(row.is_delete_marker, "the recorded flag, not the shape");
    assert_eq!(gate(&row, Content::Missing), Verdict::Live);
    let slot = storage.versions(&b).current(&b, &marked).await.unwrap();
    assert!(slot.exists && slot.is_delete_marker);
    assert_eq!(slot.version.as_ref(), Some(&marker));
    // The plain read answers what a live marker verdict means for an object:
    // there is none, and the version path serves the marker's own identity.
    assert!(storage.head_object(&b, &marked).await.is_err());
    match storage
        .head_object_version(&b, &marked, Some(&marker))
        .await
        .unwrap()
    {
        version::HeadResult::Marker { id, .. } => assert_eq!(id.as_ref(), Some(&marker)),
        version::HeadResult::Object(_) => panic!("a marker is not content"),
    }
    // The version the marker hid still reads by its id.
    assert!(
        storage
            .head_object_version(&b, &marked, Some(&hidden))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn deleting_the_last_version_of_a_prefixed_key_prunes_the_prefix_directory() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("logs/a.txt").unwrap();
    let only = put(&storage, &b, &key, b"first").await.version_id.unwrap();
    assert!(root.path().join("data").join("logs").is_dir());

    let removed = storage
        .delete_object_version(&b, &key, Some(&only), object_lock::Bypass::None)
        .await
        .unwrap();
    assert_eq!(removed.id.as_ref(), Some(&only));
    assert!(
        !root.path().join("data").join("logs").exists(),
        "the emptied prefix directory must not survive the version delete"
    );
    // The emptied bucket is deletable again (an empty prefix directory made
    // `delete_bucket` answer NotEmpty forever).
    storage.delete_bucket(&b).await.unwrap();
}

#[tokio::test]
async fn a_bucket_holding_only_a_delete_marker_is_not_empty() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"first").await.version_id.unwrap();
    let marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");

    // A marker writes no file: the tree alone reads empty, so the
    // directory-based check this leg used to satisfy would delete the
    // bucket. The metadata-plane row still refuses (S3's own rule, and the
    // pg backend's teardown gate).
    assert_eq!(
        fs::read_dir(root.path().join("data")).unwrap().count(),
        0,
        "the marker leaves no file behind"
    );
    assert!(
        has_object_rows(&storage, &b),
        "the marker is an object_meta row"
    );
    let err = storage::Error::from(storage.delete_bucket(&b).await.unwrap_err());
    assert!(matches!(err, storage::Error::NotEmpty(_)), "{err:?}");

    // Every version and the marker removed: the rows go with the last one,
    // and the bucket deletes again.
    storage
        .delete_object_version(&b, &key, Some(&marker), object_lock::Bypass::None)
        .await
        .unwrap();
    storage
        .delete_object_version(&b, &key, Some(&only), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        !has_object_rows(&storage, &b),
        "the last version's removal drops the key's object row"
    );
    storage.delete_bucket(&b).await.unwrap();
}

#[tokio::test]
async fn version_history_alone_does_not_block_a_teardown() {
    // Invariant 4 of the shared-emptiness change: a bucket's `version` /
    // `version_index` rows are **not** an emptiness probe — the teardown
    // clears them with the rest of the derived state (pg deletes them under
    // the same `clear` flag; this is fs agreeing), instead of letting them
    // pin the bucket undeletable.
    //
    // The state is reachable through the public API by **leaving the
    // versioning epoch**: an enabled bucket's superseded version is history,
    // and once the bucket stops recording versions the version-free DELETE
    // takes the live object row and leaves that history behind — so the
    // bucket holds version rows and no object row.
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"first").await;
    put(&storage, &b, &key, b"second").await;
    assert!(
        !version_rows(&storage, &b, &key).is_empty(),
        "the superseded version left history"
    );
    assert!(
        storage.blobs_dir().join("data").is_dir(),
        "and its content is in the bucket's block subtree"
    );

    storage
        .set_bucket_versioning(&b, version::State::Unversioned)
        .await
        .unwrap();
    storage.delete_object(&b, &key).await.unwrap();
    assert!(
        !has_object_rows(&storage, &b),
        "the version-free delete removed the object row"
    );
    assert!(
        !version_rows(&storage, &b, &key).is_empty(),
        "and left the version history: the bucket holds version rows, no object row"
    );
    assert_eq!(
        fs::read_dir(root.path().join("data")).unwrap().count(),
        0,
        "the tree alone reads empty: the version rows are all that is left"
    );

    // The teardown proceeds — the rows are no probe — and the removal
    // pipeline takes them with the bucket's other state.
    storage.delete_bucket(&b).await.unwrap();
    assert!(!root.path().join("data").exists());
    assert!(
        version_rows(&storage, &b, &key).is_empty(),
        "the teardown cleared the version rows"
    );
    assert!(
        !storage.blobs_dir().join("data").exists(),
        "and the bucket's block subtree with them"
    );
}

#[tokio::test]
async fn a_versioned_bucket_with_live_content_and_a_marker_is_not_empty() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let keep = object::key("keep.txt").unwrap();
    let gone = object::key("gone.txt").unwrap();
    let live = put(&storage, &b, &keep, b"keep").await.version_id.unwrap();
    put(&storage, &b, &gone, b"gone").await;
    let marker = storage.delete_object(&b, &gone).await.unwrap();
    assert!(marker.is_delete_marker);
    assert!(file_exists(&root, "keep.txt"), "the live object stays");
    assert!(
        !file_exists(&root, "gone.txt"),
        "the markered object's file is gone"
    );

    // The live version goes too: the tree now holds nothing, so a
    // directory-only predicate answers empty — the marker's row is the only
    // refusal left, and it must still answer it (the old directory-only gate
    // deleted the bucket here, which is the property this leg pins).
    storage
        .delete_object_version(&b, &keep, Some(&live), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(
        !file_exists(&root, "keep.txt"),
        "the removed live version leaves no file"
    );
    assert_eq!(
        fs::read_dir(root.path().join("data")).unwrap().count(),
        0,
        "the tree alone reads empty once the live version is gone"
    );
    assert!(
        has_object_rows(&storage, &b),
        "the marker's object_meta row is the content the tree cannot see"
    );

    let err = storage::Error::from(storage.delete_bucket(&b).await.unwrap_err());
    assert!(matches!(err, storage::Error::NotEmpty(_)), "{err:?}");
}

#[tokio::test]
async fn a_simple_delete_of_a_prefixed_key_prunes_the_prefix_directory() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("logs/a.txt").unwrap();
    put(&storage, &b, &key, b"first").await;
    assert!(root.path().join("data").join("logs").is_dir());

    let marker = storage.delete_object(&b, &key).await.unwrap();
    assert!(marker.is_delete_marker);
    assert!(
        !root.path().join("data").join("logs").exists(),
        "the marker path prunes the emptied prefix directory too"
    );
    // The pruned tree is empty, but the marker is still an `object_meta`
    // row: the teardown refuses, as the pg backend's does.
    let err = storage::Error::from(storage.delete_bucket(&b).await.unwrap_err());
    assert!(matches!(err, storage::Error::NotEmpty(_)), "{err:?}");
}

#[tokio::test]
async fn rename_is_refused_while_the_bucket_versions() {
    let (_root, storage, b) = fixture().await;
    let key = object::key("notes.txt").unwrap();
    let dst = object::key("renamed.txt").unwrap();
    put(&storage, &b, &key, b"first").await;
    // Unversioned: the rename is today's move.
    storage.rename_object(&b, &key, &dst).await.unwrap();

    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let err = storage.rename_object(&b, &dst, &key).await.unwrap_err();
    assert!(
        matches!(storage::Error::from(err), storage::Error::AccessDenied(_)),
        "the refusal is the contract's AccessDenied class (the memory backend's answer)"
    );
    // Suspended refuses too.
    storage
        .set_bucket_versioning(&b, version::State::Suspended)
        .await
        .unwrap();
    assert!(storage.rename_object(&b, &dst, &key).await.is_err());
}

#[tokio::test]
async fn the_shared_conformance_leg_holds_for_the_filesystem_backend() {
    let (_root, storage, b) = fixture().await;
    conformance_versioning(&storage, &b).await;
}

#[tokio::test]
async fn the_bucket_state_round_trips_and_a_missing_bucket_is_refused() {
    let (_root, storage, b) = fixture().await;
    assert_eq!(
        storage.bucket_versioning(&b).await.unwrap(),
        version::State::Unversioned
    );
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        storage.bucket_versioning(&b).await.unwrap(),
        version::State::Enabled
    );
    let missing = bucket::name("missing").unwrap();
    assert!(storage.bucket_versioning(&missing).await.is_err());
    assert!(
        storage
            .set_bucket_versioning(&missing, version::State::Enabled)
            .await
            .is_err()
    );
}

/// The whole storage-level lifecycle, in one journey over the real fs
/// backend and its real state dir: enable versioning, overwrite three
/// times, read every version by id, list the versions (newest first, plus
/// the delimiter roll-up and `max_keys` pagination the fs tests never
/// exercised), delete a noncurrent version, take a simple delete to a
/// marker and back, run the retention sweep, and finally delete the
/// bucket — asserting the state dir holds neither its rows nor its
/// `blobs/` subtree.
#[tokio::test]
async fn the_whole_version_lifecycle_runs_over_the_real_state_dir() {
    use std::time::{Duration, SystemTime};

    use crate::{
        _core::{
            blob::{BlockHash, BlockStore},
            version::{IdMarker, ReadResult},
        },
        _store_redb::{bucket_versioning, version_index},
        database::Error as DatabaseError,
        sweep::{Options as SweepOptions, Sweeper},
    };

    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();

    // The first write and three overwrites: four versions, newest current.
    let mut ids = Vec::new();
    for bytes in [b"one".as_slice(), b"two", b"three", b"four"] {
        ids.push(
            put(&storage, &b, &key, bytes)
                .await
                .version_id
                .expect("an enabled write mints a version id"),
        );
    }
    // Two flat sibling keys for the delimiter leg: the `-` delimiter rolls
    // them up without creating a prefix directory that would outlive the
    // final deletes (an empty directory is content to `delete_bucket`).
    let flat_a = object::key("dir-a.txt").unwrap();
    let flat_b = object::key("dir-b.txt").unwrap();
    let a_id = put(&storage, &b, &flat_a, b"a").await.version_id.unwrap();
    let b_id = put(&storage, &b, &flat_b, b"b").await.version_id.unwrap();

    // Every version reads back under its own id, with its own content.
    for (id, bytes) in ids
        .iter()
        .zip([b"one".as_slice(), b"two", b"three", b"four"])
    {
        match storage
            .get_object_version(&b, &key, Some(id), None)
            .await
            .unwrap()
        {
            ReadResult::Object(object) => {
                assert_eq!(collect_body(object.body).await.unwrap(), bytes);
                assert_eq!(object.info.version_id.as_ref(), Some(id));
            }
            ReadResult::Marker { .. } => panic!("a version is content, not a marker"),
        }
    }

    // The full listing: newest first within the key, keys in wire order.
    let expected: Vec<(String, Option<String>)> = vec![
        ("dir-a.txt".to_owned(), Some(a_id.as_str().to_owned())),
        ("dir-b.txt".to_owned(), Some(b_id.as_str().to_owned())),
        ("notes.txt".to_owned(), Some(ids[3].as_str().to_owned())),
        ("notes.txt".to_owned(), Some(ids[2].as_str().to_owned())),
        ("notes.txt".to_owned(), Some(ids[1].as_str().to_owned())),
        ("notes.txt".to_owned(), Some(ids[0].as_str().to_owned())),
    ];
    let all = list(&storage, &b).await;
    assert_eq!(listed(&all.entries), expected, "newest first per key");
    assert!(!all.is_truncated);
    assert!(all.common_prefixes.is_empty());

    // The delimiter roll-up: the two `dir-` keys collapse into one common
    // prefix, the four notes versions stay as entries.
    let rolled = page(
        &storage,
        &b,
        ListParams {
            delimiter: Some("-".to_owned()),
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(rolled.common_prefixes, ["dir-"]);
    assert_eq!(
        listed(&rolled.entries),
        expected[2..].to_vec(),
        "the rolled-up keys leave the notes versions"
    );

    // `max_keys` pagination: two entries a page, resumed through the
    // returned key + version cursors until the listing is reassembled.
    let mut collected: Vec<(String, Option<String>)> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_marker: Option<IdMarker> = None;
    let mut pages = 0;
    loop {
        let page = page(
            &storage,
            &b,
            ListParams {
                key_marker: key_marker.clone(),
                version_id_marker: version_marker.clone(),
                max_keys: 2,
                ..Default::default()
            },
        )
        .await;
        pages += 1;
        assert!(
            page.entries.len() <= 2,
            "a page never exceeds max_keys: {}",
            page.entries.len()
        );
        collected.extend(listed(&page.entries));
        if !page.is_truncated {
            assert!(page.next_key_marker.is_none());
            break;
        }
        key_marker = page.next_key_marker.clone();
        version_marker = page.next_version_id_marker.clone().map(version::IdMarker);
        assert!(key_marker.is_some() && version_marker.is_some());
    }
    assert_eq!(pages, 3, "six versions at two per page");
    assert_eq!(
        collected, expected,
        "the pages reassemble the whole listing"
    );

    // Delete a noncurrent version: only its own row goes.
    let removed = storage
        .delete_object_version(&b, &key, Some(&ids[0]), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(!removed.is_delete_marker);
    assert_eq!(removed.id.as_ref(), Some(&ids[0]));
    assert!(
        storage
            .get_object_version(&b, &key, Some(&ids[0]), None)
            .await
            .is_err(),
        "the deleted version is gone"
    );

    // A simple delete: the marker is current, the version it hid still
    // reads by its id.
    let marker = storage.delete_object(&b, &key).await.unwrap();
    assert!(marker.is_delete_marker);
    let marker_id = marker
        .id
        .clone()
        .expect("an enabled bucket's marker carries an identity");
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "a marker is not content to a plain read"
    );
    match storage
        .get_object_version(&b, &key, Some(&ids[3]), None)
        .await
        .unwrap()
    {
        ReadResult::Object(object) => {
            assert_eq!(collect_body(object.body).await.unwrap(), b"four");
        }
        ReadResult::Marker { .. } => panic!("the version the marker hides is content"),
    }
    let with_marker = list(&storage, &b).await;
    assert!(with_marker.entries[2].is_delete_marker);
    assert_eq!(with_marker.entries[2].id.as_ref(), Some(&marker_id));

    // Removing the marker shows the version it hid: current again.
    let unmarked = storage
        .delete_object_version(&b, &key, Some(&marker_id), object_lock::Bypass::None)
        .await
        .unwrap();
    assert!(unmarked.is_delete_marker);
    let current = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(current.body).await.unwrap(), b"four");
    assert_eq!(current.info.version_id.as_ref(), Some(&ids[3]));

    // The retention sweep keeps one noncurrent version: the older one
    // expires, and the block store reclaims what is left unreferenced —
    // `one` (its row deleted above), `two` (just expired) and `four` (the
    // restore republished it onto the key path, dropping its row).
    let sweeper = Sweeper::new(
        storage.clone(),
        SweepOptions {
            noncurrent_keep: 1,
            ..SweepOptions::default()
        },
    );
    let past_grace = SystemTime::now() + Duration::from_secs(601);
    let summary = sweeper.sweep_once(past_grace).await.unwrap();
    assert_eq!(
        summary.expired_versions, 1,
        "the older of the two noncurrent versions expires"
    );
    assert_eq!(
        summary.reclaimed_blocks, 3,
        "one, two and four are unreferenced and past the grace"
    );
    assert_eq!(summary.reclaimed_bytes, 3 + 3 + 4);
    let survivors = version_rows(&storage, &b, &key);
    assert_eq!(survivors.len(), 1, "the keep budget keeps the newest");
    assert_eq!(survivors[0].id(), Some(ids[2].clone()));
    assert!(
        storage
            .get_object_version(&b, &key, Some(&ids[1]), None)
            .await
            .is_err(),
        "the expired version is gone"
    );
    match storage
        .get_object_version(&b, &key, Some(&ids[2]), None)
        .await
        .unwrap()
    {
        ReadResult::Object(object) => {
            assert_eq!(
                collect_body(object.body).await.unwrap(),
                b"three",
                "the surviving version's block is kept"
            );
        }
        ReadResult::Marker { .. } => panic!("the surviving version is content"),
    }
    assert!(
        storage
            .versions(&b)
            .blobs()
            .blocks()
            .contains(&BlockHash::of(b"three"))
            .await
            .unwrap()
    );

    // Delete the rest and the bucket: the index names the survivor until
    // the bucket's state is drained with it.
    let indexed = |id: &version::Id| {
        let handle = storage.handle().clone();
        let (bucket, key, id) = (b.clone(), key.clone(), id.as_str().to_owned());
        handle
            .read(move |txn| {
                Ok(version_index::Table::open_readonly(txn)?.seq_of(
                    bucket.as_ref().as_str(),
                    key.as_ref().as_str(),
                    &id,
                )?)
            })
            .unwrap()
    };
    assert_eq!(indexed(&ids[2]), Some(3), "the survivor is indexed");

    for id in [&ids[3], &ids[2]] {
        let removed = storage
            .delete_object_version(&b, &key, Some(id), object_lock::Bypass::None)
            .await
            .unwrap();
        assert_eq!(removed.id.as_ref(), Some(id));
    }
    assert!(
        storage.get_object(&b, &key, None).await.is_err(),
        "the last version's removal leaves the key absent"
    );
    assert!(!file_exists(&root, "notes.txt"));
    for (flat, id) in [(&flat_a, &a_id), (&flat_b, &b_id)] {
        storage
            .delete_object_version(&b, flat, Some(id), object_lock::Bypass::None)
            .await
            .unwrap();
    }

    storage.delete_bucket(&b).await.unwrap();

    // The state dir holds neither the bucket's rows nor its block subtree.
    assert!(
        !storage.blobs_dir().join("data").exists(),
        "the bucket's blobs/ subtree drops with the bucket"
    );
    let handle = storage.handle().clone();
    let drained = b.clone();
    let rows_left = handle
        .read(move |txn| -> Result<usize, DatabaseError> {
            let table = rows::Table::open_readonly(txn)?;
            let mut count = 0usize;
            table.for_bucket_of(drained.as_ref(), |_, _| {
                count += 1;
                Ok(())
            })?;
            Ok(count)
        })
        .unwrap();
    assert_eq!(rows_left, 0, "the bucket's version rows are drained");
    assert_eq!(indexed(&ids[2]), None, "so is its version index");
    let state_bucket = b.clone();
    let state_row =
        handle
            .read(move |txn| {
                Ok(bucket_versioning::Table::open_readonly(txn)?
                    .get(state_bucket.as_ref().as_str())?)
            })
            .unwrap();
    assert!(state_row.is_none(), "and its versioning state row");
    assert!(storage.bucket_versioning(&b).await.is_err());
}

/// Per-version metadata parity: the supersede carries the version's tags,
/// checksum, owner and ACL into its row, and the version-addressed
/// accessors answer them exactly as the write recorded them — a swapped
/// element or a wrong wire fails every assertion below.
#[tokio::test]
async fn a_superseded_version_carries_its_tags_checksum_owner_and_acl() {
    use crate::_core::checksum;

    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();

    let tags = object::Tags::from_pairs([
        ("env".to_owned(), "prod".to_owned()),
        ("team".to_owned(), "storage".to_owned()),
    ])
    .unwrap();
    let owner = testutil::owner_id();
    let grants = vec![testutil::all_users_read_grant()];
    let acl = Acl {
        owner: Some(owner.clone()),
        grants: grants.clone(),
    };
    let expected_checksum = checksum::Recorded {
        part: checksum::Part {
            algorithm: checksum::Algorithm::Md5,
            value: checksum::Value(testutil::md5_wire(b"first")),
        },
        kind: checksum::Type::FullObject,
    };

    // The first version, written with the full element set.
    let chunk = Bytes::copy_from_slice(b"first");
    let stream: BodyStream = Box::pin(stream::once(async move { Ok(chunk) }));
    let staged = storage
        .stage_body(
            &b,
            &key,
            stream,
            Some(testutil::checksum_tee(
                checksum::Algorithm::Md5,
                &testutil::md5_wire(b"first"),
            )),
            None,
        )
        .await
        .unwrap();
    let first = storage
        .commit_object(&b, &key, staged, tags.clone(), Some(&owner), &acl, None)
        .await
        .unwrap();
    let first_id = first
        .version_id
        .clone()
        .expect("an enabled write mints a version id");
    assert_eq!(
        first.checksum.as_ref(),
        Some(&expected_checksum),
        "the tee digest is recorded on the write"
    );

    // The overwrite moves it into history.
    let second = put(&storage, &b, &key, b"second").await;
    assert_ne!(second.version_id.as_ref(), Some(&first_id));

    // The version-addressed read serves every element back.
    let info = match storage
        .get_object_version(&b, &key, Some(&first_id), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(object) => object.info,
        version::ReadResult::Marker { .. } => panic!("the superseded version is content"),
    };
    assert_eq!(info.tags, tags, "the tag set round-trips");
    assert_eq!(
        info.checksum.as_ref(),
        Some(&expected_checksum),
        "the checksum round-trips, algorithm and kind included"
    );
    assert_eq!(info.owner.as_ref(), Some(&owner), "the owner round-trips");
    assert_eq!(info.acl.grants, grants, "the grant set round-trips");
    assert_eq!(info.version_id.as_ref(), Some(&first_id));

    // The head agrees with the get, element for element.
    let head = match storage
        .head_object_version(&b, &key, Some(&first_id))
        .await
        .unwrap()
    {
        version::HeadResult::Object(info) => info,
        version::HeadResult::Marker { .. } => panic!("the superseded version is content"),
    };
    assert_eq!(head.tags, info.tags);
    assert_eq!(head.checksum, info.checksum);
    assert_eq!(head.owner, info.owner);
    assert_eq!(head.acl, info.acl);

    // The element-scoped accessors recombine the same values.
    assert_eq!(
        storage
            .version_tags(&b, &key, Some(&first_id))
            .await
            .unwrap(),
        tags
    );
    assert_eq!(
        storage
            .version_acl(&b, &key, Some(&first_id))
            .await
            .unwrap(),
        acl,
        "the ACL's owner element is the row's own owner element"
    );
    // And the marker form refuses metadata, like the memory backend.
    let marker = storage.delete_object(&b, &key).await.unwrap().id.unwrap();
    assert!(
        storage.version_tags(&b, &key, Some(&marker)).await.is_err(),
        "a delete marker holds no tags"
    );
}

#[tokio::test]
async fn a_version_addressed_read_of_an_unknown_id_is_no_such_version() {
    let (_root, storage, b) = fixture().await;
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"first").await;
    let err = storage
        .get_object_version(&b, &key, Some(&version::Id::new("nope").unwrap()), None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("nope"), "the answer names the id");
    assert!(storage.head_object_version(&b, &key, None).await.is_ok());
}

/// The object-file orphan passes — the scanner's reclamation and the
/// doctor's meta-orphan repair — must never reclaim a current delete
/// marker's row: the marker's file is absent **by design**, and its
/// identity, sequence and version-index entry survive every pass (the rule's
/// home is `meta::orphan_candidate`).
#[tokio::test]
async fn the_orphan_passes_leave_a_current_delete_marker_alone() {
    use std::time::Duration;

    use futures::StreamExt;

    use crate::{
        _core::{
            cleanup::{Cleanup, CleanupOptions},
            mode::AccessMode,
        },
        _store_redb::version_index,
        FsCleanup, Scanner, ScannerOptions,
    };

    /// The identity index's sequence for `(b, key, id)`, as the store
    /// records it.
    fn index_seq(
        storage: &FsStorage,
        b: &bucket::Name,
        key: &object::Key,
        id: &version::Id,
    ) -> Option<u64> {
        let (bucket, key, id) = (b.clone(), key.clone(), id.clone());
        storage
            .handle()
            .read(move |txn| {
                Ok(version_index::Table::open_readonly(txn)?.seq_of(
                    bucket.as_ref(),
                    key.as_ref(),
                    id.as_str(),
                )?)
            })
            .unwrap()
    }

    /// The marker's row and its index entry, as the store records them.
    async fn assert_marker_intact(
        storage: &FsStorage,
        b: &bucket::Name,
        key: &object::Key,
        id: &version::Id,
        superseded: &version::Id,
    ) {
        let stored = storage
            .meta_store()
            .stored_entry(b, key)
            .await
            .unwrap()
            .expect("the marker's meta row survives");
        assert!(stored.is_delete_marker, "the row still records the marker");
        assert_eq!(stored.version_id.as_ref(), Some(id));
        assert!(
            index_seq(storage, b, key, superseded).is_some(),
            "the superseded marker's index entry survives"
        );
    }

    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"content").await;
    // Two deletes: the first marker becomes history (a version row whose
    // identity has an index entry), the second is the recorded current.
    let superseded = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    let marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    assert!(
        index_seq(&storage, &b, &key, &superseded).is_some(),
        "the superseded marker's index entry exists before the passes"
    );

    // The scanner's orphan pass: the marker's file is absent, and its row
    // is not an orphan candidate.
    let scanner = Scanner::new(
        storage.clone(),
        ScannerOptions {
            enabled: true,
            delay: Duration::from_millis(1),
            max_wait: Duration::from_millis(10),
            cycle: Duration::from_millis(50),
            access: AccessMode::ReadWrite,
        },
    );
    let summary = scanner.scan_once().await.unwrap();
    assert_eq!(summary.reclaimed, 0, "the marker is not reclaimed");
    assert_marker_intact(&storage, &b, &key, &marker, &superseded).await;

    // The doctor's meta-orphan repair.
    let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
    let mut actions = cleanup.reclaim_meta_orphans().await.unwrap();
    while let Some(action) = actions.next().await {
        let action = action.unwrap();
        assert!(
            !action.description.contains("orphaned meta entry"),
            "the marker is not an orphan: {action:?}"
        );
    }
    assert_marker_intact(&storage, &b, &key, &marker, &superseded).await;
}

/// A page whose key count is past the resolution window
/// ([`RESOLVE_CONCURRENCY`](crate::version::list)) must list exactly what
/// the one-key-at-a-time walk listed: the same keys, in the same order,
/// with each key's current version, its history and its arm (a versioned
/// key, a marked key, a key whose file went out of band under the page)
/// resolved independently — a resolution that reports its answer out of
/// turn, drops a key, or reads another key's slot fails here.
#[tokio::test]
async fn a_page_wider_than_the_resolution_window_lists_every_key_in_order() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();

    let mut expected: Vec<(String, Option<String>)> = Vec::new();
    for i in 0..40 {
        let name = format!("k{i:03}.txt");
        let key = object::key(&name).unwrap();
        let first = put(&storage, &b, &key, b"one")
            .await
            .version_id
            .expect("an enabled write mints an identity");
        match i {
            // A superseded version: the current one leads, the history row
            // follows.
            0 => {
                let second = put(&storage, &b, &key, b"two")
                    .await
                    .version_id
                    .expect("an enabled write mints an identity");
                expected.push((name.clone(), Some(second.as_str().to_owned())));
                expected.push((name, Some(first.as_str().to_owned())));
            }
            // A delete marker: the marker leads, the version it hid follows.
            1 => {
                let marker = storage
                    .delete_object(&b, &key)
                    .await
                    .unwrap()
                    .id
                    .expect("an enabled bucket's marker carries an identity");
                expected.push((name.clone(), Some(marker.as_str().to_owned())));
                expected.push((name, Some(first.as_str().to_owned())));
            }
            // An out-of-band removal: the row still describes content, so
            // the key has no current version — and no history — to list.
            2 => {
                fs::remove_file(root.path().join("data").join(&name)).unwrap();
            }
            _ => expected.push((name, Some(first.as_str().to_owned()))),
        }
    }

    let all = list(&storage, &b).await;
    assert_eq!(
        listed(&all.entries),
        expected,
        "the wide page keeps every key, in the wire's order"
    );

    // And a delimiter page over the same keys rolls the same set up: the
    // grouping runs on the resolved entries, so a key the concurrent walk
    // dropped (or listed twice) would change the roll-up too. The four
    // `5`-suffixed keys leave the entries as one common prefix each.
    let rolled = page(
        &storage,
        &b,
        ListParams {
            delimiter: Some("5".to_owned()),
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(rolled.common_prefixes, ["k005", "k015", "k025", "k035"]);
    assert_eq!(rolled.entries.len(), expected.len() - 4);
}

/// The page walk is bounded by the listing's own bound, not by the bucket: a
/// page resumed past a `key_marker` never opens — never **resolves** — the
/// keys below the marker.
///
/// The proof is the resolution's own side effect: a key whose file drifted
/// out of band is healed by the `resolve_object_slot` the page runs for it
/// (the row is rewritten with the file it found), so the keys the page never
/// reached keep exactly the rows the drift left them. The walk this test
/// replaced resolved and healed **every** key under the prefix before cutting
/// the page, so it fails on that shape and passes only on a bounded one.
#[tokio::test]
async fn a_page_past_the_key_marker_never_resolves_the_keys_before_it() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let names: Vec<String> = (0..6).map(|i| format!("k{i}.txt")).collect();
    let mut recorded = Vec::new();
    for name in &names {
        let key = object::key(name).unwrap();
        put(&storage, &b, &key, b"before").await;
        recorded.push(stored_row(&storage, &b, &key).await);
    }
    // Drift every key out of band: the recorded size no longer describes the
    // file, so a resolution heals that key's row — and only a resolution
    // does. The ETag is unaffected (nothing recomputes it here).
    let tampered = names
        .iter()
        .map(|name| {
            let path = root.path().join("data").join(name);
            let mut bytes = fs::read(&path).unwrap();
            bytes.extend_from_slice(b"-tampered");
            fs::write(&path, &bytes).unwrap();
            bytes.len() as u64
        })
        .collect::<Vec<u64>>();

    // The page resumes past the third key: the keys below the marker are
    // behind the cursor and must not be read, and the marker's own key is
    // skipped whole (a bare `key_marker` needs no entry of it).
    let page = page(
        &storage,
        &b,
        ListParams {
            key_marker: Some(names[2].clone()),
            max_keys: 10,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        listed(&page.entries),
        [
            (names[3].clone(), None),
            (names[4].clone(), None),
            (names[5].clone(), None),
        ],
        "the marker skips its own key and every key below it"
    );

    // The page's keys were resolved, so their rows now describe the file on
    // disk (the heal the read path's gate writes).
    for (name, size) in names[3..].iter().zip(&tampered[3..]) {
        let now = stored_row(&storage, &b, &object::key(name).unwrap()).await;
        assert_eq!(
            now.size, *size,
            "{name} is in the page, so the walk resolved and healed it"
        );
    }
    // ...and the keys below the marker kept the recording the drift left:
    // they were never opened.
    for index in 0..3 {
        let now = stored_row(&storage, &b, &object::key(&names[index]).unwrap()).await;
        assert_eq!(
            now, recorded[index],
            "{} is behind the marker, so nothing may have resolved it",
            names[index]
        );
    }
}

/// A rolled-up group is found from the members it needs, never by walking
/// all of them: the walk counts the group once, skips its remaining keys
/// without opening them, and jumps the group's whole key range on the next
/// round.
///
/// The group is forty members — more than two resolve pipes
/// ([`RESOLVE_CONCURRENCY`](crate::version::list::RESOLVE_CONCURRENCY))
/// wide while that stays below twenty — so "every member" and "a group's
/// worth of work" are far apart: the walk may open the pipe's depth of
/// members while the first one's answer is in flight — a bounded,
/// documented cost — and must leave the rest of the group untouched. The
/// drift-and-heal proof is the marker test's: only a resolution rewrites a
/// drifted row. The walk this test replaced resolved every key under the
/// prefix, all forty members included; the bound below
/// (`RESOLVE_CONCURRENCY` + 8, checked against the forty) still fails that
/// walk only while `RESOLVE_CONCURRENCY` stays below 32 — raise it past
/// either margin and this test's fixture, not its prose, is what has to
/// grow.
#[tokio::test]
async fn a_rolled_up_group_never_resolves_every_member() {
    let (root, storage, b) = fixture().await;
    let group: Vec<String> = (0..40).map(|i| format!("roll-{i:02}.txt")).collect();
    let tail = "tail.txt".to_owned();
    let names: Vec<String> = group.iter().cloned().chain([tail.clone()]).collect();
    for name in &names {
        let key = object::key(name).unwrap();
        put(&storage, &b, &key, format!("{name}!").as_bytes()).await;
    }
    let mut recorded = Vec::new();
    for name in &names {
        recorded.push(stored_row(&storage, &b, &object::key(name).unwrap()).await);
    }
    let tampered = names
        .iter()
        .map(|name| {
            let path = root.path().join("data").join(name);
            let mut bytes = fs::read(&path).unwrap();
            bytes.extend_from_slice(b"-tampered");
            fs::write(&path, &bytes).unwrap();
            bytes.len() as u64
        })
        .collect::<Vec<u64>>();

    let page = page(
        &storage,
        &b,
        ListParams {
            delimiter: Some("-".to_owned()),
            max_keys: 1000,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        page.common_prefixes,
        ["roll-"],
        "the forty members are one entry"
    );
    assert_eq!(listed(&page.entries), [(tail.clone(), None)]);

    // The first member's resolution is what told the walk the group yields,
    // and the group's remaining members are duplicates the page drops: the
    // walk may open the resolve pipe's worth of them (they were pushed while
    // the first one's answer was in flight — the pipe is one
    // `RESOLVE_CONCURRENCY` wide), never a member more.
    let mut resolved: Vec<&String> = Vec::new();
    for (index, name) in group.iter().enumerate() {
        let row = stored_row(&storage, &b, &object::key(name).unwrap()).await;
        if row.size == tampered[index] {
            resolved.push(name);
        } else {
            assert_eq!(
                row, recorded[index],
                "{name} must keep the row the drift left it"
            );
        }
    }
    assert_eq!(
        resolved.first().copied(),
        Some(&group[0]),
        "the group's first member is the one the walk reads it from"
    );
    // The bound follows the resolve pipe's own width, with a slot of slack
    // as headroom for a walk that fills the pipe across a window boundary
    // — never a re-spelled 16: raising `RESOLVE_CONCURRENCY` cannot fail a
    // walk that respects it, while a walk that opens every member — forty
    // of them — still fails here while `RESOLVE_CONCURRENCY` + 8 stays
    // under forty (below 32).
    let bound = RESOLVE_CONCURRENCY + 8;
    assert!(
        resolved.len() <= bound,
        "a rolled-up group costs a pipe's worth of resolutions, never one \
         per member: {} of {} members were opened (bound {bound})",
        resolved.len(),
        group.len()
    );
    // The plain key after the group is in the page and resolves normally.
    let tail_row = stored_row(&storage, &b, &object::key(&tail).unwrap()).await;
    assert_eq!(tail_row.size, tampered[tampered.len() - 1]);
}

/// The state flip is serialized with the write path: a
/// `set_bucket_versioning` call parks on the bucket mutation lock while a
/// writer holds it, so no write's state read can straddle the flip.
#[tokio::test]
async fn setting_the_versioning_state_waits_on_the_bucket_mutation_lock() {
    let (_root, storage, b) = fixture().await;

    let guard = storage.lock_bucket_mutations(&b).await;
    let setter = storage.clone();
    let set_bucket = b.clone();
    let set = tokio::spawn(async move {
        setter
            .set_bucket_versioning(&set_bucket, version::State::Enabled)
            .await
    });
    testutil::wait_for_lock_waiter().await;
    assert!(!set.is_finished(), "the flip waits on the lock");
    drop(guard);
    set.await.unwrap().unwrap();
    assert_eq!(
        storage.bucket_versioning(&b).await.unwrap(),
        version::State::Enabled
    );
}

/// The paging walk over one key with a version chain longer than a window's
/// per-key row bound (2026-10-06 review: the walk buffers at most
/// `max_keys + 3` rows of a key). Every version must still be enumerated
/// exactly once across the pages — including the pages that resume inside
/// the key, where the read streams from the marker down.
#[tokio::test]
async fn a_version_chain_longer_than_the_window_row_bound_still_pages_whole() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();

    // Thirty versions: far past the five rows a window may buffer at
    // `max_keys = 2`, so both the capped read and the streamed resume run.
    let mut expected = Vec::new();
    for index in 0..30u32 {
        let body = format!("v{index:02}");
        let id = put(&storage, &b, &key, body.as_bytes())
            .await
            .version_id
            .expect("an enabled write mints a version id");
        expected.push(Some(id.as_str().to_owned()));
    }
    expected.reverse(); // newest first

    let mut collected: Vec<Option<String>> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_marker: Option<version::IdMarker> = None;
    let mut pages = 0;
    loop {
        let page = page(
            &storage,
            &b,
            ListParams {
                key_marker: key_marker.clone(),
                version_id_marker: version_marker.clone(),
                max_keys: 2,
                ..Default::default()
            },
        )
        .await;
        pages += 1;
        assert!(pages < 40, "the walk must terminate");
        assert!(page.entries.len() <= 2, "a page never exceeds max_keys");
        collected.extend(
            page.entries
                .iter()
                .map(|entry| entry.id.as_ref().map(|id| id.as_str().to_owned())),
        );
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker.clone();
        version_marker = page.next_version_id_marker.clone().map(version::IdMarker);
        assert!(key_marker.is_some() && version_marker.is_some());
    }
    assert_eq!(pages, 15, "thirty versions at two per page");
    assert_eq!(collected, expected, "every version is listed exactly once");
}

/// The window's total row budget (2026-10-06 review): with more keys than
/// `WINDOW_KEY_ROWS` per-key caps allow, the walk leaves the key it cannot
/// read at its full cap to the next window — and still enumerates every
/// version exactly once, with every page within `max_keys`.
#[tokio::test]
async fn a_window_budget_that_skips_a_key_still_pages_every_version() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();

    // Four keys, six versions each: 20 rows against a window budget of
    // `3 * (2 + 3)` rows, so one window cannot hold all four keys.
    let mut expected: Vec<(String, Option<String>)> = Vec::new();
    for index in 0..4u32 {
        let key = object::key(format!("k{index}.txt")).unwrap();
        let mut ids = Vec::new();
        for version_index in 0..6u32 {
            let body = format!("v{index}-{version_index}");
            ids.push(
                put(&storage, &b, &key, body.as_bytes())
                    .await
                    .version_id
                    .expect("an enabled write mints a version id")
                    .as_str()
                    .to_owned(),
            );
        }
        ids.reverse(); // newest first
        for id in ids {
            expected.push((key.as_ref().to_owned(), Some(id)));
        }
    }

    let mut collected: Vec<(String, Option<String>)> = Vec::new();
    let mut key_marker: Option<String> = None;
    let mut version_marker: Option<version::IdMarker> = None;
    let mut pages = 0;
    loop {
        let page = page(
            &storage,
            &b,
            ListParams {
                key_marker: key_marker.clone(),
                version_id_marker: version_marker.clone(),
                max_keys: 2,
                ..Default::default()
            },
        )
        .await;
        pages += 1;
        assert!(pages < 40, "the walk must terminate");
        assert!(page.entries.len() <= 2, "a page never exceeds max_keys");
        collected.extend(page.entries.iter().map(|entry| {
            (
                entry.key.as_ref().to_owned(),
                entry.id.as_ref().map(|id| id.as_str().to_owned()),
            )
        }));
        if !page.is_truncated {
            break;
        }
        key_marker = page.next_key_marker.clone();
        version_marker = page.next_version_id_marker.clone().map(version::IdMarker);
        assert!(key_marker.is_some() && version_marker.is_some());
    }
    assert_eq!(pages, 12, "twenty-four versions at two per page");
    assert_eq!(collected, expected, "every version is listed exactly once");
}

/// RC1's pin (the wire behaviour FR-040 asserts): a **suspended PUT over a
/// real current version** replaces the null version in place. The write
/// succeeds, the answer carries the null identity (`x-amz-version-id: null`),
/// the object is served under it, and the version the slot moved off survives
/// addressable as history.
///
/// What this journey pins that no other does: the **incoming null's sequence
/// sits below the outgoing slot's**. The shared conformance leg's journeys
/// (`crates/tinio-util/src/testing/mod.rs`, the `versioned-journey` and
/// converge buckets) also overwrite a live real version while suspended, but
/// their keys already hold a superseded null row at 0, so their plan takes the
/// free sequence 2 over the slot's 1 and never reaches the guard's refusal
/// branch. Here the first write is enabled (no null row exists), so the plan's
/// `(Null, 0)` is *below* the recorded 1 — the shape RC1's store fix admits
/// and the pre-fix guard refused.
#[tokio::test]
async fn a_suspended_put_over_a_real_version_answers_the_null_identity() {
    let (_root, storage, _b) = fixture().await;
    let b = bucket::name("suspended-null").unwrap();
    storage
        .create_bucket(&b, None, &Acl::default_private(None))
        .await
        .unwrap();
    let key = object::key("k.txt").unwrap();
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let real = put(&storage, &b, &key, b"one").await;
    let real_id = real
        .version_id
        .clone()
        .expect("an enabled write mints an identity");

    storage
        .set_bucket_versioning(&b, version::State::Suspended)
        .await
        .unwrap();
    // The replacement over the live real version: the PUT must land, and its
    // answer is the null identity.
    let replaced = put(&storage, &b, &key, b"two").await;
    assert!(
        replaced.version_id.is_none(),
        "a suspended write answers the null version"
    );
    assert_eq!(replaced.etag, ETag::from_content(b"two"));

    // The key is served under the null identity, with the new bytes.
    let head = storage.head_object(&b, &key).await.unwrap();
    assert!(
        head.version_id.is_none(),
        "the served identity is the null version"
    );
    assert_eq!(head.etag, ETag::from_content(b"two"));
    let get = storage.get_object(&b, &key, None).await.unwrap();
    assert_eq!(collect_body(get.body).await.unwrap(), b"two");

    // No torn state: the slot records the null's own sequence (0 — free on
    // this key, no superseded null occupies it) and the version the slot moved
    // off is addressable history.
    let row = storage
        .meta_store()
        .stored_entry(&b, &key)
        .await
        .unwrap()
        .expect("the replacement recorded the row");
    assert!(row.version_id.is_none(), "the slot names the null version");
    assert_eq!(row.version_seq, 0, "the null's own sequence");
    assert!(!row.is_delete_marker);
    match storage
        .get_object_version(&b, &key, Some(&real_id), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(old) => {
            assert_eq!(collect_body(old.body).await.unwrap(), b"one");
        }
        version::ReadResult::Marker { .. } => panic!("the superseded version is content"),
    }
}

#[tokio::test]
async fn the_current_versions_tags_and_acl_are_the_object_rows_own() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let only = put(&storage, &b, &key, b"only")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // A current version has no version-table row (D37): its metadata home is
    // the `object_meta` row, so the version-addressed element writes land
    // there — the plain reads answer the same values, which is the cross-check
    // that the two paths touched one row.
    let tags = object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())]).unwrap();
    storage
        .set_version_tags(&b, &key, Some(&only), tags.clone())
        .await
        .unwrap();
    assert_eq!(
        storage.version_tags(&b, &key, Some(&only)).await.unwrap(),
        tags
    );
    assert_eq!(storage.get_object_tags(&b, &key).await.unwrap(), tags);

    // Replace-all on the grants, the row's owner element preserved (the write
    // records no owner, and an ACL put never changes one).
    let grants = vec![testutil::all_users_read_grant()];
    storage
        .set_version_acl(
            &b,
            &key,
            Some(&only),
            &Acl {
                owner: Some(testutil::owner_id()),
                grants: grants.clone(),
            },
        )
        .await
        .unwrap();
    let acl = storage.version_acl(&b, &key, Some(&only)).await.unwrap();
    assert_eq!(acl.grants, grants, "the grants replace");
    assert_eq!(acl.owner, None, "the row's owner element is untouched");
    assert_eq!(
        storage.get_object_acl(&b, &key).await.unwrap().grants,
        grants,
        "the plain ACL read agrees"
    );

    // Neither the slot nor the content moved with the element writes.
    assert_eq!(
        stored_row(&storage, &b, &key).await.version_id.as_ref(),
        Some(&only)
    );
    match storage
        .get_object_version(&b, &key, Some(&only), None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(got) => {
            assert_eq!(collect_body(got.body).await.unwrap(), b"only");
        }
        version::ReadResult::Marker { .. } => panic!("the current version is content"),
    }
}

#[tokio::test]
async fn a_live_marker_answers_the_stamp_its_own_row_records() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    put(&storage, &b, &key, b"first").await;

    let marker = storage
        .delete_object(&b, &key)
        .await
        .unwrap()
        .id
        .expect("an enabled bucket's marker carries an identity");
    // A **live** marker has no version-table row of its own: a current version
    // carries no row (D37), so the `object_meta` row the delete wrote is the
    // marker's only record — and the stamp the version-addressed read must
    // answer. (The version the marker superseded does have its own history
    // row; the marker does not.)
    assert!(
        version_rows(&storage, &b, &key)
            .iter()
            .all(|row| row.id() != Some(marker.clone())),
        "a live marker records no version-table row of its own"
    );
    let recorded = stored_row(&storage, &b, &key).await;
    assert!(recorded.is_delete_marker && recorded.version_id.as_ref() == Some(&marker));

    match storage
        .get_object_version(&b, &key, Some(&marker), None)
        .await
        .unwrap()
    {
        version::ReadResult::Marker { id, last_modified } => {
            assert_eq!(id.as_ref(), Some(&marker));
            assert_eq!(
                last_modified,
                from_nanos(recorded.mtime),
                "the marker's own recorded stamp, never a fresh one"
            );
        }
        version::ReadResult::Object(_) => panic!("a marker is not content"),
    }
    match storage
        .head_object_version(&b, &key, Some(&marker))
        .await
        .unwrap()
    {
        version::HeadResult::Marker { last_modified, .. } => assert_eq!(
            last_modified,
            from_nanos(recorded.mtime),
            "the head agrees with the get"
        ),
        version::HeadResult::Object(_) => panic!("a marker is not content"),
    }
}

#[tokio::test]
async fn a_null_identity_marker_answers_the_null_version_id() {
    let (_root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Suspended)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    assert!(
        put(&storage, &b, &key, b"only").await.version_id.is_none(),
        "a suspended write records the null version"
    );

    // A suspended DELETE makes the null-identity marker current (the design's
    // suspended rule): `versionId=null` addresses it, and the answer is a
    // **marker** carrying the null identity — never the object version's
    // identity, and never an error.
    let marker = storage.delete_object(&b, &key).await.unwrap();
    assert!(
        marker.is_delete_marker && marker.id.is_none(),
        "a suspended delete makes a null-identity marker current"
    );
    let recorded = stored_row(&storage, &b, &key).await;
    assert!(recorded.is_delete_marker && recorded.version_id.is_none());
    match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Marker { id, last_modified } => {
            assert!(
                id.is_none(),
                "the null-identity marker carries the null version id"
            );
            assert_eq!(
                last_modified,
                from_nanos(recorded.mtime),
                "the marker row's own stamp"
            );
        }
        version::ReadResult::Object(_) => panic!("the null-identity marker is not content"),
    }
    match storage.head_object_version(&b, &key, None).await.unwrap() {
        version::HeadResult::Marker { id, .. } => assert!(id.is_none(), "the head agrees"),
        version::HeadResult::Object(_) => panic!("the null-identity marker is not content"),
    }
    // The per-version metadata pairs refuse it, naming the null identity.
    assert!(
        storage.version_tags(&b, &key, None).await.is_err(),
        "a delete marker holds no tags"
    );
}

#[tokio::test]
async fn a_degraded_identity_never_comes_back_through_the_version_addressed_reads() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("notes.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The drift, then the null version's read: the gate's own heal serves the
    // edited bytes **and retires the stale identifier in the same write** —
    // before the engine's heal, which preserves whatever the row records.
    fs::write(root.path().join("data").join("notes.txt"), b"edited").unwrap();
    let served = match storage
        .get_object_version(&b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(got) => got,
        version::ReadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    };
    assert!(
        served.info.version_id.is_none(),
        "the degraded slot names no version"
    );
    assert!(
        stored_row(&storage, &b, &key).await.version_id.is_none(),
        "the identity the gate judged stale is retired by the healing read"
    );

    // The retired identity never comes back: the refreshed row now matches the
    // edited content, so a resolve that trusted the object row alone would
    // serve the drift under it.
    assert!(
        storage
            .get_object_version(&b, &key, Some(&old), None)
            .await
            .is_err(),
        "a retired identity must not resolve against the refreshed row"
    );
    assert!(
        storage
            .head_object_version(&b, &key, Some(&old))
            .await
            .is_err(),
        "the head takes the same verdict"
    );
}

/// The engine's versioned bodies read fs's **real** gate.
///
/// The `tinio-ops` suite drives these bodies over an in-memory row plane whose
/// `CurrentRows` port is hand-rolled (`TestMeta::current`, in that crate's
/// `testutil`), so nothing there exercises them against a backend's own gate
/// — the double answers for itself and can drift from both real
/// implementations. `tinio-mem` is not the other leg it looks like: mem's
/// whole version group is mem-local (the design's E16), so it has no
/// `CurrentRows` implementation for the engine to read and no engine versioned
/// body to run at all.
///
/// This leg is that missing run, against the one real gate that exists: it
/// calls the engine bodies **directly**, over the plane `FsStorage` builds for
/// its own version paths (`version_plane`, whose row handle is fs's `Rows` —
/// `current` applies `meta::gate`), and with no fs pre-step in front
/// (`retire_degraded_identity` is deliberately skipped, so what answers is the
/// engine body plus the gate rather than fs's adapter).
///
/// The drifted file is the discriminator: the row still records an identity
/// the bytes no longer match, so the gate's verdict is what turns the
/// addressed read into the null version. A plan that read the recorded
/// identity instead of the gate's answer would serve the stale id here.
///
/// Both bodies heal the row they read through the engine's preserving heal —
/// which keeps the recorded identity, the trap fs's pre-step exists for — so
/// the two legs run over two keys, each read while its row is still
/// stale-named.
#[tokio::test]
async fn the_engine_versioned_bodies_read_the_real_gate() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("drift.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints an identity");

    // Drift the file behind the backend's back. Nothing has read the key
    // since, so the row still records `old` and no heal has run.
    let drifted = b"drifted out of band, longer than the original";
    fs::write(root.path().join("data").join("drift.txt"), drifted).unwrap();
    assert_eq!(
        stored_row(&storage, &b, &key).await.version_id.as_ref(),
        Some(&old),
        "the row is still stale-named: the gate, not the row, is the truth"
    );

    let plane = storage.version_plane(&b);
    let engine = storage.objects();
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(
        slot.is_null() && slot.degraded,
        "the gate's slot for the drifted key: {slot:?}"
    );

    // The engine body, over that slot, serves the null version and the drifted
    // bytes — the identity the *row* records (`old`) is never served. The head
    // gets a key of its own: both bodies heal the row they read (preserving the
    // recorded identity), so the second leg must not run behind the first.
    match engine
        .get_object_version(&plane, &b, &key, None, None)
        .await
        .unwrap()
    {
        version::ReadResult::Object(served) => {
            assert!(
                served.info.version_id.is_none(),
                "the degraded slot names no version: {:?}",
                served.info.version_id
            );
            assert_eq!(collect_body(served.body).await.unwrap(), drifted);
        }
        version::ReadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    }

    // The head takes the same verdict from its own resolution, over its own
    // drifted key.
    let head_key = object::key("head-drift.txt").unwrap();
    put(&storage, &b, &head_key, b"first")
        .await
        .version_id
        .expect("an enabled write mints an identity");
    fs::write(root.path().join("data").join("head-drift.txt"), drifted).unwrap();
    match engine
        .head_object_version(&plane, &b, &head_key, None)
        .await
        .unwrap()
    {
        version::HeadResult::Object(info) => {
            assert!(info.version_id.is_none(), "{:?}", info.version_id);
        }
        version::HeadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    }
}

/// The **null-version address of a file-gone key whose row still records
/// content** — the corner the refactor report left unsettled: *which* layer
/// answers its `NoSuchKey`?
///
/// The state: an enabled bucket, one committed key, the file removed behind
/// the backend's back. The row still records the identity the write minted,
/// so the staleness gate (`meta::gate`) reads absent content against a row
/// that still described content as `Verdict::Degraded` — the slot
/// `Current { exists: true, version: None, degraded: true }`. The slot's
/// gated identity **is** the null version, which is the address
/// `get_object_version(None, ..)` carries.
///
/// The two layers, and the call chain that decides between them for this
/// input (fs's version-path trait impl, `crates/tinio-fs/src/backend/version.rs`):
///
/// 1. `retire_degraded_identity` (`:716`) takes the G3 pre-step's address
///    guard over the slot read (`slot`, `:737`): `id.as_ref() == version`
///    (`:723`). For a degraded slot `id` is `None`, so the null-version
///    address **matches**, and the pre-step runs.
/// 2. `objects::resolve_object_slot_stamped` (`objects.rs:496`) resolves the
///    object file first (`resolve_object_file`, `objects.rs:338` — the
///    `open_object` at `:349`), and a missing file answers `NoSuchKey`
///    there (`objects.rs:396`, the open's `NotFound` mapping).
///    **Nothing after it runs**: not the row read (`:512`), not the gate,
///    and not the heal (`ensure_row`, `:534`).
/// 3. So the engine body (`self.objects().get_object_version(&plane, ..)`,
///    `version.rs:1071`) — which answers the same state through
///    `require_content`'s content probe (`tinio_ops/src/object.rs:739`) and
///    would answer the same `NoSuchKey` — never runs.
///
/// What this test pins, and what it deliberately does not claim: the answer
/// alone cannot attribute the layer, because **the body's own answer for
/// this state is the same error** — the second half drives the body alone
/// over this storage's own plane, the pre-step bypassed, and asserts that it
/// is the very error the public call answered, wrapped by fs's projection.
/// The side-effect question the report left open ("the pre-step's path can
/// attempt a heal; the body's would not, or vice versa") is answered here:
/// **neither layer heals**, because both probe the content before any heal
/// call — `resolve_object_file` first in the pre-step, `require_content`
/// first in the body — so the row comes back byte-identical and no file is
/// re-materialized. The pin is therefore the whole observable contract of
/// this corner: the answer, its two-source indistinguishability, and the
/// untouched state.
///
/// The recorded-address leg is the contrast, and it is the observable that
/// shows the guard is what routes the null address to the pre-step: an
/// address the slot does **not** name (`Some(&old)`, while the slot's gated
/// identity is the null version) fails the guard, so the pre-step is skipped
/// and the body answers — and the body's answer for that address is
/// `NoSuchVersion` (the version table names no such version), not the
/// `NoSuchKey` the null address gets. The existing
/// `an_out_of_band_removal_degrades_the_key_to_the_null_version_and_retires_the_id`
/// asserts only `is_err()` for that address; this leg pins the variant the
/// body actually answers.
#[tokio::test]
async fn a_null_address_of_a_gone_file_answers_one_nosuchkey_from_either_layer_with_no_heal() {
    let (root, storage, b) = fixture().await;
    storage
        .set_bucket_versioning(&b, version::State::Enabled)
        .await
        .unwrap();
    let key = object::key("gone.txt").unwrap();
    let old = put(&storage, &b, &key, b"first")
        .await
        .version_id
        .expect("an enabled write mints a version id");

    // The removal, behind the backend's back: the row still describes the
    // content the file held, and nothing has read the key since.
    fs::remove_file(root.path().join("data").join("gone.txt")).unwrap();
    let before = stored_row(&storage, &b, &key).await;
    assert_eq!(
        before.version_id.as_ref(),
        Some(&old),
        "the premise: the row still records the identity"
    );
    assert_eq!(before.size, 5, "and the content elements it described");

    // The address is the null version: the gate reads the file-gone row as
    // the key's degraded null slot, whose gated identity the address names.
    let slot = storage.versions(&b).current(&b, &key).await.unwrap();
    assert!(
        slot.exists && slot.is_null() && slot.degraded && !slot.is_delete_marker,
        "the degraded null slot, the address's own guard: {slot:?}"
    );

    // The public answer, on the null address: the pre-step's file
    // resolution raises it before the engine body is called.
    let err = storage
        .get_object_version(&b, &key, None, None)
        .await
        .expect_err("the null-address read of a file-gone key");
    assert!(
        matches!(&err, crate::Error::Storage(storage::Error::NoSuchKey(k)) if k == &key),
        "the get answers NoSuchKey for the key: {err:?}"
    );
    let head_err = storage
        .head_object_version(&b, &key, None)
        .await
        .expect_err("the null-address head of a file-gone key");
    assert!(
        matches!(&head_err, crate::Error::Storage(storage::Error::NoSuchKey(k)) if k == &key),
        "the head agrees with the get: {head_err:?}"
    );

    // The side-effect half: the pre-step's path did not heal. Its file
    // resolution failed before `ensure_row`, so the row is byte-identical,
    // no version row was stamped, and the file was not re-materialized.
    assert_eq!(
        stored_row(&storage, &b, &key).await,
        before,
        "no heal: the row the write left is the row the answer left"
    );
    assert_eq!(
        version_rows(&storage, &b, &key).len(),
        0,
        "no version row was written"
    );
    assert!(
        !file_exists(&root, "gone.txt"),
        "the failed read re-materialized nothing"
    );

    // The layer question, as far as the surface reaches: the engine body
    // alone — this storage's own plane, no fs pre-step in front — answers
    // the *same* error for the same state, so the public answer carries no
    // layer information (and the body heals nothing either: its content
    // probe fails before its read-heal).
    let plane = storage.version_plane(&b);
    let body_err = storage
        .objects()
        .get_object_version(&plane, &b, &key, None, None)
        .await
        .expect_err("the engine body over the same state");
    assert!(
        matches!(&body_err, storage::Error::NoSuchKey(k) if k == &key),
        "the body's own answer for the null address: {body_err:?}"
    );
    let crate::Error::Storage(inner) = err else {
        panic!("the public answer is the storage class: {err:?}");
    };
    assert_eq!(
        format!("{inner:?}"),
        format!("{body_err:?}"),
        "the two layers answer one error: the answer cannot attribute the layer"
    );
    assert_eq!(
        stored_row(&storage, &b, &key).await,
        before,
        "and the body's own path heals nothing here either"
    );
    assert!(
        !file_exists(&root, "gone.txt"),
        "nor does it re-materialize the file"
    );

    // The contrast: an address the slot does not name (the recorded one)
    // fails the pre-step's guard, so the body answers it — with the version
    // table's verdict, not the content probe's.
    let recorded = storage
        .get_object_version(&b, &key, Some(&old), None)
        .await
        .expect_err("the recorded-address read of the file-gone key");
    assert!(
        matches!(&recorded, crate::Error::Storage(storage::Error::NoSuchVersion(id)) if id.as_str() == old.as_str()),
        "the guard's other arm, answered by the body: {recorded:?}"
    );
    assert_eq!(
        stored_row(&storage, &b, &key).await,
        before,
        "and that arm leaves the row alone too"
    );
}

#[path = "tests/object_lock.rs"]
mod object_lock_tests;
