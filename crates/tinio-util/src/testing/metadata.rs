//! The persistence contracts' conformance suite: the rules every backend that
//! implements [`tinio_core::meta`] and [`tinio_core::content`] must satisfy,
//! driven through the contracts' own methods only.
//!
//! Seven entry points — a backend calls the ones it implements:
//!
//! - [`assert_metadata_conformance`] — the aggregate: [`BucketMeta`],
//!   [`ObjectMeta`] and [`UploadMeta`] over a [`Metadata`] store: ordering,
//!   per-bucket boundaries, both traversal shapes (their healthy halves — the
//!   corrupt-row halves need raw access, D43), atomicity, drain idempotence,
//!   batch order, the refusals and their projections.
//! - [`assert_bucket_meta_conformance`] — [`BucketMeta`]'s legs alone: the
//!   bucket row's lifecycle.
//! - [`assert_object_meta_conformance`] — [`ObjectMeta`]'s legs alone: the
//!   point and batch writes, the retained part lists, the tag/ACL element
//!   writes, plus both traversals.
//! - [`assert_upload_meta_conformance`] — [`UploadMeta`]'s legs alone.
//! - [`assert_version_conformance`] — [`VersionMeta`]'s state machine, the
//!   version rows and the current slot ([`Metadata`] joins the bound because
//!   the slot writer itself probes the bucket row).
//! - [`assert_guarded_write_conformance`] — the two **opt-in** guarded writers
//!   ([`UploadMeta::create_guarded`], [`VersionMeta::set_bucket_state_guarded`]).
//!   Deliberately outside the aggregate: both are defaulted, and a backend that
//!   takes the default (a lazy-row or out-of-process store) would fail a leg it
//!   never claimed.
//! - [`assert_content_conformance`] — [`ObjectContent`]'s staged write and
//!   its visibility point, the ranged read, the move/copy/remove primitives,
//!   the content walk and the content-plane bucket lifecycle.
//!
//! The three per-category entry points — [`assert_bucket_meta_conformance`],
//! [`assert_object_meta_conformance`] and [`assert_upload_meta_conformance`]
//! — are the ones a category implementation's own task runs beside its module.
//!
//! A leg asserts behaviour, never a table, a row shape or a transaction
//! boundary: the same suite runs against redb, a filesystem tree, an in-memory
//! table and PostgreSQL. Two legs cannot be expressed here and belong to the
//! consumer with raw access to its own store (D43): a **corrupt row** (only the
//! backend can plant one) and a **heal write failure** (only the backend can
//! force one). Their named owners: P1 Task 6 Step 1
//! (`crates/tinio-store-redb/src/meta/object.rs`, which holds the raw table
//! handles — R7 moved the legs there), P2 Task 2 for `tinio-mem`, and the
//! pg spike's raw-SQL leg for PostgreSQL.
//!
//! The 8 MiB staged leg ([`assert_content_conformance`], D43) is deliberately
//! the largest: 128 chunks of 64 KiB, read back whole and through a window.

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::stream;

use super::{read_body, unique_bucket};
use crate::_core::{
    ETag,
    acl::{Acl, AclGrants, OwnerId},
    blob::{BlobRef, BlockHash},
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, PublishId, StagedObject, stage_body},
    cors,
    meta::{
        BucketMeta, Metadata, ObjectMeta, UploadMeta, bucket as bucket_row,
        object::{Identity, PartRow, Row},
        upload,
        version::{Current, Kind, Stored, VersionMeta},
    },
    multipart::{self, PartNumber},
    object,
    storage::{self, BodyStream, ByteRange},
    version,
};

/// The time every leg records: a whole second, so a backend whose clock
/// resolves to 100 ns stores it exactly.
const T0: Duration = Duration::from_secs(1_700_000_000);
/// A second distinction for the legs that need a *different* recorded time.
const T1: Duration = Duration::from_secs(1_700_000_001);

/// Two valid ETag literals (32 hex digits).
const ETAG_A: &str = "d41d8cd98f00b204e9800998ecf8427e";
const ETAG_B: &str = "0cc175b9c0f1b6a831c399e269772661";

fn t0() -> SystemTime {
    SystemTime::UNIX_EPOCH + T0
}

fn t1() -> SystemTime {
    SystemTime::UNIX_EPOCH + T1
}

fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid key literal")
}

fn bucket_name(prefix: &str) -> bucket::Name {
    bucket::name(unique_bucket(prefix)).expect("a valid bucket name")
}

fn part(number: u32) -> PartNumber {
    multipart::part_number(number).expect("a valid part number")
}

fn owner() -> OwnerId {
    // A canonical id is exactly 64 lowercase hex digits (acl::OwnerId::new);
    // a readable fixture name is not one, and every ACL leg panicked on it.
    OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
        .expect("a valid owner id")
}

/// A second canonical account, so an ACL leg can tell a replaced grant set
/// from the one it replaced.
fn other_owner() -> OwnerId {
    OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100")
        .expect("a valid owner id")
}

fn grants() -> AclGrants {
    Acl::default_private(Some(owner())).grants
}

fn tags(pairs: &[(&str, &str)]) -> object::Tags {
    object::Tags::from_pairs(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
    )
    .expect("valid tag pairs")
}

fn pairs(tags: &object::Tags) -> Vec<(String, String)> {
    tags.iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn etag(value: &str) -> ETag {
    ETag::new(value).expect("a valid ETag literal")
}

/// One decoded object row with the given content identity.
fn row(size: u64, tag: &str, identity: u64) -> Row {
    Row {
        etag: etag(tag),
        size,
        mtime: t0(),
        identity,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
    }
}

/// A fresh bucket row at `created`.
fn bucket_at(created: SystemTime) -> bucket_row::Row {
    bucket_row::Row::at(created)
}

/// One decoded version row: an object version of five bytes, or a delete
/// marker.
fn version_row(seq: u64, id: Option<version::Id>, kind: Kind) -> Stored {
    let size = if kind.is_marker() { 0 } else { 5 };
    Stored {
        id,
        blob: if kind.is_marker() {
            BlobRef::empty()
        } else {
            BlobRef::single(BlockHash::of(b"conf"), size)
        },
        size,
        etag: etag(ETAG_A),
        created: t0(),
        became_noncurrent: 0,
        kind,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        seq,
    }
}

/// Read a key through [`ObjectContent::open`], expecting bytes at every step.
async fn read<C>(
    store: &C,
    bucket: &bucket::Name,
    key: &ContentKey,
    range: Option<ByteRange>,
) -> Vec<u8>
where
    C: ObjectContent,
{
    let stream = store
        .open(bucket, key, range)
        .await
        .expect("open must not fail on a published key");
    read_body(stream).await.expect("the body stream reads")
}

/// Assert every rule the three metadata categories carry.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_metadata_conformance<M>(store: &M)
where
    M: Metadata,
{
    bucket_legs(store).await;
    object_legs(store).await;
    traversal_legs(store).await;
    upload_legs(store).await;
    refusal_legs(store).await;
}

/// Assert every rule [`BucketMeta`] carries — the bucket leg alone.
///
/// The per-category entry point a category implementation's own task runs
/// beside its module (plan 2026-10-03, ruling R7: the shared `contract.rs`
/// holds no per-task leg). The aggregate
/// [`assert_metadata_conformance`] remains the one full-suite call.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_bucket_meta_conformance<M>(store: &M)
where
    M: Metadata,
{
    bucket_legs(store).await;
}

/// Assert every rule [`ObjectMeta`] carries — the object legs alone (the
/// point and batch writes, the retained part lists, the tag/ACL element
/// writes) plus both traversals.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_object_meta_conformance<M>(store: &M)
where
    M: Metadata,
{
    object_legs(store).await;
    traversal_legs(store).await;
}

/// Assert every rule [`UploadMeta`] carries — the upload leg alone.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_upload_meta_conformance<M>(store: &M)
where
    M: Metadata,
{
    upload_legs(store).await;
}

/// Assert the two **opt-in** guarded writers: [`UploadMeta::create_guarded`]
/// and [`VersionMeta::set_bucket_state_guarded`], each re-checking the bucket
/// row inside the write transaction it commits.
///
/// They stay out of the aggregate suites on purpose. Both methods are
/// defaulted, and the default forwards to the unguarded write: a backend whose
/// bucket rows materialize lazily — or one that is not this process's redb at
/// all, the pg spike being the live example — takes the default and would fail
/// a leg it never claimed. A backend that overrides them (redb's own
/// implementation, and therefore tinio-mem and tinio-fs, which sit on it)
/// calls this beside the aggregate.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_guarded_write_conformance<S>(store: &S)
where
    S: Metadata + UploadMeta + VersionMeta,
{
    guarded_create_leg(store).await;
    guarded_state_leg(store).await;
}

/// Assert every rule [`VersionMeta`] carries.
///
/// The bound pairs the version plane with [`Metadata`] for one reason: the slot
/// writer is a guarded writer (it probes the bucket row inside its own write
/// transaction), so the legs plant the row a live bucket has before they drive
/// it. The guarded *state write* is not asserted here — it is opt-in and lives
/// in [`assert_guarded_write_conformance`].
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_version_conformance<V>(store: &V)
where
    V: Metadata + VersionMeta,
{
    let bucket = bucket_name("conf-version");
    let k = key("v.txt");
    let v1 = version::Id::new("v1").expect("a valid version id");
    let v2 = version::Id::new("v2").expect("a valid version id");
    // The bucket row every guarded writer probes. It is not a state row: the
    // unversioned leg below still sees a bucket with no versioning state.
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();

    // The state machine: a bucket with no versioning state row is
    // Unversioned (the bucket row planted above is not one), and the
    // transitions round-trip.
    assert_eq!(
        store.bucket_state(&bucket).await.unwrap(),
        version::State::Unversioned,
        "a bucket with no state row is Unversioned"
    );
    store
        .set_bucket_state(&bucket, version::State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        store.bucket_state(&bucket).await.unwrap(),
        version::State::Enabled
    );
    store
        .set_bucket_state(&bucket, version::State::Suspended)
        .await
        .unwrap();
    assert_eq!(
        store.bucket_state(&bucket).await.unwrap(),
        version::State::Suspended
    );

    assert!(
        store.current_slot(&bucket, &k).await.unwrap().is_none(),
        "a key with nothing recorded has no current slot"
    );

    // Three rows: a named object version, the null version, and a delete
    // marker.
    store
        .put_version(&bucket, &k, &version_row(1, Some(v1.clone()), Kind::Object))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(2, None, Kind::Object))
        .await
        .unwrap();
    store
        .put_version(&bucket, &k, &version_row(3, Some(v2.clone()), Kind::Marker))
        .await
        .unwrap();

    let first = store
        .get_version(&bucket, &k, 1)
        .await
        .unwrap()
        .expect("the row just written");
    assert_eq!(first.seq, 1);
    assert_eq!(first.id.as_ref(), Some(&v1));
    assert!(!first.kind.is_marker());
    assert_eq!(first.size, 5);

    let null = store
        .get_version(&bucket, &k, 2)
        .await
        .unwrap()
        .expect("the null version's row");
    assert_eq!(null.id, None, "the literal null is the null version");

    // The current slot can say all three states (review item 3).
    store
        .advance_slot(&bucket, &k, Some(v1.clone()), 1)
        .await
        .unwrap();
    assert_eq!(
        store.current_slot(&bucket, &k).await.unwrap(),
        Some(Current {
            exists: true,
            version: Some(v1.clone()),
            is_delete_marker: false,
        })
    );
    store.advance_slot(&bucket, &k, None, 2).await.unwrap();
    let slot = store
        .current_slot(&bucket, &k)
        .await
        .unwrap()
        .expect("the null version is a legitimate current");
    assert!(slot.is_null(), "the null version is a current with no id");
    store
        .advance_slot(&bucket, &k, Some(v2.clone()), 3)
        .await
        .unwrap();
    let slot = store
        .current_slot(&bucket, &k)
        .await
        .unwrap()
        .expect("a delete marker is a legitimate current");
    assert!(
        slot.is_delete_marker,
        "the current row's kind, not the slot, decides a marker"
    );

    // The index lookup answers by identity, including the null version's.
    assert_eq!(
        store.version_seq_of(&bucket, &k, Some(&v1)).await.unwrap(),
        Some(1)
    );
    assert_eq!(
        store.version_seq_of(&bucket, &k, Some(&v2)).await.unwrap(),
        Some(3)
    );
    assert_eq!(
        store.version_seq_of(&bucket, &k, None).await.unwrap(),
        Some(2),
        "the null version indexes too"
    );
    let unknown = version::Id::new("never-written").expect("a valid version id");
    assert_eq!(
        store
            .version_seq_of(&bucket, &k, Some(&unknown))
            .await
            .unwrap(),
        None
    );

    // The listing is newest first.
    let listed = store.list_versions(&bucket, &k).await.unwrap();
    assert_eq!(
        listed.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![3, 2, 1],
        "list_versions is newest first"
    );

    // Expiring a row is idempotent and takes its index entry with it.
    store.expire(&bucket, &k, 2).await.unwrap();
    store.expire(&bucket, &k, 2).await.unwrap();
    assert!(
        store.get_version(&bucket, &k, 2).await.unwrap().is_none(),
        "expire removes the row"
    );
    assert_eq!(
        store.version_seq_of(&bucket, &k, None).await.unwrap(),
        None,
        "…and its index entry, in the same transaction"
    );
    assert_eq!(store.list_versions(&bucket, &k).await.unwrap().len(), 2);

    // A slot advanced to a sequence with no version row is still a recorded
    // slot: the identity is the recorded fact and a missing row is not a
    // marker (the `current_slot` rule).
    let dangling = version::Id::new("dangling").expect("a valid version id");
    store
        .advance_slot(&bucket, &k, Some(dangling.clone()), 9)
        .await
        .unwrap();
    assert_eq!(
        store.current_slot(&bucket, &k).await.unwrap(),
        Some(Current {
            exists: true,
            version: Some(dangling),
            is_delete_marker: false,
        }),
        "a slot with no version row at its sequence reads back as a recorded slot"
    );

    // The engine's two row reads, on a second key so these legs do not depend
    // on the state above.
    let k2 = key("newest.txt");
    assert!(
        store.latest(&bucket, &k2).await.unwrap().is_none(),
        "a key with no row has no newest row"
    );
    store
        .put_version(
            &bucket,
            &k2,
            &version_row(1, Some(v1.clone()), Kind::Object),
        )
        .await
        .unwrap();
    store
        .put_version(&bucket, &k2, &version_row(2, None, Kind::Object))
        .await
        .unwrap();
    let (seq, newest) = store
        .latest(&bucket, &k2)
        .await
        .unwrap()
        .expect("the newest row");
    assert_eq!(seq, 2, "latest answers the key's highest sequence");
    assert_eq!(newest.id, None, "…and that row, the null version here");
    assert_eq!(newest.seq, 2, "a decoded row carries its own sequence");

    // The row-overwrite guard: the row at a sequence may only be rewritten by
    // the same version. A different content address at that sequence is
    // another version's committed row — refused, never overwritten, because
    // replacing it would drop that version's blob out of the live set.
    let mut clash = version_row(1, Some(v1.clone()), Kind::Object);
    clash.blob = BlobRef::single(BlockHash::of(b"other"), 9);
    clash.size = 9;
    assert!(
        store.put_version(&bucket, &k2, &clash).await.is_err(),
        "a row naming different content at the same sequence is refused"
    );
    let intact = store
        .get_version(&bucket, &k2, 1)
        .await
        .unwrap()
        .expect("the refused row is still there");
    assert_eq!(
        (intact.size, intact.blob),
        (5, BlobRef::single(BlockHash::of(b"conf"), 5)),
        "the refusal overwrote nothing"
    );
    // The same version's own row is the crash-window adoption path, not a
    // conflict.
    store
        .put_version(
            &bucket,
            &k2,
            &version_row(1, Some(v1.clone()), Kind::Object),
        )
        .await
        .expect("the same version's row is rewritten");

    // The one-null-version rule: committing the null identity displaces the
    // row the index's single null entry named — with that entry, in the same
    // transaction — so no null-id row is ever stranded unaddressable.
    store
        .put_version(&bucket, &k2, &version_row(4, None, Kind::Object))
        .await
        .unwrap();
    assert_eq!(
        store.version_seq_of(&bucket, &k2, None).await.unwrap(),
        Some(4),
        "the index's null entry names the row just committed"
    );
    assert!(
        store.get_version(&bucket, &k2, 2).await.unwrap().is_none(),
        "the older null-id row was displaced"
    );
    let mut remaining = store
        .list_versions(&bucket, &k2)
        .await
        .unwrap()
        .iter()
        .map(|row| row.seq)
        .collect::<Vec<_>>();
    remaining.sort_unstable();
    assert_eq!(remaining, vec![1, 4], "exactly one null-id row remains");

    // The bucket-wide healing walk: every decodable row of this bucket, and
    // nothing of another.
    let mut seqs = store
        .bucket_rows(&bucket)
        .await
        .unwrap()
        .iter()
        .map(|row| row.seq)
        .collect::<Vec<_>>();
    seqs.sort_unstable();
    assert_eq!(
        seqs,
        vec![1, 1, 3, 4],
        "bucket_rows walks every key's decodable rows"
    );
    assert!(
        store
            .bucket_rows(&bucket_name("conf-version-empty"))
            .await
            .unwrap()
            .is_empty(),
        "a bucket with no version row has no bucket rows"
    );

    // A batched retirement: one call takes the whole listed history with its
    // index entries, and re-running it is a no-op (the sweep re-runs after a
    // crash). A key's rows of *other* keys are untouched.
    store.expire_many(&bucket, &k2, &[1, 4]).await.unwrap();
    store.expire_many(&bucket, &k2, &[1, 4]).await.unwrap();
    assert!(
        store.list_versions(&bucket, &k2).await.unwrap().is_empty(),
        "expire_many removes the whole listed history"
    );
    assert_eq!(
        store.version_seq_of(&bucket, &k2, Some(&v1)).await.unwrap(),
        None,
        "…and its index entries, in the same transaction"
    );
    let mut kept = store
        .bucket_rows(&bucket)
        .await
        .unwrap()
        .iter()
        .map(|row| row.seq)
        .collect::<Vec<_>>();
    kept.sort_unstable();
    assert_eq!(kept, vec![1, 3], "the batch named only one key's sequences");

    // The bucket-wide drain: one call takes every key's version rows with their
    // index entries, another bucket's rows are untouched, and re-running it is
    // a no-op (a teardown re-runs after a crash).
    let k3 = key("drain.txt");
    store
        .put_version(
            &bucket,
            &k3,
            &version_row(1, Some(v1.clone()), Kind::Object),
        )
        .await
        .unwrap();
    store
        .put_version(
            &bucket,
            &k3,
            &version_row(2, Some(v2.clone()), Kind::Object),
        )
        .await
        .unwrap();
    let other = bucket_name("conf-version-other");
    // The version writers probe the bucket row like every other guarded
    // writer, so the legs plant it first.
    BucketMeta::create(store, &other, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .put_version(&other, &k3, &version_row(1, Some(v1.clone()), Kind::Object))
        .await
        .unwrap();

    VersionMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(
        store.bucket_rows(&bucket).await.unwrap().is_empty(),
        "drain_bucket removes every key's version rows"
    );
    assert!(
        store.list_versions(&bucket, &k).await.unwrap().is_empty(),
        "…the first key's history too"
    );
    assert!(
        store.get_version(&bucket, &k, 1).await.unwrap().is_none(),
        "…the rows are gone"
    );
    assert_eq!(
        store.version_seq_of(&bucket, &k, Some(&v1)).await.unwrap(),
        None,
        "…and their index entries, in the same transaction"
    );
    assert_eq!(
        store.bucket_rows(&other).await.unwrap().len(),
        1,
        "another bucket's rows are untouched"
    );

    // Idempotent: the second call removes nothing and reports success.
    VersionMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(
        store.bucket_rows(&bucket).await.unwrap().is_empty(),
        "a second drain of a bucket with no rows is a no-op"
    );
}

/// The guarded upload create's leg (see [`assert_guarded_write_conformance`]).
async fn guarded_create_leg<M>(store: &M)
where
    M: Metadata + UploadMeta,
{
    // The guarded create: the bucket row is checked inside the writing
    // transaction, so a create cannot be raced by a `remove_if_empty` (the
    // TOCTOU-free upsert). A bucket with no row refuses and projects the
    // refusal without writing an upload row; one with a row lands and reads
    // back.
    let target = key("conf-upload-guarded.txt");
    let guarded = bucket_name("conf-upload-guarded");
    let absent = bucket_name("conf-upload-guarded-absent");
    let state = upload::Row {
        upload_id: "conf-upload-guarded".to_owned(),
        key: target.clone(),
        initiated_at: t0(),
        checksum: None,
        tags: object::Tags::empty(),
        owner: None,
        acl: Acl::default_private(None),
    };
    let refused = match store
        .create_guarded(&absent, "conf-upload-guarded", &state)
        .await
    {
        Ok(()) => panic!("create_guarded must refuse a bucket with no row"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "create_guarded's refusal must project to NoSuchBucket, got {refused:?}"
    );
    assert!(
        UploadMeta::get(store, &absent, &target, "conf-upload-guarded")
            .await
            .unwrap()
            .is_none(),
        "the refused guarded create wrote no upload row"
    );
    BucketMeta::create(store, &guarded, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .create_guarded(&guarded, "conf-upload-guarded", &state)
        .await
        .unwrap();
    assert_eq!(
        UploadMeta::get(store, &guarded, &target, "conf-upload-guarded")
            .await
            .unwrap()
            .expect("the guarded create just landed")
            .upload_id,
        "conf-upload-guarded"
    );
    UploadMeta::drain_bucket(store, &guarded).await.unwrap();
    BucketMeta::remove(store, &guarded).await.unwrap();
}

/// The guarded version-state write's leg (see
/// [`assert_guarded_write_conformance`]).
async fn guarded_state_leg<V>(store: &V)
where
    V: Metadata + VersionMeta,
{
    // The guarded state write: the bucket row is checked inside the writing
    // transaction, so the write cannot be raced by a `remove_if_empty` (the
    // TOCTOU-free half of the bucket half). A bucket with no row refuses and
    // projects the refusal without writing a state row; one with a row lands
    // and reads back.
    let guarded = bucket_name("conf-version-guarded");
    let absent = bucket_name("conf-version-guarded-absent");
    let refused = match store
        .set_bucket_state_guarded(&absent, version::State::Enabled)
        .await
    {
        Ok(()) => panic!("set_bucket_state_guarded must refuse a bucket with no row"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "set_bucket_state_guarded's refusal must project to NoSuchBucket, got {refused:?}"
    );
    assert_eq!(
        store.bucket_state(&absent).await.unwrap(),
        version::State::Unversioned,
        "the refused guarded write wrote no state row"
    );
    BucketMeta::create(store, &guarded, &bucket_at(t0()))
        .await
        .unwrap();
    store
        .set_bucket_state_guarded(&guarded, version::State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        store.bucket_state(&guarded).await.unwrap(),
        version::State::Enabled,
        "the guarded write on a live bucket lands"
    );
    BucketMeta::remove(store, &guarded).await.unwrap();
}

/// Assert every rule [`ObjectContent`] carries.
///
/// # Panics
///
/// On the first violated rule; the message names the rule.
pub async fn assert_content_conformance<C>(store: &C)
where
    C: ObjectContent,
{
    let bucket = bucket_name("conf-content");
    store.create_bucket(&bucket).await.unwrap();
    assert!(
        store.is_empty(&bucket).await.unwrap(),
        "a fresh content bucket is empty"
    );

    let a = key("a.bin");
    let a_key = ContentKey::Object(a.clone());
    assert!(
        store.stat(&bucket, &a).await.unwrap().is_none(),
        "a key with no bytes has no entry"
    );
    assert!(
        store.open(&bucket, &a_key, None).await.is_err(),
        "a missing key is an error: D23's open has no None arm"
    );

    // Publish is the visibility point: the staged write must not be readable
    // before it, and must be readable after it. The body streams through
    // [`stage_body`] — the streaming stage a caller that must not hold the
    // object whole uses (the engine's materialize) — and its answer is the
    // **landed** byte count, never the requested length.
    let mut staged = store.stage(&bucket, &a_key).await.unwrap();
    let body: BodyStream = Box::pin(stream::iter([
        Ok(Bytes::from_static(b"hel")),
        Ok(Bytes::from_static(b"lo")),
    ]));
    let landed = stage_body(&mut staged, body).await.unwrap();
    assert_eq!(landed, 5, "stage_body answers the bytes that landed");
    assert!(
        store.open(&bucket, &a_key, None).await.is_err(),
        "a staged write is invisible before publish"
    );
    store.publish(&bucket, staged, &a_key).await.unwrap();
    assert!(
        store.open(&bucket, &a_key, None).await.is_ok(),
        "publish makes the bytes visible"
    );

    let entry = store
        .stat(&bucket, &a)
        .await
        .unwrap()
        .expect("the published object's entry");
    assert_eq!(entry.len, 5);
    assert_eq!(entry.key, a, "stat's entry names the key it was asked for");
    assert!(
        !store.is_empty(&bucket).await.unwrap(),
        "published bytes are content"
    );

    // The range semantics: inclusive, open-ended and suffix.
    assert_eq!(read(store, &bucket, &a_key, None).await, b"hello");
    assert_eq!(
        read(store, &bucket, &a_key, Some(ByteRange::Inclusive(1, 3))).await,
        b"ell",
        "an inclusive range starts at its first byte"
    );
    assert_eq!(
        read(store, &bucket, &a_key, Some(ByteRange::From(3))).await,
        b"lo",
        "an open-ended range runs to the end"
    );
    assert_eq!(
        read(store, &bucket, &a_key, Some(ByteRange::Suffix(2))).await,
        b"lo",
        "a suffix range is the last N bytes"
    );

    // A re-publish replaces the bytes.
    let mut again = store.stage(&bucket, &a_key).await.unwrap();
    again.write(Bytes::from_static(b"HELLO")).await.unwrap();
    store.publish(&bucket, again, &a_key).await.unwrap();
    assert_eq!(
        read(store, &bucket, &a_key, None).await,
        b"HELLO",
        "a second publish replaces the first"
    );

    // The part key shape addresses staged part bytes.
    let part_key = ContentKey::Part {
        upload_id: "6f1d0f4a-5c2e-4b8a-9f31-2d7c9e5a1b40".to_owned(),
        number: part(2),
    };
    let mut staged = store.stage(&bucket, &part_key).await.unwrap();
    staged
        .write(Bytes::from_static(b"part-bytes"))
        .await
        .unwrap();
    store.publish(&bucket, staged, &part_key).await.unwrap();
    assert_eq!(
        read(store, &bucket, &part_key, None).await,
        b"part-bytes",
        "a part key addresses the upload's part bytes"
    );

    // Copy duplicates and rename moves.
    let b = key("b.bin");
    let b_key = ContentKey::Object(b.clone());
    store.copy(&bucket, &a, &b).await.unwrap();
    assert_eq!(
        read(store, &bucket, &b_key, None).await,
        b"HELLO",
        "copy duplicates the bytes"
    );
    let c = key("c.bin");
    let c_key = ContentKey::Object(c.clone());
    store.rename(&bucket, &b, &c).await.unwrap();
    assert!(
        store.open(&bucket, &b_key, None).await.is_err(),
        "rename leaves no source bytes"
    );
    assert_eq!(
        read(store, &bucket, &c_key, None).await,
        b"HELLO",
        "rename moves the bytes"
    );

    // A self-move is a no-op (R14): the bytes stay where they are. A
    // read-then-write-then-remove implementation deletes the value it just
    // wrote, so the two calls below are the leg that catches it.
    store.rename(&bucket, &c, &c).await.unwrap();
    assert_eq!(
        read(store, &bucket, &c_key, None).await,
        b"HELLO",
        "a self-rename leaves the bytes where they are"
    );
    store.copy(&bucket, &c, &c).await.unwrap();
    assert_eq!(
        read(store, &bucket, &c_key, None).await,
        b"HELLO",
        "so does a self-copy"
    );

    // Remove is idempotent.
    store.remove(&bucket, &c_key).await.unwrap();
    store.remove(&bucket, &c_key).await.unwrap();
    assert!(store.open(&bucket, &c_key, None).await.is_err());

    // A batch removal (D23's byte plane): every key in one call, idempotent,
    // and an empty batch removes nothing. The default arm loops `remove`; a
    // backend whose bytes share one store commits the batch together.
    let batch_a = ContentKey::Object(key("batch-a.bin"));
    let batch_b = ContentKey::Object(key("batch-b.bin"));
    let batch_part = ContentKey::Part {
        upload_id: "0c3b7d11-8a4e-4c62-b0d5-7f2e91a4c508".to_owned(),
        number: part(1),
    };
    let mut staged = store.stage(&bucket, &batch_a).await.unwrap();
    staged.write(Bytes::from_static(b"batch-a")).await.unwrap();
    store.publish(&bucket, staged, &batch_a).await.unwrap();
    let mut staged = store.stage(&bucket, &batch_b).await.unwrap();
    staged.write(Bytes::from_static(b"batch-b")).await.unwrap();
    store.publish(&bucket, staged, &batch_b).await.unwrap();
    let mut staged = store.stage(&bucket, &batch_part).await.unwrap();
    staged
        .write(Bytes::from_static(b"batch-part"))
        .await
        .unwrap();
    store.publish(&bucket, staged, &batch_part).await.unwrap();
    store.remove_many(&bucket, &[]).await.unwrap();
    assert!(
        store.open(&bucket, &batch_a, None).await.is_ok(),
        "an empty batch removes nothing"
    );
    let batch = [batch_a.clone(), batch_b.clone(), batch_part.clone()];
    store.remove_many(&bucket, &batch).await.unwrap();
    for target in &batch {
        assert!(
            store.open(&bucket, target, None).await.is_err(),
            "remove_many removes every key it was given"
        );
    }
    store.remove_many(&bucket, &batch).await.unwrap();

    // A conditional removal: the compensation of a write whose row write was
    // refused must not delete bytes a later writer published in its place.
    let guarded = ContentKey::Object(key("guarded.bin"));
    let mut staged = store.stage(&bucket, &guarded).await.unwrap();
    staged.write(Bytes::from_static(b"mine")).await.unwrap();
    store.publish(&bucket, staged, &guarded).await.unwrap();
    assert!(
        !store
            .remove_if_matches(&bucket, &guarded, b"")
            .await
            .unwrap(),
        "an empty expectation never matches bytes that exist"
    );
    assert!(
        !store
            .remove_if_matches(&bucket, &guarded, b"theirs")
            .await
            .unwrap(),
        "different bytes are left alone"
    );
    assert_eq!(
        read(store, &bucket, &guarded, None).await,
        b"mine",
        "…and the mismatch removed nothing"
    );
    assert!(
        store
            .remove_if_matches(&bucket, &guarded, b"mine")
            .await
            .unwrap(),
        "the bytes the caller published are removed"
    );
    assert!(
        store.open(&bucket, &guarded, None).await.is_err(),
        "…and they are gone"
    );
    assert!(
        !store
            .remove_if_matches(&bucket, &guarded, b"mine")
            .await
            .unwrap(),
        "absent bytes answer false, never an error"
    );

    // A tagged publish carries the identity a compensation matches: every
    // plane answers the live id's removal (a plane that records the tag
    // compares it, a plane that keeps none falls back to the bytes — its
    // stale-id blind spot is the trait's documented default, and the
    // recording plane's own suite leg lives with its decorator).
    let tagged = ContentKey::Object(key("tagged.bin"));
    let tag = PublishId::new(1);
    let mut staged = store.stage(&bucket, &tagged).await.unwrap();
    staged.write(Bytes::from_static(b"tagged")).await.unwrap();
    store
        .publish_tagged(&bucket, staged, &tagged, tag)
        .await
        .unwrap();
    assert!(
        store
            .remove_if_tagged(&bucket, &tagged, b"tagged", tag)
            .await
            .unwrap(),
        "the live publish's own id removes its bytes"
    );
    assert!(
        store.open(&bucket, &tagged, None).await.is_err(),
        "…and they are gone"
    );
    assert!(
        !store
            .remove_if_tagged(&bucket, &tagged, b"tagged", tag)
            .await
            .unwrap(),
        "an already-spent tag answers false, never an error"
    );

    // Part bytes count as content too.
    assert!(
        !store.is_empty(&bucket).await.unwrap(),
        "a staged part's bytes are content"
    );
    store.remove(&bucket, &a_key).await.unwrap();
    store.remove(&bucket, &part_key).await.unwrap();
    assert!(
        store.is_empty(&bucket).await.unwrap(),
        "with every object and part gone the bucket is empty"
    );
    store.remove_bucket(&bucket).await.unwrap();
    store.remove_bucket(&bucket).await.unwrap();

    // A directory-only bucket is not empty: a folder marker is content even
    // though it carries no bytes (design §1 property 9 — for fs the marker is
    // a directory with no metadata row, and `is_empty` must still see it).
    // The leg names its own bucket: the marker is its only content and, being
    // a directory for fs, is not removable through this plane uniformly, so
    // the bucket stands.
    let dir = bucket_name("conf-dir");
    store.create_bucket(&dir).await.unwrap();
    let marker = ContentKey::Object(key("dir/"));
    let staged = store.stage(&dir, &marker).await.unwrap();
    store.publish(&dir, staged, &marker).await.unwrap();
    assert!(
        !store.is_empty(&dir).await.unwrap(),
        "a bucket holding only a folder marker is not empty"
    );

    scan_legs(store).await;
    staged_leg(store).await;
}

/// The content walk: byte ordering, the prefix bound, the exclusivity of
/// `after`, the page cap, and object keys only.
async fn scan_legs<C>(store: &C)
where
    C: ObjectContent,
{
    let bucket = bucket_name("conf-scan");
    store.create_bucket(&bucket).await.unwrap();
    let names = ["alpha", "alpha-1", "alpine", "beta"];
    for name in names {
        let k = ContentKey::Object(key(name));
        let mut staged = store.stage(&bucket, &k).await.unwrap();
        staged.write(Bytes::from_static(b"x")).await.unwrap();
        store.publish(&bucket, staged, &k).await.unwrap();
    }

    // The scanned bucket holds a **part key** too, inside the walked prefix:
    // property 3 says `scan` yields object keys only, so an implementation
    // that enumerated the part plane would add an entry to the exact lists
    // asserted below. The part's bytes are proved present first, so the
    // exclusion is real (review item 6).
    let part_key = ContentKey::Part {
        upload_id: "7a2e1c3d-8b4f-4a6e-b0c1-3e5d9f2a7c48".to_owned(),
        number: part(1),
    };
    let mut staged = store.stage(&bucket, &part_key).await.unwrap();
    staged.write(Bytes::from_static(b"part")).await.unwrap();
    store.publish(&bucket, staged, &part_key).await.unwrap();
    assert_eq!(
        read(store, &bucket, &part_key, None).await,
        b"part",
        "the part key's bytes live in this very bucket"
    );

    assert_eq!(
        walked(store, &bucket, "", None, 100).await,
        vec![key("alpha"), key("alpha-1"), key("alpine"), key("beta")],
        "scan is ascending by the key's UTF-8 bytes"
    );
    assert_eq!(
        walked(store, &bucket, "alpha", None, 100).await,
        vec![key("alpha"), key("alpha-1")],
        "prefix is a bound the implementation must not scan past (alpine is outside it)"
    );
    let after = key("alpha");
    assert_eq!(
        walked(store, &bucket, "alpha", Some(&after), 100).await,
        vec![key("alpha-1")],
        "after is exclusive"
    );
    assert_eq!(
        store.scan(&bucket, "", None, 2).await.unwrap().len(),
        2,
        "limit caps the page"
    );
    assert!(
        store.scan(&bucket, "", None, 0).await.unwrap().is_empty(),
        "a zero limit is an empty page"
    );

    // B1's inputs reach both reads: the scanned entry and the point probe
    // agree on the identity and the timestamp (a store with neither answers
    // the same `0`/`None` on both).
    let scanned = store.scan(&bucket, "alpha", None, 100).await.unwrap();
    let stat = store
        .stat(&bucket, &key("alpha"))
        .await
        .unwrap()
        .expect("the published object's entry");
    let entry = scanned
        .iter()
        .find(|entry| entry.key.as_ref() == "alpha")
        .expect("alpha is in the scanned page");
    assert_eq!(entry.len, stat.len, "scan and stat agree on the length");
    assert_eq!(
        entry.identity, stat.identity,
        "scan and stat agree on the file identity (B1)"
    );
    assert_eq!(
        entry.mtime, stat.mtime,
        "scan and stat agree on the timestamp (B1)"
    );

    // `after` is a **filter**, not a walk start: a value sorting below the
    // prefix must not cut the page short, even when a non-matching key sorts
    // between the two bounds. S3's `start-after` is any client key, so this
    // is the reachable case the legs above never exercise.
    let probe = key("al");
    let probe_key = ContentKey::Object(probe.clone());
    let mut staged = store.stage(&bucket, &probe_key).await.unwrap();
    staged.write(Bytes::from_static(b"x")).await.unwrap();
    store.publish(&bucket, staged, &probe_key).await.unwrap();
    let below = key("a");
    assert_eq!(
        walked(store, &bucket, "alpha", Some(&below), 100).await,
        vec![key("alpha"), key("alpha-1")],
        "an `after` below the prefix cannot hide the prefix's page (`al` sorts inside the gap)"
    );
    store.remove(&bucket, &probe_key).await.unwrap();

    for name in names {
        store
            .remove(&bucket, &ContentKey::Object(key(name)))
            .await
            .unwrap();
    }
    store.remove(&bucket, &part_key).await.unwrap();
    assert!(store.is_empty(&bucket).await.unwrap());
    store.remove_bucket(&bucket).await.unwrap();
}

/// The walked object keys of one `scan`. Every call site asserts the
/// **exact** list, so a stray part-key entry (the scanned bucket holds one,
/// `scan_legs`) breaks the equality — that is the property 3 proof.
async fn walked<C>(
    store: &C,
    bucket: &bucket::Name,
    prefix: &str,
    after: Option<&object::Key>,
    limit: usize,
) -> Vec<object::Key>
where
    C: ObjectContent,
{
    let entries: Vec<ContentEntry> = store.scan(bucket, prefix, after, limit).await.unwrap();
    entries.into_iter().map(|entry| entry.key).collect()
}

/// The 8 MiB staged leg (D43): 128 chunks of 64 KiB, read whole and through a
/// window.
async fn staged_leg<C>(store: &C)
where
    C: ObjectContent,
{
    const CHUNK: usize = 64 * 1024;
    const CHUNKS: usize = 128;

    let bucket = bucket_name("conf-staged");
    store.create_bucket(&bucket).await.unwrap();
    let big = key("big/staged-8m.bin");
    let big_key = ContentKey::Object(big.clone());

    let mut staged = store.stage(&bucket, &big_key).await.unwrap();
    for index in 0..CHUNKS {
        staged
            .write(Bytes::from(chunk(index, CHUNK)))
            .await
            .unwrap();
    }
    store.publish(&bucket, staged, &big_key).await.unwrap();

    let expected: Vec<u8> = (0..CHUNKS).flat_map(|index| chunk(index, CHUNK)).collect();
    assert_eq!(expected.len(), CHUNK * CHUNKS, "the leg is 8 MiB");

    let whole = read(store, &bucket, &big_key, None).await;
    assert_eq!(whole.len(), expected.len(), "the whole 8 MiB reads back");
    assert_eq!(whole, expected, "byte for byte, across the chunk seams");

    let entry = store
        .stat(&bucket, &big)
        .await
        .unwrap()
        .expect("the big object's entry");
    assert_eq!(entry.len, (CHUNK * CHUNKS) as u64);

    // `Inclusive(a, b)` is the closed `a..=b` form (`storage::range`): the
    // window is `b - a + 1` = 4097 bytes, one more than the difference, so the
    // expectation slices through `start + 4096` inclusive.
    let start = CHUNK + 7;
    let window = read(
        store,
        &bucket,
        &big_key,
        Some(ByteRange::Inclusive(start as u64, (start + 4096) as u64)),
    )
    .await;
    assert_eq!(
        window,
        &expected[start..=start + 4096],
        "a ranged read inside the object agrees with the whole read (A..=B)"
    );

    // The tail, where the final chunk boundary is.
    let tail = read(store, &bucket, &big_key, Some(ByteRange::Suffix(3))).await;
    assert_eq!(tail, &expected[expected.len() - 3..]);

    store.remove(&bucket, &big_key).await.unwrap();
    assert!(store.is_empty(&bucket).await.unwrap());
    store.remove_bucket(&bucket).await.unwrap();
}

/// One deterministic chunk: byte `j` of chunk `i` is `(i + j) & 0xFF`, so
/// a seam that repeats or drops a chunk changes the read.
fn chunk(index: usize, len: usize) -> Vec<u8> {
    (0..len).map(|offset| (index + offset) as u8).collect()
}

/// The bucket row's lifecycle: create-or-refuse, the existence-conditioned
/// setters, the upserts, batch loads, the ordered listing and the removal.
async fn bucket_legs<M>(store: &M)
where
    M: Metadata,
{
    let name = bucket_name("conf-bucket");
    assert!(
        store.created_at(&name).await.unwrap().is_none(),
        "a bucket that was never created has no row"
    );
    assert!(
        store.row(&name).await.unwrap().is_none(),
        "and no decoded row either"
    );

    // Create is insert-or-refuse in one transaction (review item 10).
    let fresh = bucket_at(t0());
    BucketMeta::create(store, &name, &fresh).await.unwrap();
    let refused = match BucketMeta::create(store, &name, &fresh).await {
        Ok(()) => panic!("create must refuse a bucket that already has a row"),
        Err(err) => err.into(),
    };
    match refused {
        storage::Error::AlreadyExists(existing) => assert_eq!(existing, name),
        other => panic!("create's refusal must project to AlreadyExists, got {other:?}"),
    }
    assert_eq!(
        store.created_at(&name).await.unwrap(),
        Some(t0()),
        "create records the row's own creation time"
    );

    // The existence-conditioned setters signal presence (review item 9).
    assert!(
        BucketMeta::set_tags(store, &name, &tags(&[("team", "storage")]))
            .await
            .unwrap(),
        "set_tags on a recorded bucket reports the write"
    );
    assert!(
        BucketMeta::set_acl(store, &name, Some(&owner()), &grants())
            .await
            .unwrap(),
        "set_acl on a recorded bucket reports the write"
    );
    // A configuration the wire can carry: `cors::Config::default()` has no
    // rules, encodes to the empty wire, and the row decoder reads that back as
    // `None` (`bucket_row::decode_cors`), so asserting `is_some()` for it is
    // unsatisfiable by construction.
    let cors_config = cors::Config {
        rules: vec![cors::Rule {
            id: Some("conf".to_owned()),
            allowed_methods: vec!["GET".to_owned()],
            allowed_origins: vec!["https://conformance.test".to_owned()],
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: Some(600),
        }],
    };
    assert!(
        store.set_cors(&name, &cors_config).await.unwrap(),
        "set_cors on a recorded bucket reports the write"
    );
    let decoded = store
        .row(&name)
        .await
        .unwrap()
        .expect("the row just written");
    assert_eq!(decoded.created, t0());
    assert_eq!(
        pairs(&decoded.tags),
        vec![("team".to_owned(), "storage".to_owned())]
    );
    assert_eq!(decoded.owner.as_ref(), Some(&owner()));
    assert!(
        !decoded.acl.grants.is_empty(),
        "set_acl records the grants on the row"
    );
    assert_eq!(
        decoded.cors.as_ref(),
        Some(&cors_config),
        "set_cors records the configuration the wire round-trips"
    );
    let only = [name.clone()];
    assert_eq!(
        store.owners(&only).await.unwrap(),
        vec![Some(owner())],
        "owners answers the recorded owner in the input's order"
    );

    let absent = bucket_name("conf-bucket-absent");
    let empty: AclGrants = Vec::new();
    assert!(
        !BucketMeta::set_tags(store, &absent, &tags(&[("a", "b")]))
            .await
            .unwrap(),
        "set_tags on a missing bucket writes nothing and says so"
    );
    assert!(
        !BucketMeta::clear_tags(store, &absent).await.unwrap(),
        "clear_tags on a missing bucket writes nothing and says so"
    );
    assert!(
        !BucketMeta::set_acl(store, &absent, None, &empty)
            .await
            .unwrap(),
        "set_acl on a missing bucket writes nothing and says so"
    );
    assert!(
        !store.set_cors(&absent, &cors_config).await.unwrap(),
        "set_cors on a missing bucket writes nothing and says so"
    );
    assert!(
        !store.clear_cors(&absent).await.unwrap(),
        "clear_cors on a missing bucket writes nothing and says so"
    );

    // The create-or-update twins (design 2026-10-03, D10): the element write
    // and the first-sight insert are one transaction, so no reader sees the
    // row without its element. `set_tags_or_create` on a bucket with no row
    // records it — with the tags AND the creation time the caller passed.
    let fresh_tags = bucket_name("conf-bucket-tags-create");
    assert!(
        store.row(&fresh_tags).await.unwrap().is_none(),
        "the tag twin starts from a bucket with no row"
    );
    BucketMeta::set_tags_or_create(store, &fresh_tags, &tags(&[("first", "sight")]), t0())
        .await
        .unwrap();
    let created = store
        .row(&fresh_tags)
        .await
        .unwrap()
        .expect("set_tags_or_create recorded the first-sight row");
    assert_eq!(
        created.created,
        t0(),
        "a first-sight row carries the creation time the caller passed"
    );
    assert_eq!(
        pairs(&created.tags),
        vec![("first".to_owned(), "sight".to_owned())],
        "…and the tag set it was asked to write"
    );

    // On an existing row the tag element is replaced while the creation time
    // and the other elements survive, and a second identical write is a no-op
    // that still answers `Ok` (the contract promises the answer, not whether
    // the no-op commits).
    let kept_tags = bucket_name("conf-bucket-tags-keep");
    BucketMeta::create(store, &kept_tags, &bucket_at(t0()))
        .await
        .unwrap();
    BucketMeta::set_acl(store, &kept_tags, Some(&owner()), &grants())
        .await
        .unwrap();
    BucketMeta::set_tags_or_create(store, &kept_tags, &tags(&[("env", "prod")]), t1())
        .await
        .unwrap();
    BucketMeta::set_tags_or_create(store, &kept_tags, &tags(&[("env", "prod")]), t1())
        .await
        .expect("writing an identical tag set still answers Ok");
    let tagged = store
        .row(&kept_tags)
        .await
        .unwrap()
        .expect("the tagged row");
    assert_eq!(
        pairs(&tagged.tags),
        vec![("env".to_owned(), "prod".to_owned())],
        "the tag element is replaced"
    );
    assert_eq!(
        tagged.created,
        t0(),
        "an existing row keeps its recorded creation time, not the caller's `now`"
    );
    assert_eq!(
        tagged.owner.as_ref(),
        Some(&owner()),
        "…and its owner element"
    );
    assert_eq!(tagged.acl.grants, grants(), "…and its grant set");

    // `set_grants_or_create`'s first sight carries the grants and NO owner —
    // a bucket the API never sighted has none to carry (the lazy first-sight
    // policy).
    let fresh_grants = bucket_name("conf-bucket-grants-create");
    assert!(store.row(&fresh_grants).await.unwrap().is_none());
    BucketMeta::set_grants_or_create(store, &fresh_grants, &grants(), t0())
        .await
        .unwrap();
    let sighted = store
        .row(&fresh_grants)
        .await
        .unwrap()
        .expect("set_grants_or_create recorded the first-sight row");
    assert_eq!(
        sighted.acl.grants,
        grants(),
        "a first-sight row records the grant set"
    );
    assert_eq!(
        sighted.owner, None,
        "…and no owner: a bucket the API never sighted has none to carry"
    );
    assert_eq!(
        sighted.created,
        t0(),
        "…with the creation time the caller passed"
    );

    // On an existing row the grants are replaced while the recorded owner and
    // creation time ride untouched.
    let kept_grants = bucket_name("conf-bucket-grants-keep");
    BucketMeta::create(store, &kept_grants, &bucket_at(t0()))
        .await
        .unwrap();
    BucketMeta::set_acl(store, &kept_grants, Some(&owner()), &grants())
        .await
        .unwrap();
    let replaced = Acl::default_private(Some(other_owner())).grants;
    BucketMeta::set_grants_or_create(store, &kept_grants, &replaced, t1())
        .await
        .unwrap();
    let regranted = store
        .row(&kept_grants)
        .await
        .unwrap()
        .expect("the regranted row");
    assert_eq!(regranted.acl.grants, replaced, "the grant set is replaced");
    assert_eq!(
        regranted.owner.as_ref(),
        Some(&owner()),
        "the recorded owner element survives the grant write"
    );
    assert_eq!(
        regranted.created,
        t0(),
        "…and the recorded creation time, not the caller's `now`"
    );

    for bucket in [&fresh_tags, &kept_tags, &fresh_grants, &kept_grants] {
        BucketMeta::remove(store, bucket).await.unwrap();
    }

    // Record_if_absent keeps the first recorded time; record_full upserts.
    let kept = bucket_name("conf-bucket-kept");
    assert_eq!(store.record_if_absent(&kept, t0()).await.unwrap(), t0());
    assert_eq!(
        store.record_if_absent(&kept, t1()).await.unwrap(),
        t0(),
        "a second record_if_absent must not move the recorded time"
    );
    let mut upsert = bucket_at(t1());
    upsert.tags = tags(&[("stage", "rewritten")]);
    store.record_full(&kept, &upsert).await.unwrap();
    let reloaded = store.row(&kept).await.unwrap().expect("the upserted row");
    assert_eq!(reloaded.created, t1());
    assert_eq!(
        pairs(&reloaded.tags),
        vec![("stage".to_owned(), "rewritten".to_owned())]
    );

    // Load_many answers in the input's order, with None for absence.
    assert_eq!(
        store
            .load_many(&[name.clone(), absent.clone(), kept.clone()])
            .await
            .unwrap(),
        vec![Some(t0()), None, Some(t1())],
        "load_many answers in the input's order, None for a bucket with no row"
    );

    // Load_all carries every recorded bucket, ascending by name.
    let all = store.load_all().await.unwrap();
    assert!(
        all.iter()
            .any(|(n, t)| n == &kept.to_string() && *t == t1()),
        "load_all carries the bucket and its recorded creation time"
    );
    let mut sorted = all.clone();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    assert_eq!(
        all, sorted,
        "load_all is ascending by the name's UTF-8 bytes"
    );

    // Clear_tags and clear_cors are idempotent on a recorded bucket.
    assert!(BucketMeta::clear_tags(store, &name).await.unwrap());
    assert!(BucketMeta::clear_tags(store, &name).await.unwrap());
    assert!(store.row(&name).await.unwrap().unwrap().tags.is_empty());
    assert!(store.clear_cors(&name).await.unwrap());
    assert!(store.row(&name).await.unwrap().unwrap().cors.is_none());

    // Remove is idempotent and drops the whole row.
    BucketMeta::remove(store, &kept).await.unwrap();
    BucketMeta::remove(store, &kept).await.unwrap();
    assert!(store.row(&kept).await.unwrap().is_none());

    // Record is the creation-time write that clears the other elements.
    store.record(&name, t1()).await.unwrap();
    let cleared = store
        .row(&name)
        .await
        .unwrap()
        .expect("the re-recorded row");
    assert_eq!(cleared.created, t1());
    assert!(cleared.tags.is_empty(), "record clears the tag set");
    assert!(cleared.owner.is_none(), "record clears the owner element");
    assert!(
        cleared.cors.is_none(),
        "record clears the CORS configuration"
    );

    BucketMeta::remove(store, &name).await.unwrap();
    BucketMeta::remove(store, &absent).await.unwrap();
}

/// The object row's lifecycle: point reads and writes, the retained part list,
/// the preserving rewrite, the element setters, batch writes and loads.
async fn object_legs<M>(store: &M)
where
    M: Metadata,
{
    let bucket = bucket_name("conf-object");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();

    let a = key("a.txt");
    let b = key("b.txt");
    assert!(
        ObjectMeta::get(store, &bucket, &a).await.unwrap().is_none(),
        "a key that was never written has no row"
    );

    store.put(&bucket, &a, &row(5, ETAG_A, 1)).await.unwrap();
    let got = ObjectMeta::get(store, &bucket, &a)
        .await
        .unwrap()
        .expect("the row just written");
    assert_eq!(
        got,
        row(5, ETAG_A, 1),
        "a point read answers what put wrote"
    );

    // Put_with_parts writes the retained list; an overwrite clears it
    // (review item 5).
    let retained = vec![
        PartRow::from_raw(1, 5, "", ""),
        PartRow::from_raw(2, 6, "", ""),
    ];
    store
        .put_with_parts(&bucket, &a, &row(11, ETAG_A, 2), &retained)
        .await
        .unwrap();
    let parts = store.parts(&bucket, &a).await.unwrap();
    assert_eq!(
        parts.iter().map(|p| p.number).collect::<Vec<_>>(),
        vec![1, 2],
        "put_with_parts retains the part list in ascending part-number order"
    );
    store.put(&bucket, &a, &row(5, ETAG_A, 3)).await.unwrap();
    assert!(
        store.parts(&bucket, &a).await.unwrap().is_empty(),
        "an overwrite is a fresh object: put clears the retained parts"
    );

    // Put_preserving refreshes the content-derived elements and carries the
    // recorded ones over.
    let mut recorded = row(5, ETAG_A, 3);
    recorded.tags = tags(&[("keep", "me")]);
    recorded.owner = Some(owner());
    store.put(&bucket, &a, &recorded).await.unwrap();
    let preserved = store
        .put_preserving(
            &bucket,
            &a,
            &Identity {
                etag: etag(ETAG_B),
                size: 9,
                mtime: t1(),
                file_identity: 77,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        pairs(&preserved.tags),
        vec![("keep".to_owned(), "me".to_owned())],
        "the rewrite carries the tag set over"
    );
    assert_eq!(preserved.owner.as_ref(), Some(&owner()));
    let healed = ObjectMeta::get(store, &bucket, &a)
        .await
        .unwrap()
        .expect("the refreshed row");
    assert_eq!(healed.size, 9, "the rewrite records the new size");
    assert_eq!(healed.identity, 77, "and the new identity");
    assert_eq!(healed.mtime, t1(), "and the new modification time");
    assert_eq!(
        pairs(&healed.tags),
        vec![("keep".to_owned(), "me".to_owned())],
        "and carries the recorded tag set"
    );

    // The element setters answer presence, and create nothing for a missing
    // key.
    assert!(
        ObjectMeta::set_tags(store, &bucket, &a, &tags(&[("k", "v")]))
            .await
            .unwrap()
    );
    assert!(ObjectMeta::clear_tags(store, &bucket, &a).await.unwrap());
    store
        .ensure_tags(&bucket, &a, &tags(&[("e", "1")]))
        .await
        .unwrap();
    assert_eq!(
        pairs(
            &ObjectMeta::get(store, &bucket, &a)
                .await
                .unwrap()
                .unwrap()
                .tags
        ),
        vec![("e".to_owned(), "1".to_owned())]
    );
    assert!(
        ObjectMeta::set_acl(store, &bucket, &a, Some(&owner()), &grants())
            .await
            .unwrap()
    );
    assert_eq!(
        ObjectMeta::get(store, &bucket, &a)
            .await
            .unwrap()
            .unwrap()
            .owner
            .as_ref(),
        Some(&owner())
    );

    let missing = key("missing.txt");
    let empty: AclGrants = Vec::new();
    assert!(
        !ObjectMeta::set_tags(store, &bucket, &missing, &tags(&[("k", "v")]))
            .await
            .unwrap(),
        "set_tags on a missing row writes nothing and says so"
    );
    assert!(
        !ObjectMeta::clear_tags(store, &bucket, &missing)
            .await
            .unwrap(),
        "clear_tags on a missing row writes nothing and says so"
    );
    assert!(
        !ObjectMeta::set_acl(store, &bucket, &missing, None, &empty)
            .await
            .unwrap(),
        "set_acl on a missing row writes nothing and says so"
    );
    store
        .ensure_tags(&bucket, &missing, &tags(&[("k", "v")]))
        .await
        .unwrap();
    assert!(
        ObjectMeta::get(store, &bucket, &missing)
            .await
            .unwrap()
            .is_none(),
        "ensure_tags creates no row for a missing key"
    );

    // Load is one call, in the input's order, with None for absence.
    let loaded = store
        .load(&bucket, &[b.clone(), a.clone(), missing.clone()])
        .await
        .unwrap();
    assert_eq!(loaded.len(), 3);
    assert!(loaded[0].is_none());
    assert_eq!(loaded[1].as_ref().map(|r| r.size), Some(9));
    assert!(loaded[2].is_none());

    // Put_batch clears each written key's retained parts, exactly as put
    // does (review item 5): the key starts the leg holding a retained list.
    let batch_retained = vec![PartRow::from_raw(1, 7, "", "")];
    store
        .put_with_parts(&bucket, &b, &row(7, ETAG_A, 4), &batch_retained)
        .await
        .unwrap();
    assert_eq!(
        store.parts(&bucket, &b).await.unwrap().len(),
        1,
        "the batch leg starts from a key that holds retained parts"
    );

    // Put_batch writes every row of the batch, in the input's order.
    store
        .put_batch(
            &bucket,
            &[
                (b.clone(), row(3, ETAG_B, 4)),
                (missing.clone(), row(4, ETAG_B, 5)),
            ],
        )
        .await
        .unwrap();
    assert!(
        store.parts(&bucket, &b).await.unwrap().is_empty(),
        "put_batch clears the key's retained parts, like put"
    );
    assert_eq!(
        ObjectMeta::get(store, &bucket, &b)
            .await
            .unwrap()
            .unwrap()
            .size,
        3
    );
    assert_eq!(
        ObjectMeta::get(store, &bucket, &missing)
            .await
            .unwrap()
            .unwrap()
            .size,
        4
    );

    // Move_ moves the row and its retained parts together.
    let moved = key("moved.txt");
    let retained = vec![PartRow::from_raw(1, 3, "", "")];
    store
        .put_with_parts(&bucket, &b, &row(6, ETAG_B, 6), &retained)
        .await
        .unwrap();
    store.move_(&bucket, &b, &moved).await.unwrap();
    assert!(
        ObjectMeta::get(store, &bucket, &b).await.unwrap().is_none(),
        "move_ leaves no source row"
    );
    assert_eq!(
        ObjectMeta::get(store, &bucket, &moved)
            .await
            .unwrap()
            .unwrap()
            .size,
        6
    );
    assert_eq!(
        store.parts(&bucket, &moved).await.unwrap().len(),
        1,
        "move_ carries the retained part list"
    );
    assert!(
        store.parts(&bucket, &b).await.unwrap().is_empty(),
        "and leaves none behind"
    );
    store
        .move_(&bucket, &key("never-written.txt"), &key("never-moved.txt"))
        .await
        .unwrap();

    // A move onto the same key is a no-op too (R14): the row is already where
    // it is asked to go, so it must survive in place — with its retained part
    // list.
    store.move_(&bucket, &moved, &moved).await.unwrap();
    assert_eq!(
        ObjectMeta::get(store, &bucket, &moved)
            .await
            .unwrap()
            .unwrap()
            .size,
        6,
        "a self-move leaves the row where it is"
    );
    assert_eq!(
        store.parts(&bucket, &moved).await.unwrap().len(),
        1,
        "and its retained part list with it"
    );

    // Remove and remove_many are idempotent.
    assert!(ObjectMeta::remove(store, &bucket, &moved).await.unwrap());
    assert!(
        !ObjectMeta::remove(store, &bucket, &moved).await.unwrap(),
        "remove is idempotent and says the second call found nothing"
    );
    store
        .remove_many(&bucket, &[a.clone(), missing.clone(), moved.clone()])
        .await
        .unwrap();
    let remaining = [a.clone()];
    store.remove_many(&bucket, &remaining).await.unwrap();
    assert!(ObjectMeta::get(store, &bucket, &a).await.unwrap().is_none());

    // Drain_bucket is idempotent and empties the bucket.
    ObjectMeta::drain_bucket(store, &bucket).await.unwrap();
    ObjectMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(store.walk(&bucket).await.unwrap().is_empty());
    assert!(store.walk_healing(&bucket).await.unwrap().is_empty());

    BucketMeta::remove(store, &bucket).await.unwrap();
}

/// The two traversals: byte ordering, the per-bucket boundary and the heal
/// decision the healing traversal reports.
async fn traversal_legs<M>(store: &M)
where
    M: Metadata,
{
    let bucket = bucket_name("conf-walk");
    let other = bucket_name("conf-walk-other");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    BucketMeta::create(store, &other, &bucket_at(t0()))
        .await
        .unwrap();

    // The adversarial pair: '-' (0x2D) sorts before '/' (0x2F), so a
    // byte-ordered walk yields this order and a locale collation does not.
    let names = ["data", "data-2", "data/", "data/leaf"];
    for (index, name) in names.iter().enumerate() {
        store
            .put(&bucket, &key(name), &row(1, ETAG_A, index as u64))
            .await
            .unwrap();
    }
    store
        .put(&other, &key("data"), &row(2, ETAG_A, 9))
        .await
        .unwrap();

    let walked = store.walk(&bucket).await.unwrap();
    assert_eq!(
        walked.iter().map(|(k, _)| &**k).collect::<Vec<_>>(),
        names.to_vec(),
        "a walk is ascending by the key's UTF-8 bytes (data-2 before data/)"
    );
    assert!(
        walked.iter().all(|(_, r)| r.size == 1),
        "a per-bucket walk never yields another bucket's row"
    );

    let other_walk = store.walk(&other).await.unwrap();
    assert_eq!(
        other_walk.len(),
        1,
        "the other bucket holds exactly its own row"
    );
    assert_eq!(&*other_walk[0].0, "data");
    assert_eq!(
        other_walk[0].1.size, 2,
        "the other bucket's row is untouched by the first bucket's walk"
    );

    // The healing traversal never fails, yields every healthy row, orders
    // identically, and reports a served row as Some (review item 7).
    let healed = store.walk_healing(&bucket).await.unwrap();
    assert_eq!(healed.len(), walked.len(), "walk_healing yields every row");
    assert!(
        healed.iter().all(|(_, r)| r.is_some()),
        "a healthy row is served as Some"
    );
    assert_eq!(
        healed.iter().map(|(k, _)| &**k).collect::<Vec<_>>(),
        names.to_vec(),
        "both traversals order identically"
    );

    ObjectMeta::drain_bucket(store, &bucket).await.unwrap();
    ObjectMeta::drain_bucket(store, &other).await.unwrap();
    BucketMeta::remove(store, &bucket).await.unwrap();
    BucketMeta::remove(store, &other).await.unwrap();
}

/// The upload rows: creation, the key check, part order, the completion join,
/// the listings and both drains.
async fn upload_legs<M>(store: &M)
where
    M: Metadata,
{
    let bucket = bucket_name("conf-upload");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    let target = key("big.bin");
    let id = "conf-upload-1";

    assert!(
        UploadMeta::get(store, &bucket, &target, id)
            .await
            .unwrap()
            .is_none(),
        "an upload that was never created has no row"
    );
    assert!(
        !store.has_uploads(&bucket).await.unwrap(),
        "a bucket with no upload reports none"
    );

    let state = upload::Row {
        upload_id: id.to_owned(),
        key: target.clone(),
        initiated_at: t0(),
        checksum: None,
        tags: tags(&[("from", "upload")]),
        owner: Some(owner()),
        acl: Acl::default_private(Some(owner())),
    };
    UploadMeta::create(store, &bucket, id, &state)
        .await
        .unwrap();
    UploadMeta::create(store, &bucket, id, &state)
        .await
        .unwrap();

    let got = UploadMeta::get(store, &bucket, &target, id)
        .await
        .unwrap()
        .expect("the upload just created");
    assert_eq!(got.initiated_at, t0());
    assert_eq!(
        pairs(&got.tags),
        vec![("from".to_owned(), "upload".to_owned())]
    );
    assert!(
        store.has_uploads(&bucket).await.unwrap(),
        "has_uploads sees the in-progress upload"
    );
    assert!(
        UploadMeta::get(store, &bucket, &key("other.bin"), id)
            .await
            .unwrap()
            .is_none(),
        "an upload is present only under its own key"
    );

    // Part numbers order numerically, not lexically.
    for number in [10u32, 2, 1] {
        store
            .put_part(&bucket, &target, id, part(number), &etag(ETAG_A), None)
            .await
            .unwrap();
    }
    let listed = store.list_parts(&bucket, id, part(1)).await.unwrap();
    assert_eq!(
        listed.iter().map(|p| p.number).collect::<Vec<_>>(),
        vec![1, 2, 10],
        "part numbers order numerically (10 after 2)"
    );
    assert_eq!(listed[0].etag.as_str(), ETAG_A);
    assert_eq!(
        store.list_parts(&bucket, id, part(2)).await.unwrap().len(),
        2,
        "list_parts honours its lower bound"
    );

    // A bounded page over the same rows: at or after the **raw** `from`,
    // capped at `max`, with the one-lookahead "more rows" flag. `max = 0`
    // answers nothing and is *not* truncated (the resume arithmetic must not
    // advance past a page it never received), and a bound past the last row
    // is an empty, untruncated page.
    let page = store.list_parts_page(&bucket, id, 1, 2).await.unwrap();
    assert_eq!(
        page.rows,
        listed[..2].to_vec(),
        "a page is the same join as list_parts, from the raw lower bound"
    );
    assert!(page.more, "a page with a row after it reports more");
    assert_eq!(
        page.raw_last,
        Some(2),
        "the raw last number names the page's last raw row — the caller's marker"
    );
    let page = store.list_parts_page(&bucket, id, 2, 2).await.unwrap();
    assert_eq!(
        page.rows.iter().map(|p| p.number).collect::<Vec<_>>(),
        vec![2, 10]
    );
    assert!(
        !page.more,
        "the page that reaches the last row reports no more"
    );
    assert_eq!(page.raw_last, Some(10), "and still names its last raw row");
    let page = store.list_parts_page(&bucket, id, 11, 2).await.unwrap();
    assert!(
        page.rows.is_empty(),
        "a lower bound past the last row is empty"
    );
    assert!(!page.more);
    assert_eq!(page.raw_last, None, "a raw read of no row names no row");
    let page = store.list_parts_page(&bucket, id, 1, 0).await.unwrap();
    assert!(
        page.rows.is_empty() && !page.more,
        "max = 0 is empty and untruncated"
    );
    assert_eq!(page.raw_last, None, "max = 0 reads no raw row");

    // The completion join answers in the input's order, with None for a gap.
    let records = store
        .part_records(&bucket, &target, id, &[part(10), part(9), part(1)])
        .await
        .unwrap()
        .expect("the upload's key matches");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].as_ref().map(|r| r.number), Some(10));
    assert!(records[1].is_none(), "a requested part with no row is None");
    assert_eq!(records[2].as_ref().map(|r| r.number), Some(1));
    assert!(
        store
            .part_records(&bucket, &key("other.bin"), id, &[part(1)])
            .await
            .unwrap()
            .is_none(),
        "a key that does not match the upload answers the outer None"
    );

    // The listings.
    let uploads = store.list_uploads(&bucket).await.unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].upload_id, id);
    assert_eq!(uploads[0].key, target);
    assert_eq!(uploads[0].bucket, bucket);
    let (page, prefixes, truncated, marker) = store
        .list_uploads_page(&bucket, "", None, None, 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 1, "the page carries the bucket's only upload");
    assert!(prefixes.is_empty(), "no delimiter was asked for");
    assert!(!truncated);
    assert!(marker.is_none());
    assert!(
        store
            .walk_uploads()
            .await
            .unwrap()
            .iter()
            .any(|u| u.upload_id == id),
        "walk_uploads spans the store"
    );

    // The page's depth: the shallow single-upload call above cannot reach a
    // boundary, a cursor, a rollup or `max = 0`, so three uploads join the
    // bucket — one outside any prefix and two inside `group/`. The order is
    // by key and only then by upload id, so the pages below are
    // deterministic.
    for (upload_id, upload_key) in [
        ("conf-upload-2", "alpha.bin"),
        ("conf-upload-3", "group/one.bin"),
        ("conf-upload-4", "group/two.bin"),
    ] {
        UploadMeta::create(
            store,
            &bucket,
            upload_id,
            &upload::Row {
                upload_id: upload_id.to_owned(),
                key: key(upload_key),
                initiated_at: t0(),
                checksum: None,
                tags: object::Tags::empty(),
                owner: None,
                acl: Acl::default_private(None),
            },
        )
        .await
        .unwrap();
    }

    // The whole bucket, in order: the key first, the upload id second.
    let (page, prefixes, truncated, marker) = store
        .list_uploads_page(&bucket, "", None, None, 10)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| (&*u.key, u.upload_id.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("alpha.bin", "conf-upload-2"),
            ("big.bin", "conf-upload-1"),
            ("group/one.bin", "conf-upload-3"),
            ("group/two.bin", "conf-upload-4"),
        ],
        "the listing orders by key and only then by upload id"
    );
    assert!(
        prefixes.is_empty() && !truncated && marker.is_none(),
        "a page that holds every entry carries no prefix, no truncation and no marker"
    );

    // The prefix filter narrows the page to its group, whose order stays the
    // upload id's.
    let (page, prefixes, truncated, marker) = store
        .list_uploads_page(&bucket, "group/", None, None, 10)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| u.upload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["conf-upload-3", "conf-upload-4"],
        "the prefix filter keeps only the matching keys"
    );
    assert!(prefixes.is_empty() && !truncated && marker.is_none());

    // A boundary: the marker is the page's last entry, exclusive-after, so
    // the resumed page is exactly the rest — no gap, no repeat.
    let (page, _, truncated, marker) = store
        .list_uploads_page(&bucket, "", None, None, 2)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| u.upload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["conf-upload-2", "conf-upload-1"],
        "the page takes the two smallest entries"
    );
    assert!(truncated, "an entry after the page reports truncation");
    let marker = marker.expect("a truncated page carries the resume marker");
    let (page, _, truncated, next) = store
        .list_uploads_page(&bucket, "", None, Some(&marker), 2)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| u.upload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["conf-upload-3", "conf-upload-4"],
        "the resumed page is the rest of the listing"
    );
    assert!(
        !truncated && next.is_none(),
        "the page that reaches the last entry is untruncated and markerless"
    );
    // The marker names an entry, not a key group: resuming on it skips that
    // one entry and nothing else.
    let (page, _, _, _) = store
        .list_uploads_page(&bucket, "", None, Some(&marker), 10)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| u.upload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["conf-upload-3", "conf-upload-4"],
        "the marker is exclusive-after"
    );

    // A short page and the empty page: `max` past the end is untruncated,
    // and `max = 0` answers nothing without claiming truncation — the resume
    // arithmetic must not advance past a page it never received.
    let (page, _, truncated, marker) = store
        .list_uploads_page(&bucket, "", None, None, 4)
        .await
        .unwrap();
    assert_eq!(
        page.len(),
        4,
        "a page holds every entry when max reaches it"
    );
    assert!(!truncated && marker.is_none());
    let (page, prefixes, truncated, marker) = store
        .list_uploads_page(&bucket, "", None, None, 0)
        .await
        .unwrap();
    assert!(
        page.is_empty() && prefixes.is_empty() && !truncated && marker.is_none(),
        "max = 0 is empty, untruncated and markerless"
    );

    // A delimiter rolls a group up once: `group/` is one common prefix, and
    // the page keeps only the keys that do not roll up.
    let (page, prefixes, truncated, marker) = store
        .list_uploads_page(&bucket, "", Some("/"), None, 10)
        .await
        .unwrap();
    assert_eq!(
        page.iter()
            .map(|u| u.upload_id.as_str())
            .collect::<Vec<_>>(),
        vec!["conf-upload-2", "conf-upload-1"],
        "the rolled-up group leaves the page as one prefix"
    );
    assert_eq!(
        prefixes,
        vec!["group/".to_owned()],
        "the delimiter rolls the group up exactly once"
    );
    assert!(!truncated && marker.is_none());

    // The extra uploads leave before the drains below count the bucket.
    for upload_id in ["conf-upload-2", "conf-upload-3", "conf-upload-4"] {
        store.drain_upload(&bucket, upload_id).await.unwrap();
    }
    assert_eq!(
        store.list_uploads(&bucket).await.unwrap().len(),
        1,
        "only the original upload remains"
    );

    // The bucket boundary holds for uploads too (property 3): an upload
    // created in another bucket is invisible to every probe scoped to this
    // one, and visible to the store-wide walk. It stays alive through the
    // drains below, so the final `has_uploads` also proves the boundary.
    let elsewhere = bucket_name("conf-upload-other");
    BucketMeta::create(store, &elsewhere, &bucket_at(t0()))
        .await
        .unwrap();
    let other_id = "conf-upload-other-1";
    UploadMeta::create(
        store,
        &elsewhere,
        other_id,
        &upload::Row {
            upload_id: other_id.to_owned(),
            key: target.clone(),
            initiated_at: t0(),
            checksum: None,
            tags: object::Tags::empty(),
            owner: None,
            acl: Acl::default_private(None),
        },
    )
    .await
    .unwrap();
    assert!(
        !store
            .list_uploads(&bucket)
            .await
            .unwrap()
            .iter()
            .any(|u| u.upload_id == other_id),
        "list_uploads never yields another bucket's upload"
    );
    assert!(
        store
            .walk_uploads()
            .await
            .unwrap()
            .iter()
            .any(|u| u.upload_id == other_id),
        "walk_uploads spans every bucket"
    );

    // Drain_upload takes the parts with it and is idempotent.
    store.drain_upload(&bucket, id).await.unwrap();
    store.drain_upload(&bucket, id).await.unwrap();
    assert!(
        UploadMeta::get(store, &bucket, &target, id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .list_parts(&bucket, id, part(1))
            .await
            .unwrap()
            .is_empty(),
        "the drain removes the upload's parts"
    );
    assert!(!store.has_uploads(&bucket).await.unwrap());

    // Drain_upload_checked: the identity check and the drain in one
    // transaction (the abort path). A key that does not match the upload is
    // refused with the NoSuchUpload projection, and nothing is drained.
    let checked = "conf-upload-checked";
    let other_key = key("other.bin");
    UploadMeta::create(
        store,
        &bucket,
        checked,
        &upload::Row {
            upload_id: checked.to_owned(),
            key: target.clone(),
            initiated_at: t0(),
            checksum: None,
            tags: object::Tags::empty(),
            owner: None,
            acl: Acl::default_private(None),
        },
    )
    .await
    .unwrap();
    store
        .put_part(&bucket, &target, checked, part(1), &etag(ETAG_A), None)
        .await
        .unwrap();

    let mismatched = match store
        .drain_upload_checked(&bucket, &other_key, checked)
        .await
    {
        Ok(()) => panic!("a key that does not match the upload must be refused"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(mismatched, storage::Error::NoSuchUpload(ref id) if id == checked),
        "a mismatched key must project to NoSuchUpload, got {mismatched:?}"
    );
    assert!(
        UploadMeta::get(store, &bucket, &target, checked)
            .await
            .unwrap()
            .is_some(),
        "the refused checked drain drained nothing: the upload still answers"
    );
    assert_eq!(
        store
            .list_parts(&bucket, checked, part(1))
            .await
            .unwrap()
            .len(),
        1,
        "…and its parts are untouched"
    );

    // An upload that was never created is refused the same way.
    let never = "conf-upload-never";
    let absent = match store.drain_upload_checked(&bucket, &target, never).await {
        Ok(()) => panic!("an upload with no row must be refused"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(absent, storage::Error::NoSuchUpload(ref id) if id == never),
        "an absent upload must project to NoSuchUpload, got {absent:?}"
    );

    // The matching call drains the upload and its parts in one call.
    store
        .drain_upload_checked(&bucket, &target, checked)
        .await
        .unwrap();
    assert!(
        UploadMeta::get(store, &bucket, &target, checked)
            .await
            .unwrap()
            .is_none(),
        "the checked drain removed the upload row"
    );
    assert!(
        store
            .list_parts(&bucket, checked, part(1))
            .await
            .unwrap()
            .is_empty(),
        "…and its parts"
    );
    assert!(
        !store.has_uploads(&bucket).await.unwrap(),
        "…leaving the bucket with no upload"
    );

    // A second call has nothing left to take, so it refuses rather than
    // succeeding silently — the row is gone and the key can no longer match.
    let consumed = match store.drain_upload_checked(&bucket, &target, checked).await {
        Ok(()) => panic!("a consumed upload must be refused, not silently drained"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(consumed, storage::Error::NoSuchUpload(ref id) if id == checked),
        "a second checked drain must project to NoSuchUpload, got {consumed:?}"
    );

    // Drain_bucket removes every upload of the bucket.
    let second = upload::Row {
        upload_id: "conf-upload-2".to_owned(),
        key: target.clone(),
        initiated_at: t0(),
        checksum: None,
        tags: object::Tags::empty(),
        owner: None,
        acl: Acl::default_private(None),
    };
    UploadMeta::create(store, &bucket, "conf-upload-2", &second)
        .await
        .unwrap();
    store
        .put_part(
            &bucket,
            &target,
            "conf-upload-2",
            part(1),
            &etag(ETAG_B),
            None,
        )
        .await
        .unwrap();
    assert!(store.has_uploads(&bucket).await.unwrap());
    UploadMeta::drain_bucket(store, &bucket).await.unwrap();
    UploadMeta::drain_bucket(store, &bucket).await.unwrap();
    assert!(!store.has_uploads(&bucket).await.unwrap());
    assert!(
        store
            .list_parts(&bucket, "conf-upload-2", part(1))
            .await
            .unwrap()
            .is_empty()
    );

    UploadMeta::drain_bucket(store, &elsewhere).await.unwrap();
    BucketMeta::remove(store, &bucket).await.unwrap();
    BucketMeta::remove(store, &elsewhere).await.unwrap();
}

/// The refusals and their projections, and the atomicity of a refused
/// multi-row operation (D22, review item 9).
async fn refusal_legs<M>(store: &M)
where
    M: Metadata,
{
    let bucket = bucket_name("conf-refuse");
    let absent = bucket_name("conf-refuse-absent");
    BucketMeta::create(store, &bucket, &bucket_at(t0()))
        .await
        .unwrap();
    let k = key("guarded.txt");

    let refused = match store.put_guarded(&absent, &k, &row(1, ETAG_A, 1)).await {
        Ok(()) => panic!("put_guarded must refuse a bucket with no row"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(refused, storage::Error::NoSuchBucket(_)),
        "put_guarded's refusal must project to NoSuchBucket, got {refused:?}"
    );
    assert!(
        ObjectMeta::get(store, &absent, &k).await.unwrap().is_none(),
        "the refused guarded write wrote no object row"
    );
    assert!(
        store.row(&absent).await.unwrap().is_none(),
        "and created no bucket row either"
    );

    store
        .put_guarded(&bucket, &k, &row(1, ETAG_A, 1))
        .await
        .unwrap();
    assert_eq!(
        ObjectMeta::get(store, &bucket, &k)
            .await
            .unwrap()
            .unwrap()
            .size,
        1
    );

    let missing = match store.remove_if_empty(&absent).await {
        Ok(()) => panic!("remove_if_empty must refuse a bucket with no row"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(missing, storage::Error::NoSuchBucket(_)),
        "remove_if_empty on no row must project to NoSuchBucket, got {missing:?}"
    );

    // A refusal is atomic: the bucket row and the object row both survive it.
    let occupied = match store.remove_if_empty(&bucket).await {
        Ok(()) => panic!("remove_if_empty must refuse a bucket that holds an object"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(occupied, storage::Error::NotEmpty(_)),
        "remove_if_empty on a non-empty bucket must project to NotEmpty, got {occupied:?}"
    );
    assert!(
        store.row(&bucket).await.unwrap().is_some(),
        "the refused delete left the bucket row in place"
    );
    assert!(
        ObjectMeta::get(store, &bucket, &k).await.unwrap().is_some(),
        "and the object row in place"
    );

    // An in-progress upload blocks the delete too.
    let upload_id = "conf-refuse-upload";
    UploadMeta::create(
        store,
        &bucket,
        upload_id,
        &upload::Row {
            upload_id: upload_id.to_owned(),
            key: k.clone(),
            initiated_at: t0(),
            checksum: None,
            tags: object::Tags::empty(),
            owner: None,
            acl: Acl::default_private(None),
        },
    )
    .await
    .unwrap();
    ObjectMeta::remove(store, &bucket, &k).await.unwrap();
    let blocked = match store.remove_if_empty(&bucket).await {
        Ok(()) => panic!("remove_if_empty must refuse a bucket that holds an upload"),
        Err(err) => err.into(),
    };
    assert!(
        matches!(blocked, storage::Error::NotEmpty(_)),
        "an in-progress upload keeps the bucket non-empty, got {blocked:?}"
    );
    UploadMeta::drain_bucket(store, &bucket).await.unwrap();

    // The successful path: the TOCTOU-free delete removes the row, and the
    // idempotence of the plain removal is the second call (a second
    // remove_if_empty must refuse: a bucket with no row is NoSuchBucket).
    store.remove_if_empty(&bucket).await.unwrap();
    BucketMeta::remove(store, &bucket).await.unwrap();
    assert!(store.row(&bucket).await.unwrap().is_none());
}
