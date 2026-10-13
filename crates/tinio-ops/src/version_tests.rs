use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use async_trait::async_trait;

use super::*;
use crate::{
    _core::{
        ETag,
        acl::{AclGrants, Grant, Grantee, OwnerId, Permission},
        blob::BlobRef,
        content::{ContentEntry, ContentKey},
        meta::object::PartRow,
        storage::{BodyStream, Error as StorageError, collect_body},
        version::State,
    },
    _store_redb::RedbContent,
    BucketRow, MetadataFailure,
    testutil::{Base, BlockMem, TestMeta, body},
};

/// The group's receiver over the test base — the row-backed store's
/// policies, so a row write is the operation's answer.
fn ops(base: &Base) -> ObjectOps<TestMeta, RedbContent> {
    base.objects(BucketRow::Guaranteed, MetadataFailure::Fail)
}

/// The content plane that makes the race the label rule exists for
/// deterministic: the first `stat` it serves moves the key's slot to a
/// fresh identity — the concurrent overwrite landing **inside** the
/// version-addressed read, after its resolution and before the plain read
/// it delegates to has finished. Every other call, and every `stat` once
/// the move has happened (or when the test pre-fires it), is the real
/// in-memory plane's.
#[derive(Clone)]
struct MovesOnFirstStat {
    inner: RedbContent,
    rows: Arc<TestMeta>,
    bucket: bucket::Name,
    key: object::Key,
    fresh: Id,
    moved: Arc<AtomicBool>,
}

#[async_trait]
impl ObjectContent for MovesOnFirstStat {
    type Error = <RedbContent as ObjectContent>::Error;
    type Staged = <RedbContent as ObjectContent>::Staged;

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error> {
        if !self.moved.swap(true, Ordering::SeqCst) {
            <TestMeta as VersionMeta>::advance_slot(
                &self.rows,
                &self.bucket,
                &self.key,
                Some(self.fresh.clone()),
                9,
            )
            .await?;
        }
        self.inner.stat(bucket, key).await
    }

    async fn stage(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error> {
        self.inner.stage(bucket, key).await
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        self.inner.publish(bucket, staged, key).await
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        self.inner.open(bucket, key, range).await
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        self.inner.remove(bucket, key).await
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.inner.rename(bucket, from, to).await
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.inner.copy(bucket, from, to).await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error> {
        self.inner.scan(bucket, prefix, after, limit).await
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.inner.create_bucket(bucket).await
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.inner.remove_bucket(bucket).await
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        self.inner.is_empty(bucket).await
    }
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

/// One versioned write through the version-creating object body.
async fn put_versioned(
    base: &Base,
    plane: &Plane<TestMeta, RedbContent, BlockMem>,
    name: &bucket::Name,
    key: &object::Key,
    bytes: &'static [u8],
    owner: Option<&OwnerId>,
) -> object::Info {
    let staged = ops(base)
        .stage_body(name, key, body(&[bytes]), None, None)
        .await
        .unwrap();
    ops(base)
        .put_object_versioned(
            plane,
            name,
            key,
            staged,
            object::Tags::empty(),
            owner,
            &Acl::default_private(None),
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap()
}

/// The version-creating delete, through the body the backends call.
async fn delete_marker(
    base: &Base,
    plane: &Plane<TestMeta, RedbContent, BlockMem>,
    name: &bucket::Name,
    key: &object::Key,
) -> version::Marker {
    ops(base)
        .create_delete_marker(plane, plane.version_rows.as_ref(), name, key)
        .await
        .unwrap()
}

/// A versioned bucket whose key holds one enabled version.
async fn versioned(
    base: &Base,
    key: &object::Key,
    bytes: &'static [u8],
) -> (
    bucket::Name,
    Plane<TestMeta, RedbContent, BlockMem>,
    object::Info,
) {
    let name = bucket(base, "data").await;
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let info = put_versioned(base, &plane, &name, key, bytes, None).await;
    (name, plane, info)
}

/// A served body's bytes.
async fn read(stream: BodyStream) -> Vec<u8> {
    collect_body(stream).await.unwrap().to_vec()
}

/// A canonical owner id.
fn owner() -> OwnerId {
    OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
}

/// A grant set a write replaces the version's with.
fn grants() -> AclGrants {
    vec![Grant {
        grantee: Grantee::Canonical(owner()),
        permission: Permission::Read,
    }]
}

#[tokio::test]
async fn a_version_addressed_read_serves_the_version_it_names() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first
        .version_id
        .clone()
        .expect("an enabled write mints an identity");
    let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    let second_id = second
        .version_id
        .clone()
        .expect("the overwrite mints its own identity");
    assert_ne!(first_id, second_id);

    // The superseded version: its own row and the block plane, never the
    // current object.
    match ops(&base)
        .get_object_version(&plane, &name, &key, Some(&first_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"first");
            assert_eq!(got.info.version_id.as_ref(), Some(&first_id));
            assert_eq!(got.info.etag, ETag::from_content(b"first"));
            assert_eq!(got.info.size, 5);
            assert!(got.info.bucket_records_versions);
            assert_eq!(got.served_range, None);
        }
        ReadResult::Marker { .. } => panic!("the superseded version is content"),
    }
    // The current version: the plain read's own serve, labelled with the
    // identity the gated slot resolved.
    match ops(&base)
        .get_object_version(&plane, &name, &key, Some(&second_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"second");
            assert_eq!(got.info.version_id.as_ref(), Some(&second_id));
            assert_eq!(got.info.etag, ETag::from_content(b"second"));
        }
        ReadResult::Marker { .. } => panic!("the current version is content"),
    }
    // The head agrees with the get, on every element it answers.
    match ops(&base)
        .head_object_version(&plane, &name, &key, Some(&first_id))
        .await
        .unwrap()
    {
        HeadResult::Object(info) => {
            assert_eq!(info.version_id.as_ref(), Some(&first_id));
            assert_eq!(info.size, 5);
            assert_eq!(info.etag, ETag::from_content(b"first"));
            assert!(info.bucket_records_versions);
        }
        HeadResult::Marker { .. } => panic!("the superseded version is content"),
    }
    // A range resolves against the version's own length and names the
    // window it served.
    match ops(&base)
        .get_object_version(
            &plane,
            &name,
            &key,
            Some(&first_id),
            Some(ByteRange::Inclusive(1, 3)),
        )
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"irs");
            assert_eq!(got.served_range, Some((1, 3)));
        }
        ReadResult::Marker { .. } => panic!("the superseded version is content"),
    }
    // An identity that names no version is the contract's own refusal.
    let unknown = Id::new("no-such-version").unwrap();
    let err = ops(&base)
        .get_object_version(&plane, &name, &key, Some(&unknown), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::NoSuchVersion(ref named) if named == "no-such-version"),
        "{err:?}"
    );
    let err = ops(&base)
        .head_object_version(&plane, &name, &key, Some(&unknown))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::NoSuchVersion(_)), "{err:?}");
}

#[tokio::test]
async fn a_version_addressed_read_of_a_delete_marker_answers_the_marker() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();
    let marker = delete_marker(&base, &plane, &name, &key).await;
    let marker_id = marker
        .id
        .clone()
        .expect("an enabled bucket's marker carries an identity");
    assert!(marker.is_delete_marker);

    // A current marker is a slot, not a version row. Its stamp is the
    // object row the marker port materializes.
    let current = plane.versions.current(&name, &key).await.unwrap();
    assert!(current.is_delete_marker);
    assert_eq!(current.version.as_ref(), Some(&marker_id));
    assert_eq!(current.seq, 2, "the marker takes the key's next sequence");
    assert!(
        VersionMeta::get_version(&*plane.version_rows, &name, &key, current.seq)
            .await
            .unwrap()
            .is_none(),
        "a current marker has no version row"
    );
    let row = ObjectMeta::get(&*base.meta, &name, &key)
        .await
        .unwrap()
        .expect("the marker's object row");

    // A marker is a **success**, never an error, for the reads.
    match ops(&base)
        .get_object_version(&plane, &name, &key, Some(&marker_id), None)
        .await
        .unwrap()
    {
        ReadResult::Marker { id, last_modified } => {
            assert_eq!(id.as_ref(), Some(&marker_id));
            assert_eq!(last_modified, row.mtime, "the marker's own stamp");
        }
        ReadResult::Object(_) => panic!("the current version is a marker, not content"),
    }
    match ops(&base)
        .head_object_version(&plane, &name, &key, Some(&marker_id))
        .await
        .unwrap()
    {
        HeadResult::Marker { id, last_modified } => {
            assert_eq!(id.as_ref(), Some(&marker_id));
            assert_eq!(last_modified, row.mtime);
        }
        HeadResult::Object(_) => panic!("the current version is a marker, not content"),
    }
    // The version the marker hid is still served by its own identity.
    match ops(&base)
        .get_object_version(&plane, &name, &key, Some(&first_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"first");
            assert_eq!(got.info.version_id.as_ref(), Some(&first_id));
        }
        ReadResult::Marker { .. } => panic!("the superseded version is content"),
    }
    // A marker holds no metadata: every pair refuses, naming the marker.
    let refusals = [
        ops(&base)
            .version_tags(&plane, &name, &key, Some(&marker_id))
            .await
            .unwrap_err(),
        ops(&base)
            .set_version_tags(&plane, &name, &key, Some(&marker_id), object::Tags::empty())
            .await
            .unwrap_err(),
        ops(&base)
            .version_acl(&plane, &name, &key, Some(&marker_id))
            .await
            .unwrap_err(),
        ops(&base)
            .set_version_acl(
                &plane,
                &name,
                &key,
                Some(&marker_id),
                &Acl::default_private(None),
            )
            .await
            .unwrap_err(),
    ];
    for err in refusals {
        assert!(
            matches!(&err, StorageError::NoSuchVersion(named) if named == marker_id.as_str()),
            "a marker must refuse metadata with its own identity: {err:?}"
        );
    }
}

#[tokio::test]
async fn a_null_identity_marker_answers_the_null_version_id() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Suspended)
        .await
        .unwrap();
    let null = put_versioned(&base, &plane, &name, &key, b"null", None).await;
    assert_eq!(
        null.version_id, None,
        "a suspended write records the null version"
    );
    let marker = delete_marker(&base, &plane, &name, &key).await;
    assert!(
        marker.is_delete_marker && marker.id.is_none(),
        "a suspended delete makes a null-identity marker current"
    );

    // `versionId=null` addresses the marker: a successful marker answer
    // carrying the null identity and the object row's stamp — never the
    // object version's identity, and never an error.
    let current = plane.versions.current(&name, &key).await.unwrap();
    assert!(current.is_delete_marker && current.version.is_none());
    let row = ObjectMeta::get(&*base.meta, &name, &key)
        .await
        .unwrap()
        .expect("the marker's object row");
    match ops(&base)
        .get_object_version(&plane, &name, &key, None, None)
        .await
        .unwrap()
    {
        ReadResult::Marker { id, last_modified } => {
            assert_eq!(id, None, "the null-identity marker carries the null id");
            assert_eq!(last_modified, row.mtime, "the marker row's own stamp");
        }
        ReadResult::Object(_) => panic!("the null-identity marker is not content"),
    }
    match ops(&base)
        .head_object_version(&plane, &name, &key, None)
        .await
        .unwrap()
    {
        HeadResult::Marker { id, .. } => assert_eq!(id, None),
        HeadResult::Object(_) => panic!("the null-identity marker is not content"),
    }
    // The per-version metadata pairs refuse it, naming the null identity.
    for err in [
        ops(&base)
            .version_tags(&plane, &name, &key, None)
            .await
            .unwrap_err(),
        ops(&base)
            .version_acl(&plane, &name, &key, None)
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(&err, StorageError::NoSuchVersion(named) if named == "null"),
            "a marker must refuse metadata as the null version: {err:?}"
        );
    }
    assert!(
        VersionMeta::list_versions(&*plane.version_rows, &name, &key)
            .await
            .unwrap()
            .is_empty(),
        "the removed null version does not stay in history"
    );
}

#[tokio::test]
async fn a_second_delete_keeps_the_previous_marker() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, _) = versioned(&base, &key, b"first").await;
    let first = delete_marker(&base, &plane, &name, &key).await;
    let first_id = first
        .id
        .clone()
        .expect("an enabled marker carries an identity");
    let second = delete_marker(&base, &plane, &name, &key).await;
    let second_id = second.id.expect("the second marker carries an identity");
    assert_ne!(first_id, second_id);

    let rows = VersionMeta::list_versions(&*plane.version_rows, &name, &key)
        .await
        .unwrap();
    assert!(
        rows.iter()
            .any(|row| row.kind == Kind::Marker && row.id.as_ref() == Some(&first_id)),
        "the previous marker stays in history: {rows:?}"
    );

    ops(&base)
        .delete_object_version(&plane, &name, &key, Some(&second_id))
        .await
        .unwrap();
    let current = plane.versions.current(&name, &key).await.unwrap();
    assert!(current.is_delete_marker);
    assert_eq!(current.version.as_ref(), Some(&first_id));
    let err = ops(&base).get_object(&name, &key, None).await.unwrap_err();
    assert!(matches!(err, StorageError::NoSuchKey(_)), "{err:?}");
}

#[tokio::test]
async fn restoring_a_delete_marker_removes_the_keys_bytes() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, _) = versioned(&base, &key, b"v1").await;
    let marker = delete_marker(&base, &plane, &name, &key).await;
    let marker_id = marker.id.expect("the marker carries an identity");
    let next = put_versioned(&base, &plane, &name, &key, b"v3", None).await;
    let next_id = next.version_id.expect("the overwrite mints an identity");

    ops(&base)
        .delete_object_version(&plane, &name, &key, Some(&next_id))
        .await
        .unwrap();
    let current = plane.versions.current(&name, &key).await.unwrap();
    assert!(current.is_delete_marker);
    assert_eq!(current.version.as_ref(), Some(&marker_id));
    let err = ops(&base).get_object(&name, &key, None).await.unwrap_err();
    assert!(matches!(err, StorageError::NoSuchKey(_)), "{err:?}");
}

#[tokio::test]
async fn deleting_a_version_refuses_a_stale_index_entry() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();
    let _second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    let history = VersionMeta::list_versions(&*plane.version_rows, &name, &key)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.id.as_ref() == Some(&first_id))
        .expect("the first version is history");
    let stale = Id::new("stale-index").unwrap();
    base.counted().answer_next_version_seq(history.seq);

    let err = ops(&base)
        .delete_object_version(&plane, &name, &key, Some(&stale))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StorageError::NoSuchVersion(ref named) if named == "stale-index"),
        "{err:?}"
    );
    let still = VersionMeta::get_version(&*plane.version_rows, &name, &key, history.seq)
        .await
        .unwrap()
        .expect("the history row was not expired");
    assert_eq!(still.id.as_ref(), Some(&first_id));
}

#[tokio::test]
async fn the_current_slot_is_the_gated_one_not_the_rows_recorded_identity() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();

    // The content drifts from the row — an out-of-band edit's shape,
    // written with the contract's own row writer (the version columns
    // ride the write untouched, so the recorded identity survives while
    // the content-derived elements no longer describe the key).
    let stored = base.meta.get(&name, &key).await.unwrap().expect("the row");
    base.meta
        .put(
            &name,
            &key,
            &Row {
                etag: ETag::from_content(b"drifted"),
                size: 7,
                mtime: SystemTime::now(),
                identity: 0,
                tags: stored.tags.clone(),
                checksum: stored.checksum.clone(),
                owner: stored.owner.clone(),
                acl: stored.acl.clone(),
            },
        )
        .await
        .unwrap();

    // The recorded identity is not the current version any more: the gate
    // degraded the slot, so the address resolves nothing — a body that
    // reached for the row's recorded identity (an ungated `Metadata::get`)
    // would serve the drift under it.
    let err = ops(&base)
        .get_object_version(&plane, &name, &key, Some(&first_id), None)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::NoSuchVersion(_)), "{err:?}");
    assert!(
        ops(&base)
            .head_object_version(&plane, &name, &key, Some(&first_id))
            .await
            .is_err(),
        "the head takes the same gate verdict"
    );
    // The null version is the gated truth, and it serves the current
    // bytes without an identity.
    match ops(&base)
        .get_object_version(&plane, &name, &key, None, None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"first");
            assert_eq!(
                got.info.version_id, None,
                "the degraded slot carries no identity"
            );
            assert_eq!(got.info.size, 5, "the served size is the content's");
        }
        ReadResult::Marker { .. } => panic!("the drift is content, not a marker"),
    }
}

#[tokio::test]
async fn a_current_version_read_labels_the_content_it_served() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    // The read's plane and receiver over the moving content: `fires` puts
    // the slot move on the read's own first content probe (the concurrent
    // overwrite landing inside the read), `!fires` leaves the plane still.
    let reader = |key: &object::Key, fires: bool| {
        let meta = Arc::clone(&ops(&base).meta);
        let content = MovesOnFirstStat {
            inner: (*base.content).clone(),
            rows: Arc::clone(&meta),
            bucket: name.clone(),
            key: key.clone(),
            fresh: Id::new("moved-on").unwrap(),
            moved: Arc::new(AtomicBool::new(!fires)),
        };
        let plane = base.plane_with(content.clone());
        let ops = ObjectOps {
            meta,
            content: Arc::new(content),
            metadata_failure: MetadataFailure::Fail,
            bucket_row: BucketRow::Guaranteed,
        };
        (plane, ops)
    };

    // The control: the slot stands still, and the answer carries the
    // identity the request named.
    let still = object::key("still.txt").unwrap();
    let first = put_versioned(&base, &plane, &name, &still, b"still", None).await;
    let still_id = first
        .version_id
        .clone()
        .expect("an enabled write mints an identity");
    let (still_plane, still_ops) = reader(&still, false);
    match still_ops
        .get_object_version(&still_plane, &name, &still, Some(&still_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"still");
            assert_eq!(
                got.info.version_id.as_ref(),
                Some(&still_id),
                "the version the request named is still the current one"
            );
        }
        ReadResult::Marker { .. } => panic!("the current version is content"),
    }

    // The race: the resolution sees `moving` current, the read's own
    // content probe then takes the slot to a fresh identity, and the plain
    // read serves the bytes it was about to serve — so those bytes are no
    // longer the version the request named, and the answer says the null
    // version instead of stamping them with the identifier that moved off.
    let moving = object::key("moving.txt").unwrap();
    let first = put_versioned(&base, &plane, &name, &moving, b"moving", None).await;
    let moving_id = first
        .version_id
        .clone()
        .expect("an enabled write mints an identity");
    let (moving_plane, moving_ops) = reader(&moving, true);
    match moving_ops
        .get_object_version(&moving_plane, &name, &moving, Some(&moving_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"moving");
            assert_eq!(
                got.info.version_id, None,
                "the served bytes are not the named version any more"
            );
        }
        ReadResult::Marker { .. } => panic!("the current version is content"),
    }
    // The head takes the same rule, from its own resolution.
    let head_key = object::key("moving-head.txt").unwrap();
    let first = put_versioned(&base, &plane, &name, &head_key, b"head", None).await;
    let head_id = first
        .version_id
        .clone()
        .expect("an enabled write mints an identity");
    let (head_plane, head_ops) = reader(&head_key, true);
    match head_ops
        .head_object_version(&head_plane, &name, &head_key, Some(&head_id))
        .await
        .unwrap()
    {
        HeadResult::Object(info) => assert_eq!(
            info.version_id, None,
            "the head labels what its own resolution served"
        ),
        HeadResult::Marker { .. } => panic!("the current version is content"),
    }
}

#[tokio::test]
async fn the_per_version_tag_and_acl_pairs_answer_from_the_versions_own_row() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let name = bucket(&base, "data").await;
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();
    let first = put_versioned(&base, &plane, &name, &key, b"first", Some(&owner())).await;
    let first_id = first.version_id.clone().unwrap();
    let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    let second_id = second.version_id.clone().unwrap();

    // The history version's tags and ACL are its version row's, and the
    // write lands on that row alone — the row's own stamp survives it.
    let before = plane
        .version_rows
        .get_version(&name, &key, 1)
        .await
        .unwrap()
        .expect("the superseded version's row");
    assert_ne!(before.became_noncurrent, 0, "the row is history");
    let tags = object::Tags::from_pairs([("env".to_owned(), "prod".to_owned())]).unwrap();
    ops(&base)
        .set_version_tags(&plane, &name, &key, Some(&first_id), tags.clone())
        .await
        .unwrap();
    assert_eq!(
        ops(&base)
            .version_tags(&plane, &name, &key, Some(&first_id))
            .await
            .unwrap(),
        tags
    );
    let after = plane
        .version_rows
        .get_version(&name, &key, 1)
        .await
        .unwrap()
        .expect("the superseded version's row");
    assert_eq!(after.tags, tags);
    assert_eq!(
        after.became_noncurrent, before.became_noncurrent,
        "the rewrite preserves the row's noncurrent stamp"
    );
    ops(&base)
        .set_version_acl(
            &plane,
            &name,
            &key,
            Some(&first_id),
            &Acl {
                owner: None,
                grants: grants(),
            },
        )
        .await
        .unwrap();
    let acl = ops(&base)
        .version_acl(&plane, &name, &key, Some(&first_id))
        .await
        .unwrap();
    assert_eq!(acl.grants, grants());
    assert_eq!(
        acl.owner.as_ref(),
        Some(&owner()),
        "the row's owner element is its own"
    );

    // The current version's tags and ACL are the object row's — the same
    // elements the plain tag/ACL bodies read, which is the cross-check
    // that the two paths answer one row.
    ops(&base)
        .set_version_tags(&plane, &name, &key, Some(&second_id), tags.clone())
        .await
        .unwrap();
    assert_eq!(
        ops(&base)
            .version_tags(&plane, &name, &key, Some(&second_id))
            .await
            .unwrap(),
        tags
    );
    assert_eq!(
        ops(&base).get_object_tags(&name, &key).await.unwrap(),
        tags,
        "the version-addressed write landed on the object row"
    );
    ops(&base)
        .set_version_acl(
            &plane,
            &name,
            &key,
            Some(&second_id),
            &Acl {
                owner: Some(owner()),
                grants: grants(),
            },
        )
        .await
        .unwrap();
    let current_acl = ops(&base)
        .version_acl(&plane, &name, &key, Some(&second_id))
        .await
        .unwrap();
    assert_eq!(current_acl.grants, grants());
    // The current version's row records no owner (the overwrite wrote
    // none), and the ACL put never changes it.
    assert_eq!(current_acl.owner, None);
    // The slot still names the same version: the element writes touch
    // neither the version columns nor the content.
    let slot = plane
        .version_rows
        .current_slot(&name, &key)
        .await
        .unwrap()
        .expect("a slot");
    assert_eq!(slot.version.as_ref(), Some(&second_id));
    match ops(&base)
        .get_object_version(&plane, &name, &key, Some(&second_id), None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => assert_eq!(read(got.body).await, b"second"),
        ReadResult::Marker { .. } => panic!("the current version is content"),
    }
}

#[tokio::test]
async fn a_suspended_replacement_leaves_no_history_row_and_serves_the_null_version() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Suspended)
        .await
        .unwrap();
    let first = put_versioned(&base, &plane, &name, &key, b"first", None).await;
    assert_eq!(
        first.version_id, None,
        "a suspended write records the null version"
    );
    let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    assert_eq!(second.version_id, None);
    assert!(
        plane.versions.latest(&name, &key).await.unwrap().is_none(),
        "the in-place replacement leaves no history row"
    );
    // `versionId=null` is the current null version, serving the newest
    // bytes under no identity.
    match ops(&base)
        .get_object_version(&plane, &name, &key, None, None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"second");
            assert_eq!(got.info.version_id, None);
            assert!(
                got.info.bucket_records_versions,
                "a suspended bucket records them"
            );
        }
        ReadResult::Marker { .. } => panic!("the replacement is content, not a marker"),
    }
    // The per-version pairs address the same null version.
    assert_eq!(
        ops(&base)
            .version_tags(&plane, &name, &key, None)
            .await
            .unwrap(),
        object::Tags::empty()
    );
    assert!(
        ops(&base)
            .version_tags(&plane, &name, &key, Some(&Id::new("gone").unwrap()))
            .await
            .is_err(),
        "an identity naming no version is refused"
    );
}

#[tokio::test]
async fn an_unversioned_bucket_serves_the_null_version_and_reports_no_recording() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    let info = put_versioned(&base, &plane, &name, &key, b"plain", None).await;
    assert_eq!(info.version_id, None);

    assert_eq!(
        ops(&base).bucket_versioning(&plane, &name).await.unwrap(),
        State::Unversioned
    );
    match ops(&base)
        .get_object_version(&plane, &name, &key, None, None)
        .await
        .unwrap()
    {
        ReadResult::Object(got) => {
            assert_eq!(read(got.body).await, b"plain");
            assert_eq!(got.info.version_id, None);
            assert!(
                !got.info.bucket_records_versions,
                "an unversioned bucket records no versions"
            );
        }
        ReadResult::Marker { .. } => panic!("an unversioned write is content, not a marker"),
    }
    // The state pair's write is visible, and the version pairs then answer
    // the null version's own row.
    ops(&base)
        .set_bucket_versioning(&plane, &name, State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        ops(&base).bucket_versioning(&plane, &name).await.unwrap(),
        State::Enabled
    );
    match ops(&base)
        .head_object_version(&plane, &name, &key, None)
        .await
        .unwrap()
    {
        HeadResult::Object(info) => {
            assert!(info.bucket_records_versions);
            assert_eq!(info.version_id, None);
        }
        HeadResult::Marker { .. } => panic!("the null version is content"),
    }
}

/// The engine's versioned write stamps the row it supersedes.
///
/// The mechanism is the store's (`advance_slot` records when the row it
/// moved off stopped being current, in the same commit that records the
/// incoming identity — pinned in redb's own suite), and the close-out
/// filed the missing half: nothing asserted it **through the engine
/// body**, so a composition that superseded without stamping, or stamped
/// in a second transaction, would have gone unnoticed at this level.
///
/// The ordering is the point: the stamp and the slot move are one commit,
/// so a reader that sees the incoming current version has already seen the
/// outgoing one stamped as history.
#[tokio::test]
async fn a_versioned_write_stamps_the_row_it_supersedes() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("stamped.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Enabled)
        .await
        .unwrap();

    let first = put_versioned(&base, &plane, &name, &key, b"first", None).await;
    let first_id = first
        .version_id
        .clone()
        .expect("an enabled write mints an identity");
    let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    assert_ne!(second.version_id, first.version_id);

    // The write moved the first version into history: its row is at the
    // sequence the slot left, and it says when it stopped being current.
    let (history_seq, history) = plane
        .versions
        .latest(&name, &key)
        .await
        .unwrap()
        .expect("the superseded version's history row");
    assert_eq!(
        history.id.as_ref(),
        Some(&first_id),
        "the history row is the version the write moved off"
    );
    assert_ne!(
        history.became_noncurrent, 0,
        "the engine's write stamps the row it supersedes"
    );

    // The same advance recorded the incoming identity: the engine's own
    // view of the slot names the version the write minted, at a sequence
    // the stamped row does not hold — the two halves of one commit cannot
    // be seen apart.
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert_eq!(
        slot.version.as_ref(),
        second.version_id.as_ref(),
        "the slot names the version the write minted"
    );
    assert_ne!(
        slot.seq, history_seq,
        "the slot moved off the sequence the stamped row sits at"
    );
}

/// `stat` answers `None` once, then the inner plane. The versioned put's
/// post-publish stat is that once.
#[derive(Clone)]
struct FirstStatNone {
    inner: RedbContent,
    pending: Arc<AtomicBool>,
}

#[async_trait]
impl ObjectContent for FirstStatNone {
    type Error = <RedbContent as ObjectContent>::Error;
    type Staged = <RedbContent as ObjectContent>::Staged;

    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error> {
        if self.pending.swap(false, Ordering::SeqCst) {
            return Ok(None);
        }
        self.inner.stat(bucket, key).await
    }

    async fn stage(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error> {
        self.inner.stage(bucket, key).await
    }

    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error> {
        self.inner.publish(bucket, staged, key).await
    }

    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        self.inner.open(bucket, key, range).await
    }

    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error> {
        self.inner.remove(bucket, key).await
    }

    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.inner.rename(bucket, from, to).await
    }

    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error> {
        self.inner.copy(bucket, from, to).await
    }

    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error> {
        self.inner.scan(bucket, prefix, after, limit).await
    }

    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.inner.create_bucket(bucket).await
    }

    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error> {
        self.inner.remove_bucket(bucket).await
    }

    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error> {
        self.inner.is_empty(bucket).await
    }
}

#[tokio::test]
async fn a_missing_stat_after_publish_restores_the_outgoing_version() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, _, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();
    let content = FirstStatNone {
        inner: (*base.content).clone(),
        pending: Arc::new(AtomicBool::new(true)),
    };
    let plane = base.plane_with(content.clone());
    let writer = ObjectOps {
        meta: Arc::clone(&plane.version_rows),
        content: Arc::new(content),
        metadata_failure: MetadataFailure::Fail,
        bucket_row: BucketRow::Guaranteed,
    };
    let staged = writer
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let info = writer
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap();
    assert_eq!(info.version_id, None);
    assert_eq!(info.size, 0);
    assert_eq!(info.etag, ETag::EMPTY);
    let got = writer.get_object(&name, &key, None).await.unwrap();
    assert_eq!(read(got.body).await, b"first");
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert_eq!(slot.version.as_ref(), Some(&first_id));
    assert!(
        VersionMeta::list_versions(&*plane.version_rows, &name, &key)
            .await
            .unwrap()
            .is_empty(),
        "the outgoing version was taken back out of history"
    );
}

#[tokio::test]
async fn a_failed_advance_over_a_marker_restores_the_marker() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, _) = versioned(&base, &key, b"first").await;
    let marker = delete_marker(&base, &plane, &name, &key).await;
    let marker_id = marker.id.expect("the marker carries an identity");
    base.counted().refuse_next_advance_slot();
    let staged = ops(&base)
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let err = ops(&base)
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert!(slot.is_delete_marker);
    assert_eq!(slot.version.as_ref(), Some(&marker_id));
    let err = ops(&base).get_object(&name, &key, None).await.unwrap_err();
    assert!(matches!(err, StorageError::NoSuchKey(_)), "{err:?}");
}

#[tokio::test]
async fn deleting_the_current_version_restores_an_older_row_when_latest_fails_closed() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();
    let second = put_versioned(&base, &plane, &name, &key, b"second", None).await;
    let second_id = second.version_id.expect("the overwrite mints an identity");
    base.counted().hide_next_latest();
    ops(&base)
        .delete_object_version(&plane, &name, &key, Some(&second_id))
        .await
        .unwrap();
    let got = ops(&base).get_object(&name, &key, None).await.unwrap();
    assert_eq!(read(got.body).await, b"first");
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert_eq!(slot.version.as_ref(), Some(&first_id));
}

#[tokio::test]
async fn a_failed_marker_write_restores_the_object() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, first) = versioned(&base, &key, b"first").await;
    let first_id = first.version_id.clone().unwrap();
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
    base.counted().refuse_next_marker();
    let err = ops(&base)
        .create_delete_marker(&plane, plane.version_rows.as_ref(), &name, &key)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");
    let got = ops(&base).get_object(&name, &key, None).await.unwrap();
    assert_eq!(read(got.body).await, b"first");
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert_eq!(slot.version.as_ref(), Some(&first_id));
    assert!(!slot.is_delete_marker);
    assert_eq!(
        ops(&base)
            .list_object_parts(&name, &key)
            .await
            .unwrap()
            .len(),
        1,
        "the retained parts come back with the object"
    );
    assert!(
        VersionMeta::list_versions(&*plane.version_rows, &name, &key)
            .await
            .unwrap()
            .is_empty(),
        "the superseded row is taken back with the refusal"
    );
}

#[tokio::test]
async fn a_refused_suspended_replacement_keeps_the_null_bytes() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Suspended)
        .await
        .unwrap();
    put_versioned(&base, &plane, &name, &key, b"first", None).await;
    base.counted().refuse_next_put_with_parts();
    let staged = ops(&base)
        .stage_body(&name, &key, body(&[b"second"]), None, None)
        .await
        .unwrap();
    let err = ops(&base)
        .put_object_versioned(
            &plane,
            &name,
            &key,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            plane.version_rows.as_ref(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");
    let got = ops(&base).get_object(&name, &key, None).await.unwrap();
    assert_eq!(read(got.body).await, b"first");
    assert!(
        plane.versions.latest(&name, &key).await.unwrap().is_none(),
        "the held null version is taken back with the refusal"
    );
}

#[tokio::test]
async fn a_failed_suspended_delete_keeps_the_null_bytes() {
    let base = Base::new();
    let name = bucket(&base, "data").await;
    let key = object::key("notes.txt").unwrap();
    let plane = base.version_plane();
    plane
        .versions
        .set_state(&name, State::Suspended)
        .await
        .unwrap();
    put_versioned(&base, &plane, &name, &key, b"first", None).await;
    base.counted().refuse_next_marker();
    let err = ops(&base)
        .create_delete_marker(&plane, plane.version_rows.as_ref(), &name, &key)
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Io(_)), "{err:?}");
    let got = ops(&base).get_object(&name, &key, None).await.unwrap();
    assert_eq!(read(got.body).await, b"first");
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert!(!slot.is_delete_marker);
    assert!(
        VersionMeta::list_versions(&*plane.version_rows, &name, &key)
            .await
            .unwrap()
            .is_empty(),
        "the held null version is taken back with the refusal"
    );
}

#[tokio::test]
async fn a_hidden_latest_does_not_reuse_a_live_history_sequence() {
    let base = Base::new();
    let key = object::key("notes.txt").unwrap();
    let (name, plane, _) = versioned(&base, &key, b"first").await;
    let occupant = Stored {
        id: Some(Id::new("occupant").unwrap()),
        size: 0,
        blob: BlobRef::empty(),
        etag: ETag::EMPTY,
        created: SystemTime::now(),
        became_noncurrent: 1,
        kind: Kind::Marker,
        tags: object::Tags::empty(),
        checksum: None,
        owner: None,
        acl: Acl::default_private(None),
        seq: 5,
    };
    VersionMeta::put_version(&*plane.version_rows, &name, &key, &occupant)
        .await
        .unwrap();
    base.counted().hide_next_latest();
    put_versioned(&base, &plane, &name, &key, b"second", None).await;
    let slot = plane.versions.current(&name, &key).await.unwrap();
    assert_eq!(
        slot.seq, 6,
        "the next sequence starts past the live history"
    );
    assert!(
        VersionMeta::get_version(&*plane.version_rows, &name, &key, 5)
            .await
            .unwrap()
            .is_some(),
        "the history row keeps its own sequence"
    );
}
