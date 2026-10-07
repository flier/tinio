//! The object-content contract: the bytes side of a backend, standing beside
//! the metadata rows.
//!
//! [`ObjectContent`] owns the objects and the multipart parts themselves — the
//! staged write and its publish point, the ranged read, the move, copy and
//! delete primitives, the content walk, and the bucket lifecycle of the content
//! plane. It carries no row type and no table: a backend answers it from its own
//! transport (redb + block files, a filesystem tree, PostgreSQL large objects).
//!
//! A key on this plane is a [`ContentKey`] — an object key or a part key
//! (`upload_id`, `number`) — because the multipart bytes cross the same seam as
//! the object bytes (D23). A walk yields [`ContentEntry`], whose key is the
//! **object** key (spec §1 property 3: part keys are never enumerated) and which
//! carries the identity and the optional timestamp the read-path staleness gate
//! needs (review B1, round 39): a store with no timestamp answers `None` and
//! never fabricates one. A write is staged first: [`ObjectContent::stage`] answers the
//! backend's own [`StagedObject`], the caller streams chunks into it, and
//! [`ObjectContent::publish`] makes them visible — publish is the visibility
//! point, and nothing observes a half-published object.
//!
//! Bucket names and object keys arrive pre-validated ([
//! `crate::bucket::Name`], [`crate::object::Key`]); part numbers as
//! [`crate::multipart::PartNumber`].

use std::{error::Error as StdError, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use crate::{
    bucket, multipart, object,
    storage::{self, BodyStream, ByteRange},
};

/// The key a content-plane call addresses: an object key, or one multipart
/// part's key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentKey {
    /// One object's bytes.
    Object(object::Key),
    /// One staged part's bytes, identified by its upload and its number.
    Part {
        /// The upload the part belongs to.
        upload_id: String,
        /// The part's number.
        number: multipart::PartNumber,
    },
}

impl ContentKey {
    /// The object key when this key names an object.
    pub fn object(&self) -> Option<&object::Key> {
        match self {
            Self::Object(key) => Some(key),
            Self::Part { .. } => None,
        }
    }

    /// Whether this key names a staged part rather than an object.
    pub fn is_part(&self) -> bool {
        matches!(self, Self::Part { .. })
    }
}

/// One entry of a content walk or a content probe: the **object** key, its
/// length, the backend's file identity and its optional modification time.
///
/// The key is an [`object::Key`], never a [`ContentKey`]: `scan` and `stat`
/// address objects only, so a part key is not representable here (spec §1
/// property 3). `identity` is opaque to the contract (`0` marks an unavailable
/// identity — mem's byte table has none); `mtime` is `None` when the store has
/// no timestamp (D42 as amended: a store without one must not fabricate it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentEntry {
    /// The object key this entry describes.
    pub key: object::Key,
    /// The object's length in bytes.
    pub len: u64,
    /// The backend's file identity at read time (`0` = unavailable).
    pub identity: u64,
    /// The backend's modification time (`None` = the store keeps none).
    pub mtime: Option<SystemTime>,
}

/// The staged half of a write: the handle [`ObjectContent::stage`] answers, into
/// which the caller streams the body before [`ObjectContent::publish`].
///
/// The associated error is the content contract's own (review item 4): the
/// pinned `<Self as ObjectContent>::Error` cannot be named from inside this
/// trait, and a staged write must report a typed refusal (mem's
/// `EntityTooLarge`) rather than a lossy one.
///
/// # Examples
///
/// ```ignore
/// use bytes::Bytes;
/// use tinio_core::content::{ObjectContent, StagedObject};
/// use tinio_core::{bucket, object};
///
/// async fn stage_it<C: ObjectContent>(store: &C, bucket: &bucket::Name) {
///     let key = tinio_core::content::ContentKey::Object(object::key("a.txt").unwrap());
///     let mut staged = store.stage(bucket, &key).await.unwrap();
///     staged.write(Bytes::from_static(b"hello")).await.unwrap();
///     store.publish(bucket, staged, &key).await.unwrap();
/// }
/// ```
#[async_trait]
pub trait StagedObject: Send + Sync + 'static {
    /// This backend's staged-write error; it must project into
    /// [`storage::Error`] and be the content contract's error.
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;

    /// Append one chunk to the staged object. The refusal is typed: a store at
    /// its size limit answers its own error here, before publish.
    async fn write(&mut self, chunk: Bytes) -> Result<(), Self::Error>;
}

/// The bytes-side contract: objects, parts and the content plane's buckets.
///
/// # Examples
///
/// ```ignore
/// use tinio_core::{
///     bucket,
///     content::{ContentKey, ObjectContent},
///     object,
///     storage::{self, BodyStream},
/// };
///
/// async fn put<C: ObjectContent>(
///     store: &C,
///     bucket: &bucket::Name,
///     key: &object::Key,
///     chunks: Vec<bytes::Bytes>,
/// ) -> Result<(), storage::Error> {
///     let content_key = ContentKey::Object(key.clone());
///     let mut staged = store.stage(bucket, &content_key).await.map_err(Into::into)?;
///     for chunk in chunks {
///         staged.write(chunk).await.map_err(Into::into)?;
///     }
///     store
///         .publish(bucket, staged, &content_key)
///         .await
///         .map_err(Into::into)
/// }
/// ```
#[async_trait]
pub trait ObjectContent: Send + Sync + 'static {
    /// This backend's content error; it must project into [`storage::Error`].
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;

    /// The backend's staged-write handle. Its error **is** this contract's
    /// error, so a typed refusal survives the stage/publish pair.
    type Staged: StagedObject<Error = Self::Error>;

    /// Begin a staged write at `key`. The target key is known up front (D38):
    /// a chunked store inserts its chunks under the key before publish, and the
    /// staging area is the implementation's own.
    async fn stage(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Self::Staged, Self::Error>;

    /// Publish the staged bytes at `key`. **The visibility point**: when this
    /// returns, the bytes are visible to every subsequent
    /// [`ObjectContent::open`] and [`ObjectContent::stat`]; the metadata row
    /// describing them is written after it, never before.
    async fn publish(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
    ) -> Result<(), Self::Error>;

    /// Open `key` as a stream, clipped to `range` when one is given.
    ///
    /// A missing key is an **error** (D23's pinned return has no `None` arm):
    /// the backend's own not-found error, projecting to
    /// [`storage::Error::NoSuchKey`] for an object. A caller that must tell
    /// absence from emptiness asks
    /// [`ObjectContent::stat`] (or, for an object, the metadata row) first. An
    /// unsatisfiable range is the caller's to reject before the call, so this
    /// only resolves the requested window.
    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error>;

    /// Remove `key`'s bytes (idempotent).
    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Self::Error>;

    /// Rename an object's bytes. `Err` when the source holds nothing — the
    /// metadata move is the caller's, and a rename that moved no bytes must not
    /// be recorded as a move. A move **onto the same key** (`from == to`) is a
    /// no-op: the bytes are already where they are asked to go, and a
    /// read-then-write-then-remove would delete them (R14 — the rule
    /// `ObjectMeta::move_` carries).
    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error>;

    /// Copy an object's bytes within one bucket (D23: same-bucket
    /// object-to-object only; a cross-bucket or ranged copy streams). A copy
    /// **onto the same key** is the same no-op rename answers, so the pair
    /// reads alike.
    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Self::Error>;

    /// One page of the bucket's objects, in ascending key order: entries at or
    /// after `after` (exclusive when given), capped at `limit`. `prefix` is a
    /// bound the implementation **must not scan past**, and `after` is a
    /// **filter** over that prefix's range rather than a walk start: a value
    /// sorting below `prefix` cannot shorten the page (S3's `start-after` is
    /// any client key). Only object keys are yielded — part keys are never
    /// enumerated (property 3).
    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Self::Error>;

    /// The object's entry as the content plane sees it (`None` when it holds
    /// nothing) — the cheap probe a listing or a scheduling decision uses,
    /// never a rehash.
    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Self::Error>;

    /// Create the bucket's content namespace (fs creates the directory).
    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;

    /// Remove the bucket's content namespace (idempotent). The caller has
    /// established emptiness; a backend that also counts content answers
    /// [`storage::Error::NotEmpty`] rather than losing bytes.
    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;

    /// Whether the bucket holds no content the backend counts: no object bytes
    /// and no staged part bytes. Upload rows are a separate probe
    /// ([`crate::meta::UploadMeta::has_uploads`]) — this is the content-side
    /// authority the engine's `delete_bucket` refusal reads.
    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Self::Error>;
}

/// Stream `body` into `staged`, one chunk at a time, and answer the number of
/// bytes that landed.
///
/// The streaming counterpart of
/// [`collect_body`](crate::storage::collect_body) for the content contract's
/// write path: a caller that must not hold the object whole (the version
/// engine's materialize) pumps through this, and the answer is the **landed**
/// byte count — the bytes the stage was actually given, which is what a caller
/// recording a row must record (the read path's staleness gate compares the row
/// against the file).
///
/// A chunk's error projects onto [`storage::Error`] through
/// [`storage::io`]; a stage's own typed refusal keeps its
/// projection ([StagedObject::Error] is required to have one).
///
/// # Examples
///
/// ```ignore
/// let mut staged = store.stage(bucket, &key).await?;
/// let landed = stage_body(&mut staged, body).await?;
/// store.publish(bucket, staged, &key).await?;
/// ```
pub async fn stage_body<S>(staged: &mut S, body: BodyStream) -> Result<u64, storage::Error>
where
    S: StagedObject + ?Sized,
{
    let mut body = body;
    let mut landed = 0u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(storage::io)?;
        landed += chunk.len() as u64;
        staged.write(chunk).await.map_err(Into::into)?;
    }
    Ok(landed)
}

#[cfg(test)]
mod tests {
    use std::io;

    use futures::stream;

    use super::*;

    /// A stage that keeps what it is given, for the `stage_body` legs.
    struct Collect(Vec<u8>);

    #[async_trait]
    impl StagedObject for Collect {
        type Error = storage::Error;

        async fn write(&mut self, chunk: Bytes) -> Result<(), Self::Error> {
            self.0.extend_from_slice(&chunk);
            Ok(())
        }
    }

    /// `stage_body` is the streaming stage: every chunk lands, in order, and
    /// the answer is the **landed** byte count.
    #[tokio::test]
    async fn stage_body_streams_every_chunk_and_answers_what_landed() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"hel")),
            Ok(Bytes::from_static(b"lo")),
        ]));
        let mut stage = Collect(Vec::new());
        let landed = stage_body(&mut stage, body).await.unwrap();
        assert_eq!(
            landed, 5,
            "the count is what landed, not what was asked for"
        );
        assert_eq!(stage.0, b"hello", "every chunk lands, in order");
    }

    /// A failed chunk is the projection, and nothing after it is staged.
    #[tokio::test]
    async fn stage_body_projects_a_failed_chunk() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"head")),
            Err(io::Error::other("boom")),
            Ok(Bytes::from_static(b"tail")),
        ]));
        let mut stage = Collect(Vec::new());
        let err = stage_body(&mut stage, body).await.unwrap_err();
        assert!(matches!(err, storage::Error::Io(_)));
        assert_eq!(stage.0, b"head", "the tail never reaches the stage");
    }

    #[test]
    fn a_content_key_tells_an_object_from_a_part() {
        let object = ContentKey::Object(object::key("a.txt").unwrap());
        let named = object.object().expect("an object key");
        assert_eq!(&**named, "a.txt");
        assert!(!object.is_part());

        let part = ContentKey::Part {
            upload_id: "u1".into(),
            number: multipart::part_number(3).unwrap(),
        };
        assert_eq!(part.object(), None);
        assert!(part.is_part());
    }

    #[test]
    fn a_content_entry_carries_the_gate_inputs_and_never_fabricates_a_time() {
        let entry = ContentEntry {
            key: object::key("a.txt").unwrap(),
            len: 5,
            identity: 9,
            mtime: None,
        };
        assert_eq!(
            entry.key.as_ref(),
            "a.txt",
            "the entry names the object key"
        );
        assert_eq!(entry.len, 5);
        assert_eq!(entry.identity, 9);
        assert_eq!(
            entry.mtime, None,
            "a store with no timestamp answers None (D42)"
        );
        let timed = ContentEntry {
            mtime: Some(SystemTime::UNIX_EPOCH),
            ..entry.clone()
        };
        assert_ne!(timed, entry);
    }
}
