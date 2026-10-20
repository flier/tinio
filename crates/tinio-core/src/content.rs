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

use std::{error::Error as StdError, io::Error as IoError, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use derive_more::{From, Into};
use futures::StreamExt;
use md5::{Digest as _, Md5};
use tracing::{Span, field, instrument};

use crate::{
    bucket,
    etag::ETag,
    multipart, object,
    storage::{self, BodyStream, ByteRange},
};

/// Whether `body` is the single-part content `etag` names.
///
/// A composed ETag is never a content MD5, so it does not match. The body is
/// hashed as it arrives; nothing here retains it.
///
/// # Errors
///
/// The stream's own I/O failure.
pub async fn body_matches_etag(body: &mut BodyStream, etag: &ETag) -> Result<bool, IoError> {
    let ETag::Single(want) = etag else {
        return Ok(false);
    };
    let mut hasher = Md5::new();
    while let Some(chunk) = body.next().await {
        hasher.update(&chunk?);
    }
    Ok(<[u8; 16]>::from(hasher.finalize()) == *want)
}

/// The key a content-plane call addresses: an object key, or one multipart
/// part's key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

/// The identity of one publish, minted by the caller: what
/// [`ObjectContent::remove_if_tagged`] matches against.
///
/// Two writers can publish byte-identical content at one key — an empty body
/// matches an empty body — and a byte comparison cannot tell their publishes
/// apart. A plane that records the tag answers the stale compensation's
/// `false` instead of deleting the later writer's bytes; a plane that keeps no
/// identity falls back to the byte compare, documented per method.
///
/// # Examples
///
/// ```
/// use tinio_core::content::PublishId;
///
/// let id = PublishId::from(7_u64);
/// assert_eq!(u64::from(id), 7);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, From, Into)]
pub struct PublishId(pub(crate) u64);

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
    /// This backend's content error; it must project into [`storage::Error`]
    /// and absorb a stream's I/O failure (the digest compare re-reads the body).
    type Error: StdError + Send + Sync + 'static + Into<storage::Error> + From<IoError>;

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

    /// Whether a publish through [`publish_tagged`](Self::publish_tagged) is
    /// recorded, so a later compensation can match that publish and leave a
    /// newer writer's bytes alone.
    ///
    /// The default is `false`: the tag is dropped and the compensation falls
    /// back to a byte compare. A plane that stores the tag overrides this.
    fn records_publish_identity(&self) -> bool {
        false
    }

    /// Remove every key's bytes (idempotent; an empty batch removes
    /// nothing).
    ///
    /// The default is one [`remove`](Self::remove) per key, which is what a
    /// backend whose bytes live one file per key wants. A backend whose
    /// bytes share one store overrides it to commit the batch together —
    /// the caller's rows are already gone by then, so a partial batch would
    /// leave bytes nothing can reach (D23's byte plane).
    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[ContentKey],
    ) -> Result<(), Self::Error> {
        for key in keys {
            self.remove(bucket, key).await?;
        }
        Ok(())
    }

    /// Remove `key`'s bytes only while they are still `expected`.
    ///
    /// The guard a failed write's compensation needs. Two writers can publish
    /// the same key, and the one whose row write was refused must not delete
    /// the bytes the other published in its place — a plain
    /// [`remove`](Self::remove) cannot tell them apart.
    ///
    /// Answers whether the bytes were removed. `false` covers both "they
    /// differ" and "there are none": absence is not an error, the idempotence
    /// [`remove`](Self::remove) has. The default reads the bytes back and
    /// compares, then removes; a backend whose bytes and comparison share one
    /// store overrides it to compare and remove in one commit.
    ///
    /// The default cannot tell a backend's own "no such key" error from a real
    /// read fault — a failed [`open`](Self::open) and a failed stream read both
    /// answer `false`, the safe direction for a removal: the bytes are left
    /// where they are. A backend that needs that distinction keeps its
    /// override, where the compare and the removal share the store's own error
    /// type — the shared store's and the decorator's both do.
    async fn remove_if_matches(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
    ) -> Result<bool, Self::Error> {
        let Ok(body) = self.open(bucket, key, None).await else {
            return Ok(false);
        };
        let mut stored = Vec::new();
        let mut body = body;
        while let Some(chunk) = futures::StreamExt::next(&mut body).await {
            // The stream's error is an `io::Error`, which the contract's
            // associated type is not required to accept: there is no portable
            // way to hand it back, and inventing one would widen every
            // implementation's error. Unreadable bytes are therefore "not
            // mine", the safe direction for a removal guard.
            let Ok(bytes) = chunk else {
                return Ok(false);
            };
            stored.extend_from_slice(&bytes);
        }
        if stored != expected {
            return Ok(false);
        }
        self.remove(bucket, key).await?;
        Ok(true)
    }

    /// Publish the staged bytes at `key`, tagged with `id` — the identity a
    /// later [`remove_if_tagged`](Self::remove_if_tagged) matches.
    ///
    /// The default drops the tag and delegates to
    /// [`publish`](Self::publish): a plane that keeps no per-publish identity
    /// has nowhere to record it, and its [`remove_if_tagged`](Self::remove_if_tagged)
    /// falls back to the byte compare with the same blind spot for
    /// byte-identical bodies — documented, not silently different. `id` must
    /// be unique per publish at least across the window a compensation lives
    /// in (one publish and the row write that follows it).
    async fn publish_tagged(
        &self,
        bucket: &bucket::Name,
        staged: Self::Staged,
        key: &ContentKey,
        id: PublishId,
    ) -> Result<(), Self::Error> {
        // A plane without publish identity has nowhere to keep the tag.
        let _ = id;
        self.publish(bucket, staged, key).await
    }

    /// Remove `key`'s bytes only while they are still the publish `id` named.
    ///
    /// The guard that closes [`remove_if_matches`](Self::remove_if_matches)'s
    /// blind spot: two publishes of byte-identical content are indistinguishable
    /// by their bytes, so a stale writer's compensation must match the
    /// *publish*, not the content. `expected` is the fallback the compare uses
    /// on a plane that keeps no identity.
    ///
    /// The default ignores the tag and answers exactly
    /// [`remove_if_matches`](Self::remove_if_matches) — a plane that dropped
    /// the tag at publish keeps the byte compare's blind spot, documented
    /// rather than silent. A plane that records the tag answers `false` for a
    /// stale one even when the bytes are equal.
    async fn remove_if_tagged(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        id: PublishId,
    ) -> Result<bool, Self::Error> {
        let _ = id;
        self.remove_if_matches(bucket, key, expected).await
    }

    /// Remove `key` when it is still publish `id` and a content MD5 of the
    /// stored bytes equals `etag`.
    ///
    /// A composed ETag never matches: the compare is the single-part digest,
    /// so a multipart object is left in place. The default re-reads the body
    /// and, on a match, hands those bytes to
    /// [`remove_if_tagged`](Self::remove_if_tagged) — the byte compare is what
    /// a plane with no publish identity can still decide. A plane that already
    /// holds the bytes overrides this and hashes them in place, so the caller
    /// keeps no second copy of the object.
    async fn remove_if_etag(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        etag: &ETag,
        id: PublishId,
    ) -> Result<bool, Self::Error> {
        let ETag::Single(want) = etag else {
            return Ok(false);
        };
        let mut body = self.open(bucket, key, None).await?;
        let mut hasher = Md5::new();
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(Self::Error::from)?;
            hasher.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        if <[u8; 16]>::from(hasher.finalize()) != *want {
            return Ok(false);
        }
        self.remove_if_tagged(bucket, key, &bytes, id).await
    }

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
/// The pump itself is [`stage_body_md5`], which is this call with the digest
/// kept: a write path that records a content ETag takes that form.
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
    stage_body_md5(staged, body).await.map(|(landed, _)| landed)
}

/// Stream `body` into `staged`, one chunk at a time, and answer both the
/// number of bytes that landed and the content MD5 over them.
///
/// The one implementation of the body pump: [`stage_body`] is this function
/// with the digest dropped, and [`hash_body`] is it with a stage that keeps
/// nothing. The answer's count is the same **landed** byte count
/// [`stage_body`] explains, and the digest is what a write path recording a
/// content ETag stores (a multipart part's MD5, an object commit's).
///
/// A chunk's error projects onto [`storage::Error`] through
/// [`storage::io`]; a stage's own typed refusal keeps its
/// projection ([StagedObject::Error] is required to have one).
///
/// # Examples
///
/// ```ignore
/// let mut staged = store.stage(bucket, &key).await?;
/// let (landed, digest) = stage_body_md5(&mut staged, body).await?;
/// let etag = ETag::Single(digest);
/// store.publish(bucket, staged, &key).await?;
/// ```
#[instrument(
    target = "tinio::storage",
    level = "debug",
    skip_all,
    fields(bytes = field::Empty)
)]
pub async fn stage_body_md5<S>(
    staged: &mut S,
    body: BodyStream,
) -> Result<(u64, [u8; 16]), storage::Error>
where
    S: StagedObject + ?Sized,
{
    let mut body = body;
    let mut landed = 0u64;
    let mut hasher = Md5::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(storage::io)?;
        landed += chunk.len() as u64;
        hasher.update(&chunk);
        staged.write(chunk).await.map_err(Into::into)?;
    }
    Span::current().record("bytes", landed);
    Ok((landed, hasher.finalize().into()))
}

/// The content MD5 of `body`, hashed as it arrives, with nothing kept.
///
/// The stage-less form of [`stage_body_md5`]: the pump drains into a stage
/// that drops every chunk, for a caller that holds bytes it did not write (a
/// read-path heal re-hashing the object it just opened).
/// [`body_matches_etag`] is the comparison form of the same walk.
///
/// # Errors
///
/// The stream's own failure, projected the way the pump projects it.
#[instrument(
    target = "tinio::storage",
    level = "debug",
    skip_all,
    fields(bytes = field::Empty)
)]
pub async fn hash_body(body: BodyStream) -> Result<[u8; 16], storage::Error> {
    let (landed, digest) = stage_body_md5(&mut Discard, body).await?;
    Span::current().record("bytes", landed);
    Ok(digest)
}

/// The pump's sink for a caller that has no stage: every chunk is dropped.
struct Discard;

#[async_trait]
impl StagedObject for Discard {
    type Error = storage::Error;

    async fn write(&mut self, _chunk: Bytes) -> Result<(), Self::Error> {
        Ok(())
    }
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

    /// `stage_body_md5` is the same pump with the digest kept: the count is
    /// `stage_body`'s, and the digest is the content MD5 of what landed.
    #[tokio::test]
    async fn stage_body_md5_answers_the_landed_count_beside_the_digest() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"hel")),
            Ok(Bytes::from_static(b"lo")),
        ]));
        let mut stage = Collect(Vec::new());
        let (landed, digest) = stage_body_md5(&mut stage, body).await.unwrap();
        assert_eq!(landed, 5, "the count is `stage_body`'s");
        assert_eq!(stage.0, b"hello", "every chunk lands, in order");
        assert_eq!(
            ETag::Single(digest),
            ETag::from_content(b"hello"),
            "the digest is the content MD5 of what landed"
        );
    }

    /// `hash_body` is the pump with no stage: nothing is kept, the digest is
    /// the one the stage form answers, and a failed chunk is the projection.
    #[tokio::test]
    async fn hash_body_hashes_the_stream_and_keeps_nothing() {
        let body: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"hel")),
            Ok(Bytes::from_static(b"lo")),
        ]));
        assert_eq!(
            ETag::Single(hash_body(body).await.unwrap()),
            ETag::from_content(b"hello"),
            "the digest is the content MD5 of the whole stream"
        );

        let failing: BodyStream = Box::pin(stream::iter([
            Ok(Bytes::from_static(b"head")),
            Err(io::Error::other("boom")),
        ]));
        let err = hash_body(failing).await.unwrap_err();
        assert!(matches!(err, storage::Error::Io(_)));
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
