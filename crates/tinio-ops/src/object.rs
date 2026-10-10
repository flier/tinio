//! The object group: the object lifecycle, the tag/ACL element pairs, the
//! retained part list and the read-heal, over the row plane and the content
//! plane.
//!
//! Every body is a method of [`ObjectOps`], and every one of them is written
//! against the two contracts only: the bytes and their visibility point are the
//! content plane's ([`ObjectContent`]), the rows are the row plane's
//! ([`Metadata`] with [`ObjectMeta`]), and the two
//! policies the
//! receiver carries decide how a row is written.
//!
//! Four rules shape the group.
//!
//! - **Supersede before publish, row last.** A write that creates a version
//!   moves the outgoing content into the block plane **before** the publish
//!   overwrites it, then publishes, then writes the row, then advances the
//!   slot. The first draft of the design had that order reversed; publishing
//!   first destroys the bytes the supersede needs.
//! - **One contract call per atomic step.** The engine never decomposes a
//!   contract method's atomicity into several calls: a rename moves the bytes
//!   by `ObjectContent::rename` and the metadata by `ObjectMeta::move_`, a
//!   copy moves the bytes by `ObjectContent::copy`, and a row write is one
//!   call.
//! - **Reads heal, they do not refuse.** A read whose row is absent or stale
//!   against [`ObjectContent::stat`] recomputes the content ETag and persists
//!   the row, so a hand-dropped file keeps being served; the persist degrades
//!   on a read-only store (the crate's one read-path rule) instead of failing
//!   the read. A listing heals the same way, through one row batch per page.
//! - **A version-free shape and a version-creating shape, never one body with
//!   a flag.** `tinio-mem` builds no version plane (E16), so the version-free
//!   write is no body of this crate's: it is the core trait's default
//!   `put_object` over the engine's stage and commit — the shape both backends
//!   take — and the version-creating shape is
//!   [`ObjectOps::put_object_versioned`] beside the version group. The
//!   version-creating delete stays in the backends — writing the marker as a
//!   version row would displace the one null version. A backend's `Storage`
//!   implementation asks its own store which put it runs.
//!
//! The version-free bodies serve `version_id: None` and
//! `bucket_records_versions: false`: the object row carries no version columns
//! (D37) and the plane-free receiver has no handle to the version rows, so the
//! version-disclosure fields are the **caller's** to fill when its bucket
//! records versions — the same adapter seam `tinio-mem`'s listing has today.
//!
//! # Examples
//!
//! The group over the in-memory reference planes — the shape a backend wires:
//! its own row plane, its own content plane, its own policies.
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use futures::stream;
//! use tinio_core::{
//!     acl::Acl,
//!     bucket,
//!     content::{ObjectContent, StagedObject},
//!     meta::ObjectMeta,
//!     object,
//! };
//! use tinio_ops::{BucketOps, BucketRow, MetadataFailure, ObjectOps};
//! use tinio_store_redb::{RedbContent, RedbMetadata};
//!
//! # struct NoHooks;
//! # #[async_trait::async_trait]
//! # impl tinio_ops::Hooks for NoHooks {
//! #     async fn before_bucket_delete(
//! #         &self,
//! #         _bucket: &bucket::Name,
//! #     ) -> Result<(), tinio_core::storage::Error> {
//! #         Ok(())
//! #     }
//! #     async fn lock_part(
//! #         &self,
//! #         _bucket: &bucket::Name,
//! #         _upload_id: &str,
//! #     ) -> Box<dyn tinio_ops::Guard> {
//! #         Box::new(tinio_ops::NoGuard)
//! #     }
//! # }
//! # #[tokio::main]
//! # async fn main() {
//! let meta = Arc::new(RedbMetadata::in_memory().unwrap());
//! let content = Arc::new(RedbContent::from_driver(meta.driver()));
//! let buckets = BucketOps {
//!     meta: Arc::clone(&meta),
//!     content: Arc::clone(&content),
//!     hooks: NoHooks,
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//! let objects = ObjectOps {
//!     meta: Arc::clone(&meta),
//!     content: Arc::clone(&content),
//!     metadata_failure: MetadataFailure::Fail,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//!
//! let name = bucket::name("data").unwrap();
//! buckets
//!     .create_bucket(&name, None, &Acl::default_private(None))
//!     .await
//!     .unwrap();
//!
//! let key = object::key("notes.txt").unwrap();
//! let body = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"hello"))]));
//! // The version-free write: the core trait's default `put_object` is this
//! // pair — stage, then commit with empty tags, no owner, the default ACL.
//! let staged = objects
//!     .stage_body(&name, &key, body, None, None)
//!     .await
//!     .unwrap();
//! let put = objects
//!     .commit_object(
//!         &name,
//!         &key,
//!         staged,
//!         object::Tags::empty(),
//!         None,
//!         &Acl::default_private(None),
//!     )
//!     .await
//!     .unwrap();
//! assert_eq!(put.etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
//!
//! let head = objects.head_object(&name, &key).await.unwrap();
//! assert_eq!(head.size, 5, "the read serves what the content holds");
//! let tags = object::Tags::from_pairs([("team".to_owned(), "core".to_owned())]).unwrap();
//! objects.put_object_tags(&name, &key, &tags).await.unwrap();
//! assert_eq!(objects.get_object_tags(&name, &key).await.unwrap(), tags);
//!
//! // A planted key with no row is served from the content, and healed.
//! let planted = object::key("planted.txt").unwrap();
//! let content_key = tinio_core::content::ContentKey::Object(planted.clone());
//! let mut staged = content.stage(&name, &content_key).await.unwrap();
//! staged
//!     .write(bytes::Bytes::from_static(b"dropped"))
//!     .await
//!     .unwrap();
//! content.publish(&name, staged, &content_key).await.unwrap();
//! let healed = objects.head_object(&name, &planted).await.unwrap();
//! assert_eq!(healed.etag, tinio_core::ETag::from_content(b"dropped"));
//! assert!(
//!     meta.get(&name, &planted).await.unwrap().is_some(),
//!     "the read healed the row"
//! );
//! # }
//! ```

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, SystemTime},
};

use crate::{
    _core::{
        ETag, acl,
        acl::{Acl, AclGrants},
        bucket, checksum,
        content::{ContentEntry, ContentKey, ObjectContent, PublishId, hash_body, stage_body_md5},
        meta::{
            Metadata, ObjectMeta,
            object::{Identity, Preserved, Row},
        },
        multipart::ObjectPart,
        object,
        storage::{self, BodyStream, ByteRange, Error, GetObjectResult, TagWriteResult},
        version,
    },
    BucketRow, MetadataFailure, StagedBody,
    ops::{projected, served_without_writing},
};

/// The jitter window the composed-ETag keep rule falls back to when no file
/// identity is available (a stored or current identity of `0` — platforms or
/// filesystems without one): a same-size mtime drift within this window is a
/// touch (an antivirus or indexer scan) and keeps a multipart `MD5-of-MD5s-N`
/// ETag; a larger drift re-hashes, because a wrong ETag is never served.
///
/// Where the file identity exists (unix dev+inode; Windows volume serial +
/// file index) the comparison is exact and this window is unused. One bounded
/// platform limitation on both branches: Windows `FILETIME` granularity
/// (~16 ms) means a rewrite landing in the same tick as the recorded mtime is
/// indistinguishable, so the stale ETag is served until the next change.
pub(crate) const COMPOSED_MTIME_JITTER: Duration = Duration::from_secs(60);

/// The identity one publish carries on a store whose failed row write is
/// compensated.
///
/// The contract asks only that an id be unique per publish across the window a
/// compensation lives in — one publish and the row write that follows it — so a
/// process-local sequence mixed with the clock is enough, and it keeps two
/// runs (and two writers within one run) apart without a dependency the engine
/// does not already have.
fn mint_publish_id() -> PublishId {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let sequence = NEXT.fetch_add(1, AtomicOrdering::Relaxed);
    let clock = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_nanos() as u64)
        .unwrap_or_default();
    PublishId::from(sequence ^ clock)
}

/// The absolute time difference between two instants (symmetric — a drift into
/// the past counts the same as one into the future).
pub(crate) fn mtime_drift(a: SystemTime, b: SystemTime) -> Duration {
    a.duration_since(b)
        .unwrap_or_else(|_| b.duration_since(a).unwrap_or_default())
}

/// Whether a stored row still describes the content `entry` names — the
/// staleness gate a read and a listing share.
///
/// The size must agree, the mtime must agree **when the store has one** (a
/// store with no timestamp must not fabricate one, D42), and the file identity
/// is compared only when both sides expose one (`0` on either side falls back
/// to the pair alone — the mtime-preserving-replacement hole the identity
/// closes where it exists).
pub(crate) fn row_matches(row: &Row, entry: &ContentEntry) -> bool {
    row.size == entry.len
        && entry
            .mtime
            .is_none_or(|mtime| storage::to_nanos(row.mtime) == storage::to_nanos(mtime))
        && (row.identity == 0 || entry.identity == 0 || row.identity == entry.identity)
}

/// The composed-ETag keep decision: whether a stored `MD5-of-MD5s-N` ETag on
/// the same-size content may be kept without re-hashing.
///
/// On platforms with a file identity (both nonzero) the check is exact — the
/// identity must match **and** the mtime must be unchanged, because an
/// in-place same-size rewrite keeps the identity and is indistinguishable from
/// a touch without hashing. Where no identity exists, the mtime jitter window
/// is the fallback ([`COMPOSED_MTIME_JITTER`]). A store with no timestamp at
/// all cannot confirm the touch and re-hashes: a stale composed ETag is never
/// served.
pub(crate) fn composed_keep(row: &Row, entry: &ContentEntry) -> bool {
    if !matches!(row.etag, ETag::Composed(_, _)) || row.size != entry.len {
        return false;
    }
    let Some(mtime) = entry.mtime else {
        return false;
    };
    if row.identity != 0 && entry.identity != 0 {
        entry.identity == row.identity && mtime_drift(mtime, row.mtime) == Duration::ZERO
    } else {
        mtime_drift(mtime, row.mtime) <= COMPOSED_MTIME_JITTER
    }
}

/// What the object group's bodies need: the row plane, the content plane and
/// the two policies.
///
/// The group reaches no hook (E4 holds the seam at two methods, and the object
/// group calls neither), so the receiver carries only the handles its bodies
/// use — the same per-group shape [`crate::BucketOps`] has.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
///
/// use tinio_ops::{BucketRow, MetadataFailure, ObjectOps};
///
/// let objects = ObjectOps {
///     meta: Arc::clone(&rows),
///     content: Arc::clone(&content),
///     metadata_failure: MetadataFailure::Fail,
///     bucket_row: BucketRow::Guaranteed,
/// };
/// let head = objects.head_object(&bucket, &key).await?;
/// ```
pub struct ObjectOps<M, C> {
    /// The row plane: object, bucket, upload and version rows (`Metadata`).
    pub meta: Arc<M>,
    /// The content plane: object and part bytes, and the bucket namespace.
    pub content: Arc<C>,
    /// What a metadata-row write that fails after the content is published
    /// means ([`MetadataFailure`]).
    pub metadata_failure: MetadataFailure,
    /// Whether the backend's buckets require their row ([`BucketRow`]).
    pub bucket_row: BucketRow,
}

impl<M, C> ObjectOps<M, C>
where
    M: Metadata,
    C: ObjectContent,
{
    /// The content entry a key must have, or `NoSuchKey`.
    pub(crate) async fn require_content(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<ContentEntry, Error> {
        match self.content.stat(bucket, key).await.map_err(projected)? {
            Some(entry) => Ok(entry),
            None => Err(storage::no_such_key(key)),
        }
    }

    /// The content's MD5, streamed — the ETag a healed row records.
    async fn content_etag(&self, bucket: &bucket::Name, key: &object::Key) -> Result<ETag, Error> {
        let body = self
            .content
            .open(bucket, &ContentKey::Object(key.clone()), None)
            .await
            .map_err(projected)?;
        Ok(ETag::Single(hash_body(body).await.map_err(projected)?))
    }

    /// The row a read serves for `entry`: the stored one while it still
    /// describes the content, else the refreshed one, persisted.
    ///
    /// The refresh computes the content ETag (`open` and hash) — keeping a
    /// stored **composed** ETag when the composed-keep rule holds, because a
    /// multipart `MD5-of-MD5s-N` form has no content-derived equivalent — and
    /// carries the recorded elements (tags, checksum, owner, ACL) over. The
    /// persist is the contract's read-path heal ([`ObjectMeta::put_preserving`]),
    /// which refreshes the row **and leaves the key's retained part list
    /// alone** — a heal is not an overwrite.
    ///
    /// The receiver's [`BucketRow`] policy decides one thing around it: the
    /// preserving write is a **guarded** write, so a store whose buckets
    /// outlive their rows records the bucket row it needs first, lazily (the
    /// same first-sight write `head_bucket` makes). That added bucket-row
    /// write is the [`BucketRow::Lazy`] policy's documented side effect: a
    /// bucket a read has served has been sighted.
    ///
    /// Either write **degrades** on a read-only store — the value the write
    /// would have produced is served, with one event, and nothing is
    /// persisted, the bucket row included — because a read is not failed by a
    /// store it cannot write (the crate's one read-path rule).
    pub(crate) async fn healed_row(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        entry: &ContentEntry,
    ) -> Result<Row, Error> {
        let stored = ObjectMeta::get(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        if let Some(row) = &stored
            && row_matches(row, entry)
        {
            return Ok(row.clone());
        }
        let etag = match &stored {
            Some(row) if composed_keep(row, entry) => row.etag.clone(),
            _ => self.content_etag(bucket, key).await?,
        };
        let mtime = entry.mtime.unwrap_or_else(|| {
            stored
                .as_ref()
                .map_or_else(SystemTime::now, |row| row.mtime)
        });
        let identity = Identity {
            etag,
            size: entry.len,
            mtime,
            file_identity: entry.identity,
        };
        if self.bucket_row == BucketRow::Lazy {
            // The guarded write needs a bucket row; a store whose buckets
            // outlive their rows may never have recorded one. Recording it is
            // the lazy first sight — and it degrades exactly as the heal's own
            // write does, so a read-only store still serves.
            let now = SystemTime::now();
            served_without_writing(
                self.meta
                    .record_if_absent(bucket, now)
                    .await
                    .map_err(projected),
                now,
                "the bucket row a lazy read-heal records",
            )?;
        }
        let preserved = served_without_writing(
            ObjectMeta::put_preserving(&*self.meta, bucket, key, &identity)
                .await
                .map_err(projected),
            stored.as_ref().map_or_else(Preserved::none, Row::preserved),
            "the object read-heal",
        )?;
        Ok(Row {
            etag: identity.etag,
            size: identity.size,
            mtime: identity.mtime,
            identity: identity.file_identity,
            tags: preserved.tags,
            checksum: preserved.checksum,
            owner: preserved.owner,
            acl: preserved.acl,
        })
    }

    /// Write one object row under the receiver's [`BucketRow`] policy: the
    /// guarded form (the bucket row must exist, inside the contract's own
    /// transaction, so a concurrent teardown cannot be raced) or the plain
    /// upsert.
    ///
    /// The write is the object row **alone**: an overwrite is a fresh object,
    /// so the retained part list dies with it — what the contract's `put` and
    /// `put_guarded` do, and what a commit needs. A heal is the opposite case
    /// and takes [`ObjectMeta::put_preserving`] instead.
    pub(crate) async fn write_row(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
    ) -> Result<(), Error> {
        match self.bucket_row {
            BucketRow::Guaranteed => ObjectMeta::put_guarded(&*self.meta, bucket, key, row)
                .await
                .map_err(projected),
            BucketRow::Lazy => ObjectMeta::put(&*self.meta, bucket, key, row)
                .await
                .map_err(projected),
        }
    }

    /// The write's shared tail: publish the staged bytes at `key` and record
    /// the row the content now describes.
    #[allow(clippy::too_many_arguments)]
    async fn publish_and_record(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody<C::Staged>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
        records_versions: bool,
    ) -> Result<object::Info, Error> {
        let (staged, _staged_key, _len, etag, checksum) = staged.into_parts();
        let content_key = ContentKey::Object(key.clone());
        // The visibility point is the contract's: when this returns, the bytes
        // are visible to every read. A store whose `Fail` policy makes the row
        // write the operation's answer publishes through the **tagged** form,
        // so the compensation the engine runs can match its own publish and
        // never a newer writer's; a store whose failed row is healed by the
        // next read (`Warn`) publishes untagged and takes nothing back.
        let publish_id = match self.metadata_failure {
            MetadataFailure::Warn => None,
            MetadataFailure::Fail => Some(mint_publish_id()),
        };
        match publish_id {
            Some(id) => self
                .content
                .publish_tagged(bucket, staged, &content_key, id)
                .await
                .map_err(projected)?,
            None => self
                .content
                .publish(bucket, staged, &content_key)
                .await
                .map_err(projected)?,
        }
        // The content-side truth after the publish. A publish that left no
        // content behind is a folder marker (the content plane created the
        // namespace and holds no bytes), and a marker holds no row: the write
        // params are accept-and-dropped, exactly as the S3 surface has it.
        //
        // No engine test reaches this branch: the in-memory reference content
        // plane keeps every key it is given (a marker key is an ordinary key
        // there), so only a plane with a directory notion — fs's — can answer
        // `None` here. A backend that keeps its own marker branch (fs does,
        // for its marker `last_modified`) may take it before this body.
        let Some(entry) = self.content.stat(bucket, key).await.map_err(projected)? else {
            return Ok(Self::marker_info(key));
        };
        let mtime = entry.mtime.unwrap_or_else(SystemTime::now);
        let row = Row {
            etag: etag.clone(),
            size: entry.len,
            mtime,
            identity: entry.identity,
            tags: tags.clone(),
            checksum: checksum.clone(),
            owner: owner.cloned(),
            acl: Acl {
                owner: None,
                grants: acl.grants.clone(),
            },
        };
        match self.write_row(bucket, key, &row).await {
            Ok(()) => {}
            Err(err) => match self.metadata_failure {
                MetadataFailure::Warn => {
                    tracing::warn!(
                        bucket = %bucket,
                        key = %key,
                        error = %err,
                        "the object row was not persisted after the publish"
                    );
                }
                MetadataFailure::Fail => {
                    // The caller's compensation, taken here: the bytes this
                    // call published are taken back, and nothing else is.
                    if let Err(failure) = self
                        .compensate_publish(bucket, key, publish_id, &etag)
                        .await
                    {
                        tracing::warn!(
                            bucket = %bucket,
                            key = %key,
                            error = %failure,
                            "the compensation for a refused object row failed"
                        );
                    }
                    return Err(err);
                }
            },
        }
        Ok(object::Info {
            key: key.clone(),
            size: entry.len,
            last_modified: mtime,
            etag,
            tags,
            checksum,
            owner: owner.cloned(),
            acl: row.acl,
            version_id: None,
            bucket_records_versions: records_versions,
        })
    }

    /// Take back one publish whose row write was refused — the
    /// [`MetadataFailure::Fail`] policy's compensation.
    ///
    /// The content plane hashes what it stores and takes the publish back only
    /// while the digest and the publish identity still name this call. The
    /// engine keeps no second copy of the object.
    async fn compensate_publish(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        publish_id: Option<PublishId>,
        etag: &ETag,
    ) -> Result<(), Error> {
        let Some(id) = publish_id else {
            return Ok(());
        };
        match self
            .content
            .remove_if_etag(bucket, &ContentKey::Object(key.clone()), etag, id)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => {
                tracing::info!(
                    bucket = %bucket,
                    key = %key,
                    "a refused row write's published bytes are no longer this call's: left in place"
                );
                Ok(())
            }
            Err(err) => {
                // Nothing there to take back (a concurrent delete, or a
                // compensation that already ran): the refusal is still the
                // operation's answer.
                let err: Error = projected(err);
                if matches!(err, Error::NoSuchKey(_)) {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    /// One served `Info` from a content entry and the row describing it.
    pub(crate) fn info_of(
        key: &object::Key,
        entry: &ContentEntry,
        row: &Row,
        records_versions: bool,
    ) -> object::Info {
        object::Info {
            key: key.clone(),
            size: entry.len,
            last_modified: entry.mtime.unwrap_or(row.mtime),
            etag: row.etag.clone(),
            tags: row.tags.clone(),
            checksum: row.checksum.clone(),
            owner: row.owner.clone(),
            acl: row.acl.clone(),
            version_id: None,
            bucket_records_versions: records_versions,
        }
    }

    /// The answer a folder-marker publish carries: the content plane created
    /// the namespace and holds no bytes, so the write params are
    /// accept-and-dropped and nothing about the marker is recorded — no row,
    /// no version, no tags (`object::Info`'s own empty-content shape).
    ///
    /// The answer both commit bodies return when their publish left no content
    /// behind: the marker stays outside the object and version model, so the
    /// versioned and the version-free commit answer the same `Info`.
    pub(crate) fn marker_info(key: &object::Key) -> object::Info {
        object::Info {
            key: key.clone(),
            size: 0,
            last_modified: SystemTime::now(),
            etag: ETag::EMPTY,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: None,
            bucket_records_versions: false,
        }
    }

    /// The streaming phase of a write: buffer `body` outside the backend's
    /// write locks, so a slow client never stalls other writers.
    ///
    /// The untagged write is not a body of its own here: the core trait's
    /// default `put_object` is exactly this stage beside
    /// [`commit_object`](Self::commit_object) with empty tags, no owner and
    /// the default private ACL, and both backends take that default — the
    /// engine carries no second copy of the pair to drift from it.
    ///
    /// The body streams into the content plane's own stage for `key` (D38 —
    /// the target key is known before publish) while two facts are computed
    /// over it: its byte count and its content MD5 (the ETag the commit
    /// records). The stage is cheap to discard — the bytes are published only
    /// by the later [`commit_object`](Self::commit_object).
    ///
    /// `tee` is the server's checksum slot: the interface wraps the body when
    /// the client sent a single `x-amz-checksum-*` header, so the digest is
    /// already filled at stream end; the engine reads it and, when the tee
    /// promised MD5, fills the tee's MD5 cell from the hash it computes
    /// anyway. A mismatch fails at the wrapper (the stream errors), never
    /// here. `carried` is a recorded value the caller already holds — a
    /// version-addressed copy's source version checksum, kind included — and
    /// takes precedence over the tee's streamed digest.
    ///
    /// # Errors
    ///
    /// The content plane's own refusals (a store at its size limit answers its
    /// typed error here, before publish) and a body stream's own failure.
    pub async fn stage_body(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        body: BodyStream,
        tee: Option<Arc<checksum::PartChecksum>>,
        carried: Option<checksum::Recorded>,
    ) -> Result<StagedBody<C::Staged>, Error> {
        let content_key = ContentKey::Object(key.clone());
        let mut staged = self
            .content
            .stage(bucket, &content_key)
            .await
            .map_err(projected)?;
        // The body streams into the stage through the content contract's own
        // pump, which answers the landed count and the content MD5 together —
        // the ETag the commit records is that digest.
        let (len, digest) = stage_body_md5(&mut staged, body).await.map_err(projected)?;
        let etag = ETag::Single(digest);
        // The tee's promise, kept: the digest this stream computed is the one
        // the tee would have hashed, so a consumer that asked for MD5 finds it.
        // The result is deliberately discarded: a cell that is already full
        // holds another writer's digest for the same bytes (a lost race at
        // worst, never a wrong one), and there is nothing to report it to here.
        if let (ETag::Single(digest), Some(cell)) =
            (&etag, tee.as_ref().and_then(|t| t.etag.as_ref()))
        {
            let _ = cell.set(*digest);
        }
        // The stage's recorded checksum: the carried value first, else the
        // tee's streamed digest under the write path's FULL_OBJECT rule.
        let streamed = tee.as_ref().and_then(|tee| tee.digest.get()).cloned();
        let checksum = carried.or(streamed.map(|part| checksum::Recorded {
            part,
            kind: checksum::Type::FullObject,
        }));
        Ok(StagedBody::new(staged, key.clone(), len, etag, checksum))
    }

    /// The mutation phase of a write: publish the staged bytes at `key` (the
    /// contract's atomic visibility point) and then record the row describing
    /// them.
    ///
    /// Publisher then row, never the reverse: a row describes content, so it
    /// can only follow it — and the gap between the two is exactly what the
    /// read-heal covers. `tags` — validated by the interface — and the stage's
    /// recorded checksum ride in that **one** row write, so no second tag write
    /// follows the commit; `owner` is the recorded owner element (`None` = the
    /// empty owner wire) and `acl` the grant set recorded beside it. How the
    /// row is written is the receiver's [`BucketRow`] policy, and a failed row
    /// write is read through [`MetadataFailure`].
    ///
    /// The staged body must have been staged for `key` (the contract's `stage`
    /// is keyed, D38): the caller passes the same key to both calls.
    ///
    /// # Errors
    ///
    /// The content plane's own failures, and the row plane's under
    /// [`MetadataFailure::Fail`].
    pub async fn commit_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        staged: StagedBody<C::Staged>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<object::Info, Error> {
        self.publish_and_record(bucket, key, staged, tags, owner, acl, false)
            .await
    }

    /// Stream an object body out, optionally partial (S3 `Range` semantics).
    ///
    /// The content plane is the authority on existence (`stat`), the row is
    /// read and **healed** against it, and the body is the content plane's own
    /// open — the range resolves against the served length, and the answer
    /// names the inclusive window it served (`None` for a full read).
    ///
    /// # Errors
    ///
    /// `NoSuchKey` for a key the content plane does not hold, `InvalidRange`
    /// for a range the object cannot satisfy, and whatever the planes answer
    /// otherwise.
    pub async fn get_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        range: Option<ByteRange>,
    ) -> Result<GetObjectResult, Error> {
        let entry = self.require_content(bucket, key).await?;
        let row = self.healed_row(bucket, key, &entry).await?;
        let served_range = match range {
            Some(range) => Some(range.resolve(entry.len)?),
            None => None,
        };
        let body = self
            .content
            .open(bucket, &ContentKey::Object(key.clone()), range)
            .await
            .map_err(projected)?;
        Ok(GetObjectResult {
            info: Self::info_of(key, &entry, &row, false),
            body,
            served_range,
        })
    }

    /// Object metadata; `NoSuchKey` when the content plane holds nothing.
    ///
    /// The same read and the same heal as [`get_object`](Self::get_object),
    /// without the body: a hand-dropped file keeps being served with its
    /// content ETag, and the row it earns is persisted.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` for a key the content plane does not hold.
    pub async fn head_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Info, Error> {
        let entry = self.require_content(bucket, key).await?;
        let row = self.healed_row(bucket, key, &entry).await?;
        Ok(Self::info_of(key, &entry, &row, false))
    }

    /// Move `src` to `dst` (S3 RenameObject).
    ///
    /// The bytes move by the content plane's own call and the metadata by the
    /// row plane's — one contract call each (the atomicity the engine does not
    /// decompose is each call's own), and a rename is not a fresh object: the
    /// source's mtime, tags, recorded checksum, owner, ACL and retained parts
    /// ride the row to the destination. The bytes go first, so a crash between
    /// the two leaves the destination's content in place and its record
    /// recomputed by the next read. The source's row is healed first, so a
    /// hand-dropped source moves a complete record.
    ///
    /// The **versioning guard is the caller's**: a bucket that records
    /// versions refuses the key-moving operation
    /// (`VersionOps::ensure_rename_allowed`), and that call belongs to the
    /// backend whose feature gate owns the version handles — the engine's
    /// plane-free body cannot consult them. A backend calls the guard before
    /// this body.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the source has no content, and the planes' own
    /// failures.
    pub async fn rename_object(
        &self,
        bucket: &bucket::Name,
        src: &object::Key,
        dst: &object::Key,
    ) -> Result<object::Info, Error> {
        if src == dst {
            // Degenerate: the interface answers 412 before calling, and a
            // backend-level guard keeps the move idempotent. The bytes are
            // already where they are asked to go, and a read-then-move would
            // delete them (R14 — the rule `ObjectContent::rename` and
            // `ObjectMeta::move_` both carry).
            return self.head_object(bucket, src).await;
        }
        let entry = self.require_content(bucket, src).await?;
        let row = self.healed_row(bucket, src, &entry).await?;
        self.content
            .rename(bucket, src, dst)
            .await
            .map_err(projected)?;
        ObjectMeta::move_(&*self.meta, bucket, src, dst)
            .await
            .map_err(projected)?;
        Ok(object::Info {
            key: dst.clone(),
            size: entry.len,
            last_modified: entry.mtime.unwrap_or(row.mtime),
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
            owner: row.owner,
            acl: row.acl,
            version_id: None,
            bucket_records_versions: false,
        })
    }

    /// Delete an object's bytes and its row. S3 semantics: idempotent —
    /// missing objects are `Ok`.
    ///
    /// The version-free shape. A bucket that records versions creates a delete
    /// marker in the backend, not here: a marker written as a version row
    /// would displace the one null version. The answer is
    /// [`version::Marker::none`] — no version was created.
    ///
    /// A folder-marker key's removal is the content plane's (the namespace
    /// side owns what a marker's deletion means); the row is removed
    /// idempotently either way.
    ///
    /// # Errors
    ///
    /// Whatever the planes answer.
    pub async fn delete_object(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<version::Marker, Error> {
        self.content
            .remove(bucket, &ContentKey::Object(key.clone()))
            .await
            .map_err(projected)?;
        ObjectMeta::remove(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        Ok(version::Marker::none())
    }

    /// The object row an element read or write starts from.
    ///
    /// `Guaranteed`: the row is the object, and its absence is `NoSuchKey`.
    /// `Lazy`: the content is the object, and the row is healed before the
    /// element is read — a hand-dropped file has been tagged with nothing yet.
    async fn element_row(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Row, Error> {
        match self.bucket_row {
            BucketRow::Guaranteed => ObjectMeta::get(&*self.meta, bucket, key)
                .await
                .map_err(projected)?
                .ok_or_else(|| storage::no_such_key(key)),
            BucketRow::Lazy => {
                let entry = self.require_content(bucket, key).await?;
                self.healed_row(bucket, key, &entry).await
            }
        }
    }

    /// The object's tag set (S3 GetObjectTagging). `NoSuchKey` when the
    /// object is missing.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the store's row policy says the object is missing.
    pub async fn get_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        Ok(self.element_row(bucket, key).await?.tags)
    }

    /// Replace the object's tag set (S3 PutObjectTagging — replace-all, no
    /// merge). `NoSuchKey` when the object is missing.
    ///
    /// The answer's version fields are the caller's to fill when its bucket
    /// records versions: the plane-free body touches the row's tag element
    /// only (`set_tags` is one transaction, and its `Ok(false)` is the missing
    /// object).
    ///
    /// A [`BucketRow::Lazy`] store pays **two** writes here — the read-heal
    /// that gives the tag a row to land on, then the element write — where the
    /// hardened store pays one; the healed write is what keeps a hand-dropped
    /// object's tags from being dropped on the floor, and it is the only
    /// element write with that shape (the tag *clear* is the one lenient call).
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the store's row policy says the object is missing.
    pub async fn put_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<TagWriteResult, Error> {
        if self.bucket_row == BucketRow::Lazy {
            // The heal gives the tag a row to land on. The element's own
            // `Ok(false)` is not the missing-object answer here: the content
            // already established the key.
            self.element_row(bucket, key).await?;
            ObjectMeta::set_tags(&*self.meta, bucket, key, tags)
                .await
                .map_err(projected)?;
        } else if !ObjectMeta::set_tags(&*self.meta, bucket, key, tags)
            .await
            .map_err(projected)?
        {
            return Err(storage::no_such_key(key));
        }
        Ok(TagWriteResult::untouched())
    }

    /// Remove the object's tag set (S3 DeleteObjectTagging). S3 semantics:
    /// idempotent — a missing object is `Ok` and answers no identity.
    ///
    /// The clear is the one call, on either row policy: the contract's element
    /// clear answers `Ok(false)` for a key with no row, which is exactly a
    /// missing object (or a hand-dropped one that was never tagged), so no
    /// existence probe and no content read is paid for a lenient delete.
    ///
    /// # Errors
    ///
    /// Whatever the row plane answers for a real fault; never `NoSuchKey`.
    pub async fn delete_object_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<TagWriteResult, Error> {
        ObjectMeta::clear_tags(&*self.meta, bucket, key)
            .await
            .map_err(projected)?;
        Ok(TagWriteResult::untouched())
    }

    /// The object's ACL (S3 GetObjectAcl). `NoSuchKey` when the object is
    /// missing.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the store's row policy says the object is missing.
    pub async fn get_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Acl, Error> {
        let row = self.element_row(bucket, key).await?;
        Ok(Acl {
            owner: row.owner,
            grants: row.acl.grants,
        })
    }

    /// Replace the object's grant set (S3 PutObjectAcl — replace-all, no
    /// merge). The owner element is preserved, never changed by a put.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the store's row policy says the object is missing.
    pub async fn put_object_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<(), Error> {
        let owner = self.element_row(bucket, key).await?.owner;
        if !ObjectMeta::set_acl(&*self.meta, bucket, key, owner.as_ref(), grants)
            .await
            .map_err(projected)?
        {
            return Err(storage::no_such_key(key));
        }
        Ok(())
    }

    /// The retained part rows of a completed multipart object (S3
    /// GetObjectAttributes `ObjectParts`): the parts the object was composed
    /// of at its last multipart completion, in part-number order.
    ///
    /// Empty for an object that was not multipart-completed — a plain put or
    /// copy has no parts — and `NoSuchKey` for an object that is not there.
    /// A stored part number outside the API's range refuses the read with
    /// `InvalidPartNumber` rather than panicking on a row this crate did not
    /// write.
    ///
    /// # Errors
    ///
    /// `NoSuchKey` when the store's row policy says the object is missing;
    /// `InvalidPartNumber` for a stored row no S3 part can project.
    pub async fn list_object_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<ObjectPart>, Error> {
        // A guaranteed store's missing row is the missing object. A lazy
        // store's existence is the file, and this read does not heal: the
        // parts are the row's, empty when the file was never completed.
        if self.bucket_row == BucketRow::Guaranteed {
            self.element_row(bucket, key).await?;
        } else {
            self.require_content(bucket, key).await?;
        }
        ObjectMeta::parts(&*self.meta, bucket, key)
            .await
            .map_err(projected)?
            .into_iter()
            .map(ObjectPart::try_from)
            .collect::<Result<Vec<ObjectPart>, Error>>()
    }
}

#[cfg(test)]
#[path = "object_tests.rs"]
mod tests;
