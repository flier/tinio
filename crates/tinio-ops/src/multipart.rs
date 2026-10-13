//! The multipart group: the upload lifecycle, the part pipeline and the
//! part-write lock, over the row plane, the content plane and the seam.
//!
//! Every body is a method of [`MultipartOps`] and is written against the two
//! contracts only: the upload rows are the row plane's ([`Metadata`] with
//! [`UploadMeta`]) and the part bytes are the content plane's
//! ([`ObjectContent`], addressed by [`ContentKey::Part`] — obligation 11), so
//! a part's bytes cross the same seam as an object's and a backend's byte
//! plane needs no second entry point.
//!
//! Three rules shape the group.
//!
//! - **Bytes first, the row last.** A part is published, then its `PARTS` row
//!   is written — the contract's order, so a completion that reads part rows
//!   never finds a row whose bytes are absent. A refused row write is the
//!   operation's answer and the engine takes its own publish back: a part row
//!   is the only thing that names a part's bytes, there is no read-heal for a
//!   part (`MetadataFailure` is the *object* path's policy and no part body
//!   consults it).
//! - **The part lock spans the commit phase only** (obligation 13, E15): the
//!   body streams the client's bytes into the staged value **before** taking
//!   [`Hooks::lock_part`]'s guard, and drops it once the part row is written.
//!   A slow body stream therefore never blocks another part write of the same
//!   upload, and the plain take-back above is safe: no other writer of this
//!   number can publish between this call's publish and its unsend.
//! - **The upload row is the identity.** `(bucket, key, upload_id)` names an
//!   upload and every body re-checks the stored key through the contract
//!   (the pre-check before staging, and the row write's own in-transaction
//!   check), so a client's wrong key can never address another upload's parts.
//!
//! # What stays with the backend
//!
//! - **The part stat plane.** The contract carries a size and an mtime for an
//!   *object* (`ContentEntry`) and none for a part (D23), so
//!   [`MultipartOps::list_parts`] answers the row page and the backend joins
//!   the sizes: `tinio-fs` stats the part files, `tinio-mem` reads its own
//!   stat table.
//! - **`complete_multipart_upload`.** The one body this group does **not**
//!   carry, and it is a finding rather than a gap in the receiver: a
//!   completion's commit half is not expressible as contract calls for either
//!   backend in view. `tinio-fs` assembles file-to-file and commits the
//!   assembled object, the version branch and the consumed upload in **one**
//!   state transaction under the bucket mutation lock (a sequence of contract
//!   calls would split that transaction and route a versioned completion
//!   around `publish_versioned`); `tinio-mem` decides the write's version
//!   outcome inside a single transaction (the bucket's state can flip between
//!   the caller's read and the write) and forgives the consumed parts' bytes
//!   in the publish that lands the object. Both keep their own body; the
//!   assembly is backend-side in each. The commit half's irreducible core is a
//!   **slot-carrying parts write**, the second capability the 2026-10-10
//!   design names and deliberately does not land
//!   (`docs/superpowers/specs/2026-10-10-version-slot-write-contract.md` §4:
//!   `commit_completion`, `VersionMeta::restore_slot`'s row half with the
//!   retained parts kept instead of cleared and no version row consumed);
//!   deferring changes nothing observable — both completions are correct
//!   today — and the capability's home is the multipart wave's to rule.
//! - The upload-id guards a store's own layout needs (`tinio-fs` maps an id
//!   to a directory, so it serves only the v4 UUID [`MultipartOps::create_multipart_upload`]
//!   allocates), the reserved/folder-marker key refusals, the part files and
//!   their layout, and every error variant with its projection.
//!
//! # Examples
//!
//! The group over the in-memory reference planes — the shape a backend wires:
//! its own row plane, its own content plane, its own hooks.
//!
//! ```rust
//! use std::sync::Arc;
//!
//! use futures::stream;
//! use tinio_core::{
//!     acl::Acl,
//!     bucket,
//!     multipart::{CompletedPart, part_number},
//!     object,
//! };
//! use tinio_ops::{BucketOps, BucketRow, MetadataFailure, MultipartOps};
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
//! let parts = MultipartOps {
//!     meta: Arc::clone(&meta),
//!     content: Arc::clone(&content),
//!     hooks: NoHooks,
//!     bucket_row: BucketRow::Guaranteed,
//! };
//!
//! let name = bucket::name("data").unwrap();
//! buckets
//!     .create_bucket(&name, None, &Acl::default_private(None))
//!     .await
//!     .unwrap();
//!
//! let key = object::key("big.bin").unwrap();
//! let upload = parts
//!     .create_multipart_upload(
//!         &name,
//!         &key,
//!         None,
//!         object::Tags::empty(),
//!         None,
//!         &Acl::default_private(None),
//!     )
//!     .await
//!     .unwrap();
//! let body = Box::pin(stream::iter([Ok(bytes::Bytes::from_static(b"hello"))]));
//! let part = parts
//!     .upload_part(
//!         &name,
//!         &key,
//!         &upload.upload_id,
//!         part_number(1).unwrap(),
//!         body,
//!         None,
//!     )
//!     .await
//!     .unwrap();
//! assert_eq!(part.size, 5);
//! assert_eq!(part.etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
//!
//! let page = parts
//!     .list_parts(&name, &key, &upload.upload_id, None, 1000)
//!     .await
//!     .unwrap();
//! assert_eq!(page.rows.len(), 1, "the part row is the page");
//! assert!(!page.truncated);
//! # }
//! ```

use std::{sync::Arc, time::SystemTime};

use crate::{
    _core::{
        BodyStream, ETag, acl,
        acl::Acl,
        bucket, checksum,
        content::{ContentKey, ObjectContent, stage_body_md5},
        meta::{
            Metadata, UploadMeta,
            upload::{PartRow, Row as UploadRow},
        },
        multipart::{MultipartUpload, PartInfo, PartNumber},
        object,
        storage::{
            self, Error, ListUploadsParams, UploadsListing, key_marker_order, split_uploads_order,
        },
    },
    hooks::Hooks,
    ops::{BucketRow, projected},
};

/// One page of an upload's parts, before the backend's own size/mtime join.
///
/// The contract's `PartsPage` carries the rows, the truncation flag and the
/// raw read's last number; this value adds the **resume marker** the S3
/// listing needs, resolved by the rule
/// [`MultipartOps::list_parts`] documents — and it deliberately carries no
/// [`PartInfo`]: the contract has no part stat (D23), so the size and mtime
/// of each row are the backend's to join.
///
/// # Examples
///
/// ```rust
/// use tinio_ops::multipart::PartsPage;
///
/// let page = PartsPage {
///     rows: Vec::new(),
///     truncated: false,
///     next_part_number_marker: None,
/// };
/// assert!(page.rows.is_empty() && !page.truncated);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsPage {
    /// The page's rows, in ascending part-number order.
    pub rows: Vec<PartRow>,
    /// Whether more rows remain after the page.
    pub truncated: bool,
    /// The resume marker for the next page, when the page is truncated: the
    /// last part number the raw read served, or the raw read's own last
    /// number when the join emptied the page.
    pub next_part_number_marker: Option<u32>,
}

/// What the multipart group's bodies need: the row plane, the content plane,
/// the seam and the bucket-row policy.
///
/// The receiver reaches the part lock through [`Hooks::lock_part`] (the
/// group's one hook) and carries no version plane (E16): no multipart body
/// creates a version row — `tinio-mem`'s completing version branch is its
/// own, and `tinio-fs`'s completion runs its versioned publish itself.
///
/// # Examples
///
/// ```ignore
/// use std::sync::Arc;
///
/// use tinio_ops::{BucketRow, MultipartOps};
///
/// let parts = MultipartOps {
///     meta: Arc::clone(&rows),
///     content: Arc::clone(&content),
///     hooks: hooks.clone(),
///     bucket_row: BucketRow::Guaranteed,
/// };
/// let upload = parts
///     .create_multipart_upload(&bucket, &key, None, tags, owner, acl)
///     .await?;
/// ```
pub struct MultipartOps<M, C, H> {
    /// The row plane: upload, part, bucket and object rows (`Metadata`).
    pub meta: Arc<M>,
    /// The content plane: part and object bytes, and the bucket namespace.
    pub content: Arc<C>,
    /// The backend's seam ([`Hooks`]): the part lock.
    pub hooks: H,
    /// Whether the backend's buckets require their row ([`BucketRow`]).
    /// It decides the create's row write: a row-backed store records the
    /// upload **guarded** (the bucket row is asserted inside the transaction,
    /// so a concurrent teardown cannot be raced), a store whose buckets
    /// outlive their rows records it plainly.
    pub bucket_row: BucketRow,
}

impl<M, C, H> MultipartOps<M, C, H>
where
    M: Metadata,
    C: ObjectContent,
    H: Hooks,
{
    /// Start a multipart upload: allocate a fresh id and record the upload's
    /// state (create-time checksum spec, tags, owner and ACL included).
    ///
    /// The id is a v4 UUID, allocated here rather than by the caller: a part
    /// is addressed by `(upload_id, number)` across the whole group (its
    /// content key and its row key alike), and a backend that maps the pair
    /// onto its own namespace has to be able to validate it (`tinio-fs`'s
    /// part directory is the upload id). The upload directory — where a
    /// filesystem backend keeps the parts — is the first part write's to
    /// create, never this call's.
    ///
    /// The row write follows the receiver's [`BucketRow`] policy: `Guaranteed`
    /// records it guarded (a missing bucket row refuses with `NoSuchBucket`
    /// inside the contract's transaction) and `Lazy` records it plainly.
    ///
    /// # Errors
    ///
    /// The contract's own failures — `NoSuchBucket` for the guarded policy's
    /// missing bucket row included.
    pub async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, Error> {
        // The create time is read before the id is allocated, as this call has
        // always recorded it.
        let initiated_at = SystemTime::now();
        let row = UploadRow {
            upload_id: uuid::Uuid::new_v4().to_string(),
            key: key.clone(),
            initiated_at,
            checksum: checksum.clone(),
            tags: tags.clone(),
            owner: owner.cloned(),
            acl: acl.clone(),
        };
        match self.bucket_row {
            BucketRow::Guaranteed => {
                UploadMeta::create_guarded(&*self.meta, bucket, &row.upload_id, &row)
                    .await
                    .map_err(projected)?
            }
            BucketRow::Lazy => UploadMeta::create(&*self.meta, bucket, &row.upload_id, &row)
                .await
                .map_err(projected)?,
        }
        // The row is the recorded state and the answer is the same state: the
        // create borrows it, so its own values (never a second round of the
        // arguments' clones) are what the caller gets back.
        Ok(MultipartUpload {
            upload_id: row.upload_id,
            bucket: bucket.clone(),
            key: row.key,
            initiated_at: row.initiated_at,
            checksum: row.checksum,
            tags: row.tags,
            owner: row.owner,
            acl: row.acl,
        })
    }

    /// The upload's persisted state — `NoSuchUpload` when the upload does not
    /// exist or records another key.
    ///
    /// # Errors
    ///
    /// `NoSuchUpload` for an upload the store does not hold under this key.
    pub async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        UploadMeta::get(&*self.meta, bucket, key, upload_id)
            .await
            .map_err(projected)?
            .map(|row| row.to_upload(bucket))
            .ok_or_else(|| storage::no_such_upload(upload_id))
    }

    /// Upload one part: stream the client's bytes into the part's content
    /// key, then commit them under the part lock.
    ///
    /// The phases are obligation 13's:
    ///
    /// 1. the upload's identity is checked through the contract **before any
    ///    disk write** — an `UploadPart` for an aborted or completed upload
    ///    must leave no residue, so the body is not streamed and no part file
    ///    is created;
    /// 2. the body streams into the content plane's stage for
    ///    [`ContentKey::Part`], with the content MD5 computed while it lands
    ///    (the tee's promised digest, when the server sent one, is the same
    ///    value and is preferred — a part's ETag **is** its content MD5, and
    ///    the tee's cell is filled from the computed hash when it was not);
    /// 3. [`Hooks::lock_part`]'s guard is taken — the streaming is already
    ///    done, so a slow client never holds it;
    /// 4. the stage is published (the contract's visibility point);
    /// 5. the `PARTS` row is written, re-checking the upload's stored key
    ///    inside the contract's own transaction;
    /// 6. the guard is dropped — it spans the publish and the row, and
    ///    nothing else.
    ///
    /// A refused row write is the operation's answer, and the bytes this call
    /// published are taken back before it is answered: a part row is the only
    /// thing that names a part's bytes (there is no read-heal for a part), the
    /// guard is still held, and the plain removal is therefore this call's own
    /// publish and no other writer's. What the take-back leaves behind is the
    /// **empty upload directory** the publish created — residue, not state:
    /// nothing lists it, and an abort, a completion or the backend's own
    /// orphan sweep reclaims it (fs's `remove_part_dir` and its startup
    /// stage). A backend whose row plane answers differently keeps its own
    /// body — `tinio-mem` restores the bytes the number held before, because a
    /// row that survives a refused write must keep the bytes it describes.
    ///
    /// # Errors
    ///
    /// `NoSuchUpload` for an upload the store does not hold under this key
    /// (before the body is read, and again when a body failure raced a
    /// concurrent abort), the content plane's own refusals (a store at its
    /// size limit answers its typed error before the publish), and the row
    /// plane's refusal of the part row.
    pub async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        self.require_upload(bucket, key, upload_id).await?;
        let content_key = ContentKey::Part {
            upload_id: upload_id.to_owned(),
            number: part_number,
        };
        let mut staged = self
            .content
            .stage(bucket, &content_key)
            .await
            .map_err(projected)?;
        // The body streams into the stage through the content contract's own
        // pump, which answers the landed count and the content MD5 together —
        // the part's ETag is that digest, computed while the bytes land.
        let streamed = stage_body_md5(&mut staged, body).await;
        let (len, computed) = match streamed {
            Ok(facts) => facts,
            Err(err) => {
                // An abort or a completion may have consumed the upload while
                // the body streamed: the upload's absence is the accurate
                // answer then (the fs backend's rule, shared here), and any
                // other failure is the caller's own.
                if self.require_upload(bucket, key, upload_id).await.is_err() {
                    return Err(storage::no_such_upload(upload_id));
                }
                return Err(projected(err));
            }
        };
        // The tee's promise, kept — and preferred: a part's ETag is its
        // content MD5, so the digest the tee computed over the same stream is
        // that value. A cell that is already full holds another writer's
        // digest for the same bytes (a lost race at worst, never a wrong one),
        // so a set result is discarded like every other tee fill.
        let etag = match checksum.as_ref().and_then(|tee| tee.etag_digest()) {
            Some(digest) => ETag::Single(digest),
            None => {
                if let Some(cell) = checksum.as_ref().and_then(|tee| tee.etag.as_ref()) {
                    let _ = cell.set(computed);
                }
                ETag::Single(computed)
            }
        };
        // The commit phase, and the only span the part lock covers.
        let guard = self.hooks.lock_part(bucket, upload_id).await;
        self.content
            .publish(bucket, staged, &content_key)
            .await
            .map_err(projected)?;
        if let Err(err) = UploadMeta::put_part(
            &*self.meta,
            bucket,
            key,
            upload_id,
            part_number,
            &etag,
            checksum.as_deref(),
        )
        .await
        .map_err(projected)
        {
            // The refusal is the answer, and it is answered with the plane in
            // step: the bytes this call published are taken back. The guard is
            // still held, so the removal can name no other writer's publish.
            if let Err(failure) = self.content.remove(bucket, &content_key).await {
                tracing::warn!(
                    bucket = %bucket,
                    upload_id = %upload_id,
                    part = %part_number,
                    error = %failure,
                    "the take-back for a refused part row failed"
                );
            }
            return Err(err);
        }
        drop(guard);
        Ok(PartInfo {
            part_number,
            size: len,
            etag,
            last_modified: SystemTime::now(),
            // The digest committed atomically with the part row.
            checksum: checksum.as_ref().and_then(|c| c.digest.get()).cloned(),
        })
    }

    /// `UploadPartCopy`'s portable arm (S3): stream the source into the part
    /// pipeline.
    ///
    /// A part's ETag is the content MD5 of the part bytes and the copy path
    /// carries no client checksum (R1), so this is [`upload_part`](Self::upload_part)
    /// over the source's body — the source read (and its range) is the
    /// caller's, because how an object's bytes are opened is the object
    /// group's business and at least one backend has a cheaper route for it.
    /// `tinio-fs` takes the kernel's `copy_file_range` where the platform has
    /// one (its own `put_part_copy`, under the same part lock) and calls this
    /// arm from `copy_part`'s `#[cfg(not(unix))]` branch — after the same
    /// destination guards its fast path applies, so the two arms refuse the
    /// same keys.
    ///
    /// # Errors
    ///
    /// Whatever [`upload_part`](Self::upload_part) answers.
    pub async fn copy_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        source: BodyStream,
    ) -> Result<PartInfo, Error> {
        self.upload_part(bucket, key, upload_id, part_number, source, None)
            .await
    }

    /// One page of an upload's parts, the shared half of the S3 listing.
    ///
    /// The upload's existence and its recorded key are one contract read; the
    /// page itself is the contract's (`UploadMeta::list_parts_page`, O(page)
    /// rows with the checksum join), and the resume marker is the rule both
    /// backends answer:
    ///
    /// - `max_parts == 0` requests nothing — an empty, untruncated page with
    ///   no marker, because an exclusive-after marker would skip the first
    ///   part of the next page forever;
    /// - the marker is **exclusive**, and `u32::MAX` has no successor: a
    ///   marker of `u32::MAX` answers the empty page rather than re-including
    ///   the marker part itself (unreachable through S3's `1..=10000` range,
    ///   which this layer does not rely on);
    /// - a truncated page's resume marker is the last number the page
    ///   **served** — `tinio-fs`'s own listing has always named that row, and
    ///   it is the same number the raw read served whenever the contract's
    ///   ETag self-heal dropped nothing. When the self-heal **emptied** a
    ///   truncated page there is no served row to name, and the marker falls
    ///   back to the raw read's own last number: the contract carries it for
    ///   exactly that, and a markerless truncated page would send the client
    ///   back to the same page forever. (The fallback names a smaller number
    ///   than the raw read did in that corner — the client re-reads the healed
    ///   rows, which the join drops again — never a larger one, so no row is
    ///   ever skipped.)
    ///
    /// The rows are the contract's; their sizes and mtimes are the backend's
    /// to join (the contract has no part stat, D23).
    ///
    /// # Errors
    ///
    /// `NoSuchUpload` for an upload the store does not hold under this key.
    pub async fn list_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        marker: Option<u32>,
        max_parts: usize,
    ) -> Result<PartsPage, Error> {
        self.require_upload(bucket, key, upload_id).await?;
        // The marker's successor first: it is pure, so the two empty-page
        // guards answer the same value whichever order they run in.
        let Some(start) = marker.map_or(Some(0), |m| m.checked_add(1)) else {
            return Ok(Self::empty_parts_page());
        };
        if max_parts == 0 {
            return Ok(Self::empty_parts_page());
        }
        let page = UploadMeta::list_parts_page(&*self.meta, bucket, upload_id, start, max_parts)
            .await
            .map_err(projected)?;
        // The resume marker: the last row the page SERVED, or — when the
        // self-heal emptied a truncated page — the raw read's own last
        // number. Either way the client advances past everything the read
        // consumed.
        let served_last = page.rows.last().map(|row| row.number);
        let next = if page.more {
            served_last.or(page.raw_last)
        } else {
            None
        };
        Ok(PartsPage {
            rows: page.rows,
            truncated: page.more,
            next_part_number_marker: next,
        })
    }

    /// The answer the two empty-page guards share: nothing served, not
    /// truncated, and no marker to resume from.
    fn empty_parts_page() -> PartsPage {
        PartsPage {
            rows: Vec::new(),
            truncated: false,
            next_part_number_marker: None,
        }
    }

    /// Abort an upload: its records leave in **one** transaction.
    ///
    /// The identity (the stored key) and the `UPLOADS`/`UPLOAD_CHECKSUMS` +
    /// `PARTS`/`PART_CHECKSUMS` removal are the contract's
    /// `drain_upload_checked`, one call, so a racing completion cannot slip
    /// between the check and the removal. The part bytes leave after their
    /// rows — the backend's own byte plane and stat plane, in whatever batch
    /// it keeps them in — because a row must never outlive its bytes.
    ///
    /// # Errors
    ///
    /// `NoSuchUpload` for an absent or key-mismatched upload (nothing is
    /// drained), and the row plane's own failures.
    pub async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.meta
            .drain_upload_checked(bucket, key, upload_id)
            .await
            .map_err(projected)?;
        Ok(())
    }

    /// One page of the bucket's in-progress uploads (S3 ListMultipartUploads).
    ///
    /// The resume marker is the composite `key\0upload_id` order
    /// (`key_marker_order`), so a page can resume inside a same-key group; the
    /// prefix filter, the delimiter rollup, the truncation and the resume
    /// rules are the contract's own page (`UploadMeta::list_uploads_page`) and
    /// the marker is split back into the S3 pair (`split_uploads_order`) —
    /// both conversions have one home in `tinio_core`, shared by the
    /// backends.
    ///
    /// # Errors
    ///
    /// The row plane's own failures.
    pub async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        let marker = key_marker_order(
            params.key_marker.as_deref(),
            params.upload_id_marker.as_deref(),
        );
        let (uploads, common_prefixes, truncated, next) = self
            .meta
            .list_uploads_page(
                &params.bucket,
                &params.prefix,
                params.delimiter.as_deref(),
                marker.as_deref(),
                params.max_uploads,
            )
            .await
            .map_err(projected)?;
        let (next_key_marker, next_upload_id_marker) = match next {
            Some(next) => {
                let (key, upload_id) = split_uploads_order(&next);
                (Some(key.to_string()), upload_id.map(str::to_string))
            }
            None => (None, None),
        };
        Ok(UploadsListing {
            uploads,
            common_prefixes,
            truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    /// The upload's existence and its stored key, or `NoSuchUpload`.
    async fn require_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        if UploadMeta::get(&*self.meta, bucket, key, upload_id)
            .await
            .map_err(projected)?
            .is_none()
        {
            return Err(storage::no_such_upload(upload_id));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "multipart_tests.rs"]
mod tests;
