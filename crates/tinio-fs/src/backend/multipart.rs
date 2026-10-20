//! Multipart operations of the fs backend (task T044).
//!
//! The bodies are the ops engine's (`tinio_ops`): the upload lifecycle, the
//! part pipeline and the part lock live once, in `tinio_ops::MultipartOps`,
//! and this module calls them with the adapter steps only this backend can
//! take. Uploads and part ETags live in the state database; part contents stay
//! under `<state-dir>/multipart/`. Completion assembles into a temp file,
//! renames under the bucket mutation lock (the symlink policy is
//! re-checked there), consumes the upload and writes `OBJECT_META` in one
//! transaction, then removes the part directory best-effort. Reserved
//! `.tinio` keys and folder markers are refused up front.
//!
//! What stays fs-side, and why:
//!
//! - the **part files and their layout** (`<state>/multipart/<bucket>/<upload_id>/part-<n>`,
//!   the upload id validated as the UUID a create allocated — a
//!   client-supplied id never reaches the filesystem unvalidated), which the
//!   content plane's [`tinio_core::content::ContentKey::Part`] mapping owns, and the per-part file
//!   stat the listing joins (the contract has no part stat, D23);
//! - the **assembly and the completion's commit**, which are not expressible
//!   as contract calls here: the assembled object, the version branch and the
//!   consumed upload land in **one** state transaction
//!   (`complete_object_state`) under the bucket mutation lock, and the
//!   versioned publish is `publish_versioned`'s;
//! - the **part lock's home** ([`multipart::Store::part_lock`]) and its guard's
//!   construction ([`FsHooks::lock_part`]), which the engine takes across the
//!   commit phase only (E15);
//! - the unix **`copy_file_range` fast path** of `copy_part` (the store's
//!   `put_part_copy`, which commits through the same part lock); the portable
//!   arm is the engine's, called from this module's `#[cfg(not(unix))]`
//!   branch behind the same destination guards;
//! - the **reserved/folder-marker refusals**, the concurrency cap
//!   ([`multipart::Store::ensure_capacity`]) and every error variant with its
//!   projection.

// The Windows stream fallback of `copy_part` reads the source through the
// `ObjectOps` trait method (the unix fast path never does).
use std::{io::ErrorKind, sync::Arc};

#[cfg(test)]
use tinio_util::testing::part_number;
use tokio::fs;

use super::{Error, FsHooks, FsStorage};
#[cfg(not(unix))]
use crate::_core::storage::ObjectOps;
#[cfg(unix)]
use crate::fsutil;
use crate::{
    _core::{
        BodyStream, acl,
        acl::Acl,
        bucket, checksum,
        multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
        object::{self, Info},
        storage::{
            ByteRange, ListPartsParams, ListUploadsParams, MultipartOps, PartsListing,
            UploadsListing, access_denied, invalid_key, no_such_upload,
        },
    },
    _ops::{BucketRow, MultipartOps as EngineMultipartOps},
    _store_redb::RedbMetadata,
    backend::version::Publication,
    content::FsContent,
};

/// The unix fast path of the part-copy primitive: `copy_file_range` the
/// source's (range of) bytes into a staged part file, then the store's
/// shared publish. The part ETag is the staged bytes' own content MD5 —
/// a range never carries the source's ETag.
#[cfg(unix)]
impl FsStorage {
    #[expect(
        clippy::too_many_arguments,
        reason = "the contract's `copy_part` shape — source and destination bucket/key, upload id, part number and range — which this unix fast path must match to override it"
    )]
    pub(crate) async fn copy_part_fast(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        range: Option<ByteRange>,
    ) -> Result<PartInfo, Error> {
        self.ensure_bucket(dst_bucket).await?;
        if dst_key.is_reserved() {
            return Err(access_denied(dst_key).into());
        }
        let (_path, file, size, _mtime, _identity) =
            self.resolve_object_file(src_bucket, src_key).await?;
        let (start, len) = match range {
            Some(range) => {
                let (start, end) = range.resolve(size)?;
                (start, end - start + 1)
            }
            None => (0, size),
        };
        let std_file = file.into_std().await;
        self.multipart_store
            .put_part_copy(
                dst_bucket,
                dst_key,
                upload_id,
                part_number,
                std_file,
                start,
                len,
            )
            .await
    }
}

#[async_trait::async_trait]
impl MultipartOps for FsStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_bucket(bucket).await?;
        // The multipart path must not be a backdoor for `.tinio` (FR-020).
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // Folder markers are never objects: refuse the upload up front
        // (completion would have nowhere legal to materialize it).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()).into());
        }
        // The concurrency cap is this store's policy (CWE-770), not the
        // engine's: it runs before the upload row is recorded.
        self.multipart_store.ensure_capacity().await?;
        // The engine records the row and allocates the id. fs's policy is the
        // lazy one: the bucket row is recorded by the bucket's own paths, and
        // an upload row needs none of it.
        self.multipart_ops()
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl)
            .await
            .map_err(Error::from)
    }

    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // The facade's identity guard: a client-supplied id may only name a
        // UUID (as the create allocates) — anything else answers NoSuchUpload
        // rather than a record no part path can address.
        if uuid::Uuid::parse_str(upload_id).is_err() {
            return Err(no_such_upload(upload_id).into());
        }
        self.multipart_ops()
            .get_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)
    }

    async fn upload_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // The engine's part body: the upload identity first (no body is read
        // and no part file is created for an aborted or completed upload),
        // the body streamed into this crate's content plane under
        // `tinio_core::content::ContentKey::Part`, then the publish and the `PARTS` row under
        // `Hooks::lock_part`'s guard — the same store lock this backend's
        // other part path takes.
        let info = self
            .multipart_ops()
            .upload_part(bucket, key, upload_id, part_number, body, checksum)
            .await
            .map_err(Error::from)?;
        // The engine's answer carries the streamed byte count and the wall
        // clock; fs's part stamp has always been the part file's own — the
        // same single stat the pre-engine body read.
        self.stamp_part(bucket, upload_id, info).await
    }

    async fn copy_part(
        &self,
        src_bucket: &bucket::Name,
        src_key: &object::Key,
        dst_bucket: &bucket::Name,
        dst_key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        range: Option<ByteRange>,
    ) -> Result<PartInfo, Error> {
        #[cfg(unix)]
        {
            self.copy_part_fast(
                src_bucket,
                src_key,
                dst_bucket,
                dst_key,
                upload_id,
                part_number,
                range,
            )
            .await
        }
        #[cfg(not(unix))]
        {
            // No kernel copy primitive on Windows — the ops engine's portable
            // arm (the part pipeline with no client checksum, R1). The source
            // is read FIRST, exactly as the pre-engine arm did (`get_object`
            // then the trait's `upload_part`), so a request invalid at both
            // ends still answers the source's error; the destination guards
            // the fast path applies then follow — the bucket probe (the
            // engine's stage probes it again) and the reserved-key refusal —
            // and the part file's own stamp (the engine's answer carries the
            // counted bytes and a wall clock).
            let get = self.get_object(src_bucket, src_key, range).await?;
            self.ensure_bucket(dst_bucket).await?;
            if dst_key.is_reserved() {
                return Err(access_denied(dst_key).into());
            }
            let info = self
                .multipart_ops()
                .copy_part(dst_bucket, dst_key, upload_id, part_number, get.body)
                .await
                .map_err(Error::from)?;
            self.stamp_part(dst_bucket, upload_id, info).await
        }
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The engine owns the upload's identity check, the marker arithmetic
        // and the resume rule; the store applies the marker skip and the page
        // cut inside its scan (a page costs O(page) reads), and `max_parts = 0`
        // is an empty, untruncated page with no marker. The resume marker is
        // the RAW last part number: a truncated page whose parts all vanished
        // in the join must still advance the client (a marker from the emitted
        // page would loop forever).
        let page = self
            .multipart_ops()
            .list_parts(
                &params.bucket,
                &params.key,
                &params.upload_id,
                params.part_number_marker,
                params.max_parts,
            )
            .await
            .map_err(Error::from)?;
        // The fs half of the listing: the size and mtime of each served row
        // are its part file's (the contract carries no part stat, D23), and a
        // part whose file vanished between the read and this join is skipped
        // rather than failing the listing. `stamped_part` stats before it
        // reads the number, so a **tampered** raw row — a number outside the
        // legal range *and* a missing file — is skipped here where this site
        // once answered `InvalidPartNumber`: no writer produces that shape and
        // no test pins it, and the store's own `list_parts` pins the
        // stat-first order (its `u32::MAX` marker test).
        let mut parts = Vec::with_capacity(page.rows.len());
        for row in &page.rows {
            match self
                .multipart_store
                .stamped_part(
                    &params.bucket,
                    &params.upload_id,
                    row.number,
                    row.etag.clone(),
                    row.checksum.clone(),
                )
                .await
            {
                Ok(part) => parts.push(part),
                Err(Error::Io(err)) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(PartsListing {
            parts,
            truncated: page.truncated,
            next_part_number_marker: page.next_part_number_marker,
        })
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<Info, Error> {
        let bucket_dir = self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // Folder markers are never objects — a multipart upload cannot
        // materialize one (the dir branch of `put_object` would be the
        // only legal mapping, and completion is not it).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()).into());
        }
        // Fail-fast: reject an unresolvable key before the (potentially
        // long) assembly. The path is not kept — phase 2 re-resolves
        // under the mutation lock so a followed bucket symlink retargeted
        // during assembly cannot send the rename to a stale target.
        let _ = self.resolve_key(&bucket_dir, key).await?;
        // Phase 1 (the store's own lock): verify + assemble into a temp
        // file. The upload is NOT consumed here — its records must
        // outlive the rename so a crash in between leaves the upload
        // listed and a client retry completes idempotently (§5.6). The
        // assembly also returns each part's `(number, size, checksum
        // row)` — the retained `OBJECT_PARTS` list.
        let (temp, etag, part_rows) = self
            .multipart_store
            .complete(bucket, key, upload_id, parts)
            .await?;
        // Phase 2 (the mutation lock): the version branch, the rename and
        // the landed stamp are `publish_versioned`'s — the SAME ordering
        // `commit_staged` runs, one home, so neither path can forget a step
        // (the version branch commits the outgoing version's row before the
        // rename, the engine's supersede protocol). The rename cannot race a
        // `delete_bucket` — a bucket deleted between the phases is reported,
        // not silently recreated — and the guard the publish answers is kept
        // until this completion's state transaction is done. A failure here
        // (deleted bucket, swapped symlink) must not strand the assembled
        // temp in `tmp/` — remove it on the error path.
        let published = match self
            .publish_versioned(bucket, key, &temp, Publication::Versioned)
            .await
        {
            Ok(published) => published,
            Err(err) => {
                let _ = fs::remove_file(&temp).await;
                return Err(err);
            }
        };
        // The key path the publish landed on — consumed by the unix chown
        // below, and by the error path that removes a wrongly-owned file.
        #[cfg_attr(
            not(unix),
            expect(
                unused_variables,
                reason = "`target` is consumed only by the unix chown below, so off unix the binding is deliberately unused"
            )
        )]
        let target = published.path.as_path();
        let (size, mtime) = (published.size, published.mtime);
        // The unix ownership hardening (spec 2026-09-05 §5a): the
        // completion is where the assembled object lands, so the final
        // chown happens here — the upload's create-time owner is read
        // from the upload row (never re-ferried through the interface),
        // and an unmapped owner stays server-user-owned (still `0600`).
        // A chown failure fails closed BEFORE the state transaction
        // consumes the upload (the records must survive for the retry):
        // remove the file and error the request — a wrongly-owned 0600
        // file must not be left behind, and a retry re-runs the whole
        // completion idempotently.
        #[cfg(unix)]
        if !self.owner_uids.is_empty() {
            // The row's create-time owner through the metadata facade (D23's
            // `UploadMeta::get`) — no raw table read from the bytes layer.
            let stored_owner = self.meta_store.upload_owner(bucket, key, upload_id).await?;
            if let Some(uid) = self.owner_uid(stored_owner.as_ref())
                && let Err(err) = fsutil::chown_file(target, uid).await
            {
                let _ = fs::remove_file(&target).await;
                return Err(err.into());
            }
        }
        // The single state transaction: consume the upload records,
        // persist the object row (tags + the interface-computed composite
        // checksum — the backend never hashes — plus the upload's
        // create-time owner/ACL, read from the row this transaction
        // consumes, the tags precedent), and retain the assembled parts
        // in `OBJECT_PARTS`. The stamp is the publish's — the file it
        // renamed and read back — never a second stat here.
        let (tags, owner, acl) = self
            .complete_object_state(
                bucket,
                key,
                upload_id,
                &etag,
                size,
                mtime,
                published.identity,
                checksum.clone(),
                part_rows,
                published.marker.id.clone(),
                published.seq,
                published.superseded,
            )
            .await?;
        // The part files are dead weight after the consume — remove them
        // best-effort. A failure must NOT fail the completion (the object
        // is committed and a client retry would answer NoSuchUpload):
        // log it, and the startup orphan stage reclaims the residue after
        // the idle grace.
        if let Err(err) = self
            .multipart_store
            .remove_part_dir(bucket, upload_id)
            .await
        {
            tracing::warn!(error = %err, "part directory not removed after completion");
        }
        Ok(Info {
            key: key.clone(),
            size,
            last_modified: mtime,
            etag,
            tags,
            checksum,
            owner,
            acl,
            version_id: published.marker.id,
            // The publish's plan resolved the state; no second read.
            bucket_records_versions: published.records_versions,
        })
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        // The identity check and the four-table drain are ONE contract
        // transaction (the engine body), so a racing complete cannot slip
        // between them; an absent or key-mismatched upload answers
        // NoSuchUpload and drains nothing.
        self.multipart_ops()
            .abort_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)?;
        // The part files are residue once their rows are gone — the removal is
        // best-effort (a failure is what the startup orphan stage reclaims
        // after the idle grace, never a client-visible error).
        let _ = self
            .multipart_store
            .remove_part_dir(bucket, upload_id)
            .await;
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The resume marker is the composite `key\0upload_id` (see
        // `tinio_core::storage::uploads_order`), so the page can resume
        // inside a same-key group (S3 `upload-id-marker`); a bare key
        // marker skips the whole key group — the conversion has one home
        // in tinio-core (shared with the mem backend), and the store pages
        // inside its scan — bounded memory, off the async thread (item 7e).
        let mut page = self
            .multipart_ops()
            .list_multipart_uploads(params)
            .await
            .map_err(Error::from)?;
        // The facade's identity guard (see `get_multipart_upload`): only a
        // UUID id can name a part path, so a tampered or corrupt `UPLOADS`
        // row is dropped from the served page. The page's marker,
        // truncation and prefixes stay the raw read's — the resume protocol
        // survives a shorter page (the marker is exclusive-after).
        page.uploads
            .retain(|upload| uuid::Uuid::parse_str(&upload.upload_id).is_ok());
        Ok(page)
    }
}

impl FsStorage {
    /// The multipart-group receiver over this backend's handles.
    ///
    /// The policy is fs's, declared and never inferred: an upload row is
    /// recorded plainly ([`BucketRow::Lazy`] — the
    /// store's buckets outlive their rows, so an upload needs no bucket row).
    /// The hooks are [`FsHooks`], whose part lock is the multipart store's own
    /// map slot.
    ///
    /// The receiver's handles are the storage's own, resolved once
    /// ([`ReceiverHandles`](super::ReceiverHandles)) — the same content plane
    /// and hooks the bucket group takes, where the pre-engine body cloned
    /// nothing: no `FsStorage` deep copy rides an upload any more.
    pub(crate) fn multipart_ops(&self) -> EngineMultipartOps<RedbMetadata, FsContent, FsHooks> {
        let handles = self.receiver_handles();
        EngineMultipartOps {
            meta: self.meta_store().engine(),
            content: Arc::clone(&handles.own_content),
            hooks: FsHooks {
                storage: Arc::clone(&handles.storage),
            },
            bucket_row: BucketRow::Lazy,
        }
    }

    /// The fs part stamp: the part file's own size and mtime, which this
    /// backend's listings and responses have always served (the engine's
    /// answer carries the counted byte count and a wall clock instead).
    async fn stamp_part(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        info: PartInfo,
    ) -> Result<PartInfo, Error> {
        self.multipart_store
            .stamped_part(
                bucket,
                upload_id,
                u32::from(info.part_number),
                info.etag,
                info.checksum,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use bytes::Bytes;
    use futures::stream;

    use super::*;
    use crate::{
        _core::{
            CompletedPart,
            acl::Acl,
            multipart::{self, MIN_PART_BYTES},
            object,
            storage::{BucketOps, Error as StorageError, ObjectOps},
        },
        _util::testing::{body, read_body},
        testutil::{
            all_users_read_grant, bucket, checksum_tee, fs_options, md5_wire, owner_id, storage,
        },
    };

    #[tokio::test]
    async fn upload_part_stream_error_aborts_staging() {
        // A body stream that errors mid-way (the checksum tee fails the
        // stream the same way) must leave no part file and no PARTS row.
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let err = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(1),
                Box::pin(stream::iter(vec![
                    Ok::<_, io::Error>(Bytes::from_static(b"x")),
                    Err(io::Error::other("boom")),
                ])),
                None,
            )
            .await
            .unwrap_err();
        // The engine answers the layering error (`storage::Error`), which this
        // crate's `#[from]` projection nests under `Storage` — the same shape
        // every ops-engine body's failure has, and the same wire error
        // (`From<Error> for storage::Error` unwraps it).
        assert!(
            matches!(err, Error::Storage(StorageError::Io(_))),
            "{err:?}"
        );
        let listing = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 1000,
                part_number_marker: None,
            })
            .await
            .unwrap();
        assert!(
            listing.parts.is_empty(),
            "a failed stream must not commit a part"
        );
        storage
            .abort_multipart_upload(&b, &k, &upload.upload_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn multipart_lifecycle_via_contract() {
        let (root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let mut parts = Vec::new();
        // Non-final parts must be >= the 5 MiB minimum the backend
        // enforces at complete; the final part may be small.
        let min = MIN_PART_BYTES as usize;
        let parts_data: [Vec<u8>; 3] = [vec![b'a'; min], vec![b'b'; min], b"ij".to_vec()];
        for (i, data) in parts_data.iter().enumerate() {
            let part = storage
                .upload_part(
                    &b,
                    &k,
                    &upload.upload_id,
                    multipart::part_number((i + 1) as u32).expect("a valid part number"),
                    body(data.clone()),
                    None,
                )
                .await
                .unwrap();
            parts.push(part);
        }
        let listing = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 2,
                part_number_marker: None,
            })
            .await
            .unwrap();
        assert_eq!(listing.parts.len(), 2);
        assert!(listing.truncated);
        assert_eq!(listing.next_part_number_marker, Some(2));

        let completed: Vec<_> = parts
            .iter()
            .map(|p| CompletedPart {
                part_number: p.part_number,
                etag: p.etag.clone(),
            })
            .collect();
        let info = storage
            .complete_multipart_upload(&b, &k, &upload.upload_id, &completed, None)
            .await
            .unwrap();
        let expected = parts_data.concat();
        assert_eq!(info.size, expected.len() as u64);
        // MD5-of-MD5s-3 reference (computed from raw part digests).
        assert_eq!(info.etag.as_str(), "bee3dfeaffb829f8b15e911120368526-3");

        // The completed upload's part directory is removed — the
        // records AND the part files are gone, no leak for the sweep
        // to reclaim later.
        assert!(
            fs::metadata(
                root.path()
                    .join(".tinio/multipart/data")
                    .join(&upload.upload_id)
            )
            .await
            .is_err(),
            "the completed upload's part directory must be removed"
        );

        let get = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(get.body).await.unwrap(), expected);
        assert_eq!(get.info.etag.as_str(), "bee3dfeaffb829f8b15e911120368526-3");

        storage.delete_object(&b, &k).await.unwrap();
        storage.delete_bucket(&b).await.unwrap();
    }

    #[tokio::test]
    async fn part_number_marker_pagination() {
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        for i in 1..=5u32 {
            storage
                .upload_part(&b, &k, &upload.upload_id, part_number(i), body(b"x"), None)
                .await
                .unwrap();
        }
        let page1 = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 2,
                part_number_marker: None,
            })
            .await
            .unwrap();
        let page2 = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 2,
                part_number_marker: page1.next_part_number_marker,
            })
            .await
            .unwrap();
        let page3 = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 2,
                part_number_marker: page2.next_part_number_marker,
            })
            .await
            .unwrap();
        assert_eq!(page1.parts.len() + page2.parts.len() + page3.parts.len(), 5);
        assert!(!page3.truncated);
        storage
            .abort_multipart_upload(&b, &k, &upload.upload_id)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn list_multipart_uploads_filters_by_prefix() {
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        storage
            .create_multipart_upload(
                &b,
                &object::key("a.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &object::key("b.bin").unwrap(),
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: "b".into(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
            })
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .uploads
            .iter()
            .map(|u| u.key.as_ref().as_str())
            .collect();
        assert_eq!(keys, ["b.bin"]);
    }

    #[tokio::test]
    async fn delimiter_rollup_paginates_and_resumes() {
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        for key in ["dir/a.bin", "dir/b.bin", "dir/sub/c.bin", "z.bin"] {
            storage
                .create_multipart_upload(
                    &b,
                    &object::key(key).unwrap(),
                    None,
                    object::Tags::empty(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
        }
        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: Some("/".into()),
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })
            .await
            .unwrap();
        assert!(page.uploads.is_empty(), "{:?}", page.uploads);
        assert_eq!(page.common_prefixes, ["dir/"]);
        assert!(page.truncated);
        assert_eq!(page.next_key_marker.as_deref(), Some("dir/"));
        assert_eq!(page.next_upload_id_marker, None);

        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: Some("/".into()),
                key_marker: page.next_key_marker,
                upload_id_marker: page.next_upload_id_marker,
                max_uploads: 10,
            })
            .await
            .unwrap();
        let keys: Vec<&str> = page
            .uploads
            .iter()
            .map(|u| u.key.as_ref().as_str())
            .collect();
        assert_eq!(keys, ["z.bin"]);
        assert!(page.common_prefixes.is_empty());
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn bare_key_marker_skips_the_whole_key_group() {
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("same.bin").unwrap();
        let u1 = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let page = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: None,
                key_marker: Some(u1.key.to_string()),
                upload_id_marker: None,
                max_uploads: 10,
            })
            .await
            .unwrap();
        assert!(page.uploads.is_empty(), "{:?}", page.uploads);
        assert!(!page.truncated);
    }

    #[tokio::test]
    async fn same_key_uploads_paginate_without_skipping() {
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("same.bin").unwrap();
        storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let page1 = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })
            .await
            .unwrap();
        assert!(page1.truncated);
        let page2 = storage
            .list_multipart_uploads(ListUploadsParams {
                bucket: b.clone(),
                prefix: String::new(),
                delimiter: None,
                key_marker: page1.next_key_marker.clone(),
                upload_id_marker: page1.next_upload_id_marker.clone(),
                max_uploads: 10,
            })
            .await
            .unwrap();
        let ids: Vec<String> = page2.uploads.iter().map(|u| u.upload_id.clone()).collect();
        assert_eq!(ids.len(), 1, "{ids:?}");
        assert_ne!(ids[0], page1.uploads[0].upload_id);
    }

    #[tokio::test]
    async fn complete_multipart_commits_to_bucket_target_at_rename() {
        // With follow_symlinks, assembly captures a path under the
        // current bucket target; a retarget before the rename must not
        // leave the object on the stale path (subsequent reads use the
        // new target).
        use crate::{
            FsOptions,
            testutil::{link_dir, retarget_bucket_during_commit, wait_for_tmp},
        };
        let root = tempfile::tempdir().unwrap();
        let target_a = tempfile::tempdir().unwrap();
        let target_b = tempfile::tempdir().unwrap();
        let link = root.path().join("data");
        link_dir(target_a.path(), &link);
        let storage = FsStorage::new(
            root.path(),
            FsOptions {
                follow_symlinks: true,
                ..fs_options()
            },
        )
        .unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(1),
                body(b"hello"),
                None,
            )
            .await
            .unwrap();
        let completed = vec![CompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
        }];
        let storage2 = storage.clone();
        let b2 = b.clone();
        let k2 = k.clone();
        let upload_id = upload.upload_id.clone();
        retarget_bucket_during_commit(
            &storage,
            &b,
            &link,
            target_b.path(),
            wait_for_tmp(&storage),
            move || async move {
                storage2
                    .complete_multipart_upload(&b2, &k2, &upload_id, &completed, None)
                    .await
                    .unwrap()
            },
        )
        .await;
        assert!(
            target_b.path().join("big.bin").exists(),
            "object must land under the bucket target at rename"
        );
        assert!(
            !target_a.path().join("big.bin").exists(),
            "stale pre-assembly path must not receive the object"
        );
        let get = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(get.body).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn fs_complete_retains_object_parts() {
        // spec 2026-08-31: completion persists the assembled parts into
        // OBJECT_PARTS — list_object_parts serves them in part order with
        // sizes and the per-part checksums, and the object row carries
        // the completion's tags and the interface-computed composite
        // checksum (the backend never hashes).
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("big.bin").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                tags.clone(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // Two parts: the non-final first must meet the 5 MiB S3 minimum;
        // both upload with a tee so the retained rows carry per-part
        // checksums (the digest values are the parts' real content MD5s).
        let min = MIN_PART_BYTES as usize;
        let first = vec![b'a'; min];
        let p1 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(1),
                body(first.clone()),
                Some(checksum_tee(checksum::Algorithm::Md5, &md5_wire(&first))),
            )
            .await
            .unwrap();
        let p2 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(2),
                body(b"world"),
                Some(checksum_tee(checksum::Algorithm::Md5, &md5_wire(b"world"))),
            )
            .await
            .unwrap();
        let composite = checksum::Recorded {
            part: checksum::Part {
                algorithm: checksum::Algorithm::Md5,
                value: checksum::Value("AAAAAAAAAAAAAAAAAAAAAA==".into()),
            },
            kind: checksum::Type::Composite,
        };
        let completed = [
            CompletedPart {
                part_number: p1.part_number,
                etag: p1.etag,
            },
            CompletedPart {
                part_number: p2.part_number,
                etag: p2.etag,
            },
        ];
        let info = storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload.upload_id,
                &completed,
                Some(composite.clone()),
            )
            .await
            .unwrap();
        assert_eq!(info.tags, tags, "completion applies the create-time tags");
        assert_eq!(info.checksum.as_ref(), Some(&composite));

        // The retained part rows: order, sizes, per-part checksums.
        let parts = storage.list_object_parts(&b, &k).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(u32::from(parts[0].part_number), 1);
        assert_eq!(parts[0].size, min as u64);
        let checksum = parts[0].checksum.as_ref().expect("part 1's checksum");
        assert_eq!(checksum.algorithm, checksum::Algorithm::Md5);
        assert_eq!(checksum.value.0, md5_wire(&first));
        assert_eq!(u32::from(parts[1].part_number), 2);
        assert_eq!(parts[1].size, 5);
        let checksum = parts[1].checksum.as_ref().expect("part 2's checksum");
        assert_eq!(checksum.value.0, md5_wire(b"world"));

        // A second completion over the same key replaces the rows (the
        // old completion's parts must not accumulate).
        let upload2 = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let p = storage
            .upload_part(
                &b,
                &k,
                &upload2.upload_id,
                part_number(3),
                body(b"tail"),
                None,
            )
            .await
            .unwrap();
        storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload2.upload_id,
                &[CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag,
                }],
                None,
            )
            .await
            .unwrap();
        let parts = storage.list_object_parts(&b, &k).await.unwrap();
        assert_eq!(parts.len(), 1, "re-completion replaces the retained rows");
        assert_eq!(u32::from(parts[0].part_number), 3);
    }

    #[tokio::test]
    async fn fs_complete_applies_the_uploads_owner_and_acl() {
        // The create-time owner/ACL ride in the upload row and are
        // applied by the completion — never re-ferried through the
        // interface (the tags precedent, spec 2026-09-05).
        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let k = object::key("big.bin").unwrap();
        let owner = owner_id();
        let grants = vec![all_users_read_grant()];
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                Some(&owner),
                &Acl {
                    owner: Some(owner.clone()),
                    grants: grants.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(upload.owner.as_ref(), Some(&owner));
        assert_eq!(upload.acl.grants, grants);
        let part = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(1),
                body(b"hello"),
                None,
            )
            .await
            .unwrap();
        let info = storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag,
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(info.owner.as_ref(), Some(&owner));
        assert_eq!(info.acl.grants, grants);
        let head = storage.head_object(&b, &k).await.unwrap();
        assert_eq!(head.owner.as_ref(), Some(&owner));
        assert_eq!(head.acl.grants, grants);
        let got = storage.get_object_acl(&b, &k).await.unwrap();
        assert_eq!(got.owner.as_ref(), Some(&owner));
        assert_eq!(got.grants, grants);
    }

    /// The completion's versioned branch: on an enabled bucket a multipart
    /// completion supersedes the current version into history and the
    /// returned `Info` names the version it created.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn fs_complete_on_a_versioned_bucket_supersedes_the_current_version() {
        use crate::{
            _core::{
                ETag,
                storage::VersionOps,
                version::{self, ListParams, ReadResult},
            },
            _store_redb::version::Slot,
        };

        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        storage
            .set_bucket_versioning(&b, version::State::Enabled)
            .await
            .unwrap();
        let k = object::key("big.bin").unwrap();
        // The previous version: a plain enabled write.
        storage.put_object(&b, &k, body(b"first")).await.unwrap();
        let previous = storage
            .head_object(&b, &k)
            .await
            .unwrap()
            .version_id
            .expect("an enabled write mints a version id");

        // A one-part upload completed over it (the final part may be small).
        let upload = storage
            .create_multipart_upload(
                &b,
                &k,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                part_number(1),
                body(b"second"),
                None,
            )
            .await
            .unwrap();
        let info = storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag,
                }],
                None,
            )
            .await
            .unwrap();

        let fresh = info
            .version_id
            .clone()
            .expect("the completion on an enabled bucket mints a version id");
        assert_ne!(
            fresh, previous,
            "the completion's version is its own identity"
        );
        // The completed object is current and serves the assembled bytes.
        let current = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(current.body).await.unwrap(), b"second");
        assert_eq!(current.info.version_id.as_ref(), Some(&fresh));

        // The previous version was superseded into history, not overwritten
        // in place: it is reachable only through the version table now.
        match storage
            .get_object_version(&b, &k, Some(&previous), None)
            .await
            .unwrap()
        {
            ReadResult::Object(object) => {
                assert_eq!(read_body(object.body).await.unwrap(), b"first");
                assert_eq!(object.info.version_id.as_ref(), Some(&previous));
            }
            ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        // The listing names both versions of the key, newest first.
        let page = storage
            .list_object_versions(
                &b,
                ListParams {
                    prefix: None,
                    delimiter: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: 10,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].id.as_ref(), Some(&fresh));
        assert_eq!(page.entries[1].id.as_ref(), Some(&previous));
        // The completion's own state transaction stamped the superseded row
        // noncurrent — the same rule the plain write path follows — so the
        // version is history for every reader (`Row::is_history`) and its
        // retention clock runs from the moment the slot moved off it.
        let rows = storage
            .handle()
            .read(|txn| {
                let table = crate::_store_redb::version::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_key(b.as_ref().as_str(), k.as_ref().as_str(), |row| {
                    out.push(row);
                    Ok(())
                })?;
                Ok(out)
            })
            .unwrap();
        assert_eq!(rows.len(), 1, "the superseded version's row");
        assert!(
            rows[0].is_history(Slot::At(rows[0].seq)),
            "the completion must stamp the superseded row noncurrent: {rows:?}"
        );

        // The completion's ETag is the composed `MD5-of-MD5s-N` form — not
        // the content MD5 — and that is what the version row records. A plain
        // write then supersedes the composed version; the version read and
        // the listing must both answer the row's ETag verbatim. This is the
        // discriminating leg: any path that recomputes the ETag from the
        // stored bytes answers the plain content MD5 and fails both asserts.
        let composed = info.etag.clone();
        assert!(
            matches!(composed, ETag::Composed(..)) && composed != ETag::from_content(b"second"),
            "the completion's ETag is the composed form, never the content MD5: {composed}"
        );
        storage.put_object(&b, &k, body(b"third")).await.unwrap();
        match storage
            .get_object_version(&b, &k, Some(&fresh), None)
            .await
            .unwrap()
        {
            ReadResult::Object(object) => assert_eq!(
                object.info.etag, composed,
                "the version read serves the row's own ETag element"
            ),
            ReadResult::Marker { .. } => panic!("the superseded version is content"),
        }
        let page = storage
            .list_object_versions(
                &b,
                ListParams {
                    prefix: None,
                    delimiter: None,
                    key_marker: None,
                    version_id_marker: None,
                    max_keys: 10,
                },
            )
            .await
            .unwrap();
        let listed = page
            .entries
            .iter()
            .find(|entry| entry.id.as_ref() == Some(&fresh))
            .expect("the composed version must still list");
        assert_eq!(
            listed.etag, composed,
            "the listing entry serves the row's own ETag element"
        );

        // Delete the plain current version: the composed version is
        // materialized back onto the key path, and restoring it must not
        // change what its ETag is. Both the version-addressed read and the
        // plain read of the restored key must answer the composed form —
        // recording the publish's plain content MD5 instead fails both.
        let third = storage
            .head_object(&b, &k)
            .await
            .unwrap()
            .version_id
            .expect("the plain write minted an identity");
        storage
            .delete_object_version(&b, &k, Some(&third))
            .await
            .unwrap();
        match storage
            .get_object_version(&b, &k, Some(&fresh), None)
            .await
            .unwrap()
        {
            ReadResult::Object(object) => {
                assert_eq!(
                    object.info.version_id.as_ref(),
                    Some(&fresh),
                    "the composed version is current again"
                );
                assert_eq!(
                    object.info.etag, composed,
                    "the restored version keeps its composed ETag for a version-addressed read"
                );
            }
            ReadResult::Marker { .. } => panic!("the restored version is content"),
        }
        let restored = storage.get_object(&b, &k, None).await.unwrap();
        assert_eq!(read_body(restored.body).await.unwrap(), b"second");
        assert_eq!(
            restored.info.version_id.as_ref(),
            Some(&fresh),
            "the plain read names the restored version"
        );
        assert_eq!(
            restored.info.etag, composed,
            "the restored current-version row records the composed ETag, never the publish's MD5"
        );
    }

    /// The portable arm of `copy_part` (the platforms without a kernel copy
    /// primitive): the source is read through the object group and rides the
    /// ops engine's `copy_part` — the part pipeline with no client checksum
    /// (R1) — behind the same destination guards the fast path runs.
    #[cfg(not(unix))]
    #[tokio::test]
    async fn the_windows_copy_arm_reads_the_source_and_uploads_the_part() {
        // The ETag is this test's own import: nothing else in this module is
        // unix-compiled out with it, so a module-level import would be unused
        // (and denied) on unix.
        use crate::_core::ETag;

        let (_root, storage) = storage();
        let b = bucket(&storage).await;
        let src = object::key("src.bin").unwrap();
        storage
            .put_object(&b, &src, body(b"hello world".to_vec()))
            .await
            .unwrap();
        let dst = object::key("dst.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &dst,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();

        // A range is the part's own bytes: the ETag is that range's content
        // MD5, never the source's.
        let ranged = storage
            .copy_part(
                &b,
                &src,
                &b,
                &dst,
                &upload.upload_id,
                part_number(1),
                Some(ByteRange::Inclusive(0, 4)),
            )
            .await
            .unwrap();
        assert_eq!(ranged.size, 5);
        assert_eq!(ranged.etag, ETag::from_content(b"hello"));

        // The whole source, as the completion's final part.
        let whole = storage
            .copy_part(&b, &src, &b, &dst, &upload.upload_id, part_number(2), None)
            .await
            .unwrap();
        assert_eq!(whole.size, 11);
        assert_eq!(whole.etag, ETag::from_content(b"hello world"));

        // A reserved destination is refused by this arm's own guard (FR-020)
        // — which runs **after** the source read (the guard order the revert
        // restored); the source here is valid, so the answer is the
        // destination's refusal.
        let reserved = object::key(".tinio/part.bin").unwrap();
        let err = storage
            .copy_part(
                &b,
                &src,
                &b,
                &reserved,
                &upload.upload_id,
                part_number(3),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::AccessDenied(_))),
            "{err:?}"
        );

        // Both ends invalid: the source key does not exist *and* the
        // destination is reserved. The order above is the discriminator — the
        // source read runs first, so the answer is the **source's**
        // `NoSuchKey`, never the destination's `AccessDenied` the unix fast
        // path answers (its guards run destination-first: reserved key, then
        // source). A revert to the destination-first order would answer the
        // other error here and nowhere else.
        let missing = object::key("missing.bin").unwrap();
        let err = storage
            .copy_part(
                &b,
                &missing,
                &b,
                &reserved,
                &upload.upload_id,
                part_number(4),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(StorageError::NoSuchKey(_))),
            "the source is read first, so its error wins: {err:?}"
        );

        // The copied part completes: the object serves the source's bytes
        // (the unlisted range part is discarded).
        let info = storage
            .complete_multipart_upload(
                &b,
                &dst,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: whole.part_number,
                    etag: whole.etag,
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(info.size, 11);
        let get = storage.get_object(&b, &dst, None).await.unwrap();
        assert_eq!(read_body(get.body).await.unwrap(), b"hello world");
    }
}
