//! Multipart operations of the fs backend (task T044).
//!
//! Uploads and part ETags live in the state database; part contents stay
//! under `<state-dir>/multipart/`. Completion assembles into a temp file,
//! renames under the bucket mutation lock (the symlink policy is
//! re-checked there), consumes the upload and writes `OBJECT_META` in one
//! transaction, then removes the part directory best-effort. Reserved
//! `.tinio` keys and folder markers are refused up front.

// The Windows stream fallback of `copy_part` calls the ObjectOps trait
// method (the unix fast path never does).
use std::sync::Arc;

use tokio::fs;

use super::{Error, FsStorage};
#[cfg(not(unix))]
use crate::_core::storage::ObjectOps;
use crate::{
    _core::{
        BodyStream, bucket, checksum,
        multipart::{CompletedPart, MultipartUpload, PartInfo, PartNumber},
        object::{self, Info},
        storage::{
            ByteRange, ListPartsParams, ListUploadsParams, MultipartOps, PartsListing,
            UploadsListing, access_denied, invalid_key, key_marker_order, split_uploads_order,
        },
    },
    write::AtomicWriter,
};

/// The unix fast path of the part-copy primitive: `copy_file_range` the
/// source's (range of) bytes into a staged part file, then the store's
/// shared publish. The part ETag is the staged bytes' own content MD5 —
/// a range never carries the source's ETag.
#[cfg(unix)]
impl FsStorage {
    #[allow(clippy::too_many_arguments)]
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
        self.multipart_store
            .create(bucket, key, checksum, tags)
            .await
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
        self.multipart_store
            .get_upload(bucket, key, upload_id)
            .await
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
        self.multipart_store
            .put_part(bucket, key, upload_id, part_number, body, checksum)
            .await
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
            // No kernel copy primitive on Windows — the contract's
            // stream default (get range → upload part). The copy path
            // carries no client checksum (R1) — no tee slot.
            let get = self.get_object(src_bucket, src_key, range).await?;
            self.upload_part(dst_bucket, dst_key, upload_id, part_number, get.body, None)
                .await
        }
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The store applies the marker skip and the page cut inside its
        // scan (a page costs O(page) reads); `max_parts = 0` is an empty,
        // untruncated page with no marker (the store's contract). The
        // resume marker is the store's RAW last part number: a truncated
        // page whose parts all vanished in the store's pass 2 must still
        // advance the client (marker from the emitted page would loop
        // forever).
        let (parts, truncated, raw_last) = self
            .multipart_store
            .list_parts(
                &params.bucket,
                &params.key,
                &params.upload_id,
                params.part_number_marker,
                params.max_parts,
            )
            .await?;
        let next = if truncated { raw_last } else { None };
        Ok(PartsListing {
            parts,
            truncated,
            next_part_number_marker: next,
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
        // Phase 2 (the mutation lock): the rename cannot race a
        // `delete_bucket` — and a bucket deleted between the phases is
        // reported, not silently recreated. A failure here (deleted
        // bucket, swapped symlink) must not strand the assembled temp in
        // `tmp/` — remove it on the error path. Re-resolve under the
        // lock: `ensure_bucket` returns the current followed target, and
        // `resolve_key` re-runs the symlink policy against it.
        let _guard = self.lock_bucket_mutations(bucket).await;
        let phase2 = async {
            let bucket_dir = self.ensure_bucket(bucket).await?;
            let target = self.resolve_key(&bucket_dir, key).await?;
            // F03: the bucket root bounds the first-into-a-new-prefix
            // ancestor sync.
            AtomicWriter::commit(&temp, &target, Some(&bucket_dir)).await?;
            Ok::<_, Error>(target)
        }
        .await;
        let target = match phase2 {
            Ok(target) => target,
            Err(err) => {
                let _ = fs::remove_file(&temp).await;
                return Err(err);
            }
        };
        // Consume the upload and persist the object's meta entry in one
        // all-or-nothing transaction (rename → single-txn state, §5.3).
        // On failure the records survive the rollback, so a client retry
        // re-runs the completion idempotently — the rename already
        // landed, and re-assembly overwrites it atomically.
        let metadata = fs::metadata(&target).await?;
        let size = metadata.len();
        let mtime = metadata.modified()?;
        // The single state transaction: consume the upload records,
        // persist the object row (tags + the interface-computed composite
        // checksum — the backend never hashes), and retain the assembled
        // parts in `OBJECT_PARTS`.
        let tags = self
            .complete_object_state(
                bucket,
                key,
                upload_id,
                &etag,
                &target,
                &metadata,
                checksum.clone(),
                part_rows,
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
        self.multipart_store.abort(bucket, key, upload_id).await
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
        // in tinio-core (shared with the mem backend).
        let marker = key_marker_order(
            params.key_marker.as_deref(),
            params.upload_id_marker.as_deref(),
        );
        // The store pages inside its scan — bounded memory, off the
        // async thread (item 7e): page size, resume marker, and order
        // are identical to the old full-load pagination.
        let (uploads, common_prefixes, truncated, next) = self
            .multipart_store
            .list_uploads_page(
                &params.bucket,
                &params.prefix,
                params.delimiter.as_deref(),
                marker.as_deref(),
                params.max_uploads,
            )
            .await?;
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
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::stream;

    use super::*;
    use crate::{
        _core::{
            object,
            storage::{BucketOps, ObjectOps},
        },
        _util::testing::{body, read_body},
        testutil::{checksum_tee, fs_options, md5_wire, storage},
    };

    #[tokio::test]
    async fn upload_part_stream_error_aborts_staging() {
        // A body stream that errors mid-way (the checksum tee fails the
        // stream the same way) must leave no part file and no PARTS row.
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        let err = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
                Box::pin(stream::iter(vec![
                    Ok::<_, std::io::Error>(Bytes::from_static(b"x")),
                    Err(std::io::Error::other("boom")),
                ])),
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        let mut parts = Vec::new();
        // Non-final parts must be >= the 5 MiB minimum the backend
        // enforces at complete; the final part may be small.
        let min = crate::_core::multipart::MIN_PART_BYTES as usize;
        let parts_data: [Vec<u8>; 3] = [vec![b'a'; min], vec![b'b'; min], b"ij".to_vec()];
        for (i, data) in parts_data.iter().enumerate() {
            let part = storage
                .upload_part(
                    &b,
                    &k,
                    &upload.upload_id,
                    ((i + 1) as u32).into(),
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        for i in 1..=5u32 {
            storage
                .upload_part(&b, &k, &upload.upload_id, i.into(), body(b"x"), None)
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        storage
            .create_multipart_upload(
                &b,
                &object::key("a.bin").unwrap(),
                None,
                object::Tags::empty(),
            )
            .await
            .unwrap();
        storage
            .create_multipart_upload(
                &b,
                &object::key("b.bin").unwrap(),
                None,
                object::Tags::empty(),
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        for key in ["dir/a.bin", "dir/b.bin", "dir/sub/c.bin", "z.bin"] {
            storage
                .create_multipart_upload(
                    &b,
                    &object::key(key).unwrap(),
                    None,
                    object::Tags::empty(),
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("same.bin").unwrap();
        let u1 = storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("same.bin").unwrap();
        storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        storage
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
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
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        let part = storage
            .upload_part(&b, &k, &upload.upload_id, 1.into(), body(b"hello"), None)
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
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let upload = storage
            .create_multipart_upload(&b, &k, None, tags.clone())
            .await
            .unwrap();
        // Two parts: the non-final first must meet the 5 MiB S3 minimum;
        // both upload with a tee so the retained rows carry per-part
        // checksums (the digest values are the parts' real content MD5s).
        let min = crate::_core::multipart::MIN_PART_BYTES as usize;
        let first = vec![b'a'; min];
        let p1 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
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
                2.into(),
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
            crate::_core::CompletedPart {
                part_number: p1.part_number,
                etag: p1.etag,
            },
            crate::_core::CompletedPart {
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
            .create_multipart_upload(&b, &k, None, object::Tags::empty())
            .await
            .unwrap();
        let p = storage
            .upload_part(&b, &k, &upload2.upload_id, 3.into(), body(b"tail"), None)
            .await
            .unwrap();
        storage
            .complete_multipart_upload(
                &b,
                &k,
                &upload2.upload_id,
                &[crate::_core::CompletedPart {
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
}
