//! Multipart completion: check the part list, copy one part at a time, publish,
//! and take the publish back when the record does not land.

use std::time::SystemTime;

use tinio_core::{
    ETag,
    bucket::Name,
    checksum,
    content::{ContentKey, ObjectContent, PublishId, stage_body},
    meta::{
        BucketMeta, ObjectMeta, UploadMeta,
        object::{PartRow, Row},
    },
    multipart::{self, CompletedPart, check_part_minimum},
    object, storage,
};
use tinio_ops::object::mint_publish_id;
use tracing::warn;

use crate::{
    _store_pg::content::{LiveGeneration, LiveSpan},
    Error,
    storage::PgStorage,
    version::Landed,
};

impl PgStorage {
    /// Verify `parts`, copy each one into the object's stage, and commit.
    ///
    /// Lengths are one read. Each part is opened only while it is copied, so
    /// the completion does not hold every part at once. A failure of the row
    /// write, the slot move, or the upload drain takes the publish back: the
    /// caller sees the failure and the key is the object it was. Part bytes
    /// deleted after a successful drain are best-effort — the object is
    /// committed, and a failure there would make a retry `NoSuchUpload`.
    ///
    /// # Errors
    ///
    /// `NoSuchBucket` for a missing bucket, `NoSuchUpload` for a missing or
    /// key-mismatched upload, `NoParts` for an empty list, `InvalidPart` for a
    /// missing, out-of-order or mismatched part, `PartTooSmall` for a non-final
    /// part below the minimum, and the planes' own failures.
    pub(crate) async fn finish_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        if parts.is_empty() {
            return Err(storage::no_parts().into());
        }
        // The bucket's row before the upload's: an upload row cannot outlive its bucket, so a
        // missing bucket would otherwise be reported as a missing upload.
        if BucketMeta::row(&*self.store, bucket)
            .await
            .map_err(storage::Error::from)?
            .is_none()
        {
            return Err(storage::no_such_bucket(bucket).into());
        }
        let upload = UploadMeta::get(&*self.store, bucket, key, upload_id)
            .await
            .map_err(storage::Error::from)?
            .ok_or_else(|| storage::no_such_upload(upload_id))?;
        let numbers: Vec<_> = parts.iter().map(|part| part.part_number).collect();
        let records = UploadMeta::part_records(&*self.store, bucket, key, upload_id, &numbers)
            .await
            .map_err(storage::Error::from)?
            .ok_or_else(|| storage::no_such_upload(upload_id))?;

        let keys: Vec<ContentKey> = parts
            .iter()
            .map(|part| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: part.part_number,
            })
            .collect();
        let lens = self
            .store
            .live_lens(bucket, &keys)
            .await
            .map_err(storage::Error::from)?;

        let mut infos = Vec::with_capacity(parts.len());
        let mut retained = Vec::with_capacity(parts.len());
        let mut spans = Vec::with_capacity(parts.len());
        let mut prev = 0u32;
        for (index, part) in parts.iter().enumerate() {
            let n = u32::from(part.part_number);
            if n <= prev {
                return Err(storage::invalid_part(n).into());
            }
            prev = n;
            let Some(record) = records.get(index).and_then(Option::as_ref) else {
                return Err(storage::invalid_part(n).into());
            };
            let Some(span) = lens.get(index).copied().flatten() else {
                return Err(storage::invalid_part(n).into());
            };
            check_part_minimum(n, span.len, index + 1 == parts.len())?;
            if record.etag != part.etag {
                return Err(storage::invalid_part(n).into());
            }
            retained.push(PartRow {
                number: n,
                size: span.len,
                checksum: record.checksum.clone(),
            });
            infos.push(multipart::PartInfo {
                part_number: part.part_number,
                size: span.len,
                etag: record.etag.clone(),
                last_modified: upload.initiated_at,
                checksum: record.checksum.clone(),
            });
            spans.push(span);
        }
        // The list is non-empty and every part pushed one info, so `None` is the
        // impossible case — the contract's own refusal, not a panic.
        let Some(etag) = ETag::composed_from_parts(&infos) else {
            return Err(storage::no_parts().into());
        };

        // Open before the copy. A supersede has to move the outgoing version
        // while its bytes are still the key's; the publish overwrites them.
        let write = self.open_put_write(bucket, key).await?;
        let before = ObjectMeta::get(&*self.store, bucket, key)
            .await
            .map_err(storage::Error::from)?;
        // The part list the key holds now. The row write replaces it, so a
        // rollback has to have read it first.
        let before_parts = if before.is_some() {
            ObjectMeta::parts(&*self.store, bucket, key)
                .await
                .map_err(storage::Error::from)?
        } else {
            Vec::new()
        };
        let content_key = ContentKey::Object(key.clone());
        let previous = self
            .store
            .live_generation(bucket, &content_key)
            .await
            .map_err(storage::Error::from)?;
        let publish = mint_publish_id();
        let published = self
            .copy_parts(bucket, &content_key, &keys, &spans, publish)
            .await?;

        let entry = self
            .store
            .stat(bucket, key)
            .await
            .map_err(storage::Error::from)?
            .ok_or_else(|| storage::no_such_key(key))?;
        let mtime = entry.mtime.unwrap_or_else(SystemTime::now);
        let row = Row {
            etag: etag.clone(),
            size: entry.len,
            mtime,
            identity: entry.identity,
            tags: upload.tags,
            checksum,
            owner: upload.owner,
            acl: upload.acl,
        };
        if let Err(err) = ObjectMeta::put_with_parts(&*self.store, bucket, key, &row, &retained)
            .await
            .map_err(Error::from)
        {
            self.take_back(
                bucket,
                key,
                &content_key,
                published,
                previous,
                &write,
                before.as_ref(),
                &before_parts,
                publish,
                Landed::Publish,
            )
            .await?;
            return Err(err);
        }
        if let Err(err) = self.close_put_write(bucket, key, &write).await {
            self.take_back(
                bucket,
                key,
                &content_key,
                published,
                previous,
                &write,
                before.as_ref(),
                &before_parts,
                publish,
                Landed::Row,
            )
            .await?;
            return Err(err);
        }

        let stored = UploadMeta::list_parts(&*self.store, bucket, upload_id, 1.into())
            .await
            .map_err(storage::Error::from)?;
        if let Err(err) = self
            .store
            .drain_upload_checked(bucket, key, upload_id)
            .await
        {
            // The row and the slot landed, and the upload is still there. Put
            // the key back so a retry completes this same upload.
            self.take_back(
                bucket,
                key,
                &content_key,
                published,
                previous,
                &write,
                before.as_ref(),
                &before_parts,
                publish,
                if write.records() {
                    Landed::Slot
                } else {
                    Landed::Row
                },
            )
            .await?;
            return Err(storage::Error::from(err).into());
        }
        let removed: Vec<ContentKey> = stored
            .iter()
            .map(|part| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: part.number.into(),
            })
            .collect();
        self.remove_part_bytes_best_effort(bucket, &removed).await;

        Ok(object::Info {
            key: key.clone(),
            size: row.size,
            last_modified: mtime,
            etag: row.etag,
            tags: row.tags,
            checksum: row.checksum,
            owner: row.owner,
            acl: row.acl,
            version_id: write.identity(),
            bucket_records_versions: write.records(),
        })
    }

    async fn copy_parts(
        &self,
        bucket: &Name,
        content_key: &ContentKey,
        keys: &[ContentKey],
        spans: &[LiveSpan],
        publish: PublishId,
    ) -> Result<LiveGeneration, Error> {
        let mut staged = self
            .store
            .stage(bucket, content_key)
            .await
            .map_err(storage::Error::from)?;
        for (key, span) in keys.iter().zip(spans) {
            // The generation the check read, not whatever is live now. A part
            // upload that lands during the copy moves the head and leaves
            // these chunks until the completion drains them.
            let body = self
                .store
                .open_generation(bucket, key, span.seq, span.len)
                .await
                .map_err(storage::Error::from)?;
            stage_body(&mut staged, body).await?;
        }
        Ok(self.store.publish_generation(staged, Some(publish)).await?)
    }

    /// Put the live generation back to the one this completion published over.
    ///
    /// `previous: None` clears the head: the publish was the key's first, and
    /// undoing it leaves no content. A zero-row update means a newer writer
    /// already owns the head. Answers whether this call restored it.
    pub(crate) async fn restore_published_live(
        &self,
        bucket: &Name,
        key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
    ) -> Result<bool, Error> {
        Ok(self
            .store
            .restore_live(bucket, key, published, previous)
            .await
            .map_err(storage::Error::from)?)
    }

    /// Restore the head, then the row, while this publish still owns the head.
    ///
    /// A newer writer keeps both. `row_landed` is the row this completion
    /// wrote; a publish that failed before the row write leaves the row alone.
    #[expect(
        clippy::too_many_arguments,
        reason = "the unversioned undo takes the published generation, the one it replaces and the object row it replaces"
    )]
    pub(crate) async fn restore_unversioned(
        &self,
        bucket: &Name,
        key: &object::Key,
        content_key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
        before: Option<&Row>,
        before_parts: &[PartRow],
        row_landed: bool,
    ) -> Result<bool, Error> {
        let restored = self
            .restore_published_live(bucket, content_key, published, previous)
            .await?;
        if !restored {
            return Ok(false);
        }
        if row_landed {
            if let Some(row) = before {
                ObjectMeta::put_with_parts(&*self.store, bucket, key, row, before_parts)
                    .await
                    .map_err(storage::Error::from)?;
            } else {
                ObjectMeta::remove(&*self.store, bucket, key)
                    .await
                    .map_err(storage::Error::from)?;
            }
        }
        Ok(true)
    }

    /// Drop part bytes after their rows are gone. A failure is logged: the
    /// caller has already committed, and surfacing it would turn a retry into
    /// `NoSuchUpload`.
    pub(crate) async fn remove_part_bytes_best_effort(&self, bucket: &Name, keys: &[ContentKey]) {
        if keys.is_empty() {
            return;
        }
        if let Err(err) = ObjectContent::remove_many(&*self.store, bucket, keys).await {
            warn!(error = %err, "part bytes left after their rows were removed");
        }
    }
}
