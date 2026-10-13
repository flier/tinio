//! The MultipartOps implementation for MemoryStorage.
//!
//! Upload rows and part rows are the shared UploadMeta contract's
//! (MemoryStorage::metadata); a part's bytes cross the shared object-content
//! plane behind mem's accounting decorator (MemoryStorage::content, addressed
//! by a ContentKey::Part). The write order is the contract's: a part's bytes are
//! published before its PARTS row is written, so a completion that reads part
//! rows never finds a row whose bytes are absent, and a refused row write takes
//! the published bytes back out through the decorator so the accounting
//! follows. The mem-local part_meta table keeps the size/mtime pair the content
//! contract carries for objects only (D23 has no part stat): it is a stat
//! plane, not a byte plane, and never passes through the counter.
//!
//! Five bodies are the ops engine's (`tinio_ops::MultipartOps`, built by
//! [`MemoryStorage::multipart_ops`]): the create, the upload read, the part
//! page, the abort and the uploads listing — every one of them a sequence of
//! contract calls with no mem-local plane in it. Two stay here, and the reason
//! is mem's alone: `upload_part` because a refused PARTS row must **restore**
//! the bytes the number held before this call's publish (the surviving row
//! describes them — a plain take-back would leave a live row without its
//! bytes, and only mem can afford to read those bytes before publishing), and
//! `complete_multipart_upload` because its write's version outcome is decided
//! inside ONE transaction (the bucket's state can flip between the caller's
//! read and the write) and its publish forgives the consumed parts' bytes in
//! the same critical section — both mem-local by the E16/RW2 ruling.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

#[cfg(feature = "versioning")]
use crate::_core::{meta::VersionMeta, version::Id};
#[cfg(feature = "versioning")]
use crate::version;
use crate::{
    _core::{
        BodyStream, CompletedPart, ETag, ListPartsParams, ListUploadsParams, MultipartOps,
        MultipartUpload, PartInfo, PartNumber, PartsListing, UploadsListing, acl,
        acl::Acl,
        bucket::Name,
        checksum, collect_body,
        content::{ContentKey, ObjectContent, StagedObject},
        from_nanos,
        meta::{ObjectMeta, UploadMeta, object as object_row},
        multipart::{check_part_minimum, part_number},
        now_nanos, object, storage,
    },
    _store_redb::part_meta,
    Error,
    counting::report,
    error::{access_denied, invalid_key, invalid_part, no_parts, no_such_upload},
    storage::MemoryStorage,
    version::check_bucket_write,
};

impl MemoryStorage {
    /// One upload's stored part numbers, ascending, restricted to the API's
    /// representable range: a stored number outside it has no content-plane key
    /// (nothing could have published one), so the consume skips it.
    ///
    /// Known deviation: the contract's list_parts self-heals a row whose
    /// stored ETag is domain-invalid away, so such a row's content-plane bytes
    /// are never removed by a consume; only external tampering can produce it.
    async fn part_numbers(&self, bucket: &Name, upload_id: &str) -> Result<Vec<PartNumber>, Error> {
        Ok(self
            .metadata()
            .list_parts(bucket, upload_id, part_number(1).map_err(Error::from)?)
            .await?
            .into_iter()
            .filter_map(|row| part_number(row.number).ok())
            .collect())
    }

    /// Put `bytes` back under a part key whose publish this call already
    /// overwrote: the refusal compensation of `upload_part`. The bytes were
    /// readable a moment ago, so the object cap cannot refuse them; an error
    /// here is the store failing.
    async fn restore_part(
        &self,
        bucket: &Name,
        key: &ContentKey,
        bytes: Vec<u8>,
    ) -> Result<(), Error> {
        let mut staged = self.content().stage(bucket, key).await?;
        staged.write(Bytes::from(bytes)).await?;
        self.content().publish(bucket, staged, key).await
    }

    /// The consumed parts' stat sizes (the mem-local stat plane) in one
    /// snapshot: the bytes a version-recording completion releases, which is
    /// the credit its single-transaction object write checks the cap against.
    /// A number without a stat row counts as nothing (only external tampering
    /// can produce one).
    #[cfg(feature = "versioning")]
    fn consumed_bytes(
        &self,
        bucket: &Name,
        upload_id: &str,
        numbers: &[PartNumber],
    ) -> Result<u64, Error> {
        let b = bucket.as_ref().as_str();
        self.db.read(|txn| {
            let meta = part_meta::Table::open_readonly(txn)?;
            let mut credit = 0u64;
            for number in numbers {
                if let Some((size, _)) = meta.get(b, upload_id, u32::from(*number))? {
                    credit = credit.saturating_add(size);
                }
            }
            Ok(credit)
        })
    }

    /// The state machine's hook for a completion on a bucket that records no
    /// versions, with the history's fresh bytes metered in the SAME transaction:
    /// the supersede's blobs are billed before the commit, so a cap refusal takes
    /// the version rows and the blobs back together. A blob committed but never
    /// billed would be a cap a client could reach again and again — and the plain
    /// path's hook delta only grows when the bucket's state flips to Enabled
    /// between the caller's read and this transaction. The transaction opens with
    /// the in-transaction bucket probe (the atomic half the early existence check
    /// cannot be), and the accounting lock is held ACROSS it — the decorator's
    /// lock is not reentrant — so the meter is the unlocked variant.
    #[cfg(feature = "versioning")]
    async fn prepare_completion_publish(
        &self,
        bucket: &Name,
        key: &object::Key,
        now: u64,
    ) -> Result<(Option<Id>, u64, bool), Error> {
        let guard = self.content().lock_accounting().await;
        let done = self
            .db
            .write(|txn| -> Result<(Option<Id>, u64, bool), Error> {
                // The in-transaction bucket probe: redb serializes write
                // transactions, so a teardown committed before this one is visible
                // here and the hook aborts whole instead of writing version rows for
                // a bucket that is gone.
                version::check_bucket_write(txn, bucket)?;
                let (version_id, version_seq, history_bytes, records_versions) =
                    version::prepare_publish(txn, bucket, key, now)?;
                self.content()
                    .adjust_total_with_credit_unlocked(history_bytes, 0)?;
                Ok((version_id, version_seq, records_versions))
            })?;
        drop(guard);
        Ok(done)
    }

    /// Drop every stat row an upload's parts left in the mem-local plane. The
    /// contract's own drain cannot know this table, and it must leave with the
    /// rows (the baseline drained it inside the same transaction). One write
    /// transaction for the whole upload.
    fn drain_part_meta(&self, bucket: &Name, upload_id: &str) -> Result<(), Error> {
        self.db.write(|txn| -> Result<(), Error> {
            part_meta::Table::open(txn)?.drain_upload(bucket.as_ref().as_str(), upload_id)?;
            Ok(())
        })
    }
}

#[async_trait]
impl MultipartOps for MemoryStorage {
    async fn create_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        checksum: Option<checksum::Upload>,
        tags: object::Tags,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<MultipartUpload, Error> {
        // The pre-check is for error ORDER only: a reserved or marker key on
        // a missing bucket answers NoSuchBucket, exactly like the baseline
        // (the authoritative bucket check is the guarded create below, which
        // re-reads the row inside its own write transaction and closes the
        // race window this probe leaves).
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // Folder markers are never objects — refuse the upload up front
        // (the fs backend rejects them at create too; completing one
        // would materialize an invisible, undeletable object).
        if key.is_folder_marker() {
            return Err(invalid_key(key.to_string()));
        }
        // The engine's create body: a fresh UUID id and one contract
        // transaction that writes the upload row and its checksum spec — under
        // mem's policy, the **guarded** form, which refuses when the bucket row
        // is gone (the guard closes the window the pre-check above leaves
        // open).
        self.multipart_ops()
            .create_multipart_upload(bucket, key, checksum, tags, owner, acl)
            .await
            .map_err(Error::from)
    }

    async fn get_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        // The engine's read: one contract lookup serves the existence check,
        // the stored-key identity match, initiated_at, the create-time tags and
        // the checksum spec (the row is present only when it records this key).
        self.multipart_ops()
            .get_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)
    }

    async fn upload_part(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        body: BodyStream,
        checksum: Option<Arc<checksum::PartChecksum>>,
    ) -> Result<PartInfo, Error> {
        // Fast-fail on a missing bucket before buffering the body.
        self.ensure_bucket(bucket).await?;
        let data = collect_body(body).await?;
        // The tee's MD5: a part's ETag IS its content MD5 — the raw digest
        // replaces the second hash over the buffer.
        let etag = match checksum.as_ref().and_then(|c| c.etag_digest()) {
            Some(digest) => ETag::Single(digest),
            None => ETag::from_content(&data),
        };
        let now = now_nanos();
        let size = data.len() as u64;
        let content_key = ContentKey::Part {
            upload_id: upload_id.to_owned(),
            number: part_number,
        };
        // The bytes this number already holds, read BEFORE the publish below
        // overwrites them: the row write can be refused, and the refusal must
        // not leave the surviving PARTS row without its bytes. Missing bytes
        // (InvalidPart) mean there is nothing to restore and the compensation
        // removes the new publish instead; any other read failure is the
        // store's own.
        let previous = match self.content().open(bucket, &content_key, None).await {
            Ok(stored) => Some(collect_body(stored).await?),
            Err(Error::Storage(storage::Error::InvalidPart(_))) => None,
            Err(err) => return Err(err),
        };
        // The contract's order: the bytes first. The staged write is where
        // max_object_bytes refuses (a refused stage publishes nothing) and
        // publish is the visibility point.
        // The published bytes are kept so the compensation below can name
        // exactly what this call published (a later writer's bytes under the
        // same key must not be mistaken for them).
        let published = Bytes::from(data);
        // The publish's identity, recorded with the bytes: a later writer can
        // publish the same bytes under this number (an empty part), and the
        // refusal's compensation must still tell its own publish from theirs.
        let publish_id = self.content().mint_publish_id();
        let mut staged = self.content().stage(bucket, &content_key).await?;
        staged.write(published.clone()).await?;
        self.content()
            .publish_tagged(bucket, staged, &content_key, publish_id)
            .await?;
        // The row the bytes describe: one contract transaction that re-checks
        // the upload's stored key, so the row and its bytes cannot disagree.
        if let Err(err) = self
            .metadata()
            .put_part(
                bucket,
                key,
                upload_id,
                part_number,
                &etag,
                checksum.as_deref(),
            )
            .await
        {
            // The publish above already replaced this number's bytes, so the
            // refusal has to put the plane back in step. When the number still
            // has a PARTS row (the usual refusal: the upload records another
            // key), its old bytes go back — the single-transaction baseline's
            // rollback, rebuilt through the content plane. When the row is
            // gone (a concurrent abort or complete drained the upload), the
            // fresh publish is dropped instead: restoring bytes no row names
            // would leak them. A store failing mid-compensation cannot do
            // better, and the caller still sees the row write's refusal.
            let rowed = self
                .metadata()
                .list_parts(bucket, upload_id, part_number)
                .await
                .is_ok_and(|rows| {
                    rows.first()
                        .is_some_and(|row| row.number == u32::from(part_number))
                });
            let compensated = match previous {
                Some(bytes) if rowed => self.restore_part(bucket, &content_key, bytes).await,
                _ => {
                    self.roll_back_publish(bucket, &content_key, &published, publish_id, "PARTS")
                        .await
                }
            };
            if let Err(failure) = compensated {
                // A compensation that itself fails is reported here; the drop
                // half books its own release in the same failure — its error arm
                // releases the part's remembered length — so the caller must not
                // release again on top: a part's stored length IS its ledger
                // entry, and subtracting it twice would bill the total below the
                // bytes the plane still holds. A failed RESTORE is the other
                // case: the row survives, its key still names bytes, and the
                // entry the publish just left is that key's own length — so
                // nothing may be released for it either.
                report(format_args!(
                    "part compensation after a refused PARTS row failed: {failure}"
                ));
            }
            return Err(err.into());
        }
        // The stat row (mem's own plane): written after the PARTS row, so a
        // listed part never lacks its bytes.
        self.db.write(|txn| -> Result<(), Error> {
            // The in-transaction bucket probe every raw write here opens with:
            // a teardown that committed first must not leave this stat row
            // behind for a bucket that is gone.
            check_bucket_write(txn, bucket)?;
            part_meta::Table::open(txn)?.put(
                bucket.as_ref().as_str(),
                upload_id,
                u32::from(part_number),
                size,
                now,
            )?;
            Ok(())
        })?;
        Ok(PartInfo {
            part_number,
            size,
            etag,
            last_modified: from_nanos(now),
            // The digest committed atomically with the part row.
            checksum: checksum.as_ref().and_then(|c| c.digest.get()).cloned(),
        })
    }

    async fn list_parts(&self, params: ListPartsParams) -> Result<PartsListing, Error> {
        // Bucket existence first (the fs backend answers NoSuchBucket before
        // anything else).
        self.ensure_bucket(&params.bucket).await?;
        // The engine owns the upload's identity check, the marker arithmetic
        // (the exclusive `+1`, and the `u32::MAX` and `max_parts = 0` corners)
        // and the resume rule; the page's rows and the truncation flag are the
        // contract's own read.
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
        // The stat join (mem's own plane) for exactly the page's rows, one
        // read transaction.
        let b = params.bucket.as_ref().as_str();
        let id = params.upload_id.as_str();
        let metas: Vec<Option<(u64, u64)>> = self.db.read(|txn| {
            let meta = part_meta::Table::open_readonly(txn)?;
            page.rows
                .iter()
                .map(|row| Ok(meta.get(b, id, row.number)?))
                .collect::<Result<Vec<_>, Error>>()
        })?;
        let mut parts = Vec::with_capacity(page.rows.len());
        for (row, stat) in page.rows.iter().zip(metas) {
            // A part row without its stat row is skipped (the fs contract).
            let Some((size, mtime)) = stat else {
                continue;
            };
            // The listed part's own write stamp is mem's: the contract's part
            // row carries it, read out of redb's `part_stamps` — the sibling
            // table `put_part` writes in the part row's own transaction — so
            // `to_info` answers that instant as `LastModified`, never the
            // upload's creation time. The `part_meta` mtime rides the
            // projection as the fallback, and stands only for a row that
            // carries no stamp (an older database, or a part written before
            // the stamp existed).
            parts.push(row.to_info(size, from_nanos(mtime)).map_err(Error::from)?);
        }
        Ok(PartsListing {
            parts,
            truncated: page.truncated,
            next_part_number_marker: page.next_part_number_marker,
        })
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
        parts: &[CompletedPart],
        checksum: Option<checksum::Recorded>,
    ) -> Result<object::Info, Error> {
        // Bucket existence first (the fs backend answers NoSuchBucket before
        // anything else — NoParts only for a real upload).
        self.ensure_bucket(bucket).await?;
        if parts.is_empty() {
            return Err(no_parts());
        }
        // The create-time tags, owner and ACL applied at completion come from
        // the upload row — read, never re-ferried through the interface. The
        // contract's get is the identity check on the stored key and the row
        // fetch in one lookup.
        let upload = UploadMeta::get(&**self.metadata(), bucket, key, upload_id)
            .await?
            .ok_or_else(|| no_such_upload(upload_id))?;
        // The completion-time records (stored ETag and checksum, in the
        // requested order) and the stat rows; the outer None of part_records
        // is an upload whose key does not match.
        let numbers: Vec<PartNumber> = parts.iter().map(|part| part.part_number).collect();
        let records = self
            .metadata()
            .part_records(bucket, key, upload_id, &numbers)
            .await?
            .ok_or_else(|| no_such_upload(upload_id))?;
        let b = bucket.as_ref().as_str();
        let metas: Vec<Option<(u64, u64)>> = self.db.read(|txn| {
            let meta = part_meta::Table::open_readonly(txn)?;
            numbers
                .iter()
                .map(|number| Ok(meta.get(b, upload_id, u32::from(*number))?))
                .collect::<Result<Vec<_>, Error>>()
        })?;
        // Verify and assemble before the first write: a refused completion
        // (NoParts, InvalidPart, PartTooSmall) leaves the upload exactly as it
        // was.
        let mut data = Vec::new();
        let mut retained = Vec::with_capacity(parts.len());
        let mut infos = Vec::with_capacity(parts.len());
        let mut prev = 0u32;
        for (index, part) in parts.iter().enumerate() {
            let n = u32::from(part.part_number);
            if n <= prev {
                return Err(invalid_part(n));
            }
            prev = n;
            let Some(record) = records[index].as_ref() else {
                return Err(invalid_part(n));
            };
            // A part row without its stat row reads as absent (the fs split).
            let Some((size, mtime)) = metas[index] else {
                return Err(invalid_part(n));
            };
            // The S3 non-final minimum (the shared rule), enforced against the
            // part state this commit consumes.
            check_part_minimum(n, size, index + 1 == parts.len())?;
            if record.etag != part.etag {
                return Err(invalid_part(n));
            }
            // The part bytes cross the same content seam as the object bytes;
            // a stored row whose bytes are absent answers InvalidPart (the
            // bytes-first order makes that state unreachable for a live row).
            let content_key = ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: part.part_number,
            };
            let body = self.content().open(bucket, &content_key, None).await?;
            data.extend_from_slice(&collect_body(body).await?);
            infos.push(PartInfo {
                part_number: part.part_number,
                size,
                etag: record.etag.clone(),
                last_modified: from_nanos(mtime),
                checksum: record.checksum.clone(),
            });
            retained.push(object_row::PartRow {
                number: n,
                size,
                checksum: record.checksum.clone(),
            });
        }
        // The parts list is checked non-empty at this operation's entry (the
        // NoParts refusal above), and every part pushes exactly one info, so a
        // composed ETag always exists here; the refusal arm keeps the impossible
        // case an error rather than a panic (style.md §Panics).
        let Some(etag) = ETag::composed_from_parts(&infos) else {
            return Err(no_parts());
        };
        let now = now_nanos();
        let size = data.len() as u64;
        let object_key = ContentKey::Object(key.clone());
        // The row the object's bytes describe: built once, written by whichever
        // write path this bucket takes, so both carry the same facts.
        let row = object_row::Row {
            etag: etag.clone(),
            size,
            mtime: from_nanos(now),
            identity: 0,
            tags: upload.tags.clone(),
            checksum: checksum.clone(),
            owner: upload.owner.clone(),
            acl: upload.acl.clone(),
        };
        // The upload's own part numbers, read before its rows go: the part
        // bytes leave only after they can no longer be listed, and for a
        // version-recording bucket they are the credit the object's size is
        // admitted against.
        let consumed = self.part_numbers(bucket, upload_id).await?;
        let part_keys: Vec<ContentKey> = consumed
            .iter()
            .map(|number| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number: *number,
            })
            .collect();
        // A bucket that records versions takes the single-transaction core (the
        // object put path's shape): the outgoing version, the assembled bytes,
        // the OBJECT_META row, the retained OBJECT_PARTS rows and the fresh
        // identity land in ONE transaction, so a cap refusal aborts the whole
        // completion and leaves the upload, its parts and its history as they
        // were. The state is read ahead only to pick the path; the core's own
        // transaction re-reads it, so a flip between the two is still decided
        // by the write.
        #[cfg(feature = "versioning")]
        let (version_id, records_versions) =
            if VersionMeta::bucket_state(self.metadata().as_ref(), bucket)
                .await
                .map_err(Error::from)?
                .records_versions()
            {
                // The credit: the bytes this completion releases, read from the
                // mem-local stat plane before any of it moves. The accounting lock
                // is held ACROSS the transaction because the core meters inside it
                // and the decorator's lock is not reentrant: everything inside uses
                // the unlocked entry points.
                let credit = self.consumed_bytes(bucket, upload_id, &consumed)?;
                let guard = self.content().lock_accounting().await;
                // The assembled body is written raw by the core, so the
                // single-object cap is checked here (never the total: the core's
                // meter owns that, with the credit above).
                self.content().check_object_bytes_unlocked(size)?;
                let done = self.db.write(|txn| {
                    version::complete_versioned(
                        self, txn, bucket, key, now, &data, &row, &retained, credit,
                    )
                })?;
                drop(guard);
                (done.version_id, done.records_versions)
            } else {
                // A bucket that records no versions keeps the contract's order: the
                // assembled bytes are staged first (where max_object_bytes refuses,
                // before any version state moves), then the state machine's hook
                // runs — in ONE transaction, with the history's fresh bytes metered
                // inside it — and is the authority on what this write records (the
                // state can flip between the read above and the write), and the
                // bytes publish as the row's predecessor.
                let mut staged = self.content().stage(bucket, &object_key).await?;
                staged.write(Bytes::from(data)).await?;
                let (version_id, version_seq, records_versions) =
                    self.prepare_completion_publish(bucket, key, now).await?;
                // The publish's cap check forgives the consumed parts (the credit)
                // while the total still counts them until the batch release below:
                // the total is exact at the end of the consume, not at the publish.
                self.content()
                    .publish_replacing(bucket, staged, &object_key, &part_keys)
                    .await?;
                self.metadata()
                    .put_with_parts(bucket, key, &row, &retained)
                    .await?;
                // The slot the completion made current: only a version-recording
                // bucket moves it (the hook's own answer).
                if records_versions {
                    self.metadata()
                        .advance_slot(bucket, key, version_id.clone(), version_seq)
                        .await?;
                }
                (version_id, records_versions)
            };
        // Without the versioning feature no bucket records an identity, so
        // there is no state to read and every bucket takes this path.
        #[cfg(not(feature = "versioning"))]
        let (version_id, records_versions) = {
            let mut staged = self.content().stage(bucket, &object_key).await?;
            staged.write(Bytes::from(data)).await?;
            self.content()
                .publish_replacing(bucket, staged, &object_key, &part_keys)
                .await?;
            self.metadata()
                .put_with_parts(bucket, key, &row, &retained)
                .await?;
            (None, false)
        };
        // The consume: one contract transaction drains the upload row, its
        // checksum row and every part row (idempotent, the fs drain parity), the
        // mem-local stat plane leaves with them, and the part bytes leave LAST
        // in one batch through the decorator — the release that makes the
        // credited bytes real, exactly once.
        //
        // Known deviation: the drain is a contract transaction of its own, so a
        // completion is not one transaction end to end; the object write has
        // already committed, and the in-memory backend has no crash surface
        // between it and the consume.
        self.metadata().drain_upload(bucket, upload_id).await?;
        self.drain_part_meta(bucket, upload_id)?;
        self.content().remove_many(bucket, &part_keys).await?;
        Ok(object::Info {
            key: key.clone(),
            size,
            last_modified: from_nanos(now),
            etag,
            tags: upload.tags,
            checksum,
            owner: upload.owner,
            acl: upload.acl,
            version_id,
            bucket_records_versions: records_versions,
        })
    }

    async fn abort_multipart_upload(
        &self,
        bucket: &Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        // Bucket existence first (the fs backend answers NoSuchBucket before
        // anything else).
        self.ensure_bucket(bucket).await?;
        // The upload's own part numbers, read before its rows go: the bytes
        // leave only after they can no longer be listed.
        let numbers = self.part_numbers(bucket, upload_id).await?;
        // The engine's abort: the identity check and the drain in ONE contract
        // transaction, so a racing complete cannot slip between them — an
        // absent or key-mismatched upload refuses with NoSuchUpload and drains
        // nothing.
        self.multipart_ops()
            .abort_multipart_upload(bucket, key, upload_id)
            .await
            .map_err(Error::from)?;
        // The mem-local stat plane leaves with the rows (the contract's drain
        // cannot know it).
        self.drain_part_meta(bucket, upload_id)?;
        // The bytes leave only after their rows, in one batch through the
        // decorator, so the accounting follows exactly once.
        let part_keys: Vec<ContentKey> = numbers
            .into_iter()
            .map(|number| ContentKey::Part {
                upload_id: upload_id.to_owned(),
                number,
            })
            .collect();
        self.content().remove_many(bucket, &part_keys).await?;
        Ok(())
    }

    async fn list_multipart_uploads(
        &self,
        params: ListUploadsParams,
    ) -> Result<UploadsListing, Error> {
        self.ensure_bucket(&params.bucket).await?;
        // The engine's page: the resume marker (the composite key plus
        // upload_id, computed and split back in `tinio_core`), the prefix
        // filter, the delimiter rollup and the truncation/resume rules.
        self.multipart_ops()
            .list_multipart_uploads(params)
            .await
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{
            BucketOps, CompletedPart, ListUploadsParams, MultipartOps, ObjectOps, PartInfo,
            acl::Acl,
            bucket,
            content::ContentKey,
            multipart::{MIN_PART_BYTES, part_number},
            object,
            storage::Error::*,
        },
        _util::testing::{
            body,
            metadata::{assert_content_conformance, assert_upload_meta_conformance},
            read_body,
        },
        MemoryOptions,
        testutil::{checksum_tee, publish_tagged_bytes},
    };

    fn completed(part: &PartInfo) -> CompletedPart {
        CompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
        }
    }

    async fn with_bucket() -> (MemoryStorage, Name) {
        let storage = MemoryStorage::new().unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        (storage, name)
    }

    /// A refused PARTS row write must not leave the number's OLD row without
    /// bytes: the publish above has already overwritten them, so the refusal
    /// path puts them back. The single-transaction baseline rolled the whole
    /// part write back; Review Focus 5's invariant is that a live row always
    /// has its bytes.
    #[tokio::test]
    async fn a_refused_part_row_write_keeps_the_row_and_its_bytes_together() {
        let (storage, b) = with_bucket().await;
        let key = object::key("part.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let first = storage
            .upload_part(
                &b,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"first"),
                None,
            )
            .await
            .unwrap();
        let content_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: first.part_number,
        };

        // The same part number under a key the upload does not record: the
        // bytes publish, then the PARTS row write is refused (NoSuchUpload).
        let other = object::key("other.bin").unwrap();
        let err = storage
            .upload_part(
                &b,
                &other,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"second"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Storage(NoSuchUpload(id)) if id == &upload.upload_id),
            "the row write is refused: {err}"
        );

        // The old row survives, so its bytes must: the row and the bytes the
        // completion would read still agree.
        let rows = storage
            .metadata()
            .list_parts(&b, &upload.upload_id, part_number(1).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "the old PARTS row is still there");
        assert_eq!(rows[0].etag, first.etag, "and it is still the old part");
        let stream = storage
            .content()
            .open(&b, &content_key, None)
            .await
            .expect("the old row's bytes are still readable");
        assert_eq!(read_body(stream).await.unwrap(), b"first");
    }

    /// The other half of the refusal compensation: when the upload's rows are
    /// gone (a concurrent abort or complete drained it), the freshly published
    /// bytes must leave too — restoring bytes no row names would leak them.
    #[tokio::test]
    async fn a_refused_part_row_write_drops_the_bytes_when_the_row_is_gone() {
        let (storage, b) = with_bucket().await;
        let key = object::key("drained.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let first = storage
            .upload_part(
                &b,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"kept"),
                None,
            )
            .await
            .unwrap();
        let content_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: first.part_number,
        };
        // The rows leave while the bytes stay: the window a drain opens.
        storage
            .metadata()
            .drain_upload(&b, &upload.upload_id)
            .await
            .unwrap();
        assert!(
            storage.content().open(&b, &content_key, None).await.is_ok(),
            "the old bytes are still in the plane"
        );

        // A part write for the drained upload: the bytes publish, the row
        // write is refused, and the compensation must take the bytes out.
        let err = storage
            .upload_part(
                &b,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"new"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Storage(NoSuchUpload(id)) if id == &upload.upload_id),
            "the row write is refused: {err}"
        );
        assert!(
            storage
                .content()
                .open(&b, &content_key, None)
                .await
                .is_err(),
            "the compensation dropped the bytes no row names"
        );
    }

    /// The drop half of a refused part write's compensation never takes another
    /// writer's publish: the tag and the comparison are one contract call, so a
    /// publish that landed after ours owns the key — its bytes stay, and its
    /// accounting entry stays with them **even when it published the same
    /// bytes** (two empty parts), which the byte compare alone cannot tell
    /// apart.
    #[tokio::test]
    async fn a_refused_part_write_leaves_another_writers_bytes_alone() {
        // A tracked total, so the release half is observable too: the
        // replacement's bytes must stay billed.
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(1024),
        })
        .unwrap();
        let b = bucket::name("part-race").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = ContentKey::Part {
            upload_id: "u".to_owned(),
            number: part_number(1).unwrap(),
        };
        // Ours first, then a later writer's over the same key — the same
        // bytes, so only the recorded publish identity separates them.
        let ours = storage.content().mint_publish_id();
        publish_tagged_bytes(&storage, &b, &key, b"mine", ours).await;
        let theirs = storage.content().mint_publish_id();
        publish_tagged_bytes(&storage, &b, &key, b"mine", theirs).await;
        assert_eq!(
            storage.total_bytes(),
            4,
            "an equal-length replacement bills only the difference"
        );

        // The refused write's compensation names ITS publish and finds
        // another's: nothing is deleted and nothing is released.
        storage
            .roll_back_publish(&b, &key, b"mine", ours, "PARTS")
            .await
            .unwrap();
        assert_eq!(
            read_body(storage.content().open(&b, &key, None).await.unwrap())
                .await
                .unwrap(),
            b"mine",
            "the later writer's byte-identical content survives the compensation"
        );
        assert_eq!(storage.total_bytes(), 4, "and stays billed");

        // Its own publish it does take back, accounting included.
        storage
            .roll_back_publish(&b, &key, b"mine", theirs, "PARTS")
            .await
            .unwrap();
        assert!(
            storage.content().open(&b, &key, None).await.is_err(),
            "our own publish is taken back"
        );
        assert_eq!(storage.total_bytes(), 0, "and released");
    }

    /// A refused version-recording completion leaves the repository exactly as
    /// it was: the object core is ONE transaction, so the upload, its part rows
    /// and bytes, the outgoing version and the history all survive untouched.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_refused_versioned_completion_leaves_history_rows_and_bytes_untouched() {
        use crate::_core::{
            storage::VersionOps,
            version::{ListParams, ReadResult, State},
        };

        // The assembled object exceeds the single-object cap while each part
        // fits it: the completion refuses before its transaction can move any
        // version state.
        let min = MIN_PART_BYTES as usize;
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: Some(MIN_PART_BYTES),
            max_total_bytes: None,
        })
        .unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();
        let key = object::key("refused-completion.bin").unwrap();
        // The version the completion would supersede.
        storage
            .put_object(&b, &key, body(b"first".to_vec()))
            .await
            .unwrap();
        let params = || ListParams {
            max_keys: 1000,
            ..Default::default()
        };
        let first_id = storage
            .list_object_versions(&b, params())
            .await
            .unwrap()
            .entries[0]
            .id
            .clone()
            .expect("an enabled write mints an identity");
        let upload = storage
            .create_multipart_upload(
                &b,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let first = storage
            .upload_part(
                &b,
                &key,
                &upload.upload_id,
                1.into(),
                body(vec![b'a'; min]),
                None,
            )
            .await
            .unwrap();
        let second = storage
            .upload_part(&b, &key, &upload.upload_id, 2.into(), body(b"b"), None)
            .await
            .unwrap();
        let first_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: first.part_number,
        };
        let second_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: second.part_number,
        };

        let err = storage
            .complete_multipart_upload(
                &b,
                &key,
                &upload.upload_id,
                &[completed(&first), completed(&second)],
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Storage(EntityTooLarge { size, limit })
                    if size == MIN_PART_BYTES + 1 && limit == MIN_PART_BYTES
            ),
            "the assembled object is refused: {err}"
        );

        // The upload is intact: its row, both part rows and both byte values.
        assert!(
            UploadMeta::get(&**storage.metadata(), &b, &key, &upload.upload_id)
                .await
                .unwrap()
                .is_some(),
            "the upload row survives the refusal"
        );
        let rows = storage
            .metadata()
            .list_parts(&b, &upload.upload_id, part_number(1).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 2, "the part rows survive the refusal");
        assert_eq!(
            read_body(storage.content().open(&b, &first_key, None).await.unwrap())
                .await
                .unwrap()
                .len(),
            min,
            "the first part's bytes survive the refusal"
        );
        assert_eq!(
            read_body(storage.content().open(&b, &second_key, None).await.unwrap())
                .await
                .unwrap(),
            b"b",
            "the second part's bytes survive the refusal"
        );

        // No fresh version and no history row: the outgoing version is still
        // current, readable and alone.
        let page = storage.list_object_versions(&b, params()).await.unwrap();
        assert_eq!(page.entries.len(), 1, "no version entered history");
        assert_eq!(page.entries[0].id.as_ref(), Some(&first_id));
        let ReadResult::Object(old) = storage
            .get_object_version(&b, &key, Some(&first_id), None)
            .await
            .unwrap()
        else {
            panic!("the outgoing version is still an object version");
        };
        assert_eq!(read_body(old.body).await.unwrap(), b"first");
    }

    /// The hook the plain path runs meters the history's fresh bytes in the
    /// transaction that stores them: a cap refusal takes the version rows and the
    /// blobs back together. The public surface cannot reach a growing hook delta
    /// on that path — a bucket that records no versions never supersedes, so the
    /// delta there is always zero, and only the state flip between the
    /// completion's read and its transaction makes it grow — so the shape is
    /// pinned white-box, at the layer that flip would hit.
    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_refused_hook_takes_its_version_rows_and_history_blobs_back() {
        use crate::_core::{
            storage::VersionOps,
            version::{ListParams, ReadResult, State},
        };

        // The cap is exactly what the bucket already holds: the hook's fresh
        // history bytes (the outgoing version's copy) cannot fit.
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(5),
        })
        .unwrap();
        let b = bucket::name("hook-cap").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();
        let key = object::key("superseded.bin").unwrap();
        storage
            .put_object(&b, &key, body(b"first".to_vec()))
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 5, "the cap is the bytes stored");

        let params = || ListParams {
            max_keys: 1000,
            ..Default::default()
        };
        let before = storage.list_object_versions(&b, params()).await.unwrap();
        assert_eq!(before.entries.len(), 1, "one current version");
        let first_id = before.entries[0]
            .id
            .clone()
            .expect("an enabled write mints an identity");

        let err = storage
            .prepare_completion_publish(&b, &key, now_nanos())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::Storage(EntityTooLarge { .. })),
            "the hook's fresh history bytes cannot fit the cap: {err}"
        );

        // Nothing moved and nothing was billed: the version is still current and
        // alone (no history row), its bytes are readable, and the total is the
        // bytes the bucket really holds.
        assert_eq!(storage.total_bytes(), 5, "the breach billed nothing");
        let after = storage.list_object_versions(&b, params()).await.unwrap();
        assert_eq!(after.entries.len(), 1, "no version entered the history");
        assert_eq!(after.entries[0].id.as_ref(), Some(&first_id));
        let ReadResult::Object(still) = storage
            .get_object_version(&b, &key, Some(&first_id), None)
            .await
            .unwrap()
        else {
            panic!("the version is still an object version");
        };
        assert_eq!(read_body(still.body).await.unwrap(), b"first");
    }

    /// A completion releases its part bytes exactly once, on both write paths:
    /// the plane's total ends at the bytes the repository really holds.
    #[tokio::test]
    async fn a_completed_upload_releases_its_part_bytes_exactly_once() {
        let options = || MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(1024),
        };
        // An unversioned bucket with nothing at the key: the assembled object
        // is all that is left.
        let storage = MemoryStorage::with_options(options()).unwrap();
        let b = bucket::name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("once.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &b,
                &key,
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
                &key,
                &upload.upload_id,
                1.into(),
                body(b"assembled"),
                None,
            )
            .await
            .unwrap();
        let content_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: part.part_number,
        };
        assert_eq!(storage.total_bytes(), 9, "the part's bytes are counted");
        let info = storage
            .complete_multipart_upload(&b, &key, &upload.upload_id, &[completed(&part)], None)
            .await
            .unwrap();
        assert_eq!(info.size, 9);
        assert!(
            storage
                .content()
                .open(&b, &content_key, None)
                .await
                .is_err(),
            "the part bytes leave with the consume"
        );
        assert_eq!(
            storage.total_bytes(),
            9,
            "the total is the object's bytes alone"
        );

        // A version-recording bucket with an outgoing version: the plane ends
        // at the live object PLUS the history copy the supersede stored, and
        // the released part is still subtracted exactly once.
        #[cfg(feature = "versioning")]
        {
            use crate::_core::{storage::VersionOps, version::State};

            let storage = MemoryStorage::with_options(options()).unwrap();
            let b = bucket::name("data").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("once-versioned.bin").unwrap();
            storage
                .put_object(&b, &key, body(b"first".to_vec()))
                .await
                .unwrap();
            let upload = storage
                .create_multipart_upload(
                    &b,
                    &key,
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
                    &key,
                    &upload.upload_id,
                    1.into(),
                    body(b"assembled"),
                    None,
                )
                .await
                .unwrap();
            let content_key = ContentKey::Part {
                upload_id: upload.upload_id.clone(),
                number: part.part_number,
            };
            assert_eq!(
                storage.total_bytes(),
                14,
                "the outgoing object and the part are both counted"
            );
            let info = storage
                .complete_multipart_upload(&b, &key, &upload.upload_id, &[completed(&part)], None)
                .await
                .unwrap();
            assert!(
                info.version_id.is_some(),
                "a versioned completion mints an identity"
            );
            assert!(
                storage
                    .content()
                    .open(&b, &content_key, None)
                    .await
                    .is_err(),
                "the part bytes leave with the consume"
            );
            assert_eq!(
                storage.total_bytes(),
                14,
                "9 live object bytes + 5 history bytes, the part released once"
            );
        }
    }

    #[tokio::test]
    async fn part_size_limit_rejects_oversized_parts() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: Some(4),
            max_total_bytes: None,
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &name,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();

        let err = storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"12345"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { size: 5, limit: 4 })),
            "{err}"
        );
        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"1234"),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn abort_releases_part_bytes() {
        let storage = MemoryStorage::with_options(MemoryOptions {
            max_object_bytes: None,
            max_total_bytes: Some(8),
        })
        .unwrap();
        let name = bucket::name("data").unwrap();
        storage
            .create_bucket(&name, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("big.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &name,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();

        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(1).unwrap(),
                body(b"12345"),
                None,
            )
            .await
            .unwrap();
        let err = storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(2).unwrap(),
                body(b"1234"),
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::Storage(EntityTooLarge { .. })),
            "{err}"
        );
        storage
            .upload_part(
                &name,
                &key,
                &upload.upload_id,
                part_number(2).unwrap(),
                body(b"123"),
                None,
            )
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 8);

        storage
            .abort_multipart_upload(&name, &key, &upload.upload_id)
            .await
            .unwrap();
        assert_eq!(storage.total_bytes(), 0);

        // The freed capacity is reusable by a new upload.
        let u2 = storage
            .create_multipart_upload(
                &name,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        storage
            .upload_part(
                &name,
                &key,
                &u2.upload_id,
                part_number(1).unwrap(),
                body(b"12345678"),
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn upload_ids_are_unique() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let a = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let b = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        assert_ne!(a.upload_id, b.upload_id);
        assert_eq!(a.upload_id.len(), 36);
    }

    #[tokio::test]
    async fn complete_uses_only_listed_parts() {
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        // The listed non-final parts must be >= the 5 MiB minimum (the
        // authoritative in-txn check); the unlisted part may be small.
        let min = MIN_PART_BYTES as usize;
        let data1 = vec![b'a'; min];
        let data2 = vec![b'b'; min];
        let mut uploaded = Vec::new();
        for (n, data) in [
            (1u32, data1.clone()),
            (2, data2.clone()),
            (3, b"ccc".to_vec()),
        ] {
            uploaded.push(
                storage
                    .upload_part(&bucket, &key, &upload.upload_id, n.into(), body(data), None)
                    .await
                    .unwrap(),
            );
        }
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[completed(&uploaded[0]), completed(&uploaded[1])],
                None,
            )
            .await
            .unwrap();
        let expect = [data1, data2].concat();
        assert_eq!(completed.size, expect.len() as u64);
        let got = storage.get_object(&bucket, &key, None).await.unwrap();
        assert_eq!(read_body(got.body).await.unwrap(), expect);
        assert!(completed.etag.as_str().ends_with("-2"));
    }

    #[tokio::test]
    async fn a_failed_complete_survives_in_txn() {
        // The authoritative in-txn size check fails the complete — and
        // the write transaction rolls back, leaving the upload alive for
        // a corrected retry (S3). The failure shape itself (one byte
        // under the minimum → PartTooSmall) is the shared conformance
        // leg.
        let (storage, bucket) = with_bucket().await;
        let key = object::key("a.bin").unwrap();
        let min = MIN_PART_BYTES as usize;
        let upload = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let under = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(vec![b'b'; min - 1]),
                None,
            )
            .await
            .unwrap();
        let small = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                2.into(),
                body(b"x".to_vec()),
                None,
            )
            .await
            .unwrap();
        let err = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[completed(&under), completed(&small)],
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(PartTooSmall { .. })), "{err}");
        assert!(
            storage
                .get_multipart_upload(&bucket, &key, &upload.upload_id)
                .await
                .is_ok(),
            "the failed complete must leave the upload alive"
        );
    }

    #[tokio::test]
    async fn list_uploads_on_missing_bucket_is_no_such_bucket() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("gone").unwrap();
        assert!(matches!(
            storage
                .list_multipart_uploads(ListUploadsParams {
                    bucket,
                    prefix: String::new(),
                    delimiter: None,
                    key_marker: None,
                    upload_id_marker: None,
                    max_uploads: 1000,
                })
                .await
                .unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
    }

    #[tokio::test]
    async fn mem_complete_retains_object_parts() {
        // spec 2026-08-31: completion persists the assembled parts into
        // OBJECT_PARTS — list_object_parts serves them in part order with
        // sizes and the per-part checksums, and the object row carries
        // the completion's tags and the interface-computed composite
        // checksum (the backend never hashes).
        let (storage, b) = with_bucket().await;
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
        assert_eq!(upload.tags, tags, "the create-time tags ride on the upload");
        assert_eq!(
            storage
                .get_multipart_upload(&b, &k, &upload.upload_id)
                .await
                .unwrap()
                .tags,
            tags,
            "the stored upload row echoes the create-time tags"
        );
        // Two parts: the non-final first must meet the 5 MiB S3 minimum;
        // both upload with a preset tee so the retained rows carry
        // per-part checksums (the digest values are the tee's — the
        // backend never validates them against the bytes).
        let min = MIN_PART_BYTES as usize;
        let first = vec![b'a'; min];
        let p1 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
                body(first.clone()),
                Some(checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==")),
            )
            .await
            .unwrap();
        let p2 = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                2.into(),
                body(b"world".to_vec()),
                Some(checksum_tee(
                    checksum::Algorithm::Md5,
                    "N7SrFkGmSbsK8h0dJ9nK1Q==",
                )),
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
        assert_eq!(
            storage.head_object(&b, &k).await.unwrap().checksum,
            Some(composite),
            "the object row carries the composite"
        );

        // The retained part rows: order, sizes, per-part checksums.
        let parts = storage.list_object_parts(&b, &k).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(u32::from(parts[0].part_number), 1);
        assert_eq!(parts[0].size, min as u64);
        let checksum = parts[0].checksum.as_ref().expect("part 1's checksum");
        assert_eq!(checksum.algorithm, checksum::Algorithm::Crc32);
        assert_eq!(checksum.value.0, "NhCmhg==");
        assert_eq!(u32::from(parts[1].part_number), 2);
        assert_eq!(parts[1].size, 5);
        let checksum = parts[1].checksum.as_ref().expect("part 2's checksum");
        assert_eq!(checksum.algorithm, checksum::Algorithm::Md5);
        assert_eq!(checksum.value.0, "N7SrFkGmSbsK8h0dJ9nK1Q==");

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
            .upload_part(&b, &k, &upload2.upload_id, 3.into(), body(b"tail"), None)
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
        assert!(
            parts[0].checksum.is_none(),
            "a part uploaded without a tee retains no checksum"
        );
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn a_completion_supersedes_and_names_its_version_on_an_enabled_bucket() {
        use crate::_core::{
            storage::VersionOps,
            version::{ListParams, ReadResult, State},
        };

        let (storage, b) = with_bucket().await;
        storage
            .set_bucket_versioning(&b, State::Enabled)
            .await
            .unwrap();
        let key = object::key("versioned-mp.bin").unwrap();
        // A plain write first: the version the completion must supersede.
        storage
            .put_object(&b, &key, body(b"first".to_vec()))
            .await
            .unwrap();
        let params = || ListParams {
            max_keys: 1000,
            ..Default::default()
        };
        let first_id = storage
            .list_object_versions(&b, params())
            .await
            .unwrap()
            .entries[0]
            .id
            .clone()
            .expect("an enabled write mints an identity");

        let upload = storage
            .create_multipart_upload(
                &b,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap();
        let part = storage
            .upload_part(&b, &key, &upload.upload_id, 1.into(), body(b"mp"), None)
            .await
            .unwrap();
        let info = storage
            .complete_multipart_upload(
                &b,
                &key,
                &upload.upload_id,
                &[CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag,
                }],
                None,
            )
            .await
            .unwrap();
        // The completion ran the publish protocol: its Info names the fresh
        // identity and the plain read serves the completed bytes under it.
        let current_id = info
            .version_id
            .clone()
            .expect("a completion on an enabled bucket mints an identity");
        assert_ne!(current_id, first_id);
        let head = storage.head_object(&b, &key).await.unwrap();
        assert_eq!(head.version_id.as_ref(), Some(&current_id));
        // The completed object's composed ETag rides with the version.
        assert_eq!(head.etag, info.etag);
        assert_eq!(
            read_body(storage.get_object(&b, &key, None).await.unwrap().body)
                .await
                .unwrap(),
            b"mp"
        );

        // The superseded version is intact under its own id — the completion
        // superseded the outgoing version instead of overwriting the row.
        let ReadResult::Object(old) = storage
            .get_object_version(&b, &key, Some(&first_id), None)
            .await
            .unwrap()
        else {
            panic!("the superseded version must be an object version");
        };
        assert_eq!(read_body(old.body).await.unwrap(), b"first");
        assert_eq!(old.info.version_id.as_ref(), Some(&first_id));

        // Both versions are listed, current first.
        let page = storage.list_object_versions(&b, params()).await.unwrap();
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].id.as_ref(), Some(&current_id));
        assert_eq!(page.entries[1].id.as_ref(), Some(&first_id));
    }
    /// The UploadMeta contract's own legs, run from this crate's second
    /// consumer (P2 Task 2's acceptance).
    #[tokio::test]
    async fn upload_meta_conformance() {
        let storage = MemoryStorage::new().unwrap();
        assert_upload_meta_conformance(&**storage.metadata()).await;
    }

    /// The ObjectContent contract's own legs — the D23 part-key surface, the
    /// move/copy/scan/stat/bucket-lifecycle primitives and the 8 MiB staged
    /// leg — run from the second consumer.
    #[tokio::test]
    async fn object_content_conformance() {
        let storage = MemoryStorage::new().unwrap();
        assert_content_conformance(&**storage.content()).await;
    }

    /// A part's PARTS row exists exactly while its bytes are readable: the
    /// bytes-first order observed at the two points that matter — after the
    /// part upload, and after the completion that consumes it.
    #[tokio::test]
    async fn a_part_row_exists_exactly_while_its_bytes_are_readable() {
        let (storage, b) = with_bucket().await;
        let k = object::key("order.bin").unwrap();
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
                1.into(),
                body(b"bytes".to_vec()),
                None,
            )
            .await
            .unwrap();
        // The row is there...
        let rows = storage
            .metadata()
            .list_parts(&b, &upload.upload_id, part_number(1).unwrap())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "the part row landed");
        // ...and its bytes are readable.
        let content_key = ContentKey::Part {
            upload_id: upload.upload_id.clone(),
            number: part.part_number,
        };
        let stream = storage
            .content()
            .open(&b, &content_key, None)
            .await
            .unwrap();
        assert_eq!(read_body(stream).await.unwrap(), b"bytes");

        // The completion consumes the part: the row is gone first, and its
        // bytes are gone with it (a row never outlives its bytes).
        storage
            .complete_multipart_upload(&b, &k, &upload.upload_id, &[completed(&part)], None)
            .await
            .unwrap();
        assert!(
            storage
                .metadata()
                .list_parts(&b, &upload.upload_id, part_number(1).unwrap())
                .await
                .unwrap()
                .is_empty(),
            "the consumed part row is drained"
        );
        assert!(
            storage
                .content()
                .open(&b, &content_key, None)
                .await
                .is_err(),
            "the consumed part's bytes are gone"
        );
    }
}
