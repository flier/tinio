//! The `UploadMeta` home over [`crate::RedbMetadata`] — Task 7's implementation.
//!
//! Upload records live in the shared `uploads` table (the five-tuple
//! `(key, initiated-at unix nanos, tags wire, owner wire, acl grants wire)`,
//! the create-time checksum spec in the separate `upload_checksums` row, a
//! part's ETag in `parts` and its checksum in `part_checksums`. The
//! decode/encode rules are [`tinio_core::meta::upload`]'s, so this module
//! cannot drift from the contract's row types.
//!
//! Every method is one read or one write transaction (design item 1): the
//! identity check of `put_part` runs **inside** the transaction that writes
//! the `parts` and `part_checksums` rows, so the two cannot disagree; `create`
//! writes the upload row and its checksum row together (a `None` spec clears a
//! stale checksum row); `part_records` is the completion-time join in one read
//! transaction. A refused identity (`NoSuchUpload`) projects through
//! [`Error::Refused`] (D22).

use redb::{ReadTransaction, WriteTransaction};

use crate::{
    _core::{
        ETag, MultipartUpload, bucket, checksum,
        meta::{
            UploadMeta,
            upload::{PartRecord, PartRow, PartsPage, Row},
        },
        multipart, object, storage,
    },
    Error, RedbMetadata,
    bucket::Table as BucketTable,
    part::Table as PartTable,
    part_checksum::Table as PartChecksumTable,
    upload::Table as UploadTable,
    upload_checksum::Table as UploadChecksumTable,
};

/// Decode one stored upload row plus its checksum spec.
///
/// `None` is a domain-invalid key — the row reads as absent (the decode rule
/// is [`Row::from_raw`]'s; the checksum spec rides its own table).
fn decode_row(
    upload_id: &str,
    key: &str,
    initiated_at: u64,
    tags: &str,
    owner: &str,
    grants: &str,
    checksum: Option<checksum::Upload>,
) -> Option<Row> {
    let mut row = Row::from_raw(upload_id, key, initiated_at, tags, owner, grants)?;
    row.checksum = checksum;
    Some(row)
}

/// The composite `(key, upload_id)` order the listings answer in — the order
/// the pagination engine requires.
fn sort_uploads(uploads: &mut [MultipartUpload]) {
    uploads.sort_by(|a, b| {
        (a.key.as_ref(), a.upload_id.as_str()).cmp(&(b.key.as_ref(), b.upload_id.as_str()))
    });
}

/// Materialize one bucket's uploads: every `uploads` row of `bucket` decoded
/// with the `upload_checksums` join and projected with `name` — the body the
/// two bucket listings share. The caller owns the sort (`list_uploads`) or the
/// pagination (`list_uploads_page`), because only one of them takes the walk to
/// the blocking pool.
fn collect_uploads(
    txn: &ReadTransaction,
    bucket: &str,
    name: &bucket::Name,
) -> Result<Vec<MultipartUpload>, Error> {
    let table = UploadTable::open_readonly(txn)?;
    let checksums = UploadChecksumTable::open_readonly(txn)?;
    let mut uploads = Vec::new();
    table.for_bucket(bucket, |upload_id, stored| {
        let (key, initiated_at, tags, owner, grants) = stored;
        let checksum = checksums.get(bucket, upload_id)?;
        if let Some(row) = decode_row(upload_id, key, initiated_at, tags, owner, grants, checksum) {
            uploads.push(row.to_upload(name));
        }
        Ok(())
    })?;
    Ok(uploads)
}

/// Attach the per-part checksum to each recorded `(number, hex)` row, skipping
/// a part whose stored ETag the domain rejects (the `PartRow` self-heal) — the
/// join the two part listings share. The checksum source is the caller's:
/// `list_parts` reads the table per part, while `list_parts_page` passes `None`
/// when its `has_upload` probe says the upload has no checksum rows, so the
/// point reads are skipped entirely (F03).
fn join_parts(
    recorded: Vec<(u32, String)>,
    mut checksum_of: impl FnMut(u32) -> Result<Option<checksum::Part>, Error>,
) -> Result<Vec<PartRow>, Error> {
    let mut rows = Vec::with_capacity(recorded.len());
    for (number, hex) in recorded {
        let Ok(etag) = ETag::new(&hex) else {
            continue;
        };
        rows.push(PartRow {
            number,
            etag,
            checksum: checksum_of(number)?,
        });
    }
    Ok(rows)
}

/// One upload row and its checksum spec, written through the caller's
/// transaction: the shared body of [`UploadMeta::create`] and
/// [`UploadMeta::create_guarded`].
fn put_upload_row(
    txn: &mut WriteTransaction,
    bucket: &str,
    upload_id: &str,
    row: &Row,
) -> Result<(), Error> {
    let wires = row.to_raw();
    UploadTable::open(txn)?.put(
        bucket,
        upload_id,
        wires.key.as_ref(),
        row.initiated_at,
        &wires.tags,
        &wires.owner,
        &wires.grants,
    )?;
    // The spec's own row commits with the upload row: write it, or clear a
    // stale row of an id being reused.
    let mut specs = UploadChecksumTable::open(txn)?;
    match &row.checksum {
        Some(spec) => specs.put(bucket, upload_id, spec)?,
        None => specs.remove(bucket, upload_id)?,
    }
    Ok(())
}

#[async_trait::async_trait]
impl UploadMeta for RedbMetadata {
    async fn create(&self, bucket: &bucket::Name, upload_id: &str, row: &Row) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let upload_id = upload_id.to_string();
        let row = row.clone();
        self.driver
            .write(move |txn| put_upload_row(txn, &bucket, &upload_id, &row))
            .await
    }

    /// The guarded create: the bucket row is read in the transaction that
    /// writes the upload row, so a concurrent `remove_if_empty` cannot race
    /// it (D22's refusal projection, the `put_guarded` shape).
    async fn create_guarded(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &Row,
    ) -> Result<(), Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let upload_id = upload_id.to_string();
        let row = row.clone();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                if BucketTable::open(txn)?.row(&bucket)?.is_none() {
                    return Err(Error::Refused(refusal));
                }
                put_upload_row(txn, &bucket, &upload_id, &row)
            })
            .await
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<Row>, Error> {
        let bucket = bucket.to_string();
        let key = key.as_ref().to_owned();
        let upload_id = upload_id.to_string();
        self.driver.read(move |txn| {
            // One lookup: the row, present only when it records `key`.
            let Some((stored_key, initiated_at, tags, owner, grants)) =
                UploadTable::open_readonly(txn)?.get_matching(&bucket, &key, &upload_id)?
            else {
                return Ok(None);
            };
            let checksum = UploadChecksumTable::open_readonly(txn)?.get(&bucket, &upload_id)?;
            Ok(decode_row(
                &upload_id,
                &stored_key,
                initiated_at,
                &tags,
                &owner,
                &grants,
                checksum,
            ))
        })
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: multipart::PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let key = key.as_ref().to_owned();
        let upload_id = upload_id.to_string();
        let number = u32::from(number);
        let etag = etag.clone();
        // The tee slot's digest, owned: the write closure outlives this frame.
        let digest = checksum.and_then(|part| part.digest.get().cloned());
        self.driver
            .write(move |txn| {
                let uploads = UploadTable::open(txn)?;
                if !uploads.key_matches(&bucket, &key, &upload_id)? {
                    return Err(Error::Refused(storage::no_such_upload(&upload_id)));
                }
                drop(uploads);
                PartTable::open(txn)?.put(&bucket, &upload_id, number, &etag)?;
                // The checksum row commits atomically with the part row:
                // write the tee's digest, or clear a stale row from a
                // previous upload of this part number.
                PartChecksumTable::open(txn)?.set(&bucket, &upload_id, number, digest.as_ref())?;
                Ok(())
            })
            .await
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: multipart::PartNumber,
    ) -> Result<Vec<PartRow>, Error> {
        let bucket = bucket.to_string();
        let upload_id = upload_id.to_string();
        let start = u32::from(from);
        // An unbounded part walk with a checksum point read per part: the
        // decode and the allocation run on the blocking pool (the driver's
        // own rule for traversals).
        self.driver
            .read_blocking(move |txn| {
                // Every part at or after `from`, in part-number order. A
                // domain-invalid stored ETag reads as absent (the `PartRow`
                // self-heal); the checksum table self-heals its own wire.
                let (recorded, _) = PartTable::open_readonly(txn)?.list_from(
                    &bucket,
                    &upload_id,
                    start,
                    usize::MAX,
                )?;
                let checksums = PartChecksumTable::open_readonly(txn)?;
                join_parts(recorded, |number| {
                    checksums.get(&bucket, &upload_id, number)
                })
            })
            .await
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<PartsPage, Error> {
        let bucket = bucket.to_string();
        let upload_id = upload_id.to_string();
        self.driver.read(move |txn| {
            // `max == 0` asks for nothing and must not read as truncated:
            // the caller's resume marker would otherwise advance past a
            // page it never received.
            if max == 0 {
                return Ok(PartsPage {
                    rows: Vec::new(),
                    more: false,
                    raw_last: None,
                });
            }
            // One read transaction: the page's `PARTS` rows (`from`
            // inclusive, `max` rows, plus the one-lookahead flag) and the
            // `PART_CHECKSUMS` join — the same join `list_parts` performs,
            // capped at the page.
            let (recorded, more) =
                PartTable::open_readonly(txn)?.list_from(&bucket, &upload_id, from, max)?;
            // The raw read's last number, taken before the join drops a
            // domain-invalid row: the answer's marker for a truncated page
            // that serves nothing (the empty-raw read is the `max == 0` arm
            // above and answers `None` there).
            let raw_last = recorded.last().map(|(number, _)| *number);
            // Probed once per page: an upload with no checksum rows at all
            // (the checksum feature off ⇒ the table holds none for it)
            // skips the per-part point reads — one probe read instead of
            // one read per part (F03).
            let checksums = PartChecksumTable::open_readonly(txn)?;
            let checksums = PartChecksumTable::has_upload(&checksums, &bucket, &upload_id)?
                .then(|| PartChecksumTable::open_readonly(txn))
                .transpose()?;
            // A domain-invalid stored ETag reads as absent (the `PartRow`
            // self-heal); the checksum table self-heals its own wire.
            let rows = join_parts(recorded, |number| match checksums.as_ref() {
                Some(table) => table.get(&bucket, &upload_id, number),
                None => Ok(None),
            })?;
            Ok(PartsPage {
                rows,
                more,
                raw_last,
            })
        })
    }

    #[allow(clippy::type_complexity)]
    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[multipart::PartNumber],
    ) -> Result<Option<Vec<Option<PartRecord>>>, Error> {
        let bucket = bucket.to_string();
        let key = key.as_ref().to_owned();
        let upload_id = upload_id.to_string();
        let numbers: Vec<u32> = numbers.iter().map(|number| u32::from(*number)).collect();
        self.driver.read(move |txn| {
            if !UploadTable::open_readonly(txn)?.key_matches(&bucket, &key, &upload_id)? {
                return Ok(None);
            }
            let parts = PartTable::open_readonly(txn)?;
            let checksums = PartChecksumTable::open_readonly(txn)?;
            let mut records = Vec::with_capacity(numbers.len());
            for number in numbers {
                let row = match parts.get_hex(&bucket, &upload_id, number)? {
                    Some(hex) => {
                        let checksum = checksums.get(&bucket, &upload_id, number)?;
                        ETag::new(&hex).ok().map(|etag| PartRow {
                            number,
                            etag,
                            checksum,
                        })
                    }
                    None => None,
                };
                records.push(row.as_ref().map(PartRecord::from));
            }
            Ok(Some(records))
        })
    }

    async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, Error> {
        let name = bucket.clone();
        let bucket = bucket.to_string();
        // A whole-bucket materialize with a per-row decode and a sort: the
        // walk runs on the blocking pool (the driver's own rule for
        // traversals).
        let mut uploads = self
            .driver
            .read_blocking(move |txn| collect_uploads(txn, &bucket, &name))
            .await?;
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    #[allow(clippy::type_complexity)]
    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), Error> {
        if max_uploads == 0 {
            return Ok((Vec::new(), Vec::new(), false, None));
        }
        let name = bucket.clone();
        let bucket = bucket.to_string();
        // F18: one short read transaction on the blocking pool — materialize
        // the bucket's rows, release the transaction, then paginate. The
        // unordered engine is the shared one home for the marker, rollup,
        // truncation and resume rules.
        let mut uploads = self
            .driver
            .read_blocking(move |txn| collect_uploads(txn, &bucket, &name))
            .await?;
        uploads.retain(|upload| upload.key.starts_with(prefix));
        Ok(storage::group_and_paginate_unordered(
            uploads,
            prefix,
            delimiter,
            marker,
            max_uploads,
            |upload| upload.key.as_ref(),
            |upload| storage::uploads_order(&upload.key, &upload.upload_id),
        ))
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let bucket = bucket.to_string();
        self.driver
            .read(move |txn| UploadTable::open_readonly(txn)?.has_bucket(&bucket))
    }

    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, Error> {
        let mut uploads = self
            .driver
            .read_blocking(|txn| {
                let table = UploadTable::open_readonly(txn)?;
                let checksums = UploadChecksumTable::open_readonly(txn)?;
                let mut uploads = Vec::new();
                table.for_each(
                    |bucket, upload_id, key, initiated_at, tags, owner, grants| {
                        let Ok(name) = bucket::name(bucket) else {
                            return Ok(());
                        };
                        let checksum = checksums.get(bucket, upload_id)?;
                        if let Some(row) =
                            decode_row(upload_id, key, initiated_at, tags, owner, grants, checksum)
                        {
                            uploads.push(row.to_upload(&name));
                        }
                        Ok(())
                    },
                )?;
                Ok::<Vec<MultipartUpload>, Error>(uploads)
            })
            .await?;
        sort_uploads(&mut uploads);
        Ok(uploads)
    }

    async fn drain_upload(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let upload_id = upload_id.to_string();
        self.driver
            .write(move |txn| {
                UploadTable::open(txn)?.remove(&bucket, &upload_id)?;
                UploadChecksumTable::open(txn)?.remove(&bucket, &upload_id)?;
                PartTable::open(txn)?.drain_upload(&bucket, &upload_id)?;
                PartChecksumTable::open(txn)?.drain_upload(&bucket, &upload_id)?;
                Ok(())
            })
            .await
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let key = key.as_ref().to_owned();
        let upload_id = upload_id.to_string();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                // The identity check and the `UPLOADS` removal share ONE
                // handle: redb refuses a second `open_table` of the same
                // table in one transaction, and sharing the handle is what
                // makes the check and the drain atomic — a racing complete
                // cannot slip between them.
                let mut uploads = UploadTable::open(txn)?;
                if !uploads.key_matches(&bucket, &key, &upload_id)? {
                    return Err(Error::Refused(storage::no_such_upload(&upload_id)));
                }
                uploads.remove(&bucket, &upload_id)?;
                drop(uploads);
                UploadChecksumTable::open(txn)?.remove(&bucket, &upload_id)?;
                PartTable::open(txn)?.drain_upload(&bucket, &upload_id)?;
                PartChecksumTable::open(txn)?.drain_upload(&bucket, &upload_id)?;
                Ok(())
            })
            .await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| {
                UploadTable::open(txn)?.drain_bucket(&bucket)?;
                UploadChecksumTable::open(txn)?.drain_bucket(&bucket)?;
                PartTable::open(txn)?.drain_bucket(&bucket)?;
                PartChecksumTable::open(txn)?.drain_bucket(&bucket)?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use tinio_util::testing::metadata::assert_upload_meta_conformance;

    use crate::{
        _core::{
            ETag,
            acl::Acl,
            bucket,
            meta::{UploadMeta, upload::Row},
            object, storage,
        },
        Error, RedbMetadata,
        part::Table as PartTable,
        part_checksum::Table as PartChecksumTable,
        upload_checksum::Table as UploadChecksumTable,
    };

    /// The suite's upload leg against a fresh in-memory implementation —
    /// creation, the key check, numeric part order, the completion join, the
    /// listings and both drains. The leg lives here, beside the
    /// implementation it pins, because the shared `contract.rs` holds the
    /// row/state-protocol pins every backend shares — not a per-category
    /// conformance leg.
    #[tokio::test]
    async fn upload_meta_conforms() {
        let store = RedbMetadata::in_memory().unwrap();
        assert_upload_meta_conformance(&store).await;
    }

    /// The suite's refusal leg pins `put_guarded` and `remove_if_empty`, not
    /// `put_part`, so the part refusal is pinned here: an upload with no row
    /// and one stored under a different key both answer
    /// [`Error::Refused`] carrying `NoSuchUpload` — the projection the
    /// storage lift answers — and neither writes a part row (D22).
    #[tokio::test]
    async fn put_part_refuses_an_unknown_or_mismatched_upload() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket::name("data").unwrap();
        let key = object::key("big.bin").unwrap();
        let other = object::key("other.bin").unwrap();
        let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();

        // No upload row at all.
        let err = store
            .put_part(&bucket, &key, "conf-refuse-1", 1u32.into(), &etag, None)
            .await
            .expect_err("put_part must refuse an upload with no row");
        assert!(
            matches!(&err, Error::Refused(storage::Error::NoSuchUpload(id)) if id == "conf-refuse-1"),
            "the refusal must carry the upload id, got {err:?}"
        );
        let projected: storage::Error = err.into();
        assert!(
            matches!(projected, storage::Error::NoSuchUpload(_)),
            "the refusal must project to NoSuchUpload, got {projected:?}"
        );

        // A row that exists, but under a different key.
        store
            .create(
                &bucket,
                "conf-refuse-1",
                &Row {
                    upload_id: "conf-refuse-1".to_owned(),
                    key: key.clone(),
                    initiated_at: SystemTime::UNIX_EPOCH,
                    checksum: None,
                    tags: object::Tags::empty(),
                    owner: None,
                    acl: Acl::default_private(None),
                },
            )
            .await
            .unwrap();
        let err = store
            .put_part(&bucket, &other, "conf-refuse-1", 1u32.into(), &etag, None)
            .await
            .expect_err("put_part must refuse a key-mismatched upload");
        assert!(
            matches!(&err, Error::Refused(storage::Error::NoSuchUpload(_))),
            "a key mismatch is the same refusal, got {err:?}"
        );

        // Neither refusal wrote a part row.
        assert!(
            store
                .list_parts(&bucket, "conf-refuse-1", 1u32.into())
                .await
                .unwrap()
                .is_empty(),
            "a refused put_part writes no part row"
        );
    }

    /// The `list_parts_page` corrupt-ETag answer, planted where the raw table
    /// is reachable — the shared suite cannot express it, because `put_part`
    /// takes a validated `&ETag` (D43: a corrupt row belongs to the backend
    /// that owns the table, the way the corrupt object row belongs to
    /// `meta/object.rs`).
    ///
    /// The page's **truncation flag** is the raw read's one lookahead and its
    /// **raw last number** is the raw read's own last row, while the row join
    /// drops a domain-invalid stored ETag *after* both are taken. A truncated
    /// page whose raw rows are all invalid therefore answers an empty page,
    /// `more = true` and `raw_last = Some(2)`: the fs layer resumes from the
    /// served rows when the page serves any, and from the raw last number when
    /// it serves none, so that page still advances the client rather than
    /// re-requesting itself forever. The operator's 2026-10-07 ruling landed
    /// this (task-9 report, "Known behavioural delta"; task-9 review item 3):
    /// reaching the page takes out-of-band tampering, but the answer must not
    /// strand the caller. This test pins the answer so a reimplementation
    /// cannot change it in silence.
    #[tokio::test]
    async fn a_truncated_page_of_invalid_etags_keeps_the_raw_last_number() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket::name("data").unwrap();
        let key = object::key("big.bin").unwrap();
        let upload_id = "conf-etag-page-1";
        let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        store
            .create(
                &bucket,
                upload_id,
                &Row {
                    upload_id: upload_id.to_owned(),
                    key: key.clone(),
                    initiated_at: SystemTime::UNIX_EPOCH,
                    checksum: None,
                    tags: object::Tags::empty(),
                    owner: None,
                    acl: Acl::default_private(None),
                },
            )
            .await
            .unwrap();
        for number in [1u32, 2, 3] {
            store
                .put_part(&bucket, &key, upload_id, number.into(), &etag, None)
                .await
                .unwrap();
        }

        // The healthy page: the two rows, the lookahead's flag and the raw
        // last number the caller resumes from.
        let page = store
            .list_parts_page(&bucket, upload_id, 1, 2)
            .await
            .unwrap();
        assert_eq!(
            page.rows.iter().map(|p| p.number).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(page.more, "a third raw row is the truncation probe");
        assert_eq!(
            page.raw_last,
            Some(2),
            "the raw read's last number is the page's last raw row"
        );

        // Tamper the very rows the page reads — the page and its probe.
        for number in [1u32, 2, 3] {
            plant_etag(&store, "data", upload_id, number, "not-an-etag").await;
        }
        let page = store
            .list_parts_page(&bucket, upload_id, 1, 2)
            .await
            .unwrap();
        assert!(
            page.rows.is_empty(),
            "every raw row of the page self-heals away, so the page serves nothing"
        );
        assert!(
            page.more,
            "the truncation flag is the raw read's lookahead, which the self-heal cannot change"
        );
        assert_eq!(
            page.raw_last,
            Some(2),
            "the raw last number survives the self-heal — the fs marker for the truncated page that served nothing"
        );

        // The boundary the fallback turns on: one served row names the marker
        // the served way, and only the all-invalid page needs the raw last
        // number.
        plant_etag(
            &store,
            "data",
            upload_id,
            1,
            "d41d8cd98f00b204e9800998ecf8427e",
        )
        .await;
        let page = store
            .list_parts_page(&bucket, upload_id, 1, 2)
            .await
            .unwrap();
        assert_eq!(
            page.rows.iter().map(|p| p.number).collect::<Vec<_>>(),
            vec![1],
            "the valid row survives while its neighbour self-heals"
        );
        assert!(
            page.more,
            "and the flag still reports the raw row beyond the page"
        );
        assert_eq!(
            page.raw_last,
            Some(2),
            "the number is the raw read's, served row or not"
        );
    }

    /// Plant a raw ETag wire value for one part row — the corrupt-row seam of
    /// D43, bypassing the contract's validated `&ETag`.
    async fn plant_etag(
        store: &RedbMetadata,
        bucket: &str,
        upload_id: &str,
        number: u32,
        hex: &str,
    ) {
        let bucket = bucket.to_owned();
        let upload_id = upload_id.to_owned();
        let hex = hex.to_owned();
        store
            .driver()
            .write(move |txn| -> Result<(), Error> {
                PartTable::open(txn)?
                    .insert((bucket.as_str(), upload_id.as_str(), number), hex.as_str())?;
                Ok(())
            })
            .await
            .unwrap();
    }

    /// One upload row, identified by `upload_id` and recording `key`.
    fn upload_row(upload_id: &str, key: &object::Key) -> Row {
        Row {
            upload_id: upload_id.to_owned(),
            key: key.clone(),
            initiated_at: SystemTime::UNIX_EPOCH,
            checksum: None,
            tags: object::Tags::empty(),
            owner: None,
            acl: Acl::default_private(None),
        }
    }

    /// `drain_upload_checked` is the abort path's identity check and drain in
    /// **one write transaction**: it refuses (`NoSuchUpload`, D22) for an
    /// upload with no row and for one recorded under another key — draining
    /// nothing — and one success removes the `UPLOADS` row, its
    /// `UPLOAD_CHECKSUMS` row and both part tables together. A second call
    /// on the drained upload refuses, exactly as HEAD's key check did.
    #[tokio::test]
    async fn drain_upload_checked_refuses_before_it_drains_and_drains_whole() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket::name("data").unwrap();
        let key = object::key("big.bin").unwrap();
        let other = object::key("other.bin").unwrap();
        let upload_id = "conf-drain-1";
        let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
        let writes = || store.driver().write_lock_stats().count;

        // No row at all: the refusal names the upload, projects to
        // `NoSuchUpload`, and is still one write transaction.
        let before = writes();
        let err = store
            .drain_upload_checked(&bucket, &key, upload_id)
            .await
            .expect_err("an upload with no row must refuse");
        assert!(
            matches!(&err, Error::Refused(storage::Error::NoSuchUpload(id)) if id == upload_id),
            "the refusal must carry the upload id, got {err:?}"
        );
        assert_eq!(writes() - before, 1, "the refusal is one write transaction");
        let projected: storage::Error = err.into();
        assert!(
            matches!(projected, storage::Error::NoSuchUpload(_)),
            "the refusal must project to NoSuchUpload, got {projected:?}"
        );

        // A row recorded under another key: the same refusal and nothing is
        // drained (the stored parts survive).
        store
            .create(&bucket, upload_id, &upload_row(upload_id, &key))
            .await
            .unwrap();
        store
            .put_part(&bucket, &key, upload_id, 1u32.into(), &etag, None)
            .await
            .unwrap();
        let err = store
            .drain_upload_checked(&bucket, &other, upload_id)
            .await
            .expect_err("a key-mismatched upload must refuse");
        assert!(
            matches!(&err, Error::Refused(storage::Error::NoSuchUpload(_))),
            "a key mismatch is the same refusal, got {err:?}"
        );
        assert_eq!(
            store
                .list_parts(&bucket, upload_id, 1u32.into())
                .await
                .unwrap()
                .len(),
            1,
            "a refused check-and-drain removes no part"
        );

        // The matching key: one write transaction drains all four tables.
        let before = writes();
        store
            .drain_upload_checked(&bucket, &key, upload_id)
            .await
            .unwrap();
        assert_eq!(
            writes() - before,
            1,
            "the check-and-drain is one write transaction"
        );
        assert!(
            store.get(&bucket, &key, upload_id).await.unwrap().is_none(),
            "the upload row is gone"
        );
        assert!(
            store
                .list_parts(&bucket, upload_id, 1u32.into())
                .await
                .unwrap()
                .is_empty(),
            "the part rows are gone"
        );
        let name = bucket.to_string();
        let id = upload_id.to_owned();
        store
            .driver()
            .read(move |txn| -> Result<(), Error> {
                assert!(
                    UploadChecksumTable::open_readonly(txn)?
                        .get(&name, &id)?
                        .is_none(),
                    "the upload checksum row is drained"
                );
                assert!(
                    PartTable::open_readonly(txn)?
                        .get_hex(&name, &id, 1)?
                        .is_none(),
                    "the part row is drained"
                );
                assert!(
                    !PartChecksumTable::open_readonly(txn)?.has_upload(&name, &id)?,
                    "the part checksum rows are drained"
                );
                Ok(())
            })
            .unwrap();

        // Not idempotent by design: the drained upload no longer matches, so
        // a second abort refuses like HEAD's key check (D22).
        let err = store
            .drain_upload_checked(&bucket, &key, upload_id)
            .await
            .expect_err("a drained upload must refuse");
        assert!(
            matches!(&err, Error::Refused(storage::Error::NoSuchUpload(_))),
            "a second drain is the same refusal, got {err:?}"
        );
    }

    /// The unconditional `drain_upload` stays idempotent: draining an absent
    /// upload writes nothing and answers `Ok` (the shared suite's rule, pinned
    /// here beside the checked form it differs from).
    #[tokio::test]
    async fn drain_upload_stays_idempotent_for_an_absent_upload() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket::name("data").unwrap();
        store.drain_upload(&bucket, "conf-absent").await.unwrap();
        store.drain_upload(&bucket, "conf-absent").await.unwrap();
    }
}
