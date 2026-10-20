//! UploadMeta over the uploads, parts, upload_checksums and part_checksums
//! relations.
//!
//! The upload's state is one row keyed by (bucket, upload_id); its checksum
//! spec is the sibling upload_checksums row; its parts are the parts row (the
//! ETag) beside the part_checksums row. Every mutating method runs its
//! statements in one transaction, so a part's ETag and its checksum cannot
//! disagree, and every point read is one statement.
//!
//! The identifier check is the contract's: an upload is answered only when it
//! exists and records the caller's key. The abort path locks that upload row,
//! then removes its four tables in one statement, so a racing part write waits
//! on the same lock and cannot commit a part the drain will miss.

use std::{collections::HashMap, time::SystemTime};

use async_trait::async_trait;
use sqlx::{query, query_as, query_file_as, query_file_scalar, query_scalar};
use time::OffsetDateTime;

use crate::{
    _core::{
        ETag, MultipartUpload,
        acl::Acl,
        bucket, checksum,
        meta::{
            UploadMeta,
            upload::{PartRecord, PartRow, PartsPage, Row as UploadRow, UploadsChunk},
        },
        multipart::PartNumber,
        object,
        storage::{UnorderedPager, from_nanos, split_uploads_order, uploads_order},
    },
    Error, PgTx, begin,
    meta::{acl_composite, assert_bucket_row, project},
    store::PgStore,
    timestamp::{from_timestamp, to_timestamp},
};

/// Upsert the upload state and its checksum row inside the caller's
/// transaction — the shared body of create and create_guarded.
async fn upsert_upload(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    upload_id: &str,
    row: &UploadRow,
) -> Result<(), Error> {
    let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
    sqlx::query_file!(
        "sql/meta/upload/upsert_upload.sql",
        bucket,
        upload_id,
        row.key.as_ref(),
        to_timestamp(row.initiated_at),
        (&row.tags) as _,
        acl as _
    )
    .execute(&mut **tx)
    .await?;
    query!(
        "DELETE FROM upload_checksums WHERE bucket = $1 AND upload_id = $2",
        bucket,
        upload_id
    )
    .execute(&mut **tx)
    .await?;
    if let Some(spec) = &row.checksum {
        // The algorithm binds as the contract type; only the kind stays a wire
        // string, because the empty sentinel "no derivation fixed" has no enum
        // variant to hold it.
        let kind = spec.r#type.map_or_else(String::new, |ty| ty.to_string());
        sqlx::query_file!(
            "sql/meta/upload/upsert_upload_checksum.sql",
            bucket,
            upload_id,
            spec.algorithm as _,
            kind
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// Assert inside tx that the upload exists **and records `key`**, refusing with
/// [`Error::NoSuchUpload`] otherwise.
///
/// The read takes the upload row's lock (`FOR UPDATE`), so the check and
/// whatever the caller writes beside it are one serialized unit against a
/// drain: a drain takes the same lock before it deletes, so it can never remove
/// the upload between the check and the write. One statement text and one
/// comparison serve the two locked callers — [`UploadMeta::put_part`], which
/// writes a part beside the checked row, and
/// [`UploadMeta::drain_upload_checked`], which removes it — while
/// [`UploadMeta::part_records`] keeps its own unlocked read, because it answers
/// `Option` rather than an error.
async fn require_upload(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    upload_id: &str,
    key: &object::Key,
) -> Result<(), Error> {
    let locked = query_file_scalar!(
        "sql/meta/upload/lock_upload_row.sql",
        bucket,
        upload_id,
        key
    )
    .fetch_optional(&mut **tx)
    .await?;
    if locked.is_none() {
        return Err(Error::NoSuchUpload(upload_id.to_owned()));
    }
    Ok(())
}

/// The seven columns every upload projection selects, in the order
/// `sql/meta/upload/*.sql` spells them: the upload's identifier, key and
/// instant, then its tags and ACL composite (which carries the owner), then
/// the optional checksum row's algorithm and kind.
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode below is a typed conversion, not a
/// positional read.
///
/// The checksum algorithm is read as its stored **text**, not as
/// [`checksum::Algorithm`]: the contract's mapping for that column is strict,
/// and the contract's rule for the column is self-healing instead -- a stored
/// label outside the ten API names must not fail the whole read (the upload is
/// then served with no spec, exactly as the reference store's
/// `checksum::Upload::from_wire_opt` answers it).
struct Stored {
    upload_id: String,
    key: String,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<String>,
    kind: Option<String>,
}

/// The same eight columns preceded by the owning bucket, which is how the
/// cross-bucket walk reads them.
struct StoredWalked {
    bucket: String,
    upload_id: String,
    key: String,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<String>,
    kind: Option<String>,
}

/// The upload's stored key alone, which is what the range read checks its
/// caller's key against (the two locked writers check inside their statement,
/// through `require_upload`).
struct StoredKey {
    key: object::Key,
}

/// One part projection row: the part's number, its stored ETag, then the
/// optional checksum row's algorithm and value -- the algorithm as its stored
/// text for the same self-healing reason [`Stored`]'s is -- then the part's own
/// write stamp.
///
/// The field order is the column order here too, so `modified_at` is spelled
/// **last** in both: the two statements that build this struct
/// (`list_parts_page.sql`, `part_records_rows.sql`) select the stamp as their
/// final column.
struct StoredPart {
    number: i64,
    etag: String,
    algorithm: Option<String>,
    value: Option<String>,
    modified_at: i64,
}

/// The domain instant a stored unix-**millisecond** part stamp names, or `None`
/// for the stamps that mean "this store records none".
///
/// `parts.modified_at` is a `bigint` because the contract's part row carries a
/// raw stamp (the migration says why), and every write path names it from
/// PostgreSQL's own clock; `0` is what the column's `DEFAULT` leaves on a row
/// an older writer inserted, and a value the domain wire cannot carry (a
/// negative pre-epoch clock) reads the same way. Both answer `None`, so the
/// projection falls back to the upload's creation time -- the pre-ruling
/// answer -- rather than inventing an instant from a sentinel.
fn part_stamp(millis: i64) -> Option<SystemTime> {
    u64::try_from(millis)
        .ok()
        .filter(|millis| *millis != 0)
        .map(|millis| from_nanos(millis.saturating_mul(1_000_000)))
}

/// Decode one upload projection row from the stored columns: the tags and ACL
/// arrive as the contract types (the ACL carrying the owner), and the checksum
/// spec is re-derived by the contract's own healing read -- a stored algorithm
/// or kind outside the domain leaves the upload without a spec rather than
/// failing the read.
fn upload_row(
    upload_id: &str,
    key: &str,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<String>,
    kind: Option<String>,
) -> Option<UploadRow> {
    let mut decoded =
        UploadRow::from_values(upload_id, key, from_timestamp(initiated_at), tags, acl)?;
    decoded.checksum = algorithm
        .as_deref()
        .zip(kind.as_deref())
        .and_then(|(algorithm, kind)| checksum::Upload::from_wire_opt(algorithm, kind));
    Some(decoded)
}

/// The next Unicode scalar value above `c`, `None` when `c` is the last one
/// (`U+10FFFF`). The surrogate range answers `None` too: no key can hold one,
/// so the strip loop drops the character instead of bounding with a value no
/// `text` can carry.
fn next_scalar(c: char) -> Option<char> {
    char::from_u32(u32::from(c) + 1)
}

/// The exclusive upper bound of the byte range covering every key that starts
/// with `prefix`: the smallest string greater than all of them, or `None` when
/// no such string exists.
///
/// UTF-8 byte order follows code point order, so incrementing the prefix's last
/// character makes `[prefix, upper)` exactly `prefix`'s prefix set — which is
/// why the predicate is a range the plan keeps as an `Index Cond` rather than a
/// `starts_with` call it can only filter with. A trailing `U+10FFFF` (or a lone
/// surrogate) has no successor: it is dropped and the character before it
/// incremented in its place, which makes the range a superset of the prefix set
/// that the statement's `starts_with` then narrows. A prefix whose whole body
/// is such characters — the empty prefix included — has no upper bound at all,
/// because the largest code point can itself be extended; the caller reads
/// without one then (a specific statement), never with an invented bound that
/// would drop a key carrying `U+10FFFF` right after the prefix. The content
/// plane's own `prefix_upper` (`content.rs`) is the same rule over a key space
/// whose floor always has a successor, so it never answers `None`.
///
/// The crate's meta plane has one home for the rule: this one, shared by the
/// uploads listing and the version listing, so the two ranges cannot drift.
pub(crate) fn prefix_upper(prefix: &str) -> Option<String> {
    let mut base = prefix;
    loop {
        let last = base.chars().next_back()?;
        let Some(next) = next_scalar(last) else {
            base = &base[..base.len() - last.len_utf8()];
            continue;
        };
        let mut upper = String::with_capacity(base.len() + 4);
        upper.push_str(&base[..base.len() - last.len_utf8()]);
        upper.push(next);
        return Some(upper);
    }
}

/// The bounded listing read's cursor: the statement's scan lower bound (`$2`)
/// beside the row the read resumes strictly after (`$5`, `$6`), both in the
/// listing's own byte order, with `''` as the "no cursor" sentinel — every
/// stored key is longer than the empty string, so the bound is a no-op then.
///
/// The two bounds carry different jobs. The tuple is the exact resume rule,
/// and `$2` is what makes it a seek: the scan starts at the cursor row rather
/// than walking the prefix to find it. A cursor whose lower bound is the empty
/// string leaves the seek to the prefix's own range.
///
/// Both are derived from a listing marker, because the composite order of the
/// last row a chunk served **is** the marker the next chunk resumes from:
/// [`Self::marker`] is the one conversion for both.
#[derive(Debug, Clone)]
struct UploadsCursor {
    /// `$2`, the scan's inclusive lower bound.
    lower: String,
    /// `$5`, the resumed-from key (`''` = none).
    after_key: String,
    /// `$6`, the resumed-from upload id (`''` = none).
    after_uid: String,
}

impl UploadsCursor {
    /// The cursor a listing marker opens or continues with.
    ///
    /// A marker naming a row (`key\0upload_id`, including the `U+10FFFF`
    /// sentinel a bare `key-marker` becomes — no upload id carries it) resumes
    /// strictly after that row: the tuple bound sorts every one of its own
    /// key's rows below the marker the way the pager does. A marker that is a
    /// rolled-up prefix names no row at all, and the composite orders above it
    /// are exactly the keys at or above it, so only the scan's lower bound
    /// narrows and the tuple bound stays the sentinel.
    fn marker(prefix: &str, marker: Option<&str>) -> Self {
        let (lower, after_key, after_uid) = match marker {
            Some(order) => match split_uploads_order(order) {
                (key, Some(upload_id)) => (key, key, upload_id),
                (key, None) => (key, key, ""),
            },
            None => (prefix, "", ""),
        };
        // The scan never starts below the prefix: a listing addresses the
        // prefix's own range, and a marker sorting below it (a stale resumption
        // of an earlier request) would otherwise seek behind it.
        let from = if lower < prefix { prefix } else { lower };
        Self {
            lower: from.to_owned(),
            after_key: after_key.to_owned(),
            after_uid: after_uid.to_owned(),
        }
    }
}

impl Stored {
    /// The contract row; `None` is a domain-invalid key or wire. The row is
    /// consumed: its tag set and ACL move into the contract row rather than
    /// being copied.
    fn decode(self) -> Option<UploadRow> {
        upload_row(
            &self.upload_id,
            &self.key,
            self.initiated_at,
            self.tags,
            self.acl,
            self.algorithm,
            self.kind,
        )
    }

    /// The contract's S3-facing shape; a row that does not decode is skipped.
    /// The name says the row is consumed (`to_upload` would promise otherwise).
    fn into_upload(self, bucket: &bucket::Name) -> Option<MultipartUpload> {
        self.decode().map(|row| row.to_upload(bucket))
    }
}

impl StoredWalked {
    /// The contract's S3-facing shape; a row whose bucket, key or wires do not
    /// decode is skipped. The row is consumed: its tag set and ACL move into
    /// the contract row rather than being copied.
    fn into_upload(self) -> Option<MultipartUpload> {
        let bucket = bucket::name(self.bucket).ok()?;
        upload_row(
            &self.upload_id,
            &self.key,
            self.initiated_at,
            self.tags,
            self.acl,
            self.algorithm,
            self.kind,
        )
        .map(|row| row.to_upload(&bucket))
    }
}

impl StoredPart {
    /// The stored checksum, or `None` when the checksum row is absent or a
    /// domain-invalid element hides it -- the contract's own healing part
    /// decode, so a stored label outside the ten API names serves the part
    /// without a checksum instead of failing the listing.
    fn checksum(&self) -> Option<checksum::Part> {
        checksum::Part::from_wire_opt(self.algorithm.as_deref()?, self.value.as_deref()?)
    }

    /// The part number the contract's `u32` can name, or `None` for a stored
    /// `bigint` outside it: a number the API has no part for reads as absent,
    /// exactly as a domain-invalid ETag does, rather than being served as the
    /// part its `as` truncation would invent (2^32 would answer part 0).
    fn number(&self) -> Option<u32> {
        u32::try_from(self.number).ok()
    }

    /// The listing row; `None` is a domain-invalid ETag or part number.
    fn part(self) -> Option<PartRow> {
        let number = self.number()?;
        let etag = self.etag.parse::<ETag>().ok()?;
        Some(PartRow {
            number,
            etag,
            checksum: self.checksum(),
            written_at: part_stamp(self.modified_at),
        })
    }

    /// The record row the range read answers with: the same stored columns in
    /// the completion-time shape, built from the row this call consumes rather
    /// than through a listing row the answer would only clone.
    fn record(self) -> Option<PartRecord> {
        let number = self.number()?;
        let etag = self.etag.parse::<ETag>().ok()?;
        Some(PartRecord {
            number,
            etag,
            checksum: self.checksum(),
            written_at: part_stamp(self.modified_at),
        })
    }
}

impl PgStore {
    /// One bounded chunk of the bucket's uploads: at most `max` rows strictly
    /// after `cursor`, in the listing's own order, with [`UploadsChunk::more`]
    /// telling whether a row follows them.
    ///
    /// The statement reads one row past the page (the `LIMIT` is `max + 1`),
    /// which is what makes `more` a fact rather than a guess; the prefix
    /// chooses between the two statements — the bounded one and the open one a
    /// prefix with no successor reads through — because only one of them has
    /// an `Index Cond`-able upper bound to spell.
    async fn uploads_chunk(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        cursor: &UploadsCursor,
        max: usize,
    ) -> Result<UploadsChunk, Error> {
        // A chunk the caller requested nothing of reads nothing, and answers
        // no truncation either: the resume arithmetic must never advance past
        // rows it never received.
        if max == 0 {
            return Ok(UploadsChunk {
                uploads: Vec::new(),
                more: false,
            });
        }
        let limit = i64::try_from(max.saturating_add(1)).unwrap_or(i64::MAX);
        let mut rows = match prefix_upper(prefix) {
            Some(upper) => {
                query_file_as!(
                    Stored,
                    "sql/meta/upload/list_uploads_chunk.sql",
                    bucket,
                    &cursor.lower,
                    &upper,
                    prefix,
                    &cursor.after_key,
                    &cursor.after_uid,
                    limit
                )
                .fetch_all(self.pool())
                .await?
            }
            None => {
                query_file_as!(
                    Stored,
                    "sql/meta/upload/list_uploads_chunk_open.sql",
                    bucket,
                    &cursor.lower,
                    prefix,
                    &cursor.after_key,
                    &cursor.after_uid,
                    limit
                )
                .fetch_all(self.pool())
                .await?
            }
        };
        let more = rows.len() > max;
        rows.truncate(max);
        Ok(UploadsChunk {
            uploads: rows
                .into_iter()
                .filter_map(|row| row.into_upload(bucket))
                .collect(),
            more,
        })
    }
}

#[async_trait]
impl UploadMeta for PgStore {
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &UploadRow,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        upsert_upload(&mut tx, bucket, upload_id, row).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create_guarded(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &UploadRow,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        upsert_upload(&mut tx, bucket, upload_id, row).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<UploadRow>, Error> {
        let row = query_file_as!(Stored, "sql/meta/upload/get.sql", bucket, upload_id)
            .fetch_optional(self.pool())
            .await?;
        Ok(row
            .and_then(Stored::decode)
            .filter(|decoded| decoded.key == *key))
    }

    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        // The locked check is `require_upload`'s; the row lock makes the key
        // check and the part write one serialized unit against an abort, since
        // a drain blocks on the same lock until this transaction ends.
        if let Err(err) = require_upload(&mut tx, bucket, upload_id, key).await {
            tx.rollback().await?;
            return Err(err);
        }
        sqlx::query_file!(
            "sql/meta/upload/put_part_upsert.sql",
            bucket,
            upload_id,
            u32::from(number) as i64,
            etag.as_str()
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM part_checksums WHERE bucket = $1 AND upload_id = $2 AND number = $3",
            bucket,
            upload_id,
            u32::from(number) as i64
        )
        .execute(&mut *tx)
        .await?;
        if let Some(part) = checksum.and_then(|tee| tee.digest.get()) {
            sqlx::query_file!(
                "sql/meta/upload/put_part_write_checksum.sql",
                bucket,
                upload_id,
                u32::from(number) as i64,
                part.algorithm as _,
                part.value.as_str()
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: PartNumber,
    ) -> Result<Vec<PartRow>, Error> {
        // The page statement with the limit bound away: PostgreSQL reads a NULL
        // LIMIT as LIMIT ALL, so this answer is the unbounded read it was, and
        // one statement text serves both readers.
        let rows = query_file_as!(
            StoredPart,
            "sql/meta/upload/list_parts_page.sql",
            bucket,
            upload_id,
            u32::from(from) as i64,
            None::<i64>
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows.into_iter().filter_map(StoredPart::part).collect())
    }

    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<PartsPage, Error> {
        // max == 0 must never read as truncated: the caller's resume
        // arithmetic would advance past parts it never received.
        if max == 0 {
            return Ok(PartsPage {
                rows: Vec::new(),
                more: false,
                raw_last: None,
            });
        }
        // O(page): the LIMIT is the page plus its truncation probe, so the
        // read never scans the rest of the upload.
        let mut rows = query_file_as!(
            StoredPart,
            "sql/meta/upload/list_parts_page.sql",
            bucket,
            upload_id,
            from as i64,
            max as i64 + 1
        )
        .fetch_all(self.pool())
        .await?;
        let more = rows.len() > max;
        let kept = rows.len().min(max);
        // The raw read's last number, before the join dropped any
        // domain-invalid row: the caller's resume marker must be able to
        // advance past a raw row the join did not serve.
        let raw_last = if kept == 0 {
            None
        } else {
            Some(rows[kept - 1].number as u32)
        };
        rows.truncate(kept);
        let decoded = rows.into_iter().filter_map(StoredPart::part).collect();
        Ok(PartsPage {
            rows: decoded,
            more,
            raw_last,
        })
    }

    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[PartNumber],
    ) -> Result<Option<Vec<Option<PartRecord>>>, Error> {
        let stored = query_as!(
            StoredKey,
            r#"SELECT key AS "key: object::Key" FROM uploads WHERE bucket = $1 AND upload_id = $2"#,
            bucket,
            upload_id
        )
        .fetch_optional(self.pool())
        .await?;
        let Some(stored) = stored else {
            return Ok(None);
        };
        if stored.key != *key {
            return Ok(None);
        }
        if numbers.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let wanted: Vec<i64> = numbers
            .iter()
            .map(|number| u32::from(*number) as i64)
            .collect();
        let rows = query_file_as!(
            StoredPart,
            "sql/meta/upload/part_records_rows.sql",
            bucket,
            upload_id,
            &wanted
        )
        .fetch_all(self.pool())
        .await?;
        let mut found: HashMap<i64, PartRecord> = HashMap::new();
        for row in rows {
            let number = row.number;
            // A part whose stored ETag does not decode is no record: it is left
            // out of the map, so it reads exactly as an unstored part does.
            if let Some(record) = row.record() {
                found.insert(number, record);
            }
        }
        Ok(Some(project(wanted.iter(), &mut found)))
    }

    async fn list_uploads(&self, bucket: &bucket::Name) -> Result<Vec<MultipartUpload>, Error> {
        let rows = query_file_as!(Stored, "sql/meta/upload/list_uploads.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| row.into_upload(bucket))
            .collect())
    }

    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), Error> {
        // Nothing was requested and nothing emitted — no page and no resume
        // marker either, or the client's next request would skip the first
        // upload of the next page forever (the shared engine's own rule).
        if max_uploads == 0 {
            return Ok((Vec::new(), Vec::new(), false, None));
        }
        // The shared pager is fed a chunk at a time, never the whole prefix:
        // each pull is the `max_uploads + 1` rows after a cursor, and the pull
        // repeats until the pager holds one entry past the page — the
        // delimiter roll-up's one-group lookahead, which a group's later
        // members may only complete in a later chunk. The chunk boundary
        // therefore decides nothing; the page is the pager's answer either way.
        let mut pager = UnorderedPager::new(
            prefix,
            delimiter,
            marker,
            max_uploads,
            |upload: &MultipartUpload| upload.key.as_ref(),
        );
        let mut cursor = UploadsCursor::marker(prefix, marker);
        loop {
            let chunk = self
                .uploads_chunk(bucket, prefix, &cursor, max_uploads)
                .await?;
            let Some(last) = chunk.uploads.last() else {
                break; // the prefix's rows are exhausted
            };
            // The last row's composite order is the next pull's marker — the
            // same conversion the listing's own marker went through, so a
            // chunk resumes exactly where the pager's order says it should.
            cursor =
                UploadsCursor::marker(prefix, Some(&uploads_order(&last.key, &last.upload_id)));
            for upload in chunk.uploads {
                pager.offer(upload, |upload| {
                    uploads_order(&upload.key, &upload.upload_id)
                });
            }
            if pager.saturated() || !chunk.more {
                break;
            }
        }
        Ok(pager.finish())
    }

    async fn list_uploads_chunk(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&str>,
        max: usize,
    ) -> Result<UploadsChunk, Error> {
        self.uploads_chunk(bucket, prefix, &UploadsCursor::marker(prefix, after), max)
            .await
    }

    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let present = query_scalar!(
            r#"SELECT exists(SELECT 1 FROM uploads WHERE bucket = $1) AS "present!""#,
            bucket
        )
        .fetch_one(self.pool())
        .await?;
        Ok(present)
    }

    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, Error> {
        let rows = query_file_as!(StoredWalked, "sql/meta/upload/walk_uploads.sql")
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(StoredWalked::into_upload)
            .collect())
    }

    async fn drain_upload(&self, bucket: &bucket::Name, upload_id: &str) -> Result<(), Error> {
        sqlx::query_file!(
            "sql/meta/upload/drain_upload.sql",
            bucket,
            &[upload_id.to_owned()]
        )
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), Error> {
        // Lock the upload row, then delete. A second abort waits on the lock
        // and finds the row gone. put_part takes the same lock before it
        // inserts, so a part committed while this drain waited is visible to
        // the delete.
        let mut tx = begin(self.pool()).await?;
        require_upload(&mut tx, bucket, upload_id, key).await?;
        sqlx::query_file!(
            "sql/meta/upload/drain_upload.sql",
            bucket,
            &[upload_id.to_owned()]
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        sqlx::query_file!("sql/meta/upload/drain_bucket.sql", bucket)
            .execute(self.pool())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "upload_tests.rs"]
mod tests;
