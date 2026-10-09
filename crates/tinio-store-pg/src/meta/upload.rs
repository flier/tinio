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

use std::collections::HashMap;

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
            upload::{PartRecord, PartRow, PartsPage, Row as UploadRow},
        },
        multipart::PartNumber,
        object,
        storage::{group_and_paginate_unordered, uploads_order},
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
struct Stored {
    upload_id: String,
    key: String,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<checksum::Algorithm>,
    kind: Option<String>,
}

/// The same seven columns preceded by the owning bucket, which is how the
/// cross-bucket walk reads them.
struct StoredWalked {
    bucket: String,
    upload_id: String,
    key: String,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<checksum::Algorithm>,
    kind: Option<String>,
}

/// The upload's stored key alone, which is what the range read checks its
/// caller's key against (the two locked writers check inside their statement,
/// through `require_upload`).
struct StoredKey {
    key: object::Key,
}

/// One part projection row: the part's number, its stored ETag, then the
/// optional checksum row's algorithm and value.
struct StoredPart {
    number: i64,
    etag: String,
    algorithm: Option<checksum::Algorithm>,
    value: Option<String>,
}

/// Decode one upload projection row from the stored columns: the tags and ACL
/// arrive as the contract types (the ACL carrying the owner), and the checksum
/// algorithm is already the contract type; only its kind remains a wire string,
/// because the empty sentinel "no derivation fixed" has no enum variant to hold
/// it.
fn upload_row(
    upload_id: &str,
    key: &str,
    initiated_at: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    algorithm: Option<checksum::Algorithm>,
    kind: Option<&str>,
) -> Option<UploadRow> {
    let mut decoded =
        UploadRow::from_values(upload_id, key, from_timestamp(initiated_at), tags, acl)?;
    decoded.checksum = algorithm.zip(kind).and_then(|(algorithm, kind)| {
        checksum::stored_type(kind)
            .ok()
            .map(|r#type| checksum::Upload { algorithm, r#type })
    });
    Some(decoded)
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
            self.kind.as_deref(),
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
            self.kind.as_deref(),
        )
        .map(|row| row.to_upload(&bucket))
    }
}

impl StoredPart {
    /// The stored checksum, or `None` when the checksum row is absent or a
    /// domain-invalid ETag hides it: the algorithm is already the contract
    /// type, so only the value's base64 wire is wrapped — moved out of the row
    /// this call consumes.
    fn checksum(self) -> Option<checksum::Part> {
        Some(checksum::Part {
            algorithm: self.algorithm?,
            value: checksum::Value(self.value?),
        })
    }

    /// The listing row; `None` is a domain-invalid ETag.
    fn part(self) -> Option<PartRow> {
        let number = self.number as u32;
        let etag = ETag::new(&self.etag).ok()?;
        Some(PartRow {
            number,
            etag,
            checksum: self.checksum(),
        })
    }

    /// The record row the range read answers with: the same three stored
    /// columns in the completion-time shape, built from the row this call
    /// consumes rather than through a listing row the answer would only clone.
    fn record(self) -> Option<PartRecord> {
        let number = self.number as u32;
        let etag = ETag::new(&self.etag).ok()?;
        Some(PartRecord {
            number,
            etag,
            checksum: self.checksum(),
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
        // One SELECT narrows the scan to the prefix bound; the rows feed the
        // shared unordered pager, which owns the marker, rollup, truncation and
        // resume rules. No ORDER BY here: the engine is order-independent.
        let rows = query_file_as!(
            Stored,
            "sql/meta/upload/list_uploads_page.sql",
            bucket,
            prefix
        )
        .fetch_all(self.pool())
        .await?;
        let uploads: Vec<MultipartUpload> = rows
            .into_iter()
            .filter_map(|row| row.into_upload(bucket))
            .collect();
        Ok(group_and_paginate_unordered(
            uploads,
            prefix,
            delimiter,
            marker,
            max_uploads,
            |upload| upload.key.as_ref(),
            |upload| uploads_order(&upload.key, &upload.upload_id),
        ))
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
        sqlx::query_file!("sql/meta/upload/drain_upload.sql", bucket, upload_id)
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
        sqlx::query_file!("sql/meta/upload/drain_upload.sql", bucket, upload_id)
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
