//! The decoded multipart-upload rows: the upload state, an upload's part rows,
//! the completion-time part record, and the completed object's retained
//! `OBJECT_PARTS` row.
//!
//! The upload's stored value is the five-tuple `(key, initiated-at unix nanos,
//! tags wire, owner wire, acl grants wire)`, its checksum spec is the separate
//! `UPLOAD_CHECKSUMS` row, its parts are the `PARTS` row (the ETag) beside the
//! `PART_CHECKSUMS` row (the checksum), and the completed object's retained
//! parts are the `OBJECT_PARTS` rows. Every decode self-heals an element
//! exactly as the landed table layer does.
//!
//! A part row carries the store's own write stamp for that part: the stamp's
//! meaning, and the one datum a stamp-less row decodes to, are the field's
//! own contract ([`PartRow::written_at`]).

use std::time::SystemTime;

use async_trait::async_trait;

use super::{Metadata, bucket as bucket_row};
use crate::{
    ETag, MultipartUpload,
    acl::{Acl, OwnerId},
    bucket, checksum, multipart, object,
    storage::{self, from_nanos, to_nanos},
};

/// A decoded in-progress upload row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// The upload identifier (its value-row key's second element).
    pub upload_id: String,
    /// The upload's target key.
    pub key: object::Key,
    /// The upload's initiation time.
    pub initiated_at: SystemTime,
    /// The create-time checksum spec (`None` = no checksum upload).
    pub checksum: Option<checksum::Upload>,
    /// The create-time tag set (empty when none), applied to the object at
    /// completion.
    pub tags: object::Tags,
    /// The create-time owner element (`None` = the empty owner wire).
    pub owner: Option<OwnerId>,
    /// The create-time grant set, applied to the object at completion.
    pub acl: Acl,
}

impl Row {
    /// The upload state as the S3 surface answers it.
    pub fn to_upload(&self, bucket: &bucket::Name) -> MultipartUpload {
        MultipartUpload {
            upload_id: self.upload_id.clone(),
            bucket: bucket.clone(),
            key: self.key.clone(),
            initiated_at: self.initiated_at,
            checksum: self.checksum.clone(),
            tags: self.tags.clone(),
            owner: self.owner.clone(),
            acl: self.acl.clone(),
        }
    }
}

/// The five value elements of one `UPLOADS` row, owned. The upload's
/// identifier is the row's key, so it is not an element.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wires {
    /// The target key.
    pub key: object::Key,
    /// The initiation time in unix nanoseconds.
    pub initiated_at: u64,
    /// The tag-set wire.
    pub tags: String,
    /// The owner wire.
    pub owner: String,
    /// The ACL grants wire.
    pub grants: String,
}

impl Row {
    /// Decode one stored value tuple. Every element self-heals: a
    /// domain-invalid key cannot be a key at all, so `key` is already
    /// validated by the caller and the metadata wires heal here.
    pub fn from_raw(
        upload_id: &str,
        key: &str,
        initiated_at: u64,
        tags: &str,
        owner: &str,
        grants: &str,
    ) -> Option<Self> {
        Some(Self {
            upload_id: upload_id.to_owned(),
            key: object::key(key).ok()?,
            initiated_at: from_nanos(initiated_at),
            checksum: None,
            tags: object::Tags::from_wire_limited(tags, object::OBJECT_TAGS_MAX),
            owner: bucket_row::decode_owner(owner),
            acl: bucket_row::decode_acl(grants),
        })
    }

    /// Decode a row from the contract's own values: the tags and the ACL
    /// arrive whole (the ACL carrying the owner), and the row splits the ACL
    /// ([`Acl::into_parts`]) exactly as its wire decode does.
    pub fn from_values(
        upload_id: &str,
        key: &str,
        initiated_at: SystemTime,
        tags: object::Tags,
        acl: Acl,
    ) -> Option<Self> {
        let (owner, grants) = acl.into_parts();
        Some(Self {
            upload_id: upload_id.to_owned(),
            key: object::key(key).ok()?,
            initiated_at,
            checksum: None,
            tags,
            owner,
            acl: Acl::from_parts(None, grants),
        })
    }

    /// Encode the row back into its value tuple's elements.
    pub fn to_raw(&self) -> Wires {
        Wires {
            key: self.key.clone(),
            initiated_at: to_nanos(self.initiated_at),
            tags: self.tags.to_wire(),
            owner: bucket_row::encode_owner(self.owner.as_ref()),
            grants: bucket_row::encode_grants(&self.acl),
        }
    }
}

/// One uploaded part as the contract answers it: its number, its ETag, its
/// stored checksum and its own write stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartRow {
    /// Part number.
    pub number: u32,
    /// The part's ETag.
    pub etag: ETag,
    /// The stored per-part checksum (`None` = none, or a domain-invalid stored
    /// wire — self-healing).
    pub checksum: Option<checksum::Part>,
    /// The instant the **store** last wrote this part — its own write stamp,
    /// which is what the listed part's `LastModified` answers
    /// ([`PartRow::to_info`]). `None` = the row predates the stamp: the decode
    /// allowance for a part stored before this datum existed (and for a
    /// database that predates it), never a licence for a live backend to omit
    /// it — the shared conformance suite pins that a written part carries one.
    ///
    /// The stamp is the store's, not the caller's: a backend reads it from its
    /// own plane (the part file's modification time, the row's write instant).
    /// The projection's `fallback` — the upload's creation instant — is what a
    /// stamp-less old row answers, so a database written before the stamp keeps
    /// reading with a coherent `LastModified` rather than failing to decode.
    pub written_at: Option<SystemTime>,
}

impl PartRow {
    /// Decode the `PARTS` element and the `PART_CHECKSUMS` elements. `None` is
    /// a domain-invalid ETag — the part reads as absent (self-healing, the
    /// point read's rule); a domain-invalid checksum self-heals to `None` and
    /// the part is still listed.
    ///
    /// The element pair carries the part's identity only, so the decoded row
    /// holds no write stamp ([`Self::written_at`] is `None`): a reader that
    /// read a stamp column sets it.
    pub fn from_raw(number: u32, etag: &str, algorithm: &str, value: &str) -> Option<Self> {
        Some(Self {
            number,
            etag: ETag::new(etag).ok()?,
            checksum: checksum::Part::from_wire_opt(algorithm, value),
            written_at: None,
        })
    }

    /// Encode the row back into its elements `(etag, algorithm, value)`.
    pub fn to_raw(&self) -> (String, String, String) {
        match self.checksum.as_ref() {
            Some(part) => {
                let (algorithm, value) = part.to_wire();
                (
                    self.etag.as_str().to_owned(),
                    algorithm.to_owned(),
                    value.to_owned(),
                )
            }
            None => (self.etag.as_str().to_owned(), String::new(), String::new()),
        }
    }

    /// The part as the S3 surface answers it. The size belongs to the
    /// backend's own content plane and is the caller's; the modification time
    /// is the row's **own** write stamp ([`Self::written_at`]).
    ///
    /// `fallback` is the stamp-less row's answer, and the reason this
    /// projection takes one: a row that predates the stamp decodes to
    /// [`Self::written_at`] `None`, and answers the upload's creation instant
    /// — what every backend served before the stamp existed, kept rather than
    /// invented so an old row still reads. A conformant backend's own rows
    /// carry the stamp, so the fallback is never what a live row answers.
    ///
    /// A stored part number outside the API's `1..=10000` is not a
    /// representable [`multipart::PartInfo`]: the S3 surface has no such
    /// part, so the projection refuses with
    /// [`storage::Error::InvalidPartNumber`], naming the number — the row is
    /// the backend's (a stored `u32`), while the projection is the
    /// consumer's, so a row this crate did not write must not panic a
    /// `list_parts()` read.
    pub fn to_info(
        &self,
        size: u64,
        fallback: SystemTime,
    ) -> Result<multipart::PartInfo, storage::Error> {
        Ok(multipart::PartInfo {
            part_number: multipart::part_number(self.number)?,
            size,
            etag: self.etag.clone(),
            last_modified: self.written_at.unwrap_or(fallback),
            checksum: self.checksum.clone(),
        })
    }
}

/// One cursor chunk of a bucket's uploads: the rows a bounded read served in
/// the listing's own order, and whether the store holds any row after them.
///
/// The chunk is the unit a backend with a keyset read answers one statement
/// of. [`UploadMeta::list_uploads_page`] reads every row of the prefix before
/// it cuts one page — 100k uploads cost a scan of all of them per page
/// (F17) — so the page is assembled from chunks instead: the composite order
/// of the last served row is the next read's bound, and the pull repeats
/// until the page's roll-up is complete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadsChunk {
    /// The rows served, ascending by the composite `key\0upload_id` order
    /// ([`crate::storage::uploads_order`]).
    pub uploads: Vec<MultipartUpload>,
    /// Whether the store holds at least one row after the chunk's last one.
    pub more: bool,
}

/// One page of [`UploadMeta::list_parts_page`]: the rows the join served, the
/// raw read's truncation flag, and the raw read's own last part number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartsPage {
    /// The rows the join served, in ascending part-number order — the page's
    /// visible content.
    pub rows: Vec<PartRow>,
    /// Whether the raw read saw a row beyond the page. `max == 0` answers
    /// `false`: a page the caller requested nothing of must never read as
    /// truncated, or the resume arithmetic would advance past parts it never
    /// received.
    pub more: bool,
    /// The **raw** read's last part number — before the row join dropped any
    /// domain-invalid row — or `None` when the raw read answered no row at
    /// all.
    ///
    /// Not derivable from [`Self::rows`]: the join self-heals a
    /// domain-invalid stored ETag by dropping the row, so a truncated page
    /// whose raw rows are all invalid serves no row while the raw read did
    /// read some. The caller's resume marker must still advance past them — a
    /// truncated page that names no marker sends the client back to the same
    /// page forever — so the backend reports its own raw read fact instead of
    /// leaving the caller to re-derive what the served rows cannot carry.
    pub raw_last: Option<u32>,
}

/// The completion-time record of one requested part: the stored ETag and
/// checksum, or `None` when the part was never stored. The completion rule
/// compares the client's ETag against this record, so the size stays out of it
/// — while the part's own write stamp rides it, because the completion joins
/// the same part rows the listing does ([`PartRow::written_at`]'s rule).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartRecord {
    /// Part number.
    pub number: u32,
    /// The stored part ETag.
    pub etag: ETag,
    /// The stored per-part checksum (`None` = none, or a domain-invalid stored
    /// wire — self-healing).
    pub checksum: Option<checksum::Part>,
    /// The instant the **store** last wrote the part —
    /// [`PartRow::written_at`]'s value and rule (`None` = a row that predates
    /// the stamp).
    pub written_at: Option<SystemTime>,
}

impl PartRecord {
    /// Decode the `PARTS` element and the `PART_CHECKSUMS` elements, with the
    /// same self-heal as [`PartRow::from_raw`] and the same absent write stamp
    /// (the element pair carries none).
    pub fn from_raw(number: u32, etag: &str, algorithm: &str, value: &str) -> Option<Self> {
        let row = PartRow::from_raw(number, etag, algorithm, value)?;
        Some(Self {
            number: row.number,
            etag: row.etag,
            checksum: row.checksum,
            written_at: row.written_at,
        })
    }
}

impl From<&PartRow> for PartRecord {
    fn from(row: &PartRow) -> Self {
        Self {
            number: row.number,
            etag: row.etag.clone(),
            checksum: row.checksum.clone(),
            written_at: row.written_at,
        }
    }
}

/// Upload rows: the in-progress multipart uploads, their parts, and the
/// completed object's retained part list.
///
/// # Examples
///
/// ```ignore
/// use tinio_core::{bucket, meta::{Metadata, UploadMeta}, object};
///
/// async fn uploads<M: UploadMeta + Metadata>(store: &M, bucket: &bucket::Name) {
///     assert!(!store.has_uploads(bucket).await.unwrap());
///     let key = object::key("big.bin").unwrap();
///     assert!(store.get(bucket, &key, "upload-1").await.unwrap().is_none());
/// }
/// ```
#[async_trait]
pub trait UploadMeta: Send + Sync + 'static {
    /// Upsert the upload state, identified by `(bucket, upload_id)`, with
    /// `row`'s key, initiation time, tags, owner and ACL.
    async fn create(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert the upload state **only when the bucket row exists**: the
    /// check runs inside the write transaction, so a concurrent
    /// [`BucketMeta::remove_if_empty`](crate::meta::BucketMeta::remove_if_empty)
    /// cannot be raced by a create, and the refusal projects to
    /// [`crate::storage::Error::NoSuchBucket`].
    ///
    /// [`create`](UploadMeta::create) stays unguarded: not every backend can
    /// answer a bucket-row check inside the upload write (a bucket plane whose
    /// rows materialize lazily would refuse a bucket it has never written a
    /// row for), so the guard is opt-in and the default is the plain upsert.
    async fn create_guarded(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata,
    {
        UploadMeta::create(self, bucket, upload_id, row).await
    }

    /// The upload state, present only when the upload exists **and** records
    /// `key`.
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<Option<Row>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Upsert one part's ETag and its optional checksum, after the upload's key
    /// matches — one transaction, so the row and its checksum row cannot
    /// disagree.
    async fn put_part(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        number: multipart::PartNumber,
        etag: &ETag,
        checksum: Option<&checksum::PartChecksum>,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The upload's parts at or after `from`, in ascending part-number order.
    ///
    /// Every row carries the store's own write stamp for that part, under the
    /// field's contract ([`PartRow::written_at`]) — the stamp a listed part
    /// answers as its `LastModified` ([`PartRow::to_info`]).
    async fn list_parts(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: multipart::PartNumber,
    ) -> Result<Vec<PartRow>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// One page of the upload's parts: the rows at or after the **raw** `from`
    /// number, in ascending order, capped at `max`, plus whether more rows
    /// remain after the page — the whole answer is [`PartsPage`], which also
    /// carries the raw read's last part number for the caller's resume marker.
    ///
    /// The lower bound is a raw `u32`, not a validated
    /// [`multipart::PartNumber`] (`1..=10000`): the listing layer's resume
    /// arithmetic must be able to name every stored number — `u32::MAX`
    /// included, where an exclusive-after marker has no successor — and a page
    /// must cost O(page) rows rather than a full-upload scan ([
    /// `UploadMeta::list_parts`] takes no bound and walks the rest of the
    /// upload). `max == 0` answers an empty page and `false`: a page the
    /// caller requested nothing of must never read as truncated, or the
    /// client's resume arithmetic would advance the marker past parts it
    /// never received.
    ///
    /// The row join is [`UploadMeta::list_parts`]'s: a domain-invalid stored
    /// ETag reads as absent (the row is skipped, self-healing) and a
    /// domain-invalid checksum self-heals to `None` — which is why
    /// [`PartsPage::raw_last`] is part of the answer: the truncation flag is
    /// the raw read's and the rows are the join's, so a caller resuming from
    /// the served rows alone would stall on a truncated page the self-heal
    /// emptied. The upload's existence and its recorded key are the caller's
    /// check ([`UploadMeta::get`]) — this call answers rows for whatever
    /// `(bucket, upload_id)` it is given.
    async fn list_parts_page(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
        from: u32,
        max: usize,
    ) -> Result<PartsPage, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// The completion-time join: one record per requested part number, in the
    /// input's order, `None` for a number with no stored part. The outer
    /// `None` is an upload whose key does not match.
    async fn part_records(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        numbers: &[multipart::PartNumber],
    ) -> Result<Option<Vec<Option<PartRecord>>>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Every in-progress upload of the bucket.
    async fn list_uploads(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// One page of the bucket's uploads: the page's entries, its rolled-up
    /// common prefixes, whether more remain, and the next marker.
    ///
    /// `max_uploads == 0` answers an empty page, no common prefixes, `false`
    /// and no marker: a page the caller requested nothing of must never read
    /// as truncated, or the client's resume arithmetic would advance the
    /// marker past uploads it never received. A truncated page always carries
    /// its resume marker — the last entry of the page, exclusive-after.
    async fn list_uploads_page(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        delimiter: Option<&str>,
        marker: Option<&str>,
        max_uploads: usize,
    ) -> Result<(Vec<MultipartUpload>, Vec<String>, bool, Option<String>), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// One chunk of the bucket's uploads, in the listing's own order — the
    /// composite `key\0upload_id` of [`crate::storage::uploads_order`],
    /// ascending — strictly after `after` (`None` = the first row inside
    /// `prefix`), capped at `max` rows.
    ///
    /// This is the cursor form a listing page is assembled with:
    /// [`UploadMeta::list_uploads_page`] reads every row of the prefix before
    /// it cuts one page, so the page body pulls this chunk by chunk and stops
    /// only once the shared pager's `max + 1`-entry lookahead is satisfied —
    /// never at a chunk boundary: a delimiter group is one entry however many
    /// rows it spans, so a bound applied before the roll-up would split a
    /// group across chunks and the page would come back short of its last
    /// entry. A backend whose rows are seekable overrides this body with a
    /// keyset read; the page it answers is the one
    /// [`UploadMeta::list_uploads_page`] answers for the same arguments.
    ///
    /// The default body is the materialized read — today's behaviour, so a
    /// backend that does not override this costs exactly what it always did:
    /// the whole prefix is read, filtered by the cursor and capped at `max`.
    /// It reaches that answer by delegating to
    /// [`UploadMeta::list_uploads_page`], which reads **every row of the
    /// prefix** before it cuts a page — so a pager that loops this default
    /// pays one full-prefix scan **per chunk**, the very cost the
    /// bounded-listing work exists to remove. That is why a store whose rows
    /// can be seeked overrides it (redb's keyset read, pg's seeked statement),
    /// and why the default is deliberately **not** removed: correctness never
    /// depends on the override landing.
    ///
    /// `max == 0` answers no rows and `more == false`: a chunk the caller
    /// requested nothing of must never read as more remaining, or the resume
    /// arithmetic would advance past uploads it never received.
    async fn list_uploads_chunk(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&str>,
        max: usize,
    ) -> Result<UploadsChunk, <Self as Metadata>::Error>
    where
        Self: Metadata,
    {
        // The engine's own marker is exclusive-after on the composite order,
        // which is exactly this chunk's cursor. No delimiter: a chunk is
        // plain rows, never a roll-up, so the engine's page is the `max`
        // smallest orders after the cursor and its truncation flag is
        // `more`.
        let (uploads, _prefixes, more, _next) = self
            .list_uploads_page(bucket, prefix, None, after, max)
            .await?;
        Ok(UploadsChunk { uploads, more })
    }

    /// Whether the bucket has any in-progress upload (the empty-bucket probe).
    async fn has_uploads(&self, bucket: &bucket::Name) -> Result<bool, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Every in-progress upload, across all buckets (the orphan sweep's feed).
    async fn walk_uploads(&self) -> Result<Vec<MultipartUpload>, <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove one upload row and its parts (idempotent).
    async fn drain_upload(
        &self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove `(bucket, upload_id)` and its parts **only when the upload
    /// exists and records `key`** — the abort path's identity check and
    /// drain in **one transaction**, so a racing complete cannot slip
    /// between the check and the removal (the check reads the same handle
    /// the drain removes).
    ///
    /// An upload with no row, or one recorded under a different key,
    /// projects to [`crate::storage::Error::NoSuchUpload`] (D22) and
    /// nothing is drained; the answer is a refusal, never an empty
    /// success, exactly as if the upload was already consumed.
    async fn drain_upload_checked(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;

    /// Remove every upload row and every part of the bucket (idempotent).
    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error>
    where
        Self: Metadata;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_upload_row_round_trips_and_self_heals() {
        let row = Row::from_raw("u1", "dir/big.bin", 42, "env=prod", "", "")
            .expect("a valid key decodes");
        assert_eq!(row.upload_id, "u1");
        assert_eq!(row.key.as_ref(), "dir/big.bin");
        assert_eq!(row.initiated_at, from_nanos(42));
        assert_eq!(row.tags.iter().collect::<Vec<_>>(), [("env", "prod")]);
        let wires = row.to_raw();
        assert_eq!(
            Row::from_raw(
                &row.upload_id,
                &wires.key,
                wires.initiated_at,
                &wires.tags,
                &wires.owner,
                &wires.grants
            )
            .unwrap(),
            row
        );
        // A domain-invalid key cannot be a key at all.
        assert!(Row::from_raw("u1", "../evil", 0, "", "", "").is_none());
        // The metadata wires heal independently.
        let healed = Row::from_raw("u1", "k", 0, "team=%zz&", "not-an-owner", "id=short,READ")
            .expect("the key is valid");
        assert!(healed.tags.is_empty());
        assert_eq!(healed.owner, None);
        assert!(healed.acl.grants.is_empty());
    }

    #[test]
    fn an_upload_row_becomes_the_s3_state() {
        let row = Row::from_raw("u1", "big.bin", 0, "", "", "").unwrap();
        let bucket = bucket::name("data").unwrap();
        let upload = row.to_upload(&bucket);
        assert_eq!(upload.upload_id, "u1");
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key.as_ref(), "big.bin");
        assert_eq!(upload.checksum, None);
    }

    #[test]
    fn a_part_row_round_trips_and_self_heals_its_etag_and_checksum() {
        let part = PartRow {
            number: 10,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            checksum: Some(checksum::Part {
                algorithm: checksum::Algorithm::Crc32,
                value: checksum::Value("NhCmhg==".into()),
            }),
            // The element pair carries no stamp, so the round trip's row holds
            // none either.
            written_at: None,
        };
        let (etag, algorithm, value) = part.to_raw();
        assert_eq!(PartRow::from_raw(10, &etag, &algorithm, &value), Some(part));
        // A domain-invalid ETag makes the part read as absent.
        assert_eq!(PartRow::from_raw(1, "not-an-etag", "", ""), None);
        // A domain-invalid checksum heals; the part is still listed.
        let healed = PartRow::from_raw(1, &etag, "BLAKE3", "AA==").unwrap();
        assert_eq!(healed.checksum, None);
        assert_eq!(healed.etag.as_str(), etag);
    }

    #[test]
    fn the_part_record_is_the_completion_view_of_a_part_row() {
        let row = PartRow {
            number: 2,
            etag: ETag::new("900150983cd24fb0d6963f7d28e17f72").unwrap(),
            checksum: None,
            written_at: None,
        };
        let record = PartRecord::from(&row);
        assert_eq!(record.number, 2);
        assert_eq!(record.etag, row.etag);
        assert_eq!(
            PartRecord::from_raw(2, &row.etag.as_str(), "", ""),
            Some(record)
        );
        assert_eq!(PartRecord::from_raw(2, "not-an-etag", "", ""), None);
    }

    /// The listed part's modification time is the **part's own** write stamp:
    /// a row that records one answers it, and only a row that records none
    /// answers the fallback the caller passes (the upload's creation time).
    #[test]
    fn a_part_reports_its_own_write_stamp_and_falls_back_only_when_it_has_none() {
        let written = from_nanos(1_700_000_000_000_000_000);
        let upload_created = from_nanos(1_600_000_000_000_000_000);
        let mut row = PartRow {
            number: 1,
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            checksum: None,
            written_at: Some(written),
        };
        assert_eq!(
            row.to_info(5, upload_created).unwrap().last_modified,
            written,
            "a part's LastModified is the store's own stamp for that part"
        );
        // The completion-time view carries the same stamp.
        assert_eq!(PartRecord::from(&row).written_at, Some(written));
        // The store that records none answers the fallback, not an invention.
        row.written_at = None;
        assert_eq!(
            row.to_info(5, upload_created).unwrap().last_modified,
            upload_created
        );
    }

    /// A stored part number outside the API range is not a representable
    /// `PartInfo`: the projection refuses and names the number instead of
    /// panicking. The row itself (`PartRow`) is the backend's raw `u32` and
    /// is served either way; only the projection into the S3 shape can fail.
    #[test]
    fn an_out_of_range_stored_part_number_refuses_the_part_info_projection() {
        for number in [0, 10_001, u32::MAX] {
            let row = PartRow {
                number,
                etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
                checksum: None,
                written_at: None,
            };
            assert!(
                matches!(
                    row.to_info(5, SystemTime::UNIX_EPOCH),
                    Err(storage::Error::InvalidPartNumber(n)) if n == number
                ),
                "a stored part number of {number} must refuse the PartInfo projection"
            );
        }
    }
}
