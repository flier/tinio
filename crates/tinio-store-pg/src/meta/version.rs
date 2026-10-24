//! The VersionMeta read plane over the version_rows, version_index,
//! bucket_versioning and current_slots relations.
//!
//! A version row and its index entry commit together or not at all: the index
//! is keyed by the row's identity wire (bucket, key, id), so the two writes
//! share one transaction and a removal retires the entry only when it still
//! points at the sequence being removed (two rows can share the null wire, and
//! the index names only the newest of them).
//!
//! The current slot is separate state (current_slots): the slot records the
//! identity, the sequence and the delete-marker bit. `recorded_marker`
//! resolves the bit from the version row at that sequence when the row exists,
//! falling back to the recorded column when it does not -- a **restore** consumes
//! the row whose kind the bit came from, so the bit must outlive it (the single
//! landed migration `0001_init.sql`). The listing and the point read both call
//! it. Nothing is derived from
//! an object row, so a content rewrite can never silently un-version an object.
//!
//! Every write and every Object Lock rule lives in `super::version_lock`; this
//! module owns the row shapes, the decoders and the reads.

use std::time::SystemTime;

use sqlx::query_file_as;
use time::OffsetDateTime;

use crate::{
    _core::{
        ETag,
        acl::Acl,
        bucket,
        meta::{
            object::Row as ObjectRow,
            version::{Current, Kind, Slot, Stored},
        },
        object,
        version::{Entry, Id, ListParams, State, paginate},
    },
    Error,
    meta::object::object_row,
    store::PgStore,
    timestamp::from_timestamp,
};

/// The eleven columns every version read selects, in the order
/// `sql/meta/version/*.sql` spells them: the row's sequence, its identifier,
/// blob, size and the derived elements, then the metadata wires and the ACL
/// composite (which carries the owner).
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode below is a typed conversion, not a
/// positional read.
pub(super) struct StoredVersion {
    pub(super) seq: i64,
    pub(super) id: String,
    pub(super) blob: String,
    pub(super) size: i64,
    pub(super) etag: String,
    pub(super) created: OffsetDateTime,
    pub(super) became_noncurrent: i64,
    pub(super) kind: Kind,
    pub(super) tags: object::Tags,
    pub(super) checksum: String,
    pub(super) acl: Acl,
}

/// The slot read's columns, in `current_slot.sql`'s order: the sequence the
/// slot records, the current version's identity wire (which the left join
/// leaves NULL for a slot whose row is gone), the bit the slot's own record
/// carries (a restored marker outlives the row it came from), and the row's
/// kind, NULL when the row is gone. The row's kind decides when the row is
/// there; the recorded bit answers when it is not.
struct StoredSlot {
    seq: i64,
    version_id: Option<String>,
    is_delete_marker: bool,
    kind: Option<Kind>,
}

/// The slot's marker bit. The version row at the slot's sequence decides when
/// that row is there; the recorded column answers when the row is gone.
///
/// This is the rule the contract fixed for every backend (R7, written into
/// [`VersionMeta::current_slot`](crate::_core::meta::VersionMeta::current_slot)):
/// a present version row decides the bit, and the slot's own recorded bit
/// answers only where no row is. The order is row-first, so a stale recorded
/// bit never overrides the row the slot names.
fn recorded_marker(kind: Option<Kind>, bit: bool) -> bool {
    kind.map_or(bit, Kind::is_marker)
}

/// One row of `bucket_listing.sql`: a key's object row and slot, then one of
/// its version rows -- the whole of what a version listing serves a key, read
/// bucket-wide in one statement. A key with no version row (or none the two
/// joins matched) answers a single row whose version columns are NULL.
/// `slot_kind` is the version row at the slot's own sequence, and
/// `is_delete_marker` is the bit the slot recorded. `recorded_marker` turns
/// the pair into the marker the listing reports, repeated on every row of the key.
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode below is a typed conversion, not a
/// positional read.
struct StoredListing {
    key: String,
    etag: String,
    size: i64,
    mtime: OffsetDateTime,
    identity: i64,
    tags: object::Tags,
    checksum: String,
    acl: Acl,
    slot_seq: Option<i64>,
    version_id: Option<String>,
    is_delete_marker: Option<bool>,
    slot_kind: Option<Kind>,
    row_seq: Option<i64>,
    id: Option<String>,
    blob: Option<String>,
    row_size: Option<i64>,
    row_etag: Option<String>,
    created: Option<OffsetDateTime>,
    became_noncurrent: Option<i64>,
    row_kind: Option<Kind>,
    row_tags: Option<object::Tags>,
    row_checksum: Option<String>,
    row_acl: Option<Acl>,
}

/// The one column `bucket_state` reads: the bucket's versioning state.
pub(super) struct StoredState {
    pub(super) state: State,
}

impl StoredVersion {
    /// The sequence and the contract row; `None` is a corrupt blob, kind or
    /// identifier -- what the row is and where its content lives cannot be
    /// guessed, so the row reads as absent (self-healing). The row is consumed:
    /// its tag set and ACL move into the contract row rather than being copied.
    pub(super) fn decode(self) -> Option<(u64, Stored)> {
        let seq = self.seq as u64;
        Stored::from_values(
            seq,
            &self.id,
            &self.blob,
            self.size as u64,
            &self.etag,
            from_timestamp(self.created),
            self.became_noncurrent as u64,
            self.kind,
            self.tags,
            &self.checksum,
            self.acl,
        )
        .map(|stored| (seq, stored))
    }

    /// The contract row alone, for the reads that do not answer the sequence.
    pub(super) fn contract(self) -> Option<Stored> {
        self.decode().map(|(_, stored)| stored)
    }
}

impl StoredListing {
    /// The key's object row, decoded by [`object_row`] -- the strict walk's rule, so a
    /// listing refuses a row whose elements cannot be read rather than serving the key as
    /// empty.
    fn elements(&self) -> Option<ObjectRow> {
        object_row(
            &self.etag,
            self.size,
            self.mtime,
            self.identity,
            self.tags.clone(),
            &self.checksum,
            self.acl.clone(),
        )
    }

    /// The key's current version as the gate resolves it for a key whose object row is there:
    /// the sequence the slot records (0 for a key with no slot row, the state a bucket that
    /// never recorded a version leaves), the recorded identity, and the marker bit.
    ///
    /// The bit is `recorded_marker` on the slot's own sequence. A newer version
    /// row on this same listing row does not get a vote.
    fn current(&self) -> (u64, Option<Id>, bool) {
        (
            self.slot_seq.unwrap_or(0) as u64,
            self.version_id.as_deref().and_then(Id::from_wire),
            recorded_marker(self.slot_kind, self.is_delete_marker.unwrap_or(false)),
        )
    }

    /// This row's version row as the contract's own. `None` is the join's half-empty row -- a
    /// key the statement served without a version row -- and a domain-invalid blob or
    /// identifier, which self-heals as absent ([`Stored::from_values`]' rule, the same decode
    /// `list_versions` applies) rather than failing the listing.
    fn stored(&self) -> Option<Stored> {
        Stored::from_values(
            self.row_seq? as u64,
            self.id.as_deref()?,
            self.blob.as_deref()?,
            self.row_size? as u64,
            self.row_etag.as_deref()?,
            from_timestamp(self.created?),
            self.became_noncurrent? as u64,
            self.row_kind?,
            self.row_tags.clone()?,
            self.row_checksum.as_deref()?,
            self.row_acl.clone()?,
        )
    }
}

/// One key's versions as [`PgStore::bucket_versions`] returns them: the key, the facts its
/// object row and its slot record for the **current** version, and its version rows newest
/// first -- the shape a `ListObjectVersions` page is cut from, one key at a time.
///
/// The current version is read from the object row and the slot, never from a version row of
/// its own: a delete marker's row carries the empty elements `advance_slot` materialized, so no
/// stale element of the object the marker hid can be served as the current version's.
pub struct KeyVersions {
    /// The key the versions belong to.
    pub key: object::Key,
    /// The current version's recorded identity; `None` is the null version.
    pub version: Option<Id>,
    /// The sequence the key's slot records, or 0 for a key with no slot row.
    pub seq: u64,
    /// Whether the current version is a delete marker.
    pub is_delete_marker: bool,
    /// The current version's recorded elements: this key's object row.
    pub elements: ObjectRow,
    /// The key's history, newest first: every version row that is not the current version's own
    /// ([`Stored::is_history`], the contract's own predicate).
    pub history: Vec<Stored>,
}

impl KeyVersions {
    /// The entry a key's first listing row opens: its object row's elements beside the facts
    /// the slot records. The marker bit is the statement's, not this row's version row: the
    /// newest joined row is history whenever the slot names an older sequence.
    fn open(row: &StoredListing, key: object::Key, bucket: &bucket::Name) -> Result<Self, Error> {
        let elements = row.elements().ok_or_else(|| Error::Corrupt {
            bucket: bucket.clone(),
            key: key.to_string(),
        })?;
        let (seq, version, is_delete_marker) = row.current();
        let mut opened = Self {
            key,
            version,
            seq,
            is_delete_marker,
            elements,
            history: Vec::new(),
        };
        opened.absorb(row);
        Ok(opened)
    }

    /// Absorb one further listing row of this key: the version row it carries when that row is
    /// history. A row at the slot's own sequence is the current version's own row (a marker's),
    /// so it is never listed twice.
    fn absorb(&mut self, row: &StoredListing) {
        let at = Slot::At(self.seq);
        if let Some(stored) = row.stored()
            && stored.is_history(at)
        {
            self.history.push(stored);
        }
    }
}

impl PgStore {
    /// The key's slot row in one read: the sequence it records beside the contract's
    /// [`Current`] facts. `None` for a key with no slot.
    ///
    /// A gate resolve needs both facts together — the sequence a write's monotonic move plans
    /// from (the contract's [`crate::_core::meta::VersionMeta::current_slot`] shape carries none) and the identity
    /// and marker bit [`Current`] carries — so they are one statement rather than the two
    /// sequential reads they used to be.
    pub async fn slot_record(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<(u64, Current)>, Error> {
        let row = query_file_as!(StoredSlot, "sql/meta/version/current_slot.sql", bucket, key)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map(|row| {
            (
                row.seq as u64,
                Current {
                    exists: true,
                    version: row.version_id.as_deref().and_then(Id::from_wire),
                    is_delete_marker: recorded_marker(row.kind, row.is_delete_marker),
                },
            )
        }))
    }

    /// A bucket's versions, one key at a time: every key the bucket holds, each with the facts
    /// its slot records and its version rows newest first -- one key per group of
    /// [`KeyVersions`], read in **one** statement.
    ///
    /// Resolving a listing one key at a time cost a slot read and a version-row read per key,
    /// so a page holding a handful of keys paid `1 + 2N` round trips and the count grew with the
    /// bucket. The statement reads the same rows either way -- a listing is the bucket's own
    /// read, however it is cut -- so what is bounded here is the round trips a page pays, which
    /// is the cost the caller sees.
    ///
    /// The statement's own order is the listing's: keys ascending, each key's version rows
    /// newest first, so the caller cuts its page without sorting.
    pub async fn bucket_versions(&self, bucket: &bucket::Name) -> Result<Vec<KeyVersions>, Error> {
        let rows = query_file_as!(StoredListing, "sql/meta/version/bucket_listing.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        let mut versions: Vec<KeyVersions> = Vec::new();
        for row in rows {
            let key = object::key(&row.key).map_err(|_| Error::Corrupt {
                bucket: bucket.clone(),
                key: row.key.clone(),
            })?;
            // Every row of one key arrives together, the statement ordering by key: the key's
            // object row and slot are read where the key first appears, and the rows that
            // follow contribute their own version row alone.
            match versions.last_mut() {
                Some(open) if open.key == key => open.absorb(&row),
                _ => versions.push(KeyVersions::open(&row, key, bucket)?),
            }
        }
        Ok(versions)
    }

    /// A bucket's versions, bounded to the page
    /// `paginate` cuts: the shape
    /// [`PgStore::bucket_versions`] answers for the whole bucket, read a chunk
    /// at a time until the cut is decided.
    ///
    /// [`PgStore::bucket_versions`] reads every key of the bucket per page — 380k rows and
    /// 182ms for one 1000-key page of a 200k-object bucket — because the statement has no
    /// `LIMIT` and no key bound. This read spends both: the statement seeks to the cursor,
    /// narrows to the prefix's own byte range and stops one row past the chunk, and the chunk
    /// loop repeats only while the page is still undecided —
    /// `paginate` is the engine that decides it, exactly as
    /// the fs and mem walks drive it, so a page is never short of the entries its own cut
    /// needed. Every row a chunk answers is bounded by the caller's `max_keys` plus one probe.
    ///
    /// The prefix is applied here, not by `paginate`: that
    /// engine rolls a matching key up and emits every other key as an entry, so without the
    /// read's range a listing carries the bucket's keys the prefix never asked for — the answer
    /// fs's and mem's walks never serve, because they filter their keys as they walk them. The
    /// range is the exact rule `prefix_upper` states, so the
    /// page is the one a filtered stream cuts: the prefix's keys only, in the listing's order,
    /// with the same truncation flag and resume markers.
    pub async fn bucket_versions_page(
        &self,
        bucket: &bucket::Name,
        params: &ListParams,
    ) -> Result<Vec<KeyVersions>, Error> {
        // A page cut to nothing asks for nothing (`paginate`'s own rule): the
        // empty answer without reading a row.
        if params.max_keys == 0 {
            return Ok(Vec::new());
        }
        let prefix = params.prefix.as_deref().unwrap_or_default();
        let chunk = params.max_keys.saturating_add(1);
        let mut versions: Vec<KeyVersions> = Vec::new();
        // The first chunk starts at the key_marker's own key, read whole: a
        // `version_id_marker` resumes *inside* it, so its rows are needed.
        let mut cursor = VersionCursor::At(params.key_marker.clone().unwrap_or_default());
        loop {
            let rows = self
                .version_rows_chunk(bucket, prefix, &cursor, chunk)
                .await?;
            let Some(last) = rows.last() else {
                break; // the bucket's rows are exhausted
            };
            let next = match last.row_seq {
                Some(seq) => VersionCursor::In(last.key.clone(), seq),
                None => VersionCursor::Past(last.key.clone()),
            };
            let full = rows.len() >= chunk;
            for row in rows {
                let key = object::key(&row.key).map_err(|_| Error::Corrupt {
                    bucket: bucket.clone(),
                    key: row.key.clone(),
                })?;
                // Every row of one key arrives together, the statement ordering by key: the
                // key's object row and slot are read where the key first appears -- a chunk
                // boundary inside a key does not reopen it -- and the rows that follow
                // contribute their own version row alone.
                match versions.last_mut() {
                    Some(open) if open.key == key => open.absorb(&row),
                    _ => versions.push(KeyVersions::open(&row, key, bucket)?),
                }
            }
            if page_is_decided(&versions, params) {
                break; // the page's own cut already answers truncation
            }
            if !full {
                break; // the statement's limit was not reached: nothing follows
            }
            cursor = next;
        }
        Ok(versions)
    }

    /// One bounded chunk of a bucket's listing rows: at most `limit` rows in the listing's
    /// order (keys ascending, each key's version rows newest first) inside `prefix`'s byte
    /// range, from `cursor` on.
    ///
    /// The statement is `sql/meta/version/bucket_listing_page.sql`, or its sibling
    /// `bucket_listing_page_open.sql` for a prefix with no upper bound; `limit` is the caller's
    /// page plus one probe, so a chunk is at most one page's worth of rows.
    ///
    /// Both `version_rows` joins carry `$2` in their `ON` clause, and so does `current_slots`: it
    /// is a no-op there — a row's key is its object row's key, which `$2` already bounds, and a
    /// `LEFT JOIN` keeps an unmatched object row either way — and it is what lets each inner
    /// index scan start at the cursor. Without it the planner merges the joins over the bucket's
    /// whole ranges, and a page 50k keys into the bucket reads every row before it: 205k buffers
    /// against this bound's few thousand.
    async fn version_rows_chunk(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        cursor: &VersionCursor,
        limit: usize,
    ) -> Result<Vec<StoredListing>, Error> {
        let (key, seq) = cursor.bounds();
        // The scan never starts below the prefix: the listing holds the prefix's own keys, so
        // a cursor sorting below it (a resumption of an earlier, wider request) would seek
        // behind the range the read may serve.
        let lower = if key < prefix { prefix } else { key };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = match super::upload::prefix_upper(prefix) {
            Some(upper) => {
                query_file_as!(
                    StoredListing,
                    "sql/meta/version/bucket_listing_page.sql",
                    bucket,
                    lower,
                    seq,
                    limit,
                    prefix,
                    &upper
                )
                .fetch_all(self.pool())
                .await?
            }
            None => {
                query_file_as!(
                    StoredListing,
                    "sql/meta/version/bucket_listing_page_open.sql",
                    bucket,
                    lower,
                    seq,
                    limit,
                    prefix
                )
                .fetch_all(self.pool())
                .await?
            }
        };
        Ok(rows)
    }
}

/// The bounded listing read's resume cursor: where the next chunk's statement starts in the
/// bucket's key order, and how much of the bound's own key is still owed.
#[derive(Debug, Clone)]
enum VersionCursor {
    /// The statement's first chunk: the key its bound names is read whole, because a
    /// `version_id_marker` resumes *inside* that key (`paginate`'s own start rule).
    At(String),
    /// A continuation inside one key: the rows after this sequence. The statement's order is
    /// newest first, so `r.seq < seq` is exactly the rows the key still owes.
    In(String, i64),
    /// A continuation past a key whose rows are done — a key the join answered a single
    /// NULL-sequence row for holds no version row at all, so none of it is left.
    Past(String),
}

impl VersionCursor {
    /// The bounds the statement binds: the scan's lower bound (`$2`) and the exclusive
    /// sequence inside it (`$3`, NULL = the whole key is owed).
    fn bounds(&self) -> (&str, Option<i64>) {
        match self {
            Self::At(key) => (key, None),
            Self::In(key, seq) => (key, Some(*seq)),
            // The sequence below every storable one: a version row's sequence is written from a
            // `u64`, so no row of the key answers `r.seq < -1` and the whole key sorts behind
            // the bound.
            Self::Past(key) => (key, Some(-1)),
        }
    }
}

/// Whether `paginate` already answers a truncated page from
/// the entries accumulated so far — the stop rule the bounded read shares with the fs and mem
/// walks. Entries are accumulated in the listing's own order, so a page that is not yet
/// truncated may still need rows the walk has not read; one that is truncated never will,
/// because the cut stops at the entry it has already seen.
fn page_is_decided(versions: &[KeyVersions], params: &ListParams) -> bool {
    paginate(&probe_entries(versions), params).is_truncated
}

/// The accumulated listing's entries, as far as the page cut reads them: an entry's key and its
/// version identity, which is all `paginate` tests (the
/// roll-up reads the key, the resume lookup reads the identity's wire form, and the page size is
/// a count). A probe therefore clones neither a version's tag set, ACL, size nor timestamp — the
/// listing body builds the entries it serves from the same rows.
fn probe_entries(versions: &[KeyVersions]) -> Vec<Entry> {
    let mut entries = Vec::new();
    for key_versions in versions {
        entries.push(probe_entry(
            &key_versions.key,
            key_versions.version.as_ref(),
        ));
        for stored in &key_versions.history {
            entries.push(probe_entry(&key_versions.key, stored.id.as_ref()));
        }
    }
    entries
}

/// One probe entry: the key and the identity, with the elements the cut never reads left empty.
fn probe_entry(key: &object::Key, id: Option<&Id>) -> Entry {
    Entry {
        key: key.clone(),
        id: id.cloned(),
        seq: 0,
        is_delete_marker: false,
        size: 0,
        last_modified: SystemTime::UNIX_EPOCH,
        etag: ETag::EMPTY,
    }
}

#[cfg(test)]
mod tests;
