//! The version contract over mem's rows: the state machine, the
//! content-addressed blob table, and the version operations.
//!
//! mem has no filesystem, so the design's "the current version is the file"
//! degenerates to "the current version is the row": the object row plus the
//! shared `object_meta` row carry the current object version, and a superseded
//! version's whole content moves into the mem-local `blobs` table keyed by its
//! content hash — mem is content-addressed at whole-blob granularity, with no
//! chunking and no block files (the block engine is an fs-side concern, and the
//! conformance suite asserts semantics, never layout). The move and the
//! materialize are row copies, and every operation runs in one write
//! transaction, so the crash windows the fs supersede protocol guards do not
//! exist here.
//!
//! # The current delete marker, and the sequence rule
//!
//! mem's plain object paths are keyed on the shared `object_meta` row: a key
//! with no meta row is "not found" for every plain read and absent from every
//! plain listing. A **current** delete marker therefore gets no meta row — it
//! is a version row that is not history (`Row::is_history`), carrying no
//! index entry (a current version's identity is the slot itself, exactly as a
//! current object version's is; the index is history's). The marker's row is
//! written by the delete that makes it current and by the supersede that moves
//! it into history — and on a sequence left free by the rule below, so no
//! marker ever lands on a row another version occupies.
//!
//! # Sequences
//!
//! **No write may overwrite a version row that names different content**, and
//! the sequence rules are what make that structural rather than guarded:
//!
//! - A version-creating write, and a delete marker, take one past the highest
//!   the key records — the current slot's own sequence or the newest history
//!   row's, whichever is greater ([`Peek::next_seq`]) — so the row they write
//!   lands on a free sequence by construction.
//! - The **null version's sequence is 0**: a write that produces the null
//!   version records 0, and a suspended bucket's in-place replacement keeps
//!   the identity and the sequence the slot already records. When 0 is not
//!   free — a superseded null version's row already sits there, the one case
//!   the index's shared `null` wire value lets arise, a later null version
//!   following an earlier one into history — the incoming null takes the
//!   key's next sequence instead.
//!
//! A key holds **at most one `null`-id row** (AWS's model): when a supersede
//! moves a null version into history and an older null-id row already sits in
//! the table, the older row is replaced — removed with its index entry in the
//! same transaction — rather than a second one added, because the shared
//! `null` index entry can name only one of them and the other would be
//! unaddressable. The live null version is not that row (it is the slot's),
//! so a live null and one superseded null row still coexist as before.
//!
//! A supersede writes the outgoing version's row on the sequence its slot
//! records, which this same rule has left free, so it can never be another
//! version's row. A delete whose current version is the null-identity marker
//! removes that marker's row in place rather than superseding it, so repeated
//! suspended deletes keep one marker and add no history row.
//!
//! The shared tables (`versions`, `version_index`, `bucket_versioning`) are
//! `tinio_store_redb`'s — the same ones `tinio-fs` opens — and the write path's
//! **policy is the shared `tinio_version::plan_write`**, imported rather than
//! copied: the decision (whether to supersede, which identity and sequence
//! the incoming version takes, the one-null-row convergence, the marker's
//! fresh sequence) is a pure function of values, and only its execution is
//! mem's — one transaction of row operations, with no I/O to abstract over.
//!
//! # The contract boundary
//!
//! The bucket's versioning state is `VersionMeta`'s (`bucket_state`,
//! `set_bucket_state`, and the teardown's `drain_bucket`), and the
//! bucket-existence guard is the metadata contract's
//! (`BucketMeta::created_at`). The **row-level** reads and writes stay on the
//! shared tables, and the reason is structural rather than a missing contract
//! call: every site below is a step inside a multi-table atomic unit
//! (`peek`+`resolve`, a supersede, a delete's drop+materialize, the listing's
//! bounded window walk) that runs in ONE caller-owned transaction, and the
//! contract's methods each open their own — a contract call cannot join that
//! transaction, and splitting the unit across transactions would both break
//! the one-transaction rule and change what a concurrent reader observes. The
//! `blobs` table is mem-local besides (no contract carries a blob table), so
//! its byte accounting has no contract home at all.
//!
//! The module is gated on the `versioning` feature. With the feature off, no
//! `impl VersionOps` for `MemoryStorage` is compiled at all and the trait's
//! own default bodies answer the contract's refusal: `State::Unversioned`
//! from `bucket_versioning` and the access-denied class from the other nine —
//! the shape the [`VersionOps`] doc states for a backend that does not
//! version, with no per-backend twin to drift from it.
//!
//! `MemoryOptions::max_total_bytes` covers the history beside the live
//! content: a blob's bytes enter the tracked total when its blob row is
//! written (a content-addressed dedup hit adds nothing — the bytes are
//! already counted) and leave it when the row is deleted, which is the
//! moment its last naming version row goes: an explicit version delete, the
//! one-null-row convergence's displacement, a materialize that moved the
//! content back onto the key path, or the bucket teardown's drain (which
//! releases everything it drains). The last-reference test is a walk of the
//! bucket's version rows — mem is content-addressed per bucket, so another
//! key's row may name the same blob and only a bucket-wide walk can tell —
//! and it is what keeps the total equal to the bytes actually held rather
//! than a reference count that could drift. `rename_object` — an operation
//! no spec or plan gives versioning semantics — is refused while a bucket
//! records versions (the contract's shared rule, applied by the module's
//! `rename_refusal`) rather than destroying the destination's current
//! version and the source's history; an unversioned bucket keeps today's row
//! move.

use std::time::SystemTime;

use bytes::Bytes;
use futures::stream::iter;
use redb::WriteTransaction;

// The feature-on `records_versions` body is this import's only user — with
// the feature off the body answers the constant `false` and the shared
// state table is never opened.
#[cfg(feature = "versioning")]
use crate::_store_redb::bucket_versioning;
// The feature-off `VersionOps` impl below is the only user, so the gate is
// the import's too: the feature-on build (whose impl lives in `enabled`)
// does not carry an unused import.
#[cfg(not(feature = "versioning"))]
use crate::{_core::storage::VersionOps, storage::MemoryStorage};
use crate::{
    _core::{
        BodyStream, ByteRange, ETag, GetObjectResult,
        acl::{Acl, OwnerId},
        bucket::Name,
        checksum, object,
        storage::invalid_range,
        version::Id,
    },
    _store_redb::bucket as store_bucket,
    Error,
    error::no_such_bucket,
};

/// The feature-off `VersionOps`: the trait's default bodies own every answer
/// (the refusal class for the nine, `Unversioned` for `bucket_versioning`),
/// so there is nothing to write here and nothing to forget.
#[cfg(not(feature = "versioning"))]
impl VersionOps for MemoryStorage {}

/// Build a served read's result over content already in hand: the shared
/// ranged-read construction of both the version-addressed reads and the
/// plain read (`object::get_object`), so the crate has exactly one. The
/// body is one `Bytes` chunk — mem serves whole objects or a resolved
/// slice of one, never a streamed file.
#[allow(clippy::too_many_arguments)]
pub(crate) fn object_result(
    key: &object::Key,
    size: u64,
    last_modified: SystemTime,
    etag: ETag,
    tags: object::Tags,
    checksum: Option<checksum::Recorded>,
    owner: Option<OwnerId>,
    acl: Acl,
    version_id: Option<Id>,
    records_versions: bool,
    data: &[u8],
    served: Option<(u64, u64)>,
) -> Result<GetObjectResult, Error> {
    let body: Vec<u8> = match served {
        // The stored row's claimed size and the bytes beside it are two facts:
        // a row whose size runs past its content (only reachable by editing the
        // store behind the backend) refuses the range as unsatisfiable instead
        // of panicking on the slice.
        Some((start, end)) => data
            .get(start as usize..=end as usize)
            .ok_or_else(|| invalid_range(ByteRange::Inclusive(start, end), data.len() as u64))?
            .to_vec(),
        None => data.to_vec(),
    };
    Ok(GetObjectResult {
        info: object::Info {
            key: key.clone(),
            size,
            last_modified,
            etag,
            tags,
            checksum,
            owner,
            acl,
            version_id,
            bucket_records_versions: records_versions,
        },
        body: Box::pin(iter(vec![Ok(Bytes::from(body))])) as BodyStream,
        served_range: served,
    })
}

/// Whether the bucket records versions (state enabled or suspended) — the
/// carrier's value for [`object::Info::bucket_records_versions`], read from
/// the shared state table inside the caller's own read transaction, so the
/// state and the row it describes are one point in time.
///
/// The feature gate is **inside** the body (the fs backend's shape): the
/// callers carry no cfg pair of their own. With the feature off no state
/// row can exist — the writes that set one are compiled out — so there is
/// nothing to read and the answer is the constant `false`.
#[cfg_attr(not(feature = "versioning"), allow(dead_code))]
pub(crate) fn records_versions(txn: &redb::ReadTransaction, bucket: &Name) -> Result<bool, Error> {
    #[cfg(feature = "versioning")]
    {
        Ok(bucket_versioning::Table::open_readonly(txn)?
            .get(bucket.as_ref().as_str())?
            .unwrap_or_default()
            .records_versions())
    }
    #[cfg(not(feature = "versioning"))]
    {
        let _ = (txn, bucket);
        Ok(false)
    }
}

/// What a completed versioned write recorded: the identity that became current
/// and whether the bucket records versions at all.
///
/// The caller — the multipart complete — reads exactly these two. The sequence
/// the slot moved to and the history bytes the publish stored are consumed
/// inside the core's own transaction (the slot columns and the meter), so the
/// core does not hand them back.
#[cfg(feature = "versioning")]
pub(crate) struct CompletedWrite {
    /// The identity the slot now records (`None` is the null version).
    pub version_id: Option<Id>,
    /// Whether the bucket records versions (state enabled or suspended).
    pub records_versions: bool,
}

/// The **in-transaction** bucket-row recheck, the first statement of every
/// raw read transaction: the retired table-level `check_bucket`'s probe, kept
/// where it makes the operation atomic rather than merely early.
/// `ensure_bucket` answers the no-race case; this probe is what a concurrent
/// teardown cannot slip past — the transaction sees the committed delete and
/// refuses, instead of answering `Absent`/`NoSuchVersion` for a bucket that
/// is gone. Only the versioned read closures use it.
#[cfg(feature = "versioning")]
pub(crate) fn check_bucket_read(txn: &redb::ReadTransaction, bucket: &Name) -> Result<(), Error> {
    if !store_bucket::Table::open_readonly(txn)?.exists(bucket.as_ref().as_str())? {
        return Err(no_such_bucket(bucket));
    }
    Ok(())
}

/// [`check_bucket_read`]'s write-transaction twin — the probe every raw write
/// closure opens with, where the guarantee is strictest: redb serializes
/// write transactions, so a bucket teardown that committed first is visible
/// here and the write aborts whole. Ungated: mem's part-stat plane probes
/// with it too.
pub(crate) fn check_bucket_write(txn: &mut WriteTransaction, bucket: &Name) -> Result<(), Error> {
    if !store_bucket::Table::open(txn)?.exists(bucket.as_ref().as_str())? {
        return Err(no_such_bucket(bucket));
    }
    Ok(())
}

/// The versioned implementation, compiled with the `versioning` feature.
#[cfg(feature = "versioning")]
mod enabled {
    use std::{
        cmp::{Reverse, min},
        collections::{BTreeMap, HashSet},
        mem,
        ops::Bound,
    };

    use async_trait::async_trait;
    use redb::{ReadableTable, WriteTransaction};

    use super::{check_bucket_read, check_bucket_write};
    use crate::{
        _core::{
            ByteRange, ETag,
            acl::Acl,
            blob::{BlobRef, BlockHash},
            bucket::Name,
            content::ContentKey,
            from_nanos,
            meta::{VersionMeta, object as object_row},
            object,
            storage::{VersionOps, common_prefix, marker_has_no_metadata},
            version::{
                self, CurrentVersion, HeadResult, Id, ListParams, Listing, Marker, ReadResult,
                State, wire_of,
            },
        },
        _store_redb::{
            bucket_versioning, object_meta, object_part, objects,
            scan::{drain_pair, for_each_pair},
            version as stored, version_index,
        },
        _version::{Current as Slot, Identity, Intent, Seqs, plan_write},
        DatabaseError, Error,
        error::{no_such_key, no_such_version},
        storage::MemoryStorage,
        version::{CompletedWrite, object_result, records_versions},
    };

    /// `(bucket, content-hash-hex)` → whole-blob bytes: the content-addressed
    /// store of superseded versions' content. mem has no block engine, so a
    /// version's content is one blob (the design's whole-blob v1 shape), and a
    /// rewrite is a dedup hit — the key is the hash, so the same content never
    /// lands twice.
    pub(crate) mod blobs {
        use redb::TableDefinition;

        use crate::_store_redb::table::{self, TableDef};

        /// The per-table marker: the table definition for the shared handle
        /// arms (opening, ensuring and the read/write split are
        /// `tinio_store_redb::table`'s; a mem-local table cannot add inherent
        /// methods to that handle, so this module's accessors are the
        /// `put_blob` / `get_blob` free functions beside it).
        pub enum Def {}

        impl TableDef for Def {
            type Key = (&'static str, &'static str);
            type Value = &'static [u8];

            const DEF: TableDefinition<'static, Self::Key, Self::Value> =
                TableDefinition::new("blobs");
        }

        /// Handle to the blob table (writable or read-only).
        pub type Table<'txn, T = redb::Table<'txn, (&'static str, &'static str), &'static [u8]>> =
            table::Table<'txn, Def, T>;
    }

    /// Store one blob under its content hash (content-addressed: a rewrite is
    /// a dedup hit, never a second copy). Answers the bytes the blob table
    /// **newly** holds — the blob's own length for a fresh insert, `0` for a
    /// dedup hit, whose bytes an earlier insert already metered (the caller
    /// folds the answer into the write's one byte delta).
    fn put_blob(
        txn: &mut WriteTransaction,
        b: &str,
        hash: &str,
        data: &[u8],
    ) -> Result<u64, Error> {
        let mut table = blobs::Table::open(txn)?;
        let replaced = table.insert((b, hash), data).map_err(DatabaseError::from)?;
        Ok(match replaced {
            Some(_) => 0, // already stored, and already metered
            None => data.len() as u64,
        })
    }

    /// One blob's bytes, read in a read transaction.
    fn get_blob(txn: &redb::ReadTransaction, b: &str, hash: &str) -> Result<Vec<u8>, Error> {
        Ok(blobs::Table::open_readonly(txn)?
            .get((b, hash))
            .map_err(DatabaseError::from)?
            .map(|guard| guard.value().to_vec())
            .unwrap_or_default())
    }

    /// The write-transaction twin of [`get_blob`].
    fn get_blob_mut(txn: &mut WriteTransaction, b: &str, hash: &str) -> Result<Vec<u8>, Error> {
        Ok(blobs::Table::open(txn)?
            .get((b, hash))
            .map_err(DatabaseError::from)?
            .map(|guard| guard.value().to_vec())
            .unwrap_or_default())
    }

    /// Release one version row's blob **when that row was the last one
    /// naming it**: the blob row is deleted and the bytes it held answered
    /// (the caller folds them into its one byte delta). `0` when the row
    /// holds no content (a delete marker), when the blob row is already
    /// gone, or when another row of the bucket still names the same content.
    ///
    /// The last-reference test is a walk of the bucket's version rows, and
    /// it is what makes the answer exact: mem is content-addressed per
    /// **bucket**, so another key's row can name the very blob this row
    /// named (one content, one blob row) and a key-local check would delete
    /// a blob a live version still reads. The walk is the price of that
    /// exactness; mem has no sweep, so the only other release path is the
    /// bucket teardown's drain.
    ///
    /// The caller must have removed `row` already — the walk cannot tell
    /// the dropped row from a live one, and a row's blob is by definition
    /// still named while the row is in the table.
    fn release_blob(txn: &mut WriteTransaction, b: &str, row: &stored::Row) -> Result<u64, Error> {
        if row.kind == stored::Kind::Marker {
            return Ok(0);
        }
        let blob = row.blob();
        let hash = blob.hash();
        let mut referenced = false;
        stored::Table::open(txn)?.for_bucket_of(b, |_, other| {
            if other.kind != stored::Kind::Marker && other.blob().hash() == hash {
                referenced = true;
            }
            Ok(())
        })?;
        if referenced {
            return Ok(0);
        }
        let hex = hash.to_hex();
        let mut table = blobs::Table::open(txn)?;
        let held = table
            .get((b, hex.as_str()))
            .map_err(DatabaseError::from)?
            .map(|guard| guard.value().len() as u64);
        let Some(held) = held else {
            return Ok(0); // already released (or never stored)
        };
        table
            .remove((b, hex.as_str()))
            .map_err(DatabaseError::from)?;
        Ok(held)
    }

    /// Delete every blob of `bucket`, answering the bytes they held (bucket
    /// teardown — the design's bucket-delete drops the bucket's
    /// content-addressed subtree; the caller releases the bytes, so the
    /// drain and the accounting cannot disagree).
    fn drain_blobs(txn: &mut WriteTransaction, b: &str) -> Result<u64, Error> {
        let held = {
            let table = blobs::Table::open(txn)?;
            let mut held = 0u64;
            for_each_pair(
                &*table,
                (b, ""),
                |bucket, _| bucket == b,
                |_, _, value| -> Result<(), Error> {
                    held += value.len() as u64;
                    Ok(())
                },
            )?;
            held
        };
        let mut table = blobs::Table::open(txn)?;
        drain_pair(&mut table, (b, ""), |bucket, _| bucket == b)?;
        Ok(held)
    }

    /// One key's raw rows, read out of a transaction.
    struct Peek {
        /// The `object_meta` row — the current version's metadata. Absent
        /// when the current version is a delete marker, or when the key has
        /// no current version.
        object_meta: Option<object_meta::Stored>,
        /// Whether the object row exists (the current object version's
        /// content; always written in the same transaction as its meta row).
        content: bool,
        /// The key's version rows, newest first.
        rows: Vec<stored::Row>,
    }

    impl Peek {
        /// The key's current version, or `None` when it has none: the meta
        /// row's object version, or the version row that is not history
        /// (`Row::is_history` — a current delete marker, carrying no index
        /// entry).
        fn current(&self) -> Option<Current> {
            if let Some(object_meta) = &self.object_meta
                && self.content
            {
                return Some(Current::Object(Box::new(object_meta.clone())));
            }
            self.rows
                .iter()
                .find(|row| !row.is_history(stored::Slot::Unknown))
                .map(|row| Current::Marker(Box::new(row.clone())))
        }

        /// The key's current slot, as the shared write-path decision takes
        /// it: the meta row's object version, the marker row stamped
        /// current, or nothing. mem's row and content are written in one
        /// transaction, so the gate never degrades a slot: `degraded` is
        /// always `false` here.
        fn slot(&self) -> Slot {
            match self.current() {
                Some(Current::Object(meta)) => Slot {
                    exists: true,
                    version: meta.version_id.clone(),
                    is_delete_marker: false,
                    seq: meta.version_seq,
                    degraded: false,
                },
                Some(Current::Marker(row)) => Slot {
                    exists: true,
                    version: row.id(),
                    is_delete_marker: true,
                    seq: row.seq,
                    degraded: false,
                },
                None => Slot::default(),
            }
        }

        /// The key's sequence view, as the shared write-path decision takes
        /// it: the newest row's sequence and whether a row occupies
        /// sequence 0 — read off the rows already in hand.
        fn seqs(&self) -> Seqs {
            Seqs {
                latest: self.rows.iter().map(|row| row.seq).max().unwrap_or(0),
                null_at_zero: self.rows.iter().any(|row| row.seq == 0),
            }
        }
    }

    /// The key's current version.
    enum Current {
        /// An object version: its content is the object row, its metadata
        /// and identity the meta row.
        Object(Box<object_meta::Stored>),
        /// A delete marker: a version row stamped current, holding no
        /// content.
        Marker(Box<stored::Row>),
    }

    /// One key's rows in a read transaction.
    fn peek(txn: &redb::ReadTransaction, bucket: &Name, key: &object::Key) -> Result<Peek, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let object_meta = object_meta::Table::open_readonly(txn)?.get(b, k)?;
        let content = objects::Table::open_readonly(txn)?.get(b, k)?.is_some();
        let mut rows = Vec::new();
        stored::Table::open_readonly(txn)?.for_key(b, k, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(Peek {
            object_meta,
            content,
            rows,
        })
    }

    /// One key's rows in the write transaction a mutation runs in.
    fn peek_mut(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Peek, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let object_meta = object_meta::Table::open(txn)?.get(b, k)?;
        let content = objects::Table::open(txn)?.get(b, k)?.is_some();
        let mut rows = Vec::new();
        stored::Table::open(txn)?.for_key(b, k, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(Peek {
            object_meta,
            content,
            rows,
        })
    }

    /// The version a request addresses, resolved over a key's rows.
    enum Found {
        /// The current object version (its content is the object row): the
        /// `object_meta` row the resolver judged matched, carried with the
        /// answer so a caller never re-reads it and never re-derives that the
        /// answer was the current slot.
        Current(Box<object_meta::Stored>),
        /// One version row — a noncurrent version, or the current marker.
        Row(Box<stored::Row>),
    }

    /// Resolve `version` (`None` = the null version) against `peek` and the
    /// version-index entry's sequence. The current slot is checked first: a
    /// current version — the meta row's object version, or the marker row
    /// stamped current — carries no index entry, and the null version's index
    /// entry may name a superseded version while a newer null version is
    /// current.
    fn resolve(peek: &Peek, index_seq: Option<u64>, version: Option<&Id>) -> Option<Found> {
        if let Some(object_meta) = &peek.object_meta
            && peek.content
            && match version {
                Some(id) => object_meta.version_id.as_ref() == Some(id),
                None => object_meta.version_id.is_none(),
            }
        {
            return Some(Found::Current(Box::new(object_meta.clone())));
        }
        let wire = wire_of(version);
        if let Some(row) = peek
            .rows
            .iter()
            .find(|row| !row.is_history(stored::Slot::Unknown) && row.id == wire)
        {
            return Some(Found::Row(Box::new(row.clone())));
        }
        let seq = index_seq?;
        peek.rows
            .iter()
            .find(|row| row.seq == seq && row.id == wire)
            .map(|row| Found::Row(Box::new(row.clone())))
    }

    /// The version a **metadata** operation addresses: the current slot or a
    /// content version row. A delete marker holds no tags and no ACL (the
    /// contract's rule — [`marker_has_no_metadata`]) and an identity that
    /// resolves to nothing is not found, so both answer the same not-found
    /// class; [`Self::locate`] is that collapse's one home, leaving each of
    /// the four metadata operations only its two real arms.
    enum Target {
        /// The current version, with the `object_meta` row the resolver
        /// judged.
        Current(Box<object_meta::Stored>),
        /// One content version row.
        Row(Box<stored::Row>),
    }

    /// Resolve one metadata operation's target: `resolve`'s answer narrowed
    /// to the two arms the metadata operations act on.
    fn locate(found: Option<Found>, version: Option<&Id>) -> Result<Target, Error> {
        match found {
            Some(Found::Current(stored)) => Ok(Target::Current(stored)),
            Some(Found::Row(row)) if row.kind == stored::Kind::Object => Ok(Target::Row(row)),
            _ => Err(marker_has_no_metadata(version)),
        }
    }

    /// The index sequence the request's identity resolves to, if any.
    fn index_seq(
        txn: &redb::ReadTransaction,
        bucket: &Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<u64>, Error> {
        let wire = wire_of(version);
        Ok(version_index::Table::open_readonly(txn)?.seq_of(
            bucket.as_ref().as_str(),
            key.as_ref().as_str(),
            &wire,
        )?)
    }

    /// The same lookup in a write transaction.
    fn index_seq_mut(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Option<u64>, Error> {
        let wire = wire_of(version);
        Ok(version_index::Table::open(txn)?.seq_of(
            bucket.as_ref().as_str(),
            key.as_ref().as_str(),
            &wire,
        )?)
    }

    /// A version row's content, read back out of the blob table. A row whose
    /// blob is missing is impossible while it is referenced (mem never
    /// reclaims a referenced blob), so the empty default is unreachable
    /// defence, never served content.
    fn blob(txn: &redb::ReadTransaction, b: &str, row: &stored::Row) -> Result<Vec<u8>, Error> {
        get_blob(txn, b, &row.blob().hash().to_hex())
    }

    /// The write-transaction twin of [`blob`].
    fn blob_mut(txn: &mut WriteTransaction, b: &str, row: &stored::Row) -> Result<Vec<u8>, Error> {
        get_blob_mut(txn, b, &row.blob().hash().to_hex())
    }

    /// Move the key's current version into history: an object's whole content
    /// goes into the blob table under its content hash, and its row plus its
    /// index entry are written in the caller's transaction — mem has no file
    /// to rename, so the row write *is* the whole move. A current delete
    /// marker holds no content: its row is re-stamped noncurrent. A key with
    /// no current version is a no-op.
    ///
    /// `converge_null` is the write plan's decision (`plan_write`'s
    /// `converge_null`): the row this supersede commits carries the null
    /// identity, so the one-null-row convergence runs. The caller passes the
    /// plan's answer rather than re-deriving it from the row.
    ///
    /// Answers the byte delta this move puts on the tracked total: the
    /// freshly stored blob's bytes (a dedup hit stores nothing — the earlier
    /// insert metered them), minus the bytes of a blob the convergence
    /// displaced and no longer referenced.
    fn supersede(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        peek: &Peek,
        now: u64,
        converge_null: bool,
    ) -> Result<i64, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let Some(current) = peek.current() else {
            return Ok(0);
        };
        let mut delta = 0i64;
        let row = match current {
            Current::Marker(row) => stored::Row {
                became_noncurrent: now,
                ..*row
            },
            Current::Object(meta) => {
                let data = {
                    let objects = objects::Table::open(txn)?;
                    let guard = objects.get(b, k)?.ok_or_else(|| no_such_key(key))?;
                    guard.value().to_vec()
                };
                // The content is content-addressed: the same bytes are one
                // blob, however many versions name them.
                let hash = BlockHash::of(&data);
                delta += put_blob(txn, b, &hash.to_hex(), &data)? as i64;
                // The five metadata wires ride the same one home the fs
                // supersede and the object-meta encoder use: the recorded
                // ETag, not a hash of the bytes, so a multipart-composed
                // `MD5-of-MD5s-N` value survives the supersede verbatim.
                let (etag, tags, checksum, owner, acl) = object_meta::wires(Some(&meta));
                stored::Row {
                    id: wire_of(meta.version_id.as_ref()),
                    blob: stored::encode_blob(&BlobRef::single(hash, data.len() as u64)),
                    size: data.len() as u64,
                    etag,
                    created: meta.mtime,
                    became_noncurrent: now,
                    kind: stored::Kind::Object,
                    tags,
                    checksum,
                    owner,
                    acl,
                    seq: meta.version_seq,
                }
            }
        };
        // The null-version convergence (AWS's one-null-version model): a key
        // holds at most one `null`-id row, so the index's single `null` entry
        // always names it and no older row is stranded. When this supersede
        // commits the null identity and an older null-id row already sits in
        // history, that row is replaced — removed with its index entry in
        // this same transaction — rather than a second null-id row being
        // added: the index is keyed by the identity and names only one of
        // them, so the older row would be permanently unaddressable (and
        // would stay so after the pointed-at row expired). The trigger is the
        // write plan's, never a second spelling of it.
        //
        // The stale entry is read BEFORE this supersede's own row and entry
        // are written (the entry it reads is the one the row it commits would
        // otherwise overwrite), but the removal runs after them: the
        // displaced row's blob is released only if nothing else — this
        // supersede's own row included — names that content.
        let stale = if converge_null {
            // The key holds at most one null-id row (the convergence rule),
            // so the index's single `null` entry names the row to displace —
            // one point read, never a walk of the key's whole history. An
            // entry naming this supersede's own sequence is the crash-window
            // adoption of its own row: nothing to displace.
            index_seq_mut(txn, bucket, key, None)?.filter(|stale| *stale != row.seq)
        } else {
            None
        };
        version_index::Table::open(txn)?.put(b, k, &row.id, row.seq)?;
        stored::Table::open(txn)?.put(b, k, &row)?;
        if let Some(stale) = stale {
            let displaced = stored::Table::open(txn)?.get(b, k, stale)?;
            stored::Table::open(txn)?.remove(b, k, stale)?;
            version_index::Table::open(txn)?.remove_pointing_at(b, k, &row.id, stale)?;
            if let Some(displaced) = displaced {
                delta -= release_blob(txn, b, &displaced)? as i64;
            }
        }
        Ok(delta)
    }

    /// Remove one version row and its index entry (idempotent — a sequence
    /// holding no row is `Ok`), answering the byte delta the removal puts on
    /// the tracked total: the row's blob is released (its byte count
    /// negated) when this row was the last one naming it.
    fn drop_row(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        row: &stored::Row,
    ) -> Result<i64, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        // The entry goes only when it points at this row: two rows can share
        // the null identity, and removing one must not retire the other's
        // entry.
        version_index::Table::open(txn)?.remove_pointing_at(b, k, &row.id, row.seq)?;
        stored::Table::open(txn)?.remove(b, k, row.seq)?;
        // The row is gone: its blob follows when nothing else names it.
        Ok(-(release_blob(txn, b, row)? as i64))
    }

    /// Remove a **current** version's row: a current version carries no index
    /// entry (its identity is the row itself), so only the row goes — an
    /// index entry a superseded version of the same null identity left behind
    /// is untouched.
    fn remove_current_row(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        seq: u64,
    ) -> Result<(), Error> {
        stored::Table::open(txn)?.remove(bucket.as_ref().as_str(), key.as_ref().as_str(), seq)?;
        Ok(())
    }

    /// Remove the current object version's rows (its content, metadata and
    /// retained part list) — the materialize's first half. A current delete
    /// marker has no such rows. Answers the byte delta: the content row's
    /// bytes leave the tracked total (negated).
    fn drop_current(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<i64, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let removed = {
            let mut objects = objects::Table::open(txn)?;
            let removed = objects
                .get(b, k)?
                .map(|guard| guard.value().len() as u64)
                .unwrap_or(0);
            objects.remove(b, k)?;
            removed
        };
        object_meta::Table::open(txn)?.remove(b, k)?;
        crate::_store_redb::object_part::Table::open(txn)?.remove_key(b, k)?;
        Ok(-(removed as i64))
    }

    /// Make the newest remaining version current again — the materialize half
    /// of the move, and the only path that turns history back into the
    /// current slot. An object version's content is copied out of the blob
    /// table into the object row with its metadata; a marker becomes the
    /// current marker in place. No version remains ⇒ the key is gone (the
    /// caller already removed the outgoing current rows).
    ///
    /// Answers the byte delta: the rebuilt object row's bytes enter the
    /// tracked total, and the blob they came from — the row's own last
    /// naming row, dropped below — leaves it when nothing else names that
    /// content (a key sharing the content keeps the blob, and then the bytes
    /// are genuinely held twice: once as the blob, once as the object row).
    fn materialize(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<i64, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let newest = { stored::Table::open(txn)?.latest(b, k)? };
        let Some((seq, row)) = newest else {
            return Ok(0);
        };
        if row.kind == stored::Kind::Marker {
            // The marker it hid becomes current: re-stamp its row in place,
            // and drop the index entry it carried in history — a current
            // version carries none, exactly as the object arm's `drop_row`
            // leaves none. The entry goes only when it points at this row.
            version_index::Table::open(txn)?.remove_pointing_at(b, k, &row.id, row.seq)?;
            stored::Table::open(txn)?.put(
                b,
                k,
                &stored::Row {
                    became_noncurrent: 0,
                    ..row
                },
            )?;
            return Ok(0);
        }
        let data = blob_mut(txn, b, &row)?;
        let size = data.len() as u64;
        objects::Table::open(txn)?.put(b, k, &data)?;
        // The version row's recorded ETag rides the rebuilt row: the
        // materialized version keeps the value it was written with, composed
        // form included.
        object_meta::Table::open(txn)?.put(b, k, &row.stored(seq, row.size, row.created, 0))?;
        // The bytes the rebuilt object row now holds, plus whatever the blob
        // release answers (its bytes are the row's own `size`; the copy above
        // is what replaces them).
        Ok(size as i64 + drop_row(txn, bucket, key, &row)?)
    }

    /// The publish hook of the object and multipart write paths: read the key's current
    /// rows inside the caller's transaction, move the outgoing version into
    /// history where the design says to, and answer the identity and the
    /// sequence the incoming version must record — `(None, 0)` for a write
    /// that records the null version, which covers both the unversioned bucket
    /// and the suspended bucket's replacement of its null version — together
    /// with the byte delta the move puts on the tracked total (the freshly
    /// stored blob's bytes; `0` when nothing was superseded or the content was
    /// a dedup hit) and whether the bucket records versions (the state read
    /// above, carried into the committed [`object::Info`] so the server never
    /// reads it again). The caller folds the delta into the write's own, one
    /// adjustment per transaction.
    ///
    /// The decision is the shared `tinio_version::plan_write`'s — one pure
    /// function both backends execute, so their write policies cannot drift;
    /// this function owns only mem's reads, its supersede and its row writes.
    /// The three arms are the spec's write paths: an enabled bucket always
    /// supersedes the outgoing version and mints a fresh identity; a suspended
    /// bucket replaces an existing null version in place (same identity, same
    /// sequence, nothing enters history) and supersedes anything else before
    /// writing a fresh null version — at sequence 0, the null version's own,
    /// unless a superseded null version's row already sits there, when the
    /// incoming null takes the key's next sequence so no row is overwritten;
    /// an unversioned bucket leaves the row exactly as it was.
    pub(crate) fn prepare_publish(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        now: u64,
    ) -> Result<(Option<Id>, u64, i64, bool), Error> {
        let state = {
            bucket_versioning::Table::open(txn)?
                .get(bucket.as_ref().as_str())?
                .unwrap_or_default()
        };
        let peek = peek_mut(txn, bucket, key)?;
        let plan = plan_write(Intent::Put, state, &peek.slot(), &peek.seqs());
        let mut delta = 0i64;
        if plan.supersede {
            delta = supersede(txn, bucket, key, &peek, now, plan.converge_null)?;
        }
        let id = match plan.identity {
            Identity::Fresh => Some(version::mint()),
            Identity::Null | Identity::Unversioned => None,
        };
        Ok((id, plan.seq, delta, state.records_versions()))
    }

    /// The **single-transaction** core of a versioned multipart completion: the
    /// assembled bytes, the object row (slot columns included), the retained
    /// part rows and the accounting are one write transaction, so a refusal
    /// anywhere in it leaves the history rows, the object row, the object bytes
    /// and the tracked total exactly as they were.
    ///
    /// It is the versioned put's transaction with the completion's part rows —
    /// the in-transaction bucket probe first, then the publish hook
    /// ([`prepare_publish`], the shared plan both backends execute), then the
    /// bytes, the row and the parts, and ONE accounting adjustment last.
    ///
    /// The slot columns land **with the row** (`version_id`/`version_seq`,
    /// `is_delete_marker: false`), exactly as the put path writes them. The
    /// contract's `advance_slot` derives the marker bit from the version row at
    /// the slot's sequence, and a completion's incoming version is the
    /// **current** one — a `Put` intent writes no version row, so the derived bit
    /// is `false`; its monotonic guard (a slot never moves backwards) cannot
    /// apply either, because the sequence is the one `prepare_publish` just
    /// chose. Reaching the same state through `put_with_parts` +
    /// `advance_slot` would split it across transactions for no gain.
    ///
    /// `credit` is the part bytes the caller is about to release: it widens the
    /// `max_total_bytes` check only (the reservation stays the exact delta), so
    /// a completion that assembles over the parts it consumes can land where
    /// the transient part-plus-object projection would not fit.
    ///
    /// The caller holds the accounting lock
    /// (`CountingContent::lock_accounting`) across this call and must not touch
    /// any locking accounting entry point while it runs — the lock is not
    /// reentrant. The bytes are this core's to write: a caller that also
    /// publishes them through the content plane would store and meter them
    /// twice, and `row.size` must be `data.len()`, the length the meter bills.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn complete_versioned(
        storage: &MemoryStorage,
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        now: u64,
        data: &[u8],
        row: &object_row::Row,
        parts: &[object_row::PartRow],
        credit: u64,
    ) -> Result<CompletedWrite, Error> {
        check_bucket_write(txn, bucket)?;
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let old_len = {
            let objects = objects::Table::open(txn)?;
            objects
                .get(b, k)?
                .map(|guard| guard.value().len() as u64)
                .unwrap_or(0)
        };
        let delta = data.len() as i64 - old_len as i64;
        // The publish hook runs BEFORE the bytes below become visible: what it
        // moves into history is the outgoing content, so the object row must
        // still hold it.
        let (version_id, version_seq, history_bytes, records_versions) =
            prepare_publish(txn, bucket, key, now)?;
        {
            let mut objects = objects::Table::open(txn)?;
            objects.put(b, k, data)?;
        }
        let stored = object_meta::Stored {
            version_id: version_id.clone(),
            version_seq,
            is_delete_marker: false,
            etag: row.etag.clone(),
            size: row.size,
            mtime: now,
            file_identity: 0,
            tags: row.tags.clone(),
            checksum: row.checksum.clone(),
            owner: row.owner.clone(),
            acl: row.acl.clone(),
        };
        {
            let mut table = object_meta::Table::open(txn)?;
            table.put(b, k, &stored)?;
        }
        {
            let mut table = object_part::Table::open(txn)?;
            table.remove_key(b, k)?;
            for part in parts {
                table.put(
                    b,
                    k,
                    &object_part::Stored {
                        part_number: part.number,
                        size: part.size,
                        checksum: part.checksum.clone(),
                    },
                )?;
            }
        }
        // The bytes this completion admits, in ONE adjustment after every
        // fallible row operation and inside the transaction: the object row's
        // own delta plus whatever the publish newly stored in the history's
        // blob table, credited with the parts the caller is about to release.
        // A limit breach aborts the whole transaction here, leaving the rows,
        // the bytes and the tracked total untouched.
        storage.adjust_total_with_credit_unlocked(delta + history_bytes, credit)?;
        Ok(CompletedWrite {
            version_id,
            records_versions,
        })
    }

    /// Repair the window an unversioned write leaves when the bucket begins
    /// recording versions between the write's dispatch read and its row landing
    /// (only a concurrent `PutBucketVersioning` can do that): the row is
    /// written, its bytes are published and metered, and the bucket's state now
    /// expects an identity the row does not carry.
    ///
    /// One transaction re-reads the state, takes the sequence the shared write
    /// plan would take for a put, mints the identity it would mint, and writes
    /// both into the row's slot columns. The content columns stay exactly as the
    /// write left them and **no byte is charged or refunded**: the metered total
    /// already covers them. No version row is written either — the current
    /// version *is* the object row (the resolver checks the slot first, and a
    /// current version carries no index entry), so a row here would make the
    /// listing read the same version twice.
    ///
    /// Residual: the content this write replaced inside the window is gone. The
    /// unversioned path overwrites in place instead of superseding into the
    /// history, so there is nothing to move into a version row; the repair makes
    /// the current content addressable and the state self-consistent, it does
    /// not resurrect the superseded one. Answers `None` when there is nothing
    /// to repair: the state flipped back before this ran, the row is gone, the
    /// row already carries an identity (another writer's or an earlier
    /// repair's — not this call's to claim), or the plan records the null
    /// version (a suspended bucket's row already is it).
    pub(crate) async fn record_current_as_version(
        storage: &MemoryStorage,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<Option<Id>, Error> {
        let b = bucket.as_ref().as_str().to_owned();
        let k = key.as_ref().as_str().to_owned();
        let bucket = bucket.clone();
        let key = key.clone();
        storage.db.write(move |txn| {
            // The in-transaction probe every raw write here opens with: the
            // repair must not identify a row for a bucket a teardown already
            // removed.
            check_bucket_write(txn, &bucket)?;
            let state = bucket_versioning::Table::open(txn)?
                .get(&b)?
                .unwrap_or_default();
            if !state.records_versions() {
                return Ok(None);
            }
            let Some(mut stored) = object_meta::Table::open(txn)?.get(&b, &k)? else {
                return Ok(None);
            };
            if stored.version_id.is_some() {
                // A versioned write (or an earlier repair) already gave the row
                // its identity — one this repair did not mint and must not
                // claim: a racing versioned writer's identity reported as the
                // unversioned write's own version would be a lie.
                return Ok(None);
            }
            let peek = peek_mut(txn, &bucket, &key)?;
            let plan = plan_write(Intent::Put, state, &peek.slot(), &peek.seqs());
            let id = match plan.identity {
                Identity::Fresh => Some(version::mint()),
                Identity::Null | Identity::Unversioned => None,
            };
            stored.version_id = id.clone();
            stored.version_seq = plan.seq;
            stored.is_delete_marker = false;
            object_meta::Table::open(txn)?.put(&b, &k, &stored)?;
            Ok(id)
        })
    }

    /// The simple `DELETE` on a versioned bucket: move the current version
    /// into history and make a delete marker the current version — a fresh
    /// identity on an enabled bucket, the null version on a suspended one
    /// (where the null version — an object version, or the null-identity
    /// marker a previous suspended delete made current — is removed rather
    /// than moved, so repeated deletes keep one marker and add no history
    /// row). The marker's row always takes a fresh sequence, and a current
    /// version carries no index entry: a marker can therefore neither land on
    /// a sequence a superseded version's row occupies nor displace the index
    /// entry that keeps a superseded null version addressable. Answers the
    /// marker that became current; an unversioned bucket answers
    /// [`Marker::none`] and writes nothing (today's delete). Answers the
    /// byte delta the move puts on the tracked total alongside it (the
    /// freshly stored blob's bytes; a delete that superseded nothing — a
    /// folder marker, an unversioned bucket, a key with no current version —
    /// answers `0`).
    pub(crate) fn insert_marker(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
        now: u64,
    ) -> Result<(Marker, i64), Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let state = {
            bucket_versioning::Table::open(txn)?
                .get(b)?
                .unwrap_or_default()
        };
        if state == State::Unversioned {
            return Ok((Marker::none(), 0));
        }
        let peek = peek_mut(txn, bucket, key)?;
        let plan = plan_write(Intent::Delete, state, &peek.slot(), &peek.seqs());
        let mut delta = 0i64;
        if plan.supersede {
            delta = supersede(txn, bucket, key, &peek, now, plan.converge_null)?;
        } else if let Some(Current::Marker(row)) = peek.current() {
            // A current delete marker is a row here, and the plan does not
            // move it into history — a suspended delete replaces the
            // null-identity marker rather than superseding it (a superseded
            // marker row would be a phantom history entry repeated deletes
            // must not grow): the incoming marker's row takes its place. A
            // current version carries no index entry, so only the row goes.
            remove_current_row(txn, bucket, key, row.seq)?;
        }
        let id = match plan.identity {
            Identity::Fresh => Some(version::mint()),
            Identity::Null | Identity::Unversioned => None,
        };
        // A fresh sequence — never the outgoing version's, and never the null
        // version's 0 — on a row the index does not name (a current version
        // carries no index entry). The plan resolved it.
        let seq = plan.seq;
        let row = stored::Row {
            id: wire_of(id.as_ref()),
            blob: stored::encode_blob(&BlobRef::empty()),
            size: 0,
            etag: String::new(),
            created: now,
            became_noncurrent: 0,
            kind: stored::Kind::Marker,
            tags: String::new(),
            checksum: String::new(),
            owner: String::new(),
            acl: String::new(),
            seq,
        };
        stored::Table::open(txn)?.put(b, k, &row)?;
        Ok((
            Marker {
                id,
                is_delete_marker: true,
            },
            delta,
        ))
    }

    /// Drop a whole bucket's version plane **inside the caller's own
    /// transaction**: the version rows and their index (a row and its index
    /// entry retire together), the state row and the blobs this bucket's
    /// history holds. Answers the drained blobs' bytes, so the caller releases
    /// exactly what this drain removed.
    ///
    /// The contract's `VersionMeta::drain_bucket` retires the rows and the
    /// index in a transaction of its own, and the state row and the blobs are
    /// not its to retire anyway (the state setter would write an
    /// `Unversioned` row back where the teardown retires the row, and
    /// `blobs` is mem-local) — so the bucket teardown elides the contract
    /// call and drains all four here, which is what makes the bucket row, the
    /// version plane and the object/upload planes one commit (ruling
    /// 2026-10-08, mem's P2 deviation).
    pub(crate) fn drain_bucket_in_txn(
        txn: &mut WriteTransaction,
        bucket: &Name,
    ) -> Result<u64, Error> {
        let b = bucket.as_ref().as_str();
        stored::Table::open(txn)?.drain_bucket(b)?;
        version_index::Table::open(txn)?.drain_bucket(b)?;
        bucket_versioning::Table::open(txn)?.drain_bucket(b)?;
        drain_blobs(txn, b)
    }

    /// One listed version, built from its row. The ETag is the row's own
    /// element — the value the version was written with, a multipart-composed
    /// `MD5-of-MD5s-N` form included — so a listing page reads no version's
    /// content back out of the blob table. A delete marker answers the
    /// empty-content ETag.
    fn entry_of(key: &object::Key, row: &stored::Row) -> version::Entry {
        let marker = row.kind == stored::Kind::Marker;
        version::Entry {
            key: key.clone(),
            id: row.id(),
            seq: row.seq,
            is_delete_marker: marker,
            size: row.size,
            last_modified: from_nanos(row.created),
            etag: if marker { ETag::EMPTY } else { row.etag() },
        }
    }

    /// The version listing's resume bound: the key the next window starts at,
    /// or the key it starts after. `After` is the stronger bound —
    /// `At(key)` includes the key, `After(key)` skips it whole.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Cursor {
        /// The next window reads this key first.
        At(String),
        /// The next window reads from the first key after this one.
        After(String),
    }

    impl Cursor {
        /// The bound a store range scan takes.
        fn as_bound(&self) -> Bound<&str> {
            match self {
                Cursor::At(key) => Bound::Included(key.as_str()),
                Cursor::After(key) => Bound::Excluded(key.as_str()),
            }
        }

        /// The bound's position: its key, and whether the key is already
        /// skipped. Ordering two positions orders the bounds — the later key
        /// wins, and at the same key the skipping bound is the later one.
        fn position(&self) -> (&str, bool) {
            match self {
                Cursor::At(key) => (key, false),
                Cursor::After(key) => (key, true),
            }
        }

        /// The later of two bounds.
        fn later(self, other: Option<Self>) -> Self {
            match other {
                Some(other) if other.position() > self.position() => other,
                _ => self,
            }
        }
    }

    /// The bound that lies past the delimiter group `rolled`: every key in
    /// `[rolled, bound)` is a member of that group, so a walk resumed at the
    /// bound has skipped the whole group. `None` when the prefix ends in the
    /// code point no successor exists for (`U+10FFFF`) — such a group is
    /// walked member by member (the members' keys are compared, never
    /// listed).
    fn group_end(rolled: &str) -> Option<Cursor> {
        let mut head = rolled.chars();
        let last = head.next_back()?;
        let next = char::from_u32(u32::from(last) + 1)?;
        let mut end = head.as_str().to_owned();
        end.push(next);
        Some(Cursor::At(end))
    }

    /// One key of a read window: its gated current-slot row, as the gating
    /// walk reports it (`None` = no row, or one the store reads as missing),
    /// and its version rows in the table's own `(key, seq)` order.
    struct WindowKey {
        /// The key, raw: the listing's own prefix and domain filters run on
        /// it.
        key: String,
        /// The current slot's gated row.
        slot: Option<object_meta::Stored>,
        /// The key's version rows, ascending sequence.
        rows: Vec<stored::Row>,
    }

    /// One read window: the keys both tables cover from the cursor, in key
    /// order, and the bound the next window starts at.
    struct Window {
        /// The window's keys, ascending.
        keys: Vec<WindowKey>,
        /// The next window's bound; `None` = both tables were read to the
        /// end of the bucket.
        next: Option<Cursor>,
    }

    /// Read one window: the gated current slots and the version rows of the
    /// first `budget` keys at or after `cursor`.
    ///
    /// mem's listing is the fs one's bounded walk with no filesystem under
    /// it: the two tables are keyed the same way and the window covers the
    /// same span, so a page costs a page here too (a `max_keys` page of a
    /// bucket with a million keys walks the page's keys, never the million).
    /// A window ends at a key **both** tables covered (see the fs walk's
    /// rule): the slot walk stops before the key that would make it
    /// `budget + 1` keys, the row walk before the key that would, and the
    /// window keeps the keys at or before the earlier of the two stops —
    /// never a key whose slot is known and whose rows are not. The stop is
    /// checked on a key boundary, so a key's version rows are read whole.
    fn read_window(
        txn: &redb::ReadTransaction,
        bucket: &str,
        cursor: &Cursor,
        budget: usize,
    ) -> Result<Window, Error> {
        let from = cursor.as_bound();
        let mut slots: BTreeMap<String, Option<object_meta::Stored>> = BTreeMap::new();
        {
            let table = object_meta::Table::open_readonly(txn)?;
            table.for_bucket_gated_from(bucket, from, |key, slot| {
                if slots.len() == budget {
                    return Ok(false);
                }
                slots.insert(key.as_ref().to_owned(), slot);
                Ok(true)
            })?;
        }
        let slots_end = slots.len() < budget;
        let mut history: BTreeMap<String, Vec<stored::Row>> = BTreeMap::new();
        {
            let table = stored::Table::open_readonly(txn)?;
            table.for_bucket_from(bucket, from, |key, row| match history.get_mut(key) {
                Some(rows) => {
                    rows.push(row);
                    Ok(true)
                }
                None => {
                    if history.len() == budget {
                        return Ok(false);
                    }
                    history.insert(key.to_owned(), vec![row]);
                    Ok(true)
                }
            })?;
        }
        let history_end = history.len() < budget;

        // The window's coverage: a walk that stopped early bounds it (the
        // other walked past that key), and two early stops meet at the
        // earlier key. Both walks reaching the bucket's end means nothing is
        // left to read.
        let through = match (slots_end, history_end) {
            (true, true) => None,
            (false, true) => slots.keys().next_back().cloned(),
            (true, false) => history.keys().next_back().cloned(),
            (false, false) => match (slots.keys().next_back(), history.keys().next_back()) {
                (Some(slot), Some(historic)) => Some(min(slot, historic).clone()),
                // Unreachable — a walk stops holding its budget's worth of
                // keys — and a window covering what it read is the safe
                // answer.
                (Some(key), None) | (None, Some(key)) => Some(key.clone()),
                (None, None) => None,
            },
        };
        if let Some(through) = &through {
            slots.retain(|key, _| key <= through);
            history.retain(|key, _| key <= through);
        }
        let mut keys: BTreeMap<String, WindowKey> = BTreeMap::new();
        for (key, slot) in slots {
            keys.insert(
                key.clone(),
                WindowKey {
                    key,
                    slot,
                    rows: Vec::new(),
                },
            );
        }
        for (key, rows) in history {
            keys.entry(key.clone())
                .or_insert_with(|| WindowKey {
                    key,
                    slot: None,
                    rows: Vec::new(),
                })
                .rows = rows;
        }
        Ok(Window {
            keys: keys.into_values().collect(),
            next: through.map(Cursor::After),
        })
    }

    /// The bounded page walk's state: the entries the page is cut from, the
    /// delimiter groups already fed to it, and the bound those groups pushed
    /// the walk past.
    struct Walk<'a> {
        /// The listing's parameters — the one source of the prefix, the
        /// delimiter and the markers.
        params: &'a ListParams,
        /// The prefix, materialized once (every key is compared against it).
        prefix: String,
        /// The rolled-up groups already fed to the page — the walk's own
        /// mirror of `version::paginate`'s roll-up dedup, so a group's
        /// remaining members are skipped.
        covered: HashSet<String>,
        /// The bound a rolled-up group pushed the walk past.
        jump: Option<Cursor>,
        /// The entries the page is cut from, in the listing's order.
        entries: Vec<version::Entry>,
    }

    impl<'a> Walk<'a> {
        /// Start a walk under `params` with `prefix` materialized.
        fn new(params: &'a ListParams, prefix: String) -> Self {
            Self {
                params,
                prefix,
                covered: HashSet::new(),
                jump: None,
                entries: Vec::new(),
            }
        }

        /// The first key the listing can contain: the later of the prefix (no
        /// listed key lies below it) and the `key_marker`. The marker's own
        /// key is **included** — a bare `key_marker` skips the key whole
        /// inside `version::paginate` (which needs no entry of it), and a
        /// `version_id_marker` resumes inside it (which needs its entries up
        /// to the named one).
        fn start_key(&self) -> String {
            match self.params.key_marker.as_deref() {
                Some(marker) if marker > self.prefix.as_str() => marker.to_owned(),
                _ => self.prefix.clone(),
            }
        }

        /// The bound the next window starts at: the window's own resume
        /// bound, pushed past the rolled-up groups already fed to the page.
        fn next_cursor(&mut self, window: &Window) -> Option<Cursor> {
            let jump = self.jump.take();
            window.next.clone().map(|next| next.later(jump))
        }

        /// Feed one window to the page: the keys that can contribute entries,
        /// in key order, and a rolled-up group marked in as soon as one of
        /// its keys yields — the group's remaining members are then skipped
        /// (and jumped over), because `version::paginate` drops them as
        /// duplicates of the group's prefix.
        fn absorb(&mut self, window: &Window) {
            let delimiter = self.params.delimiter.as_deref();
            for window_key in &window.keys {
                let raw = window_key.key.as_str();
                if !raw.starts_with(&self.prefix) {
                    continue; // outside the listing's prefix
                }
                let Ok(key) = object::key(raw) else {
                    continue; // a domain-invalid key is skipped, like a listing
                };
                if key.is_reserved() || key.is_folder_marker() {
                    continue;
                }
                if self.params.version_id_marker.is_none()
                    && self.params.key_marker.as_deref() == Some(raw)
                {
                    continue; // the whole key is behind the marker
                }
                let rolled =
                    delimiter.and_then(|delimiter| common_prefix(raw, &self.prefix, delimiter));
                if let Some(rolled) = rolled
                    && self.covered.contains(rolled)
                {
                    continue; // the group is already in the page
                }
                let entries = entries_of(&key, window_key.slot.as_ref(), &window_key.rows);
                if entries.is_empty() {
                    continue; // the key has no version to list
                }
                if let Some(rolled) = rolled {
                    // The exception is the key a `version_id_marker` resumes
                    // inside: the page may drop every entry of it (an
                    // unmatched marker), so the group is not counted in until
                    // a member the drop cannot reach has been read. One extra
                    // member's read is the whole cost, and its entries are
                    // duplicates the page drops anyway.
                    let resumed = self.params.version_id_marker.is_some()
                        && self.params.key_marker.as_deref() == Some(raw);
                    if !resumed {
                        self.covered.insert(rolled.to_owned());
                        self.jump = match (self.jump.take(), group_end(rolled)) {
                            (Some(jump), Some(end)) => Some(jump.later(Some(end))),
                            (None, Some(end)) => Some(end),
                            (jump, None) => jump,
                        };
                    }
                }
                self.entries.extend(entries);
            }
        }
    }

    /// The entries one key contributes, in the listing's order: its current
    /// version — the meta row's own object version, or the one version row
    /// that is not history (a current delete marker has no meta row, so its
    /// row is the slot) — and then its noncurrent versions newest first.
    ///
    /// mem's slot is the row itself: a key whose meta row is gone has no
    /// current version, so every version row is history and must list (the
    /// row an aborted publish left unstamped included — a version like any
    /// other, never a key's second "latest").
    fn entries_of(
        key: &object::Key,
        slot: Option<&object_meta::Stored>,
        rows: &[stored::Row],
    ) -> Vec<version::Entry> {
        let mut entries: Vec<version::Entry> = Vec::new();
        match slot {
            Some(stored) => entries.push(version::Entry {
                key: key.clone(),
                id: stored.version_id.clone(),
                seq: stored.version_seq,
                is_delete_marker: false,
                size: stored.size,
                last_modified: from_nanos(stored.mtime),
                etag: stored.etag.clone(),
            }),
            None => {
                if let Some(row) = rows
                    .iter()
                    .find(|row| !row.is_history(stored::Slot::Unknown))
                {
                    entries.push(entry_of(key, row));
                }
            }
        }
        let mut older: Vec<&stored::Row> = rows
            .iter()
            .filter(|row| row.is_history(stored::Slot::Unknown))
            .collect();
        older.sort_by_key(|row| Reverse(row.seq));
        for row in older {
            entries.push(entry_of(key, row));
        }
        entries
    }

    /// The bucket-existence guard every version operation answers first — the
    /// method every path shares ([MemoryStorage::ensure_bucket]). It answers the
    /// no-race case and the error order; the atomic half is the in-transaction
    /// [`check_bucket_read`]/[`check_bucket_write`] probe each raw closure
    /// below opens with — or, for the state write, the contract's own guarded
    /// variant — because this read is a transaction of its own and a teardown
    /// can commit between it and the operation's own transaction.
    async fn ensure_bucket(storage: &MemoryStorage, bucket: &Name) -> Result<(), Error> {
        storage.ensure_bucket(bucket).await
    }

    #[async_trait]
    impl VersionOps for MemoryStorage {
        async fn bucket_versioning(&self, bucket: &Name) -> Result<State, Error> {
            ensure_bucket(self, bucket).await?;
            // The state row is `VersionMeta`'s (the bucket's versioning state
            // is the version contract's bucket half, D26): a row-less bucket
            // answers `State::Unversioned`, the contract's own default.
            Ok(self.metadata().bucket_state(bucket).await?)
        }

        async fn set_bucket_versioning(&self, bucket: &Name, state: State) -> Result<(), Error> {
            // The outer probe answers only the error order: the flip is the
            // contract's guarded write, whose own transaction re-reads the
            // bucket row, so a teardown committing inside this window cannot
            // leave a state row behind for a bucket that is gone.
            ensure_bucket(self, bucket).await?;
            self.metadata()
                .set_bucket_state_guarded(bucket, state)
                .await?;
            Ok(())
        }

        async fn get_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            range: Option<ByteRange>,
        ) -> Result<ReadResult, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| {
                check_bucket_read(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let records = records_versions(txn, bucket)?;
                let peek = peek(txn, bucket, key)?;
                let index = index_seq(txn, bucket, key, version)?;
                let Some(found) = resolve(&peek, index, version) else {
                    return Err(no_such_version(wire_of(version)));
                };
                match found {
                    Found::Current(stored) => {
                        let data = {
                            let objects = objects::Table::open_readonly(txn)?;
                            let guard = objects
                                .get(bucket.as_ref().as_str(), key.as_ref().as_str())?
                                .ok_or_else(|| no_such_key(key))?;
                            guard.value().to_vec()
                        };
                        let served = range.map(|r| r.resolve(stored.size)).transpose()?;
                        Ok(ReadResult::Object(object_result(
                            key,
                            stored.size,
                            from_nanos(stored.mtime),
                            stored.etag.clone(),
                            stored.tags.clone(),
                            stored.checksum.clone(),
                            stored.owner.clone(),
                            stored.acl.clone(),
                            stored.version_id.clone(),
                            records,
                            &data,
                            served,
                        )?))
                    }
                    Found::Row(row) if row.kind == stored::Kind::Marker => Ok(ReadResult::Marker {
                        id: row.id(),
                        last_modified: from_nanos(row.created),
                    }),
                    Found::Row(row) => {
                        let data = blob(txn, bucket.as_ref().as_str(), &row)?;
                        let served = range.map(|r| r.resolve(row.size)).transpose()?;
                        Ok(ReadResult::Object(object_result(
                            key,
                            row.size,
                            from_nanos(row.created),
                            row.etag(),
                            row.tags(),
                            row.checksum(),
                            row.owner(),
                            row.acl(),
                            row.id(),
                            records,
                            &data,
                            served,
                        )?))
                    }
                }
            })
        }

        async fn head_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<HeadResult, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| {
                check_bucket_read(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let records = records_versions(txn, bucket)?;
                let peek = peek(txn, bucket, key)?;
                let index = index_seq(txn, bucket, key, version)?;
                let Some(found) = resolve(&peek, index, version) else {
                    return Err(no_such_version(wire_of(version)));
                };
                match found {
                    Found::Current(stored) => Ok(HeadResult::Object(object::Info {
                        key: key.clone(),
                        size: stored.size,
                        last_modified: from_nanos(stored.mtime),
                        etag: stored.etag.clone(),
                        tags: stored.tags.clone(),
                        checksum: stored.checksum.clone(),
                        owner: stored.owner.clone(),
                        acl: stored.acl.clone(),
                        version_id: stored.version_id.clone(),
                        bucket_records_versions: records,
                    })),
                    Found::Row(row) if row.kind == stored::Kind::Marker => Ok(HeadResult::Marker {
                        id: row.id(),
                        last_modified: from_nanos(row.created),
                    }),
                    Found::Row(row) => Ok(HeadResult::Object(row.info(key, records))),
                }
            })
        }

        async fn delete_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Marker, Error> {
            ensure_bucket(self, bucket).await?;
            // The accounting lock is taken BEFORE the transaction and held
            // across it: the release below runs inside the write, and the
            // decorator's lock is not reentrant — the closure may only use the
            // `*_unlocked` entry point.
            let guard = self.lock_accounting().await;
            let marker = self.db.write(|txn| -> Result<Marker, Error> {
                check_bucket_write(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let peek = peek_mut(txn, bucket, key)?;
                let index = index_seq_mut(txn, bucket, key, version)?;
                let Some(found) = resolve(&peek, index, version) else {
                    return Err(no_such_version(wire_of(version)));
                };
                let (marker, delta) = match found {
                    Found::Current(stored) => {
                        // The current object version is permanently removed:
                        // the newest remaining version becomes current again,
                        // or the key disappears with it.
                        let mut delta = drop_current(txn, bucket, key)?;
                        delta += materialize(txn, bucket, key)?;
                        (
                            Marker {
                                id: stored.version_id,
                                is_delete_marker: false,
                            },
                            delta,
                        )
                    }
                    Found::Row(row) if !row.is_history(stored::Slot::Unknown) => {
                        // The current delete marker: removing it shows the
                        // version it hid. Its row is the only record it has —
                        // a current version carries no index entry — so the
                        // row goes without the index.
                        remove_current_row(txn, bucket, key, row.seq)?;
                        let delta = materialize(txn, bucket, key)?;
                        (
                            Marker {
                                id: row.id(),
                                is_delete_marker: true,
                            },
                            delta,
                        )
                    }
                    Found::Row(row) => {
                        let delta = drop_row(txn, bucket, key, &row)?;
                        (
                            Marker {
                                id: row.id(),
                                is_delete_marker: row.kind == stored::Kind::Marker,
                            },
                            delta,
                        )
                    }
                };
                // The bytes this delete moves, applied once: the dropped
                // current version's rows leave the tracked total, a
                // materialize's rebuilt object row enters it, and a blob
                // whose last naming row this delete dropped leaves it. It
                // goes through `release_total`, never `adjust_total`: a
                // delete must never fail for capacity. The net is at most a
                // re-labelling (content counted as a blob becomes content
                // counted as an object row) — it can only grow while another
                // key still shares the materialized content, when the backend
                // really does hold the bytes twice.
                self.release_total_unlocked(delta);
                Ok(marker)
            })?;
            // The key's bytes left, or a restored version's took their place:
            // either way the key's recorded publish is not the one it holds
            // any more, so no stale compensation may claim it.
            self.content()
                .forget_publish_tag(bucket, &ContentKey::Object(key.clone()));
            drop(guard);
            Ok(marker)
        }

        /// The bounded per-key probe behind the plain read's 404
        /// disclosure: the `object_meta` row is the current object slot;
        /// with no row, the key's newest version row decides — a row no
        /// stamp made history is current, and its kind says marker or
        /// object (the listing's own "what is current" rule). One
        /// exact-key range (`Table::newest`), never a bucket walk.
        ///
        /// The one-row read is the deliberate difference from the
        /// listing's spelling of that rule: `Peek::current`/`entries_of`
        /// scan the rows they already hold for the newest one that is not
        /// history (`.find(!is_history)` over the newest-first window),
        /// while this probe judges the single newest row. The two agree
        /// in every state the model reaches — a row is stamped only when
        /// a newer version supersedes it, and the restore that revives a
        /// version resets its stamp on the newest row it revives, so any
        /// non-history row is the newest row and "the newest row is
        /// history" means "no row is current". If that stamping
        /// invariant ever weakens, the probe must read on to the newest
        /// non-history row rather than answering `Absent` off the top
        /// row; reading on is also why the listing's predicate is not
        /// reused here — it needs every row between the top and the
        /// answer, and this probe exists to decode exactly one.
        async fn current_version(
            &self,
            bucket: &Name,
            key: &object::Key,
        ) -> Result<CurrentVersion, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| -> Result<CurrentVersion, Error> {
                check_bucket_read(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Ok(CurrentVersion::Absent);
                }
                let object_meta = object_meta::Table::open_readonly(txn)?
                    .get(bucket.as_ref().as_str(), key.as_ref().as_str())?;
                if object_meta.is_some() {
                    return Ok(CurrentVersion::Object);
                }
                let newest = stored::Table::open_readonly(txn)?.newest(
                    bucket.as_ref().as_str(),
                    key.as_ref().as_str(),
                    1,
                )?;
                Ok(match newest.into_iter().next() {
                    Some((_, row)) if !row.is_history(stored::Slot::Unknown) => {
                        if row.kind == stored::Kind::Marker {
                            CurrentVersion::Marker
                        } else {
                            CurrentVersion::Object
                        }
                    }
                    _ => CurrentVersion::Absent,
                })
            })
        }

        async fn list_object_versions(
            &self,
            bucket: &Name,
            params: ListParams,
        ) -> Result<Listing, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| -> Result<Listing, Error> {
                check_bucket_read(txn, bucket)?;
                let b = bucket.as_ref().as_str();
                // A page cut to nothing asks for nothing: `max_keys = 0`
                // answers the empty page (`version::paginate`'s own rule)
                // without walking the bucket for a truncation that can never
                // come.
                if params.max_keys == 0 {
                    return Ok(version::paginate(&[], &params));
                }
                // The bounded walk, the fs listing's own shape: one window of
                // keys at a time, from the listing's first key, stopping as
                // soon as the page engine can see the page's cut. Both
                // backends answer one conformance leg, so the two walks must
                // stay each other's mirror.
                let prefix = params.prefix.clone().unwrap_or_default();
                let budget = params.max_keys.saturating_add(1);
                let mut walk = Walk::new(&params, prefix);
                let mut cursor = Cursor::At(walk.start_key());
                loop {
                    let window = read_window(txn, b, &cursor, budget)?;
                    walk.absorb(&window);
                    // The page is decided by the engine that has always cut
                    // it: its counted items never exceed the entries it is
                    // handed, so the call is only worth making once the walk
                    // holds that many — and a truncated answer already carries
                    // the page's cut and its cursors, so the walk stops there.
                    if walk.entries.len() >= params.max_keys {
                        let listing = version::paginate(&walk.entries, &params);
                        if listing.is_truncated {
                            return Ok(listing);
                        }
                    }
                    match walk.next_cursor(&window) {
                        Some(next) => cursor = next,
                        None => {
                            let entries = mem::take(&mut walk.entries);
                            return Ok(version::paginate(&entries, &params));
                        }
                    }
                }
            })
        }

        async fn version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<object::Tags, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| {
                check_bucket_read(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let peek = peek(txn, bucket, key)?;
                let index = index_seq(txn, bucket, key, version)?;
                match locate(resolve(&peek, index, version), version)? {
                    Target::Current(stored) => Ok(stored.tags),
                    Target::Row(row) => Ok(row.tags()),
                }
            })
        }

        async fn set_version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            tags: object::Tags,
        ) -> Result<(), Error> {
            ensure_bucket(self, bucket).await?;
            self.db.write(|txn| -> Result<(), Error> {
                check_bucket_write(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let b = bucket.as_ref().as_str();
                let k = key.as_ref().as_str();
                let peek = peek_mut(txn, bucket, key)?;
                let index = index_seq_mut(txn, bucket, key, version)?;
                match locate(resolve(&peek, index, version), version)? {
                    Target::Current(_) => {
                        object_meta::Table::open(txn)?
                            .put_tags(b, k, &tags)?
                            .ok_or_else(|| no_such_key(key))?;
                        Ok(())
                    }
                    Target::Row(row) => {
                        stored::Table::open(txn)?.put(
                            b,
                            k,
                            &stored::Row {
                                tags: tags.to_wire(),
                                ..*row
                            },
                        )?;
                        Ok(())
                    }
                }
            })
        }

        async fn version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Acl, Error> {
            ensure_bucket(self, bucket).await?;
            self.db.read(|txn| {
                check_bucket_read(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let peek = peek(txn, bucket, key)?;
                let index = index_seq(txn, bucket, key, version)?;
                match locate(resolve(&peek, index, version), version)? {
                    // The owner element rides the row beside the grants (the
                    // shared decode-home rule), so both are recombined here.
                    Target::Current(stored) => Ok(Acl {
                        owner: stored.owner.clone(),
                        grants: stored.acl.grants.clone(),
                    }),
                    Target::Row(row) => Ok(Acl {
                        owner: row.owner(),
                        grants: row.acl().grants,
                    }),
                }
            })
        }

        async fn set_version_acl(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            acl: &Acl,
        ) -> Result<(), Error> {
            ensure_bucket(self, bucket).await?;
            self.db.write(|txn| -> Result<(), Error> {
                check_bucket_write(txn, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let b = bucket.as_ref().as_str();
                let k = key.as_ref().as_str();
                let peek = peek_mut(txn, bucket, key)?;
                let index = index_seq_mut(txn, bucket, key, version)?;
                match locate(resolve(&peek, index, version), version)? {
                    // Replace-all on the grants, owner preserved — the object
                    // ACL put's contract ruling, per version.
                    Target::Current(stored) => {
                        let fresh = Acl {
                            owner: None,
                            grants: acl.grants.clone(),
                        };
                        object_meta::Table::open(txn)?.put(
                            b,
                            k,
                            &object_meta::Stored {
                                acl: fresh,
                                ..*stored
                            },
                        )?;
                        Ok(())
                    }
                    Target::Row(row) => {
                        stored::Table::open(txn)?.put(
                            b,
                            k,
                            &stored::Row {
                                acl: acl.to_grants_wire(),
                                ..*row
                            },
                        )?;
                        Ok(())
                    }
                }
            })
        }
    }
}

#[cfg(feature = "versioning")]
pub(crate) use self::enabled::{
    blobs, complete_versioned, drain_bucket_in_txn, insert_marker, prepare_publish,
    record_current_as_version,
};

#[cfg(test)]
mod tests {
    // The backend journeys the fs half mirrors: overwrite keeps the previous
    // version readable, a suspended write replaces the null version without
    // history, deleting the last version removes the key, and a simple delete
    // inserts a marker. The conformance leg is the real oracle.
    #[cfg(feature = "versioning")]
    mod versioned {
        use std::{
            process,
            sync::{Arc, mpsc},
            thread,
            time::Duration,
        };

        use crate::{
            _core::{
                BucketOps, ByteRange, ETag, ObjectOps,
                acl::Acl,
                bucket, from_nanos,
                meta::object as object_row,
                now_nanos, object,
                storage::{self, VersionOps},
                version::{self, HeadResult, ListParams, ReadResult, State},
            },
            _store_redb::{
                bucket as redb_bucket, bucket_versioning, scan::for_each_pair, version as stored,
            },
            _util::testing::{body, read_body},
            Error, MemoryOptions, MemoryStorage,
            version::{
                CompletedWrite, check_bucket_read, check_bucket_write, enabled::complete_versioned,
                record_current_as_version,
            },
        };

        async fn with_bucket() -> (MemoryStorage, bucket::Name) {
            let storage = MemoryStorage::new().unwrap();
            let name = bucket::name("data").unwrap();
            storage
                .create_bucket(&name, None, &Acl::default_private(None))
                .await
                .unwrap();
            (storage, name)
        }

        fn params() -> ListParams {
            ListParams {
                max_keys: 1000,
                ..Default::default()
            }
        }

        /// The bytes the bucket's `blobs` table holds — the history's own
        /// store, read straight (the tracked total is asserted beside it).
        fn blob_bytes(storage: &MemoryStorage, b: &bucket::Name) -> u64 {
            let bucket = b.as_ref().as_str().to_owned();
            storage
                .db
                .read(|txn| -> Result<u64, Error> {
                    let table = crate::version::blobs::Table::open_readonly(txn)?;
                    let mut total = 0u64;
                    for_each_pair(
                        &*table,
                        (bucket.as_str(), ""),
                        |row_bucket, _| row_bucket == bucket,
                        |_, _, value| -> Result<(), Error> {
                            total += value.len() as u64;
                            Ok(())
                        },
                    )?;
                    Ok(total)
                })
                .unwrap()
        }

        /// The id of `key`'s current version, read off the listing (the
        /// current version leads its key's entries).
        async fn current_id(
            storage: &MemoryStorage,
            b: &bucket::Name,
            key: &object::Key,
        ) -> version::Id {
            storage
                .list_object_versions(b, params())
                .await
                .unwrap()
                .entries
                .into_iter()
                .find(|entry| entry.key.as_ref() == key.as_ref())
                .and_then(|entry| entry.id)
                .expect("an enabled write mints an identity")
        }

        #[tokio::test]
        async fn the_version_history_counts_against_the_total_limit() {
            // The cap covers the history beside the live content: the bytes
            // an overwrite moves into the blob table are metered, so the
            // write that would exceed the cap is refused exactly as a plain
            // oversized write is — and the refusal is the whole transaction's
            // (no object row, no history row, nothing metered).
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(10),
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
            let key = object::key("a.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"first!".to_vec()))
                .await
                .unwrap();
            let first = current_id(&storage, &b, &key).await;
            assert_eq!(storage.total_bytes(), 6);

            // The overwrite would keep the outgoing 6 bytes as history
            // beside the incoming 6: 12 > 10, refused.
            let err = storage
                .put_object(&b, &key, body(b"second".to_vec()))
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::Storage(storage::Error::EntityTooLarge { .. })),
                "{err}"
            );
            assert_eq!(storage.total_bytes(), 6, "a refused write meters nothing");
            assert_eq!(
                read_body(storage.get_object(&b, &key, None).await.unwrap().body)
                    .await
                    .unwrap(),
                b"first!",
                "the refused overwrite left the current version alone"
            );
            assert_eq!(
                storage
                    .list_object_versions(&b, params())
                    .await
                    .unwrap()
                    .entries
                    .len(),
                1,
                "…and left no history row"
            );

            // The write that fits pays for the history too: the superseded 6
            // bytes plus the incoming 4 are exactly the 10-byte cap.
            storage
                .put_object(&b, &key, body(b"next".to_vec()))
                .await
                .unwrap();
            assert_eq!(
                storage.total_bytes(),
                10,
                "the history's bytes are counted against the cap"
            );
            let ReadResult::Object(old) = storage
                .get_object_version(&b, &key, Some(&first), None)
                .await
                .unwrap()
            else {
                panic!("the superseded version must be an object version");
            };
            assert_eq!(
                read_body(old.body).await.unwrap(),
                b"first!",
                "the bytes the total pays for are really there"
            );
        }

        #[tokio::test]
        async fn deleting_the_last_version_naming_a_blob_releases_its_bytes() {
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(10),
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
            let key = object::key("a.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"one".to_vec()))
                .await
                .unwrap();
            let one = current_id(&storage, &b, &key).await;
            storage
                .put_object(&b, &key, body(b"second".to_vec()))
                .await
                .unwrap();
            assert_eq!(
                blob_bytes(&storage, &b),
                3,
                "the superseded version's bytes are the history"
            );
            assert_eq!(storage.total_bytes(), 9, "the object row plus the blob");

            // The version delete drops the row and — nothing else naming
            // that blob — the blob itself: both its bytes and the capacity
            // they held go.
            storage
                .delete_object_version(&b, &key, Some(&one))
                .await
                .unwrap();
            assert_eq!(
                blob_bytes(&storage, &b),
                0,
                "the last naming row's blob is released"
            );
            assert_eq!(storage.total_bytes(), 6);
            storage
                .put_object(&b, &key, body(b"four".to_vec()))
                .await
                .unwrap();
            assert_eq!(
                storage.total_bytes(),
                10,
                "the released bytes are writable again"
            );
        }

        #[tokio::test]
        async fn a_blob_two_keys_share_is_released_by_its_last_naming_row() {
            // mem is content-addressed per bucket: two keys' superseded
            // copies of one content share ONE blob row, so the bytes are
            // metered once (the second supersede is a dedup hit, storing and
            // counting nothing) and released exactly once — by whichever
            // key's row is the last to go.
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(1024),
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
            let a = object::key("a.txt").unwrap();
            let c = object::key("b.txt").unwrap();
            let shared = b"shared content";
            storage
                .put_object(&b, &a, body(shared.to_vec()))
                .await
                .unwrap();
            let a_old = current_id(&storage, &b, &a).await;
            storage
                .put_object(&b, &a, body(b"x".to_vec()))
                .await
                .unwrap();
            assert_eq!(blob_bytes(&storage, &b), shared.len() as u64);
            storage
                .put_object(&b, &c, body(shared.to_vec()))
                .await
                .unwrap();
            let c_old = current_id(&storage, &b, &c).await;
            storage
                .put_object(&b, &c, body(b"y".to_vec()))
                .await
                .unwrap();
            assert_eq!(
                blob_bytes(&storage, &b),
                shared.len() as u64,
                "one content, one blob row"
            );
            let held = storage.total_bytes();
            assert_eq!(
                held,
                2 + shared.len() as u64,
                "the shared content is metered once, not once per key"
            );

            // The first key's version row goes, but the second key's row
            // still names the blob: nothing is released.
            storage
                .delete_object_version(&b, &a, Some(&a_old))
                .await
                .unwrap();
            assert_eq!(
                blob_bytes(&storage, &b),
                shared.len() as u64,
                "a live naming row keeps the blob"
            );
            assert_eq!(storage.total_bytes(), held);

            // The last naming row goes: the blob and its bytes follow.
            storage
                .delete_object_version(&b, &c, Some(&c_old))
                .await
                .unwrap();
            assert_eq!(
                blob_bytes(&storage, &b),
                0,
                "the last naming row releases the blob"
            );
            assert_eq!(storage.total_bytes(), 2, "only the two object rows remain");
        }

        #[tokio::test]
        async fn the_memory_backend_keeps_the_previous_version_readable() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("a.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"first".to_vec()))
                .await
                .unwrap();
            let first_id = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries[0]
                .id
                .clone()
                .expect("an enabled write mints an identity");
            storage
                .put_object(&b, &key, body(b"second".to_vec()))
                .await
                .unwrap();

            // The current read serves the new content and names the new
            // version; the previous version keeps its own bytes by its id.
            let current = storage.get_object(&b, &key, None).await.unwrap();
            assert_eq!(read_body(current.body).await.unwrap(), b"second");
            assert_ne!(current.info.version_id.as_ref(), Some(&first_id));
            let ReadResult::Object(old) = storage
                .get_object_version(&b, &key, Some(&first_id), None)
                .await
                .unwrap()
            else {
                panic!("the superseded version must be an object version");
            };
            assert_eq!(read_body(old.body).await.unwrap(), b"first");
            assert_eq!(old.info.version_id.as_ref(), Some(&first_id));

            // Both are listed, newest first.
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            let ids: Vec<Option<version::Id>> =
                page.entries.iter().map(|entry| entry.id.clone()).collect();
            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], current.info.version_id.clone());
            assert_eq!(ids[1], Some(first_id));
        }

        #[tokio::test]
        async fn a_composed_multipart_etag_survives_history_the_reads_and_materialize() {
            // The discriminating leg for the row's `etag` element: a
            // multipart-composed `MD5-of-MD5s-N` value is not the content
            // MD5, so every path that recomputes the ETag from the stored
            // bytes answers a different (wrong) value. The row carries the
            // recorded form, and the reads serve it verbatim.
            use crate::_core::{CompletedPart, ETag, MultipartOps};

            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("big.bin").unwrap();
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
                    body(b"first".to_vec()),
                    None,
                )
                .await
                .unwrap();
            let completed = storage
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
            let composed = completed.etag.clone();
            assert!(
                matches!(composed, ETag::Composed(..)) && composed != ETag::from_content(b"first"),
                "the completion's ETag is the composed form, never the content MD5: {composed}"
            );
            let composed_id = completed
                .version_id
                .clone()
                .expect("an enabled completion mints an identity");

            // A plain overwrite supersedes the composed version into history.
            storage
                .put_object(&b, &key, body(b"second".to_vec()))
                .await
                .unwrap();

            // The version-addressed read and the listing both answer the
            // row's recorded ETag; a content rehash would answer the plain
            // content MD5 and fail both.
            let ReadResult::Object(old) = storage
                .get_object_version(&b, &key, Some(&composed_id), None)
                .await
                .unwrap()
            else {
                panic!("the superseded version must be an object version");
            };
            assert_eq!(
                old.info.etag, composed,
                "the version read serves the row's own ETag element"
            );
            let HeadResult::Object(head) = storage
                .head_object_version(&b, &key, Some(&composed_id))
                .await
                .unwrap()
            else {
                panic!("the superseded version must head as an object");
            };
            assert_eq!(head.etag, composed, "the head agrees with the get");
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            let listed = page
                .entries
                .iter()
                .find(|entry| entry.id.as_ref() == Some(&composed_id))
                .expect("the composed version must list");
            assert_eq!(
                listed.etag, composed,
                "the listing entry serves the row's own ETag element"
            );

            // Deleting the current version materializes the composed one back
            // onto the key path: the restored current version keeps the
            // composed form too.
            let fresh_id = storage
                .head_object(&b, &key)
                .await
                .unwrap()
                .version_id
                .expect("the overwrite minted an identity");
            storage
                .delete_object_version(&b, &key, Some(&fresh_id))
                .await
                .unwrap();
            let restored = storage.get_object(&b, &key, None).await.unwrap();
            assert_eq!(
                restored.info.etag, composed,
                "the materialized current version keeps the composed ETag"
            );
            assert_eq!(restored.info.version_id.as_ref(), Some(&composed_id));
            assert_eq!(read_body(restored.body).await.unwrap(), b"first");
        }

        #[tokio::test]
        async fn a_suspended_write_replacing_the_null_version_creates_no_history() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Suspended)
                .await
                .unwrap();
            let key = object::key("s.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"one".to_vec()))
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"two".to_vec()))
                .await
                .unwrap();

            // The Suspended arm's in-place rule, in isolation: the second
            // write replaced the existing null version — no supersede ran, so
            // no history row exists — and the replaced content is still the
            // current null version.
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            assert_eq!(
                page.entries.len(),
                1,
                "an in-place replacement writes no history"
            );
            assert_eq!(
                page.entries[0].id, None,
                "the null version keeps its identity"
            );
            assert_eq!(page.entries[0].seq, 0, "…and its zero sequence");
            assert!(!page.entries[0].is_delete_marker);
            assert_eq!(
                page.entries[0].size, 3,
                "the entry is the replaced content, not the replaced version's"
            );
            let current = storage.get_object(&b, &key, None).await.unwrap();
            assert_eq!(
                current.info.version_id, None,
                "the replacement kept the null identity"
            );
            assert_eq!(
                read_body(current.body).await.unwrap(),
                b"two",
                "the current content is the replacement's"
            );
            // `?versionId=null` resolves to the same live null version.
            let ReadResult::Object(null) = storage
                .get_object_version(&b, &key, None, None)
                .await
                .unwrap()
            else {
                panic!("the null version must be an object version");
            };
            assert_eq!(null.info.version_id, None);
            assert_eq!(
                read_body(null.body).await.unwrap(),
                b"two",
                "the null request reaches the replaced live null version"
            );
        }

        #[tokio::test]
        async fn a_suspended_write_over_a_real_version_supersedes_it_into_history() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("s2.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"one".to_vec()))
                .await
                .unwrap();
            let first = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries[0]
                .id
                .clone()
                .expect("an enabled write mints an identity");
            storage
                .set_bucket_versioning(&b, State::Suspended)
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"two".to_vec()))
                .await
                .unwrap();

            // The other Suspended arm: the outgoing real version was
            // superseded into history, and the incoming write recorded the
            // null version as current.
            let current = storage.get_object(&b, &key, None).await.unwrap();
            assert_eq!(
                current.info.version_id, None,
                "a suspended write records the null version"
            );
            assert_eq!(read_body(current.body).await.unwrap(), b"two");
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            assert_eq!(
                page.entries.len(),
                2,
                "the outgoing real version entered history"
            );
            assert_eq!(
                page.entries[0].id, None,
                "the live null version leads the listing"
            );
            assert_eq!(
                page.entries[0].seq, 0,
                "the null version's sequence is zero"
            );
            assert_eq!(
                page.entries[1].id.as_ref(),
                Some(&first),
                "the superseded identity follows"
            );
            assert_eq!(
                page.entries[1].seq, 1,
                "it kept the sequence the enabled write recorded"
            );
            // The superseded version still serves its own bytes by id.
            let ReadResult::Object(old) = storage
                .get_object_version(&b, &key, Some(&first), None)
                .await
                .unwrap()
            else {
                panic!("the superseded version must be an object version");
            };
            assert_eq!(
                read_body(old.body).await.unwrap(),
                b"one",
                "the superseded version still serves its own bytes by id"
            );
        }

        #[tokio::test]
        async fn a_null_request_reaches_the_live_null_version_over_a_superseded_nulls_index_entry()
        {
            let (storage, b) = with_bucket().await;
            let key = object::key("n.txt").unwrap();
            // 1. An unversioned write records the null version.
            storage
                .put_object(&b, &key, body(b"one".to_vec()))
                .await
                .unwrap();
            // 2. Enabling supersedes it: the null version enters history and
            //    takes an index entry of its own (the literal `null` key).
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"two".to_vec()))
                .await
                .unwrap();
            let real = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries[0]
                .id
                .clone()
                .expect("the enabled write's identity is current");
            // 3. Suspending and writing replaces the real version with a fresh
            //    live null version, leaving the superseded null's index entry
            //    in place.
            storage
                .set_bucket_versioning(&b, State::Suspended)
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"three".to_vec()))
                .await
                .unwrap();

            // The current slot is resolved before the index, so
            // `?versionId=null` reaches the live null version ("three"), not
            // the superseded null row the index still names ("one", seq 0).
            let ReadResult::Object(null) = storage
                .get_object_version(&b, &key, None, None)
                .await
                .unwrap()
            else {
                panic!("the live null version must be an object version");
            };
            assert_eq!(null.info.version_id, None);
            assert_eq!(
                read_body(null.body).await.unwrap(),
                b"three",
                "the live null version wins over the superseded null's index entry"
            );
            let HeadResult::Object(info) =
                storage.head_object_version(&b, &key, None).await.unwrap()
            else {
                panic!("the live null version must head as an object");
            };
            assert_eq!(
                info.version_id, None,
                "the head names the live null version"
            );
            assert_eq!(
                info.size, 5,
                "the head is the live null version's, never the superseded row's"
            );
            // The superseded null row is still history, and the listing shows
            // all three versions: the live null, the real one, the old null.
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            let ids: Vec<Option<version::Id>> =
                page.entries.iter().map(|entry| entry.id.clone()).collect();
            assert_eq!(
                page.entries.len(),
                3,
                "the superseded null row is still listed"
            );
            assert_eq!(ids, vec![None, Some(real), None]);
            assert_eq!(
                page.entries[2].seq, 0,
                "the superseded null row keeps the sequence the index names"
            );
            assert_eq!(
                page.entries[2].size, 3,
                "the superseded null row carries its own content's size"
            );
        }

        #[tokio::test]
        async fn an_unversioned_bucket_lists_its_object_as_the_null_version() {
            let (storage, b) = with_bucket().await;
            assert_eq!(
                storage.bucket_versioning(&b).await.unwrap(),
                State::Unversioned,
                "the bucket never recorded a state"
            );
            let key = object::key("plain.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"hello".to_vec()))
                .await
                .unwrap();

            // The S3-correct shape: an object written while the bucket is
            // unversioned is listed as its null version, sequence zero.
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            assert_eq!(
                page.entries.len(),
                1,
                "the unversioned bucket lists exactly one (null) version"
            );
            let entry = &page.entries[0];
            assert_eq!(entry.key.as_ref(), "plain.txt");
            assert_eq!(entry.id, None, "the unversioned object is the null version");
            assert_eq!(entry.seq, 0, "no version-creating write minted a sequence");
            assert!(!entry.is_delete_marker);
            assert_eq!(entry.size, 5, "the null entry carries the object's size");
            // …and `?versionId=null` reads it back as the current object.
            let current = storage.get_object(&b, &key, None).await.unwrap();
            assert_eq!(
                current.info.version_id, None,
                "the plain read names the null version"
            );
            assert_eq!(read_body(current.body).await.unwrap(), b"hello");
            let ReadResult::Object(null) = storage
                .get_object_version(&b, &key, None, None)
                .await
                .unwrap()
            else {
                panic!("the null version must be an object version");
            };
            assert_eq!(null.info.version_id, None);
            assert_eq!(
                read_body(null.body).await.unwrap(),
                b"hello",
                "the null request reads the object's content"
            );
        }

        #[tokio::test]
        async fn deleting_the_last_version_removes_the_key() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("only.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"only".to_vec()))
                .await
                .unwrap();
            let id = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries[0]
                .id
                .clone()
                .expect("an enabled write mints an identity");

            let removed = storage
                .delete_object_version(&b, &key, Some(&id))
                .await
                .unwrap();
            assert_eq!(removed.id.as_ref(), Some(&id));
            assert!(!removed.is_delete_marker);
            assert!(
                storage.get_object(&b, &key, None).await.is_err(),
                "no version remains: the key is gone"
            );
            assert!(matches!(
                storage.get_object_version(&b, &key, Some(&id), None).await,
                Err(Error::Storage(storage::Error::NoSuchVersion(_)))
            ));
            assert!(
                storage
                    .list_object_versions(&b, params())
                    .await
                    .unwrap()
                    .entries
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn deleting_a_versioned_bucket_drops_its_version_state() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("a.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"first".to_vec()))
                .await
                .unwrap();
            storage
                .put_object(&b, &key, body(b"second".to_vec()))
                .await
                .unwrap();
            storage.delete_object(&b, &key).await.unwrap();

            // The key's current version is a marker and its history holds the
            // superseded versions: the bucket is empty of objects, and its
            // version state dies with the bucket row.
            storage.delete_bucket(&b).await.unwrap();
            assert!(
                storage.bucket_versioning(&b).await.is_err(),
                "the version state row must not outlive its bucket"
            );
        }

        #[tokio::test]
        async fn a_simple_delete_inserts_a_marker() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("gone.txt").unwrap();
            storage
                .put_object(&b, &key, body(b"content".to_vec()))
                .await
                .unwrap();
            let put_id = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries[0]
                .id
                .clone()
                .expect("an enabled write mints an identity");

            let marker = storage.delete_object(&b, &key).await.unwrap();
            assert!(
                marker.is_delete_marker,
                "a versioned delete inserts a marker"
            );
            let marker_id = marker
                .id
                .clone()
                .expect("an enabled marker has an identity");
            // The plain read is unreachable; the marker answers by its id.
            assert!(storage.get_object(&b, &key, None).await.is_err());
            assert!(matches!(
                storage
                    .get_object_version(&b, &key, Some(&marker_id), None)
                    .await
                    .unwrap(),
                ReadResult::Marker { id: Some(id), .. } if id == marker_id
            ));
            // `?versionId=null` names no version on this enabled bucket: the
            // marker's identity is a real one, not the null version.
            assert!(matches!(
                storage.get_object_version(&b, &key, None, None).await,
                Err(Error::Storage(storage::Error::NoSuchVersion(_)))
            ));
            // The marker is the newest entry; the hidden version is still there.
            let page = storage.list_object_versions(&b, params()).await.unwrap();
            assert_eq!(page.entries.len(), 2);
            assert!(page.entries[0].is_delete_marker);
            assert_eq!(page.entries[0].id.as_ref(), Some(&marker_id));
            assert_eq!(page.entries[1].id.as_ref(), Some(&put_id));
            assert!(!page.entries[1].is_delete_marker);
        }

        #[tokio::test]
        async fn a_folder_marker_delete_inserts_no_version_row() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("dir/").unwrap();
            storage
                .put_object(&b, &key, body(Vec::new()))
                .await
                .unwrap();
            storage.delete_object(&b, &key).await.unwrap();

            // Folder markers stay outside the version model (the design's
            // ruling): the delete removes the marker's rows and never enters
            // the version table — an invisible marker row would occupy the
            // index and a sequence forever.
            let rows = storage
                .db
                .read(|txn| -> Result<Vec<stored::Row>, Error> {
                    let mut out = Vec::new();
                    stored::Table::open_readonly(txn)?.for_key(
                        b.as_ref().as_str(),
                        key.as_ref().as_str(),
                        |row| {
                            out.push(row);
                            Ok(())
                        },
                    )?;
                    Ok(out)
                })
                .unwrap();
            assert!(
                rows.is_empty(),
                "a folder marker delete must not enter the version table: {rows:?}"
            );
        }

        /// The version surface's missing-bucket guard, one answer per
        /// operation: existence is the contract's own bucket-row read
        /// (`ensure_bucket`), so every call on a bucket that is not there is
        /// `NoSuchBucket` — never a row-level `NoSuchVersion`/`NoSuchKey`,
        /// and never a silent `Absent`/`Unversioned` default. Pins the
        /// guard the retired table-level `check_bucket` used to answer from
        /// inside each transaction.
        #[tokio::test]
        async fn version_ops_on_a_missing_bucket_are_no_such_bucket() {
            let storage = MemoryStorage::new().unwrap();
            let ghost = bucket::name("ghost").unwrap();
            let key = object::key("a.txt").unwrap();
            let missing = |err: Error| {
                assert!(
                    matches!(err, Error::Storage(storage::Error::NoSuchBucket(_))),
                    "expected NoSuchBucket, got {err:?}"
                );
            };
            missing(storage.bucket_versioning(&ghost).await.unwrap_err());
            missing(
                storage
                    .set_bucket_versioning(&ghost, State::Enabled)
                    .await
                    .unwrap_err(),
            );
            missing(
                storage
                    .get_object_version(&ghost, &key, None, None)
                    .await
                    .unwrap_err(),
            );
            missing(
                storage
                    .head_object_version(&ghost, &key, None)
                    .await
                    .unwrap_err(),
            );
            missing(
                storage
                    .delete_object_version(&ghost, &key, None)
                    .await
                    .unwrap_err(),
            );
            missing(storage.current_version(&ghost, &key).await.unwrap_err());
            missing(
                storage
                    .list_object_versions(&ghost, params())
                    .await
                    .unwrap_err(),
            );
            missing(storage.version_tags(&ghost, &key, None).await.unwrap_err());
            missing(
                storage
                    .set_version_tags(&ghost, &key, None, object::Tags::empty())
                    .await
                    .unwrap_err(),
            );
            missing(storage.version_acl(&ghost, &key, None).await.unwrap_err());
            missing(
                storage
                    .set_version_acl(&ghost, &key, None, &Acl::default_private(None))
                    .await
                    .unwrap_err(),
            );
        }

        /// The window the in-transaction recheck exists for, made concrete:
        /// the callers' `ensure_bucket` is a transaction of its own, so the
        /// state a committed `delete_bucket` leaves behind — the row was there
        /// when that read ran, and is gone by the time the operation's own
        /// transaction opens — is the state the probe must refuse. Both sides
        /// of the split are exercised, since the raw reads and the raw writes
        /// each open their own transaction.
        #[tokio::test]
        async fn an_in_window_teardown_is_refused_inside_the_transaction() {
            let (storage, b) = with_bucket().await;
            // While the row is recorded both probes pass — the no-race case
            // the callers' guard already answers.
            storage.db.read(|txn| check_bucket_read(txn, &b)).unwrap();
            storage.db.write(|txn| check_bucket_write(txn, &b)).unwrap();
            // The committed teardown, then the operation's own transaction.
            let name = b.as_ref().as_str().to_owned();
            storage
                .db
                .write(move |txn| -> Result<(), Error> {
                    let mut buckets = redb_bucket::Table::open(txn)?;
                    buckets.remove(&name)?;
                    Ok(())
                })
                .unwrap();
            let read = storage
                .db
                .read(|txn| check_bucket_read(txn, &b))
                .unwrap_err();
            assert!(
                matches!(read, Error::Storage(storage::Error::NoSuchBucket(_))),
                "{read:?}"
            );
            let write = storage
                .db
                .write(|txn| check_bucket_write(txn, &b))
                .unwrap_err();
            assert!(
                matches!(write, Error::Storage(storage::Error::NoSuchBucket(_))),
                "{write:?}"
            );
        }

        /// The versioned sibling of the storage-level race journey
        /// (`concurrent_put_and_delete_never_orphans`): a versioned write
        /// racing a teardown must leave nothing naming the bucket behind. The
        /// versioned write path is a raw transaction, so the bucket-row
        /// recheck inside it is what refuses a write whose transaction opens
        /// after the committed delete — the caller's `ensure_bucket` is a read
        /// of its own and cannot. A write that committed first is either
        /// drained by the teardown or (landing before its emptiness probe)
        /// keeps the bucket alive; the invariant below holds in every
        /// interleaving.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_versioned_write_racing_a_teardown_leaves_nothing_behind() {
            for _ in 0..32 {
                let storage = Arc::new(MemoryStorage::new().unwrap());
                let b = bucket::name("race").unwrap();
                storage
                    .create_bucket(&b, None, &Acl::default_private(None))
                    .await
                    .unwrap();
                storage
                    .set_bucket_versioning(&b, State::Enabled)
                    .await
                    .unwrap();
                let key = object::key("a.txt").unwrap();
                let mut handles = Vec::new();
                for i in 0..24 {
                    let s = Arc::clone(&storage);
                    let b = b.clone();
                    let k = key.clone();
                    handles.push(tokio::spawn(async move {
                        if i % 3 == 0 {
                            let _ = s.delete_bucket(&b).await;
                        } else {
                            let _ = s.put_object(&b, &k, body(b"x".to_vec())).await;
                        }
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
                // The storm settles with the bucket alive or gone; when it is
                // gone, nothing may still name it.
                let bucket_name = b.as_ref().as_str().to_owned();
                let (rows, objects, state) = storage
                    .db
                    .read(|txn| -> Result<(usize, usize, bool), Error> {
                        if redb_bucket::Table::open_readonly(txn)?.exists(&bucket_name)? {
                            return Ok((0, 0, false));
                        }
                        let mut rows = 0usize;
                        stored::Table::open_readonly(txn)?.for_bucket_of(
                            &bucket_name,
                            |_, _| {
                                rows += 1;
                                Ok(())
                            },
                        )?;
                        let mut objects = 0usize;
                        for_each_pair(
                            &*crate::_store_redb::objects::Table::open_readonly(txn)?,
                            (bucket_name.as_str(), ""),
                            |row_bucket, _| row_bucket == bucket_name,
                            |_, _, _| -> Result<(), Error> {
                                objects += 1;
                                Ok(())
                            },
                        )?;
                        let state = bucket_versioning::Table::open_readonly(txn)?
                            .get(&bucket_name)?
                            .is_some();
                        Ok((rows, objects, state))
                    })
                    .unwrap();
                assert_eq!(rows, 0, "version rows outlive their bucket");
                assert_eq!(objects, 0, "object rows outlive their bucket");
                assert!(!state, "the bucket's state row outlives it");
            }
        }

        /// The single-transaction core of a versioned completion refuses as a
        /// whole: the breach inside `complete_versioned` leaves the history the
        /// publish would have moved, the object row and bytes, the retained
        /// part rows and the tracked total exactly as they were. The
        /// step-by-step shape cannot claim that — its publish hook commits in a
        /// transaction of its own — and this refusal is what separates the two.
        #[tokio::test]
        async fn a_refused_versioned_completion_leaves_history_rows_and_bytes_untouched() {
            // The cap fits the live 40 bytes and nothing beside them. The
            // completion would store the outgoing 40 in history while the
            // incoming bytes land, and the credit it is given for the parts it
            // is about to release does not close the gap: 40 + 40 - 10 > 64.
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(64),
            })
            .unwrap();
            let b = bucket::name("complete-rollback").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("big.bin").unwrap();
            let outgoing = b"o".repeat(40);
            storage
                .put_object(&b, &key, body(outgoing.clone()))
                .await
                .unwrap();
            let first = current_id(&storage, &b, &key).await;
            assert_eq!(storage.total_bytes(), 40);
            assert_eq!(blob_bytes(&storage, &b), 0, "no history yet");

            // The completion's own assembled bytes and row, driven straight at
            // the core (the caller is the multipart complete).
            let data = b"n".repeat(40);
            let now = now_nanos();
            let row = object_row::Row {
                etag: ETag::from_content(&data),
                size: data.len() as u64,
                mtime: from_nanos(now),
                identity: 0,
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            };
            let retained = [object_row::PartRow {
                number: 1,
                size: data.len() as u64,
                checksum: None,
            }];
            // The accounting lock is the caller's, exactly as the multipart
            // path holds it across the core.
            let guard = storage.lock_accounting().await;
            let refused = storage.db.write(|txn| {
                complete_versioned(&storage, txn, &b, &key, now, &data, &row, &retained, 10)
                    .map(|_| ())
            });
            drop(guard);
            let err = refused.unwrap_err();
            assert!(
                matches!(err, Error::Storage(storage::Error::EntityTooLarge { .. })),
                "{err}"
            );

            // The whole transaction rolled back: the slot still names the
            // outgoing version, the bytes are the outgoing ones, no history row
            // or blob landed, no part row was written, nothing was metered.
            assert_eq!(current_id(&storage, &b, &key).await, first);
            assert_eq!(
                storage.total_bytes(),
                40,
                "a refused completion meters nothing"
            );
            assert_eq!(blob_bytes(&storage, &b), 0, "the publish left no history");
            assert_eq!(
                read_body(storage.get_object(&b, &key, None).await.unwrap().body)
                    .await
                    .unwrap(),
                outgoing,
                "the refused completion left the current version alone"
            );
            let bucket_name = b.as_ref().as_str().to_owned();
            let key_name = key.as_ref().to_owned();
            let (versions, parts) = storage
                .db
                .read(|txn| -> Result<(usize, usize), Error> {
                    let mut versions = 0usize;
                    stored::Table::open_readonly(txn)?.for_bucket_of(&bucket_name, |_, _| {
                        versions += 1;
                        Ok(())
                    })?;
                    let parts = crate::_store_redb::object_part::Table::open_readonly(txn)?
                        .list(&bucket_name, &key_name)?
                        .len();
                    Ok((versions, parts))
                })
                .unwrap();
            assert_eq!(versions, 0, "the refused completion left no version row");
            assert_eq!(parts, 0, "the refused completion left no part row");
        }

        /// The accounting lock is taken BEFORE a versioned write's transaction
        /// and released after it, so a versioned write racing the counted
        /// publishes never deadlocks: the decorator's lock is not reentrant, so
        /// a locking accounting call from inside the held section would stall
        /// every writer. The total also lands exactly on the bytes the writes
        /// stored, so the two paths cannot double-meter under contention.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_versioned_write_and_a_counted_publish_never_deadlock() {
            // A tracked total needs a configured limit: with no
            // `max_total_bytes` the accounting adjustment is a no-op.
            let storage = Arc::new(
                MemoryStorage::with_options(MemoryOptions {
                    max_object_bytes: None,
                    max_total_bytes: Some(4096),
                })
                .unwrap(),
            );
            let versioned = bucket::name("versioned-race").unwrap();
            let counted = bucket::name("counted-race").unwrap();
            for name in [&versioned, &counted] {
                storage
                    .create_bucket(name, None, &Acl::default_private(None))
                    .await
                    .unwrap();
            }
            storage
                .set_bucket_versioning(&versioned, State::Enabled)
                .await
                .unwrap();
            // Twelve versioned keys, written once each, so every racing
            // overwrite below supersedes a real version and moves its bytes into
            // history — the transaction the lock is held across.
            let written: Vec<object::Key> = (0..12)
                .map(|task| object::key(format!("v{task}.bin")).unwrap())
                .collect();
            for key in &written {
                storage
                    .put_object(&versioned, key, body(vec![b'v'; 8]))
                    .await
                    .unwrap();
            }
            assert_eq!(storage.total_bytes(), 12 * 8);

            let mut handles = Vec::new();
            for key in written {
                let storage = Arc::clone(&storage);
                let versioned = versioned.clone();
                handles.push(tokio::spawn(async move {
                    storage
                        .put_object(&versioned, &key, body(vec![b'v'; 8]))
                        .await
                        .unwrap();
                }));
            }
            for task in 0..12 {
                let storage = Arc::clone(&storage);
                let counted = counted.clone();
                handles.push(tokio::spawn(async move {
                    let key = object::key(format!("c{task}.bin")).unwrap();
                    storage
                        .put_object(&counted, &key, body(vec![b'c'; 8]))
                        .await
                        .unwrap();
                }));
            }

            // A re-entrant lock would stall every writer, and a stalled worker
            // pool cannot run a timer of its own: the watchdog is a plain
            // thread, independent of the runtime, and it ends the run instead of
            // letting the gate hang. The race itself is short.
            let (done, finished) = mpsc::channel();
            tokio::spawn(async move {
                let mut results = Vec::new();
                for handle in handles {
                    results.push(handle.await.map(|_| ()));
                }
                let _ = done.send(results);
            });
            let results =
                thread::spawn(
                    move || match finished.recv_timeout(Duration::from_secs(60)) {
                        Ok(results) => results,
                        Err(_) => {
                            eprintln!(
                                "tinio-mem: the accounting lock deadlocked across a versioned write"
                            );
                            process::exit(101);
                        }
                    },
                )
                .join()
                .expect("the watchdog thread panicked");
            for result in results {
                result.unwrap();
            }
            // The twelve supersedes move the outgoing bytes into history with a
            // delta of zero, and the content-addressed blob table stores that
            // one 8-byte blob once: 8 freshly stored bytes. The twelve counted
            // writes add 8 fresh bytes each, beside the 96 the first phase
            // stored.
            assert_eq!(storage.total_bytes(), 96 + 8 + 12 * 8);
        }

        /// The core records what the split shape recorded: the completion
        /// becomes the slot's version — its own identity and sequence, the marker
        /// bit clear, exactly the pair `advance_slot` would have written — the
        /// superseded version moves into history, the retained parts replace the
        /// key's list, the bytes are the assembled ones, and the meter charged
        /// the live object and the one fresh history blob.
        #[tokio::test]
        async fn a_versioned_completion_records_the_slot_and_the_history() {
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(4096),
            })
            .unwrap();
            let b = bucket::name("complete-records").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("assembled.bin").unwrap();
            storage
                .put_object(&b, &key, body(b"o".repeat(8)))
                .await
                .unwrap();
            let first = current_id(&storage, &b, &key).await;

            let data = b"n".repeat(24);
            let now = now_nanos();
            let row = object_row::Row {
                etag: ETag::from_content(&data),
                size: data.len() as u64,
                mtime: from_nanos(now),
                identity: 0,
                tags: object::Tags::empty(),
                checksum: None,
                owner: None,
                acl: Acl::default_private(None),
            };
            let retained = [object_row::PartRow {
                number: 1,
                size: data.len() as u64,
                checksum: None,
            }];
            let guard = storage.lock_accounting().await;
            let done: CompletedWrite = storage
                .db
                .write(|txn| {
                    complete_versioned(&storage, txn, &b, &key, now, &data, &row, &retained, 0)
                })
                .unwrap();
            drop(guard);

            // What the completion reports.
            assert!(done.records_versions);
            let id = done
                .version_id
                .clone()
                .expect("an enabled write mints an identity");
            assert_ne!(id, first);

            // What it stored: the current entry is the completion, the marker
            // bit is clear, the outgoing version is history beside it, the parts
            // are the completion's and the bytes are the assembled ones.
            let entries = storage
                .list_object_versions(&b, params())
                .await
                .unwrap()
                .entries;
            let current = entries
                .iter()
                .find(|entry| entry.key.as_ref() == key.as_ref())
                .expect("the completion's key is listed");
            assert_eq!(current.id, done.version_id);
            assert!(!current.is_delete_marker, "a completed put is not a marker");
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.key.as_ref() == key.as_ref()
                        && entry.id.as_ref() == Some(&first)),
                "the superseded version is listed as history"
            );
            assert_eq!(
                read_body(storage.get_object(&b, &key, None).await.unwrap().body)
                    .await
                    .unwrap(),
                data,
                "the assembled bytes are the current object"
            );
            assert_eq!(
                blob_bytes(&storage, &b),
                8,
                "the history blob holds the superseded bytes"
            );
            assert_eq!(
                storage.total_bytes(),
                24 + 8,
                "the live object and its one history blob"
            );
            let bucket_name = b.as_ref().as_str().to_owned();
            let key_name = key.as_ref().to_owned();
            let parts: Vec<(u32, u64)> = storage
                .db
                .read(|txn| -> Result<Vec<(u32, u64)>, Error> {
                    Ok(crate::_store_redb::object_part::Table::open_readonly(txn)?
                        .list(&bucket_name, &key_name)?
                        .into_iter()
                        .map(|part| (part.part_number, part.size))
                        .collect())
                })
                .unwrap();
            assert_eq!(
                parts,
                [(1, 24)],
                "the retained parts replaced the key's list"
            );
        }

        /// A stored row whose claimed size runs past the bytes beside it (only
        /// reachable by editing the store behind the backend) refuses the read
        /// as an unsatisfiable range instead of panicking on the slice.
        #[tokio::test]
        async fn a_row_whose_size_exceeds_its_bytes_refuses_the_read() {
            let (storage, b) = with_bucket().await;
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let key = object::key("tampered.bin").unwrap();
            storage
                .put_object(&b, &key, body(b"o".repeat(8)))
                .await
                .unwrap();
            let current = current_id(&storage, &b, &key).await;
            // The row still claims the eight bytes it was written with; the
            // content plane now holds four.
            let bucket_name = b.as_ref().as_str().to_owned();
            let key_name = key.as_ref().to_owned();
            storage
                .db
                .write(|txn| -> Result<(), Error> {
                    crate::_store_redb::objects::Table::open(txn)?.put(
                        &bucket_name,
                        &key_name,
                        &b"tiny"[..],
                    )?;
                    Ok(())
                })
                .unwrap();
            let err = storage
                .get_object_version(&b, &key, Some(&current), Some(ByteRange::Inclusive(0, 7)))
                .await
                .unwrap_err();
            assert!(
                matches!(err, Error::Storage(storage::Error::InvalidRange { .. })),
                "{err}"
            );
        }

        /// The window repair (ruling 2026-10-08): a row written while the bucket
        /// still looked unversioned, in a bucket that began recording versions
        /// under it, takes the identity a versioned write would have given it —
        /// without a version row, without touching the content columns, and
        /// without moving a byte of the tracked total.
        #[tokio::test]
        async fn the_window_repair_gives_an_unversioned_row_its_identity() {
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(4096),
            })
            .unwrap();
            let b = bucket::name("data").unwrap();
            storage
                .create_bucket(&b, None, &Acl::default_private(None))
                .await
                .unwrap();
            let key = object::key("window.txt").unwrap();
            // The write landed while the bucket was still unversioned.
            storage
                .put_object(&b, &key, body(b"window!!".to_vec()))
                .await
                .unwrap();
            let total = storage.total_bytes();
            // The bucket began recording versions under the row before anything
            // else ran.
            storage
                .set_bucket_versioning(&b, State::Enabled)
                .await
                .unwrap();
            let id = record_current_as_version(&storage, &b, &key)
                .await
                .unwrap()
                .expect("an enabled bucket records an identity");
            assert_eq!(
                current_id(&storage, &b, &key).await,
                id,
                "the slot carries the identity"
            );
            // A second run finds the row already identified: that identity is
            // another write's (or the earlier repair's) and this call claims
            // none of it — a version id is never handed out as freshly minted
            // twice.
            assert_eq!(
                record_current_as_version(&storage, &b, &key).await.unwrap(),
                None,
                "an identified row is not claimed again"
            );
            assert_eq!(
                current_id(&storage, &b, &key).await,
                id,
                "…and the identity it carries is left exactly as it was"
            );
            assert_eq!(
                storage.total_bytes(),
                total,
                "the repair meters nothing: the bytes were already the total's"
            );
            let bucket_name = b.as_ref().as_str().to_owned();
            let key_name = key.as_ref().as_str().to_owned();
            let rows = storage
                .db
                .read(|txn| -> Result<u64, Error> {
                    let table = stored::Table::open_readonly(txn)?;
                    let mut rows = 0u64;
                    table.for_key(&bucket_name, &key_name, |_| {
                        rows += 1;
                        Ok(())
                    })?;
                    Ok(rows)
                })
                .unwrap();
            assert_eq!(
                rows, 0,
                "the current version is the object row: the repair writes no version row"
            );
        }
    }
}
