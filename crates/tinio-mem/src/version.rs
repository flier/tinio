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
//! `tinio_store`'s — the same ones `tinio-fs` opens — and the write path's
//! **policy is the shared `tinio_version::plan_write`**, imported rather than
//! copied: the decision (whether to supersede, which identity and sequence
//! the incoming version takes, the one-null-row convergence, the marker's
//! fresh sequence) is a pure function of values, and only its execution is
//! mem's — one transaction of row operations, with no I/O to abstract over.
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

use crate::_core::{
    BodyStream, ETag, GetObjectResult,
    acl::{Acl, OwnerId},
    checksum, object,
    version::Id,
};

/// The feature-off `VersionOps`: the trait's default bodies own every answer
/// (the refusal class for the nine, `Unversioned` for `bucket_versioning`),
/// so there is nothing to write here and nothing to forget.
#[cfg(not(feature = "versioning"))]
impl crate::_core::storage::VersionOps for crate::storage::MemoryStorage {}

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
    data: &[u8],
    served: Option<(u64, u64)>,
) -> GetObjectResult {
    let body: Vec<u8> = match served {
        Some((start, end)) => data[start as usize..=end as usize].to_vec(),
        None => data.to_vec(),
    };
    GetObjectResult {
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
        },
        body: Box::pin(iter(vec![Ok(Bytes::from(body))])) as BodyStream,
        served_range: served,
    }
}

/// The versioned implementation, compiled with the `versioning` feature.
#[cfg(feature = "versioning")]
mod enabled {
    use std::{
        cmp::Reverse,
        collections::{BTreeMap, BTreeSet},
    };

    use async_trait::async_trait;
    use redb::{ReadableTable, WriteTransaction};

    use crate::{
        _core::{
            ByteRange, ETag,
            acl::Acl,
            blob::{BlobRef, BlockHash},
            bucket::Name,
            from_nanos, object,
            storage::{VersionOps, marker_has_no_metadata, rename_allowed},
            version::{
                self, HeadResult, Id, ListParams, Listing, Marker, ReadResult, State, wire_of,
            },
        },
        _store::{
            bucket, bucket_versioning, meta, objects,
            scan::{drain_pair, for_each_pair},
            version as stored, version_index,
        },
        _version::{Current as Slot, Identity, Intent, Seqs, plan_write},
        DatabaseError, Error,
        error::{no_such_key, no_such_version},
        storage::{MemoryStorage, check_bucket},
        version::object_result,
    };

    /// `(bucket, content-hash-hex)` → whole-blob bytes: the content-addressed
    /// store of superseded versions' content. mem has no block engine, so a
    /// version's content is one blob (the design's whole-blob v1 shape), and a
    /// rewrite is a dedup hit — the key is the hash, so the same content never
    /// lands twice.
    pub(crate) mod blobs {
        use redb::TableDefinition;

        use crate::_store::table::{self, TableDef};

        /// The per-table marker: the table definition for the shared handle
        /// arms (opening, ensuring and the read/write split are
        /// `tinio_store::table`'s; a mem-local table cannot add inherent
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
        meta: Option<meta::Stored>,
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
            if let Some(meta) = &self.meta
                && self.content
            {
                return Some(Current::Object(Box::new(meta.clone())));
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
        Object(Box<meta::Stored>),
        /// A delete marker: a version row stamped current, holding no
        /// content.
        Marker(Box<stored::Row>),
    }

    /// One key's rows in a read transaction.
    fn peek(txn: &redb::ReadTransaction, bucket: &Name, key: &object::Key) -> Result<Peek, Error> {
        let b = bucket.as_ref().as_str();
        let k = key.as_ref().as_str();
        let meta = meta::Table::open_readonly(txn)?.get(b, k)?;
        let content = objects::Table::open_readonly(txn)?.get(b, k)?.is_some();
        let mut rows = Vec::new();
        stored::Table::open_readonly(txn)?.for_key(b, k, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(Peek {
            meta,
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
        let meta = meta::Table::open(txn)?.get(b, k)?;
        let content = objects::Table::open(txn)?.get(b, k)?.is_some();
        let mut rows = Vec::new();
        stored::Table::open(txn)?.for_key(b, k, |row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(Peek {
            meta,
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
        Current(Box<meta::Stored>),
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
        if let Some(meta) = &peek.meta
            && peek.content
            && match version {
                Some(id) => meta.version_id.as_ref() == Some(id),
                None => meta.version_id.is_none(),
            }
        {
            return Some(Found::Current(Box::new(meta.clone())));
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
        Current(Box<meta::Stored>),
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
                let (etag, tags, checksum, owner, acl) = meta::wires(Some(&meta));
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
        meta::Table::open(txn)?.remove(b, k)?;
        crate::_store::object_part::Table::open(txn)?.remove_key(b, k)?;
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
        meta::Table::open(txn)?.put(b, k, &row.stored(seq, row.size, row.created, 0))?;
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
    /// a dedup hit). The caller folds the delta into the write's own, one
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
    ) -> Result<(Option<Id>, u64, i64), Error> {
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
        Ok((id, plan.seq, delta))
    }

    /// The rename guard as this backend must apply it — inside the
    /// transaction that would move the rows (the contract's rule cannot
    /// await, so the state read is this backend's part and the answer is the
    /// shared [`rename_allowed`](crate::_core::storage::rename_allowed)'s):
    /// a bucket that records versions refuses the key-moving operation
    /// instead of corrupting, and an unversioned bucket keeps today's move
    /// untouched.
    pub(crate) fn rename_refusal(
        txn: &mut WriteTransaction,
        bucket: &Name,
        key: &object::Key,
    ) -> Result<(), Error> {
        let state = bucket_versioning::Table::open(txn)?
            .get(bucket.as_ref().as_str())?
            .unwrap_or_default();
        rename_allowed(state, key)
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

    /// Drop a whole bucket's version state (bucket teardown): the version
    /// rows, their index, the bucket's state row and its blobs — the
    /// design's bucket-delete drops the version data with the bucket.
    /// Answers the drained blobs' bytes, so the caller releases exactly what
    /// this drain removed.
    pub(crate) fn drain_bucket(txn: &mut WriteTransaction, bucket: &Name) -> Result<u64, Error> {
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

    #[async_trait]
    impl VersionOps for MemoryStorage {
        async fn bucket_versioning(&self, bucket: &Name) -> Result<State, Error> {
            self.db.read(|txn| {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
                Ok(bucket_versioning::Table::open_readonly(txn)?
                    .get(bucket.as_ref().as_str())?
                    .unwrap_or_default())
            })
        }

        async fn set_bucket_versioning(&self, bucket: &Name, state: State) -> Result<(), Error> {
            self.db.write(|txn| {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
                bucket_versioning::Table::open(txn)?.set(bucket.as_ref().as_str(), state)?;
                Ok(())
            })
        }

        async fn get_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
            range: Option<ByteRange>,
        ) -> Result<ReadResult, Error> {
            self.db.read(|txn| {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
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
                            &data,
                            served,
                        )))
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
                            &data,
                            served,
                        )))
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
            self.db.read(|txn| {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
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
                    })),
                    Found::Row(row) if row.kind == stored::Kind::Marker => Ok(HeadResult::Marker {
                        id: row.id(),
                        last_modified: from_nanos(row.created),
                    }),
                    Found::Row(row) => Ok(HeadResult::Object(row.info(key))),
                }
            })
        }

        async fn delete_object_version(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<Marker, Error> {
            self.db.write(|txn| -> Result<Marker, Error> {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
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
                self.release_total(delta);
                Ok(marker)
            })
        }

        async fn list_object_versions(
            &self,
            bucket: &Name,
            params: ListParams,
        ) -> Result<Listing, Error> {
            self.db.read(|txn| -> Result<Listing, Error> {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
                let b = bucket.as_ref().as_str();
                let prefix = params.prefix.as_deref().unwrap_or_default();
                // The current slots (the object_meta rows) and the version
                // rows, both keyed by key; the listing merges them per key.
                let mut currents: BTreeMap<String, meta::Stored> = BTreeMap::new();
                meta::Table::open_readonly(txn)?.for_bucket_gated(b, |key, stored| {
                    if let Some(stored) = stored
                        && key.as_ref().starts_with(prefix)
                        && !key.is_reserved()
                        && !key.is_folder_marker()
                    {
                        currents.insert(key.as_ref().to_owned(), stored);
                    }
                    Ok(())
                })?;
                let mut history: BTreeMap<String, Vec<stored::Row>> = BTreeMap::new();
                stored::Table::open_readonly(txn)?.for_bucket_of(b, |key, row| {
                    if key.starts_with(prefix) {
                        history.entry(key.to_owned()).or_default().push(row);
                    }
                    Ok(())
                })?;
                let mut keys: BTreeSet<String> = currents.keys().cloned().collect();
                keys.extend(history.keys().cloned());
                let mut entries: Vec<version::Entry> = Vec::new();
                for key in keys {
                    let Ok(obj_key) = object::key(&key) else {
                        continue; // a domain-invalid key is skipped, like a listing
                    };
                    if obj_key.is_reserved() || obj_key.is_folder_marker() {
                        continue;
                    }
                    let rows = history.get(&key).map(Vec::as_slice).unwrap_or_default();
                    // The current version first: the meta row's object
                    // version, or the marker row the slot records.
                    if let Some(stored) = currents.get(&key) {
                        entries.push(version::Entry {
                            key: obj_key.clone(),
                            id: stored.version_id.clone(),
                            seq: stored.version_seq,
                            is_delete_marker: false,
                            size: stored.size,
                            last_modified: from_nanos(stored.mtime),
                            etag: stored.etag.clone(),
                        });
                    } else if let Some(row) = rows
                        .iter()
                        .find(|row| !row.is_history(stored::Slot::Unknown))
                    {
                        entries.push(entry_of(&obj_key, row));
                    }
                    // Then the noncurrent versions, newest first.
                    let mut older: Vec<&stored::Row> = rows
                        .iter()
                        .filter(|row| row.is_history(stored::Slot::Unknown))
                        .collect();
                    older.sort_by_key(|row| Reverse(row.seq));
                    for row in older {
                        entries.push(entry_of(&obj_key, row));
                    }
                }
                Ok(version::paginate(entries, &params))
            })
        }

        async fn version_tags(
            &self,
            bucket: &Name,
            key: &object::Key,
            version: Option<&Id>,
        ) -> Result<object::Tags, Error> {
            self.db.read(|txn| {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
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
            self.db.write(|txn| -> Result<(), Error> {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
                if key.is_reserved() || key.is_folder_marker() {
                    return Err(no_such_key(key));
                }
                let b = bucket.as_ref().as_str();
                let k = key.as_ref().as_str();
                let peek = peek_mut(txn, bucket, key)?;
                let index = index_seq_mut(txn, bucket, key, version)?;
                match locate(resolve(&peek, index, version), version)? {
                    Target::Current(_) => {
                        meta::Table::open(txn)?
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
            self.db.read(|txn| {
                check_bucket(&bucket::Table::open_readonly(txn)?, bucket)?;
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
            self.db.write(|txn| -> Result<(), Error> {
                check_bucket(&bucket::Table::open(txn)?, bucket)?;
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
                        meta::Table::open(txn)?.put(
                            b,
                            k,
                            &meta::Stored {
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
    blobs, drain_bucket, insert_marker, prepare_publish, rename_refusal,
};

#[cfg(test)]
mod tests {
    // The backend journeys the fs half mirrors: overwrite keeps the previous
    // version readable, a suspended write replaces the null version without
    // history, deleting the last version removes the key, and a simple delete
    // inserts a marker. The conformance leg is the real oracle.
    #[cfg(feature = "versioning")]
    mod versioned {
        use crate::{
            _core::{
                BucketOps, ObjectOps,
                acl::Acl,
                bucket, object,
                storage::{self, VersionOps},
                version::{self, HeadResult, ListParams, ReadResult, State},
            },
            _store::{scan::for_each_pair, version as stored},
            _util::testing::{body, read_body},
            Error, MemoryOptions, MemoryStorage,
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
    }
}
