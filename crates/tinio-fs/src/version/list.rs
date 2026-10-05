//! The version listing: the bucket's current objects merged with their
//! version rows, and the page.
//!
//! The walk is the `object_meta` rows of the bucket (the current slots — a
//! row whose recorded marker flag is set and whose file is gone is the key's
//! delete marker, while a row still describing content whose file went out of
//! band is a removed current version and lists no current entry), merged with the
//! version table's rows per key: the current version first, then the
//! noncurrent ones newest first, which is the order the wire's
//! `ListObjectVersions` fixes. A key that exists only as history (its current
//! slot gone) still lists; a current version that was never overwritten lists
//! with no history row at all.
//!
//! The two tables are read in **one** read transaction (one blocking-pool
//! hop, one snapshot), because the page merges them per key and must not
//! assemble a current slot of one point in time with a history of another.
//! Each key's current slot is then resolved by the read path's own
//! [`resolve_object_info`](crate::FsStorage::resolve_object_info) — one
//! open, one gate verdict, one row — and those resolutions, independent of
//! each other from the moment the rows are in hand, overlap under
//! [`RESOLVE_CONCURRENCY`] instead of paying one serial stat/open/row round
//! trip per key.

use std::{cmp::Reverse, collections::BTreeMap};

use futures::{StreamExt, stream};

use crate::{
    _core::{
        ETag,
        bucket::Name,
        from_nanos, object, storage,
        version::{self, Entry, ListParams, Listing},
    },
    _store::{meta::Stored, version as rows},
    Error, FsStorage,
    meta::GatedMeta,
};

/// How many of a listing's keys resolve at once. Each key's work is a
/// handful of latency-bound syscalls (the path policy, one open, the
/// handle's stat) plus a point read of its row and — only where the gate
/// degrades a recorded identity — one heal; none of it is CPU, so
/// overlapping is what a listing actually buys. 16 keeps the in-flight
/// open handles and blocking-pool slots this walk holds bounded and well
/// below the pool's own capacity (a listing never crowds out the request
/// work sharing it), while hiding the per-key latency many times over on
/// any local filesystem — beyond that, the useful concurrency is already
/// reached and the only thing added is pressure.
const RESOLVE_CONCURRENCY: usize = 16;

/// One page of the bucket's versions, in the wire's order: by key, the current
/// version first, then the noncurrent versions newest first. `key_marker`
/// skips the keys up to and including the marker (alone) or resumes after the
/// named version (with `version_id_marker`); a delimiter rolls a group of keys
/// up as one common prefix.
pub(crate) async fn page(
    storage: &FsStorage,
    bucket: &Name,
    params: &ListParams,
) -> Result<Listing, Error> {
    // The bucket must exist (`NoSuchBucket`, before anything else): the
    // resolutions below re-derive the directory they address
    // (`resolve_object_file`'s own `ensure_bucket`), so existence is the
    // only thing the page takes from this call.
    storage.ensure_bucket(bucket).await?;
    let prefix = params.prefix.clone().unwrap_or_default();

    // The current slots (one gated row per key, in key order) and the
    // version rows (per key, in key order), in ONE read transaction and one
    // blocking-pool hop: the rows the page merges are one snapshot, and the
    // two full-bucket traversals cost one dispatch instead of two.
    let handle = storage.handle().clone();
    let store = storage.meta_store().clone();
    let scan_bucket = bucket.clone();
    let row_prefix = prefix.clone();
    let (gated, history) = handle
        .read_blocking(move |txn| {
            let gated = store.load_bucket_in(txn, &scan_bucket)?;
            let mut history: BTreeMap<String, Vec<rows::Row>> = BTreeMap::new();
            rows::Table::open_readonly(txn)?.for_bucket_of(
                scan_bucket.as_ref().as_str(),
                |key, row| {
                    if key.starts_with(&row_prefix) {
                        history.entry(key.to_owned()).or_default().push(row);
                    }
                    Ok(())
                },
            )?;
            Ok((gated, history))
        })
        .await
        .map_err(Error::from)?;

    // The keys whose current slot resolves, in key order. The prefix,
    // reserved-key and folder-marker filters run before any filesystem
    // work, so a key the page cannot list costs nothing. A row whose etag
    // is domain-invalid is already reported as missing by the gated walk,
    // and reads as such: the key's history rows still list it.
    let slots: Vec<(object::Key, Stored)> = gated
        .into_iter()
        .filter_map(|GatedMeta { key, stored }| {
            let stored = stored?;
            if !key.as_ref().starts_with(&prefix) || key.is_reserved() || key.is_folder_marker() {
                return None;
            }
            Some((key, stored))
        })
        .collect();

    // The current slot of every key, resolved under bounded concurrency
    // (the order the results come back in is the keys' own — the map below
    // is keyed, so only the work overlaps, never the page). One resolution
    // answers both the object and the marker arm: `resolve_object_info` is
    // the call a `GET`/`HEAD` of the key makes — the path policy, one open,
    // the handle's own size/stamp/identity, the gated row and its heal — so
    // the listing can never disagree with the read about the current version
    // (a drifted file is the null version, never the stale identifier, and
    // its ETag is the content-derived one the heal wrote, not the row's
    // superseded value). The file's absence is that resolution's own
    // `NoSuchKey` — no second path stat to choose the arm — and what it
    // means is the row's shape: a row whose **recorded** marker flag is set
    // is the delete marker the API created, a row still describing content
    // is an out-of-band removal, so the mirror wins and the key has no
    // current version to list, its history rows (if any) listing it alone.
    // The flag is never inferred from the absence or from the row's shape: a
    // zero-byte object's row is shape-identical to a marker's on a
    // filesystem without a file identity. A resolve failure that is anything
    // else propagates instead of silently dropping the key from the page —
    // as does a key whose path the policy refuses — and the file that
    // vanishes under the page is the same absence, with the key left to its
    // history rows.
    let resolved: Vec<Result<(object::Key, Option<Entry>), Error>> = stream::iter(slots)
        .map(|(key, stored)| async move {
            let entry = match storage.resolve_object_info(bucket, &key).await {
                // The sequence is the slot's own recorded one, whatever the
                // gated identity: a current whose identity the gate replaced
                // with the null version still records the sequence it was
                // written with (the null version's 0 on a fresh key, the free
                // sequence a suspended write took when 0 was occupied), and
                // the history filter below hands exactly that sequence to
                // `Row::is_history` — the crash window's adopted row is the
                // unstamped row sitting on it.
                Ok((_file, size, mtime, row, version_id)) => Some(Entry {
                    key: key.clone(),
                    seq: row.version_seq,
                    id: version_id,
                    is_delete_marker: false,
                    size,
                    last_modified: mtime,
                    etag: row.etag,
                }),
                Err(Error::Storage(storage::Error::NoSuchKey(_))) => {
                    if stored.is_delete_marker {
                        Some(Entry {
                            key: key.clone(),
                            id: stored.version_id,
                            seq: stored.version_seq,
                            is_delete_marker: true,
                            size: 0,
                            last_modified: from_nanos(stored.mtime),
                            etag: ETag::EMPTY,
                        })
                    } else {
                        None
                    }
                }
                Err(err) => return Err(err),
            };
            Ok((key, entry))
        })
        .buffered(RESOLVE_CONCURRENCY)
        .collect()
        .await;
    let mut currents: BTreeMap<String, Entry> = BTreeMap::new();
    for resolved in resolved {
        // The error of the first failing key, in key order — the answer the
        // one-key-at-a-time walk gave — never whichever failure settled
        // first.
        let (key, entry) = resolved?;
        let Some(entry) = entry else {
            continue;
        };
        currents.insert(key.as_ref().as_str().to_owned(), entry);
    }

    // The page's keys, in order: both maps are already ordered, so a merge
    // walks every key exactly once — no third owned copy of the key set.
    let mut keys: Vec<&str> = Vec::with_capacity(currents.len() + history.len());
    let (mut current_keys, mut history_keys) =
        (currents.keys().peekable(), history.keys().peekable());
    loop {
        let next = match (current_keys.peek(), history_keys.peek()) {
            (Some(current), Some(historic)) if current < historic => current_keys.next(),
            (Some(current), Some(historic)) if historic < current => history_keys.next(),
            (Some(_), Some(_)) => {
                // The key has both a current slot and history rows: one
                // page key, taken from the current side.
                history_keys.next();
                current_keys.next()
            }
            (Some(_), None) => current_keys.next(),
            (None, Some(_)) => history_keys.next(),
            (None, None) => break,
        };
        keys.push(next.expect("a peeked key is present").as_str());
    }

    let mut entries: Vec<Entry> = Vec::new();
    for key in keys {
        let Ok(obj_key) = object::key(key) else {
            continue; // a domain-invalid key is skipped, like a listing
        };
        if obj_key.is_reserved() || obj_key.is_folder_marker() {
            continue;
        }
        // What the reader knows about the key's current slot, from the entry
        // it just derived: a listed current version is at its own sequence —
        // the sequence the row it was resolved from records — and a key with
        // no current entry has **no** current version at all (its file went
        // out of band, or vanished under the page). The second state is not
        // "no slot fact": nothing is current, so every version row is history
        // and must list — a row an aborted publish or an out-of-band removal
        // left unstamped at a sequence no slot holds is a version like any
        // other, never a key's second "latest".
        let slot = currents
            .get(key)
            .map_or(rows::Slot::Gone, |entry| rows::Slot::At(entry.seq));
        if let Some(entry) = currents.get(key) {
            entries.push(entry.clone());
        }
        // Only the key's **history** rows list, and the row says which it is
        // (`Row::is_history`): the stamp is the authority — a superseded row
        // is stamped noncurrent by the state transaction that moved the
        // current slot off it — with the slot knowledge above for the rows no
        // stamp reached. In the crash window the outgoing version's row is
        // already committed while the version is still current: it is
        // unstamped and sits at the current slot's own sequence (the listed
        // entry's), so it is not history and listing it would serve two
        // "latest" versions of one key (Review Focus 1). The match is the
        // *slot's* sequence, never `0`: a superseded null version's row sits
        // at the null's own sequence (0 on the fresh-key path, the free
        // sequence a later null took when 0 was occupied), and a current null
        // does not make the row at 0 a duplicate of itself — hiding that row
        // would drop the superseded null version from the listing entirely.
        let mut older: Vec<&rows::Row> = history
            .get(key)
            .map(|rows| rows.iter().filter(|row| row.is_history(slot)).collect())
            .unwrap_or_default();
        older.sort_by_key(|row| Reverse(row.seq));
        for row in older {
            entries.push(entry_of(&obj_key, row));
        }
    }

    Ok(version::paginate(entries, params))
}

/// One listed version, built from its row. The ETag comes from the row's own
/// element — the value the version was written with, composed `-N` form
/// included — so a listing page never reads a version's content; a delete
/// marker answers the empty-content ETag.
fn entry_of(key: &object::Key, row: &rows::Row) -> Entry {
    let marker = row.kind == rows::Kind::Marker;
    Entry {
        key: key.clone(),
        id: row.id(),
        seq: row.seq,
        is_delete_marker: marker,
        size: row.size,
        last_modified: from_nanos(row.created),
        etag: if marker { ETag::EMPTY } else { row.etag() },
    }
}
