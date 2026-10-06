//! The version listing: the bucket's current objects merged with their
//! version rows, and the page.
//!
//! The walk is **bounded**: `ListObjectVersions` cuts a `max_keys` page, so
//! the listing reads the rows that page needs and nothing else — never the
//! whole bucket (the page of a million-key bucket costs a page). Both tables
//! are keyed `(bucket, key[, seq])`, so the walk starts at the listing's
//! first key — the `key_marker` where it names one, the prefix otherwise —
//! and reads one **window** of keys per round: the gated `object_meta` rows
//! (the current slots — a row whose recorded marker flag is set and whose
//! file is gone is the key's delete marker, while a row still describing
//! content whose file went out of band is a removed current version and
//! lists no current entry) and the version rows of that key span, in one
//! read transaction (one blocking-pool hop, one snapshot for the two tables,
//! so a key's slot and its history are never two points in time). The round
//! ends as soon as [`version::paginate`] — the engine the page has always
//! been cut by — can see the page's cut; the keys past it are never read.
//!
//! A window covers one key per entry the page can hold, plus the one the
//! truncation probe needs. A key that rolls up under the listing's delimiter
//! contributes no entries of its own, so its members are skipped without a
//! resolution once the group is in the page — and the walk then jumps the
//! group's whole key range (the successor of the rolled prefix), so a
//! million-member group costs one window, not a million.
//!
//! The order is the wire's: by key, the current version first, then the
//! noncurrent ones newest first (`Row::is_history` is the one predicate a
//! reader asks; a key whose current slot is gone lists its history rows
//! alone, a key that was never overwritten lists with no history row at
//! all). Each key the page actually reaches has its current slot resolved by
//! the read path's own [`resolve_object_slot`](crate::FsStorage) — one
//! open, one gate verdict, one row, the call a `GET`/`HEAD` of the key
//! makes — and those resolutions, independent of each other from the moment
//! the rows are in hand, overlap under [`RESOLVE_CONCURRENCY`] instead of
//! paying one serial stat/open/row round trip per key. A key the page never
//! reaches — one before the `key_marker`, one whose delimiter group is
//! already in, or one past the cut — is never opened.
//!
//! One page is one window in every ordinary listing; a page that needs more
//! (a bucket whose keys yield no entries, or a delimiter page over many empty
//! groups) takes each window as its own snapshot. Every key is still one
//! point in time — a key's slot and its history are read together, and no key
//! is read twice — which is the per-key coherence the page has always had;
//! what widens is only the snapshot across keys of different windows (a
//! listing's page was never a snapshot promise to the wire).

use std::{
    cmp::{Reverse, min},
    collections::{BTreeMap, HashSet},
    ops::Bound,
};

use futures::{StreamExt, future::BoxFuture, stream::FuturesOrdered};

use crate::{
    _core::{
        ETag,
        bucket::Name,
        from_nanos, object, storage,
        version::{self, Entry, ListParams, Listing},
    },
    _store::{
        meta::{self, Stored},
        version as rows,
    },
    Error, FsStorage,
    database::Error as DatabaseError,
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
///
/// `pub(crate)` for the roll-up test's own bound: the walk's cost claim
/// ("a pipe's worth of resolutions, never one per member") is derived from
/// this constant rather than re-spelled, so raising it cannot make a
/// correct walk fail that test.
pub(crate) const RESOLVE_CONCURRENCY: usize = 16;

/// One page of the bucket's versions, in the wire's order: by key, the current
/// version first, then the noncurrent versions newest first. `key_marker`
/// skips the keys up to and including the marker (alone) or resumes after the
/// named version (with `version_id_marker`); a delimiter rolls a group of keys
/// up as one common prefix.
///
/// The engine owns the page: this walk only feeds it the entries up to (and
/// including) the item its truncation probe needs, so the cut, the roll-up
/// and the resume cursors stay [`version::paginate`]'s own.
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
    // A page cut to nothing asks for nothing: `max_keys = 0` answers the
    // empty page ([`version::paginate`]'s own rule) without walking the
    // bucket for a truncation that can never come.
    if params.max_keys == 0 {
        return Ok(version::paginate(Vec::new(), params));
    }

    let mut walk = Walk::new(storage, bucket, params);
    let mut cursor = Cursor::At(walk.start_key());
    loop {
        let window = walk.read(&cursor).await?;
        walk.absorb(&window).await?;
        // The page is decided by the engine that has always cut it: its
        // counted items never exceed the entries it is handed, so the call
        // is only worth making once the walk holds that many — and a
        // truncated answer already carries the page's cut and its cursors,
        // so the walk stops there. Anything else walks on.
        if walk.entries.len() >= params.max_keys {
            let listing = version::paginate(walk.entries.clone(), params);
            if listing.is_truncated {
                return Ok(listing);
            }
        }
        match walk.next_cursor(&window) {
            Some(next) => cursor = next,
            None => return Ok(version::paginate(walk.entries, params)),
        }
    }
}

/// The walk's resume bound: the key the next window starts at, or the key it
/// starts after. `After` is the stronger bound — `At(key)` includes the key,
/// `After(key)` skips it whole.
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
/// code point no successor exists for (`U+10FFFF`) — such a group is walked
/// member by member (the members' keys are compared, never resolved).
fn group_end(rolled: &str) -> Option<Cursor> {
    let mut head = rolled.chars();
    let last = head.next_back()?;
    let next = char::from_u32(u32::from(last) + 1)?;
    let mut end = head.as_str().to_owned();
    end.push(next);
    Some(Cursor::At(end))
}

/// One key of a read window: its gated current-slot row, as the gating walk
/// reports it (`None` = no row, or one the store reads as missing), and its
/// version rows in the table's own `(key, seq)` order.
struct WindowKey {
    /// The key, raw: the listing's own prefix and domain filters run on it.
    key: String,
    /// The current slot's gated row.
    slot: Option<Stored>,
    /// The key's version rows, ascending sequence.
    rows: Vec<rows::Row>,
}

/// One read window: the keys both tables cover from the cursor, in key order,
/// and the bound the next window starts at.
struct Window {
    /// The window's keys, ascending.
    keys: Vec<WindowKey>,
    /// The next window's bound; `None` = both tables were read to the end of
    /// the bucket.
    next: Option<Cursor>,
}

/// Read one window: the gated current slots and the version rows of the first
/// `budget` keys at or after `cursor`.
///
/// The two tables are read in **one** transaction — one blocking-pool hop —
/// so a key's slot row and its version rows are one snapshot: the page merges
/// them per key and must not assemble a current slot of one point in time
/// with a history of another.
///
/// A window ends at a key **both** tables covered. The slot walk stops before
/// the key that would make it `budget + 1` keys, the row walk before the key
/// that would, and the window keeps only the keys at or before the earlier of
/// the two stops — the keys past it are unread by one side, so the next
/// window re-reads them rather than pair a known slot with unknown history
/// (or the reverse). The stop is checked on a key boundary, so a key's own
/// version rows are always read whole.
fn read_window(
    txn: &redb::ReadTransaction,
    bucket: &str,
    cursor: &Cursor,
    budget: usize,
) -> Result<Window, DatabaseError> {
    let from = cursor.as_bound();
    // The current slots: one gated row per key, in key order.
    let mut slots: BTreeMap<String, Option<Stored>> = BTreeMap::new();
    {
        let table = meta::Table::open_readonly(txn)?;
        table.for_bucket_gated_from(bucket, from, |key, stored| {
            if slots.len() == budget {
                return Ok(false);
            }
            slots.insert(key.as_ref().to_owned(), stored);
            Ok(true)
        })?;
    }
    let slots_end = slots.len() < budget;
    // The version rows: the same key span, key order, sequence ascending. A
    // row whose shape is corrupt is skipped, never fails the walk.
    let mut history: BTreeMap<String, Vec<rows::Row>> = BTreeMap::new();
    {
        let table = rows::Table::open_readonly(txn)?;
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

    // The window's coverage: a walk that stopped early bounds it (the other
    // walked past that key), and two early stops meet at the earlier key.
    // Both walks reaching the bucket's end means nothing is left to read.
    let through = match (slots_end, history_end) {
        (true, true) => None,
        (false, true) => slots.keys().next_back().cloned(),
        (true, false) => history.keys().next_back().cloned(),
        (false, false) => match (slots.keys().next_back(), history.keys().next_back()) {
            (Some(slot), Some(historic)) => Some(min(slot, historic).clone()),
            // Unreachable — a walk stops holding its budget's worth of keys
            // — and a window covering what it read is the safe answer.
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

/// One window key the walk feeds the page: where it sits in the window, its
/// validated key, and the delimiter group it rolls up into.
struct Candidate {
    /// The key's index in the window it came from.
    index: usize,
    /// The key, validated into the domain (its `resolve_object_slot`
    /// address).
    key: object::Key,
    /// The rolled-up prefix the key's entries group under, `None` = a key
    /// the page lists one version at a time.
    rolled: Option<String>,
}

/// The bounded page walk: the window loop, the per-key resolution and the
/// entries the page is cut from.
struct Walk<'a> {
    /// The storage whose bucket and meta store the walk reads.
    storage: &'a FsStorage,
    /// The bucket being listed.
    bucket: &'a Name,
    /// The listing's parameters — the one source of the prefix, the
    /// delimiter, the markers and the page size.
    params: &'a ListParams,
    /// The prefix, materialized once (every key is compared against it).
    prefix: String,
    /// How many keys one window covers: one per entry the page can hold,
    /// plus the one the truncation probe needs.
    budget: usize,
    /// The rolled-up groups already fed to the page — the walk's own mirror
    /// of [`version::paginate`]'s roll-up dedup, so a group's remaining
    /// members are skipped, never resolved.
    covered: HashSet<String>,
    /// The bound a rolled-up group pushed the walk past (see [`group_end`]).
    jump: Option<Cursor>,
    /// The entries the page is cut from, in the listing's order.
    entries: Vec<Entry>,
}

impl<'a> Walk<'a> {
    /// Start a walk of `storage`'s `bucket` under `params`.
    fn new(storage: &'a FsStorage, bucket: &'a Name, params: &'a ListParams) -> Self {
        Self {
            storage,
            bucket,
            params,
            prefix: params.prefix.clone().unwrap_or_default(),
            budget: params.max_keys.saturating_add(1),
            covered: HashSet::new(),
            jump: None,
            entries: Vec::new(),
        }
    }

    /// The first key the listing can contain: the later of the prefix (no
    /// listed key lies below it) and the `key_marker`. The marker's own key
    /// is **included** — a bare `key_marker` skips the key whole inside
    /// [`version::paginate`] (which needs no entry of it), and a
    /// `version_id_marker` resumes inside it (which needs its entries up to
    /// the named one).
    fn start_key(&self) -> String {
        match self.params.key_marker.as_deref() {
            Some(marker) if marker > self.prefix.as_str() => marker.to_owned(),
            _ => self.prefix.clone(),
        }
    }

    /// Read one window at `cursor` (one blocking-pool hop, one snapshot).
    async fn read(&self, cursor: &Cursor) -> Result<Window, Error> {
        let handle = self.storage.handle().clone();
        let bucket = self.bucket.clone();
        let cursor = cursor.clone();
        let budget = self.budget;
        handle
            .read_blocking(move |txn| read_window(txn, bucket.as_ref().as_str(), &cursor, budget))
            .await
            .map_err(Error::from)
    }

    /// The bound the next window starts at: the window's own resume bound,
    /// pushed past the rolled-up groups the walk has already fed the page.
    fn next_cursor(&mut self, window: &Window) -> Option<Cursor> {
        let jump = self.jump.take();
        window.next.clone().map(|next| next.later(jump))
    }

    /// The window's keys the page may take entries from, in key order: the
    /// keys that pass the listing's own filters — prefix, key domain, the
    /// reserved and folder-marker rules, and the key a bare `key_marker`
    /// skips whole. Built before any resolution, so a key the page can never
    /// reach costs no open; a delimiter group's already-fed members are
    /// dropped later, when the group is marked in (which can only be known
    /// once a member has answered).
    fn candidates(&self, window: &Window) -> Vec<Candidate> {
        let delimiter = self.params.delimiter.as_deref();
        window
            .keys
            .iter()
            .enumerate()
            .filter_map(|(index, window_key)| {
                let raw = window_key.key.as_str();
                if !raw.starts_with(&self.prefix) {
                    return None; // outside the listing's prefix
                }
                let Ok(key) = object::key(raw) else {
                    return None; // a domain-invalid key is skipped, like a listing
                };
                if key.is_reserved() || key.is_folder_marker() {
                    return None;
                }
                if self.params.version_id_marker.is_none()
                    && self.params.key_marker.as_deref() == Some(raw)
                {
                    return None; // the whole key is behind the marker
                }
                let rolled = delimiter
                    .and_then(|delimiter| storage::common_prefix(raw, &self.prefix, delimiter))
                    .map(str::to_owned);
                Some(Candidate { index, key, rolled })
            })
            .collect()
    }

    /// Feed one window to the page: the keys that can contribute entries are
    /// resolved in order under [`RESOLVE_CONCURRENCY`], and a rolled-up
    /// group is marked in as soon as one of its keys yields — the group's
    /// remaining members are then skipped (and jumped over), because
    /// [`version::paginate`] drops them as duplicates of the group's prefix.
    async fn absorb(&mut self, window: &Window) -> Result<(), Error> {
        let candidates = self.candidates(window);
        let mut pending: FuturesOrdered<BoxFuture<'_, Result<Vec<Entry>, Error>>> =
            FuturesOrdered::new();
        // The candidates whose resolution is in flight or folded, in the
        // order they were pushed (the fold's own order).
        let mut pushed: Vec<usize> = Vec::new();
        let (mut next, mut folded) = (0, 0);
        while folded < pushed.len() || next < candidates.len() {
            // Keep the pipe full — at most [`RESOLVE_CONCURRENCY`] keys in
            // flight — and fold the oldest result, so the entries land in
            // the window's key order whatever order the resolutions settle
            // in, and the error of the first failing key, in that order, is
            // the answer (never whichever failure settled first).
            while next < candidates.len() && pending.len() < RESOLVE_CONCURRENCY {
                let candidate = &candidates[next];
                next += 1;
                // A group an earlier member of this same window has already
                // fed the page: the rest of its keys are duplicates
                // `paginate` drops, so the walk never opens them.
                if let Some(rolled) = &candidate.rolled
                    && self.covered.contains(rolled)
                {
                    continue;
                }
                let WindowKey { slot, rows, .. } = &window.keys[candidate.index];
                let (storage, bucket) = (self.storage, self.bucket);
                let (slot, key) = (slot.as_ref(), &candidate.key);
                pending.push_back(Box::pin(async move {
                    entries_of(storage, bucket, key, slot, rows).await
                }));
                pushed.push(next - 1);
            }
            let Some(entries) = pending.next().await else {
                break; // every candidate of the window was skipped
            };
            let entries = entries?;
            let candidate = &candidates[pushed[folded]];
            folded += 1;
            if entries.is_empty() {
                continue; // the key has no version to list
            }
            if let Some(rolled) = &candidate.rolled {
                let window_key = &window.keys[candidate.index].key;
                // The exception is the key a `version_id_marker` resumes
                // inside: the page may drop every entry of it (an unmatched
                // marker), so the group is not counted in until a member the
                // drop cannot reach has been read. One extra member's
                // resolution is the whole cost, and its entries are
                // duplicates the page drops anyway.
                let resumed = self.params.version_id_marker.is_some()
                    && self.params.key_marker.as_deref() == Some(window_key.as_str());
                if !resumed {
                    self.covered.insert(rolled.clone());
                    self.jump = match (self.jump.take(), group_end(rolled)) {
                        (Some(jump), Some(end)) => Some(jump.later(Some(end))),
                        (None, Some(end)) => Some(end),
                        (jump, None) => jump,
                    };
                }
            }
            self.entries.extend(entries);
        }
        Ok(())
    }
}

/// The entries one key contributes, in the listing's order: its current
/// version — resolved by the read path's own slot, or the recorded delete
/// marker when the file is gone — and then its noncurrent versions newest
/// first.
///
/// `resolve_object_slot` is the call a `GET`/`HEAD` of the key makes (the
/// path policy, one open, the handle's own size/stamp/identity, the gated row
/// and its heal), so the listing can never disagree with the read about the
/// current version: a drifted file is the null version, never the stale
/// identifier, and its ETag is the content-derived one the heal wrote. The
/// file's absence is that resolution's own `NoSuchKey` — no second path stat
/// to choose the arm — and what it means is the row's shape: a row whose
/// **recorded** marker flag is set is the delete marker the API created, a
/// row still describing content is an out-of-band removal, so the mirror wins
/// and the key has no current version to list, its history rows (if any)
/// listing it alone. The flag is never inferred from the absence or from the
/// row's shape: a zero-byte object's row is shape-identical to a marker's on
/// a filesystem without a file identity. A resolve failure that is anything
/// else propagates instead of silently dropping the key from the page — as
/// does a key whose path the policy refuses — and the file that vanishes
/// under the page is the same absence, with the key left to its history rows.
///
/// The noncurrent rows are `Row::is_history`'s: the stamp is the authority —
/// a superseded row is stamped noncurrent by the state transaction that moved
/// the current slot off it — with the slot knowledge below for the rows no
/// stamp reached. In the crash window the outgoing version's row is already
/// committed while the version is still current: it is unstamped and sits at
/// the current slot's own sequence, so it is not history and listing it would
/// serve two "latest" versions of one key (Review Focus 1). The match is the
/// *slot's* sequence, never `0`: a superseded null version's row sits at the
/// null's own sequence (0 on the fresh-key path, the free sequence a later
/// null took when 0 was occupied), and a current null does not make the row
/// at 0 a duplicate of itself — hiding that row would drop the superseded
/// null version from the listing entirely.
async fn entries_of(
    storage: &FsStorage,
    bucket: &Name,
    key: &object::Key,
    slot: Option<&Stored>,
    rows: &[rows::Row],
) -> Result<Vec<Entry>, Error> {
    // Only a key with a current-slot row has a current version to resolve:
    // a key that exists as history alone has none (its file may exist out of
    // band — the meta row is what makes a current version), so its rows all
    // list as history.
    let current = match slot {
        None => None,
        Some(stored) => match storage.resolve_object_slot(bucket, key).await {
            // The sequence is the slot's own recorded one, whatever the
            // gated identity: a current whose identity the gate replaced
            // with the null version still records the sequence it was
            // written with.
            Ok((_file, size, mtime, row, version_id)) => Some(Entry {
                key: key.clone(),
                id: version_id,
                seq: row.version_seq,
                is_delete_marker: false,
                size,
                last_modified: mtime,
                etag: row.etag,
            }),
            Err(Error::Storage(storage::Error::NoSuchKey(_))) => {
                if stored.is_delete_marker {
                    Some(Entry {
                        key: key.clone(),
                        id: stored.version_id.clone(),
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
        },
    };
    let mut entries: Vec<Entry> = Vec::new();
    let slot = match current {
        Some(entry) => {
            let slot = rows::Slot::At(entry.seq);
            entries.push(entry);
            slot
        }
        // The key has no current version at all: its file went out of band,
        // or vanished under the page. That is not "no slot fact" — every
        // version row is history and must list, a row an aborted publish or
        // an out-of-band removal left unstamped at a sequence no slot holds
        // included: it is a version like any other, never a key's second
        // "latest".
        None => rows::Slot::Gone,
    };
    let mut older: Vec<&rows::Row> = rows.iter().filter(|row| row.is_history(slot)).collect();
    older.sort_by_key(|row| Reverse(row.seq));
    for row in older {
        entries.push(entry_of(key, row));
    }
    Ok(entries)
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
