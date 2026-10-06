//! The ETag metadata store (task T039, migrated to redb per meta-redb-spec).
//!
//! Entries live in the `OBJECT_META` table of `<state-dir>/meta.redb`:
//! key `(bucket, key)`, value `(etag hex, size, mtime unix nanos, file
//! identity, tags wire, checksum wire, owner wire, acl wire, version id
//! wire, version sequence)` (spec 2026-08-31 — the object row also records
//! the API-written tag set and the write-time checksum; spec 2026-09-05 —
//! the owner and ACL elements, the owner riding its own wire; spec
//! 2026-10-03-s3-versioning — the current version's identity: the version
//! id's wire form, the literal `null` for the null version, and its
//! per-key sequence). The composite key keeps one bucket's entries
//! contiguous, so `walk` and `remove_bucket` are cheap prefix range scans.
//!
//! Entries are served only when size + mtime match the object file
//! (FR-022); otherwise the ETag is recomputed streaming and the entry
//! rewritten. The tags/checksum/owner/ACL elements and the current-version
//! identity are NOT recomputable from the object file: a content-derived
//! rewrite (an ETag recompute) carries the replaced row's elements over —
//! they describe the object as last written through the API, and only an
//! API write (PUT/copy/complete/delete/tag op) clears them. Redb
//! transactions replace the old temp+rename writes under an in-process
//! lock: single-entry operations are one short transaction, and a bucket
//! removal is one atomic range deletion.
//!
//! `tinio-core` domain types stay serde-free (constitution I); the stored
//! value uses plain strings and is validated into the domain types on read.
//! A domain-invalid stored value is reported as missing (the caller
//! recomputes from the object file and rewrites — self-healing, FR-022);
//! a domain-invalid tags/checksum element heals to empty/`None` on the
//! same read (the row's etag is still served); under redb's table-level
//! consistency a single bad entry no longer exists on its own.
//!
//! The pipeline stage adds three primitives over this store (pipeline-spec.md
//! §3.2): [`Store::set_batch`] / [`Store::set_batch_owned`] (one write
//! transaction per batch, per-entry last-write-wins — the write-pipeline
//! task's commit), and the gating-load pair [`Store::load_entries`] /
//! [`Store::load_bucket`] (one read transaction each, rows aligned with
//! `get()` semantics and exposing the file identity — the producers'
//! gate, P2/R1; the full-bucket form is the scanner's materialized
//! snapshot — one short transaction loaded into memory before the walk,
//! §3.7 whole-bucket-in-memory, never a held-open window).

use std::{
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
};

pub use object::{Key, key};

pub use crate::bucket::{Name, name};
use crate::{
    _core::{
        acl,
        acl::{Acl, AclGrants},
        checksum,
        etag::ETag,
        from_nanos, object, to_nanos, version,
    },
    _store::{bucket_versioning, meta, object_part},
    Error, bucket,
    database::{Error as DatabaseError, Handle, for_bucket_strict},
    etag::{self, HashBuffer},
};

/// The jitter-window fallback when no file identity is available (a
/// stored identity of `0` — platforms without one, or filesystems
/// without file IDs): a same-size mtime drift within this window is
/// treated as a touch (antivirus/indexer) and keeps a multipart
/// `MD5-of-MD5s-N` ETag; a larger drift is re-hashed. Where the file
/// identity exists (unix dev+inode; Windows volume serial + file index)
/// the comparison is exact and this window is unused — an in-place
/// same-size rewrite cannot be distinguished from a touch without
/// hashing, so any mtime drift re-hashes there (correctness first, F04;
/// a touch degrades the composed form but a wrong ETag is never served).
pub(crate) const COMPOSED_MTIME_JITTER: Duration = Duration::from_secs(60);

/// A tag rewrite's answer: the row the write left behind plus the
/// bucket's records-versions bit, both read in the rewrite's own
/// transaction. The backend gates the row's identity against the object
/// file (the read paths' staleness rule) and answers the contract's
/// `TagWriteResult` — no second read anywhere.
#[derive(Debug)]
pub struct TagRow {
    /// The row the tag write left behind (its other elements verbatim).
    pub(crate) row: meta::Stored,
    /// Whether the bucket records versions (read off the same write
    /// transaction's state table).
    pub(crate) records_versions: bool,
}

/// The absolute time difference between two instants (symmetric — an
/// mtime drift into the past counts the same as into the future).
pub(crate) fn mtime_drift(a: SystemTime, b: SystemTime) -> Duration {
    a.duration_since(b)
        .unwrap_or_else(|_| b.duration_since(a).unwrap_or_default())
}

/// The composed-ETag keep decision (P1 — the single home of the rule,
/// shared by `Store::ensure_etag` and the IO-pipeline compute task
/// `etag::ComputeTask`, F27): whether a stored `MD5-of-MD5s-N` ETag on the
/// same-size file may be kept without re-hashing.
///
/// On platforms with a file identity (both identities nonzero) the check
/// is exact: the identity must match AND the mtime must be unchanged —
/// any drift re-hashes, because an in-place same-size rewrite keeps the
/// identity and is indistinguishable from a touch without hashing (F04:
/// serving a stale composed ETag forever is worse than re-hashing a
/// touch and degrading the composed form to the content MD5). Where no
/// identity exists (stored or current `0`), the mtime jitter window is
/// the fallback — a drift within [`COMPOSED_MTIME_JITTER`] is treated as
/// a touch (documented risk: a same-size in-place rewrite inside the
/// window is kept). One bounded platform limitation on both branches:
/// Windows `FILETIME` clock granularity (~16 ms) means a rewrite landing
/// in the SAME tick as the recorded mtime is indistinguishable — the
/// stale ETag is then served until the next mtime or size change.
pub(crate) fn composed_keep(
    stored_identity: u64,
    stored_mtime: u64,
    current_identity: u64,
    current_mtime: SystemTime,
) -> bool {
    if stored_identity != 0 && current_identity != 0 {
        current_identity == stored_identity
            && mtime_drift(current_mtime, from_nanos(stored_mtime)) == Duration::ZERO
    } else {
        mtime_drift(current_mtime, from_nanos(stored_mtime)) <= COMPOSED_MTIME_JITTER
    }
}

/// The composed-ETag keep GATE — the shape + size conjunct of the P1
/// rule (the identity/mtime decision is [`composed_keep`]): whether
/// `etag` is a composed form recorded for a file of `stored_size` that
/// is still `size` bytes. One home for the conjunct, shared by
/// [`Store::ensure_etag`] and the IO-pipeline compute task
/// `etag::ComputeTask` — a size change is a content change, never a touch
/// (F27).
pub(crate) fn composed_gate(etag: &ETag, stored_size: u64, size: u64) -> bool {
    matches!(etag, ETag::Composed(_, _)) && stored_size == size
}

/// The streaming content MD5 of `path` plus the hash-time size, mtime,
/// and file identity of the hashed handle: [`etag::md5_of_path`]
/// (the same symlink-policy open as the IO-pipeline compute task, R3 —
/// torn-file verified, F19) awaited directly — the hash IO runs on the
/// tokio blocking pool, reading into a pooled 64 KiB buffer
/// ([`etag::HashBuffer`], item 4 — one allocation per hash concurrency,
/// never per request).
async fn md5_of_file(
    path: &Path,
    follow_symlinks: bool,
) -> Result<([u8; 16], u64, SystemTime, u64), Error> {
    let mut buf = HashBuffer::acquire();
    etag::md5_of_path(path, follow_symlinks, buf.as_mut()).await
}

/// Whether a stored `(size, mtime, identity)` still matches the object
/// file (FR-022 — served only on a match; else recomputed). The single
/// home of the rule: [`Record::matches`], the hot-path tuple reads, and
/// the producer enqueue gate (pipeline-spec.md §3.2) share it.
///
/// The file identity closes the mtime-preserving-replacement hole (F01):
/// a same-size replacement that restores the mtime (`cp -p`, `rsync -a`)
/// is invisible to the size+mtime pair but changes the identity — a gate
/// that never consults it would serve the old ETag forever. The identity
/// is compared only when both sides expose one (a `0` on either side —
/// no platform identity, or an identity-less filesystem — falls back to
/// the size+mtime pair alone).
pub(crate) fn entry_matches(
    stored_size: u64,
    stored_mtime: u64,
    stored_identity: u64,
    size: u64,
    mtime: SystemTime,
    identity: u64,
) -> bool {
    stored_size == size
        && stored_mtime == to_nanos(mtime)
        && (stored_identity == 0 || identity == 0 || stored_identity == identity)
}

/// What a reader found at the key path: the file's own stamp (the reader's
/// open or stat read it), or nothing at all. The staleness gate's second
/// input beside the recorded row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Content {
    /// The file the key resolves to, as the caller's own open or stat read
    /// it — never a second path stat of a file the caller already holds.
    Present {
        /// The file's byte count.
        size: u64,
        /// The file's modification time.
        mtime: SystemTime,
        /// The file's identity (`0` when the platform or the filesystem has
        /// none).
        identity: u64,
    },
    /// No file: the path holds nothing. A delete marker's key, or a file
    /// removed out of band while the row still described it. Only the
    /// version adapter's current-slot gate (a `versioning`-gated module) has
    /// a missing file to report: every plain read resolves the file it
    /// serves and reports `NoSuchKey` when there is none, so a stripped build
    /// never constructs this arm.
    #[cfg_attr(not(feature = "versioning"), allow(dead_code))]
    Missing,
}

/// The staleness gate's verdict on a recorded row — whether the identity the
/// row records still names what the key holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The recorded identity still names the content: serve it. A row whose
    /// **recorded marker flag** is set and whose file is gone is this verdict
    /// too — a marker holds no content, so there is nothing for it to drift
    /// from, and its identity is the current version itself.
    Live,
    /// The recorded identity is stale: the content drifted from the row
    /// ([`entry_matches`]), or the file is gone while the row still described
    /// content (an out-of-band removal — the mirror wins). The version
    /// degrades to the null version, and the recorded identity is the row's
    /// to clear.
    Degraded,
}

/// The staleness gate — the one home of the decision every current-slot
/// reader takes: the plain read's `resolve_object_info` (which serves the
/// gated identity and hands the rewrite the stale one) and the version
/// adapter's `current_with_row` (the engine's view of the current slot) both
/// call it, so the two can never disagree about what is current. The leaf
/// comparison is [`entry_matches`]; this is what the two readers make of it,
/// including what a **missing** file means (the row's recorded marker flag,
/// read and never inferred from the row's shape — a zero-byte object's row is
/// shape-identical to a marker's where the filesystem has no identity).
pub(crate) fn gate(stored: &meta::Stored, content: Content) -> Verdict {
    match content {
        Content::Present {
            size,
            mtime,
            identity,
        } => {
            if entry_matches(
                stored.size,
                stored.mtime,
                stored.file_identity,
                size,
                mtime,
                identity,
            ) {
                Verdict::Live
            } else {
                Verdict::Degraded
            }
        }
        Content::Missing if stored.is_delete_marker => Verdict::Live,
        Content::Missing => Verdict::Degraded,
    }
}

/// A validated meta record (the parsed form of the stored entry).
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_fs::meta::Record;
///
/// let record = Record {
///     key: "dir/file.txt".into(),
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     size: 4,
///     mtime: 0,
/// };
/// assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Object key (validated).
    pub key: object::Key,
    /// ETag (single MD5 or composed `-N` form).
    pub etag: ETag,
    /// Object size in bytes at record time.
    pub size: u64,
    /// Object mtime in unix nanoseconds at record time.
    pub mtime: u64,
}

impl Record {
    /// Whether the recorded size + mtime still match the object file
    /// (FR-022 — served only on a match; else recomputed). The record
    /// hides the file identity by design — the identity-aware gate lives
    /// on the store's hot-path reads and the producer enqueues.
    pub fn matches(&self, size: u64, mtime: SystemTime) -> bool {
        entry_matches(self.size, self.mtime, 0, size, mtime, 0)
    }
}

/// One row of the gating-load **traversal** (pipeline-spec.md P2/R1):
/// the key plus its stored entry when present and domain-valid. `stored`
/// is `None` when the row is missing **or its etag is domain-invalid** —
/// both are reported as missing so the caller recomputes from the object
/// file and rewrites (self-healing), never a hard error per row. The
/// batched point-read form ([`Store::load_entries`]) returns bare
/// `stored` slots instead — its rows are index-aligned with the
/// caller's own keys, which are new information only here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatedMeta {
    /// The row's key.
    pub key: object::Key,
    /// The validated stored entry, when present and valid.
    pub stored: Option<meta::Stored>,
}

/// One entry of a [`Store::set_batch`] write batch (pipeline-spec.md
/// §3.2): the producers (list/scanner) build it from an IO-pipeline
/// compute result plus the walk-time size/mtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEntry {
    /// The object key.
    pub key: object::Key,
    /// The (re)computed ETag.
    pub etag: ETag,
    /// The object size in bytes at walk time.
    pub size: u64,
    /// The object mtime at walk time.
    pub mtime: SystemTime,
    /// The file identity at hash time (`0` marks an unavailable platform
    /// identity; see `fsutil::file_identity`).
    pub identity: u64,
}

/// The ETag metadata store of a state dir.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::{ETag, bucket, object};
/// use tinio_fs::meta;
/// use tokio::runtime::Runtime;
/// let state = tempfile::tempdir().unwrap();
/// let store = meta::store(state.path()).unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let key = object::key("dir/file.txt").unwrap();
/// let etag = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
/// Runtime::new().unwrap().block_on(async {
///     store
///         .set(&bucket, &key, &etag, 4, SystemTime::UNIX_EPOCH, 0)
///         .await
///         .unwrap();
///     let record = store.get(&bucket, &key).await.unwrap().unwrap();
///     assert_eq!(record.etag, etag);
///     assert_eq!(record.size, 4);
///     assert!(record.matches(4, SystemTime::UNIX_EPOCH));
/// });
/// ```
#[derive(Debug, Clone)]
pub struct Store {
    /// The shared state-database handle (the redb single writer replaces
    /// the old in-process lock).
    handle: Arc<Handle>,
}

impl Store {
    /// Create a store over a shared state-database handle (the `FsStorage`
    /// construction path — one handle across all stores).
    pub(crate) fn from_handle(handle: Arc<Handle>) -> Self {
        Self { handle }
    }

    /// The stored [`meta::Stored`] of `key` — the hot-path read, without
    /// building a [`Record`] (no key clone).
    /// A domain-invalid stored value cannot be trusted: it is reported as
    /// missing so the caller recomputes the ETag from the object file and
    /// rewrites the entry (self-healing, FR-022) instead of failing every
    /// read of that object with 500.
    ///
    /// The version engine's adapter reads the whole row too — it needs the
    /// current version identity, the recorded identity the staleness gate
    /// compares, and the metadata elements a superseded version carries into
    /// its row.
    pub(crate) async fn stored_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<meta::Stored>, Error> {
        self.handle
            .read(|txn| {
                meta::Table::open_readonly(txn)?
                    .get(bucket, key)
                    .map_err(Into::into)
            })
            .map_err(Into::into)
    }

    /// The stored record for `key`, if any (unvalidated against the object
    /// file — the caller compares via [`Record::matches`]).
    pub async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Record>, Error> {
        let Some(stored) = self.stored_entry(bucket, key).await? else {
            return Ok(None);
        };
        Ok(Some(Record {
            key: key.clone(),
            etag: stored.etag,
            size: stored.size,
            mtime: stored.mtime,
        }))
    }

    /// The ETag of `key` when the stored entry still matches the object
    /// file (`size` + `mtime` + `identity` — F01), else `None` (recompute
    /// needed, FR-022). `identity` is the file identity of the same
    /// handle the caller's `size`/`mtime` came from (`0` when
    /// unavailable — the identity is then not consulted).
    pub async fn etag_matching(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Result<Option<ETag>, Error> {
        let Some(stored) = self.stored_entry(bucket, key).await? else {
            return Ok(None);
        };
        if entry_matches(
            stored.size,
            stored.mtime,
            stored.file_identity,
            size,
            mtime,
            identity,
        ) {
            Ok(Some(stored.etag))
        } else {
            Ok(None)
        }
    }

    /// The ETag of an object file at `path`, ensuring a matching entry —
    /// the `ensure_row` row's etag: the stored entry when it
    /// still matches, else the content MD5 recomputed streaming. Returns
    /// the ETag and whether it was (re)computed.
    #[allow(clippy::too_many_arguments)]
    pub async fn ensure_etag(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        follow_symlinks: bool,
    ) -> Result<(ETag, bool), Error> {
        let (row, recomputed) = self
            .ensure_row(
                bucket,
                key,
                path,
                size,
                mtime,
                identity,
                follow_symlinks,
                None,
            )
            .await?;
        Ok((row.etag, recomputed))
    }

    /// The full ensured row of an object file at `path`: the stored entry
    /// when it still matches (`size` + `mtime` + `identity` — F01), else
    /// the entry the rewrite left — the content MD5 recomputed streaming
    /// with the replaced row's tags/checksum elements carried over
    /// (FR-022 + spec 2026-08-31: the elements are NOT recomputable from
    /// content, so a content-derived rewrite preserves them — they
    /// describe the object as last written through the API; only an API
    /// write clears them). Returns the validated row (the etag, tags and
    /// checksum the read paths serve) and whether it was (re)written —
    /// one read drives the decision, so a stale entry costs one read,
    /// not two. `identity` is the file identity of the same handle the
    /// caller's `size`/`mtime` came from (`0` when unavailable) — the
    /// gate never opens the path itself. The re-hash opens the file under
    /// the `follow_symlinks` policy (nofollow when disabled — a swap to a
    /// symlink between the metadata check and the hash is rejected, R3)
    /// and persists the hash-time metadata (F19).
    ///
    /// `stale` is the current-version identity the caller's staleness gate
    /// judged invalid; the rewrite that answers this call applies the clear
    /// in the same write transaction, so the row this call returns can never
    /// still name a version the content no longer is (see
    /// [`Self::rewrite_preserving`]). Callers with no gate verdict pass
    /// `None` — the identity then rides the row exactly as it always has.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn ensure_row(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        follow_symlinks: bool,
        stale: Option<&version::Id>,
    ) -> Result<(meta::Stored, bool), Error> {
        // The replaced row is kept in hand for the whole call: the tail's
        // read-only stand-in carries its elements over exactly as the
        // rewrite's own read would have (`not_written_back`), so the two
        // answers agree.
        let mut previous = self.stored_entry(bucket, key).await?;
        if let Some(stored) = previous.take() {
            if entry_matches(
                stored.size,
                stored.mtime,
                stored.file_identity,
                size,
                mtime,
                identity,
            ) {
                return Ok((stored, false));
            }
            // The composed-ETag keep decision (P1, [`composed_keep`] —
            // one home for the rule, F27): on platforms with a file
            // identity the identity must match AND the mtime must be
            // unchanged — any drift re-hashes, because an in-place
            // same-size rewrite keeps the identity and is
            // indistinguishable from a touch without hashing (F04: a
            // stale composed ETag is never served forever). Where no
            // identity exists, the mtime jitter window is the fallback.
            // The caller's `size`/`mtime`/`identity` describe the open
            // file — no path re-stat on this branch (data-path review
            // 2026-08-29, finding 5).
            if composed_gate(&stored.etag, stored.size, size)
                && composed_keep(stored.file_identity, stored.mtime, identity, mtime)
            {
                let Carried {
                    tags,
                    checksum,
                    owner,
                    acl,
                    version_id,
                    version_seq,
                } = self
                    .rewrite_preserving(bucket, key, &stored.etag, size, mtime, identity, stale)
                    .await
                    .or_else(|err| not_written_back(Some(&stored), stale, err))?;
                return Ok((
                    meta::Stored {
                        etag: stored.etag,
                        size,
                        mtime: to_nanos(mtime),
                        file_identity: identity,
                        tags,
                        checksum,
                        owner,
                        acl,
                        version_id,
                        version_seq,
                        is_delete_marker: false,
                    },
                    false,
                ));
            }
            // The stale-entry case falls through to the shared tail; the
            // row it replaced rides along for the carry-over.
            previous = Some(stored);
        }
        // One recompute-and-rewrite tail for both the stale-entry and
        // missing-entry cases (the matching and composed-keep branches
        // return above) — the rewrite rule has ONE home (F19).
        let (digest, size, mtime, identity) = md5_of_file(path, follow_symlinks).await?;
        let etag = ETag::Single(digest);
        let Carried {
            tags,
            checksum,
            owner,
            acl,
            version_id,
            version_seq,
        } = self
            .rewrite_preserving(bucket, key, &etag, size, mtime, identity, stale)
            .await
            .or_else(|err| not_written_back(previous.as_ref(), stale, err))?;
        Ok((
            meta::Stored {
                etag,
                size,
                mtime: to_nanos(mtime),
                file_identity: identity,
                tags,
                checksum,
                owner,
                acl,
                version_id,
                version_seq,
                is_delete_marker: false,
            },
            true,
        ))
    }

    /// The ETag of an object file at `path`: the stored entry when it still
    /// matches (`size` + `mtime` + `identity` — F01), else the content
    /// MD5 recomputed streaming and the entry rewritten (FR-022).
    #[allow(clippy::too_many_arguments)]
    pub async fn etag_for_file(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        path: &Path,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        follow_symlinks: bool,
    ) -> Result<ETag, Error> {
        Ok(self
            .ensure_etag(bucket, key, path, size, mtime, identity, follow_symlinks)
            .await?
            .0)
    }

    /// Store (or overwrite) the entry for `key` — one write transaction,
    /// the content-derived rewrite form (`rewrite_preserving`):
    /// the etag/size/mtime/identity columns are replaced and the replaced
    /// row's tags/checksum/owner/ACL elements and current-version identity
    /// are carried over (empty/`None` + `0` when the row is absent).
    /// `identity` is the file identity at record time (see
    /// `fsutil::file_identity`); `0` marks an unavailable platform
    /// identity.
    pub async fn set(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Result<(), Error> {
        self.rewrite_preserving(bucket, key, etag, size, mtime, identity, None)
            .await
            .map(|_| ())
    }

    /// One write transaction: read the row being replaced, then
    /// re-insert it with `etag`/`size`/`mtime`/`identity` refreshed and
    /// the row's tags + checksum + owner + ACL + current-version identity
    /// elements carried over (empty/`None`/the private default/`None` +
    /// `0` when the row is absent) — the content-derived rewrite semantics
    /// (see the module doc). Returns the elements written, so the caller's
    /// response metadata matches the row exactly (the read-modify-write
    /// runs inside the single write transaction — redb's serialized
    /// writers make the carried-over elements the latest committed at
    /// rewrite time). Dropping the version identity here would make a
    /// versioned object look unversioned after an ETag heal or a tag
    /// write; it rides the row like every other element.
    ///
    /// `stale` is the identity the caller's staleness gate judged invalid
    /// (the read path's out-of-band-edit verdict). The row this
    /// transaction writes must not keep naming it — the gate judged the
    /// content gone, and a row that still carried the identifier would
    /// match on the next read and resurrect it. The clear rides the
    /// shared [`carried`] rule, whose compare-and-set runs inside this
    /// same transaction, so a concurrent write that installed its own
    /// live version is never cleared by a reader that judged the previous
    /// content.
    #[allow(clippy::too_many_arguments)]
    async fn rewrite_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
        stale: Option<&version::Id>,
    ) -> Result<Carried, Error> {
        // The closure runs on the blocking pool (`Handle::write` is
        // async, G3 revision) — clone the borrowed captures into it.
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        let stale = stale.cloned();
        self.handle
            .write(move |txn| {
                let mut table = meta::Table::open(txn)?;
                let row = table.get(&bucket, &key)?;
                // The carry-over rule, from its one home (`carried`): the
                // row this transaction re-read is the one the write
                // preserves, and the gate's verdict (`stale`) is applied
                // here, where the row is written.
                let carried = carried(row.as_ref(), stale.as_ref());
                table.put(
                    &bucket,
                    &key,
                    &meta::Stored {
                        etag: etag.clone(),
                        size,
                        mtime: to_nanos(mtime),
                        file_identity: identity,
                        tags: carried.tags.clone(),
                        checksum: carried.checksum.clone(),
                        owner: carried.owner.clone(),
                        acl: carried.acl.clone(),
                        version_id: carried.version_id.clone(),
                        version_seq: carried.version_seq,
                        is_delete_marker: false,
                    },
                )?;
                Ok(carried)
            })
            .await
            .map_err(Into::into)
    }

    /// The stored tag set of `key` — empty when the row is absent (a
    /// hand-dropped object has never been tagged through the API) or its
    /// wire is domain-invalid (self-healing).
    pub async fn tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<object::Tags, Error> {
        Ok(self
            .stored_entry(bucket, key)
            .await?
            .map_or_else(object::Tags::empty, |row| row.tags))
    }

    /// Replace the stored tag set of `key`, preserving the row's other
    /// elements — one read-modify-write transaction (last-writer-wins
    /// against concurrent writes, like every tagging op — spec
    /// 2026-08-31: tagging ops take no per-key lock). Answers the
    /// rewritten row and the bucket's records-versions bit ([`TagRow`]),
    /// both read in the SAME transaction — the tag write's answer, no
    /// second read — or `None` when no row was updated: a missing row is
    /// not created here — the object file exists but its row is absent (a
    /// hand-dropped object), and the caller heals the row first (the
    /// etag is not derivable without a content hash).
    pub async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<Option<TagRow>, Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let tags = tags.clone();
        self.handle
            .write_if(move |txn| {
                let row = {
                    let mut table = meta::Table::open(txn)?;
                    if table.put_tags(&bucket, &key, &tags)?.is_none() {
                        return Ok(None);
                    }
                    table.get(&bucket, &key)?
                };
                let Some(row) = row else {
                    return Ok(None);
                };
                let records_versions = bucket_versioning::Table::open(txn)?
                    .get(bucket.as_ref().as_str())?
                    .unwrap_or_default()
                    .records_versions();
                Ok(Some(TagRow {
                    row,
                    records_versions,
                }))
            })
            .await
            .map_err(Into::into)
    }

    /// `set_tags` of an object file whose row may be missing (a
    /// hand-dropped object): the plain tag update when the row exists,
    /// else the content-hash heal + tag insert in ONE write
    /// transaction — the first tagging of a hand-dropped object costs
    /// one fsync, not three. The hash-time metadata rules the inserted
    /// row (F19, like `ensure_row`); a row that appears between the
    /// probe and the write is tag-updated, never clobbered. A file that
    /// vanishes before the hash surfaces as the hash's `NotFound`
    /// (the caller maps it to `NoSuchKey`). Answers the written row and
    /// the bucket state ([`TagRow`]), the state read riding the same
    /// transaction.
    pub(crate) async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        path: &Path,
        follow_symlinks: bool,
        tags: &object::Tags,
    ) -> Result<TagRow, Error> {
        if let Some(written) = self.set_tags(bucket, key, tags).await? {
            return Ok(written);
        }
        let (digest, size, mtime, identity) = md5_of_file(path, follow_symlinks).await?;
        let etag = ETag::Single(digest);
        let bucket = bucket.clone();
        let key = key.clone();
        let tags = tags.clone();
        self.handle
            .write(move |txn| {
                // The bucket state first: the write handle borrows the
                // transaction exclusively, so each table is a separate
                // pass (the shared-handle rule).
                let records_versions = bucket_versioning::Table::open(txn)?
                    .get(bucket.as_ref().as_str())?
                    .unwrap_or_default()
                    .records_versions();
                let mut table = meta::Table::open(txn)?;
                // A row that appeared during the hash keeps its checksum,
                // owner, ACL and current-version identity (the
                // etag/size/mtime/identity are the hash-time ones — the row
                // describes the same file read at hash time).
                let (checksum, owner, acl, version_id, version_seq) =
                    table.get(&bucket, &key)?.map_or_else(
                        || (None, None, Acl::default_private(None), None, 0),
                        |row| {
                            (
                                row.checksum,
                                row.owner,
                                row.acl,
                                row.version_id,
                                row.version_seq,
                            )
                        },
                    );
                let stored = meta::Stored {
                    etag: etag.clone(),
                    size,
                    mtime: to_nanos(mtime),
                    file_identity: identity,
                    tags: tags.clone(),
                    checksum,
                    owner,
                    acl,
                    version_id,
                    version_seq,
                    is_delete_marker: false,
                };
                table.put(&bucket, &key, &stored)?;
                Ok(TagRow {
                    row: stored,
                    records_versions,
                })
            })
            .await
            .map_err(Into::into)
    }

    /// Clear the stored tag set of `key`, preserving the row's other
    /// elements (idempotent — a missing row is a no-op, and an
    /// already-empty set commits nothing). Answers the cleared row and
    /// the bucket's records-versions bit ([`TagRow`]), read in the SAME
    /// transaction — or `None` when no row was there to clear.
    pub async fn clear_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<TagRow>, Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.handle
            .write_if(move |txn| {
                let row = {
                    let mut table = meta::Table::open(txn)?;
                    if table.clear_tags(&bucket, &key)?.is_none() {
                        return Ok(None);
                    }
                    table.get(&bucket, &key)?
                };
                let Some(row) = row else {
                    return Ok(None);
                };
                let records_versions = bucket_versioning::Table::open(txn)?
                    .get(bucket.as_ref().as_str())?
                    .unwrap_or_default()
                    .records_versions();
                Ok(Some(TagRow {
                    row,
                    records_versions,
                }))
            })
            .await
            .map_err(Into::into)
    }

    /// The stored ACL of `key` — the owner element recombined with the
    /// grant set (the row form keeps them apart); the private default
    /// with no owner when the row is absent (a hand-dropped object has
    /// never had an ACL written) or its wires are domain-invalid
    /// (self-healing).
    pub async fn acl(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Acl, Error> {
        Ok(self.stored_entry(bucket, key).await?.map_or_else(
            || Acl::default_private(None),
            |row| Acl {
                owner: row.owner,
                grants: row.acl.grants,
            },
        ))
    }

    /// Replace the stored grant set of `key` (S3 PutObjectAcl —
    /// replace-all, no merge), preserving the row's other elements —
    /// the owner element included, a put never changes it. One
    /// read-modify-write transaction, mirroring [`Self::set_tags`]
    /// (last-writer-wins against concurrent writers; no per-key lock).
    /// Returns whether a row was updated: a missing row is not created
    /// here (the object file exists but its row is absent — a
    /// hand-dropped object; the row self-heals on the next read with the
    /// private default, matching the tags op's contract).
    pub async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        grants: &AclGrants,
    ) -> Result<bool, Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let fresh = Acl {
            owner: None,
            grants: grants.clone(),
        };
        self.handle
            .write_if(move |txn| {
                let mut table = meta::Table::open(txn)?;
                let Some(row) = table.get(&bucket, &key)? else {
                    // Nothing to change — no commit (no fsync).
                    return Ok(None);
                };
                // The row's other elements — the current-version identity
                // included — ride the update untouched.
                table.put(
                    &bucket,
                    &key,
                    &meta::Stored {
                        acl: fresh.clone(),
                        ..row
                    },
                )?;
                Ok(Some(()))
            })
            .await
            .map(|wrote| wrote.is_some())
            .map_err(Into::into)
    }

    /// Upsert a whole batch in ONE write transaction, per-entry
    /// last-write-wins (a duplicated key in `entries` is settled by its
    /// last occurrence; pipeline-spec.md §3.2). Each entry carries its
    /// file identity at record time (see `fsutil::file_identity`;
    /// `0` marks an unavailable platform identity). The batching policy
    /// lives in the producers (pipeline-spec.md task 4) — this is the
    /// primitive they call once per batch. An empty `entries` is a no-op:
    /// no write transaction is opened (the streaming flusher may flush an
    /// empty accumulator — a pointless write-lock + fsync is skipped).
    ///
    /// Slice form — the batch is cloned into the write closure (the
    /// closure must be `'static` for `spawn_blocking`, G3 revision);
    /// callers that own a `Vec` (the write-pipeline task) use
    /// [`Self::set_batch_owned`] to skip the clone (data-path review
    /// 2026-08-29, finding 2).
    pub async fn set_batch(
        &self,
        bucket: &bucket::Name,
        entries: &[BatchEntry],
    ) -> Result<(), Error> {
        self.set_batch_owned(bucket, entries.to_vec()).await
    }

    /// The by-value form of [`Self::set_batch`] — the batch is **moved**
    /// into the write closure instead of cloned. The
    /// `MetaWriteBatchTask` primitive: the task owns its `Vec` and must
    /// not re-copy it on the DB pipeline's only data path (the closure
    /// must be `'static` for `spawn_blocking`, G3 revision — the
    /// DB-pipeline worker is a blocking-model worker dedicated to DB
    /// writes, so the `spawn_blocking` hop there is acceptable, P1).
    ///
    /// Each entry is a content-derived rewrite (`rewrite_preserving`):
    /// the replaced row's tags/checksum/owner/ACL elements and
    /// current-version identity are carried over, so a producer recompute
    /// (a stale ETag refreshed by the list/scanner walk) never clears the
    /// API-written metadata nor makes a versioned object look unversioned.
    pub async fn set_batch_owned(
        &self,
        bucket: &bucket::Name,
        entries: Vec<BatchEntry>,
    ) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }
        let bucket = bucket.clone();
        self.handle
            .write(move |txn| {
                let mut table = meta::Table::open(txn)?;
                for entry in &entries {
                    let row = table.get(&bucket, &entry.key)?;
                    let tags = row
                        .as_ref()
                        .map_or_else(object::Tags::empty, |row| row.tags.clone());
                    let checksum = row.as_ref().and_then(|row| row.checksum.clone());
                    let owner = row.as_ref().and_then(|row| row.owner.clone());
                    let acl = row
                        .as_ref()
                        .map_or_else(|| Acl::default_private(None), |row| row.acl.clone());
                    let version_id = row.as_ref().and_then(|row| row.version_id.clone());
                    let version_seq = row.as_ref().map_or(0, |row| row.version_seq);
                    table.put(
                        &bucket,
                        &entry.key,
                        &meta::Stored {
                            etag: entry.etag.clone(),
                            size: entry.size,
                            mtime: to_nanos(entry.mtime),
                            file_identity: entry.identity,
                            tags: tags.clone(),
                            checksum: checksum.clone(),
                            owner: owner.clone(),
                            acl: acl.clone(),
                            version_id,
                            version_seq,
                            is_delete_marker: false,
                        },
                    )?;
                }
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    /// Remove the entry for `key` and its retained `OBJECT_PARTS` rows
    /// (idempotent — a missing entry is Ok; the parts rows die with the
    /// record, so a delete never leaves a completed object's part list
    /// behind).
    pub async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.handle
            .write(move |txn| {
                meta::Table::open(txn)?.remove(&bucket, &key)?;
                object_part::Table::open(txn)?.remove_key(&bucket, &key)?;
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    /// Remove every entry of `keys` — and each key's retained
    /// `OBJECT_PARTS` rows — in ONE write transaction (idempotent per
    /// key — a missing entry is Ok), the remove-side counterpart of the
    /// upsert batch [`Self::set_batch_owned`]: the scanner's orphan pass
    /// removes its confirmed orphans with one redb write-lock acquisition
    /// instead of one transaction per key (P04 — a mass out-of-band `rm`
    /// no longer serializes M write locks against live PUTs). The keys
    /// are **moved** into the write closure — the caller owns the `Vec`
    /// (data-path review 2026-08-29, finding 2 — no re-copy on the
    /// reclamation path). An empty `keys` is a no-op: no write
    /// transaction is opened.
    pub async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: Vec<object::Key>,
    ) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let bucket = bucket.clone();
        self.handle
            .write(move |txn| {
                // One handle per table (redb refuses a second open of one
                // table in a transaction); the two tables are drained in
                // separate passes — the handle borrows cannot overlap.
                {
                    let mut table = meta::Table::open(txn)?;
                    for key in &keys {
                        table.remove(&bucket, key)?;
                    }
                }
                {
                    let mut parts = object_part::Table::open(txn)?;
                    for key in &keys {
                        parts.remove_key(&bucket, key)?;
                    }
                }
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    /// The gating-load **batch point read** (pipeline-spec.md P2/R1) —
    /// the list producer's page read: one read transaction, `get` per
    /// requested key in request order, hot-path cost O(page). Returns
    /// one slot per requested key, **index-aligned with the request** —
    /// the caller maps slots back to its own keys (the list's page is
    /// already in key order; nothing is keyed by lookup). The keys are
    /// borrowed (`IntoIterator<Item = &Key>`), so a page's keys pass by
    /// reference — no intermediate key `Vec`, no per-row key clones
    /// (data-path review 2026-08-29, finding 3). Row semantics align
    /// with [`Self::get`]: a missing or domain-invalid etag reports
    /// `None` (the caller recomputes and rewrites — self-healing). The
    /// requested keys are already validated, so the key-domain skip of
    /// the traversal form cannot occur here.
    pub async fn load_entries<'a>(
        &self,
        bucket: &bucket::Name,
        keys: impl IntoIterator<Item = &'a object::Key>,
    ) -> Result<Vec<Option<meta::Stored>>, Error> {
        self.handle
            .read(|txn| {
                let table = meta::Table::open_readonly(txn)?;
                keys.into_iter()
                    .map(|key| table.get(bucket, key))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(Into::into)
            })
            .map_err(Into::into)
    }

    /// The gating-load **full-bucket traversal** (pipeline-spec.md
    /// P2/R1) — one read transaction over every row of `bucket`, in key
    /// order. Per-row [`Self::get`] semantics: a domain-invalid etag
    /// reports `stored: None` (missing — recompute and rewrite), a
    /// domain-invalid key skips the row — a corrupt row never fails the
    /// walk (unlike [`Self::walk`], which reports `CorruptMeta` and
    /// hides the file identity).
    ///
    /// **Synchronous by design (P3, data-path review 2026-08-27)**: the
    /// traversal materializes every row of the bucket (validation +
    /// allocation — hundreds of ms at 1M rows), so it must never be
    /// called from request code (sync, blocks the caller); the sync
    /// signature means any async caller must consciously wrap the call
    /// in `spawn_blocking`. The scanner does exactly that at bucket
    /// start — the materialized snapshot replaces the old held-open
    /// gating window (data-path review 2026-08-29, finding 1):
    /// identical snapshot semantics, but the short-lived transaction is
    /// released before the walk, so the DB pipeline's commits never
    /// collide with a pinned read transaction.
    pub fn load_bucket(&self, bucket: &bucket::Name) -> Result<Vec<GatedMeta>, Error> {
        self.handle
            .read(|txn| self.load_bucket_in(txn, bucket))
            .map_err(Into::into)
    }

    /// [`Self::load_bucket`]'s walk inside a **caller-provided** read
    /// transaction — the same rows, the same order, the same per-row
    /// self-healing semantics; the caller owns the transaction (and
    /// therefore the snapshot and the blocking-pool hop). The version
    /// listing reads the gated current slots and the bucket's version rows
    /// from one transaction this way, so its two tables are one point in
    /// time and one dispatch.
    pub(crate) fn load_bucket_in(
        &self,
        txn: &redb::ReadTransaction,
        bucket: &bucket::Name,
    ) -> Result<Vec<GatedMeta>, DatabaseError> {
        let table = meta::Table::open_readonly(txn)?;
        let mut out = Vec::new();
        table.for_bucket_gated(bucket, |key, stored| {
            out.push(GatedMeta { key, stored });
            Ok(())
        })?;
        Ok(out)
    }

    /// Walk every stored entry of `bucket` in key order (the scanner's
    /// reclamation pass and `doctor`'s meta-orphan check read this).
    /// Corrupt rows fail the walk (`CorruptMeta`) — the reclamation and
    /// doctor callers must not skip them.
    ///
    /// **Async via the blocking pool (P3, data-path review 2026-08-27)**:
    /// the walk is a full-bucket table scan (row-by-row validation +
    /// allocation), so the closure + read transaction run on the tokio
    /// blocking pool through `Handle::read_blocking` — the
    /// same G3 hop shape as the write path — and never block a runtime
    /// worker's request tasks.
    pub async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<Record>, Error> {
        let bucket = bucket.clone();
        self.handle
            .read_blocking(move |txn| {
                let table = meta::Table::open_readonly(txn)?;
                let mut out = Vec::new();
                for_bucket_strict(&table, &bucket, |key, etag, size, mtime| {
                    out.push(Record {
                        key,
                        etag,
                        size,
                        mtime,
                    });
                    Ok(())
                })?;
                Ok(out)
            })
            .await
            .map_err(Into::into)
    }

    /// Remove the whole meta subtree of `bucket` (one atomic range
    /// deletion). Test-only since the production teardown goes through
    /// [`FsStorage::remove_bucket_state`].
    #[cfg(test)]
    pub async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.clone();
        self.handle
            .write(move |txn| {
                meta::Table::open(txn)?
                    .drain_bucket(&bucket)
                    .map_err(Into::into)
            })
            .await
            .map_err(Into::into)
    }
}

/// The row elements a content-derived rewrite carries over from the row it
/// replaces — the one shape [`carried`] builds, which
/// [`Store::rewrite_preserving`] returns and [`not_written_back`] serves,
/// so the write's answer and the read-only stand-in can never describe
/// different values (FR-023 amended, design 2026-10-03 **Read paths**).
///
/// The tags, checksum, owner and ACL are the elements the API last wrote
/// (only an API write clears them — a content-derived rewrite preserves
/// them), and `version_id`/`version_seq` are the row's current-version
/// identity, cleared (`None` + `0`) when the caller's staleness gate judged
/// it invalid — the compare-and-set [`carried`] applies. A row that was not
/// there leaves every element at its default.
struct Carried {
    /// The carried-over tag set (empty when no row was there).
    tags: object::Tags,
    /// The carried-over checksum record.
    checksum: Option<checksum::Recorded>,
    /// The carried-over owner.
    owner: Option<acl::OwnerId>,
    /// The carried-over ACL (the private default when no row was there).
    acl: Acl,
    /// The carried-over current-version identity.
    version_id: Option<version::Id>,
    /// The carried-over current version's sequence number.
    version_seq: u64,
}

/// The carry-over rule a content-derived rewrite applies to the row it
/// replaces — **one home** for the decision, taken by the write closure
/// (which passes the row it re-reads inside its transaction) and by the
/// read-only stand-in [`not_written_back`] (which passes its `previous`),
/// so the two can never describe different values (FR-023 amended, design
/// 2026-10-03 **Read paths**).
///
/// The tags, checksum, owner and ACL ride the rewrite verbatim; the
/// current-version identity is a **compare-and-set**: it rides the row
/// only while the row still names what `stale` judged invalid — a
/// concurrent write that installed its own version in between moved the
/// field, and its identity is not this reader's to clear. A row that was
/// not there ([`None`], or an absent `previous`) leaves every element at
/// its default.
fn carried(row: Option<&meta::Stored>, stale: Option<&version::Id>) -> Carried {
    let Some(row) = row else {
        return Carried {
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: None,
            version_seq: 0,
        };
    };
    let cleared = stale.is_some() && row.version_id.as_ref() == stale;
    Carried {
        tags: row.tags.clone(),
        checksum: row.checksum.clone(),
        owner: row.owner.clone(),
        acl: row.acl.clone(),
        version_id: if cleared {
            None
        } else {
            row.version_id.clone()
        },
        version_seq: if cleared { 0 } else { row.version_seq },
    }
}

/// The carry-over elements a **read-only** handle's refused rewrite serves
/// (FR-023 amended, design 2026-10-03 **Read paths**): the recomputation
/// is answered but "simply not written back".
///
/// The values are the ones [`Store::rewrite_preserving`] would have
/// written — the replaced row's elements through the shared [`carried`]
/// rule, and the absent row's defaults when there was no row. Every error
/// that is *not* the read-only refusal propagates unchanged: a read-only
/// run degrades, it does not swallow real failures.
fn not_written_back(
    previous: Option<&meta::Stored>,
    stale: Option<&version::Id>,
    err: Error,
) -> Result<Carried, Error> {
    if !err.is_read_only() {
        return Err(err);
    }
    Ok(carried(previous, stale))
}

/// Create a store over its **own** state database at `<state_dir>`.
///
/// Each call opens the `meta.redb` file exclusively — creating two
/// standalone stores (of any kind) over the same state dir at once fails
/// with `DatabaseAlreadyOpen`. Production code constructs one
/// [`crate::FsStorage`] per root and shares its single handle; this
/// constructor is for standalone/embedded use and tests.
///
/// # Errors
///
/// When the state database cannot be opened (a corrupt or unwritable
/// `meta.redb`).
#[inline]
pub fn store(state_dir: &Path) -> Result<Store, Error> {
    Ok(Store::from_handle(Handle::open(state_dir)?))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs::File, io::Write, thread::sleep, time::Duration};

    use prop::collection;
    use proptest::prelude::*;
    use tokio::fs;

    use super::*;
    use crate::{
        _core::{
            acl,
            acl::{Acl, AclGrants},
            bucket, object, version,
        },
        _store::meta::{Stored, Table},
        _util::testing::etag,
        database, fsutil, meta, testutil,
    };

    fn mtime(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// Inject an invalid etag value straight into the table (defensive
    /// read-path test — the public API can never write one). Runs on its
    /// own handle, dropped before the store opens the same file (redb's
    /// file lock is exclusive).
    fn corrupt_entry(state_dir: &Path, bucket: &str, key: &str) {
        let db = database::open(state_dir).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut table = Table::open(&mut txn).unwrap();
            table
                .insert(
                    (bucket, key),
                    ("not-an-etag", 1, 1, 0, "", "", "", "", "null", 0, false),
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }

    /// Inject a domain-invalid key row straight into the table (same
    /// defensive pattern as [`corrupt_entry`] — the public API can never
    /// write one).
    fn corrupt_key_entry(state_dir: &Path, bucket: &str, key: &str) {
        let db = database::open(state_dir).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut table = Table::open(&mut txn).unwrap();
            table
                .insert(
                    (bucket, key),
                    (
                        "d41d8cd98f00b204e9800998ecf8427e",
                        1,
                        1,
                        0,
                        "",
                        "",
                        "",
                        "",
                        "null",
                        0,
                        false,
                    ),
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }

    /// Plant one row carrying the current-version columns the version engine
    /// writes (`version_id` / `version_seq`). Runs on its own handle, dropped
    /// before the store opens the same file (redb's file lock is exclusive).
    fn plant_versioned_row(state_dir: &Path, bucket: &bucket::Name, key: &object::Key) {
        let db = database::open(state_dir).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut table = Table::open(&mut txn).unwrap();
            table
                .put(
                    bucket,
                    key,
                    &Stored {
                        etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
                        size: 4,
                        mtime: to_nanos(mtime(100)),
                        file_identity: 0,
                        tags: object::Tags::empty(),
                        checksum: None,
                        owner: None,
                        acl: Acl::default_private(None),
                        version_id: Some(version::id("v1").unwrap()),
                        version_seq: 7,
                        is_delete_marker: false,
                    },
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }

    /// The stored row's current-version columns, through the store's own
    /// handle (the exclusive redb file lock forbids a second opener).
    async fn stored_version(
        store: &Store,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> (Option<String>, u64) {
        let row = store.stored_entry(bucket, key).await.unwrap().unwrap();
        (
            row.version_id.map(|id| id.as_str().to_owned()),
            row.version_seq,
        )
    }

    /// The current-version columns ride every content-derived rewrite: an
    /// ETag heal (`set` and `ensure_row`'s paths share `rewrite_preserving`),
    /// a scanner batch write, a tag write and an ACL write must all carry
    /// the row's version identity — the mechanical `None`/`0` default would
    /// make a versioned object look unversioned after any of them.
    #[tokio::test]
    async fn content_derived_rewrites_preserve_the_version_identity() {
        let state = tempfile::tempdir().unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        plant_versioned_row(state.path(), &b, &k);
        let store = meta::store(state.path()).unwrap();
        let planted = (Some("v1".to_owned()), 7);
        assert_eq!(stored_version(&store, &b, &k).await, planted);

        // The content-derived rewrite (`ensure_row`'s heal paths share it).
        store
            .set(
                &b,
                &k,
                &etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                5,
                mtime(200),
                0,
            )
            .await
            .unwrap();
        assert_eq!(
            stored_version(&store, &b, &k).await,
            planted,
            "`set` carries the version identity"
        );

        // The batch write (the scanner/list ETag-heal path).
        store
            .set_batch(
                &b,
                &[meta::BatchEntry {
                    key: k.clone(),
                    etag: etag("e10adc3949ba59abbe56e057f20f883e"),
                    size: 6,
                    mtime: mtime(300),
                    identity: 0,
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            stored_version(&store, &b, &k).await,
            planted,
            "`set_batch` carries the version identity"
        );

        // The read-modify-write element updates.
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        assert!(store.set_tags(&b, &k, &tags).await.unwrap().is_some());
        assert_eq!(
            stored_version(&store, &b, &k).await,
            planted,
            "`set_tags` carries the version identity"
        );
        assert!(
            store
                .set_acl(&b, &k, &vec![group_read_grant()])
                .await
                .unwrap()
        );
        assert_eq!(
            stored_version(&store, &b, &k).await,
            planted,
            "`set_acl` carries the version identity"
        );
    }

    #[tokio::test]
    async fn set_get_round_trip() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("dir/file.txt").unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                4,
                mtime(100),
                0,
            )
            .await
            .unwrap();
        let record = store.get(&b, &k).await.unwrap().unwrap();
        assert_eq!(record.key, k);
        assert_eq!(record.etag, etag("d41d8cd98f00b204e9800998ecf8427e"));
        assert_eq!(record.size, 4);
        assert!(record.matches(4, mtime(100)));
        assert!(!record.matches(5, mtime(100)));
        assert!(!record.matches(4, mtime(101)));
    }

    #[tokio::test]
    async fn get_missing_is_none() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("nope.txt").unwrap();
        assert!(store.get(&b, &k).await.unwrap().is_none());
        assert_eq!(
            store.etag_matching(&b, &k, 0, mtime(0), 0).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn etag_matching_requires_size_mtime_and_identity() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        let e = etag("d41d8cd98f00b204e9800998ecf8427e");
        store.set(&b, &k, &e, 10, mtime(42), 7).await.unwrap();
        assert_eq!(
            store.etag_matching(&b, &k, 10, mtime(42), 7).await.unwrap(),
            Some(e.clone())
        );
        assert_eq!(
            store.etag_matching(&b, &k, 11, mtime(42), 7).await.unwrap(),
            None
        );
        assert_eq!(
            store.etag_matching(&b, &k, 10, mtime(43), 7).await.unwrap(),
            None
        );
        // F01: the identity is consulted — a mtime-preserving
        // replacement (same size + mtime, new file) must not match.
        assert_eq!(
            store.etag_matching(&b, &k, 10, mtime(42), 8).await.unwrap(),
            None
        );
        // A zero identity on either side falls back to size + mtime.
        assert_eq!(
            store.etag_matching(&b, &k, 10, mtime(42), 0).await.unwrap(),
            Some(e)
        );
    }

    #[tokio::test]
    async fn set_tags_rewrites_the_element_and_reports_no_update_on_missing() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        // A missing row: no update — the caller heals the row first
        // (ensure_tags) and the `None` answers "row was not updated".
        assert!(store.set_tags(&b, &k, &tags).await.unwrap().is_none());
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                3,
                mtime(1),
                7,
            )
            .await
            .unwrap();
        assert!(store.set_tags(&b, &k, &tags).await.unwrap().is_some());
        assert_eq!(store.tags(&b, &k).await.unwrap(), tags);
        // The row's other elements ride the rewrite.
        let row = store
            .load_entries(&b, [k.clone()].iter())
            .await
            .unwrap()
            .remove(0)
            .unwrap();
        assert_eq!(row.size, 3);
        assert_eq!(row.file_identity, 7);
        // An identical set still reports a row: the `Some` is the
        // "row exists" probe of [`Self::ensure_tags`] (it short-circuits
        // the hash-heal), and the table's change-detection already
        // skipped the row write.
        assert!(store.set_tags(&b, &k, &tags).await.unwrap().is_some());
    }

    /// `ensure_tags`'s missing-row branch: a hand-dropped object (a file
    /// with no row) is hashed and its row inserted with the requested tag
    /// set in ONE transaction. The second call takes the existing-row
    /// short-circuit, preserving every content-derived element the insert
    /// wrote.
    #[tokio::test]
    async fn ensure_tags_hashes_and_inserts_the_row_of_a_hand_dropped_object() {
        let state = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        let path = root.path().join("a.txt");
        fs::write(&path, b"hello").await.unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();

        assert!(
            store.stored_entry(&b, &k).await.unwrap().is_none(),
            "a hand-dropped object has no row to start from"
        );
        store
            .ensure_tags(&b, &k, &path, false, &tags)
            .await
            .unwrap();
        let row = store
            .stored_entry(&b, &k)
            .await
            .unwrap()
            .expect("the insert branch wrote the row");
        assert_eq!(
            row.etag.as_str(),
            "5d41402abc4b2a76b9719d911017c592",
            "the row's etag is the content's MD5"
        );
        assert_eq!(row.size, 5);
        assert_eq!(row.tags, tags, "the requested tags ride the insert");
        assert!(row.checksum.is_none(), "no checksum is invented");
        assert!(row.owner.is_none(), "no owner is invented");
        assert_eq!(
            row.acl,
            Acl::default_private(None),
            "the private default rides the insert"
        );
        assert!(row.version_id.is_none(), "no version identity is invented");
        assert_eq!(row.version_seq, 0);
        // The row describes the file it was hashed from (the F19 rules).
        let metadata = fs::metadata(&path).await.unwrap();
        assert!(entry_matches(
            row.size,
            row.mtime,
            row.file_identity,
            metadata.len(),
            metadata.modified().unwrap(),
            fsutil::file_identity(&path, &metadata),
        ));

        // The row now exists: the second call is the plain tag update, which
        // preserves the content-derived elements the insert wrote.
        let later = object::Tags::from_pairs([("env".into(), "staging".into())]).unwrap();
        store
            .ensure_tags(&b, &k, &path, false, &later)
            .await
            .unwrap();
        let updated = store.stored_entry(&b, &k).await.unwrap().unwrap();
        assert_eq!(updated.tags, later);
        assert_eq!(updated.etag, row.etag);
        assert_eq!(updated.size, row.size);
        assert_eq!(updated.mtime, row.mtime);
        assert_eq!(updated.file_identity, row.file_identity);
        assert_eq!(updated.version_id, row.version_id);
        assert_eq!(updated.version_seq, row.version_seq);
    }

    #[tokio::test]
    async fn clear_tags_is_idempotent_and_preserves_the_row() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        // A missing row: no-op.
        store.clear_tags(&b, &k).await.unwrap();
        let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                3,
                mtime(1),
                7,
            )
            .await
            .unwrap();
        store.set_tags(&b, &k, &tags).await.unwrap();
        store.clear_tags(&b, &k).await.unwrap();
        assert!(store.tags(&b, &k).await.unwrap().is_empty());
        let row = store
            .load_entries(&b, [k.clone()].iter())
            .await
            .unwrap()
            .remove(0)
            .unwrap();
        assert_eq!(row.size, 3, "the clear keeps the row's other elements");
        store.clear_tags(&b, &k).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn remove_deletes_and_is_idempotent() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                1,
                mtime(1),
                0,
            )
            .await
            .unwrap();
        store.remove(&b, &k).await.unwrap();
        assert!(store.get(&b, &k).await.unwrap().is_none());
        store.remove(&b, &k).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn remove_many_deletes_all_and_is_idempotent() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let keys: Vec<object::Key> = ["a.txt", "dir/b.txt", "c.txt"]
            .into_iter()
            .map(|k| object::key(k).unwrap())
            .collect();
        for key in &keys {
            store
                .set(
                    &b,
                    key,
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                    0,
                )
                .await
                .unwrap();
        }
        // One write transaction removes every entry; an empty batch opens
        // no transaction and a repeat batch is idempotent per key.
        store.remove_many(&b, vec![]).await.unwrap();
        store.remove_many(&b, keys.clone()).await.unwrap();
        assert!(store.walk(&b).await.unwrap().is_empty());
        store.remove_many(&b, keys).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn overwrite_replaces_entry() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                1,
                mtime(1),
                0,
            )
            .await
            .unwrap();
        store
            .set(
                &b,
                &k,
                &etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                2,
                mtime(2),
                0,
            )
            .await
            .unwrap();
        let record = store.get(&b, &k).await.unwrap().unwrap();
        assert_eq!(record.size, 2);
        assert_eq!(record.etag, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
    }

    #[tokio::test]
    async fn set_batch_round_trips_entries_with_identity() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let entries = vec![
            meta::BatchEntry {
                key: object::key("a.txt").unwrap(),
                etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
                size: 1,
                mtime: mtime(1),
                identity: 11,
            },
            meta::BatchEntry {
                key: object::key("dir/b.txt").unwrap(),
                etag: etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                size: 2,
                mtime: mtime(2),
                identity: 22,
            },
            meta::BatchEntry {
                key: object::key("dir/sub/c.txt").unwrap(),
                etag: etag("e10adc3949ba59abbe56e057f20f883e"),
                size: 3,
                mtime: mtime(3),
                identity: 33,
            },
        ];
        store.set_batch(&b, &entries).await.unwrap();
        // The point-read form is index-aligned with the request — the
        // slots are mapped back by position, not by key.
        let rows = store
            .load_entries(&b, entries.iter().map(|e| &e.key))
            .await
            .unwrap();
        assert_eq!(rows.len(), entries.len());
        for (row, entry) in rows.iter().zip(&entries) {
            let stored = row.as_ref().unwrap();
            assert_eq!(stored.etag, entry.etag);
            assert_eq!(stored.size, entry.size);
            assert_eq!(stored.mtime, to_nanos(entry.mtime));
            assert_eq!(stored.file_identity, entry.identity);
        }
        // The traversal form sees the same stored rows (same
        // transaction semantics, both forms of the gating load — R1).
        let walked: Vec<Option<Stored>> = store
            .load_bucket(&b)
            .unwrap()
            .into_iter()
            .map(|row| row.stored)
            .collect();
        assert_eq!(walked, rows);
    }

    #[tokio::test]
    async fn set_batch_last_write_wins() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let entries = vec![
            meta::BatchEntry {
                key: object::key("a.txt").unwrap(),
                etag: etag("d41d8cd98f00b204e9800998ecf8427e"),
                size: 1,
                mtime: mtime(1),
                identity: 11,
            },
            meta::BatchEntry {
                key: object::key("a.txt").unwrap(),
                etag: etag("5eb63bbbe01eeed093cb22bb8f5acdc3"),
                size: 2,
                mtime: mtime(2),
                identity: 22,
            },
        ];
        store.set_batch(&b, &entries).await.unwrap();
        let rows = store
            .load_entries(&b, [object::key("a.txt").unwrap()].iter())
            .await
            .unwrap();
        let stored = rows[0].as_ref().unwrap();
        assert_eq!(stored.etag, etag("5eb63bbbe01eeed093cb22bb8f5acdc3"));
        assert_eq!(stored.size, 2);
        assert_eq!(stored.mtime, to_nanos(mtime(2)));
        assert_eq!(stored.file_identity, 22);
    }

    #[tokio::test]
    async fn set_batch_empty_is_a_no_op() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        store.set_batch(&b, &[]).await.unwrap();
        assert!(store.walk(&b).await.unwrap().is_empty());
        store
            .set(
                &b,
                &object::key("a.txt").unwrap(),
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                1,
                mtime(1),
                0,
            )
            .await
            .unwrap();
        assert_eq!(store.walk(&b).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn load_entries_returns_each_requested_key_in_order() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        store
            .set(
                &b,
                &object::key("a.txt").unwrap(),
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                1,
                mtime(1),
                7,
            )
            .await
            .unwrap();
        let keys = [
            object::key("a.txt").unwrap(),
            object::key("missing.txt").unwrap(),
            object::key("b.txt").unwrap(),
        ];
        let rows = store.load_entries(&b, keys.iter()).await.unwrap();
        assert_eq!(rows.len(), keys.len());
        assert!(rows[0].is_some());
        assert!(rows[1].is_none());
        assert!(rows[2].is_none());
    }

    #[tokio::test]
    async fn load_entries_treats_a_corrupt_etag_as_missing() {
        let state = tempfile::tempdir().unwrap();
        corrupt_entry(state.path(), "data", "a.txt");
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let rows = store
            .load_entries(&b, [object::key("a.txt").unwrap()].iter())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_none(), "{:?}", rows[0]);
    }

    #[tokio::test]
    async fn load_bucket_walks_rows_in_key_order_with_identity() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        for (key_str, identity) in [("a.txt", 11u64), ("dir/b.txt", 22), ("z.txt", 33)] {
            store
                .set(
                    &b,
                    &object::key(key_str).unwrap(),
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                    identity,
                )
                .await
                .unwrap();
        }
        let rows = store.load_bucket(&b).unwrap();
        let seen: Vec<(&str, Option<u64>)> = rows
            .iter()
            .map(|r| (&*r.key, r.stored.as_ref().map(|s| s.file_identity)))
            .collect();
        assert_eq!(
            seen,
            [
                ("a.txt", Some(11)),
                ("dir/b.txt", Some(22)),
                ("z.txt", Some(33))
            ]
        );
    }

    #[tokio::test]
    async fn load_bucket_skips_corrupt_keys_and_heals_corrupt_etags() {
        let state = tempfile::tempdir().unwrap();
        corrupt_entry(state.path(), "data", "bad-etag.txt");
        corrupt_key_entry(state.path(), "data", "../evil");
        // A valid row to anchor the order.
        {
            let db = database::open(state.path()).unwrap().db;
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = Table::open(&mut txn).unwrap();
                table
                    .insert(
                        ("data", "a.txt"),
                        (
                            "d41d8cd98f00b204e9800998ecf8427e",
                            1,
                            1,
                            9,
                            "",
                            "",
                            "",
                            "",
                            "null",
                            0,
                            false,
                        ),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let rows = store.load_bucket(&b).unwrap();
        let seen: Vec<(&str, bool)> = rows.iter().map(|r| (&*r.key, r.stored.is_some())).collect();
        assert_eq!(seen, [("a.txt", true), ("bad-etag.txt", false)]);
        assert_eq!(rows[1].key.as_ref(), "bad-etag.txt");
        assert!(rows[1].stored.is_none());
    }

    #[tokio::test]
    async fn walk_returns_all_entries_and_remove_bucket_clears() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        for (i, key_str) in ["a.txt", "dir/b.txt", "dir/sub/c.txt"].iter().enumerate() {
            store
                .set(
                    &b,
                    &object::key(*key_str).unwrap(),
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    i as u64,
                    mtime(i as u64),
                    0,
                )
                .await
                .unwrap();
        }
        let mut keys: Vec<String> = store
            .walk(&b)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.key.to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, ["a.txt", "dir/b.txt", "dir/sub/c.txt"]);

        store.remove_bucket(&b).await.unwrap();
        assert!(store.walk(&b).await.unwrap().is_empty());
        store.remove_bucket(&b).await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn entries_with_unicode_keys() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("ümlaut/文件/with spaces.txt").unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                0,
                mtime(0),
                0,
            )
            .await
            .unwrap();
        let record = store.get(&b, &k).await.unwrap().unwrap();
        assert_eq!(record.key, k);
    }

    #[tokio::test]
    async fn bucket_scan_boundaries_exclude_other_buckets() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let data = bucket::name("data").unwrap();
        let data_x = bucket::name("data-x").unwrap();
        for (b, k) in [
            (&data, "a.txt"),
            (&data, "z.txt"),
            (&data_x, "a.txt"),
            (&data_x, "z.txt"),
        ] {
            store
                .set(
                    b,
                    &object::key(k).unwrap(),
                    &etag("d41d8cd98f00b204e9800998ecf8427e"),
                    1,
                    mtime(1),
                    0,
                )
                .await
                .unwrap();
        }
        let keys: Vec<String> = store
            .walk(&data)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.key.to_string())
            .collect();
        assert_eq!(keys, ["a.txt", "z.txt"]);
        let keys: Vec<String> = store
            .walk(&data_x)
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.key.to_string())
            .collect();
        assert_eq!(keys, ["a.txt", "z.txt"]);

        store.remove_bucket(&data).await.unwrap();
        assert!(store.walk(&data).await.unwrap().is_empty());
        assert_eq!(store.walk(&data_x).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn corrupt_entry_is_treated_as_missing() {
        let state = tempfile::tempdir().unwrap();
        // Corrupt before the store opens (the redb file lock is
        // exclusive per handle).
        corrupt_entry(state.path(), "data", "a.txt");
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        // Reported as missing (the caller recomputes from the object
        // file) — never a 500 on reads.
        assert!(store.get(&b, &k).await.unwrap().is_none());
        // walk() surfaces corrupt rows (scanner/doctor must not skip).
        assert!(store.walk(&b).await.is_err());
    }

    #[tokio::test]
    async fn corrupt_entry_self_heals_on_recompute() {
        let state = tempfile::tempdir().unwrap();
        let file = state.path().join("a.txt");
        fs::write(&file, b"hello").await.unwrap();
        // Corrupt the entry first, then the recompute path must
        // rewrite it.
        corrupt_entry(state.path(), "data", "a.txt");
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
                false,
            )
            .await
            .unwrap();
        assert_eq!(etag, ETag::from_content(b"hello"));
        assert!(store.get(&b, &k).await.unwrap().is_some());
    }

    fn owner() -> acl::OwnerId {
        acl::OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
            .unwrap()
    }

    /// A recorded row for the gate's own test: `size`/`mtime`/identity and
    /// the marker flag are the caller's, the rest is inert.
    fn stored_row(size: u64, mtime: u64, identity: u64, marker: bool) -> Stored {
        Stored {
            etag: ETag::from_content(b""),
            size,
            mtime,
            file_identity: identity,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: version::Id::from_wire("v1"),
            version_seq: 1,
            is_delete_marker: marker,
        }
    }

    #[test]
    fn the_gate_is_one_decision_over_the_four_states_it_distinguishes() {
        // The one home of the staleness verdict: the plain read
        // (`resolve_object_info`) and the version adapter's current slot
        // (`current_with_row`) both call it, so no reader may carry its own
        // copy of this table. Each arm here is a state the two readers must
        // answer identically.
        let row = stored_row(4, 1_000, 7, false);
        let at = |size, mtime, identity| Content::Present {
            size,
            mtime,
            identity,
        };

        // Matching: the recorded identity names the file.
        assert_eq!(
            gate(
                &row,
                at(4, SystemTime::UNIX_EPOCH + Duration::from_nanos(1_000), 7)
            ),
            Verdict::Live
        );
        // Matching without identities (the platform has none): the size+mtime
        // pair alone still answers.
        assert_eq!(
            gate(
                &stored_row(4, 1_000, 0, false),
                at(4, SystemTime::UNIX_EPOCH + Duration::from_nanos(1_000), 0)
            ),
            Verdict::Live
        );
        // Drifted: the size, the mtime or the identity moved. (The mtime
        // drift is a millisecond — a nanosecond would be inside the clock's
        // own granularity on Windows, whose FILETIME ticks are 100 ns.)
        assert_eq!(
            gate(
                &row,
                at(5, SystemTime::UNIX_EPOCH + Duration::from_nanos(1_000), 7)
            ),
            Verdict::Degraded
        );
        assert_eq!(
            gate(
                &row,
                at(
                    4,
                    SystemTime::UNIX_EPOCH + Duration::from_nanos(1_000_000),
                    7
                )
            ),
            Verdict::Degraded
        );
        assert_eq!(
            gate(
                &row,
                at(4, SystemTime::UNIX_EPOCH + Duration::from_nanos(1_000), 8)
            ),
            Verdict::Degraded,
            "a same-size, mtime-preserving replacement is caught by the identity"
        );
        // Missing: the file is gone. A row still describing content is an
        // out-of-band removal (degraded); a recorded marker holds no content
        // to drift from, so its identity stays live.
        assert_eq!(gate(&row, Content::Missing), Verdict::Degraded);
        assert_eq!(
            gate(&stored_row(0, 1_000, 0, true), Content::Missing),
            Verdict::Live,
            "the recorded marker flag, never the row's content shape, decides"
        );
    }

    fn group_read_grant() -> acl::Grant {
        acl::Grant {
            grantee: acl::Grantee::Group(acl::GroupUri(acl::GROUP_ALL_USERS.into())),
            permission: acl::Permission::Read,
        }
    }

    /// Inject garbage owner/ACL wires straight into the table (the same
    /// defensive pattern as [`corrupt_entry`] — the row's etag stays
    /// valid, so the row is served).
    fn corrupt_acl_entry(state_dir: &Path, bucket: &str, key: &str) {
        let db = database::open(state_dir).unwrap().db;
        let mut txn = db.begin_write().unwrap();
        {
            let mut table = Table::open(&mut txn).unwrap();
            table
                .insert(
                    (bucket, key),
                    (
                        "d41d8cd98f00b204e9800998ecf8427e",
                        1,
                        1,
                        0,
                        "a=b",
                        "",
                        "not-a-canonical-id",
                        "id=short,READ",
                        "null",
                        0,
                        false,
                    ),
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }

    #[tokio::test]
    async fn fs_object_acl_round_trip_and_replace() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        store
            .set(
                &b,
                &k,
                &etag("d41d8cd98f00b204e9800998ecf8427e"),
                4,
                mtime(100),
                0,
            )
            .await
            .unwrap();
        // A fresh row has no owner and the private default.
        assert_eq!(store.acl(&b, &k).await.unwrap(), Acl::default_private(None));
        // Put → Get round-trip (replace-all, no merge).
        let grants = vec![group_read_grant()];
        assert!(store.set_acl(&b, &k, &grants).await.unwrap());
        assert_eq!(store.acl(&b, &k).await.unwrap().grants, grants);
        let replaced: AclGrants = Vec::new();
        assert!(store.set_acl(&b, &k, &replaced).await.unwrap());
        assert_eq!(store.acl(&b, &k).await.unwrap().grants, replaced);
        // The row's other elements survive the ACL writes.
        let record = store.get(&b, &k).await.unwrap().unwrap();
        assert_eq!(record.etag, etag("d41d8cd98f00b204e9800998ecf8427e"));
        assert_eq!(record.size, 4);
        // A missing row is not created (the caller heals it first, the
        // tags op's contract).
        let missing = object::key("ghost.txt").unwrap();
        assert!(!store.set_acl(&b, &missing, &grants).await.unwrap());
        assert_eq!(
            store.acl(&b, &missing).await.unwrap(),
            Acl::default_private(None)
        );
    }

    #[tokio::test]
    async fn fs_object_acl_put_preserves_the_owner_element() {
        let state = tempfile::tempdir().unwrap();
        let owner = owner();
        {
            // A row written with a recorded owner (the backend commit
            // path writes it — the store has no owner API; inject the
            // wire directly, the defensive pattern).
            let db = database::open(state.path()).unwrap().db;
            let mut txn = db.begin_write().unwrap();
            {
                let mut table = Table::open(&mut txn).unwrap();
                table
                    .insert(
                        ("data", "a.txt"),
                        (
                            "d41d8cd98f00b204e9800998ecf8427e",
                            1,
                            1,
                            0,
                            "",
                            "",
                            owner.as_str(),
                            "",
                            "null",
                            0,
                            false,
                        ),
                    )
                    .unwrap();
            }
            txn.commit().unwrap();
        }
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        // get_object_acl recombines the owner element with the grants.
        assert_eq!(
            store.acl(&b, &k).await.unwrap().owner.as_ref(),
            Some(&owner)
        );
        // A put replaces the grants and never the owner.
        let grants = vec![group_read_grant()];
        assert!(store.set_acl(&b, &k, &grants).await.unwrap());
        let got = store.acl(&b, &k).await.unwrap();
        assert_eq!(got.owner.as_ref(), Some(&owner));
        assert_eq!(got.grants, grants);
    }

    #[tokio::test]
    async fn fs_object_acl_self_heals_on_garbage() {
        let state = tempfile::tempdir().unwrap();
        corrupt_acl_entry(state.path(), "data", "a.txt");
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("a.txt").unwrap();
        // Garbage owner/ACL wires self-heal to no owner and the private
        // default; the row itself is still served (its etag is valid).
        assert_eq!(store.acl(&b, &k).await.unwrap(), Acl::default_private(None));
        let record = store.get(&b, &k).await.unwrap().unwrap();
        assert_eq!(record.etag, etag("d41d8cd98f00b204e9800998ecf8427e"));
        // The tags element survives independently.
        assert_eq!(store.tags(&b, &k).await.unwrap().to_wire(), "a=b");
    }

    #[tokio::test]
    async fn composed_etag_kept_on_identity_less_storage_within_the_jitter_window() {
        // The identity-less fallback (stored identity 0): a touch within
        // the mtime jitter window is kept — the multipart
        // `MD5-of-MD5s-N` form is preserved (P1; the strict identity
        // check is impossible without an identity — the documented risk).

        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("mp.bin").unwrap();
        let file = state.path().join("mp.bin");
        fs::write(&file, b"hello").await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
        store
            .set(
                &b,
                &k,
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                0,
            )
            .await
            .unwrap();
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                0,
                false,
            )
            .await
            .unwrap();
        assert_eq!(etag, composed);
        assert!(matches!(etag, ETag::Composed(_, 2)));
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_touch_when_identity_is_available() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("mp.bin").unwrap();
        let file = state.path().join("mp.bin");
        fs::write(&file, b"hello").await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
        store
            .set(
                &b,
                &k,
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
            )
            .await
            .unwrap();
        let handle = File::options().write(true).open(&file).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(30))
            .unwrap();
        drop(handle);
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
                false,
            )
            .await
            .unwrap();
        assert_eq!(etag, ETag::from_content(b"hello"));
        assert!(matches!(etag, ETag::Single(_)));
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_in_place_same_size_rewrite() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("mp.bin").unwrap();
        let file = state.path().join("mp.bin");
        fs::write(&file, b"hello").await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
        store
            .set(
                &b,
                &k,
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
            )
            .await
            .unwrap();
        // Overwrite in place with different same-size content. The
        // sleep lands the rewrite in a later Windows FILETIME tick
        // (~16 ms clock granularity) — a rewrite within the SAME
        // tick as the original write keeps the old mtime and is
        // indistinguishable even from the strict keep (the documented
        // platform limitation).
        sleep(Duration::from_millis(30));
        let mut handle = File::options()
            .write(true)
            .truncate(true)
            .open(&file)
            .unwrap();
        handle.write_all(b"world").unwrap();
        handle.sync_all().unwrap();
        drop(handle);
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
                false,
            )
            .await
            .unwrap();
        assert_eq!(etag, ETag::from_content(b"world"));
    }

    #[tokio::test]
    async fn composed_etag_rehashes_on_replacement() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("mp.bin").unwrap();
        let file = state.path().join("mp.bin");
        fs::write(&file, b"hello").await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
        store
            .set(
                &b,
                &k,
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
            )
            .await
            .unwrap();
        // Replace with a fresh file (old mtime, so the fallback window
        // also rules out a touch).
        let replacement = state.path().join("replacement.bin");
        fs::write(&replacement, b"hello").await.unwrap();
        let handle = File::options().write(true).open(&replacement).unwrap();
        handle
            .set_modified(metadata.modified().unwrap() + Duration::from_secs(120))
            .unwrap();
        drop(handle);
        fs::rename(&replacement, &file).await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
                false,
            )
            .await
            .unwrap();
        // Re-hashed from the (unchanged) content — the composed form
        // is replaced by the content MD5.
        assert_eq!(etag, ETag::from_content(b"hello"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn composed_etag_rehashes_on_quick_replacement() {
        let state = tempfile::tempdir().unwrap();
        let store = meta::store(state.path()).unwrap();
        let b = bucket::name("data").unwrap();
        let k = object::key("mp.bin").unwrap();
        let file = state.path().join("mp.bin");
        fs::write(&file, b"hello").await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let composed = ETag::new("5d41402abc4b2a76b9719d911017c592-2").unwrap();
        store
            .set(
                &b,
                &k,
                &composed,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
            )
            .await
            .unwrap();
        let replacement = state.path().join("replacement.bin");
        fs::write(&replacement, b"hello").await.unwrap();
        // Fresh mtime — inside the old 60 s window.
        fs::rename(&replacement, &file).await.unwrap();
        let metadata = fs::metadata(&file).await.unwrap();
        let etag = store
            .etag_for_file(
                &b,
                &k,
                &file,
                metadata.len(),
                metadata.modified().unwrap(),
                fsutil::file_identity(&file, &metadata),
                false,
            )
            .await
            .unwrap();
        assert_eq!(etag, ETag::from_content(b"hello"));
    }

    // --- property tests (T028; moved inline from `tests/proptest_meta.rs`,
    // 2026-10-01) ---
    //
    // Randomized keys/ETags/sizes round-trip through the store; size/mtime
    // mismatches invalidate the entry (recompute needed, FR-022); concurrent
    // writers never produce torn JSON (atomic temp+rename under the
    // in-process lock).

    proptest! {
        #![proptest_config(testutil::proptest_config())]

        /// Arbitrary (valid) keys and ETag values round-trip exactly.
        #[test]
        fn entries_round_trip(
            segs in collection::vec("[a-zA-Z0-9_ -]{1,12}", 1..4),
            etag_hex in "[0-9a-f]{32}",
            size in 0u64..(1 << 40),
            secs in 0u64..1_000_000_000,
        ) {
            let key = object::key(segs.join("/")).unwrap();
            let etag = ETag::new(&etag_hex).unwrap();
            let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = meta::store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                store.set(&b, &key, &etag, size, mtime, 0).await.unwrap();
                let record = store.get(&b, &key).await.unwrap().unwrap();
                prop_assert_eq!(&record.key, &key);
                prop_assert_eq!(&record.etag, &etag);
                prop_assert_eq!(record.size, size);
                prop_assert!(record.matches(size, mtime));
                prop_assert!(!record.matches(size + 1, mtime));
                prop_assert!(!record.matches(size, mtime + Duration::from_secs(1)));
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(testutil::proptest_config())]

        /// Composed multipart ETags round-trip through the stored wire form.
        #[test]
        fn composed_etags_round_trip(hex in "[0-9a-f]{32}", parts in 1u32..10_000) {
            let etag = ETag::new(&format!("{hex}-{parts}")).unwrap();
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = meta::store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let key = object::key("big.bin").unwrap();
                store.set(&b, &key, &etag, 1, SystemTime::UNIX_EPOCH, 0).await.unwrap();
                let record = store.get(&b, &key).await.unwrap().unwrap();
                prop_assert_eq!(record.etag, etag);
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(testutil::proptest_config())]

        /// Concurrent writers of complete JSON payloads never tear: the final
        /// file is exactly one writer's payload.
        #[test]
        fn concurrent_writes_never_torn(payloads in collection::vec("[a-zA-Z0-9]{1,200}", 4..12)) {
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = meta::store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let key = object::key("shared.txt").unwrap();
                let mut handles = Vec::new();
                for payload in &payloads {
                    let store = store.clone();
                    let b = b.clone();
                    let key = key.clone();
                    let payload = payload.clone();
                    handles.push(tokio::spawn(async move {
                        let etag = ETag::from_content(payload.as_bytes());
                        store.set(&b, &key, &etag, payload.len() as u64, SystemTime::UNIX_EPOCH, 0).await.unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                let record = store.get(&b, &key).await.unwrap().unwrap();
                // The surviving entry is exactly one writer's (payload length
                // and etag agree).
                let ok = payloads.iter().any(|p| {
                    p.len() as u64 == record.size && record.etag == ETag::from_content(p.as_bytes())
                });
                prop_assert!(ok, "final record {:?} matches no writer", record);
                Ok(())
            })?;
        }
    }

    proptest! {
        #![proptest_config(testutil::proptest_config())]

        /// Concurrent writers on distinct keys never lose an entry: every
        /// committed write is visible afterwards (the redb single writer
        /// serializes them).
        #[test]
        fn concurrent_distinct_key_writes_all_persist(
            keys in collection::vec("[a-z0-9]{1,16}", 1..16),
        ) {
            // Deduplicate: "distinct keys" is the property under test.
            let keys: Vec<String> = {
                let mut seen = HashSet::new();
                keys.into_iter()
                    .filter(|k| seen.insert(k.clone()))
                    .collect()
            };
            let runtime = testutil::runtime();
            runtime.block_on(async {
                let state = tempfile::tempdir().unwrap();
                let store = meta::store(state.path()).unwrap();
                let b = bucket::name("data").unwrap();
                let mut handles = Vec::new();
                for (i, key) in keys.iter().enumerate() {
                    let store = store.clone();
                    let b = b.clone();
                    let key = object::key(format!("{key}.txt")).unwrap();
                    handles.push(tokio::spawn(async move {
                        let etag = ETag::new(&format!("{i:032x}")).unwrap();
                        store
                            .set(&b, &key, &etag, i as u64, SystemTime::UNIX_EPOCH, 0)
                            .await
                            .unwrap();
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                let records = store.walk(&b).await.unwrap();
                prop_assert_eq!(records.len(), keys.len(), "every write persists");
                Ok(())
            })?;
        }
    }
}
