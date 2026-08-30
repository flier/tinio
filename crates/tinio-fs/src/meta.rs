//! The ETag metadata store (task T039, migrated to redb per meta-redb-spec).
//!
//! Entries live in the `OBJECT_META` table of `<state-dir>/meta.redb`:
//! key `(bucket, key)`, value `(etag hex, size, mtime unix nanos)`. The
//! composite key keeps one bucket's entries contiguous, so `walk` and
//! `remove_bucket` are cheap prefix range scans.
//!
//! Entries are served only when size + mtime match the object file
//! (FR-022); otherwise the ETag is recomputed streaming and the entry
//! rewritten. Redb transactions replace the old temp+rename writes under an
//! in-process lock: single-entry operations are one short transaction, and
//! a bucket removal is one atomic range deletion.
//!
//! `tinio-core` domain types stay serde-free (constitution I); the stored
//! value uses plain strings and is validated into the domain types on read.
//! A domain-invalid stored value is reported as missing (the caller
//! recomputes from the object file and rewrites — self-healing, FR-022);
//! under redb's table-level consistency a single bad entry no longer
//! exists on its own.
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
    _core::{etag::ETag, from_nanos, object, to_nanos},
    Error, bucket,
    database::{self, Handle, ObjectMetaTable},
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
    pub stored: Option<database::StoredMeta>,
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

    /// The stored [`database::StoredMeta`] of `key` — the hot-path read, without
    /// building a [`Record`] (no key clone).
    /// A domain-invalid stored value cannot be trusted: it is reported as
    /// missing so the caller recomputes the ETag from the object file and
    /// rewrites the entry (self-healing, FR-022) instead of failing every
    /// read of that object with 500.
    async fn stored_entry(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<database::StoredMeta>, Error> {
        self.handle
            .read(|txn| ObjectMetaTable::open_readonly(txn)?.get(bucket, key))
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

    /// The ETag of an object file at `path`, ensuring a matching entry:
    /// the stored entry when it still matches (`size` + `mtime` +
    /// `identity` — F01), else the content MD5 recomputed streaming and
    /// the entry rewritten (FR-022). Returns the ETag and whether it was
    /// (re)computed — one read drives the decision, so a stale entry
    /// costs one read, not two. `identity` is the file identity of the
    /// same handle the caller's `size`/`mtime` came from (`0` when
    /// unavailable) — the gate never opens the path itself. The re-hash
    /// opens the file under the `follow_symlinks` policy (nofollow when
    /// disabled — a swap to a symlink between the metadata check and the
    /// hash is rejected, R3) and persists the hash-time metadata (F19).
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
        if let Some(stored) = self.stored_entry(bucket, key).await? {
            if entry_matches(
                stored.size,
                stored.mtime,
                stored.file_identity,
                size,
                mtime,
                identity,
            ) {
                return Ok((stored.etag, false));
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
                self.set(bucket, key, &stored.etag, size, mtime, identity)
                    .await?;
                return Ok((stored.etag, false));
            }
        }
        // One recompute-and-rewrite tail for both the stale-entry and
        // missing-entry cases (the matching and composed-keep branches
        // return above) — the rewrite rule has ONE home (F19).
        let (digest, size, mtime, identity) = md5_of_file(path, follow_symlinks).await?;
        let etag = ETag::Single(digest);
        self.set(bucket, key, &etag, size, mtime, identity).await?;
        Ok((etag, true))
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

    /// Store (or overwrite) the entry for `key` — one write transaction.
    /// `identity` is the file identity at record time (see
    /// `fsutil::file_identity`); `0` marks an unavailable
    /// platform identity.
    pub async fn set(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &ETag,
        size: u64,
        mtime: SystemTime,
        identity: u64,
    ) -> Result<(), Error> {
        // The closure runs on the blocking pool (`Handle::write` is
        // async, G3 revision) — clone the borrowed captures into it.
        let bucket = bucket.clone();
        let key = key.clone();
        let etag = etag.clone();
        self.handle
            .write(move |txn| {
                ObjectMetaTable::open(txn)?.put(&bucket, &key, &etag, size, mtime, identity)
            })
            .await
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
                let mut table = ObjectMetaTable::open(txn)?;
                for entry in &entries {
                    table.put(
                        &bucket,
                        &entry.key,
                        &entry.etag,
                        entry.size,
                        entry.mtime,
                        entry.identity,
                    )?;
                }
                Ok(())
            })
            .await
            .map_err(Into::into)
    }

    /// Remove the entry for `key` (idempotent — a missing entry is Ok).
    pub async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<(), Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        self.handle
            .write(move |txn| ObjectMetaTable::open(txn)?.remove(&bucket, &key))
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
    ) -> Result<Vec<Option<database::StoredMeta>>, Error> {
        self.handle
            .read(|txn| {
                let table = ObjectMetaTable::open_readonly(txn)?;
                keys.into_iter().map(|key| table.get(bucket, key)).collect()
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
            .read(|txn| {
                let table = ObjectMetaTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket_gated(bucket, |key, stored| {
                    out.push(GatedMeta { key, stored });
                    Ok(())
                })?;
                Ok(out)
            })
            .map_err(Into::into)
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
                let table = ObjectMetaTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_bucket(&bucket, |key, etag, size, mtime| {
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
            .write(move |txn| ObjectMetaTable::open(txn)?.drain_bucket(&bucket))
            .await
            .map_err(Into::into)
    }
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
    use std::{fs::File, io::Write, thread::sleep, time::Duration};

    use tokio::fs;

    use super::*;
    use crate::{
        _core::{bucket, object},
        _util::testing::etag,
        database, fsutil, meta,
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
            let mut table = ObjectMetaTable::open(&mut txn).unwrap();
            table
                .insert((bucket, key), ("not-an-etag", 1, 1, 0))
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
            let mut table = ObjectMetaTable::open(&mut txn).unwrap();
            table
                .insert((bucket, key), ("d41d8cd98f00b204e9800998ecf8427e", 1, 1, 0))
                .unwrap();
        }
        txn.commit().unwrap();
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
        let walked: Vec<Option<database::StoredMeta>> = store
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
                let mut table = ObjectMetaTable::open(&mut txn).unwrap();
                table
                    .insert(
                        ("data", "a.txt"),
                        ("d41d8cd98f00b204e9800998ecf8427e", 1, 1, 9),
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
}
