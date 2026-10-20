//! The content-addressed block plane: [`BlockStore`] over the `blocks` table.
//!
//! PG3 puts the store here; PG14 and PG19 fix its shape. A block's bytes are
//! written as 8 MiB `bytea` rows keyed `(hash, seq)`, and **every row carries
//! the block's declared length and the publish's timestamp**, because
//! [`BlockStore::list`] and [`Entry`] report both and neither can be derived from
//! the chunks alone.
//!
//! # Staging and publishing
//!
//! [`stage`](BlockStore::stage) files the block's bytes into a **temp file** as
//! they arrive, so the stage's resident memory is one write buffer whatever the
//! block's size. The trait hands `stage` no address and the address is only
//! known at `publish`, so the rows cannot be written to the database during
//! `write`; a temp file is what keeps a 1 GiB block from costing 1 GiB of RSS
//! for the whole PUT. Nothing reaches the database before the publish, so an
//! interrupted write is never served -- the trait's headline guarantee, and the
//! one a sweep can rely on.
//!
//! The temp lives in the plane's own stage directory ([`PgBlocks::temp_dir`]),
//! one file per stage named by a UUID. [`Drop`] unlinks it, so a published, a
//! refused and an abandoned stage all remove their own temp; the directory is
//! created `0700` by the serving user (the `tinio-util` `ensure_dir` primitive
//! `tinio-blob`'s `LocalBlocks` shares), and a temp a **crashed** stage left
//! behind is reaped by the next [`stage`](BlockStore::stage) -- an age-gated
//! sweep of the directory, the same rule the fs sweeper applies to its `tmp/`.
//!
//! [`publish`](BlockStore::publish) hashes the staged bytes to the address --
//! the digest accumulated as the bytes arrived, so there is no second read for
//! it -- then **reads the temp back** and writes the rows in bounded batches:
//! one statement carries at most [`PUBLISH_BATCH_ROWS`] 8 MiB rows, so neither
//! the parameter list nor the read window ever holds the whole block, which one
//! `bytea[]` argument could not do above the driver's ~1 GiB single-value cap
//! (PG14). Each batch is its own transaction, so the statement count follows
//! from the block's own rows rather than from the body.
//!
//! `ON CONFLICT (hash, seq) DO NOTHING` covers every row a publish finds: a
//! repeat writes the rows that are missing and never rewrites the ones that are
//! there. A repeat of a whole block therefore inserts nothing and answers
//! [`Published::Existing`] with the staged bytes dropped, while a repeat that
//! fills the gap left by a publish interrupted earlier inserts those rows and
//! answers [`Published::Stored`]. A filled gap cannot splice two bodies
//! together: the address *is* the hash of the bytes, so every row this store
//! writes under an address holds content that hashes to it, and `publish`
//! checks that before it writes anything. A row a publish finds is never
//! rewritten, so a block whose *stored* rows disagree with its declared length
//! is refused at [`open`](BlockStore::open) rather than corrected by a
//! publish.
//!
//! A zero-byte block -- the empty blob the engine stores as one block -- is a
//! single row with an empty chunk, so it is listed as a block of zero bytes
//! rather than read as an absent block.
//!
//! # Completeness
//!
//! [`contains`](BlockStore::contains) is one point read of the primary key and
//! answers whether **any** row of the address exists -- the dedup lookup, not a
//! promise that the block is whole, so a partial block answers present. The
//! read is what enforces completeness (PG19): a block is served only when every
//! row its declared length needs is present, which is
//! `(len - 1) / BLOCK_CHUNK_BYTES + 1` rows, a zero-byte block being one empty
//! row. [`open`](BlockStore::open) checks that before it serves a byte, so a
//! block missing a row is an error for a full read and for a range lying inside
//! the rows that survived alike, rather than a body that is not the block's
//! bytes.
//!
//! # Reads
//!
//! [`open`](BlockStore::open) reads the block's declared length, resolves the
//! optional [`ByteRange`] against it, and streams **only the chunk rows the
//! range touches**, slicing the first and the last. It fetches them a window of
//! `READ_WINDOW_ROWS` rows at a time and gives the pooled connection back
//! before a window's bytes are yielded, so a read's resident bytes are bounded
//! by a window rather than by the block and no connection is held for the whole
//! of a client's read (R4). A missing block is the contract's `NotFound` I/O
//! error, whose message names the block's hash; a range that cannot be
//! satisfied is [`crate::Error::InvalidRange`] and is never clamped; a stored
//! block whose chunk bytes do not add up to the window is an `UnexpectedEof` on
//! the body stream, never a silently short body.
//!
//! # Ordering
//!
//! `hash` is `text COLLATE "C"` and every ordered read repeats the collation
//! (Review Focus 1), so [`list`](BlockStore::list) and
//! [`walk`](BlockStore::walk) ascend by the address's UTF-8 bytes. `walk` is a
//! lazy keyset pagination: it reads one page of addresses per step, strictly
//! after its cursor, and yields every block exactly once; an `Err` item ends
//! the walk.
//!
//! Values are always bound parameters and every identifier is a fixed literal
//! (PG26); no statement is assembled from a value.

use std::{
    env, io, mem,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
    vec::IntoIter,
};

use async_trait::async_trait;
use bytes::Bytes;
use derive_more::Debug;
use futures::stream;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, query, query_file_as, query_scalar};
use time::OffsetDateTime;
use tinio_util::fsutil::{DirOwnerUid, ensure_dir, entries_of, remove_staged_temp};
use tokio::{
    fs,
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    sync::mpsc,
};
use uuid::Uuid;

use crate::{
    _core::{
        ByteRange,
        blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
        storage::{self, BodyStream},
    },
    Error, PgStore,
    timestamp::{from_timestamp, to_timestamp},
    window::{ChunkFault, ChunkWindow, chunk_windows},
};

/// The size of one `blocks` row's chunk (PG14): 8 MiB of the block's bytes.
pub const BLOCK_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// How many of a block's rows one publish statement carries (PG14, PG22): two
/// 8 MiB rows, so a publish's parameter list is bounded well below the driver's
/// single-value cap however large the block is, and a 1 GiB block is 64
/// statements.
pub const PUBLISH_BATCH_ROWS: usize = 2;

/// How many addresses one keyset page of a walk reads.
const WALK_PAGE: i64 = 256;

/// How many of a block's rows one read fetches at a time: two 8 MiB rows, so a
/// read's resident bytes are bounded by this window rather than by the block,
/// and the pooled connection it fetches them with goes back to the pool between
/// windows (R4).
const READ_WINDOW_ROWS: i32 = 2;

/// How long an unpublished stage temp is left before the ingest path reaps it:
/// ten minutes, the fs backend's block-staging grace (`block_grace`). A stage
/// that stalls past it -- a client that stopped sending -- is residue rather
/// than a live upload, so a crashed stage's temp cannot accumulate; a temp being
/// written is younger than the grace, so a live stage is never swept.
const STALE_TEMP_GRACE: Duration = Duration::from_secs(600);

/// The stage directory's leaf under the OS temp directory:
/// `<temp dir>/tinio-store-pg`.
const TEMP_DIR_LEAF: &str = "tinio-store-pg";

/// The PostgreSQL block store: one `blocks` relation, 8 MiB chunk rows.
///
/// # Examples
///
/// ```no_run
/// use bytes::Bytes;
/// use tinio_core::blob::{BlockHash, BlockStore, Published, StagedBlock};
/// use tinio_store_pg::{PgStore, blocks::PgBlocks};
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
/// let blocks = PgBlocks::new(store);
/// let hash = BlockHash::of(b"hello");
///
/// let mut staged = blocks.stage().await?;
/// staged
///     .write(Bytes::from_static(b"hello"))
///     .await
///     .expect("write");
/// // Against a live server this writes the bytes into the block's rows and
/// // answers whether the address is new or was already stored.
/// assert_eq!(blocks.publish(staged, &hash).await?, Published::Stored);
/// assert!(blocks.contains(&hash).await?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PgBlocks {
    store: PgStore,
    /// The directory the stage's temp files are created in: the plane's own
    /// stage directory, defaulting to `<temp dir>/tinio-store-pg` and settable
    /// through [`PgBlocks::with_temp_dir`].
    temp: PathBuf,
}

impl PgBlocks {
    /// Serve blocks over the store's connection pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_core::blob::{BlockHash, BlockStore};
    /// use tinio_store_pg::{PgStore, blocks::PgBlocks};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let blocks = PgBlocks::new(store);
    /// // Against a live server this call would open the store's pool and
    /// // answer whether that address is stored; the plane serves the whole
    /// // block contract off the one pool.
    /// let stored = blocks.contains(&BlockHash::of(b"hello")).await?;
    /// println!("stored: {stored}");
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(store: PgStore) -> Self {
        Self {
            store,
            temp: default_temp_dir(),
        }
    }

    /// Serve blocks over the store's pool, staging temp files under `temp`
    /// instead of the default directory.
    ///
    /// The default is the OS temp dir's `tinio-store-pg` leaf (see
    /// [`Self::temp_dir`]); a serving caller that keeps its own state directory
    /// — the pg run's synthetic root (`R8`) holds one — points this at a
    /// `tmp/` beneath it, so a run's stage residue lives in the same
    /// per-root tree as the rest of its state rather than in the shared temp
    /// directory.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{PgStore, blocks::PgBlocks};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let blocks = PgBlocks::with_temp_dir(store, "/var/lib/tinio/.tinio/tmp");
    /// assert_eq!(
    ///     blocks.temp_dir().to_str(),
    ///     Some("/var/lib/tinio/.tinio/tmp")
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_temp_dir(store: PgStore, temp: impl Into<PathBuf>) -> Self {
        Self {
            store,
            temp: temp.into(),
        }
    }

    /// The directory this plane stages temp files in.
    ///
    /// **Where a stage's bytes live, and who owns the directory.** The default
    /// is `<OS temp dir>/tinio-store-pg` — the OS temp dir is per-user on
    /// Windows, while on a shared unix `/tmp` the leaf is created `0700` by the
    /// serving user, so a second *user* there is refused the create and must
    /// name a directory of its own with [`Self::with_temp_dir`]. The store
    /// carries no filesystem root of its own (it is the database), which is why
    /// the plane stages here rather than under a run's state directory unless a
    /// caller hands it one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{PgStore, blocks::PgBlocks};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let blocks = PgBlocks::new(store);
    /// println!("staging under {}", blocks.temp_dir().display());
    /// # Ok(())
    /// # }
    /// ```
    pub fn temp_dir(&self) -> &Path {
        &self.temp
    }

    /// Borrow the store whose pool this plane runs on.
    pub fn store(&self) -> &PgStore {
        &self.store
    }
}

/// The plane's default stage directory: `<OS temp dir>/tinio-store-pg`.
fn default_temp_dir() -> PathBuf {
    env::temp_dir().join(TEMP_DIR_LEAF)
}

/// Remove the temps in `dir` older than [`STALE_TEMP_GRACE`], best-effort: the
/// residue of a stage whose process died, or one that stalled past the grace.
///
/// The sweep runs before each [`stage`](BlockStore::stage) creates its own temp,
/// so the file it is about to write is never a candidate, and a temp a live
/// stage holds is younger than the grace. A missing directory (nothing staged
/// yet) and a directory entry that vanishes underfoot are both no-ops: this is
/// housekeeping on the ingest path, never a reason to refuse a stage.
async fn sweep_stale_temps(dir: &Path) {
    let Ok(entries) = entries_of(dir).await else {
        return;
    };
    let now = SystemTime::now();
    for (path, name) in entries {
        let Ok(metadata) = fs::metadata(&path).await else {
            continue;
        };
        if metadata.is_dir() {
            continue;
        }
        let stale = metadata
            .modified()
            .ok()
            .and_then(|modified| now.duration_since(modified).ok())
            .is_some_and(|age| age >= STALE_TEMP_GRACE);
        if stale && let Err(err) = fs::remove_file(&path).await {
            tracing::warn!(error = %err, temp = %name, "a stale block stage temp could not be removed");
        }
    }
}

/// A block staged in a temp file for a later [`BlockStore::publish`].
///
/// The engine writes the block's bytes here, and they reach the database only
/// when the publish commits; the bytes are filed straight to disk, so the stage
/// holds one write buffer, never the block. [`Drop`] unlinks the temp, so an
/// abandoned stage leaves nothing behind; only a process that dies mid-stage
/// leaves residue, which the next [`stage`](BlockStore::stage) reaps.
///
/// A stage's own digest is accumulated as the bytes arrive, so
/// [`publish`](BlockStore::publish) checks the address without a second pass
/// over the file.
///
/// # Examples
///
/// ```no_run
/// use bytes::Bytes;
/// use tinio_core::blob::{BlockStore, StagedBlock};
/// use tinio_store_pg::{
///     PgStore,
///     blocks::{PgBlocks, Staged},
/// };
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// # let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
/// # let blocks = PgBlocks::new(store);
/// let mut staged: Staged = blocks.stage().await?;
/// staged.write(Bytes::from_static(b"hello")).await?;
/// // Nothing has reached the database yet: `PgBlocks::publish` is what turns
/// // the staged bytes into the block's rows.
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Staged {
    /// The temp file the block's bytes are filed in, with the handle
    /// [`BlockStore::publish`] takes so the file is closed before it is read
    /// back.
    #[debug(skip)]
    file: Option<BufWriter<File>>,
    /// The temp's path, unlinked by [`Drop`] on every path out.
    temp: PathBuf,
    /// The bytes written so far: the block's declared length.
    len: u64,
    /// The running hash of those bytes.
    #[debug(skip)]
    hasher: Sha256,
}

impl Staged {
    /// Flush and close the write handle, so the temp's bytes are on disk and the
    /// file can be reopened for the read-back.
    async fn close(&mut self) -> io::Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().await?;
            drop(file);
        }
        Ok(())
    }

    /// The temp's path, for the read-back.
    fn path(&self) -> &Path {
        &self.temp
    }

    /// The staged bytes' address digest, for the publish's check.
    fn digest(&self) -> [u8; 32] {
        self.hasher.clone().finalize().into()
    }
}

impl Drop for Staged {
    /// Best-effort: a stage that is abandoned, published or refused removes its
    /// own temp, so the only residue the directory ever holds is a process that
    /// died mid-stage (the next `stage` reaps it). The unlink is the shared
    /// [`tinio_util::fsutil::remove_staged_temp`] discipline: a drop can land on
    /// a runtime worker, so the removal runs on the blocking pool and never
    /// waits for it.
    fn drop(&mut self) {
        drop(self.file.take());
        remove_staged_temp(mem::take(&mut self.temp));
    }
}

#[async_trait]
impl StagedBlock for Staged {
    /// Append one chunk to the stage's temp file, hashing it as it goes. Nothing
    /// is buffered beyond the file's own write buffer, so the bytes of a block
    /// of any size are filed as they arrive.
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("the stage's file was already taken"))?;
        file.write_all(&chunk).await?;
        self.len += chunk.len() as u64;
        self.hasher.update(&chunk);
        Ok(())
    }
}

#[async_trait]
impl BlockStore for PgBlocks {
    type Error = Error;
    type Staged = Staged;

    async fn stage(&self) -> Result<Self::Staged, Error> {
        let dir = self.temp_dir();
        // The residue of a crashed or stalled stage is reaped here, before this
        // stage's own (fresh) temp exists, so the sweep never touches a live
        // stage's file.
        sweep_stale_temps(dir).await;
        let path = dir.join(Uuid::new_v4().to_string());
        // The directory exists after the first stage, so the file creation is
        // attempted first and only a missing directory pays for the create and
        // the retry. `ensure_dir` hardens a new component `0700`, so a staged
        // body is not readable by another user.
        let file = match File::create(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                ensure_dir(dir, DirOwnerUid::new(None))
                    .await
                    .map_err(storage::io)?;
                File::create(&path).await.map_err(storage::io)?
            }
            Err(err) => return Err(storage::io(err).into()),
        };
        Ok(Staged {
            file: Some(BufWriter::new(file)),
            temp: path,
            len: 0,
            hasher: Sha256::new(),
        })
    }

    async fn publish(
        &self,
        mut staged: Self::Staged,
        hash: &BlockHash,
    ) -> Result<Published, Error> {
        // The write handle closes before the temp is read back: the file's own
        // bytes are what the rows are assembled from.
        staged.close().await.map_err(storage::io)?;
        // The address check, on the digest the stage accumulated as the bytes
        // arrived: these bytes hash to the address they are published under.
        // That is what makes a gap a later publish fills safe -- every row under
        // an address holds content that hashes to it, so a filled gap cannot
        // splice two bodies. A mismatch leaves the temp for `staged`'s `Drop` to
        // unlink.
        if BlockHash::from_digest(staged.digest()) != *hash {
            return Err(address_mismatch(hash));
        }
        let hash = hash.to_hex();
        let len = staged.len;
        // One clock reading for the whole block: every row of it carries the
        // same publication time, which is what list and walk report.
        let published = to_timestamp(SystemTime::now());
        // Bounded batches of rows, one statement each and one transaction each:
        // the temp is read back one batch of [`PUBLISH_BATCH_ROWS`] rows at a
        // time, so neither the parameter list nor the read window holds the
        // whole block whatever the block's size. A failure between batches is
        // residue, never a served block: the rows that landed are refused until
        // a re-publish fills the gaps.
        let mut file = File::open(staged.path()).await.map_err(storage::io)?;
        let rows = chunk_count(len);
        let mut inserted = 0;
        let mut seq = 0u64;
        while seq < rows {
            let batch = (rows - seq).min(PUBLISH_BATCH_ROWS as u64);
            // The bytes this batch's rows need: `batch` chunks, the block's last
            // possibly short.
            let start = seq * BLOCK_CHUNK_BYTES as u64;
            let end = ((seq + batch) * BLOCK_CHUNK_BYTES as u64).min(len);
            let mut buf = vec![0u8; (end - start) as usize];
            file.read_exact(&mut buf).await.map_err(storage::io)?;
            // The rows the batch writes, in order: every row one chunk but the
            // block's last, which may be short (a zero-byte block is one empty
            // row). `&pieces as _` skips BYTEA[]'s `&[Vec<u8>]` check and binds
            // the borrowed slices; copying each row to satisfy that mapping is
            // not acceptable.
            let mut pieces: Vec<&[u8]> = Vec::with_capacity(batch as usize);
            let mut offset = 0usize;
            for _ in 0..batch {
                let take = (buf.len() - offset).min(BLOCK_CHUNK_BYTES);
                pieces.push(&buf[offset..offset + take]);
                offset += take;
            }
            let seqs: Vec<i32> = (0..batch as i32).map(|index| seq as i32 + index).collect();
            inserted += sqlx::query_file!(
                "sql/blocks/publish.sql",
                hash.as_str(),
                &seqs,
                &pieces as _,
                len as i64,
                published
            )
            .execute(self.store.as_driver())
            .await?
            .rows_affected();
            seq += batch;
        }
        // No row inserted means every chunk row of this address was already
        // there: the dedup hit, and the staged temp is dropped with `staged`.
        Ok(if inserted == 0 {
            Published::Existing
        } else {
            Published::Stored
        })
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Error> {
        // The dedup lookup: one point read of the primary key. It answers
        // whether a row of this address exists -- whether the block is *whole*
        // for its declared length is `open`'s rule, not this one's.
        let present = query_scalar!(
            "SELECT exists(SELECT 1 FROM blocks WHERE hash = $1) AS \"present!\"",
            hash.to_hex()
        )
        .fetch_one(self.store.as_driver())
        .await?;
        Ok(present)
    }

    async fn open(&self, hash: &BlockHash, range: Option<ByteRange>) -> Result<BodyStream, Error> {
        let pool = self.store.as_driver();
        // One encoding of the address for every statement of this read.
        let hex = hash.to_hex();
        // The declared length and the rows that carry it, in one statement: a
        // hash with no row is the contract's NotFound, named with the address
        // the caller asked for.
        let state = load_state(pool, &hex).await?;
        if state.rows == 0 {
            return Err(not_found(hash));
        }
        // The completeness rule (PG19): the rows the declared length needs are
        // all present, or the block is refused before a byte of it is served --
        // a full read and a range inside the surviving rows alike, so a partial
        // block can never answer with bytes that are not the block's.
        if !state.complete() {
            return Err(incomplete(hash, &state));
        }
        let len = state.len as u64;
        let (start, end) = match range {
            // `resolve` clamps an inclusive end to the last byte and rejects a
            // start past it; a zero-byte block rejects every range.
            Some(range) => range.resolve(len)?,
            // No bytes exist to stream. The range is resolved before this
            // short-circuit, so `bytes=0-0` on an empty block stays an error.
            None if len == 0 => return Ok(Box::pin(stream::empty())),
            None => (0, len - 1),
        };
        // Only the chunks the window touches are read: the first and the last
        // may be sliced, everything between is whole.
        let first = (start / BLOCK_CHUNK_BYTES as u64) as i32;
        let last = (end / BLOCK_CHUNK_BYTES as u64) as i32;
        // One chunk in flight. A row short of its chunk is `UnexpectedEof` on
        // the stream, after earlier chunks of the window may already have been
        // yielded.
        let label = hex.clone();
        Ok(body(
            PgPool::clone(pool),
            hex,
            first,
            last,
            ChunkWindow::new(start, end, BLOCK_CHUNK_BYTES as u64, false),
            label,
        ))
    }

    async fn remove(&self, hash: &BlockHash) -> Result<(), Error> {
        // Idempotent by contract: a block that is not there is already removed,
        // so the row count is deliberately ignored. A single statement is its
        // own transaction.
        query!("DELETE FROM blocks WHERE hash = $1", hash.to_hex())
            .execute(self.store.as_driver())
            .await?;
        Ok(())
    }

    async fn list(&self, after: Option<&BlockHash>, limit: usize) -> Result<Vec<Entry>, Error> {
        let cursor = after.map(BlockHash::to_hex);
        page(
            self.store.as_driver(),
            cursor.as_deref(),
            limit_to_i64(limit),
        )
        .await
    }

    async fn walk(&self, after: Option<&BlockHash>) -> Result<EntryStream<Self::Error>, Error> {
        let state = Walk {
            pool: self.store.as_driver().clone(),
            cursor: after.map(BlockHash::to_hex),
            page: Vec::new().into_iter(),
            ended: false,
        };
        // Lazy and one pass: the first page is read on the stream's first poll,
        // each later page strictly after the previous page's last address.
        let walk: EntryStream<Error> = Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if state.ended {
                    return None;
                }
                if let Some(entry) = state.page.next() {
                    return Some((Ok(entry), state));
                }
                match page(&state.pool, state.cursor.as_deref(), WALK_PAGE).await {
                    // An empty page ends the walk (the `?` answers `None`), so
                    // the cursor is the page's last address — the point the
                    // next step resumes strictly after — with no separate empty
                    // check (style.md §Panics: an invariant-holding shape
                    // rather than a `last()` the page's emptiness could panic).
                    Ok(entries) => {
                        let last = entries.last()?;
                        state.cursor = Some(last.hash.to_hex());
                        state.page = entries.into_iter();
                    }
                    Err(err) => {
                        // An Err item ends the walk: the next poll sees `ended`.
                        state.ended = true;
                        return Some((Err(err), state));
                    }
                }
            }
        }));
        Ok(walk)
    }
}

/// A walk in progress: the pool, the keyset cursor, the current page, and
/// whether an error already ended the traversal (an empty page ends it too, by
/// yielding nothing).
struct Walk {
    pool: PgPool,
    cursor: Option<String>,
    page: IntoIter<Entry>,
    ended: bool,
}

/// One row of `sql/blocks/open.sql`: a chunk's sequence number and its bytes.
struct Chunk {
    seq: i32,
    chunk: Vec<u8>,
}

/// One row of `sql/blocks/page.sql`: an address with the block's declared length
/// and the publish's timestamp, both carried by every one of the block's rows.
struct Page {
    hash: String,
    len: i64,
    published: OffsetDateTime,
}

/// One row of `sql/blocks/state.sql`: the block's declared length with the
/// count and the last sequence of the rows that carry it.
struct State {
    rows: i64,
    last: i32,
    len: i64,
}

impl State {
    /// The rows the declared length needs: every byte of the block falls in a
    /// row, so a `len`-byte block is `(len - 1) / BLOCK_CHUNK_BYTES + 1` rows,
    /// and a zero-byte block is its own one empty row.
    fn chunks(&self) -> i64 {
        chunk_count(self.len as u64) as i64
    }

    /// Whether every row the declared length needs is present. The primary key
    /// makes the sequences distinct, so a count that matches *and* a last row
    /// that is the one the length implies pin the run to `0..chunks`, and both
    /// a gap in the middle and a missing tail are incomplete.
    fn complete(&self) -> bool {
        self.rows == self.chunks() && i64::from(self.last) == self.chunks() - 1
    }
}

/// One keyset page: up to `limit` addresses strictly after `after`, ascending.
///
/// The empty cursor is below every 64-character address, so one statement
/// serves both the first page and every later one, and the comparison rides the
/// `(hash, seq)` primary key while the `ORDER BY` repeats `COLLATE "C"`.
async fn page(pool: &PgPool, after: Option<&str>, limit: i64) -> Result<Vec<Entry>, Error> {
    let rows = query_file_as!(
        Page,
        "sql/blocks/page.sql",
        after.unwrap_or_default(),
        limit
    )
    .fetch_all(pool)
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(Entry {
            hash: decode_hash(&row.hash)?,
            len: row.len as u64,
            published: from_timestamp(row.published),
        });
    }
    Ok(out)
}

/// The block's declared length with the shape of the rows that carry it. A
/// block with no row reads as `rows == 0`, which is what tells `open` that the
/// address is absent rather than incomplete.
async fn load_state(pool: &PgPool, hash: &str) -> Result<State, Error> {
    // The address arrives already hex-encoded: the read's statements share it.
    let state = query_file_as!(State, "sql/blocks/state.sql", hash)
        .fetch_one(pool)
        .await?;
    Ok(state)
}

/// Stream a block's rows as a body, one window of `READ_WINDOW_ROWS` rows at
/// a time.
///
/// Each window is one `fetch_all`, which borrows a pooled connection, reads its
/// rows and gives the connection back before the window's bytes are yielded: a
/// read holds a connection for one fetch, never for the whole of a client's
/// read, and its resident bytes are bounded by one window (R4). `first` and
/// `last` are the chunk rows the range touches, in `seq` order.
fn body(
    pool: PgPool,
    hash: String,
    first: i32,
    last: i32,
    mut window: ChunkWindow,
    label: String,
) -> BodyStream {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        for (from, to) in chunk_windows(first, last, READ_WINDOW_ROWS) {
            let rows = query_file_as!(Chunk, "sql/blocks/open.sql", &hash, from, to)
                .fetch_all(&pool)
                .await;
            let rows = match rows {
                Ok(rows) => rows,
                Err(err) => {
                    let _ = tx.send(Err(io::Error::other(err))).await;
                    return;
                }
            };
            for row in rows {
                match window.push(row.seq, row.chunk) {
                    Err(fault) => {
                        let _ = tx.send(Err(short(&label, fault))).await;
                        return;
                    }
                    Ok(Some(bytes)) => {
                        if tx.send(Ok(bytes)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => {}
                }
            }
        }
        if let Err(fault) = window.finish() {
            let _ = tx.send(Err(short(&label, fault))).await;
        }
    });
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

/// The body error for a window whose bytes do not add up: the block's name with
/// what its rows served against what the window asked for.
fn short(label: &str, fault: ChunkFault) -> io::Error {
    let (served, expect) = fault.counts();
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("block {label} holds {served} of its {expect} bytes"),
    )
}

/// How many 8 MiB rows a block of `len` bytes occupies: the row the block's
/// last byte falls in, plus one. A zero-byte block is still one row, or it
/// would be indistinguishable from an absent block.
fn chunk_count(len: u64) -> u64 {
    if len == 0 {
        1
    } else {
        (len - 1) / BLOCK_CHUNK_BYTES as u64 + 1
    }
}

/// A listing limit as the statement's `LIMIT` argument.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// Decode an address read back from the table: it was written by `to_hex`, so a
/// failure is a corrupt stored row, reported rather than panicked on.
fn decode_hash(raw: &str) -> Result<BlockHash, Error> {
    raw.parse::<BlockHash>().map_err(|err| {
        Error::Storage(storage::io(io::Error::new(
            io::ErrorKind::InvalidData,
            err.to_string(),
        )))
    })
}

/// The contract's missing-block error: the block plane names its own variant,
/// and the crate's projection turns it into `storage::Error`'s I/O variant with
/// `io::ErrorKind::NotFound` — the shape the engine and the S3 layer classify
/// on.
fn address_mismatch(hash: &BlockHash) -> Error {
    Error::Storage(storage::io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("block {hash} is not the address of these bytes"),
    )))
}

/// The rows a block's declared length needs are not all present, so the read
/// refuses the block rather than serving the bytes that survived (PG19). The
/// projection keeps the stored-block corruption's own kind, `UnexpectedEof`,
/// which is also what the short-*bytes* path answers: the block promises more
/// than it holds.
fn incomplete(hash: &BlockHash, state: &State) -> Error {
    Error::Storage(storage::io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!(
            "block {hash} holds {} of its {} chunk rows",
            state.rows,
            state.chunks()
        ),
    )))
}

fn not_found(hash: &BlockHash) -> Error {
    Error::NoSuchBlock(hash.to_hex())
}

#[cfg(test)]
mod tests {
    // The tests address the crate and the standard library by absolute path on
    // purpose: a shortened path would not prove the public one.
    #![allow(clippy::absolute_paths)]
    use std::{
        io,
        path::Path,
        time::{Duration, SystemTime},
    };

    use bytes::Bytes;
    use futures::StreamExt;
    use sqlx::query_scalar;
    use tempfile::TempDir;
    use tinio_util::testing::read_body;

    use super::{
        BLOCK_CHUNK_BYTES, PUBLISH_BATCH_ROWS, PgBlocks, READ_WINDOW_ROWS, STALE_TEMP_GRACE,
        WALK_PAGE, chunk_count,
    };
    use crate::{
        _core::{
            ByteRange,
            blob::{BlockHash, BlockStore, Published, StagedBlock},
            storage::{self, collect_body},
        },
        Access, PgStore,
        pool::Pool,
    };

    /// A scratch stage directory: the legs never write the shared temp dir.
    fn stage_dir() -> TempDir {
        tempfile::tempdir().expect("a scratch stage directory")
    }

    /// A block plane over `store`, staging under `dir`'s own `blocks/` subtree.
    fn blocks_in(store: PgStore, dir: &TempDir) -> PgBlocks {
        PgBlocks::with_temp_dir(store, dir.path().join("blocks"))
    }

    /// Poll until `path` is gone: a stage's unlink runs off the worker
    /// ([`tinio_util::fsutil::remove_staged_temp`]), so the assertion waits for
    /// it rather than reading the directory at the instant the call returns.
    async fn wait_gone(path: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the stage's temp is removed");
    }

    /// Set a file's modified time, so a leg can plant residue older than the
    /// sweep's grace without waiting for it.
    fn age(path: &Path, by: Duration) {
        let file = std::fs::File::options()
            .write(true)
            .open(path)
            .expect("the residue opens");
        file.set_modified(SystemTime::now() - by)
            .expect("the residue's time moves");
    }

    /// Stage `bytes` and publish them under their own address.
    async fn publish(blocks: &PgBlocks, bytes: &[u8]) -> BlockHash {
        let hash = BlockHash::of(bytes);
        let mut staged = blocks.stage().await.expect("stage");
        staged
            .write(Bytes::copy_from_slice(bytes))
            .await
            .expect("write");
        assert_eq!(
            blocks.publish(staged, &hash).await.expect("publish"),
            Published::Stored
        );
        hash
    }

    /// Read `open` until the body yields an error.
    async fn stream_end(
        blocks: &PgBlocks,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> io::Error {
        let mut body = blocks.open(hash, range).await.expect("the block opens");
        let mut refused = None;
        while let Some(chunk) = body.next().await {
            if let Err(err) = chunk {
                refused = Some(err);
                break;
            }
        }
        refused.expect("the body ends in an error")
    }

    #[tokio::test]
    async fn the_contract_round_trips_over_the_blocks_table() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let hash = BlockHash::of(b"conformance");
        assert!(!blocks.contains(&hash).await.expect("contains"));

        // A stage is invisible: nothing is written before the publish.
        let mut staged = blocks.stage().await.expect("stage");
        staged
            .write(Bytes::from_static(b"conformance"))
            .await
            .expect("write");
        assert!(!blocks.contains(&hash).await.expect("contains"));
        assert!(blocks.list(None, 10).await.expect("list").is_empty());

        let before = SystemTime::now() - Duration::from_secs(1);
        assert_eq!(
            blocks.publish(staged, &hash).await.expect("publish"),
            Published::Stored
        );
        let after = SystemTime::now() + Duration::from_secs(1);
        assert!(blocks.contains(&hash).await.expect("contains"));

        // The dedup hit discards the staged bytes rather than adopting them.
        let mut staged = blocks.stage().await.expect("stage");
        staged
            .write(Bytes::from_static(b"conformance"))
            .await
            .expect("write");
        assert_eq!(
            blocks.publish(staged, &hash).await.expect("publish"),
            Published::Existing
        );
        // Bytes that do not hash to the address are refused, so they cannot
        // splice onto the stored block.
        let mut staged = blocks.stage().await.expect("stage");
        staged
            .write(Bytes::from_static(b"garbage"))
            .await
            .expect("write");
        assert!(blocks.publish(staged, &hash).await.is_err());
        assert_eq!(
            collect_body(blocks.open(&hash, None).await.expect("open"))
                .await
                .expect("read"),
            b"conformance"
        );

        // A range, and a range past the stored block.
        let body = blocks
            .open(&hash, Some(ByteRange::Inclusive(0, 3)))
            .await
            .expect("open");
        assert_eq!(collect_body(body).await.expect("read"), b"conf");
        assert!(
            blocks.open(&hash, Some(ByteRange::From(11))).await.is_err(),
            "a start past the stored block is an error, never a clamp"
        );

        // The listing reports the stored length and a clock reading from this run.
        let listed = blocks.list(None, 10).await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].hash, hash);
        assert_eq!(listed[0].len, 11);
        assert!(
            before <= listed[0].published && listed[0].published <= after,
            "published {:?} must sit in the window around the publish",
            listed[0].published
        );

        // Remove is idempotent.
        blocks.remove(&hash).await.expect("remove");
        assert!(!blocks.contains(&hash).await.expect("contains"));
        blocks.remove(&hash).await.expect("remove is idempotent");

        // A block that is not there: the message names it, and the projection
        // keeps NotFound -- the shape the engine and the S3 layer classify on.
        let missing = BlockHash::of(b"never published");
        let Err(err) = blocks.open(&missing, None).await else {
            panic!("opening a missing block is an error, not an empty body");
        };
        assert!(
            err.to_string().contains(&missing.to_hex()),
            "{err} must name the block it could not open"
        );
        let projected: storage::Error = err.into();
        let storage::Error::Io(io) = projected else {
            panic!("a missing block must project onto an I/O error, got {projected:?}");
        };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);

        db.drop_db().await;
    }

    #[tokio::test]
    async fn a_block_larger_than_a_chunk_round_trips_and_serves_a_range_across_it() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        // Two chunks and a short tail: the boundary is where a mis-sliced or
        // mis-sequenced chunk would show.
        let payload: Vec<u8> = (0..BLOCK_CHUNK_BYTES + 17)
            .map(|i| (i % 251) as u8)
            .collect();
        let hash = publish(&blocks, &payload).await;

        let rows = query_scalar!(
            "SELECT count(*) AS \"rows!\" FROM blocks WHERE hash = $1",
            hash.to_hex()
        )
        .fetch_one(blocks.store().as_driver())
        .await
        .expect("count the chunk rows");
        assert_eq!(rows, 2, "a block spans one row per 8 MiB");

        assert_eq!(
            collect_body(blocks.open(&hash, None).await.expect("open"))
                .await
                .expect("read"),
            payload,
            "a multi-chunk block reads back byte for byte"
        );

        let start = BLOCK_CHUNK_BYTES - 5;
        let end = BLOCK_CHUNK_BYTES + 5;
        let body = blocks
            .open(&hash, Some(ByteRange::Inclusive(start as u64, end as u64)))
            .await
            .expect("open");
        assert_eq!(
            collect_body(body).await.expect("read"),
            &payload[start..=end],
            "a range spanning the chunk boundary serves exactly its own bytes"
        );

        db.drop_db().await;
    }

    /// A body several publish batches long goes out in several statements --
    /// each carrying at most [`PUBLISH_BATCH_ROWS`] rows -- and reads back byte
    /// for byte: the statement boundary is where a batch that forgot its row
    /// offset would double a row or drop one.
    #[tokio::test]
    async fn a_block_several_publish_batches_long_round_trips() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        // Five full chunks and a short tail: three batch statements, the last
        // of them short, so the statement boundary is where a publish that
        // forgot its row offset would double a row or drop one.
        let chunks = PUBLISH_BATCH_ROWS * 2 + 2;
        let len = (chunks - 1) * BLOCK_CHUNK_BYTES + 17;
        let payload: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
        let hash = publish(&blocks, &payload).await;

        assert_eq!(
            row_count(&blocks, &hash).await,
            chunks as i64,
            "a batched publish writes one row per 8 MiB chunk, the short tail included"
        );
        assert_eq!(
            collect_body(blocks.open(&hash, None).await.expect("open"))
                .await
                .expect("read"),
            payload,
            "a block written by several batch statements reads back byte for byte"
        );
        // A window boundary and a batch boundary are different places: the read
        // serves the seam either way.
        let start = BLOCK_CHUNK_BYTES - 5;
        let end = BLOCK_CHUNK_BYTES + 5;
        let body = blocks
            .open(&hash, Some(ByteRange::Inclusive(start as u64, end as u64)))
            .await
            .expect("open");
        assert_eq!(
            collect_body(body).await.expect("read"),
            &payload[start..=end],
            "a range across a batch's own boundary serves exactly its own bytes"
        );

        db.drop_db().await;
    }

    /// The row, statement and batch arithmetic above the `bytea` cap, without
    /// materializing a gigabyte: PG14 chunks a block into 8 MiB rows precisely
    /// because one field cannot hold one that large, and `publish` writes those
    /// rows in bounded batches, so no statement's parameter list ever carries
    /// the block whole.
    ///
    /// This is the arithmetic only, and it is deliberately not the >1 GiB
    /// coverage: the materialized leg is not run — writing 1 GiB through a
    /// scratch database costs far more than the arithmetic it would re-check —
    /// so what the real publish does for a body of several batches and for the
    /// row count a large block plans is the DB-backed leg's
    /// (`a_block_several_publish_batches_long_round_trips`), and the plan's
    /// coverage row records that choice.
    #[test]
    fn a_block_larger_than_the_bytea_cap_is_planned_in_whole_chunks() {
        const GIB: u64 = 1 << 30;
        let unit = BLOCK_CHUNK_BYTES as u64;

        // A zero-byte block is still one row, or it would be indistinguishable
        // from an absent block.
        assert_eq!(chunk_count(0), 1);
        assert_eq!(chunk_count(unit), 1);
        assert_eq!(chunk_count(unit + 1), 2);

        // The cap's own neighbourhood: 1 GiB is exactly 128 rows, and a byte
        // more is a short 129th.
        assert_eq!(chunk_count(GIB), 128);
        assert_eq!(chunk_count(GIB + 1), 129);

        // The one tie between what `publish` plans and what `open` computes
        // back: a full read of a `len`-byte block asks for its last row as
        // `(len - 1) / unit`, so the planned row count has to be that row's
        // index plus one — the arithmetic that makes a 129th row addressable
        // rather than merely present.
        let len = GIB + 1;
        assert_eq!(
            chunk_count(len),
            (len - 1) / unit + 1,
            "the last row `open` addresses is the last row the plan writes"
        );

        // And the statement bound that keeps every parameter list inside the
        // cap the chunking exists to escape: a 1 GiB block is its 128 rows in
        // 64 statements, each carrying `PUBLISH_BATCH_ROWS` 8 MiB chunks.
        assert_eq!(chunk_count(GIB).div_ceil(PUBLISH_BATCH_ROWS as u64), 64);
        assert!(
            PUBLISH_BATCH_ROWS * BLOCK_CHUNK_BYTES < GIB as usize,
            "one statement's chunks are far below the cap a single value hits"
        );
    }

    /// The default stage directory is the OS temp dir's own leaf, so the plane
    /// stages under the shared temp space unless a caller points it elsewhere
    /// with [`PgBlocks::with_temp_dir`].
    #[test]
    fn the_default_stage_dir_is_the_os_temp_dirs_leaf() {
        assert_eq!(
            super::default_temp_dir(),
            std::env::temp_dir().join(super::TEMP_DIR_LEAF)
        );
    }

    /// A read holds a pooled connection for the window it is fetching, not for
    /// the whole of a client's read (R4): with a pool of exactly one
    /// connection, a body that is open and half-consumed must not keep another
    /// query from running.
    ///
    /// A read that held its connection to the end of the stream — the shape
    /// every row fetched through one driver stream has — deadlocks the second
    /// query here until the timeout, which is what makes this leg the property
    /// and not the byte count.
    #[tokio::test]
    async fn an_open_body_releases_its_pooled_connection_between_windows() {
        if !crate::tests::db::require_dsn() {
            return;
        }
        let db = crate::tests::db::TestDb::create()
            .await
            .expect("a scratch database");
        let pool = Pool::open(&db.dsn, 1).expect("a one-connection pool");
        let store = PgStore::connect_with_pool(pool, Access::ReadWrite)
            .await
            .expect("a writable store");
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        // More rows than one read window, so the body has a fetch to make while
        // the client is still reading it.
        let len = READ_WINDOW_ROWS as usize * BLOCK_CHUNK_BYTES + 17;
        let payload: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
        let hash = publish(&blocks, &payload).await;

        let mut body = blocks.open(&hash, None).await.expect("open");
        let first = body.next().await.expect("the first chunk").expect("bytes");
        assert_eq!(first.len(), BLOCK_CHUNK_BYTES, "the first row is whole");

        // The pool's one connection has to be free: the read borrows it for a
        // window fetch and gives it back before the window's bytes are yielded.
        let present = tokio::time::timeout(Duration::from_secs(5), blocks.contains(&hash))
            .await
            .expect("the read held the pool's only connection past its window fetch")
            .expect("contains");
        assert!(present);

        // And the rest of the body still reads back, window by window, over the
        // connection the query just handed back.
        let rest = read_body(body).await.expect("the rest of the body");
        assert_eq!(
            rest,
            payload[BLOCK_CHUNK_BYTES..],
            "the body serves every row it has left, byte for byte"
        );

        db.drop_db().await;
    }

    /// **The stage writes its bytes to a temp file, not into resident rows.** A
    /// body of two chunks and a tail, written in 64 KiB steps, is on disk as it
    /// arrives -- the file's length is the block's own -- and publishing it
    /// stores the block and removes the temp. The temp's size grows with the
    /// block, but the stage's *memory* stays one write buffer: no `Vec` of
    /// finished rows is ever held.
    #[tokio::test]
    async fn a_stage_files_its_bytes_to_a_temp_file_that_publish_removes() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        const TAIL: usize = 17;
        const STEP: usize = 64 * 1024;
        let len = BLOCK_CHUNK_BYTES * 2 + TAIL;
        let payload: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
        let hash = BlockHash::of(&payload);

        let mut staged = blocks.stage().await.expect("stage");
        let path = staged.path().to_path_buf();
        let mut written = 0;
        while written < len {
            let take = (len - written).min(STEP);
            staged
                .write(Bytes::copy_from_slice(&payload[written..written + take]))
                .await
                .expect("write");
            written += take;
        }
        // The bytes are on disk as they arrive (a write buffer, not the whole
        // block, is what the stage holds), so the temp has grown well past one
        // chunk while the stage is still open.
        assert!(
            std::fs::metadata(&path).expect("the temp exists").len() >= BLOCK_CHUNK_BYTES as u64,
            "the bytes are filed to the temp as they arrive, not held in memory"
        );

        assert_eq!(
            blocks.publish(staged, &hash).await.expect("publish"),
            Published::Stored
        );
        assert_eq!(
            collect_body(blocks.open(&hash, None).await.expect("open"))
                .await
                .expect("read"),
            payload,
            "a block staged to a temp file reads back byte for byte"
        );
        wait_gone(&path).await;
        assert!(!path.exists(), "a published stage removes its temp");

        db.drop_db().await;
    }

    /// **A refused publish removes its temp too.** The staged bytes do not hash
    /// to the address, so the publish refuses -- and the temp the stage filed
    /// them in goes with the stage, leaving no residue.
    #[tokio::test]
    async fn a_refused_publish_removes_its_temp() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let hash = BlockHash::of(b"the real block");
        let mut staged = blocks.stage().await.expect("stage");
        let path = staged.path().to_path_buf();
        staged
            .write(Bytes::from_static(b"garbage that is not the address"))
            .await
            .expect("write");
        assert!(
            blocks.publish(staged, &hash).await.is_err(),
            "bytes that do not hash to the address are refused"
        );

        wait_gone(&path).await;
        assert!(!path.exists(), "a refused publish removes its temp");

        db.drop_db().await;
    }

    /// **A residue temp left by a crashed stage is reclaimed by the next
    /// stage.** A temp older than [`STALE_TEMP_GRACE`] is swept; a temp younger
    /// than the grace -- what a live stage holds -- is left alone.
    #[tokio::test]
    async fn a_residue_temp_is_reclaimed_by_the_next_stage() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let stage_dir = blocks.temp_dir().to_path_buf();
        std::fs::create_dir_all(&stage_dir).expect("the stage directory");
        // The residue of a stage whose process died: older than the grace.
        let stale = stage_dir.join("stale-residue");
        std::fs::write(&stale, b"half a block").expect("the residue");
        age(&stale, STALE_TEMP_GRACE + Duration::from_secs(60));
        // A temp a live stage would hold: younger than the grace.
        let fresh = stage_dir.join("fresh");
        std::fs::write(&fresh, b"a live stage").expect("the live temp");

        let staged = blocks.stage().await.expect("a stage opens and sweeps");
        let staged_path = staged.path().to_path_buf();

        assert!(!stale.exists(), "the stale residue is reclaimed");
        assert!(
            fresh.exists(),
            "a temp younger than the grace is never swept"
        );
        assert!(staged_path.exists(), "the new stage's own temp is its own");

        drop(staged);
        db.drop_db().await;
    }

    #[tokio::test]
    async fn a_zero_length_block_publishes_lists_and_refuses_a_range() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        // The empty blob's block: one row of no bytes, not an absent block.
        let zero = BlockHash::of(b"");
        let mut staged = blocks.stage().await.expect("stage");
        staged.write(Bytes::new()).await.expect("write");
        assert_eq!(
            blocks.publish(staged, &zero).await.expect("publish"),
            Published::Stored
        );
        assert!(blocks.contains(&zero).await.expect("contains"));
        assert!(
            collect_body(blocks.open(&zero, None).await.expect("open"))
                .await
                .expect("read")
                .is_empty()
        );
        let listed = blocks.list(None, 10).await.expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].len, 0, "the empty blob is a block of zero bytes");

        let Err(err) = blocks.open(&zero, Some(ByteRange::Inclusive(0, 0))).await else {
            panic!("a range against a zero-byte block is an error, not an empty body");
        };
        let projected: storage::Error = err.into();
        assert!(
            matches!(projected, storage::Error::InvalidRange { .. }),
            "a range against a zero-byte block is InvalidRange, got {projected:?}"
        );

        let mut staged = blocks.stage().await.expect("stage");
        staged.write(Bytes::new()).await.expect("write");
        assert_eq!(
            blocks.publish(staged, &zero).await.expect("publish"),
            Published::Existing,
            "a repeated publish of the empty block is the dedup hit"
        );

        db.drop_db().await;
    }

    /// A stored block whose rows are the complete set its length needs, but
    /// whose chunk *bytes* are short, is an `UnexpectedEof` on the body stream:
    /// the window counts what the rows served against what it asked for, rather
    /// than ending the body early.
    #[tokio::test]
    async fn a_chunk_row_shorter_than_its_chunk_is_a_short_body_error() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let payload: Vec<u8> = (0..BLOCK_CHUNK_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let hash = publish(&blocks, &payload).await;
        // The head row keeps its place and loses its bytes, behind the store's
        // back, so the row set stays complete and only the bytes do not add up.
        const SERVED: usize = 1024;
        sqlx::query!(
            "UPDATE blocks SET chunk = $2 WHERE hash = $1 AND seq = 0",
            hash.to_hex(),
            &payload[..SERVED]
        )
        .execute(blocks.store().as_driver())
        .await
        .expect("the plant");

        let err = stream_end(&blocks, &hash, None).await;
        let message = err.to_string();
        assert!(
            message.contains(&hash.to_hex()),
            "the failure names the block: {message}"
        );
        assert!(
            message.contains(&format!("{} of its {}", SERVED + 17, payload.len())),
            "the failure counts what was served against the declared length: {message}"
        );
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);

        db.drop_db().await;
    }

    /// PG19's completeness rule is a property of the *block*, not of the window
    /// a reader asked for: a block whose declared `len` needs more rows than it
    /// holds must not answer even a range that lies entirely inside the rows
    /// that survived. `contains` answers whether a row exists, so it stays true
    /// here — the read is what refuses the block.
    #[tokio::test]
    async fn a_block_missing_a_row_is_refused_even_for_a_range_inside_the_survivors() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let payload: Vec<u8> = (0..BLOCK_CHUNK_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let hash = publish(&blocks, &payload).await;
        drop_row(&blocks, &hash, 1).await;

        assert!(
            blocks.contains(&hash).await.expect("contains"),
            "a row exists, so the dedup lookup answers present"
        );
        assert!(
            blocks
                .open(&hash, Some(ByteRange::Inclusive(0, 100)))
                .await
                .is_err(),
            "a range inside the surviving head is refused while a row is missing: \
             101 bytes of a block that needs two rows is not the block"
        );

        let Err(err) = blocks.open(&hash, None).await else {
            panic!("a block missing a row is refused, not served short");
        };
        assert!(
            err.to_string().contains(&hash.to_hex()),
            "{err} must name the block it could not serve"
        );

        db.drop_db().await;
    }

    /// A publish that dies part-way leaves rows behind, and a re-publish of the
    /// same address fills the gaps rather than answering `Existing` on a
    /// partial block. The seam is safe because the address *is* the hash of the
    /// bytes: every row of it holds the same content, and the rows already
    /// there are never rewritten (`ON CONFLICT ... DO NOTHING` per row).
    #[tokio::test]
    async fn a_republish_fills_the_gap_of_a_partial_block() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let payload: Vec<u8> = (0..BLOCK_CHUNK_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let hash = publish(&blocks, &payload).await;
        // The interrupted publish's residue: the head row landed, the tail did
        // not.
        drop_row(&blocks, &hash, 1).await;
        assert!(
            blocks.open(&hash, None).await.is_err(),
            "the partial block is refused before the gap is filled"
        );

        // The re-publish writes the missing row and leaves the found one alone.
        let mut staged = blocks.stage().await.expect("stage");
        staged
            .write(Bytes::copy_from_slice(&payload))
            .await
            .expect("write");
        assert_eq!(
            blocks.publish(staged, &hash).await.expect("publish"),
            Published::Stored,
            "a publish that fills a gap inserted a row, so it stored the block"
        );

        assert_eq!(row_count(&blocks, &hash).await, 2, "the gap is filled");
        assert_eq!(
            collect_body(blocks.open(&hash, None).await.expect("open"))
                .await
                .expect("read"),
            payload,
            "the whole block reads back byte for byte after the gap is filled"
        );
        let body = blocks
            .open(
                &hash,
                Some(ByteRange::Inclusive(
                    BLOCK_CHUNK_BYTES as u64 - 5,
                    BLOCK_CHUNK_BYTES as u64 + 5,
                )),
            )
            .await
            .expect("open");
        assert_eq!(
            collect_body(body).await.expect("read"),
            &payload[BLOCK_CHUNK_BYTES - 5..=BLOCK_CHUNK_BYTES + 5],
            "the range across the seam serves exactly its own bytes"
        );
        db.drop_db().await;
    }

    /// Drop one chunk row behind the store's back: the block's declared length
    /// survives, so it promises a row that is no longer there.
    async fn drop_row(blocks: &PgBlocks, hash: &BlockHash, seq: i32) {
        sqlx::query_file!(
            "sql/tests/plant_blocks_missing_tail_row.sql",
            hash.to_hex(),
            seq
        )
        .execute(blocks.store().as_driver())
        .await
        .expect("the plant");
    }

    /// How many chunk rows the block holds.
    async fn row_count(blocks: &PgBlocks, hash: &BlockHash) -> i64 {
        query_scalar!(
            "SELECT count(*) AS \"rows!\" FROM blocks WHERE hash = $1",
            hash.to_hex()
        )
        .fetch_one(blocks.store().as_driver())
        .await
        .expect("count the chunk rows")
    }

    /// An address the store never wrote fails the page it is read with, and an
    /// `Err` item ends a walk: the traversal reports the corruption instead of
    /// panicking on it.
    #[tokio::test]
    async fn an_undecodable_stored_address_is_reported_and_ends_the_walk() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);
        let hash = publish(&blocks, b"a real block").await;
        sqlx::query_file!("sql/tests/plant_blocks_undecodable_address.sql")
            .execute(blocks.store().as_driver())
            .await
            .expect("the plant");

        assert!(blocks.contains(&hash).await.expect("the real block"));
        let err = blocks
            .list(None, 10)
            .await
            .expect_err("an undecodable address is reported, never skipped");
        let projected: storage::Error = err.into();
        let storage::Error::Io(io) = projected else {
            panic!("an undecodable address must project onto an I/O error");
        };
        assert_eq!(io.kind(), io::ErrorKind::InvalidData);

        let mut walk = blocks.walk(None).await.expect("the walk starts");
        assert!(
            walk.next().await.expect("the failure item").is_err(),
            "the walk yields the failure rather than ending silently"
        );
        assert!(walk.next().await.is_none(), "an Err item ends the walk");
        db.drop_db().await;
    }

    /// A walk reads one page per step and resumes strictly after the last
    /// address of the page it just served, so a listing longer than one page is
    /// still yielded exactly once each, in ascending order.
    #[tokio::test]
    async fn a_walk_crosses_page_boundaries_exactly_once() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        // One page and a tail: the boundary is where a walk that forgot to move
        // its cursor would repeat or skip an address.
        let total = WALK_PAGE as usize + 1;
        let mut published = Vec::with_capacity(total);
        for index in 0..total {
            let payload = format!("page-{index}");
            let hash = publish(&blocks, payload.as_bytes()).await;
            published.push((hash, payload));
        }
        // The walk ascends by address, so the expectation is the published set
        // in that order — whatever order the publishes were staged in.
        published.sort_by_key(|(hash, _payload)| *hash);
        let expected: Vec<BlockHash> = published.iter().map(|(hash, _payload)| *hash).collect();

        let mut walk = blocks.walk(None).await.expect("walk");
        let mut walked = Vec::new();
        while let Some(entry) = walk.next().await {
            walked.push(entry.expect("walk entry").hash);
        }
        assert_eq!(
            walked, expected,
            "a walk longer than one page yields every block exactly once"
        );

        // Resuming inside the first page serves the rest of the listing, the
        // second page included.
        let resume = expected[WALK_PAGE as usize - 1];
        let mut walk = blocks.walk(Some(&resume)).await.expect("walk");
        let mut resumed = Vec::new();
        while let Some(entry) = walk.next().await {
            resumed.push(entry.expect("walk entry").hash);
        }
        assert_eq!(
            resumed,
            expected[WALK_PAGE as usize..].to_vec(),
            "the resume is strictly after the cursor, across the boundary"
        );

        let (hash, payload) = &published[0];
        assert_eq!(
            read_body(blocks.open(hash, None).await.expect("open"))
                .await
                .expect("read"),
            payload.as_bytes(),
            "a page boundary does not disturb the bytes a block serves"
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn list_and_walk_page_ascending_strictly_after_the_cursor() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let dir = stage_dir();
        let blocks = blocks_in(store, &dir);

        let mut published = Vec::new();
        for payload in [
            b"page-0".as_slice(),
            b"page-1",
            b"page-2",
            b"page-3",
            b"page-4",
        ] {
            published.push(publish(&blocks, payload).await);
        }
        published.sort();

        // Pages of two: every page ascends and resumes strictly after the last.
        let mut seen: Vec<BlockHash> = Vec::new();
        let mut cursor: Option<BlockHash> = None;
        loop {
            let page = blocks.list(cursor.as_ref(), 2).await.expect("list");
            if page.is_empty() {
                break;
            }
            assert!(
                page.len() <= 2,
                "a page never exceeds the limit it was asked for"
            );
            assert!(
                page.windows(2).all(|pair| pair[0].hash < pair[1].hash),
                "one page's addresses ascend"
            );
            if let Some(previous) = seen.last() {
                assert!(
                    previous < &page[0].hash,
                    "a page resumes strictly after the previous page's last address"
                );
            }
            cursor = Some(page.last().expect("a non-empty page has a last entry").hash);
            seen.extend(page.into_iter().map(|entry| entry.hash));
        }
        assert_eq!(
            seen, published,
            "every published block is listed exactly once, in ascending order"
        );

        // The walk is the same sequence lazily, resuming strictly after a cursor.
        let mut walk = blocks.walk(None).await.expect("walk");
        let mut walked = Vec::new();
        while let Some(entry) = walk.next().await {
            walked.push(entry.expect("walk entry").hash);
        }
        assert_eq!(walked, published);

        let mut walk = blocks.walk(Some(&published[2])).await.expect("walk");
        let mut resumed = Vec::new();
        while let Some(entry) = walk.next().await {
            resumed.push(entry.expect("walk entry").hash);
        }
        assert_eq!(resumed, published[3..].to_vec());

        let mut walk = blocks.walk(published.last()).await.expect("walk");
        assert!(
            walk.next().await.is_none(),
            "a walk from the last address ends immediately"
        );

        db.drop_db().await;
    }
}
