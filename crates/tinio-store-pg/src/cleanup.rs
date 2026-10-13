//! The cleanup scans over pg's own tables (PG7).
//!
//! The six statements under `sql/cleanup/` live in this crate because they read **this crate's**
//! tables — `content`, `content_heads`, `uploads`, `version_rows`, `current_slots` — which is
//! the same reason [`PgStore::live_len`](crate::PgStore::live_len) lives here: a statement's home
//! is the crate that owns the schema it reads. `tinio-pg`'s `Cleanup` keeps the **policy** —
//! which scans run, the thresholds, the dry-run shape, the live-set fold and the block sweep —
//! and calls these methods for the row facts.
//!
//! None of these is a contract operation: the four persistence contracts carry no cleanup
//! surface, so the scans are the store's own, exactly as `live_len` is. The three content/version
//! scans are reads; the two reclamations are removals, and the caller's `dry_run` decides between
//! them.
//!
//! # The bound, and what a pass costs
//!
//! **Every statement here is bounded by a keyset page**, so no scan reads a whole relation into
//! the caller and no removal binds a whole scan's arrays:
//!
//! - the order is the scan's own key — `(bucket, kind, ckey, seq)` for the content generations,
//!   `(bucket, key, seq)` for the version tables, `(bucket, ckey)` for the part heads;
//! - each page carries a `LIMIT` of [`CLEANUP_BATCH_ROWS`] and a cursor that is **strictly past**
//!   the last row the previous page returned, so consecutive pages never overlap and never skip;
//! - the scans are lazy: [`PgStore::orphan_chunk_pages`] and its siblings are `stream::unfold`
//!   streams that read one page per poll, the same shape `BlockStore::open` uses for a body — and
//!   a page that comes back **short** is the last one, so a pass reads exactly
//!   `⌈N / CLEANUP_BATCH_ROWS⌉` statements for `N` candidates (a `start` cursor that sorts before
//!   every row opens it, because no bucket name is empty).
//!
//! What that costs is `⌈N / CLEANUP_BATCH_ROWS⌉` statements, each of which the planner is free to
//! serve from the keyset's own order. Measured on a 205 000-row `content`, 42 000-row
//! `content_heads` and 100 000-row `version_rows` scratch database (`EXPLAIN (ANALYZE, BUFFERS)`,
//! grace 600 s, one page of [`CLEANUP_BATCH_ROWS`]):
//!
//! - the **retention** scan went from a sequential scan of every version row plus an external
//!   merge sort (`Seq Scan on version_rows`, `Sort … external merge Disk: 7840kB`, 100 000 rows,
//!   106.6 ms, 2 647 buffers) to `Limit → Incremental Sort → Merge Left Join → Index Scan using
//!   version_rows_pkey`: 1 024 rows walked in 0.74 ms and 174 buffers — the index serves the page
//!   order and the page stops at the limit;
//! - the **live-reference** scan is a plain `Index Scan using version_rows_pkey` over the version
//!   primary key's own order: 1 024 rows in 0.32 ms and 171 buffers, no sort at all;
//! - the **part-head** scan keeps `content_heads_pkey`'s index-only scan under its regex-derived
//!   index condition and stops after 1 024 rows in 0.76 ms and 33 buffers;
//! - the **orphan-chunk** scan is the one whose plan the keyset does not improve: the age
//!   predicate is non-selective in a steady-state store (every row older than the grace matches)
//!   and PostgreSQL estimates the `NOT EXISTS` anti-join pessimistically, so the planner keeps
//!   `Seq Scan on content` + `Hash Anti Join` + `Sort` with the `Limit` on top — the page bounds
//!   what the caller receives and binds, not what that statement scans. The saving grace is the
//!   page size: a store whose orphan generations fit in one page pays exactly the statement the
//!   unpaged scan paid (58.1 ms against the 55.3 ms the unpaged statement measured on the same
//!   data), and only a store past that pays per page.
//!
//! The one thing that is not per-page is the fold the *caller* builds from the pages (the block
//! sweep's live set, whose size is the set of live block addresses a sweep necessarily holds);
//! the rows it folds in arrive one page at a time.
//!
//! The collecting forms ([`PgStore::orphan_chunks`], [`PgStore::orphan_part_heads`],
//! [`PgStore::retention_rows`]) drain those pages into a `Vec` for a caller that wants the whole
//! list at once — the crate's own legs do. They are a convenience over the same bounded
//! statements, not a second unbounded scan: a caller that must not hold the whole answer (the
//! policy's fix pass, whose removals are per page) streams the pages instead.

use std::{pin::Pin, time::Duration};

use futures::{Stream, StreamExt, stream};
use sqlx::{PgPool, query_file, query_file_as};
use tinio_core::{blob::BlobRef, bucket, meta::version::decode_blob};

use crate::{Error, PgStore, content::ContentKind};

/// How many rows one page of a cleanup scan reads, and how many rows one batched removal takes.
///
/// The page bound is the memory a pass holds and the parameter list a removal binds, so it is
/// declared once here rather than at each call site: the chunk removal binds four arrays of one
/// page, which a page of this size keeps far below the driver's parameter ceiling. A store whose
/// candidates fit in one page costs the one statement the unpaged scan cost, which is why the
/// bound is not smaller. It is public so a test can plant one page past the first.
pub const CLEANUP_BATCH_ROWS: i64 = 1024;

/// The orphan-chunk scan's pages: at most [`CLEANUP_BATCH_ROWS`] generations per item, in
/// `(bucket, kind, ckey, seq)` order, ending after the last page.
pub type OrphanChunkStream = Pin<Box<dyn Stream<Item = Result<Vec<OrphanChunk>, Error>> + Send>>;

/// The upload-gone part scan's pages, in `(bucket, ckey)` order.
pub type OrphanPartStream = Pin<Box<dyn Stream<Item = Result<Vec<OrphanPart>, Error>> + Send>>;

/// The retention scan's pages: at most [`CLEANUP_BATCH_ROWS`] history rows per item, newest-first
/// per key.
///
/// A page boundary can fall **inside** a key, so a consumer must not judge a key until it holds
/// the row that ends it (the key's last row, or the stream's end).
pub type HistoryRowStream = Pin<Box<dyn Stream<Item = Result<Vec<HistoryRow>, Error>> + Send>>;

/// The live-reference scan's pages: at most [`CLEANUP_BATCH_ROWS`] version rows' block lists per
/// item, in the version primary key's own `(bucket, key, seq)` order (the fold does not care
/// which end of a key's history it starts from, so this scan takes the order the index already
/// serves).
pub type LiveBlobStream = Pin<Box<dyn Stream<Item = Result<Vec<BlobRef>, Error>> + Send>>;

/// One orphan chunk generation: a `content` row older than the grace window that no live head
/// names and that is not the head's **current allocation** (so a publish in flight — including
/// one re-homing a generation a later allocation superseded — is never read as residue). The
/// row's own `content.published` stamp carries the age; the head gate protects the current
/// generation whose rows an old upload may already have aged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanChunk {
    /// The bucket the generation belongs to.
    pub bucket: bucket::Name,
    /// The content kind the generation's rows are stored under.
    pub kind: ContentKind,
    /// The encoded content key (PG20).
    pub ckey: String,
    /// The generation's sequence.
    pub seq: i64,
}

/// One staged part whose `uploads` row is gone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanPart {
    /// The bucket the part belongs to.
    pub bucket: bucket::Name,
    /// The encoded part content key (PG20).
    pub ckey: String,
}

/// One version row that is **history**, with the facts the retention rule reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    /// The bucket the key belongs to.
    pub bucket: bucket::Name,
    /// The key's wire (the contract `object::Key`'s encoding), validated by the caller that
    /// expiry addresses.
    pub key: String,
    /// The per-key sequence the row is stored at.
    pub seq: i64,
    /// When the version became noncurrent (`0` = no supersede stamped it).
    pub became_noncurrent: i64,
    /// When the version was created, in nanoseconds.
    pub created_nanos: i64,
}

/// The content scan's keyset position: the last `(bucket, kind, ckey, seq)` a page returned.
///
/// [`ChunkCursor::start`] opens a pass: the comparison is a row comparison, so it stops at the
/// first differing column and the empty bucket name (which no bucket has) already sorts before
/// every generation, whatever its kind, ckey and seq.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkCursor {
    bucket: String,
    kind: ContentKind,
    ckey: String,
    seq: i64,
}

impl ChunkCursor {
    /// The position before the first page.
    fn start() -> Self {
        Self {
            bucket: String::new(),
            kind: ContentKind::Object,
            ckey: String::new(),
            seq: 0,
        }
    }

    /// The position after `row`.
    fn after(row: &OrphanChunk) -> Self {
        Self {
            bucket: row.bucket.to_string(),
            kind: row.kind,
            ckey: row.ckey.clone(),
            seq: row.seq,
        }
    }
}

/// The part scan's keyset position: the last `(bucket, ckey)` a page returned.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartCursor {
    bucket: String,
    ckey: String,
}

impl PartCursor {
    /// The position before the first page.
    fn start() -> Self {
        Self {
            bucket: String::new(),
            ckey: String::new(),
        }
    }

    /// The position after `row`.
    fn after(row: &OrphanPart) -> Self {
        Self {
            bucket: row.bucket.to_string(),
            ckey: row.ckey.clone(),
        }
    }
}

/// The version tables' keyset position: the last `(bucket, key, seq)` a page returned.
///
/// The two version-row scans page in opposite sequence directions — the retention scan reads
/// newest-first per key, the live-reference scan the primary key's own ascending order — so each
/// statement owns its comparison's direction and this cursor only records the row the page
/// stopped at. The empty bucket of [`VersionCursor::start`] sorts before every row either scan
/// can return, which is what makes one opening position serve both.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VersionCursor {
    bucket: String,
    key: String,
    seq: i64,
}

impl VersionCursor {
    /// The position before the first page.
    fn start() -> Self {
        Self {
            bucket: String::new(),
            key: String::new(),
            seq: i64::MAX,
        }
    }

    /// The position after one row of a page.
    fn after(bucket: &str, key: &str, seq: i64) -> Self {
        Self {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            seq,
        }
    }
}

/// The orphan-chunk stream's state: the pool the pages are read on, the grace in whole seconds,
/// and the position the next page resumes from.
struct ChunkScan {
    pool: PgPool,
    grace: i64,
    cursor: ChunkCursor,
    done: bool,
}

/// The part stream's state.
struct PartScan {
    pool: PgPool,
    cursor: PartCursor,
    done: bool,
}

/// The version-row streams' state, shared by the retention scan and the live-reference scan (the
/// two read the same order).
struct VersionScan {
    pool: PgPool,
    cursor: VersionCursor,
    done: bool,
}

/// One row of `live_blobs.sql`: the cursor's columns and the blob wire the fold decodes.
struct LiveBlobRow {
    bucket: String,
    key: String,
    seq: i64,
    blob: String,
}

/// The grace window in whole seconds, saturating at the `bigint` ceiling.
fn seconds(grace: Duration) -> i64 {
    grace.as_secs().try_into().unwrap_or(i64::MAX)
}

/// One page of the orphan-chunk scan, from `cursor` strictly on.
async fn orphan_chunk_page(
    pool: &PgPool,
    grace: i64,
    cursor: &ChunkCursor,
    limit: i64,
) -> Result<Vec<OrphanChunk>, Error> {
    let rows = query_file_as!(
        OrphanChunk,
        "sql/cleanup/orphan_chunks.sql",
        grace,
        cursor.bucket.as_str(),
        // The wildcard cast the crate's closed-set parameters take: the enum
        // is the store's own, so its type name has no built-in mapping in the
        // macro's table, and the cast skips the check the argument would
        // otherwise fail while `ContentKind`'s own `Encode` still writes the
        // label.
        cursor.kind as _,
        cursor.ckey.as_str(),
        cursor.seq,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One page of the upload-gone part scan, from `cursor` strictly on.
async fn orphan_part_page(
    pool: &PgPool,
    cursor: &PartCursor,
    limit: i64,
) -> Result<Vec<OrphanPart>, Error> {
    let rows = query_file_as!(
        OrphanPart,
        "sql/cleanup/orphan_part_heads.sql",
        cursor.bucket.as_str(),
        cursor.ckey.as_str(),
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One page of the retention scan, from `cursor` strictly on.
async fn history_page(
    pool: &PgPool,
    cursor: &VersionCursor,
    limit: i64,
) -> Result<Vec<HistoryRow>, Error> {
    let rows = query_file_as!(
        HistoryRow,
        "sql/cleanup/retention_rows.sql",
        cursor.bucket.as_str(),
        cursor.key.as_str(),
        cursor.seq,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// One page of the live-reference scan, from `cursor` strictly on.
///
/// A row whose blob wire does not decode names no block, so it contributes nothing to the fold
/// rather than failing the sweep — the same healing walk the contract's `bucket_rows` read
/// performs. The page itself is still the rows the cursor advanced over, so a page of corrupt
/// wires advances the scan rather than ending it.
async fn live_blob_page(
    pool: &PgPool,
    cursor: &VersionCursor,
    limit: i64,
) -> Result<Vec<LiveBlobRow>, Error> {
    let rows = query_file_as!(
        LiveBlobRow,
        "sql/cleanup/live_blobs.sql",
        cursor.bucket.as_str(),
        cursor.key.as_str(),
        cursor.seq,
        limit
    )
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

impl PgStore {
    /// The orphan chunk generations, one keyset page per item: `content` rows no live head
    /// names, older than `grace` by their own `published` stamp — and, since a publish commits
    /// its chunks before it moves the head's `live_seq`, the head's current allocation is
    /// protected for `grace` too, so a body still being staged (even one a later allocation
    /// superseded) is never read as residue.
    ///
    /// The stream is lazy: it reads a page per poll and ends when a page comes back short. An
    /// `Err` item ends it after the error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// # async fn example(store: tinio_store_pg::PgStore) -> Result<(), tinio_store_pg::Error> {
    /// let mut pages = store.orphan_chunk_pages(Duration::from_secs(600));
    /// while let Some(page) = futures::StreamExt::next(&mut pages).await {
    ///     let page = page?;
    ///     println!("{} candidates", page.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// (`ignore`: needs a live server, like every store call.)
    pub fn orphan_chunk_pages(&self, grace: Duration) -> OrphanChunkStream {
        let state = ChunkScan {
            pool: self.as_driver().clone(),
            grace: seconds(grace),
            cursor: ChunkCursor::start(),
            done: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            match orphan_chunk_page(&state.pool, state.grace, &state.cursor, CLEANUP_BATCH_ROWS)
                .await
            {
                // A page shorter than the limit is the last one: nothing is
                // left past its cursor. The cursor is the page's last
                // generation, the point the next page resumes strictly after.
                Ok(rows) => {
                    if let Some(last) = rows.last() {
                        state.cursor = ChunkCursor::after(last);
                    }
                    state.done = rows.len() < CLEANUP_BATCH_ROWS as usize;
                    Some((Ok(rows), state))
                }
                // An `Err` item ends the scan: the next poll sees `done`.
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
            }
        }))
    }

    /// Every orphan chunk generation, drained from [`Self::orphan_chunk_pages`] into one `Vec`.
    /// A caller that must not hold the whole answer streams the pages instead.
    pub async fn orphan_chunks(&self, grace: Duration) -> Result<Vec<OrphanChunk>, Error> {
        let mut pages = self.orphan_chunk_pages(grace);
        let mut rows = Vec::new();
        while let Some(page) = pages.next().await {
            rows.extend(page?);
        }
        Ok(rows)
    }

    /// Remove the orphan chunk rows `rows` named, answering how many rows went.
    /// An empty list removes nothing.
    ///
    /// The delete names the scan's keys and then applies the scan's predicate
    /// again, with the same `grace`. A generation that became live after the
    /// scan, or whose rows were rewritten inside the grace window, stays: an
    /// in-place publish does not change the sequence, and a head deleted and
    /// allocated again can reuse one, so the scan's key alone is not proof the
    /// rows are still residue. The two predicates of the scan's grace window
    /// are repeated here — the row's own `published` stamp and the head's
    /// `allocated_at` — so the delete is safe on its own, whatever the store
    /// served between the two phases.
    pub async fn reclaim_orphan_chunks(
        &self,
        rows: &[OrphanChunk],
        grace: Duration,
    ) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }
        let buckets: Vec<String> = rows.iter().map(|row| row.bucket.to_string()).collect();
        let kinds: Vec<String> = rows.iter().map(|row| row.kind.to_string()).collect();
        let ckeys: Vec<String> = rows.iter().map(|row| row.ckey.clone()).collect();
        let seqs: Vec<i64> = rows.iter().map(|row| row.seq).collect();
        let done = query_file!(
            "sql/cleanup/reclaim_orphan_chunks.sql",
            &buckets,
            &kinds,
            &ckeys,
            &seqs,
            seconds(grace)
        )
        .execute(self.pool())
        .await?;
        Ok(done.rows_affected())
    }

    /// The staged part heads whose upload row is gone — the residue of a drain interrupted
    /// between the `uploads` rows and the content removal — one keyset page per item.
    pub fn orphan_part_pages(&self) -> OrphanPartStream {
        let state = PartScan {
            pool: self.as_driver().clone(),
            cursor: PartCursor::start(),
            done: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            match orphan_part_page(&state.pool, &state.cursor, CLEANUP_BATCH_ROWS).await {
                // A short page is the last one (see `orphan_chunk_pages`).
                Ok(rows) => {
                    if let Some(last) = rows.last() {
                        state.cursor = PartCursor::after(last);
                    }
                    state.done = rows.len() < CLEANUP_BATCH_ROWS as usize;
                    Some((Ok(rows), state))
                }
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
            }
        }))
    }

    /// Every staged part head whose upload row is gone, drained from
    /// [`Self::orphan_part_pages`] into one `Vec`.
    pub async fn orphan_part_heads(&self) -> Result<Vec<OrphanPart>, Error> {
        let mut pages = self.orphan_part_pages();
        let mut rows = Vec::new();
        while let Some(page) = pages.next().await {
            rows.extend(page?);
        }
        Ok(rows)
    }

    /// Remove every staged part `rows` named — the `(bucket, ckey)` keys the scan returned —
    /// its chunks and its head together, in one statement. An empty list removes nothing, and
    /// the two removals of one part are one statement, so no partially applied page can leave
    /// a part head whose chunks are gone (or the reverse).
    pub async fn reclaim_orphan_parts(&self, rows: &[OrphanPart]) -> Result<u64, Error> {
        if rows.is_empty() {
            return Ok(0);
        }
        let buckets: Vec<String> = rows.iter().map(|row| row.bucket.to_string()).collect();
        let ckeys: Vec<String> = rows.iter().map(|row| row.ckey.clone()).collect();
        let done = query_file!("sql/cleanup/reclaim_orphan_parts.sql", &buckets, &ckeys)
            .execute(self.pool())
            .await?;
        Ok(done.rows_affected())
    }

    /// The version rows that are history — a row whose slot points somewhere
    /// else, or one a supersede stamped — one keyset page per item,
    /// newest-first per key, with the retention facts. A row with no slot and
    /// no stamp is not history: that is the window between clearing the slot
    /// and stamping the outgoing version, and a crash that stops in it. Those
    /// rows stay.
    pub fn retention_row_pages(&self) -> HistoryRowStream {
        let state = VersionScan {
            pool: self.as_driver().clone(),
            cursor: VersionCursor::start(),
            done: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            match history_page(&state.pool, &state.cursor, CLEANUP_BATCH_ROWS).await {
                // A short page is the last one (see `orphan_chunk_pages`).
                Ok(rows) => {
                    if let Some(last) = rows.last() {
                        state.cursor =
                            VersionCursor::after(&last.bucket.to_string(), &last.key, last.seq);
                    }
                    state.done = rows.len() < CLEANUP_BATCH_ROWS as usize;
                    Some((Ok(rows), state))
                }
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
            }
        }))
    }

    /// Every history row, drained from [`Self::retention_row_pages`] into one `Vec`.
    pub async fn retention_rows(&self) -> Result<Vec<HistoryRow>, Error> {
        let mut pages = self.retention_row_pages();
        let mut rows = Vec::new();
        while let Some(page) = pages.next().await {
            rows.extend(page?);
        }
        Ok(rows)
    }

    /// Every version row's block list, one keyset page per item — the block sweep's live set,
    /// read a page at a time rather than as every row of the version relation.
    pub fn live_blob_pages(&self) -> LiveBlobStream {
        let state = VersionScan {
            pool: self.as_driver().clone(),
            cursor: VersionCursor::start(),
            done: false,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }
            match live_blob_page(&state.pool, &state.cursor, CLEANUP_BATCH_ROWS).await {
                // A short page is the last one (see `orphan_chunk_pages`).
                Ok(rows) => {
                    if let Some(last) = rows.last() {
                        state.cursor = VersionCursor::after(&last.bucket, &last.key, last.seq);
                    }
                    state.done = rows.len() < CLEANUP_BATCH_ROWS as usize;
                    let blobs = rows
                        .iter()
                        .filter_map(|row| decode_blob(&row.blob))
                        .collect();
                    Some((Ok(blobs), state))
                }
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
            }
        }))
    }
}
