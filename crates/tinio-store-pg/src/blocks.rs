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
//! [`stage`](BlockStore::stage) buffers the block's bytes in memory and nothing
//! reaches the database before the publish, so an interrupted write is never
//! served -- the trait's headline guarantee, and the one a sweep can rely on.
//! [`publish`](BlockStore::publish) checks the bytes hash to the address, then
//! writes every chunk row in **one statement**. The insert runs only when the
//! address has no row yet, so a later publish cannot fill the missing `seq`
//! values of a partial block and splice the two. `ON CONFLICT DO NOTHING`
//! covers two publishes of the same address racing the empty check.
//!
//! `rows_affected` counts only the rows this publish actually inserted. No rows
//! inserted means the address already had a row and the staged bytes are
//! discarded ([`Published::Existing`]); any insert means this call stored it
//! ([`Published::Stored`]). A block whose stored length does not match its
//! rows still fails at [`open`](BlockStore::open) rather than being repaired
//! by a second publish.
//!
//! A zero-byte block -- the empty blob the engine stores as one block -- is a
//! single row with an empty chunk, so it is listed as a block of zero bytes
//! rather than read as an absent block.
//!
//! # Reads
//!
//! [`contains`](BlockStore::contains) is one point read of the primary key.
//! [`open`](BlockStore::open) reads the block's length, resolves the optional
//! [`ByteRange`] against it, selects **only the chunk rows the range touches**,
//! and slices the first and the last. A missing block is the contract's
//! `NotFound` I/O error, whose message names the block's hash; a range that
//! cannot be satisfied is [`crate::Error::InvalidRange`]
//! and is never clamped; a stored block shorter than its declared length is an
//! `UnexpectedEof` I/O error, never a short body.
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

use std::{io, time::SystemTime, vec::IntoIter};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use sqlx::{PgPool, query, query_file_as, query_scalar};
use time::OffsetDateTime;

use crate::{
    _core::{
        ByteRange,
        blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
        storage::{self, BodyStream},
    },
    Error, PgStore,
    timestamp::{from_timestamp, to_timestamp},
    window::clip_chunk,
};

/// The size of one `blocks` row's chunk (PG14): 8 MiB of the block's bytes.
pub const BLOCK_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// How many addresses one keyset page of a walk reads.
const WALK_PAGE: i64 = 256;

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
        Self { store }
    }

    /// Borrow the store whose pool this plane runs on.
    pub fn store(&self) -> &PgStore {
        &self.store
    }
}

/// A block staged in memory for a later [`BlockStore::publish`].
///
/// The engine writes the block's bytes here, and they reach the database only
/// when the publish commits; an abandoned stage is simply dropped. Version 1
/// stores a whole blob as one block, so the buffer holds the whole block (the
/// 8 MiB unit is the `blocks` row, not the stage).
///
/// # Examples
///
/// ```no_run
/// use bytes::Bytes;
/// use tinio_core::blob::StagedBlock;
/// use tinio_store_pg::blocks::Staged;
///
/// # async fn example() -> std::io::Result<()> {
/// let mut staged = Staged::default();
/// staged.write(Bytes::from_static(b"hello")).await?;
/// // Nothing has reached the database yet: `PgBlocks::publish` is what turns
/// // the buffer into the block's rows.
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default)]
pub struct Staged {
    bytes: Vec<u8>,
}

#[async_trait]
impl StagedBlock for Staged {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.bytes.extend_from_slice(&chunk);
        Ok(())
    }
}

#[async_trait]
impl BlockStore for PgBlocks {
    type Error = Error;
    type Staged = Staged;

    async fn stage(&self) -> Result<Self::Staged, Error> {
        Ok(Staged::default())
    }

    async fn publish(&self, staged: Self::Staged, hash: &BlockHash) -> Result<Published, Error> {
        let actual = BlockHash::of(&staged.bytes);
        if &actual != hash {
            return Err(address_mismatch(hash));
        }
        let hash = hash.to_hex();
        let len = staged.bytes.len() as u64;
        // One clock reading for the whole block: every row of it carries the
        // same publication time, which is what list and walk report.
        let published = to_timestamp(SystemTime::now());
        let chunks = chunk_count(len);
        let mut seqs = Vec::with_capacity(chunks as usize);
        let mut pieces: Vec<&[u8]> = Vec::with_capacity(chunks as usize);
        for seq in 0..chunks {
            let start = seq as usize * BLOCK_CHUNK_BYTES;
            let end = (start + BLOCK_CHUNK_BYTES).min(staged.bytes.len());
            seqs.push(seq as i32);
            pieces.push(&staged.bytes[start..end]);
        }
        // One statement, and only when this address has no row yet. Appending
        // the missing seqs of a partial block would splice two publishes.
        // rows_affected counts only what this publish inserted. The borrowed
        // chunk array cannot pass query_file!, the same reason insert_chunks
        // stays a runtime query.
        let inserted = sqlx::query(include_str!("../sql/blocks/publish.sql"))
            .bind(hash.as_str())
            .bind(&seqs)
            .bind(&pieces)
            .bind(len as i64)
            .bind(published)
            .execute(self.store.as_driver())
            .await?
            .rows_affected();
        // No row inserted means every chunk row of this address was already
        // there: the dedup hit, and the staged bytes are dropped with `staged`.
        Ok(if inserted == 0 {
            Published::Existing
        } else {
            Published::Stored
        })
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Error> {
        // The dedup lookup: one point read of the primary key.
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
        // One encoding of the address for both statements of this read.
        let hex = hash.to_hex();
        // The length is the block's declared one; a hash with no row is the
        // contract's NotFound, named with the address the caller asked for.
        let Some(len) = load_len(pool, &hex).await? else {
            return Err(not_found(hash));
        };
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
        // The chunk rows of the window `[first, last]`, in sequence order.
        let rows = query_file_as!(Chunk, "sql/blocks/open.sql", &hex, first, last)
            .fetch_all(pool)
            .await?;
        let mut served = 0u64;
        let mut out: Vec<Bytes> = Vec::with_capacity(rows.len());
        for row in rows {
            let Some((range, n)) = clip_chunk(
                row.chunk.len(),
                row.seq as u64,
                BLOCK_CHUNK_BYTES as u64,
                start,
                end,
            ) else {
                continue;
            };
            served += n;
            out.push(Bytes::from(row.chunk).slice(range));
        }
        // A gap in the sequence (a row removed behind the store's back) would
        // otherwise be served as a short body; the engine's UnexpectedEof rule
        // starts here.
        if served != end - start + 1 {
            return Err(short(hash, len, served));
        }
        Ok(Box::pin(stream::iter(out.into_iter().map(Ok))))
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
                    Ok(entries) if entries.is_empty() => return None,
                    Ok(entries) => {
                        let last = entries.last().expect("a non-empty page has a last entry");
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
/// whether an empty page or an error already ended the traversal.
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

/// The block's declared length, or `None` when the address has no row. The
/// address arrives already hex-encoded: the read's two statements share it.
async fn load_len(pool: &PgPool, hash: &str) -> Result<Option<u64>, Error> {
    // The block's declared length, carried by every one of its rows.
    let len = query_scalar!(
        "SELECT len FROM blocks WHERE hash = $1 ORDER BY seq LIMIT 1",
        hash
    )
    .fetch_optional(pool)
    .await?;
    Ok(len.map(|len| len as u64))
}

/// How many 8 MiB rows a block of `len` bytes occupies. A zero-byte block is
/// still one row, or it would be indistinguishable from an absent block.
fn chunk_count(len: u64) -> u64 {
    len.div_ceil(BLOCK_CHUNK_BYTES as u64).max(1)
}

/// A listing limit as the statement's `LIMIT` argument.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// Decode an address read back from the table: it was written by `to_hex`, so a
/// failure is a corrupt stored row, reported rather than panicked on.
fn decode_hash(raw: &str) -> Result<BlockHash, Error> {
    BlockHash::new(raw).map_err(|err| {
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

fn not_found(hash: &BlockHash) -> Error {
    Error::NoSuchBlock(hash.to_hex())
}

/// A stored block that holds fewer bytes than its declared length: served as an
/// error, never as a short body.
fn short(hash: &BlockHash, len: u64, served: u64) -> Error {
    Error::Storage(storage::io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("block {hash} holds {served} of its {len} bytes"),
    )))
}

#[cfg(test)]
mod tests {
    // The tests address the crate and the standard library by absolute path on
    // purpose: a shortened path would not prove the public one.
    #![allow(clippy::absolute_paths)]
    use std::{
        io,
        time::{Duration, SystemTime},
    };

    use bytes::Bytes;
    use futures::StreamExt;
    use sqlx::query_scalar;
    use tinio_util::testing::read_body;

    use super::{BLOCK_CHUNK_BYTES, PgBlocks, WALK_PAGE, chunk_count};
    use crate::_core::{
        ByteRange,
        blob::{BlockHash, BlockStore, Published, StagedBlock},
        storage::{self, collect_body},
    };

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

    #[tokio::test]
    async fn the_contract_round_trips_over_the_blocks_table() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let blocks = PgBlocks::new(store);

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
        let blocks = PgBlocks::new(store);

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

    /// The chunk arithmetic above the `bytea` cap, without materializing a
    /// gigabyte: PG14 chunks a block precisely because one field cannot hold
    /// one that large, so the plan for a >1 GiB block has to be right on its
    /// own.
    ///
    /// The materialized leg is deliberately not run — writing 1 GiB through a
    /// scratch database costs far more than the arithmetic it would re-check —
    /// and the plan's coverage row records that choice.
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
    }

    #[tokio::test]
    async fn a_zero_length_block_publishes_lists_and_refuses_a_range() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let blocks = PgBlocks::new(store);

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

    /// A stored block that holds fewer bytes than its declared length is an
    /// `UnexpectedEof` I/O error, never a short body: the row a reader is served
    /// from can be gone even though the block's own length row is not.
    #[tokio::test]
    async fn a_block_missing_one_of_its_rows_is_a_short_body_error() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let blocks = PgBlocks::new(store);

        let payload: Vec<u8> = (0..BLOCK_CHUNK_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let hash = publish(&blocks, &payload).await;
        // The tail row goes behind the store's back.
        sqlx::query_file!(
            "sql/tests/plant_blocks_missing_tail_row.sql",
            hash.to_hex(),
            1i32
        )
        .execute(blocks.store().as_driver())
        .await
        .expect("the plant");

        let err = blocks
            .open(&hash, None)
            .await
            .err()
            .expect("a block holding fewer bytes than it declares is refused");
        let message = err.to_string();
        assert!(
            message.contains(&hash.to_hex()),
            "the failure names the block: {message}"
        );
        assert!(
            message.contains(&format!("{BLOCK_CHUNK_BYTES} of its {}", payload.len())),
            "the failure counts what was served against the declared length: {message}"
        );
        let storage::Error::Io(io) = err.into() else {
            panic!("a short block must project onto an I/O error");
        };
        assert_eq!(io.kind(), io::ErrorKind::UnexpectedEof);

        // A reader asking for a window inside the surviving row still gets its
        // error rather than a body sliced out of the wrong run of the block.
        assert!(
            blocks.open(&hash, Some(ByteRange::From(0))).await.is_err(),
            "the whole block is refused, not just the tail"
        );
        db.drop_db().await;
    }

    /// An address the store never wrote fails the page it is read with, and an
    /// `Err` item ends a walk: the traversal reports the corruption instead of
    /// panicking on it.
    #[tokio::test]
    async fn an_undecodable_stored_address_is_reported_and_ends_the_walk() {
        let Some((db, store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let blocks = PgBlocks::new(store);
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
        let blocks = PgBlocks::new(store);

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
        let blocks = PgBlocks::new(store);

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
