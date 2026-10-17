//! The `bytea`-chunked content plane (PG18/PG20/PG22/PG23/PG24).
//!
//! The plane keeps object and multipart-part bytes in 1 MiB `content` rows keyed by
//! `(bucket, kind, ckey, seq, chunk_no)` and records which generation is live in
//! `content_heads.live_seq`. The shape carries three promises:
//!
//! - **Nothing is visible until publish.** [`ObjectContent::stage`] allocates a fresh
//!   generation and inserts chunks under the target key as they arrive (the keyed staging
//!   D38 asks for); `live_seq` stays `NULL`, so no reader can see the rows.
//!   [`ObjectContent::publish`] flushes the last batch and then flips `live_seq` in
//!   one guarded statement — the visibility point. An interrupted publish leaves residue,
//!   never half an object.
//! - **The key namespaces cannot collide.** An object encodes as `o:<key>` and a part as
//!   `p:<hex(upload_id)>:<number>`, discriminated by `kind` (PG20), and
//!   [`ObjectContent::scan`] filters `kind = 'object'`, so a part is never enumerated
//!   or opened as an object (spec §1 property 3).
//! - **Residue is not content.** A generation that is not live — a batch whose publish never
//!   flipped, or a superseded generation — makes neither [`ObjectContent::scan`] nor
//!   [`ObjectContent::is_empty`] true (PG23). A live generation of zero bytes
//!   (a folder marker) does make the plane non-empty.
//!
//! Every value is a bound parameter and every table or column identifier is a fixed literal;
//! every ordered read repeats `COLLATE "C"` (Review Focus 1). A generation's rows are a
//! contiguous run of `chunk_no` values from 0. A read fetches those rows a window of
//! [`READ_WINDOW_CHUNKS`] at a time, one statement per window on a connection the pool
//! hands out for that window only, so neither the bytes a read holds nor the connection it
//! borrows grows with the object (R4). [`ObjectContent::open`] decides the head and the
//! first window together — a short one is a gap ([`Error::Corrupt`]) unless the generation
//! the caller resolved is gone, which is the contract's missing content — and reports a gap
//! in a later window as `UnexpectedEof` on the body stream.
//!
//! The plane keeps no file identity and no timestamp: [`ObjectContent::scan`] and
//! [`ObjectContent::stat`] answer `identity = 0` and `mtime = None` (D42 — a
//! store with no timestamp must not fabricate one; an object's timestamp belongs to the
//! metadata row, a separate contract). It does record a publish identity:
//! `content_heads.publish_id` stores the caller's
//! [`PublishId`] as a bigint bit pattern.
//! [`ObjectContent::publish_tagged`] writes it in the same statement that flips
//! `live_seq`. [`ObjectContent::remove_if_tagged`] deletes when that identity is
//! still the head's, without re-reading the body: the tag was written with the
//! bytes. A different identity answers false and leaves the head.
//! [`ObjectContent::remove_if_matches`] compares and deletes in that same
//! transaction. An untagged publish stores NULL, so a tagged removal does not
//! take it.
//!
//! A publish's statement boundary is bounded: the stage flushes chunks in batches of
//! [`PUBLISH_BATCH_BYTES`] (four 1 MiB chunks) as they stream in, so a body far larger
//! than memory can be published without ever being held whole (PG22). A superseded
//! publish's re-home is bounded the same way ([`MOVE_BATCH_CHUNKS`] rows a statement,
//! each its own transaction) rather than re-keying the whole generation under one lock.

use std::{collections::HashMap, io, mem, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use sqlx::{PgPool, query_file_as, query_file_scalar};
use tinio_core::{
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, PublishId, StagedObject},
    etag::ETag,
    meta::BucketMeta,
    object,
    storage::{BodyStream, ByteRange},
};
use tokio::sync::mpsc;

use crate::{
    Error, PgStore, PgTx, begin,
    window::{ChunkFault, ChunkWindow, chunk_windows},
};

/// The `content_kind` PostgreSQL enum: the content plane's own discriminator,
/// which is part of every content primary key. No contract type names this set
/// (the contract's `ContentKey` distinguishes object from part by shape, not
/// by a stored tag), so it stays beside its only plane rather than on a
/// contract type -- the store carries no parallel *contract* type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, derive_more::Display)]
#[sqlx(type_name = "content_kind", rename_all = "lowercase")]
pub enum ContentKind {
    /// An object's chunks.
    #[display("object")]
    Object,
    /// A multipart part's chunks.
    #[display("part")]
    Part,
}

impl ContentKind {
    /// The `content_kind` label. One match, so an array bind cannot spell the
    /// wire differently from the enum.
    fn as_str(self) -> &'static str {
        match self {
            Self::Object => "object",
            Self::Part => "part",
        }
    }
}

/// The content plane's chunk size: 1 MiB (PG2).
pub const CONTENT_CHUNK_BYTES: usize = 1024 * 1024;

/// The publish batch bound: the stage flushes one batch of chunk rows per statement, so a
/// publish's transaction boundary is bounded (PG22).
pub const PUBLISH_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Chunks per publish batch.
pub const PUBLISH_BATCH_CHUNKS: usize = PUBLISH_BATCH_BYTES / CONTENT_CHUNK_BYTES;

/// The read window: the chunks one statement of a body read fetches, eight
/// 1 MiB rows. A read's resident bytes and the pooled connection it borrows are
/// bounded by a window rather than by the object (R4).
pub const READ_WINDOW_CHUNKS: usize = 8;

/// The re-home batch: eight chunks a statement. A superseded publish's rows are
/// re-keyed in windows of this many, each its own transaction, so a re-home's
/// boundary is bounded by a window rather than by the body, and the head's row
/// lock is not held across the generation (PG22).
pub const MOVE_BATCH_CHUNKS: u64 = 8;

/// The chunk/statement plan a publish of `len` bytes uses (PG22).
///
/// # Examples
///
/// ```
/// use tinio_store_pg::content::{PublishPlan, publish_plan};
///
/// // An empty object holds no chunk row; a 5 MiB one holds five, written by
/// // two batch statements.
/// assert_eq!(
///     publish_plan(0),
///     PublishPlan {
///         chunks: 0,
///         batches: 0
///     }
/// );
/// assert_eq!(
///     publish_plan(5 * 1024 * 1024),
///     PublishPlan {
///         chunks: 5,
///         batches: 2
///     }
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishPlan {
    /// The number of 1 MiB chunk rows the generation holds.
    pub chunks: u64,
    /// The number of batch statements the chunks are written with.
    pub batches: u64,
}

/// The plan a publish of `len` bytes executes.
///
/// # Examples
///
/// ```
/// use tinio_store_pg::content::publish_plan;
///
/// let plan = publish_plan(5 * 1024 * 1024);
/// assert_eq!(plan.chunks, 5);
/// assert_eq!(plan.batches, 2);
/// ```
pub fn publish_plan(len: u64) -> PublishPlan {
    // The count stays `u64` for the whole computation — the `expected` half of
    // the flip's guard names this many rows of the generation — and narrows
    // once, at the bind.
    let chunks = len.div_ceil(CONTENT_CHUNK_BYTES as u64);
    PublishPlan {
        chunks,
        batches: chunks.div_ceil(PUBLISH_BATCH_CHUNKS as u64),
    }
}

/// The chunk-number windows a superseded generation is re-keyed in: at most
/// [`MOVE_BATCH_CHUNKS`] rows a statement, covering `0..chunks` in order. Each
/// window is one transaction, so a re-home's boundary is bounded (PG22).
fn move_batches(chunks: u64) -> Vec<(i32, i32)> {
    let Some(last) = chunks.checked_sub(1) else {
        return Vec::new();
    };
    chunk_windows(
        0,
        i32::try_from(last).unwrap_or(i32::MAX),
        MOVE_BATCH_CHUNKS as i32,
    )
}

/// A page limit as the statement's `LIMIT` argument: a `usize` that does not fit
/// an `i64` reads as "no bound", never as a negative count.
fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}

/// The live generation a caller may put back after a publish it has to undo,
/// and the row shape every head read decodes: the sequence, the length and the
/// publish identity, in `content_heads`'s own columns (aliased to these field
/// names by the two statements that read them).
///
/// A key with no live generation has no value: the undo clears `live_seq`
/// rather than restoring one.
#[derive(Debug, Clone, Copy)]
pub struct LiveGeneration {
    seq: i64,
    len: i64,
    publish_id: Option<i64>,
}

/// One content row: the chunk's number and its bytes.
struct StoredChunk {
    chunk_no: i32,
    chunk: Vec<u8>,
}

/// One scanned head: the encoded content key and the live length it names.
struct StoredScan {
    ckey: String,
    live_len: i64,
}

/// One live generation a caller may open by sequence.
#[derive(Debug, Clone, Copy)]
pub struct LiveSpan {
    /// The generation `live_seq` named when this span was read.
    pub seq: i64,
    /// The generation's length.
    pub len: u64,
}

/// One live generation, keyed by the content key the caller asked for.
struct StoredLen {
    ckey: String,
    seq: i64,
    len: i64,
}

/// The content relation's collision-proof key namespace (PG20): an object key and a part key
/// can never share a row, because the kind discriminator is part of the relation's primary
/// key and the encoded key is prefix-disjoint.
fn object_ckey(key: &object::Key) -> String {
    format!("{OBJECT_CKEY_PREFIX}{key}")
}

/// The object key space's own prefix, and the smallest key above all of them:
/// `o:`'s last character incremented. Every object key starts with the first,
/// which is the floor [`prefix_upper`]'s strip stops at.
const OBJECT_CKEY_PREFIX: &str = "o:";
const OBJECT_CKEY_UPPER: &str = "o;";

/// The next Unicode scalar value above `c`, `None` when `c` is the last one
/// (`U+10FFFF`). The surrogate range answers `None` too: no key can hold one, so
/// the strip loop drops the character instead of bounding with a value no
/// `text` can carry.
fn next_scalar(c: char) -> Option<char> {
    char::from_u32(u32::from(c) + 1)
}

/// The exclusive upper bound of the byte range that covers every key starting
/// with `lower`: the smallest string greater than all of them.
///
/// UTF-8 byte order follows code point order, so incrementing the prefix's last
/// character makes `[lower, upper)` exactly `lower`'s prefix set — which is why
/// the predicate is a range the plan keeps as an `Index Cond` rather than a
/// `starts_with` call it can only filter with. A trailing `U+10FFFF` (or a lone
/// surrogate) has no successor and is dropped instead, the character before it
/// incremented in its place: the range is then a superset, and the statement's
/// `starts_with` is what keeps the answer exact. The strip stops at the object
/// key space's own `o:`, whose `:` always has a successor, so a prefix this
/// plane walks always has a bound.
fn prefix_upper(lower: &str) -> String {
    let mut base = lower;
    loop {
        let Some(last) = base.chars().next_back() else {
            return OBJECT_CKEY_UPPER.to_owned();
        };
        let Some(next) = next_scalar(last) else {
            base = &base[..base.len() - last.len_utf8()];
            continue;
        };
        let mut upper = String::with_capacity(base.len() + 4);
        upper.push_str(&base[..base.len() - last.len_utf8()]);
        upper.push(next);
        return upper;
    }
}

fn part_ckey(upload_id: &str, number: u32) -> String {
    let encoded = hex::encode(upload_id);
    format!("p:{encoded}:{number:04}")
}

fn content_key(key: &ContentKey) -> (ContentKind, String) {
    match key {
        ContentKey::Object(key) => (ContentKind::Object, object_ckey(key)),
        ContentKey::Part { upload_id, number } => {
            (ContentKind::Part, part_ckey(upload_id, u32::from(*number)))
        }
    }
}

fn scan_key(bucket: &bucket::Name, ckey: &str) -> Result<object::Key, Error> {
    let raw = ckey.get(2..).unwrap_or("");
    object::key(raw).map_err(|_| corrupt_content(bucket, ckey))
}

fn corrupt_content(bucket: &bucket::Name, ckey: &str) -> Error {
    Error::Corrupt {
        bucket: bucket.clone(),
        key: ckey.to_owned(),
    }
}

/// A staged write: the generation [`ObjectContent::stage`] allocated, the chunks the caller
/// streams, and the bytes that have landed.
///
/// The handle carries the `(bucket, kind, ckey)` it was staged at — staging is keyed
/// (D38), so the chunks are already under the target key when publish flips the head. The
/// key is therefore known up front: [`ObjectContent::publish`] publishes the generation the
/// handle names.
///
/// # Examples
///
/// ```no_run
/// use bytes::Bytes;
/// use tinio_core::{
///     bucket,
///     content::{ContentKey, ObjectContent, StagedObject},
///     object,
/// };
/// use tinio_store_pg::PgStore;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
/// let bucket = bucket::name("data")?;
/// let content = ContentKey::Object(object::key("a.txt")?);
/// let mut staged = store.stage(&bucket, &content).await?;
/// staged.write(Bytes::from_static(b"hello")).await?;
/// // Against a live server this writes the staged chunk under the key and
/// // flips the head, so a reader resolves `a.txt` to this generation.
/// store.publish(&bucket, staged, &content).await?;
/// # Ok(())
/// # }
/// ```
pub struct PgStaged {
    pool: PgPool,
    bucket: String,
    kind: ContentKind,
    ckey: String,
    seq: i64,
    landed: u64,
    next_chunk: i32,
    carry: Vec<u8>,
    pending: Vec<(i32, Vec<u8>)>,
}

impl PgStaged {
    /// Write the queued rows as one statement. Each batch is its own transaction and the
    /// generation stays invisible until the publish flip, so a failure here leaves residue,
    /// never a half object.
    async fn flush(&mut self) -> Result<(), Error> {
        if self.pending.is_empty() {
            return Ok(());
        }
        {
            let numbers: Vec<i32> = self
                .pending
                .iter()
                .map(|(number, _chunk)| *number)
                .collect();
            let chunks: Vec<&[u8]> = self
                .pending
                .iter()
                .map(|(_number, chunk)| chunk.as_slice())
                .collect();
            // BYTEA[]'s macro mapping is `&[Vec<u8>]`. The wildcard skips that
            // one check and still binds the borrowed slices; copying each chunk
            // to satisfy the mapping is not acceptable. `kind as _` is the enum.
            sqlx::query_file!(
                "sql/content/insert_chunks.sql",
                &self.bucket,
                self.kind as _,
                self.ckey.as_str(),
                self.seq,
                &numbers,
                &chunks as _
            )
            .execute(&self.pool)
            .await?;
        }
        self.pending.clear();
        Ok(())
    }

    /// Flush the tail: the partial 1 MiB chunk the stream left in the carry, then the
    /// queued batches. A zero-byte stage flushes nothing and still flips to live.
    async fn finish(&mut self) -> Result<(), Error> {
        if !self.carry.is_empty() {
            let tail = mem::take(&mut self.carry);
            self.pending.push((self.next_chunk, tail));
            self.next_chunk += 1;
        }
        self.flush().await
    }
}

#[async_trait]
impl StagedObject for PgStaged {
    type Error = Error;

    /// Append one chunk to the staged generation, slicing it into 1 MiB rows and flushing a
    /// full batch as one statement. The chunk the caller hands over is the largest buffer
    /// this ever holds beyond the batch it is filling, so a huge body never lands whole.
    async fn write(&mut self, chunk: Bytes) -> Result<(), Error> {
        self.landed += chunk.len() as u64;
        let mut rest: &[u8] = &chunk;
        while !rest.is_empty() {
            let room = CONTENT_CHUNK_BYTES - self.carry.len();
            let take = room.min(rest.len());
            self.carry.extend_from_slice(&rest[..take]);
            rest = &rest[take..];
            if self.carry.len() == CONTENT_CHUNK_BYTES {
                let full = mem::take(&mut self.carry);
                self.pending.push((self.next_chunk, full));
                self.next_chunk += 1;
                if self.pending.len() >= PUBLISH_BATCH_CHUNKS {
                    self.flush().await?;
                }
            }
        }
        Ok(())
    }
}

/// Drop one key's chunks and clear its live generation. The head row stays, and
/// so does `next_seq`: a later allocation continues from it instead of starting
/// again at 1, so an upload that still holds the old sequence cannot write into
/// the next object's rows. One statement, the batch drain, so the chunks and
/// the head clear together even when the caller forgot its transaction.
async fn drain_key(
    tx: &mut PgTx,
    bucket: &str,
    kind: ContentKind,
    ckey: &str,
) -> Result<(), Error> {
    let kinds = vec![kind.as_str().to_owned()];
    let ckeys = vec![ckey.to_owned()];
    sqlx::query_file!("sql/content/drain_keys.sql", bucket, &kinds, &ckeys)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Lock the live head. A missing generation is `None`; the row stays locked
/// until the caller ends the transaction.
async fn lock_head(
    tx: &mut PgTx,
    bucket: &str,
    kind: ContentKind,
    ckey: &str,
) -> Result<Option<LiveGeneration>, Error> {
    Ok(query_file_as!(
        LiveGeneration,
        "sql/content/lock_live_head.sql",
        bucket,
        kind as _,
        ckey
    )
    .fetch_optional(&mut **tx)
    .await?)
}

/// Lock the source head a rename or copy acts on, and hand the transaction back
/// with it: the caller's destination drain and key write then run under the
/// lock, so a source that disappears between the read and the key write cannot
/// destroy the destination and still report success. A source with no live
/// generation rolls the transaction back and answers [`Error::NoSuchKey`], so
/// the destination is never touched. The row stays locked until the caller ends
/// the transaction.
async fn live_source(
    mut tx: PgTx,
    wire: &str,
    from: &object::Key,
) -> Result<(PgTx, LiveGeneration), Error> {
    let from_ckey = object_ckey(from);
    let Some(source) = lock_head(&mut tx, wire, ContentKind::Object, &from_ckey).await? else {
        tx.rollback().await?;
        return Err(Error::NoSuchKey(from.clone()));
    };
    Ok((tx, source))
}

/// Whether the live generation's bytes equal `expected`, read on `tx`. A
/// different length is `false` without reading the body. A gap is
/// [`Error::Corrupt`].
///
/// The compare walks the generation one [`READ_WINDOW_CHUNKS`] window at a
/// time, so the ceiling on what this holds is one window plus the caller's own
/// `expected` buffer, whatever the object's size. The head's row lock is still
/// held for the whole compare — the bytes are what it compares — so a
/// concurrent writer of this key waits for the read, not for a materialization
/// of the generation.
async fn generation_equals(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    kind: ContentKind,
    ckey: &str,
    seq: i64,
    len: u64,
    expected: &[u8],
) -> Result<bool, Error> {
    if expected.len() as u64 != len {
        return Ok(false);
    }
    if len == 0 {
        return Ok(true);
    }
    let mut offset = 0usize;
    let mut same = true;
    walk_generation(tx, bucket, kind, ckey, seq, len, |bytes| {
        let next = offset + bytes.len();
        if expected.get(offset..next) != Some(bytes) {
            same = false;
            return Ok(false);
        }
        offset = next;
        Ok(true)
    })
    .await?;
    Ok(same)
}

/// Walk generation `seq` in chunk order, one bounded window of rows per
/// statement. `on_bytes` sees each slice the window keeps; `Ok(false)` stops
/// before the window is checked, which is how a compare reports inequality
/// without calling a short body corrupt. A gap or a short run is
/// [`Error::Corrupt`].
async fn walk_generation(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    kind: ContentKind,
    ckey: &str,
    seq: i64,
    len: u64,
    mut on_bytes: impl FnMut(&[u8]) -> Result<bool, Error>,
) -> Result<(), Error> {
    if len == 0 {
        return Ok(());
    }
    let end = len - 1;
    let first = 0i32;
    let last = (end / CONTENT_CHUNK_BYTES as u64) as i32;
    let mut window = ChunkWindow::new(0, end, CONTENT_CHUNK_BYTES as u64, true);
    let windows = chunk_windows(first, last, READ_WINDOW_CHUNKS as i32);
    let count = windows.len();
    for (index, (from, to)) in windows.into_iter().enumerate() {
        let rows = query_file_as!(
            StoredChunk,
            "sql/content/open_chunks.sql",
            bucket.as_ref(),
            kind as _,
            ckey,
            seq,
            from,
            to
        )
        .fetch_all(&mut **tx)
        .await?;
        for row in rows {
            let Some(bytes) = window
                .push(row.chunk_no, row.chunk)
                .map_err(|_| corrupt_content(bucket, ckey))?
            else {
                continue;
            };
            if !on_bytes(&bytes)? {
                return Ok(());
            }
        }
        if index + 1 == count {
            window.finish().map_err(|_| corrupt_content(bucket, ckey))?;
        }
    }
    Ok(())
}

/// The chunk-number windows a live copy writes, one bounded statement each: the
/// re-home's own window shape ([`move_batches`], [`MOVE_BATCH_CHUNKS`] rows a
/// statement), shared so the two bounded walks over a generation cannot drift
/// apart.
fn copy_batches(chunks: u64) -> Vec<(i32, i32)> {
    move_batches(chunks)
}

/// Publish `from_ckey`'s live generation onto `to_ckey` at the destination's
/// next sequence. The destination head is kept, so a writer that still holds
/// an older sequence cannot land on this one. The caller drained the
/// destination's live bytes in this same transaction.
///
/// **The copy is bounded -- one [`MOVE_BATCH_CHUNKS`]-row statement a window --
/// but it stays inside the caller's transaction.** The re-home re-keys a
/// *staged* generation in one transaction per window because nothing can see
/// those rows until the flip; a live copy has no such freedom, so the two
/// invariants the atomic statement held are re-argued here rather than
/// weakened:
///
/// - **the source head stays pinned (nothing may publish over it mid-copy).**
///   The caller's transaction holds the source head's `FOR UPDATE` lock
///   ([`live_source`]) for its whole life, so no concurrent writer can flip the
///   source's `live_seq` or drain its rows between the windows -- the bytes
///   copied are one unchanged generation. A window per *transaction* would drop
///   that lock between windows, which is exactly the freedom the live source
///   does not have (unlike the staged rows a re-home moves).
/// - **the destination becomes live only at the final flip.** Every window
///   inserts under the destination's fresh, as-yet-invisible sequence; only the
///   `publish_head` below sets `live_seq`, so a reader sees the destination's
///   old generation (already drained) or the new one whole, never a partial
///   copy.
///
/// The trade this leaves: the transaction is as long as the copy, which is what
/// the source pin costs. Only the *statement* is now bounded -- a large object
/// is no longer one `INSERT ... SELECT` over its whole generation (PG22).
async fn publish_live_copy(
    tx: &mut PgTx,
    wire: &str,
    from_ckey: &str,
    source_seq: i64,
    to_ckey: &str,
    len: i64,
) -> Result<(), Error> {
    let seq = query_file_scalar!(
        "sql/content/alloc_head.sql",
        wire,
        ContentKind::Object as _,
        to_ckey
    )
    .fetch_one(&mut **tx)
    .await?;
    // The source's chunk rows are copied one bounded window a statement, in
    // `0..chunks` order, all within the caller's transaction. A zero-byte live
    // generation has no rows, so the window list is empty and only the flip
    // below runs.
    let chunks = publish_plan(len as u64).chunks;
    for (first, last) in copy_batches(chunks) {
        sqlx::query_file!(
            "sql/content/copy_content.sql",
            to_ckey,
            seq,
            wire,
            ContentKind::Object as _,
            from_ckey,
            source_seq,
            first,
            last
        )
        .execute(&mut **tx)
        .await?;
    }
    let published = sqlx::query_file!(
        "sql/content/publish_head.sql",
        wire,
        ContentKind::Object as _,
        to_ckey,
        seq,
        len,
        None::<i64>,
        chunks as i64
    )
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if published != 1 {
        return Err(Error::PublishRaced);
    }
    Ok(())
}

/// Flip the staged generation live and record `publish_id` (`None` clears a
/// previous tag) — the visibility point.
///
/// **The flip is one statement, and both halves of the guard live in its
/// `WHERE`.** The count subquery is compared against the generation's planned
/// chunk count, and the head's `next_seq` must still be this generation's, so an
/// interrupted publish stays invisible and a superseded generation can never go
/// live. The lock is that statement's own CTE, whose `SELECT ... FOR UPDATE`
/// holds the generation's chunk rows for the whole statement — so a sweep that
/// would reclaim chunks out from under a body being written cannot delete them
/// inside the count-and-flip window.
///
/// **A zero-row flip is ambiguous by construction, and the ambiguity is resolved
/// only on that path**, inside the same transaction, by re-reading the count.
/// The count is what decides it, because the two failures are not the same
/// answer: a short run — a sweep that reclaimed chunks anyway — is
/// [`Error::PublishRaced`] and is not flipped, because re-homing cannot put
/// missing chunks back; a head whose sequence moved says a later allocation
/// already owns the head and this publish lost the *allocation* race. Two
/// concurrent PUTs of one key must resolve to one of the two bodies, never to a
/// 5xx, so the loser re-homes its generation onto the head's next free sequence
/// and flips there (see [`re_home_and_flip`]) instead of surfacing the error.
///
/// Answers the sequence the flip made live: the staged generation's own, or the
/// one a re-home moved it onto.
async fn flip_head(staged: &PgStaged, publish_id: Option<i64>) -> Result<i64, Error> {
    let expected = publish_plan(staged.landed).chunks as i64;
    let mut tx = begin(&staged.pool).await?;
    let published = sqlx::query_file!(
        "sql/content/publish_head.sql",
        &staged.bucket,
        staged.kind as _,
        &staged.ckey,
        staged.seq,
        staged.landed as i64,
        publish_id,
        expected
    )
    .execute(&mut *tx)
    .await?;
    if published.rows_affected() == 1 {
        tx.commit().await?;
        return Ok(staged.seq);
    }
    let found = query_file_scalar!(
        "sql/content/generation_count.sql",
        &staged.bucket,
        staged.kind as _,
        &staged.ckey,
        staged.seq
    )
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await?;
    if found != expected {
        return Err(Error::PublishRaced);
    }
    // **One attempt, not a retry loop.** `re_home_and_flip` allocates a sequence the head will
    // never hand out again and flips the generation's own completeness there, so a second
    // attempt could only repeat the first: the sequence it would allocate is a different one,
    // and the rows it would move are the ones this attempt already left where they are. The
    // `Err` below is the invariant's floor, kept rather than a silent success.
    if let Some(seq) = re_home_and_flip(staged, publish_id).await? {
        return Ok(seq);
    }
    Err(Error::PublishRaced)
}

/// Re-home the staged generation and make it live.
///
/// A publish whose generation a later allocation superseded cannot flip it —
/// that is the guard's whole purpose — so this moves the generation's chunk
/// rows onto a freshly allocated sequence and flips that. Answers whether the
/// generation is now live.
///
/// **The move is bounded, one window of [`MOVE_BATCH_CHUNKS`] rows a
/// statement, each its own transaction (PG22).** Rewriting a whole generation
/// under the head's row lock is what PG22 excludes: a 100 GiB body would be one
/// statement and one lock hold over 102 400 rows. The generation stays
/// invisible for the whole move — `live_seq` does not name it until the flip —
/// so a reader can neither see half a body nor a sequence that was never
/// published.
///
/// **The flip's guard is the generation's own completeness**, spelled by
/// `publish_rehomed.sql`: the target sequence was allocated by this publish and
/// the head will never hand it out again, so no other writer is writing rows at
/// it. `publish_head.sql`'s `next_seq = $4` cannot be asked here, because the
/// move ran before the flip rather than under the head's row lock; the count
/// subquery is what it would have added. A concurrent publish that lands in
/// between wins as the last writer, which is exactly the promise two racing
/// PUTs of one key get.
///
/// The move changes the rows' key. `content.chunk` is `STORAGE EXTERNAL`, so a
/// 1 MiB chunk is stored out-of-line and PostgreSQL copies its TOAST pointer,
/// not its bytes, into the new heap tuple; a body small enough to keep its
/// chunks inline (below the TOAST threshold) is physically rewritten by the
/// `seq` update, so this is not a blanket "no body bytes are rewritten" — it is
/// a re-key of the generation's own rows, never a re-read or re-upload of the
/// body.
///
/// **The rows must survive the whole window** between the superseding
/// allocation and this move. The head's `allocated_at` gate protects only the
/// head's current `next_seq` generation, so a generation a later allocation
/// superseded is published-and-unprotected for that entire span — not merely
/// the length of this move. That is what the per-row `content.published`
/// stamp and the cleanup predicates that read it are for: they
/// keep a recent generation, superseded or not, out of the sweep, and still age
/// a crashed partial out after the grace. As a floor against a reclaim that got
/// through anyway, the move's row count is checked against the generation's
/// chunk count and a short move is left re-keyed but unflipped rather than
/// flipped onto a live head whose chunk run does not match its `live_len`.
///
/// Answers the sequence the flip made live, `None` when the flip lost.
async fn re_home_and_flip(
    staged: &PgStaged,
    publish_id: Option<i64>,
) -> Result<Option<i64>, Error> {
    let chunks = publish_plan(staged.landed).chunks;
    // The target: a sequence this publish allocated and the head will not hand
    // out again, so the re-keyed rows belong to this generation alone.
    let seq = query_file_scalar!(
        "sql/content/alloc_head.sql",
        &staged.bucket,
        staged.kind as _,
        staged.ckey
    )
    .fetch_one(&staged.pool)
    .await?;
    let mut moved = 0u64;
    for (first, last) in move_batches(chunks) {
        let rows = sqlx::query_file!(
            "sql/content/move_generation.sql",
            &staged.bucket,
            staged.kind as _,
            staged.ckey,
            staged.seq,
            seq,
            first,
            last
        )
        .execute(&staged.pool)
        .await?
        .rows_affected();
        moved += rows;
    }
    if moved != chunks {
        // A concurrent reclaim took rows out from under the generation: flipping
        // now would make a live head whose chunk run is short, and `open` would
        // answer `Corrupt` on a body the metadata row reports whole. Fail
        // loudly instead of serving it.
        return Err(Error::PublishRaced);
    }
    let mut tx = begin(&staged.pool).await?;
    let published = sqlx::query_file!(
        "sql/content/publish_rehomed.sql",
        &staged.bucket,
        staged.kind as _,
        staged.ckey,
        seq,
        staged.landed as i64,
        publish_id,
        chunks as i64
    )
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if published == 1 {
        tx.commit().await?;
        return Ok(Some(seq));
    }
    tx.rollback().await?;
    Ok(None)
}

/// The lock, the compare and the delete are one transaction, so a publish that lands
/// between them loses the bytes it just stored.
///
/// A caller that holds the head's publish identity is answered from the tag
/// alone: the tag was written in the same statement as the bytes, so a matching
/// one is strictly narrower than any compare, and the compensation must not
/// re-read a large body to decide. `expected` is the byte compare for the
/// caller that holds no identity; it runs under the head's row lock, one
/// [`READ_WINDOW_CHUNKS`] window of the generation resident at a time — the
/// bytes are what it compares — so another writer of this key waits for the
/// read, never for the generation to be held whole.
async fn remove_guarded(
    store: &PgStore,
    bucket: &bucket::Name,
    key: &ContentKey,
    id: Option<PublishId>,
    expected: Option<&[u8]>,
) -> Result<bool, Error> {
    let (kind, ckey) = content_key(key);
    let wire = bucket.to_string();
    let mut tx = begin(store.pool()).await?;
    let Some(head) = lock_head(&mut tx, &wire, kind, &ckey).await? else {
        tx.rollback().await?;
        return Ok(false);
    };
    if let Some(id) = id {
        let stored = head.publish_id.map(|bits| bits as u64);
        if stored != Some(u64::from(id)) {
            tx.rollback().await?;
            return Ok(false);
        }
        drain_key(&mut tx, &wire, kind, &ckey).await?;
        tx.commit().await?;
        return Ok(true);
    }
    let Some(expected) = expected else {
        // No identity and no bytes to compare: nothing the caller named can
        // match, so the head stays.
        tx.rollback().await?;
        return Ok(false);
    };
    let matches = generation_equals(
        &mut tx,
        bucket,
        kind,
        &ckey,
        head.seq,
        head.len as u64,
        expected,
    )
    .await?;
    if !matches {
        tx.rollback().await?;
        return Ok(false);
    }
    drain_key(&mut tx, &wire, kind, &ckey).await?;
    tx.commit().await?;
    Ok(true)
}

impl PgStore {
    /// The live length of one content key, of **either** kind — the object half is the
    /// contract's own [`ObjectContent::stat`], the part half is a question the contracts leave
    /// to the backend (D23: a part carries no size anywhere, and the multipart listing and the
    /// completion are what join it). `None` when the key holds no live generation, so residue
    /// reads as absent exactly as it does for an object (PG23).
    ///
    /// The timestamp and the identity a [`ContentEntry`] carries have no part counterpart: the
    /// content relation keeps neither (D42), so this answers the length the caller asked for
    /// and nothing it did not.
    pub async fn live_len(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Option<u64>, Error> {
        Ok(self
            .live_generation(bucket, key)
            .await?
            .map(|head| head.len as u64))
    }

    /// The live generation of each key, in `keys`' order. `None` is a key with
    /// no live generation. One statement: a completion checks every part, then
    /// copies the generation it checked.
    ///
    /// # Errors
    ///
    /// The driver's answer when the read fails.
    pub async fn live_lens(
        &self,
        bucket: &bucket::Name,
        keys: &[ContentKey],
    ) -> Result<Vec<Option<LiveSpan>>, Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let ckeys: Vec<String> = keys.iter().map(|key| content_key(key).1).collect();
        let rows = query_file_as!(StoredLen, "sql/content/live_lens.sql", bucket, &ckeys)
            .fetch_all(self.pool())
            .await?;
        let mut found = HashMap::with_capacity(rows.len());
        for row in rows {
            found.insert(
                row.ckey,
                LiveSpan {
                    seq: row.seq,
                    len: row.len as u64,
                },
            );
        }
        Ok(ckeys.into_iter().map(|ckey| found.remove(&ckey)).collect())
    }

    /// The key's live generation, or `None` when nothing is live.
    ///
    /// A completion that publishes and then fails reads this before the publish
    /// and again after it, and [`Self::restore_live`] puts the earlier value
    /// back while the later sequence is still the one `live_seq` names.
    pub async fn live_generation(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
    ) -> Result<Option<LiveGeneration>, Error> {
        let (kind, ckey) = content_key(key);
        let row = query_file_as!(
            LiveGeneration,
            "sql/content/live_generation.sql",
            bucket,
            kind as _,
            ckey
        )
        .fetch_optional(self.pool())
        .await?;
        Ok(row)
    }

    /// Put `previous` back as the live generation, but only while `published`
    /// is still the one `live_seq` names.
    ///
    /// `previous: None` clears the live generation: the publish was the key's
    /// first, and undoing it leaves no content. A zero-row update is success —
    /// a newer writer already owns the head, and this call must not take it.
    /// Answers whether this call restored the head.
    pub async fn restore_live(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        published: LiveGeneration,
        previous: Option<LiveGeneration>,
    ) -> Result<bool, Error> {
        let (kind, ckey) = content_key(key);
        let (seq, len, publish_id) = match previous {
            Some(previous) => (Some(previous.seq), previous.len, previous.publish_id),
            None => (None, 0, None),
        };
        let updated = sqlx::query_file!(
            "sql/content/restore_live.sql",
            bucket,
            kind as _,
            ckey,
            seq,
            len,
            publish_id,
            published.seq
        )
        .execute(self.pool())
        .await?;
        Ok(updated.rows_affected() == 1)
    }

    /// Publish the staged generation — tagged with `id` when one is given — and
    /// answer the [`LiveGeneration`] the flip committed.
    ///
    /// The generation is the staged one's own sequence, or the sequence a re-home
    /// moved it onto, so a caller that may have to undo the publish keeps the
    /// value instead of re-reading the head it just moved.
    pub async fn publish_generation(
        &self,
        mut staged: PgStaged,
        id: Option<PublishId>,
    ) -> Result<LiveGeneration, Error> {
        staged.finish().await?;
        let publish_id = id.map(|id| u64::from(id) as i64);
        let seq = flip_head(&staged, publish_id).await?;
        Ok(LiveGeneration {
            seq,
            len: staged.landed as i64,
            publish_id,
        })
    }

    /// Open generation `seq`, `len` bytes, whether or not it is still live.
    ///
    /// A completion copies the generation it checked. A later upload of the same
    /// part moves the head; those chunks stay until the completion drains them.
    ///
    /// # Errors
    ///
    /// The driver's answer when a chunk read fails. The stream reports a gap.
    pub async fn open_generation(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        seq: i64,
        len: u64,
    ) -> Result<BodyStream, Error> {
        if len == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        let (kind, ckey) = content_key(key);
        let label = format!("{bucket}/{ckey}");
        let (first, last) = read_span(0, len - 1);
        Ok(stream_chunks(ChunkRead {
            pool: PgPool::clone(self.pool()),
            generation: Generation {
                wire: bucket.to_string(),
                kind,
                ckey,
                seq,
            },
            window: ChunkWindow::new(0, len - 1, CONTENT_CHUNK_BYTES as u64, true),
            windows: chunk_windows(first, last, READ_WINDOW_CHUNKS as i32),
            label,
        }))
    }
}

/// The first and the last chunk number an inclusive byte range `[start, end]`
/// touches.
fn read_span(start: u64, end: u64) -> (i32, i32) {
    (
        (start / CONTENT_CHUNK_BYTES as u64) as i32,
        (end / CONTENT_CHUNK_BYTES as u64) as i32,
    )
}

/// The generation one read addresses: the encoded key and sequence its rows live
/// under, and the kind that discriminates the key space.
struct Generation {
    wire: String,
    kind: ContentKind,
    ckey: String,
    seq: i64,
}

/// One read of one generation: the bytes `[start, end]`, fetched as `windows`
/// of chunk numbers, clipped by `window`.
struct ChunkRead {
    pool: PgPool,
    generation: Generation,
    window: ChunkWindow,
    windows: Vec<(i32, i32)>,
    label: String,
}

/// Stream every window of a read, with nothing resolved ahead of it: the
/// generation a caller opens by sequence, rather than by head.
fn stream_chunks(read: ChunkRead) -> BodyStream {
    stream_chunks_after(read, Vec::new())
}

/// Stream a read's remaining windows, after the bytes it already resolved.
///
/// One window is one `fetch_all`, which borrows a pooled connection, reads its
/// rows and gives the connection back before the window's bytes are yielded: a
/// read holds a connection for one fetch, never for the whole of a client's
/// read, and its resident bytes are bounded by one window (R4). One chunk is in
/// flight; a gap or a run that stops short is an error on the stream, after
/// earlier chunks may already have been yielded. `head` is what `open` resolved
/// with the head, so the first answer a client sees cannot be a gap a removal
/// left behind.
fn stream_chunks_after(read: ChunkRead, head: Vec<Bytes>) -> BodyStream {
    let ChunkRead {
        pool,
        generation,
        mut window,
        windows,
        label,
    } = read;
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        for bytes in head {
            if tx.send(Ok(bytes)).await.is_err() {
                return;
            }
        }
        let count = windows.len();
        for (index, (first, last)) in windows.into_iter().enumerate() {
            let rows = query_file_as!(
                StoredChunk,
                "sql/content/open_chunks.sql",
                &generation.wire,
                generation.kind as _,
                &generation.ckey,
                generation.seq,
                first,
                last
            )
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
                match window.push(row.chunk_no, row.chunk) {
                    Err(fault) => {
                        let _ = tx.send(Err(short_content(&label, fault))).await;
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
            if index + 1 == count
                && let Err(fault) = window.finish()
            {
                let _ = tx.send(Err(short_content(&label, fault))).await;
                return;
            }
        }
    });
    Box::pin(stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    }))
}

/// The body error for a window whose bytes do not add up: the row's name with
/// the bytes served and the bytes the read owed.
fn short_content(label: &str, fault: ChunkFault) -> io::Error {
    let (served, expect) = fault.counts();
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("content {label} holds {served} of its {expect} bytes"),
    )
}

/// Fetch one window of `[start, end]` of generation `seq`, clip it into
/// `window` and answer the bytes it keeps. `done` says this is the read's last
/// window, so the run must have covered it exactly.
///
/// The statement borrows a pooled connection for the fetch alone. A gap or a
/// run that stops short is [`Error::Corrupt`]; the caller decides what a short
/// *first* window means.
async fn fill_window(
    pool: &PgPool,
    bucket: &bucket::Name,
    generation: &Generation,
    span: (i32, i32),
    window: &mut ChunkWindow,
    done: bool,
) -> Result<Vec<Bytes>, Error> {
    let rows = query_file_as!(
        StoredChunk,
        "sql/content/open_chunks.sql",
        &generation.wire,
        generation.kind as _,
        &generation.ckey,
        generation.seq,
        span.0,
        span.1
    )
    .fetch_all(pool)
    .await?;
    let mut bytes = Vec::new();
    for row in rows {
        let Some(kept) = window
            .push(row.chunk_no, row.chunk)
            .map_err(|_| corrupt_content(bucket, &generation.ckey))?
        else {
            continue;
        };
        bytes.push(kept);
    }
    if done {
        window
            .finish()
            .map_err(|_| corrupt_content(bucket, &generation.ckey))?;
    }
    Ok(bytes)
}

/// What a short window answers once the head it was read for has been re-read:
/// the generation the caller resolved is gone — removed, or replaced by a later
/// publish — so the bytes are simply not there any more
/// ([`Error::MissingContent`], never a corruption claim about a key that was
/// deleted); it is still live, so the run really is damaged
/// ([`Error::Corrupt`]).
async fn missing_or_corrupt(
    store: &PgStore,
    bucket: &bucket::Name,
    key: &ContentKey,
    seq: i64,
) -> Result<Error, Error> {
    let live = store.live_generation(bucket, key).await?;
    Ok(match live {
        Some(head) if head.seq == seq => corrupt_content(bucket, &content_key(key).1),
        _ => Error::MissingContent(key.clone()),
    })
}

#[async_trait]
impl ObjectContent for PgStore {
    type Error = Error;
    type Staged = PgStaged;

    /// The head stores the publish identity [`ObjectContent::publish_tagged`] writes.
    fn records_publish_identity(&self) -> bool {
        true
    }

    /// Allocate a fresh generation under the target key (D38). The statement
    /// locks the bucket row first, in the same order a bucket teardown locks
    /// it, so a head cannot appear in a bucket the teardown already decided was
    /// empty. The head row's `live_seq` stays `NULL` — the incompleteness
    /// marker — so nothing written into the generation is visible until
    /// publish. A missing bucket row is [`Error::NoSuchBucket`].
    async fn stage(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<PgStaged, Error> {
        let (kind, ckey) = content_key(key);
        let wire = bucket.to_string();
        let seq = query_file_scalar!("sql/content/alloc_head.sql", &wire, kind as _, ckey)
            .fetch_optional(self.pool())
            .await?;
        let Some(seq) = seq else {
            return Err(Error::NoSuchBucket(bucket.clone()));
        };
        Ok(PgStaged {
            pool: PgPool::clone(self.pool()),
            bucket: wire,
            kind,
            ckey,
            seq,
            landed: 0,
            next_chunk: 0,
            carry: Vec::new(),
            pending: Vec::new(),
        })
    }

    /// Flush the staged tail and flip the head — one guarded statement on the ordinary
    /// path, and the visibility point. A stage another allocation superseded re-homes its
    /// generation onto the head's next free sequence and flips there, so a racing publish
    /// is last-writer-wins and never surfaces an error; only a stage whose publish never
    /// runs leaves its rows as residue for the sweep.
    async fn publish(
        &self,
        _bucket: &bucket::Name,
        staged: PgStaged,
        _key: &ContentKey,
    ) -> Result<(), Error> {
        self.publish_generation(staged, None).await.map(|_| ())
    }

    /// Publish and store `id` in the same statement that flips the head, so a
    /// later tagged removal can tell this publish from a byte-identical one.
    async fn publish_tagged(
        &self,
        _bucket: &bucket::Name,
        staged: PgStaged,
        _key: &ContentKey,
        id: PublishId,
    ) -> Result<(), Error> {
        self.publish_generation(staged, Some(id)).await.map(|_| ())
    }

    /// Open the live generation, clipped to `range` when one is given. A key with no live
    /// generation is [`Error::MissingContent`]; the caller that must tell absence from
    /// emptiness asks [`ObjectContent::stat`] first.
    ///
    /// **The head and the first window are one answer.** A removal that commits
    /// between the head's read and the first window's read must read as missing
    /// content — the bytes the caller resolved are gone — rather than as a gap,
    /// which would project to a 5xx for a key that was simply deleted. The
    /// window's rows are read before this returns and handed to the stream, so
    /// the client's first bytes never depend on a second connection's state;
    /// the windows after it are fetched lazily, one bounded statement each.
    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        let (kind, ckey) = content_key(key);
        let wire = bucket.to_string();
        let Some(head) = self.live_generation(bucket, key).await? else {
            return Err(Error::MissingContent(key.clone()));
        };
        let seq = head.seq;
        let len = head.len as u64;
        let (start, end) = match range {
            // `resolve` clamps an inclusive end to the last byte and rejects a
            // start past it; a zero-byte object rejects every range. The range
            // is resolved before the empty short-circuit, so `bytes=0-0` on an
            // empty object stays an error, as it does on the block plane.
            Some(range) => range
                .resolve(len)
                .map_err(|_| Error::InvalidRange { range, size: len })?,
            // No bytes exist to stream.
            None if len == 0 => return Ok(Box::pin(stream::empty())),
            None => (0, len - 1),
        };
        let label = format!("{bucket}/{ckey}");
        let (first, last) = read_span(start, end);
        let generation = Generation {
            wire,
            kind,
            ckey,
            seq,
        };
        let mut window = ChunkWindow::new(start, end, CONTENT_CHUNK_BYTES as u64, true);
        // The read's first window, and the windows the stream fetches after it.
        let mut windows = chunk_windows(first, last, READ_WINDOW_CHUNKS as i32);
        let rest = windows.split_off(1);
        let Some(head_window) = windows.pop() else {
            return Ok(Box::pin(stream::empty()));
        };
        let done = rest.is_empty();
        let head = match fill_window(
            self.pool(),
            bucket,
            &generation,
            head_window,
            &mut window,
            done,
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(Error::Corrupt { .. }) => {
                return Err(missing_or_corrupt(self, bucket, key, seq).await?);
            }
            Err(err) => return Err(err),
        };
        Ok(stream_chunks_after(
            ChunkRead {
                pool: PgPool::clone(self.pool()),
                generation,
                window,
                windows: rest,
                label,
            },
            head,
        ))
    }

    /// Remove `key`'s bytes (idempotent). Removing an absent key deletes nothing and
    /// answers `Ok`.
    async fn remove(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<(), Error> {
        let (kind, ckey) = content_key(key);
        let wire = bucket.to_string();
        let mut tx = begin(self.pool()).await?;
        drain_key(&mut tx, &wire, kind, &ckey).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Remove every given key's bytes in one transaction, so a batch is never half applied.
    /// An empty batch removes nothing.
    async fn remove_many(&self, bucket: &bucket::Name, keys: &[ContentKey]) -> Result<(), Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let wire = bucket.to_string();
        let mut kinds = Vec::with_capacity(keys.len());
        let mut ckeys = Vec::with_capacity(keys.len());
        for key in keys {
            let (kind, ckey) = content_key(key);
            kinds.push(kind.as_str().to_owned());
            ckeys.push(ckey);
        }
        sqlx::query_file!("sql/content/drain_keys.sql", &wire, &kinds, &ckeys)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    /// Compare and delete in one transaction. The default reads on one
    /// connection and deletes on another, so a publish that lands between them
    /// loses the bytes it just stored.
    async fn remove_if_matches(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
    ) -> Result<bool, Error> {
        remove_guarded(self, bucket, key, None, Some(expected)).await
    }

    /// Delete when the stored publish identity is still `id`. A missing or
    /// different identity answers `false` and leaves the head. The body is not
    /// read: the tag was stored with the bytes.
    async fn remove_if_tagged(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        id: PublishId,
    ) -> Result<bool, Error> {
        remove_guarded(self, bucket, key, Some(id), Some(expected)).await
    }

    /// Delete when the stored publish identity is still `id`; a composed ETag
    /// never matches, so a multipart object is left in place.
    ///
    /// The recorded identity decides, and it is the narrower guard: the tag is
    /// written in the same statement that flips the head, so a stale one names
    /// its own publish even when a newer writer published byte-identical
    /// content — the blind spot a content digest leaves. The digest the contract
    /// names is the fallback a plane with no identity keeps; this plane has one,
    /// and the identity answers without reading the body at all, so no compare
    /// holds the head's row lock across the object's bytes.
    async fn remove_if_etag(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        etag: &ETag,
        id: PublishId,
    ) -> Result<bool, Error> {
        let ETag::Single(_) = etag else {
            return Ok(false);
        };
        remove_guarded(self, bucket, key, Some(id), None).await
    }

    /// Copy the source's live generation onto the destination's next sequence,
    /// then clear the source. Both head rows stay, so neither key's `next_seq`
    /// starts over. The source's rows are copied one bounded window of
    /// [`MOVE_BATCH_CHUNKS`] chunks a statement (`publish_live_copy`), all in
    /// the one transaction that pins the source head and flips the destination.
    /// A self-move is the identity (R14); a source with no live generation is
    /// [`Error::NoSuchKey`].
    async fn rename(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return Ok(());
        }
        let wire = bucket.to_string();
        let from_ckey = object_ckey(from);
        let to_ckey = object_ckey(to);
        let tx = begin(self.pool()).await?;
        // Lock the source before clearing the destination. A source that
        // disappears between the read and the copy would otherwise destroy the
        // destination and still report success.
        let (mut tx, source) = live_source(tx, &wire, from).await?;
        drain_key(&mut tx, &wire, ContentKind::Object, &to_ckey).await?;
        if let Err(err) =
            publish_live_copy(&mut tx, &wire, &from_ckey, source.seq, &to_ckey, source.len).await
        {
            tx.rollback().await?;
            return Err(err);
        }
        drain_key(&mut tx, &wire, ContentKind::Object, &from_ckey).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Duplicate an object's live generation under the destination key, in one transaction: a
    /// bounded window of [`MOVE_BATCH_CHUNKS`] chunks a statement (`publish_live_copy`), so
    /// the copy is not one `INSERT ... SELECT` over the whole source. A self-copy is the
    /// identity; a source with no live generation is [`Error::NoSuchKey`].
    async fn copy(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        if from == to {
            return Ok(());
        }
        let wire = bucket.to_string();
        let from_ckey = object_ckey(from);
        let to_ckey = object_ckey(to);
        let tx = begin(self.pool()).await?;
        let (mut tx, source) = live_source(tx, &wire, from).await?;
        drain_key(&mut tx, &wire, ContentKind::Object, &to_ckey).await?;
        if let Err(err) =
            publish_live_copy(&mut tx, &wire, &from_ckey, source.seq, &to_ckey, source.len).await
        {
            tx.rollback().await?;
            return Err(err);
        }
        tx.commit().await?;
        Ok(())
    }

    /// One page of the bucket's live objects, in ascending key order: entries at or after
    /// `after` (exclusive when given), capped at `limit`, inside `prefix`'s byte range.
    /// Only object keys are yielded (property 3), and residue is never listed (PG23).
    async fn scan(
        &self,
        bucket: &bucket::Name,
        prefix: &str,
        after: Option<&object::Key>,
        limit: usize,
    ) -> Result<Vec<ContentEntry>, Error> {
        let lower = format!("{OBJECT_CKEY_PREFIX}{prefix}");
        // The prefix's own byte range: the upper bound is its last character
        // incremented, so the range selects exactly the keys `starts_with`
        // defines (and a superset of them for a prefix with no successor, which
        // `starts_with` then narrows). `''` is the cursor's "no cursor"
        // sentinel: every stored key is longer than the empty string, so the
        // bound is then a no-op, and the predicate stays plain comparisons the
        // generic plan keeps as an `Index Cond`.
        let upper = prefix_upper(&lower);
        let cursor = after.map(object_ckey).unwrap_or_default();
        let rows = query_file_as!(
            StoredScan,
            "sql/content/scan.sql",
            bucket,
            &lower,
            &upper,
            &cursor,
            limit_to_i64(limit)
        )
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|row| {
                Ok(ContentEntry {
                    key: scan_key(bucket, &row.ckey)?,
                    len: row.live_len as u64,
                    identity: 0,
                    mtime: None,
                })
            })
            .collect()
    }

    /// The object's live length as the content plane sees it (`None` when it holds
    /// nothing) — one point read, never a rehash. The identity is `0` and the timestamp
    /// is `None`: the content relation keeps neither (D42).
    async fn stat(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ContentEntry>, Error> {
        let ckey = object_ckey(key);
        let len = query_file_scalar!("sql/content/stat.sql", bucket, ckey)
            .fetch_optional(self.pool())
            .await?;
        Ok(len.map(|len| ContentEntry {
            key: key.clone(),
            len: len as u64,
            identity: 0,
            mtime: None,
        }))
    }

    /// Record the bucket row when it is absent.
    ///
    /// pg has no content directory: the row is the namespace, and [`Self::stage`]
    /// locks it before inserting a head. The insert does nothing when the row
    /// is already there, so a caller that recorded the owner and the ACL first
    /// keeps them.
    async fn create_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        BucketMeta::record_if_absent(self, bucket, SystemTime::now()).await?;
        Ok(())
    }

    /// Remove the bucket's content namespace (idempotent), refusing with
    /// [`Error::NotEmpty`] while a live generation remains. The statement locks
    /// the bucket's existing heads and reads `live_seq` from those locked rows,
    /// so a publish that commits first is seen and refused. Residue does not
    /// hold a bucket: the same statement removes it.
    async fn remove_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let empty = query_file_scalar!("sql/content/remove_bucket.sql", bucket)
            .fetch_one(self.pool())
            .await?;
        if !empty {
            return Err(Error::NotEmpty(bucket.clone()));
        }
        Ok(())
    }

    /// Whether the bucket holds no live generation: no object bytes and no staged part
    /// bytes. Residue — a generation whose publish never flipped — is not content (PG23).
    async fn is_empty(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        Ok(query_file_scalar!("sql/content/is_empty.sql", bucket)
            .fetch_one(self.pool())
            .await?)
    }
}

#[cfg(test)]
#[path = "content_tests.rs"]
mod tests;
