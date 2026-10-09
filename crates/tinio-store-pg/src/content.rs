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
//! contiguous run of `chunk_no` values from 0, so a missing chunk surfaces as
//! [`Error::Corrupt`] rather than an index panic.
//!
//! The plane keeps no file identity and no timestamp: [`ObjectContent::scan`] and
//! [`ObjectContent::stat`] answer `identity = 0` and `mtime = None` (D42 — a
//! store with no timestamp must not fabricate one; an object's timestamp belongs to the
//! metadata row, a separate contract). It does record a publish identity:
//! `content_heads.publish_id` stores the caller's
//! [`PublishId`] as a bigint bit pattern.
//! [`ObjectContent::publish_tagged`] writes it in the same statement that flips
//! `live_seq`. [`ObjectContent::remove_if_tagged`] deletes only while that
//! identity and the bytes still match, and [`ObjectContent::remove_if_matches`]
//! compares and deletes in that same transaction. An untagged publish stores
//! NULL, so a tagged removal does not take it.
//!
//! A publish's statement boundary is bounded: the stage flushes chunks in batches of
//! [`PUBLISH_BATCH_BYTES`] (four 1 MiB chunks) as they stream in, so a body far larger
//! than memory can be published without ever being held whole (PG22).

use std::mem;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use sqlx::{PgPool, query_file_as, query_file_scalar};
use tinio_core::{
    bucket,
    content::{ContentEntry, ContentKey, ObjectContent, PublishId, StagedObject},
    object,
    storage::{BodyStream, ByteRange},
};

use crate::{Error, PgStore, PgTx, begin, window::clip_chunk};

/// The `content_kind` PostgreSQL enum: the content plane's own discriminator,
/// which is part of every content primary key. No contract type names this set
/// (the contract's `ContentKey` distinguishes object from part by shape, not
/// by a stored tag), so it stays beside its only plane rather than on a
/// contract type -- the store carries no parallel *contract* type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "content_kind", rename_all = "lowercase")]
enum ContentKind {
    /// An object's chunks.
    Object,
    /// A multipart part's chunks.
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
    pub chunks: usize,
    /// The number of batch statements the chunks are written with.
    pub batches: usize,
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
    let chunks = (len as usize).div_ceil(CONTENT_CHUNK_BYTES);
    PublishPlan {
        chunks,
        batches: chunks.div_ceil(PUBLISH_BATCH_CHUNKS),
    }
}

/// The live head's sequence and length, which every read resolves the object's
/// bytes against.
struct StoredLiveHead {
    live_seq: i64,
    live_len: i64,
}

/// A live head locked `FOR UPDATE`: the generation a rename, copy, or guarded
/// removal is allowed to act on, plus the publish identity when one was stored.
struct LockedHead {
    live_seq: i64,
    live_len: i64,
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

/// The content relation's collision-proof key namespace (PG20): an object key and a part key
/// can never share a row, because the kind discriminator is part of the relation's primary
/// key and the encoded key is prefix-disjoint.
fn object_ckey(key: &object::Key) -> String {
    format!("o:{key}")
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

/// Delete one key's head and every generation's chunks it still holds. The caller runs it in
/// the transaction that makes the removal atomic.
async fn drain_key(
    tx: &mut PgTx,
    bucket: &str,
    kind: ContentKind,
    ckey: &str,
) -> Result<(), Error> {
    sqlx::query_file!("sql/content/drain_content.sql", bucket, kind as _, ckey)
        .execute(&mut **tx)
        .await?;
    sqlx::query_file!("sql/content/drain_head.sql", bucket, kind as _, ckey)
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
) -> Result<Option<LockedHead>, Error> {
    Ok(query_file_as!(
        LockedHead,
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
) -> Result<(PgTx, LockedHead), Error> {
    let from_ckey = object_ckey(from);
    let Some(source) = lock_head(&mut tx, wire, ContentKind::Object, &from_ckey).await? else {
        tx.rollback().await?;
        return Err(Error::NoSuchKey(from.clone()));
    };
    Ok((tx, source))
}

/// The live generation's bytes, read on `tx` so a guarded removal compares
/// what it is about to delete.
async fn read_generation(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    kind: ContentKind,
    ckey: &str,
    seq: i64,
    len: u64,
) -> Result<Vec<u8>, Error> {
    if len == 0 {
        return Ok(Vec::new());
    }
    let end = len - 1;
    let first = 0i32;
    let last = (end / CONTENT_CHUNK_BYTES as u64) as i32;
    let rows = query_file_as!(
        StoredChunk,
        "sql/content/open_chunks.sql",
        bucket.as_ref(),
        kind as _,
        ckey,
        seq,
        first,
        last
    )
    .fetch_all(&mut **tx)
    .await?;
    let mut expected = first;
    let mut served = 0u64;
    let mut out = Vec::new();
    for row in rows {
        if row.chunk_no != expected {
            return Err(corrupt_content(bucket, ckey));
        }
        expected += 1;
        let Some((range, n)) = clip_chunk(
            row.chunk.len(),
            row.chunk_no as u64,
            CONTENT_CHUNK_BYTES as u64,
            0,
            end,
        ) else {
            continue;
        };
        out.extend_from_slice(&row.chunk[range]);
        served += n;
    }
    if expected != last + 1 || served != len {
        return Err(corrupt_content(bucket, ckey));
    }
    Ok(out)
}

/// Flip the staged generation live and record `publish_id` (`None` clears a
/// previous tag). Zero rows means a later allocation already owns the head.
async fn flip_head(staged: &PgStaged, publish_id: Option<i64>) -> Result<(), Error> {
    let published = sqlx::query_file!(
        "sql/content/publish_head.sql",
        &staged.bucket,
        staged.kind as _,
        staged.ckey,
        staged.seq,
        staged.landed as i64,
        publish_id
    )
    .execute(&staged.pool)
    .await?;
    if published.rows_affected() == 0 {
        return Err(Error::PublishRaced);
    }
    Ok(())
}

/// The compare and the delete share one commit. `id` set means the stored
/// publish identity must match too.
async fn remove_guarded(
    store: &PgStore,
    bucket: &bucket::Name,
    key: &ContentKey,
    expected: &[u8],
    id: Option<PublishId>,
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
    }
    let stored = read_generation(
        &mut tx,
        bucket,
        kind,
        &ckey,
        head.live_seq,
        head.live_len as u64,
    )
    .await?;
    if stored.as_slice() != expected {
        tx.rollback().await?;
        return Ok(false);
    }
    drain_key(&mut tx, &wire, kind, &ckey).await?;
    tx.commit().await?;
    Ok(true)
}

#[async_trait]
impl ObjectContent for PgStore {
    type Error = Error;
    type Staged = PgStaged;

    /// Allocate a fresh generation under the target key (D38). The head row's `live_seq`
    /// stays `NULL` — the incompleteness marker — so nothing written into the generation is
    /// visible until publish.
    async fn stage(&self, bucket: &bucket::Name, key: &ContentKey) -> Result<PgStaged, Error> {
        let (kind, ckey) = content_key(key);
        let wire = bucket.to_string();
        let seq = query_file_scalar!("sql/content/alloc_head.sql", &wire, kind as _, ckey)
            .fetch_one(self.pool())
            .await?;
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

    /// Flush the staged tail and flip the head in one guarded statement. The flip is the
    /// visibility point; a stage another allocation superseded answers
    /// [`Error::PublishRaced`] and its rows stay residue.
    async fn publish(
        &self,
        _bucket: &bucket::Name,
        mut staged: PgStaged,
        _key: &ContentKey,
    ) -> Result<(), Error> {
        staged.finish().await?;
        flip_head(&staged, None).await
    }

    /// Publish and store `id` in the same statement that flips the head, so a
    /// later tagged removal can tell this publish from a byte-identical one.
    async fn publish_tagged(
        &self,
        _bucket: &bucket::Name,
        mut staged: PgStaged,
        _key: &ContentKey,
        id: PublishId,
    ) -> Result<(), Error> {
        staged.finish().await?;
        flip_head(&staged, Some(u64::from(id) as i64)).await
    }

    /// Open the live generation, clipped to `range` when one is given. A key with no live
    /// generation is [`Error::MissingContent`]; the caller that must tell absence from
    /// emptiness asks [`ObjectContent::stat`] first.
    async fn open(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        let (kind, ckey) = content_key(key);
        let wire = bucket.to_string();
        let head = query_file_as!(
            StoredLiveHead,
            "sql/content/live_head.sql",
            &wire,
            kind as _,
            ckey
        )
        .fetch_optional(self.pool())
        .await?;
        let Some(head) = head else {
            return Err(Error::MissingContent(key.clone()));
        };
        let seq = head.live_seq;
        let len = head.live_len as u64;
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
        let first = (start / CONTENT_CHUNK_BYTES as u64) as i32;
        let last = (end / CONTENT_CHUNK_BYTES as u64) as i32;
        let rows = query_file_as!(
            StoredChunk,
            "sql/content/open_chunks.sql",
            &wire,
            kind as _,
            ckey,
            seq,
            first,
            last
        )
        .fetch_all(self.pool())
        .await?;
        // First and last chunks are sliced to the window; every chunk between them is
        // yielded whole. The bytes are not concatenated into one buffer.
        let mut expected = first;
        let mut served = 0u64;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if row.chunk_no != expected {
                return Err(corrupt_content(bucket, &ckey));
            }
            expected += 1;
            let Some((range, n)) = clip_chunk(
                row.chunk.len(),
                row.chunk_no as u64,
                CONTENT_CHUNK_BYTES as u64,
                start,
                end,
            ) else {
                continue;
            };
            served += n;
            out.push(Bytes::from(row.chunk).slice(range));
        }
        if expected != last + 1 || served != end - start + 1 {
            return Err(corrupt_content(bucket, &ckey));
        }
        Ok(Box::pin(stream::iter(out.into_iter().map(Ok))))
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
        remove_guarded(self, bucket, key, expected, None).await
    }

    /// Compare, match the stored publish identity, and delete in one transaction.
    /// A missing or different identity answers `false` even when the bytes are
    /// equal.
    async fn remove_if_tagged(
        &self,
        bucket: &bucket::Name,
        key: &ContentKey,
        expected: &[u8],
        id: PublishId,
    ) -> Result<bool, Error> {
        remove_guarded(self, bucket, key, expected, Some(id)).await
    }

    /// Move an object's bytes and its head together, in one transaction. A self-move is the
    /// identity (R14); a source with no live generation is [`Error::NoSuchKey`].
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
        // Lock the source before deleting the destination. A source that
        // disappears between the read and the key write would otherwise destroy
        // the destination and still report success.
        let (mut tx, _) = live_source(tx, &wire, from).await?;
        drain_key(&mut tx, &wire, ContentKind::Object, &to_ckey).await?;
        sqlx::query_file!("sql/content/move_content.sql", &wire, from_ckey, to_ckey)
            .execute(&mut *tx)
            .await?;
        let moved = sqlx::query_file!("sql/content/move_head.sql", &wire, from_ckey, to_ckey)
            .execute(&mut *tx)
            .await?;
        if moved.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(Error::NoSuchKey(from.clone()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// Duplicate an object's live generation under the destination key, in one transaction.
    /// A self-copy is the identity; a source with no live generation is
    /// [`Error::NoSuchKey`].
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
        let source_seq = source.live_seq;
        let len = source.live_len;
        drain_key(&mut tx, &wire, ContentKind::Object, &to_ckey).await?;
        let seq = query_file_scalar!(
            "sql/content/alloc_head.sql",
            &wire,
            ContentKind::Object as _,
            to_ckey
        )
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query_file!(
            "sql/content/copy_content.sql",
            to_ckey,
            seq,
            &wire,
            ContentKind::Object as _,
            from_ckey,
            source_seq
        )
        .execute(&mut *tx)
        .await?;
        let published = sqlx::query_file!(
            "sql/content/publish_head.sql",
            &wire,
            ContentKind::Object as _,
            to_ckey,
            seq,
            len,
            None::<i64>
        )
        .execute(&mut *tx)
        .await?;
        if published.rows_affected() != 1 {
            tx.rollback().await?;
            return Err(Error::PublishRaced);
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
        let lower = format!("o:{prefix}");
        let upper = after.map(|key| format!("o:{key}"));
        let rows = query_file_as!(
            StoredScan,
            "sql/content/scan.sql",
            bucket,
            &lower,
            upper.as_deref(),
            limit as i64
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

    /// A content namespace with no directory: nothing to create. The bucket row is the
    /// metadata plane's.
    async fn create_bucket(&self, _bucket: &bucket::Name) -> Result<(), Error> {
        Ok(())
    }

    /// Remove the bucket's content namespace (idempotent), refusing with
    /// [`Error::NotEmpty`] while a live generation remains. Residue does not hold a
    /// bucket: the same statement removes it.
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
