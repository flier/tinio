//! The tier write stage: a body either streams into the filesystem's temp
//! file or waits in a bounded memory buffer.
//!
//! The stage is the streaming half of a write (T7). A sync or non-batch
//! body goes straight to [`FsStorage::stage_body`], whose temp file receives
//! the chunks as they arrive — the body is never collected. A batch-eligible
//! body (an unversioned bucket in [`FlushMode::Batch`](crate::FlushMode))
//! is read into a buffer bounded by `max_object_bytes`; a chunk that would
//! push the body past the cap spills the bytes already buffered, the chunk
//! and the rest of the stream into the filesystem stage instead. The
//! checksum tee rides both shapes: the server's wrapper fills it while the
//! source body streams, and a spill forwards the same tee to the filesystem
//! stage.
//!
//! Nothing here publishes: the [`TierStaged`] value is the commit's input,
//! and dropping one uncommitted leaves no temp, no row and no buffer entry
//! (the filesystem stage's own drop removes its temp; a memory stage holds
//! only process memory).

use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, stream};

use crate::{
    _core::{
        ETag, bucket, checksum, object,
        storage::{BodyStream, ObjectOps},
    },
    _fs::FsStorage,
    Error,
};

/// A staged write over the tier layer's write path.
///
/// [`ObjectOps::stage_body`](tinio_core::storage::ObjectOps::stage_body)
/// answers one of two shapes: the filesystem plane's own staged body (a temp
/// file under `<state-dir>/tmp/`) or the bounded memory buffer a
/// batch-eligible body produced. A commit consumes either shape; until then
/// the bytes are invisible to reads.
///
/// # Examples
///
/// A memory stage carries the bytes and what the stream fixed:
///
/// ```rust
/// use tinio_core::ETag;
/// use tinio_tier::{MemInfo, TierStaged};
///
/// let staged = TierStaged::Mem {
///     bytes: bytes::Bytes::from_static(b"hi"),
///     info: MemInfo {
///         etag: ETag::from_content(b"hi"),
///         checksum: None,
///     },
/// };
/// assert!(matches!(staged, TierStaged::Mem { .. }));
/// if let TierStaged::Mem { bytes, info } = &staged {
///     assert_eq!(bytes.len(), 2);
///     assert_eq!(info.etag, ETag::from_content(b"hi"));
///     assert_eq!(info.checksum, None);
/// }
/// ```
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "argued: the filesystem stage is the delegated body; boxing it would add an allocation to every write for a value consumed once"
)]
pub enum TierStaged {
    /// A batch-eligible body buffered in memory, at most `max_object_bytes`.
    Mem {
        /// The buffered bytes.
        bytes: Bytes,
        /// The metadata the stream fixed for the commit.
        info: MemInfo,
    },
    /// A body streamed into the filesystem's own staged write.
    Fs(<FsStorage as ObjectOps>::StagedBody),
}

/// What a memory-buffered stage fixed while its body streamed.
///
/// The commit publishes the bytes through the filesystem and records these
/// values; the batch buffer (a later task) reuses the same pair when it
/// defers the filesystem write.
///
/// # Examples
///
/// ```rust
/// use tinio_core::ETag;
/// use tinio_tier::MemInfo;
///
/// let info = MemInfo {
///     etag: ETag::from_content(b"hello"),
///     checksum: None,
/// };
/// assert_eq!(info.etag.as_str(), "5d41402abc4b2a76b9719d911017c592");
/// ```
#[derive(Debug)]
pub struct MemInfo {
    /// The content MD5 of the buffered bytes — the ETag every commit shape
    /// agrees on.
    pub etag: ETag,
    /// The stage's recorded checksum: the caller's carried value when one
    /// was given, else the tee's streamed digest as the write path's
    /// FULL_OBJECT kind.
    pub checksum: Option<checksum::Recorded>,
}

/// Stage `body` into a buffer bounded by `max_object_bytes`, spilling to the
/// filesystem stage once a chunk would push the body past the cap (T7).
///
/// The checksum tee is forwarded unchanged on the spill, so its MD5 cell is
/// filled from the filesystem stage's hash of the replayed body and its
/// digest was already filled by the server's wrapper while the source stream
/// was read. A body that ends at or below the cap answers a
/// [`TierStaged::Mem`]; the buffer holds at most the cap plus the one chunk
/// that triggered the spill.
///
/// # Errors
///
/// The body stream's own I/O failure, and the filesystem stage's validation
/// or I/O failure on a spill.
pub(crate) async fn stage_bounded(
    fs: &FsStorage,
    max_object_bytes: u64,
    bucket: &bucket::Name,
    key: &object::Key,
    mut body: BodyStream,
    tee: Option<Arc<checksum::PartChecksum>>,
    carried: Option<checksum::Recorded>,
) -> Result<TierStaged, Error> {
    let mut buffered = Vec::new();
    let mut total = 0_u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        if total + chunk.len() as u64 > max_object_bytes {
            buffered.extend_from_slice(&chunk);
            // The buffered prefix does not feed the tee a second time: it
            // already did while it was read, and the filesystem stage only
            // hashes the replay for the ETag.
            let replay: BodyStream =
                Box::pin(stream::iter([Ok(Bytes::from(buffered))]).chain(body));
            return Ok(TierStaged::Fs(
                fs.stage_body(bucket, key, replay, tee, carried).await?,
            ));
        }
        total += chunk.len() as u64;
        buffered.extend_from_slice(&chunk);
    }
    let bytes = Bytes::from(buffered);
    let etag = ETag::from_content(&bytes);
    // The tee's promise, kept: a consumer that asked for MD5 finds the
    // digest the stream computed (the same rule the filesystem engine's
    // stage applies). A full cell holds another writer's digest for the same
    // bytes — a lost race at worst, never a wrong one.
    if let (ETag::Single(digest), Some(cell)) =
        (&etag, tee.as_ref().and_then(|tee| tee.etag.as_ref()))
    {
        let _ = cell.set(*digest);
    }
    let streamed = tee.as_ref().and_then(|tee| tee.digest.get()).cloned();
    let checksum = carried.or(streamed.map(|part| checksum::Recorded {
        part,
        kind: checksum::Type::FullObject,
    }));
    Ok(TierStaged::Mem {
        bytes,
        info: MemInfo { etag, checksum },
    })
}
