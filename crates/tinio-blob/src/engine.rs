//! The engine: blob assembly, dedup and read-back policy lives here; block
//! reclamation — the live-set walk — lives in `sweep`.
//!
//! [`Store`] is generic over [`BlockStore`]: it hashes what it writes, skips
//! bytes whose address is already present, returns a [`BlobRef`] describing
//! where the content lives, and sweeps addresses no live reference points at.

use std::io;

use futures::{StreamExt, stream};
use sha2::{Digest, Sha256};
use tracing::{Span, field, instrument};

use crate::{
    _core::{
        ByteRange,
        blob::{BlobRef, BlockHash, BlockStore, StagedBlock},
        storage::BodyStream,
    },
    Error,
};

/// A blob store over a [`BlockStore`].
///
/// # Examples
///
/// ```rust
/// use bytes::Bytes;
/// use tinio_blob::{LocalBlocks, Store};
/// use tinio_core::{
///     BodyStream,
///     blob::{BlockHash, BlockStore},
/// };
/// use tokio::runtime::Runtime;
///
/// let dir = tempfile::tempdir().unwrap();
/// let store = Store::new(LocalBlocks::new(dir.path()));
/// let body: BodyStream = Box::pin(futures::stream::once(async {
///     Ok(Bytes::from_static(b"hello"))
/// }));
/// let runtime = Runtime::new().unwrap();
///
/// let blob = runtime.block_on(store.put(body)).unwrap();
///
/// assert_eq!(
///     blob.hash(),
///     &BlockHash::of(b"hello"),
///     "addressed by content"
/// );
/// assert!(
///     runtime
///         .block_on(store.blocks().contains(blob.hash()))
///         .unwrap()
/// );
/// ```
#[derive(Debug, Clone)]
pub struct Store<S> {
    blocks: S,
}

impl<S: BlockStore> Store<S> {
    /// Serve blobs over `blocks`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::{LocalBlocks, Store};
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = Store::new(LocalBlocks::new(dir.path()));
    ///
    /// // The wrapper adds policy, not paths: the store it was given comes
    /// // back unchanged.
    /// assert_eq!(store.blocks().temp_dir(), dir.path().join("tmp"));
    /// ```
    pub fn new(blocks: S) -> Self {
        Self { blocks }
    }

    /// The underlying block store.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::{LocalBlocks, Store};
    /// use tinio_core::blob::BlockHash;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = Store::new(LocalBlocks::new(dir.path()));
    ///
    /// // The wrapper adds policy, not paths: the store it was given is the
    /// // store `blocks` hands back.
    /// assert!(
    ///     store
    ///         .blocks()
    ///         .block_path(&BlockHash::of(b"hello"))
    ///         .starts_with(dir.path())
    /// );
    /// ```
    pub fn blocks(&self) -> &S {
        &self.blocks
    }

    /// Store `body` as a blob: hash it while staging, publish only when the
    /// address is new, and return the blob's reference.
    ///
    /// A stream that fails mid-way publishes nothing: the staged bytes are
    /// dropped and the failure surfaces as the caller's error.
    ///
    /// A repeat `put` still stages its bytes before it can learn the address —
    /// the hash is only known once the content has been read — so the dedup
    /// saves the publish, the disk space and the second copy, not the write.
    /// That is why this is one streaming pass, not a `contains` probe followed
    /// by a write.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_blob::{LocalBlocks, Store};
    /// use tinio_core::blob::BlockHash;
    /// use tinio_util::testing::body;
    /// use tokio::runtime::Runtime;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = Store::new(LocalBlocks::new(dir.path()));
    /// let runtime = Runtime::new().unwrap();
    ///
    /// let first = runtime.block_on(store.put(body(b"hello"))).unwrap();
    /// let second = runtime.block_on(store.put(body(b"hello"))).unwrap();
    ///
    /// assert_eq!(first, second, "the second put is a dedup hit");
    /// assert_eq!(first.hash(), &BlockHash::of(b"hello"));
    /// assert_eq!(
    ///     first.blocks().len(),
    ///     1,
    ///     "version 1 stores one block per blob"
    /// );
    /// ```
    #[instrument(
        target = "tinio::storage",
        level = "debug",
        skip_all,
        fields(bytes = field::Empty)
    )]
    pub async fn put(&self, mut body: BodyStream) -> Result<BlobRef, Error> {
        let mut staged = self
            .blocks
            .stage()
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        let mut hasher = Sha256::new();
        let mut len = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(Error::Io)?;
            hasher.update(&chunk);
            len += chunk.len() as u64;
            staged.write(chunk).await.map_err(Error::Io)?;
        }
        // The staged length is known here and not earlier: the field reports
        // it even when the publish below fails.
        Span::current().record("bytes", len);
        let hash = BlockHash::from_digest(hasher.finalize().into());
        self.blocks
            .publish(staged, &hash)
            .await
            .map_err(|err| Error::Storage(err.into()))?;
        Ok(BlobRef::single(hash, len))
    }

    /// Open `blob` — or a byte range within it — as a stream.
    ///
    /// The range is resolved against the block list by prefix-summing each
    /// block's length, and only the blocks that intersect it are opened, each
    /// with the sub-range it contributes.
    ///
    /// A range against an empty blob is an invalid range, never an empty body:
    /// [`ByteRange::resolve`] rejects any range on a zero-byte object, and this
    /// path answers that contract error so the S3 layer can map the AWS 416 for
    /// `bytes=0-0` — which is why the range is resolved before the empty
    /// short-circuit.
    ///
    /// A block that is not there arrives as an [`std::io::ErrorKind::NotFound`]
    /// I/O error — the shape [`BlockStore::open`] pins, and the one the S3
    /// layer classifies.
    ///
    /// A block that yields fewer bytes than the reference declares ends its part
    /// of the stream with an [`std::io::ErrorKind::UnexpectedEof`] error instead
    /// of a short body: the requested slice is served exactly or not at all, as
    /// [`BlockStore::open`] requires.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use bytes::Bytes;
    /// use futures::{StreamExt, stream};
    /// use tinio_blob::{LocalBlocks, Store};
    /// use tinio_core::{BodyStream, ByteRange};
    /// use tokio::runtime::Runtime;
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let dir = tempfile::tempdir().unwrap();
    /// let store = Store::new(LocalBlocks::new(dir.path()));
    /// let body: BodyStream = Box::pin(stream::once(async { Ok(Bytes::from_static(b"hello")) }));
    /// let blob = store.put(body).await?;
    ///
    /// let mut whole = store.open(&blob, None).await?;
    /// let mut out = Vec::new();
    /// while let Some(chunk) = whole.next().await {
    ///     out.extend_from_slice(&chunk.unwrap());
    /// }
    /// assert_eq!(out, b"hello");
    ///
    /// let mut ranged = store.open(&blob, Some(ByteRange::Inclusive(1, 3))).await?;
    /// let mut out = Vec::new();
    /// while let Some(chunk) = ranged.next().await {
    ///     out.extend_from_slice(&chunk.unwrap());
    /// }
    /// assert_eq!(out, b"ell", "bytes=1-3 is inclusive");
    /// # Ok(())
    /// # }
    ///
    /// // The body is an async helper so it can use `?`: it runs only when called.
    /// Runtime::new().unwrap().block_on(example()).unwrap();
    /// ```
    pub async fn open(
        &self,
        blob: &BlobRef,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        let (begin, end) = match range {
            Some(range) => range.resolve(blob.len())?,
            None if blob.is_empty() => return Ok(Box::pin(stream::empty())),
            None => (0, blob.len() - 1),
        };
        // One store `open` per intersecting block, each before the first byte
        // streams, and each wrapped so a block that yields fewer bytes than its
        // reference declares is an error at end of stream rather than a
        // silently short body — the "never a clamp" rule [`BlockStore::open`]
        // pins, enforced here once for every backend.
        let mut parts = Vec::new();
        let mut offset = 0u64;
        for block in blob.blocks() {
            if offset > end {
                break;
            }
            // Every block here is non-empty: a zero-length block is legal only
            // as the empty blob's sole block, which returned above. The prefix
            // sum is therefore exact, `stop` is at least one, and the
            // arithmetic below is total.
            if offset + block.len > begin {
                let start = begin.saturating_sub(offset);
                let stop = block.len.min(end - offset + 1);
                let part = self
                    .blocks
                    .open(&block.hash, Some(ByteRange::Inclusive(start, stop - 1)))
                    .await
                    .map_err(|err| Error::Storage(err.into()))?;
                parts.push(expect_exact(part, stop - start));
            }
            offset += block.len;
        }
        Ok(Box::pin(stream::iter(parts).flatten()))
    }
}

/// Serve exactly `expected` bytes from `body`, or end the stream with an
/// [`io::ErrorKind::UnexpectedEof`] error the moment the body ends short.
///
/// The engine asks a backend for the sub-range a block reference declares, so a
/// block that ends before that sub-range is a store that lost bytes — a
/// truncation, a bad restore. Counting here rather than in each backend makes
/// [`BlockStore::open`]'s "never a clamp" contract true for every
/// implementation, present and future, and costs one comparison per chunk on
/// the happy path. The bytes counted are the backend's, chunk for chunk: the
/// wrapper only decides when the stream ends.
fn expect_exact(body: BodyStream, expected: u64) -> BodyStream {
    Box::pin(stream::unfold(
        (body, expected),
        |(mut body, remaining)| async move {
            if remaining == 0 {
                return None;
            }
            match body.next().await {
                Some(Ok(chunk)) => {
                    let len = chunk.len() as u64;
                    if len > remaining {
                        // A backend that overshoots the sub-range it was
                        // asked for is serving bytes outside the slice it
                        // declared: the mirror of the short-read rule below,
                        // and forwarding the chunk whole would splice
                        // foreign bytes into the blob.
                        return Some((Err(io::Error::from(io::ErrorKind::InvalidData)), (body, 0)));
                    }
                    Some((Ok(chunk), (body, remaining - len)))
                }
                Some(Err(err)) => Some((Err(err), (body, 0))),
                None => Some((
                    Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                    (body, 0),
                )),
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::{fs, io, time::Duration};

    use bytes::Bytes;
    use futures::stream;
    use tinio_util::testing::body;
    use tokio::{task, time::timeout};

    use super::*;
    use crate::{
        _core::{
            blob::{BlockHash, BlockRef, BlockStore},
            storage::{self, BodyStream, collect_body},
        },
        LocalBlocks,
    };

    #[tokio::test]
    async fn putting_a_blob_stores_it_under_its_content_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));

        let blob = store.put(body(b"hello")).await.unwrap();

        assert_eq!(blob.hash(), &BlockHash::of(b"hello"));
        assert_eq!(blob.len(), 5);
        assert_eq!(
            blob.blocks().len(),
            1,
            "version 1 stores one block per blob"
        );
        assert!(store.blocks().contains(blob.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn putting_the_same_bytes_twice_stores_one_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));

        let first = store.put(body(b"hello")).await.unwrap();
        let second = store.put(body(b"hello")).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(
            store.blocks().list(None, 10).await.unwrap().len(),
            1,
            "the second put is a dedup hit, not a second block"
        );
    }

    #[tokio::test]
    async fn an_empty_blob_is_stored_and_reads_back_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));

        let blob = store.put(body(b"")).await.unwrap();

        assert!(blob.is_empty());
        assert_eq!(
            blob.blocks(),
            [BlockRef {
                hash: BlockHash::of(b""),
                len: 0
            }]
        );
        assert!(store.blocks().contains(blob.hash()).await.unwrap());
        assert!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap()
                .is_empty(),
            "the stored empty blob reads back as an empty stream"
        );
    }

    #[tokio::test]
    async fn a_body_that_fails_mid_stream_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let failing: BodyStream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(b"half")),
            Err(io::Error::other("the source died")),
        ]));

        assert!(store.put(failing).await.is_err());
        assert!(
            store.blocks().list(None, 10).await.unwrap().is_empty(),
            "nothing may become visible under a hash from a failed stream"
        );
        // The dropped stage's unlink runs off the async worker (the shared
        // staged-temp discipline), so the directory is not necessarily empty
        // the instant `put` returns: wait for the unlink rather than racing
        // it. The end state asserted is the same.
        let temp = store.blocks().temp_dir();
        timeout(Duration::from_secs(5), async {
            loop {
                if fs::read_dir(&temp).unwrap().next().is_none() {
                    return;
                }
                task::yield_now().await;
            }
        })
        .await
        .expect("the dropped stage removed its own temp; only a process that died mid-stage leaves residue");
    }

    #[tokio::test]
    async fn opening_a_blob_serves_its_bytes_and_a_range_within_them() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"hello")).await.unwrap();

        assert_eq!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap(),
            b"hello"
        );
        let range = Some(ByteRange::Inclusive(1, 3));
        assert_eq!(
            collect_body(store.open(&blob, range).await.unwrap())
                .await
                .unwrap(),
            b"ell"
        );
    }

    #[tokio::test]
    async fn opening_an_empty_blob_serves_an_empty_stream() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"")).await.unwrap();

        assert!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn opening_a_blob_whose_block_is_gone_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"hello")).await.unwrap();

        store.blocks().remove(blob.hash()).await.unwrap();

        // Matched, not `unwrap_err`: `BodyStream` is not `Debug`, so the `Ok`
        // side cannot be printed.
        let Err(err) = store.open(&blob, None).await else {
            panic!("a blob whose block is gone must not open");
        };
        let Error::Storage(storage::Error::Io(io)) = err else {
            panic!("expected a contract I/O error, got {err:?}");
        };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn a_block_shorter_than_the_reference_declares_is_an_error_not_a_short_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"hello")).await.unwrap();

        // The block loses bytes before the open: the backend sizes the range
        // against the file it finds, so the requested five bytes are clamped
        // away — the engine's own byte count is what refuses to serve two as
        // the whole block.
        fs::OpenOptions::new()
            .write(true)
            .open(store.blocks().block_path(blob.hash()))
            .unwrap()
            .set_len(2)
            .unwrap();

        let mut opened = store.open(&blob, None).await.unwrap();
        let mut served = Vec::new();
        let err = loop {
            match opened.next().await {
                Some(Ok(chunk)) => served.extend_from_slice(&chunk),
                Some(Err(err)) => break err,
                None => panic!("a short block must not end as a normal body"),
            }
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            served, b"he",
            "the bytes still on disk are served before the error"
        );
    }

    #[tokio::test]
    async fn a_short_second_block_is_an_error_for_that_slice_not_a_short_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = two_block_blob(&store).await;

        assert_eq!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap(),
            b"hello",
            "the same reference reads whole while both blocks are intact"
        );

        // The *second* block loses a byte before this open, while the first is
        // whole: the count is per slice, so the shortfall is caught after a
        // slice that served in full — not only when a single-block blob ends
        // short.
        fs::OpenOptions::new()
            .write(true)
            .open(store.blocks().block_path(&blob.blocks()[1].hash))
            .unwrap()
            .set_len(1)
            .unwrap();

        let mut opened = store.open(&blob, None).await.unwrap();
        let mut served = Vec::new();
        let err = loop {
            match opened.next().await {
                Some(Ok(chunk)) => served.extend_from_slice(&chunk),
                Some(Err(err)) => break err,
                None => panic!("a short block must not end as a normal body"),
            }
        };
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(
            served, b"hell",
            "the whole first block and the surviving byte of the second are served before the error"
        );
    }

    #[test]
    fn a_zero_length_block_that_is_not_the_only_block_is_rejected() {
        // The read loop prefix-sums each block's length, so it assumes every
        // block it visits contributes bytes; the invariant that makes that
        // assumption safe is enforced when the reference is built, not by a
        // guard on every read.
        assert!(
            BlobRef::new(
                BlockHash::of(b"abcde"),
                5,
                vec![
                    BlockRef {
                        hash: BlockHash::of(b"ab"),
                        len: 2,
                    },
                    BlockRef {
                        hash: BlockHash::of(b""),
                        len: 0,
                    },
                    BlockRef {
                        hash: BlockHash::of(b"cde"),
                        len: 3,
                    },
                ],
            )
            .is_err(),
            "a zero-length block is legal only as a blob's sole block"
        );
    }

    #[tokio::test]
    async fn a_range_on_an_empty_blob_is_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"")).await.unwrap();

        let Err(err) = store.open(&blob, Some(ByteRange::Inclusive(0, 0))).await else {
            panic!("a range on a zero-byte object is InvalidRange, never an empty body");
        };
        let Error::Storage(storage::Error::InvalidRange { range, size }) = err else {
            panic!("expected a contract invalid-range error, got {err:?}");
        };
        assert_eq!(range, ByteRange::Inclusive(0, 0));
        assert_eq!(size, 0, "the range is judged against the blob's own length");

        assert!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap()
                .is_empty(),
            "the no-range read of an empty blob stays an empty body"
        );
    }

    #[tokio::test]
    async fn a_range_spanning_two_blocks_is_stitched_from_both() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let first = store.put(body(b"hel")).await.unwrap();
        let second = store.put(body(b"lo")).await.unwrap();
        let blob = BlobRef::new(
            BlockHash::of(b"hello"),
            5,
            vec![
                BlockRef {
                    hash: *first.hash(),
                    len: 3,
                },
                BlockRef {
                    hash: *second.hash(),
                    len: 2,
                },
            ],
        )
        .unwrap();

        assert_eq!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap(),
            b"hello"
        );
        let range = Some(ByteRange::Inclusive(1, 3));
        assert_eq!(
            collect_body(store.open(&blob, range).await.unwrap())
                .await
                .unwrap(),
            b"ell",
            "the range crosses the block boundary, so both blocks contribute"
        );
    }

    #[tokio::test]
    async fn a_body_that_arrives_in_several_chunks_is_one_hash_and_one_length() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        // `body()` produces a single chunk, so this is the only place the
        // staging loop sees more than one: a hasher reset per chunk, or a
        // length taken from the last chunk, would pass every other test.
        let chunked: BodyStream = Box::pin(stream::iter(vec![
            Ok(Bytes::from_static(b"he")),
            Ok(Bytes::from_static(b"llo")),
        ]));

        let blob = store.put(chunked).await.unwrap();

        assert_eq!(
            blob.hash(),
            &BlockHash::of(b"hello"),
            "the hash covers every chunk, not one of them"
        );
        assert_eq!(blob.len(), 5, "the length counts every chunk, not the last");
        assert_eq!(
            collect_body(store.open(&blob, None).await.unwrap())
                .await
                .unwrap(),
            b"hello",
            "the staged chunks read back as one block"
        );
    }

    /// The five-byte blob stored as two blocks, `hel` and `lo`, addressed by
    /// the whole content: a range is resolved against the blob's five bytes
    /// while the blocks it is served from are three and two bytes long. Every
    /// block-boundary test below reads this one shape.
    async fn two_block_blob(store: &Store<LocalBlocks>) -> BlobRef {
        let first = store.put(body(b"hel")).await.unwrap();
        let second = store.put(body(b"lo")).await.unwrap();
        BlobRef::new(
            BlockHash::of(b"hello"),
            5,
            vec![
                BlockRef {
                    hash: *first.hash(),
                    len: 3,
                },
                BlockRef {
                    hash: *second.hash(),
                    len: 2,
                },
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_range_starting_exactly_at_a_block_boundary_skips_the_block_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = two_block_blob(&store).await;

        let range = Some(ByteRange::Inclusive(3, 4));
        assert_eq!(
            collect_body(store.open(&blob, range).await.unwrap())
                .await
                .unwrap(),
            b"lo",
            "byte 3 is the second block's first byte, not the first block's one-past-the-end"
        );
    }

    #[tokio::test]
    async fn a_range_ending_exactly_at_a_block_boundary_never_opens_the_blocks_beyond_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = two_block_blob(&store).await;
        // The second block is gone from the store: serving `hel` is only
        // possible if the read stops at the boundary and never opens the
        // missing block — a break one block too wide would open it and fail.
        store.blocks().remove(&blob.blocks()[1].hash).await.unwrap();

        let range = Some(ByteRange::Inclusive(0, 2));
        assert_eq!(
            collect_body(store.open(&blob, range).await.unwrap())
                .await
                .unwrap(),
            b"hel",
            "the range ends at the first block's last byte, so only that block is opened"
        );
    }

    #[tokio::test]
    async fn a_range_past_the_end_is_judged_against_the_blob_not_the_last_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = two_block_blob(&store).await;

        // Matched, not `unwrap_err`: `BodyStream` is not `Debug`, so the `Ok`
        // side cannot be printed. A size resolved against the last block (two
        // bytes) instead of the blob (five) would report `2` here.
        let Err(err) = store.open(&blob, Some(ByteRange::From(5))).await else {
            panic!("a range that starts at the blob's own length is InvalidRange");
        };
        let Error::Storage(storage::Error::InvalidRange { range, size }) = err else {
            panic!("expected a contract invalid-range error, got {err:?}");
        };
        assert_eq!(range, ByteRange::From(5));
        assert_eq!(
            size, 5,
            "the range is judged against the blob's five bytes, not the last block's two"
        );

        assert_eq!(
            collect_body(store.open(&blob, Some(ByteRange::Suffix(2))).await.unwrap())
                .await
                .unwrap(),
            b"lo",
            "bytes=-2 is the blob's last two bytes"
        );
        assert_eq!(
            collect_body(store.open(&blob, Some(ByteRange::Suffix(1))).await.unwrap())
                .await
                .unwrap(),
            b"o",
            "a suffix beginning inside the second block is clipped to its last byte"
        );
    }

    #[tokio::test]
    async fn a_block_yielding_more_than_its_slice_declares_is_an_error() {
        let body: BodyStream =
            Box::pin(stream::once(async { Ok(Bytes::from_static(b"too much")) }));
        let mut wrapped = expect_exact(body, 3);
        let first = wrapped.next().await.unwrap();
        assert!(
            matches!(first, Err(err) if err.kind() == io::ErrorKind::InvalidData),
            "an overshooting backend must not splice extra bytes into the blob"
        );
        assert!(wrapped.next().await.is_none());
    }
}
