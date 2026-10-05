//! The block-store contract: immutable, content-addressed block storage.
//!
//! A block's address is the SHA-256 of its bytes. The store writes a block once
//! and never mutates it, so a repeated write of the same content is a no-op and
//! an interrupted write can never be served: the bytes become visible only when
//! they are published under their own hash.
//!
//! This module is the contract only — how content is split into blocks, how a
//! blob's block list is built and when unreferenced blocks are reclaimed is the
//! engine's business (`tinio-blob`), so a second backend inherits that behaviour
//! by implementing [`BlockStore`] over its own transport.
//!
//! **A recorded deviation from `docs/style.md`'s derive rule.** That rule sends
//! a `Display`/`FromStr` pair to `parse-display`, and [`BlockHash`] keeps
//! `derive_more::Display` with a hand-written `FromStr` instead: `parse_display`
//! hands `#[from_str(new = …)]` the already-parsed *field*, and this type's
//! payload is `[u8; 32]`, which has no `FromStr` — every `parse-display` use in
//! the workspace is on a fieldless enum. [`crate::etag::ETag`]
//! (`crates/tinio-core/src/etag.rs:39-40,62`) is the same precedent: a
//! byte-array payload, `derive_more::Display`, and a `FromStr` written by hand.

use std::{
    error::Error as StdError, io, pin::Pin, slice::from_ref, str::FromStr, time::SystemTime,
};

use bytes::Bytes;
use derive_more::{AsRef, Deref, Display};
use futures::Stream;
use sha2::{Digest, Sha256};

use crate::{
    ByteRange,
    storage::{self, BodyStream},
};

/// A block's address: the SHA-256 of its content, rendered as lowercase hex.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::BlockHash;
///
/// let hash = BlockHash::of(b"hello");
/// assert_eq!(hash.to_hex().len(), 64);
/// assert_eq!(BlockHash::new(&hash.to_hex()).unwrap(), hash);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, AsRef, Deref, Display)]
#[as_ref(forward)]
#[display("{}", self.to_hex())]
pub struct BlockHash(#[deref] [u8; 32]);

impl BlockHash {
    /// The address of `bytes`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::BlockHash;
    ///
    /// assert_eq!(BlockHash::of(b"hello"), BlockHash::of(b"hello"));
    /// assert_ne!(
    ///     BlockHash::of(b"hello"),
    ///     BlockHash::of(b"world"),
    ///     "the address is a function of the content"
    /// );
    /// ```
    pub fn of(bytes: &[u8]) -> Self {
        Self::from_digest(Sha256::digest(bytes).into())
    }

    /// Wrap an already-computed SHA-256 digest.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::BlockHash;
    ///
    /// let hash = BlockHash::of(b"hello");
    /// assert_eq!(BlockHash::from_digest(*hash.as_bytes()), hash);
    /// ```
    pub fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// The raw digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The lowercase hex form (the on-disk name and the wire form).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::BlockHash;
    ///
    /// let hex = BlockHash::of(b"hello").to_hex();
    ///
    /// assert_eq!(hex.len(), 64);
    /// assert_eq!(BlockHash::new(&hex).unwrap().to_hex(), hex);
    /// ```
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse the hex form — the constructor for untrusted input.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::BlockHash;
    ///
    /// let hash = BlockHash::of(b"hello");
    /// assert_eq!(BlockHash::new(&hash.to_hex()).unwrap(), hash);
    /// assert!(BlockHash::new("not hex").is_err());
    /// ```
    pub fn new(hash: &str) -> Result<Self, ParseError> {
        // Decoded straight into the fixed array: `decode_to_slice` rejects a
        // wrong length and a non-hex character alike, so both failure modes
        // collapse into this constructor's one error, and the digest is never
        // copied out of an intermediate `Vec`.
        let mut digest = [0u8; 32];
        hex::decode_to_slice(hash, &mut digest).map_err(|_| ParseError(hash.to_owned()))?;
        Ok(Self(digest))
    }
}

impl FromStr for BlockHash {
    type Err = ParseError;

    fn from_str(hash: &str) -> Result<Self, Self::Err> {
        Self::new(hash)
    }
}

impl From<&str> for BlockHash {
    /// Trusted-input convenience (panics on a malformed hash — use
    /// [`BlockHash::new`] for untrusted input).
    fn from(hash: &str) -> Self {
        Self::new(hash).expect("valid block hash")
    }
}

/// A malformed block address or block list.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::{BlockHash, ParseError};
///
/// let err: ParseError = BlockHash::new("not hex").unwrap_err();
/// assert_eq!(err.to_string(), "invalid block reference: not hex");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid block reference: {0}")]
pub struct ParseError(String);

/// One block of a blob: its address and the number of bytes it contributes.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::{BlockHash, BlockRef};
///
/// let hash = BlockHash::of(b"hello");
/// let block = BlockRef { hash, len: 5 };
/// assert_eq!(block.len, 5);
/// assert_eq!(block.hash, hash);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    /// The block's address.
    pub hash: BlockHash,
    /// The block's length in bytes.
    pub len: u64,
}

/// A stored blob: its content hash, its total length, and its ordered blocks.
///
/// Version 1 stores a blob as exactly one block, whose address is the whole
/// content's hash; the list exists so splitting a blob into chunks later is an
/// engine policy change rather than a format migration.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::{BlobRef, BlockHash};
///
/// let hash = BlockHash::of(b"hello");
/// let blob = BlobRef::single(hash, 5);
/// assert_eq!(blob.len(), 5);
/// assert!(!blob.is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobRef {
    hash: BlockHash,
    len: u64,
    blocks: Blocks,
}

/// A blob's block list, with the version-1 single-block shape held inline: the
/// write path builds one of these per blob, and a one-element `Box<[BlockRef]>`
/// would be an allocation for every blob stored.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Blocks {
    /// The blob's only block.
    One(BlockRef),
    /// Two or more blocks, in content order.
    Many(Box<[BlockRef]>),
}

impl BlobRef {
    /// The version-1 shape: one block whose address is the whole content's hash.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::{BlobRef, BlockHash, BlockRef};
    ///
    /// let hash = BlockHash::of(b"hello");
    /// let blob = BlobRef::single(hash, 5);
    ///
    /// assert_eq!(blob.len(), 5);
    /// assert_eq!(blob.blocks(), [BlockRef { hash, len: 5 }]);
    /// ```
    pub fn single(hash: BlockHash, len: u64) -> Self {
        Self {
            hash,
            len,
            blocks: Blocks::One(BlockRef { hash, len }),
        }
    }

    /// The empty blob: one zero-length block whose address is the empty
    /// content's hash. The shape every delete marker's row carries, so the row
    /// needs no absent value and a decoded blob is always total; one home for
    /// it keeps the version engine and both backends from each spelling it.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::{BlobRef, BlockHash};
    ///
    /// let empty = BlobRef::empty();
    /// assert!(empty.is_empty());
    /// assert_eq!(empty.len(), 0);
    /// assert_eq!(empty, BlobRef::single(BlockHash::of(b""), 0));
    /// ```
    pub fn empty() -> Self {
        Self::single(BlockHash::of(b""), 0)
    }

    /// Assemble a reference from its parts, checking the invariants: at least
    /// one block, the declared length equal to the blocks' sum, and a
    /// zero-length block only as the blob's sole block.
    ///
    /// The sum is checked, never wrapped: a block list whose lengths overflow
    /// `u64` is rejected rather than accepted with a length the blocks cannot
    /// carry.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use tinio_core::blob::{BlobRef, BlockHash, BlockRef};
    ///
    /// let blocks = vec![
    ///     BlockRef {
    ///         hash: BlockHash::of(b"hel"),
    ///         len: 3,
    ///     },
    ///     BlockRef {
    ///         hash: BlockHash::of(b"lo"),
    ///         len: 2,
    ///     },
    /// ];
    /// let hash = BlockHash::of(b"hello");
    ///
    /// let blob = BlobRef::new(hash, 5, blocks.clone()).unwrap();
    /// assert_eq!(blob.blocks().len(), 2);
    ///
    /// assert!(
    ///     BlobRef::new(hash, 4, blocks).is_err(),
    ///     "the declared length must equal the blocks' sum"
    /// );
    /// ```
    pub fn new(
        hash: BlockHash,
        len: u64,
        blocks: impl Into<Box<[BlockRef]>>,
    ) -> Result<Self, ParseError> {
        let blocks = blocks.into();
        if blocks.is_empty() {
            return Err(ParseError("a blob has at least one block".to_owned()));
        }
        // A zero-length block is the empty blob's own shape, and only there: a
        // reader prefix-summing lengths assumes every other block contributes
        // bytes, so an empty one in the middle is rejected at construction
        // rather than re-checked by every read path.
        if blocks.len() > 1 && blocks.iter().any(|block| block.len == 0) {
            return Err(ParseError(
                "a zero-length block must be the blob's only block".to_owned(),
            ));
        }
        // `checked_add`, not `sum`: the sum of an attacker-shaped or corrupt
        // list can overflow, and a wrapped sum would accept the reference —
        // which then flows into the store's prefix-sum arithmetic.
        let Some(sum) = blocks
            .iter()
            .try_fold(0u64, |sum, block| sum.checked_add(block.len))
        else {
            return Err(ParseError(format!(
                "block lengths overflow the declared length {len}"
            )));
        };
        if sum != len {
            return Err(ParseError(format!("block list does not sum to {len}")));
        }
        let blocks = match blocks.len() {
            1 => Blocks::One(blocks[0]),
            _ => Blocks::Many(blocks),
        };
        Ok(Self { hash, len, blocks })
    }

    /// The hash of the whole content.
    pub fn hash(&self) -> &BlockHash {
        &self.hash
    }

    /// The content's total length in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether the blob is empty (a legal, stored blob of zero bytes).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The blob's blocks, in content order.
    pub fn blocks(&self) -> &[BlockRef] {
        match &self.blocks {
            Blocks::One(block) => from_ref(block),
            Blocks::Many(blocks) => blocks,
        }
    }
}

/// A block as the sweep sees it: its address, its length, and when it became
/// visible in the store.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::blob::{BlockHash, Entry};
///
/// let entry = Entry {
///     hash: BlockHash::of(b"hello"),
///     len: 5,
///     published: SystemTime::UNIX_EPOCH,
/// };
/// assert_eq!(entry.len, 5);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// The block's address.
    pub hash: BlockHash,
    /// The block's length in bytes.
    pub len: u64,
    /// When the block was published (the store's own clock: the file's
    /// modification time for the local implementation).
    ///
    /// The value is compared against the sweep's `now`, so it must be
    /// comparable to it: a backend must say how it obtains a timestamp on the
    /// caller's clock. A clock running behind makes freshly published blocks
    /// look old, and the sweep then reclaims a block a version row is about to
    /// name; one running ahead silently disables reclamation.
    pub published: SystemTime,
}

/// The entries a [`BlockStore::walk`] yields: an [`Entry`] per item, or the
/// backend's error type `E` ending the walk.
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use futures::stream;
/// use tinio_core::blob::{Entry, EntryStream};
///
/// let entries: Vec<Entry> = Vec::new();
/// let walk: EntryStream<io::Error> = Box::pin(stream::iter(entries.into_iter().map(Ok)));
/// drop(walk);
/// ```
pub type EntryStream<E> = Pin<Box<dyn Stream<Item = Result<Entry, E>> + Send>>;

/// What a publish did.
///
/// # Examples
///
/// ```rust
/// use tinio_core::blob::Published;
///
/// assert_ne!(Published::Stored, Published::Existing);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// The address was new: this call stored the bytes.
    Stored,
    /// The address was already present: the staged bytes were discarded.
    Existing,
}

/// A block being staged: the engine writes the block's bytes here and then
/// hands the value to [`BlockStore::publish`].
///
/// # Examples
///
/// ```rust
/// use std::io;
///
/// use bytes::Bytes;
/// use tinio_core::blob::StagedBlock;
/// use tokio::runtime::Runtime;
///
/// /// A sink that holds the staged block's bytes in memory.
/// struct Sink(Vec<u8>);
///
/// #[async_trait::async_trait]
/// impl StagedBlock for Sink {
///     async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
///         self.0.extend_from_slice(&chunk);
///         Ok(())
///     }
/// }
///
/// let mut staged = Sink(Vec::new());
/// Runtime::new()
///     .unwrap()
///     .block_on(staged.write(Bytes::from_static(b"hello")))
///     .unwrap();
/// assert_eq!(staged.0, b"hello");
/// ```
#[async_trait::async_trait]
pub trait StagedBlock: Send + Sync + 'static {
    /// Append a chunk of the block's bytes.
    async fn write(&mut self, chunk: Bytes) -> io::Result<()>;
}

/// Immutable, content-addressed block storage.
///
/// The store guarantees what the engine relies on: a block is named by its
/// hash, so a repeated or interrupted write can never be observed as content
/// (the bytes become visible only on [`BlockStore::publish`]); the namespace is
/// one caller-chosen root (a bucket's store, in this workspace); and `open`
/// answers an [`io::ErrorKind::NotFound`] error for a block that is not there,
/// which a sweep racing a read can produce.
///
/// # Examples
///
/// A minimal but complete store — deliberately the smallest implementation that
/// exercises every method of the trait. It does not track a publication time,
/// so the entries it lists are stamped with the store's clock at listing.
///
/// ```rust
/// use std::{collections::BTreeMap, io, sync::Mutex, time::SystemTime};
///
/// use bytes::Bytes;
/// use futures::stream;
/// use tinio_core::{
///     ByteRange,
///     blob::{BlockHash, BlockStore, Entry, EntryStream, Published, StagedBlock},
///     storage::{self, BodyStream},
/// };
/// use tokio::runtime::Runtime;
///
/// /// An in-memory store: staged bytes are held in memory, filed on publish.
/// #[derive(Default)]
/// struct Memory(Mutex<BTreeMap<BlockHash, Vec<u8>>>);
///
/// /// A sink that holds the staged block's bytes in memory.
/// #[derive(Default)]
/// struct Sink(Vec<u8>);
///
/// #[async_trait::async_trait]
/// impl StagedBlock for Sink {
///     async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
///         self.0.extend_from_slice(&chunk);
///         Ok(())
///     }
/// }
///
/// #[async_trait::async_trait]
/// impl BlockStore for Memory {
///     type Error = storage::Error;
///     type Staged = Sink;
///
///     async fn stage(&self) -> Result<Self::Staged, Self::Error> {
///         Ok(Sink::default())
///     }
///
///     async fn publish(
///         &self,
///         staged: Self::Staged,
///         hash: &BlockHash,
///     ) -> Result<Published, Self::Error> {
///         let mut blocks = self.0.lock().unwrap();
///         if blocks.contains_key(hash) {
///             return Ok(Published::Existing);
///         }
///         blocks.insert(*hash, staged.0);
///         Ok(Published::Stored)
///     }
///
///     async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
///         Ok(self.0.lock().unwrap().contains_key(hash))
///     }
///
///     async fn open(
///         &self,
///         hash: &BlockHash,
///         range: Option<ByteRange>,
///     ) -> Result<BodyStream, Self::Error> {
///         let blocks = self.0.lock().unwrap();
///         let bytes = blocks.get(hash).ok_or_else(|| {
///             storage::io(io::Error::new(io::ErrorKind::NotFound, "no such block"))
///         })?;
///         let slice = match range {
///             Some(range) => {
///                 let (start, end) = range.resolve(bytes.len() as u64)?;
///                 bytes[start as usize..=end as usize].to_vec()
///             }
///             None => bytes.clone(),
///         };
///         Ok(Box::pin(stream::once(
///             async move { Ok(Bytes::from(slice)) },
///         )))
///     }
///
///     async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
///         self.0.lock().unwrap().remove(hash);
///         Ok(())
///     }
///
///     async fn list(
///         &self,
///         after: Option<&BlockHash>,
///         limit: usize,
///     ) -> Result<Vec<Entry>, Self::Error> {
///         Ok(self.entries(after).into_iter().take(limit).collect())
///     }
///
///     async fn walk(
///         &self,
///         after: Option<&BlockHash>,
///     ) -> Result<EntryStream<Self::Error>, Self::Error> {
///         Ok(Box::pin(stream::iter(
///             self.entries(after).into_iter().map(Ok),
///         )))
///     }
/// }
///
/// impl Memory {
///     /// The blocks that sort strictly after `after`, ascending.
///     fn entries(&self, after: Option<&BlockHash>) -> Vec<Entry> {
///         self.0
///             .lock()
///             .unwrap()
///             .iter()
///             .filter(|(hash, _)| after.is_none_or(|after| *hash > after))
///             .map(|(hash, bytes)| Entry {
///                 hash: *hash,
///                 len: bytes.len() as u64,
///                 published: SystemTime::now(),
///             })
///             .collect()
///     }
/// }
///
/// let runtime = Runtime::new().unwrap();
/// let store = Memory::default();
/// let hash = BlockHash::of(b"hello");
/// let mut staged = runtime.block_on(store.stage()).unwrap();
/// runtime
///     .block_on(staged.write(Bytes::from_static(b"hello")))
///     .unwrap();
/// assert_eq!(
///     runtime.block_on(store.publish(staged, &hash)).unwrap(),
///     Published::Stored
/// );
/// assert!(runtime.block_on(store.contains(&hash)).unwrap());
/// ```
#[async_trait::async_trait]
pub trait BlockStore: Send + Sync + 'static {
    /// The backend error type, shared across the store's operations.
    ///
    /// It must convert into the contract error [`storage::Error`], like every
    /// other backend error in this workspace.
    type Error: StdError + Send + Sync + 'static + Into<storage::Error>;

    /// A block staged for a later [`BlockStore::publish`] — backend-specific.
    type Staged: StagedBlock;

    /// Begin staging a block.
    async fn stage(&self) -> Result<Self::Staged, Self::Error>;

    /// Finish and publish the staged block under `hash`.
    ///
    /// [`Published::Existing`] means the address was already present and the
    /// staged bytes were discarded (the dedup hit); [`Published::Stored`]
    /// means this call stored it.
    async fn publish(
        &self,
        staged: Self::Staged,
        hash: &BlockHash,
    ) -> Result<Published, Self::Error>;

    /// Whether the block is present — the dedup lookup.
    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error>;

    /// Open the block for reading, optionally a byte range within it.
    ///
    /// A missing block is an error whose source is an
    /// [`io::ErrorKind::NotFound`] I/O error.
    ///
    /// The `range` is the engine's intra-block sub-range, not an S3 wire range:
    /// a range that reaches past the stored block is a corruption-class error,
    /// never a clamp. Clamping an S3 `Range` header to the object the client
    /// named happens one layer up, in the engine's `open`, against the blob's
    /// declared length; by the time the engine asks a backend for a sub-range,
    /// the reference's block list and the stored block must agree, and a block
    /// shorter than the reference says is a store that lost bytes — clamping
    /// would serve the disagreement as if it were content. The engine enforces
    /// the agreement on every read: it counts the bytes each block yields and
    /// ends that part of the stream with an [`io::ErrorKind::UnexpectedEof`]
    /// error the moment a block comes up short, so the rule holds for every
    /// backend, not only for one that refuses to clamp.
    async fn open(
        &self,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error>;

    /// Remove the block. Idempotent: removing a block that is not there is `Ok`.
    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error>;

    /// List up to `limit` blocks whose address sorts strictly after `after`,
    /// in ascending address order — the sweep's page walk.
    async fn list(
        &self,
        after: Option<&BlockHash>,
        limit: usize,
    ) -> Result<Vec<Entry>, Self::Error>;

    /// Walk every block whose address sorts strictly after `after`, in
    /// ascending address order — the whole sequence [`BlockStore::list`] pages,
    /// yielded as one stream.
    ///
    /// A backend must hold `list`'s obligations across the whole traversal,
    /// not just within a page: every such block is yielded exactly once, in
    /// ascending address order. It should hold them in **one pass** — reading
    /// each of its directory-like units at most once per walk — which is the
    /// difference between this O(N) traversal and a page walk that re-reads a
    /// directory per page.
    ///
    /// The stream is pulled lazily, so the caller may await between entries (a
    /// sweep removes a dead block before asking for the next), and an `Err`
    /// item ends the walk.
    async fn walk(
        &self,
        after: Option<&BlockHash>,
    ) -> Result<EntryStream<Self::Error>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use futures::{StreamExt, stream};

    use super::*;

    #[test]
    fn a_block_hash_round_trips_through_its_hex_form() {
        let hash = BlockHash::of(b"hello");
        let hex = hash.to_hex();
        assert_eq!(
            hex, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            "the address is the SHA-256 of the bytes"
        );
        assert_eq!(BlockHash::new(&hex).unwrap(), hash);
        assert_eq!(hash.to_string(), hex);
        assert_eq!(BlockHash::from_str(&hex).unwrap(), hash);
    }

    #[test]
    fn a_block_hash_rejects_anything_that_is_not_64_hex_characters() {
        assert!(BlockHash::new("").is_err());
        assert!(BlockHash::new("zz").is_err());
        assert!(BlockHash::new(&"a".repeat(63)).is_err());
        assert!(BlockHash::new(&"a".repeat(65)).is_err());
        assert!(
            BlockHash::new(&"A".repeat(64)).is_ok(),
            "uppercase hex is the same address"
        );
    }

    #[test]
    fn a_blob_ref_rejects_a_block_list_that_disagrees_with_its_length() {
        let hash = BlockHash::of(b"hello");
        let one = BlockRef { hash, len: 5 };
        assert!(BlobRef::new(hash, 5, vec![one]).is_ok());
        assert!(
            BlobRef::new(hash, 6, vec![one]).is_err(),
            "declared length must match the blocks"
        );
        assert_eq!(
            BlobRef::new(hash, 0, Vec::new())
                .expect_err("a blob always has at least one block")
                .to_string(),
            "invalid block reference: a blob has at least one block",
            "an empty list breaks the block-count rule, and says so"
        );
        assert_eq!(BlobRef::single(hash, 5).blocks(), [one]);
    }

    #[test]
    fn a_zero_length_block_is_legal_only_as_the_whole_blob() {
        let empty = BlockHash::of(b"");
        let zero = BlockRef {
            hash: empty,
            len: 0,
        };
        assert!(
            BlobRef::new(empty, 0, vec![zero]).is_ok(),
            "the empty blob is one zero-length block, and stays accepted"
        );

        let hash = BlockHash::of(b"hello");
        let two = BlockRef { hash, len: 2 };
        assert_eq!(
            BlobRef::new(hash, 2, vec![two, zero])
                .expect_err("a zero-length block beside any other is rejected")
                .to_string(),
            "invalid block reference: a zero-length block must be the blob's only block",
            "the list sums to the declared length, so only the new rule rejects it"
        );
    }

    #[test]
    fn a_blob_ref_rejects_a_block_list_whose_lengths_overflow() {
        let hash = BlockHash::of(b"hello");
        let max = BlockRef {
            hash,
            len: u64::MAX,
        };
        // `u64::MAX + 5` wraps to 4: an unchecked sum would accept this list
        // against a declared length of 4, in release semantics, and the wrapped
        // length would then reach the store's prefix-sum arithmetic.
        assert!(
            BlobRef::new(hash, 4, vec![max, BlockRef { hash, len: 5 }]).is_err(),
            "a sum that overflows is rejected, never wrapped"
        );
        assert!(
            BlobRef::new(hash, u64::MAX, vec![max]).is_ok(),
            "a single block at the maximum length still sums exactly"
        );
    }

    /// A minimal in-memory `BlockStore`: a shape-pinning implementation that
    /// exercises every method of the trait, so its signatures are fixed by a
    /// compiling implementation rather than by prose — this module's own test
    /// double.
    #[derive(Default)]
    struct Fake {
        blocks: Mutex<BTreeMap<BlockHash, (Vec<u8>, SystemTime)>>,
    }

    #[async_trait::async_trait]
    impl StagedBlock for Vec<u8> {
        async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
            self.extend_from_slice(&chunk);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl BlockStore for Fake {
        type Error = storage::Error;
        type Staged = Vec<u8>;

        async fn stage(&self) -> Result<Self::Staged, Self::Error> {
            Ok(Vec::new())
        }

        async fn publish(
            &self,
            staged: Self::Staged,
            hash: &BlockHash,
        ) -> Result<Published, Self::Error> {
            let mut blocks = self.blocks.lock().unwrap();
            if blocks.contains_key(hash) {
                return Ok(Published::Existing);
            }
            blocks.insert(*hash, (staged, SystemTime::now()));
            Ok(Published::Stored)
        }

        async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
            Ok(self.blocks.lock().unwrap().contains_key(hash))
        }

        async fn open(
            &self,
            hash: &BlockHash,
            range: Option<ByteRange>,
        ) -> Result<BodyStream, Self::Error> {
            let blocks = self.blocks.lock().unwrap();
            let (bytes, _) = blocks.get(hash).ok_or_else(|| not_found(hash))?;
            let slice = match range {
                Some(range) => {
                    let (start, end) = range.resolve(bytes.len() as u64)?;
                    bytes[start as usize..=end as usize].to_vec()
                }
                None => bytes.clone(),
            };
            Ok(Box::pin(stream::once(
                async move { Ok(Bytes::from(slice)) },
            )))
        }

        async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
            self.blocks.lock().unwrap().remove(hash);
            Ok(())
        }

        async fn list(
            &self,
            after: Option<&BlockHash>,
            limit: usize,
        ) -> Result<Vec<Entry>, Self::Error> {
            Ok(self.entries(after).into_iter().take(limit).collect())
        }

        async fn walk(
            &self,
            after: Option<&BlockHash>,
        ) -> Result<EntryStream<Self::Error>, Self::Error> {
            Ok(Box::pin(stream::iter(
                self.entries(after).into_iter().map(Ok),
            )))
        }
    }

    impl Fake {
        /// The blocks that sort strictly after `after`, ascending.
        fn entries(&self, after: Option<&BlockHash>) -> Vec<Entry> {
            self.blocks
                .lock()
                .unwrap()
                .iter()
                .filter(|(hash, _)| after.is_none_or(|after| *hash > after))
                .map(|(hash, (bytes, published))| Entry {
                    hash: *hash,
                    len: bytes.len() as u64,
                    published: *published,
                })
                .collect()
        }
    }

    fn not_found(hash: &BlockHash) -> storage::Error {
        storage::io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no block {hash}"),
        ))
    }

    /// Stage `bytes` and publish them under their own content address: the
    /// setup the store tests share.
    async fn publish_block(store: &Fake, bytes: &'static [u8]) -> BlockHash {
        let hash = BlockHash::of(bytes);
        let mut staged = store.stage().await.unwrap();
        staged.write(Bytes::from_static(bytes)).await.unwrap();
        assert_eq!(
            store.publish(staged, &hash).await.unwrap(),
            Published::Stored
        );
        hash
    }

    /// Drain a walk into the entries it yielded, in the order it yielded them.
    async fn collect_walk(mut walk: EntryStream<storage::Error>) -> Vec<Entry> {
        let mut entries = Vec::new();
        while let Some(entry) = walk.next().await {
            entries.push(entry.expect("the fake never errors mid-walk"));
        }
        entries
    }

    #[tokio::test]
    async fn the_contract_round_trips_through_a_minimal_store() {
        let store = Fake::default();
        let hash = BlockHash::of(b"hello");
        let mut staged = store.stage().await.unwrap();
        staged.write(Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(
            store.publish(staged, &hash).await.unwrap(),
            Published::Stored
        );
        assert!(store.contains(&hash).await.unwrap());
        assert_eq!(
            store.publish(Vec::new(), &hash).await.unwrap(),
            Published::Existing,
            "a repeated publish is a no-op"
        );
        assert_eq!(store.list(None, 10).await.unwrap().len(), 1);
        store.remove(&hash).await.unwrap();
        assert!(!store.contains(&hash).await.unwrap());
        assert!(matches!(
            store.open(&hash, None).await,
            Err(storage::Error::Io(err)) if err.kind() == io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn a_block_hash_rejects_a_non_hex_character_at_a_legal_length() {
        // Every other rejection in this module is a wrong length, so this is
        // the only shape that reaches the decode's own validity check: 64
        // characters, one of them not hex.
        let input = format!("{}g", "a".repeat(63));
        let err = BlockHash::new(&input).expect_err("64 characters, one of them not hex");
        assert_eq!(err.0, input, "the error preserves the input it refused");
        assert_eq!(err.to_string(), format!("invalid block reference: {input}"));

        // 32 two-byte characters: 64 bytes, so a byte-length guard passes it
        // and only the decode itself can refuse it.
        let non_ascii = "é".repeat(32);
        assert_eq!(non_ascii.len(), 64);
        assert!(
            BlockHash::new(&non_ascii).is_err(),
            "64 bytes of non-ASCII is not an address"
        );
    }

    #[test]
    fn a_block_hash_from_str_trusts_the_hex_form() {
        let hash = BlockHash::of(b"hello");
        assert_eq!(BlockHash::from(hash.to_hex().as_str()), hash);
    }

    #[test]
    #[should_panic(expected = "valid block hash")]
    fn a_block_hash_from_str_panics_on_a_malformed_hash() {
        // `From<&str>` is the trusted-input path: a malformed hash must panic
        // rather than quietly address some other block.
        let _hash = BlockHash::from("not a hash");
    }

    #[test]
    fn a_blob_ref_new_agrees_with_single_and_keeps_block_order() {
        let hash = BlockHash::of(b"hello");
        let one = BlockRef { hash, len: 5 };
        assert_eq!(
            BlobRef::new(hash, 5, vec![one]).unwrap(),
            BlobRef::single(hash, 5),
            "a one-block list is the version-1 shape, not a distinct one"
        );

        let first = BlockRef {
            hash: BlockHash::of(b"hel"),
            len: 3,
        };
        let second = BlockRef {
            hash: BlockHash::of(b"lo"),
            len: 2,
        };
        let blob = BlobRef::new(hash, 5, vec![first, second]).unwrap();
        assert_eq!(
            blob.blocks(),
            [first, second],
            "the blocks come back in content order"
        );
    }

    #[tokio::test]
    async fn the_walk_resumes_strictly_after_its_cursor_in_ascending_order() {
        let store = Fake::default();
        let mut hashes = [
            publish_block(&store, b"alpha").await,
            publish_block(&store, b"beta").await,
            publish_block(&store, b"gamma").await,
        ];
        hashes.sort();
        let [smallest, middle, largest] = hashes;

        let walked = collect_walk(store.walk(Some(&smallest)).await.unwrap()).await;
        assert_eq!(
            walked.iter().map(|entry| entry.hash).collect::<Vec<_>>(),
            [middle, largest],
            "the cursor block itself is not re-emitted"
        );

        let walked = collect_walk(store.walk(None).await.unwrap()).await;
        assert_eq!(
            walked.iter().map(|entry| entry.hash).collect::<Vec<_>>(),
            [smallest, middle, largest],
            "a walk from the start is the whole set, ascending"
        );
    }

    #[tokio::test]
    async fn the_list_stops_at_its_limit() {
        let store = Fake::default();
        let mut hashes = [
            publish_block(&store, b"alpha").await,
            publish_block(&store, b"beta").await,
            publish_block(&store, b"gamma").await,
        ];
        hashes.sort();
        let [smallest, middle, _] = hashes;

        let listed = store.list(None, 1).await.unwrap();
        assert_eq!(listed.len(), 1, "the limit is binding");
        assert_eq!(listed[0].hash, smallest, "the smallest address comes first");

        let listed = store.list(Some(&smallest), 1).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].hash, middle,
            "the cursor still excludes its own block within a page"
        );
    }

    #[tokio::test]
    async fn a_published_block_opens_and_a_dedup_publish_discards_the_staged_bytes() {
        let store = Fake::default();
        let hash = publish_block(&store, b"hello").await;

        // The dedup hit: the address is already present, so the staged bytes
        // are discarded — an empty stage must not overwrite the block.
        let staged = store.stage().await.unwrap();
        assert!(staged.is_empty());
        assert_eq!(
            store.publish(staged, &hash).await.unwrap(),
            Published::Existing
        );

        let body = store.open(&hash, None).await.unwrap();
        assert_eq!(
            storage::collect_body(body).await.unwrap(),
            b"hello",
            "the dedup publish discarded the staged bytes, keeping the stored block"
        );
    }

    #[tokio::test]
    async fn the_listed_entry_reports_the_stored_blocks_hash_length_and_publication_time() {
        let store = Fake::default();
        let before = SystemTime::now();
        let hash = publish_block(&store, b"hello").await;
        let after = SystemTime::now();

        let listed = store.list(None, 10).await.unwrap();
        assert_eq!(listed.len(), 1);
        let entry = listed[0];
        assert_eq!(entry.hash, hash);
        assert_eq!(entry.len, 5, "the entry's length is the stored block's");
        assert!(
            entry.published >= before && entry.published <= after,
            "published {:?} must sit in the window around the publish",
            entry.published
        );
    }

    #[tokio::test]
    async fn open_serves_a_sub_range_and_rejects_one_past_the_end() {
        let store = Fake::default();
        let hash = publish_block(&store, b"hello").await;

        let body = store
            .open(&hash, Some(ByteRange::Inclusive(1, 3)))
            .await
            .unwrap();
        assert_eq!(
            storage::collect_body(body).await.unwrap(),
            b"ell",
            "the range is inclusive at both ends"
        );

        assert!(
            matches!(
                store.open(&hash, Some(ByteRange::From(6))).await,
                Err(storage::Error::InvalidRange {
                    range: ByteRange::From(6),
                    size: 5
                })
            ),
            "a start past the end is a corruption-class error, never an empty body"
        );
    }

    #[tokio::test]
    async fn remove_is_idempotent() {
        let store = Fake::default();
        let hash = publish_block(&store, b"hello").await;
        store.remove(&hash).await.unwrap();
        store
            .remove(&hash)
            .await
            .expect("removing a block that is not there is Ok");
        assert!(!store.contains(&hash).await.unwrap());
    }
}
