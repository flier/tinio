# S3 versioning, plan 1 — the block engine: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A content-addressed block store — the `tinio-core::blob` contract plus the new `tinio-blob` crate (engine + local filesystem implementation) — that stores immutable blobs by SHA-256, deduplicates them, streams them back with byte ranges, and reclaims unreferenced blocks; nothing in either place knows what S3, a bucket or a version is.

**Architecture:** The contract (`tinio-core::blob`) is a dumb, backend-agnostic surface: stage bytes, publish them under a hash, open by hash with an optional range, test existence, remove, and page through the store for the sweep. The engine (`tinio-blob::Store<S>`) owns every policy — streaming hash-while-writing, the dedup lookup, blob references and the live-set sweep — and is generic over the contract, so a future WebDAV/FTP backend implements the trait and inherits the behaviour. The local implementation writes one file per block, named by its hash, under a two-hex-character fan-out directory, published by temp + rename.

**Tech Stack:** Rust (edition 2024), `tokio` (`fs`, `io-util`), `async-trait`, `thiserror`, `sha2`, `bytes`, `futures`, `derive_more`, `hex`, `uuid` (temp names), `tempfile` (dev).

**Spec:** `docs/superpowers/specs/2026-10-03-s3-versioning-design.md` — the plan argues from the spec; read them together. This plan implements the spec's **The block store: one contract, one engine, one implementation** section and nothing else: no version tables, no S3 surface, no config keys (plans 2 and 3 own those).
> **Execution note (2026-10-03).** The operator ruled that this run accumulates uncommitted work, so every task's final **Commit** step is deliberately left unticked and unperformed; all other steps are ticked as they land. Task 1 is complete (review clean; 3 lib + 45 doc tests green). **2026-10-06 (review round).** The accumulated work has since landed as three bundled commits — `eb4b28e7` (2026-10-03, the block engine), `ee9896a6` (2026-10-05, the versioned store) and `ba5004a6` (2026-10-06, the S3 surface) — so the nine Commit steps stay unticked because they were never performed as written; every ticked step was re-verified against `ba5004a6`'s tree by this round.
>
> **Consolidated ruling note — the changes landed after this plan's final review (2026-10-03).** The notes under the task headings below record the decisions made *during* execution; this block is the consolidated record of what changed *after* its whole-branch review, so a re-run of this plan lands the same tree (plan 2's dependency note carries the same list). Each entry is part of plan 1 as shipped:
> - `BlockStore` gained a **required** `walk(after) -> Result<EntryStream<Self::Error>, Self::Error>`, with the `tinio_core::blob::EntryStream<E>` alias: paging the sweep through `list` restarted the traversal from the root on every page, which made one sweep Θ(N²) (at 10⁶ blocks, ~7.6 name-parses per block). `list` stays — this plan's Interfaces and the conformance suite's paging assertions pin it — and is now built on the walk, so the two cannot disagree.
> - The local walk lists only **canonical entries**: a regular file whose name is the lowercase hex address of its own fan-out directory. An entry that is not exactly where `block_path` puts it (an uppercase spelling, a canonical name filed under the wrong directory, a directory) is skipped, so it can never be listed as a block that `remove` cannot find — a phantom a sweep would report removed on every pass, forever.
> - `publish` **fsyncs the fan-out directory** after the rename, so the committed name is durable rather than only the content (unix only: on Windows std cannot open a directory handle, and no dependency is added for it).
> - `BlobRef`'s block list is the inline `Blocks` enum (`One`/`Many`) rather than a `Box<[BlockRef]>`: v1 stores one block per blob, and the boxed slice was an allocation for every blob stored.
> - `BlobRef::new` **rejects a zero-length block in a multi-block list** (a zero-length block is legal only as the blob's sole block), which is what makes the read path's prefix-sum arithmetic total without a guard on every read.
> - `Error::Missing`, `Error::Store` and `Error::store` were **deleted**: every backend error now projects through the contract variant because `BlockStore::Error` is already bound `Into<storage::Error>`, and boxing a `NotFound` lost the error kind the S3 layer classifies on. The variant was renamed `Contract` → `Error::Storage` in the wave-3 cleanup, the name `docs/style.md` §Error prescribes.
> - `tinio-blob`'s `versioning` feature and crate-root `#![cfg]` were **removed** by operator ruling: the feature had exactly one referent, the attribute, and its off-state compiled an empty crate; `testing` stands alone.
>
> **Wave 3 (2026-10-03) added two more, also part of this plan's tree:** the conformance target is no longer gated by `required-features = ["testing"]` — the crate's own `[dev-dependencies]` edge turns the feature on (`tinio-blob = { path = ".", features = ["testing"] }`, the shape `tinio-sandbox-linux` and `tinio-sandbox-windows` use), so `cargo test -p tinio-blob` with no flags runs it; and `Store::open` counts the bytes each block part yields and ends the stream with an `UnexpectedEof` error when a part comes up short, which makes `BlockStore::open`'s "never a clamp" contract true for every backend instead of a plan-2 promise.
>
> **Workspace refactor shipped inside this plan's tree (recorded 2026-10-06, review round).** The staged-temp and publish protocol is written over shared `tinio-util::fsutil` helpers (`commit`, `file_stream`, `ShortRead`, `remove_staged_temp`, …), extracted from `tinio-fs` in the same commit — so the shipped `tinio-blob` carries a **runtime** `tinio-util` dependency, its manifest drops `hex` and adds `smart-default`, and Task 3's manifest fence and Task 4–5 Interfaces below predate that; the tree is what shipped.


## Global Constraints

- English only: docs, artifacts, comments, commits, PRs (`docs/compress.md`). No hard-wrapped lines in this plan or any prose document — one line per paragraph, list item or table row.
- Every public item carries a doc comment **and at least one runnable rustdoc example** — the exempt classes are a closed list (constitution III, `.specify/memory/constitution.md`). The doc gate is `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps` (`docs/tests.md`), so a broken intra-doc link or a non-compiling example fails the gate.
- Unit tests live inline: `#[cfg(test)] mod tests` in the file that defines the subject (`docs/tests.md`). Async tests use `#[tokio::test]`; sync tests `#[test]`. A `tests/` target is only for a suite whose subject is the crate's public API.
- A test harness that a `tests/` target must share has to be lib code behind a **non-default feature** (`docs/tests.md`) — that is why the conformance suite (Task 9) is `feature = "testing"`, not `#[cfg(test)]`.
- Cargo: no comments in `Cargo.toml`; a dependency is pinned once in `[workspace.dependencies]` and consumed as `X.workspace = true`; `[lints.clippy] absolute_paths = "warn"` and `[lints.rust] unsafe_code = "forbid"` on every crate (`docs/cargo.md`). A new `crates/tinio-blob/` directory is a workspace member automatically (`members = ["crates/*"]`); only the `[workspace.dependencies]` line is added by hand, alphabetically among the `tinio-*` entries.
- Imports: top-level `use`, never inline; 3+ segment paths are imported (`clippy::absolute_paths`); a crate that wants `crate::_core::…` declares `#[doc(hidden)] pub extern crate tinio_core as _core;` (`docs/style.md`).
- Per-crate `Error`: a private `mod error`, thiserror-derived, re-exported, with `impl From<Error> for tinio_core::storage::Error` (`docs/style.md`). Backends call the contract error's constructor helpers (`storage::io(…)`, …), never build variants (`crates/tinio-core/src/storage/error.rs:123-239`).
- Streaming discipline (constitution V): no whole-object buffering anywhere in this crate. A blob is written chunk by chunk and read back as a stream.
- Git: every commit step asks the operator first (`docs/git.md`: writes are ask-first, per op, never auto; the index is untouched by tooling). Commit messages carry **no AI/agent trailers**.
- Gate commands (local, pre-push): `cargo test -p <crate>`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check --package <crate> --each-feature`, plus `cargo fmt` and `cargo clippy`.

## Review Focus

The failure modes below are the ones the spec implies and no task's happy-path test would catch on its own; each has its test written into the task that owns the code, in that task's own steps.

1. **An empty blob (zero bytes).** A zero-byte object is legal S3 (`data-model.md`: "zero-length content valid") and must store, dedup and read back as an empty stream — the first block of any real store is the empty one, and an off-by-one on `len == 0` breaks every zero-byte PUT later.
2. **A block that is not there.** Out-of-band deletion or a sweep racing a read leaves the address valid but the file gone; `open` must answer an `ErrorKind::NotFound` I/O error, never hang and never serve a zero-length body as if it were content.
3. **Range boundaries.** `bytes=0-0` on a one-byte block, a range covering exactly the whole block, and a range whose start is past the end: the first two must serve, the third must be `InvalidRange` — the S3 layer maps that to 416 and a wrong answer here becomes a wrong `Content-Range` later.
4. **A publish that races the sweep.** A block written within the grace window must survive a sweep that does not know about it (the live set was computed before the publish); the grace is the only thing standing between that race and silent data loss.
5. **A body that errors mid-stream.** The source stream fails after some chunks: nothing may be published, no partial block may become visible under a hash, and the failure must surface as the caller's error rather than a truncated success.

---

### Task 1: The contract types — `BlockHash`, `BlockRef`, `BlobRef`, `Entry`

**Files:**
- Create: `crates/tinio-core/src/blob.rs`
- Modify: `crates/tinio-core/src/lib.rs` (module declaration + re-export block)

**Interfaces:**
- Consumes: `crate::storage::range::ByteRange` (already exists: `crates/tinio-core/src/storage/range.rs:22-30`), `sha2::{Digest, Sha256}` (the workspace's usage pattern: `crates/tinio-core/src/acl.rs:21,79`), `derive_more::{AsRef, Deref, Display}`, `hex`.
- Produces (later tasks and plans 2–3 depend on these exact names): `tinio_core::blob::{BlockHash, ParseError, BlockRef, BlobRef, Entry}` with `BlockHash::from_digest([u8; 32]) -> Self`, `BlockHash::as_bytes(&self) -> &[u8; 32]`, `BlockHash::to_hex(&self) -> String`, `BlockHash::new(&str) -> Result<Self, ParseError>`, `BlockHash::of(bytes: &[u8]) -> Self`; `BlockRef { pub hash: BlockHash, pub len: u64 }`; `BlobRef::single(BlockHash, u64) -> Self`, `BlobRef::new(BlockHash, u64, impl Into<Box<[BlockRef]>>) -> Result<Self, ParseError>`, accessors `hash()`, `len()`, `is_empty()`, `blocks()`; `Entry { pub hash: BlockHash, pub len: u64, pub published: SystemTime }`.

- [x] **Step 1: Write the failing test**

Create `crates/tinio-core/src/blob.rs` with only the module doc and this test module, so the file compiles and the test fails on the missing types:

```rust
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

use std::{str::FromStr, time::SystemTime};

use derive_more::{AsRef, Deref};
use sha2::{Digest, Sha256};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_block_hash_round_trips_through_its_hex_form() {
        let hash = BlockHash::of(b"hello");
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
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
        assert!(BlockHash::new(&"A".repeat(64)).is_ok(), "uppercase hex is the same address");
    }

    #[test]
    fn a_blob_ref_rejects_a_block_list_that_disagrees_with_its_length() {
        let hash = BlockHash::of(b"hello");
        let one = BlockRef { hash, len: 5 };
        assert!(BlobRef::new(hash, 5, vec![one]).is_ok());
        assert!(BlobRef::new(hash, 6, vec![one]).is_err(), "declared length must match the blocks");
        assert!(BlobRef::new(hash, 0, Vec::new()).is_err(), "a blob always has at least one block");
        assert_eq!(BlobRef::single(hash, 5).blocks(), [one]);
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core --lib blob`
Expected: FAIL — `cannot find type BlockHash in this scope`.

- [x] **Step 3: Write minimal implementation**

Add above the test module in `crates/tinio-core/src/blob.rs`:

```rust
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, AsRef, Deref)]
#[as_ref(forward)]
pub struct BlockHash(#[deref] [u8; 32]);

impl BlockHash {
    /// The address of `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self::from_digest(Sha256::digest(bytes).into())
    }

    /// Wrap an already-computed SHA-256 digest.
    pub fn from_digest(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// The raw digest.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The lowercase hex form (the on-disk name and the wire form).
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse the hex form — the constructor for untrusted input.
    pub fn new(hash: &str) -> Result<Self, ParseError> {
        let bytes = hex::decode(hash).map_err(|_| ParseError(hash.to_owned()))?;
        let digest: [u8; 32] = bytes
            .try_into()
            .map_err(|_| ParseError(hash.to_owned()))?;
        Ok(Self(digest))
    }
}

impl Display for BlockHash {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.to_hex())
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

/// A malformed block-hash string.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid block hash: {0}")]
pub struct ParseError(String);

/// One block of a blob: its address and the number of bytes it contributes.
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
    blocks: Box<[BlockRef]>,
}

impl BlobRef {
    /// The version-1 shape: one block whose address is the whole content's hash.
    pub fn single(hash: BlockHash, len: u64) -> Self {
        Self { hash, len, blocks: vec![BlockRef { hash, len }].into() }
    }

    /// Assemble a reference from its parts, checking the invariants: at least
    /// one block, and the declared length equal to the blocks' sum.
    pub fn new(hash: BlockHash, len: u64, blocks: impl Into<Box<[BlockRef]>>) -> Result<Self, ParseError> {
        let blocks = blocks.into();
        let sum: u64 = blocks.iter().map(|block| block.len).sum();
        if blocks.is_empty() || sum != len {
            return Err(ParseError(format!("block list does not sum to {len}")));
        }
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
        &self.blocks
    }
}

/// A block as the sweep sees it: its address, its length, and when it became
/// visible in the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    /// The block's address.
    pub hash: BlockHash,
    /// The block's length in bytes.
    pub len: u64,
    /// When the block was published (the store's own clock: the file's
    /// modification time for the local implementation).
    pub published: SystemTime,
}
```

The types above are Task 1's whole surface, on top of the file's three imports (`std::{str::FromStr, time::SystemTime}`, `derive_more::{AsRef, Deref}`, `sha2::{Digest, Sha256}`); the traits follow in Task 2, whose Step 3 lists the four imports they add to that same block.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-core --lib blob`
Expected: PASS — three tests green.

- [x] **Step 5: Wire the module into the crate**

In `crates/tinio-core/src/lib.rs`, add `pub mod blob;` alphabetically before `pub mod bucket;`, and add `blob::BlockHash` to the `pub use self::{…}` block. Add the doc example run to the gate: `cargo test -p tinio-core --doc` — expected PASS.

- [ ] **Step 6: Commit**

Ask the operator for the git write (`docs/git.md`), then:

```bash
git add crates/tinio-core/src/blob.rs crates/tinio-core/src/lib.rs
git commit -m "feat(core): add the block-store address and blob-reference types"
```

---

### Task 2: The contract traits — `StagedBlock` and `BlockStore`

**Files:**
- Modify: `crates/tinio-core/src/blob.rs` (append the traits; the test module gains the fake store)

**Interfaces:**
- Consumes: Task 1's types; `crate::storage::{BodyStream, error::{self as storage}}`; `async_trait::async_trait`.
- Produces: `pub trait StagedBlock { async fn write(&mut self, chunk: Bytes) -> io::Result<()>; }`; `pub enum Published { Stored, Existing }`; `pub trait BlockStore { type Error; type Staged; async fn stage(&self) -> Result<Self::Staged, Self::Error>; async fn publish(&self, staged: Self::Staged, hash: &BlockHash) -> Result<Published, Self::Error>; async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error>; async fn open(&self, hash: &BlockHash, range: Option<ByteRange>) -> Result<BodyStream, Self::Error>; async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error>; async fn list(&self, after: Option<&BlockHash>, limit: usize) -> Result<Vec<Entry>, Self::Error>; async fn walk(&self, after: Option<&BlockHash>) -> Result<EntryStream<Self::Error>, Self::Error>; }` — `walk` is required and `list` is built on it (the header's consolidated note), and `EntryStream<E>` is `tinio_core::blob`'s alias.

- [x] **Step 1: Write the failing test**

Append to the `tests` module in `crates/tinio-core/src/blob.rs` — a fake in-memory store that exercises every trait method, so the signatures are pinned by a compiling implementation rather than by prose:

```rust
    /// A minimal in-memory `BlockStore`, written to pin the trait's shape and
    /// to serve as the oracle for the engine's tests in `tinio-blob`.
    pub(crate) struct Fake {
        blocks: std::sync::Mutex<std::collections::BTreeMap<BlockHash, (Vec<u8>, SystemTime)>>,
    }

    impl Fake {
        pub(crate) fn new() -> Self {
            Self { blocks: Default::default() }
        }
    }

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

        async fn publish(&self, staged: Self::Staged, hash: &BlockHash) -> Result<Published, Self::Error> {
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

        async fn open(&self, hash: &BlockHash, range: Option<ByteRange>) -> Result<BodyStream, Self::Error> {
            let blocks = self.blocks.lock().unwrap();
            let (bytes, _) = blocks.get(hash).ok_or_else(|| not_found(hash))?;
            let slice = match range {
                Some(range) => {
                    let (start, end) = range.resolve(bytes.len() as u64)?;
                    bytes[start as usize..=end as usize].to_vec()
                }
                None => bytes.clone(),
            };
            Ok(Box::pin(futures::stream::once(async move { Ok(Bytes::from(slice)) })))
        }

        async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
            self.blocks.lock().unwrap().remove(hash);
            Ok(())
        }

        async fn list(&self, after: Option<&BlockHash>, limit: usize) -> Result<Vec<Entry>, Self::Error> {
            Ok(self
                .blocks
                .lock()
                .unwrap()
                .iter()
                .filter(|(hash, _)| after.is_none_or(|after| *hash > after))
                .take(limit)
                .map(|(hash, (bytes, published))| Entry { hash: *hash, len: bytes.len() as u64, published: *published })
                .collect())
        }
    }

    #[tokio::test]
    async fn the_contract_round_trips_through_a_minimal_store() {
        let store = Fake::new();
        let hash = BlockHash::of(b"hello");
        let mut staged = store.stage().await.unwrap();
        staged.write(Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(store.publish(staged, &hash).await.unwrap(), Published::Stored);
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
            store.open(&hash, None).await.unwrap_err(),
            storage::Error::Io(err) if err.kind() == io::ErrorKind::NotFound
        ));
    }
```

Add a `fn not_found(hash: &BlockHash) -> storage::Error` helper in the test module:

```rust
    fn not_found(hash: &BlockHash) -> storage::Error {
        storage::io(io::Error::new(io::ErrorKind::NotFound, format!("no block {hash}")))
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core --lib blob`
Expected: FAIL — `cannot find trait BlockStore in this scope`.

- [x] **Step 3: Write minimal implementation**

Append the traits to `crates/tinio-core/src/blob.rs` above the test module:

```rust
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
/// use bytes::Bytes;
/// use tinio_core::blob::StagedBlock;
///
/// # async fn example() -> std::io::Result<()> {
/// let mut staged: Vec<u8> = Vec::new();
/// staged.write(Bytes::from_static(b"hello")).await?;
/// assert_eq!(staged, b"hello");
/// # Ok(())
/// # }
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
    async fn publish(&self, staged: Self::Staged, hash: &BlockHash) -> Result<Published, Self::Error>;

    /// Whether the block is present — the dedup lookup.
    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error>;

    /// Open the block for reading, optionally a byte range within it.
    ///
    /// A missing block is an error whose source is an
    /// [`io::ErrorKind::NotFound`] I/O error.
    async fn open(&self, hash: &BlockHash, range: Option<ByteRange>) -> Result<BodyStream, Self::Error>;

    /// Remove the block. Idempotent: removing a block that is not there is `Ok`.
    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error>;

    /// List up to `limit` blocks whose address sorts strictly after `after`,
    /// in ascending address order — the sweep's page walk.
    async fn list(&self, after: Option<&BlockHash>, limit: usize) -> Result<Vec<Entry>, Self::Error>;
}
```

Task 2 adds four imports to the file, on top of Task 1's three: `use bytes::Bytes;` (the `StagedBlock::write` parameter), `use std::error::Error as StdError;` (the `type Error` bound), `use std::io;` (the staged sink's result type and, in the test module, `io::ErrorKind`), and — in the test module — `use crate::{storage::{self, BodyStream}, ByteRange};`. The test module's `Bytes` and `io::ErrorKind` arrive through its `use super::*;`, so they need no lines of their own. The attribute stays fully qualified (`#[async_trait::async_trait]`) and the stream construction stays fully qualified (`futures::stream::once`), so `futures` and `async_trait` need no top-level `use`.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-core --lib blob && cargo test -p tinio-core --doc`
Expected: PASS — four lib tests and the doc examples.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-core/src/blob.rs
git commit -m "feat(core): add the BlockStore contract"
```

---

### Task 3: The `tinio-blob` crate skeleton and its error

**Files:**
- Create: `crates/tinio-blob/Cargo.toml`
- Create: `crates/tinio-blob/src/lib.rs`
- Create: `crates/tinio-blob/src/error.rs`
- Modify: `Cargo.toml` (root — one `[workspace.dependencies]` line)

**Interfaces:**
- Consumes: `tinio_core::storage` (the contract error and its helpers).
- Produces: `tinio_blob::Error` (`error::Error`, re-exported) with `impl From<Error> for tinio_core::storage::Error`; `tinio_blob::Result<T>` alias; the crate's `_core` re-export so in-crate paths read `crate::_core::…`.

- [x] **Step 1: Create the manifest and the crate skeleton**

`crates/tinio-blob/Cargo.toml` (no comments — `docs/cargo.md`):

```toml
[package]
name = "tinio-blob"
version.workspace = true
edition.workspace = true
description = "Content-addressed block engine for tinio: blobs stored by SHA-256, deduplicated, swept by live set"
publish = false

[dependencies]
async-trait.workspace = true
bytes.workspace = true
futures.workspace = true
hex.workspace = true
sha2.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["fs", "io-util", "macros", "rt", "sync", "time"] }
tracing.workspace = true
uuid = { workspace = true, features = ["v4"] }

tinio-core.workspace = true

[features]
default = ["versioning"]
versioning = []
testing = ["versioning"]

[dev-dependencies]
tempfile.workspace = true
tinio-util = { workspace = true, features = ["testing"] }
tokio = { workspace = true, features = ["fs", "io-util", "macros", "rt", "rt-multi-thread", "sync", "time"] }

[lints.clippy]
absolute_paths = "warn"

[lints.rust]
unsafe_code = "forbid"
```

Root `Cargo.toml`: add `tinio-blob = { path = "crates/tinio-blob" }` to `[workspace.dependencies]`, alphabetically before `tinio-cli`.

`crates/tinio-blob/src/lib.rs`:

```rust
#![cfg(feature = "versioning")]

//! Content-addressed block engine for tinio.
//!
//! A blob is stored once, addressed by the SHA-256 of its content, and read
//! back as a stream; blocks that no live reference points at are reclaimed by a
//! sweep. Nothing here knows what S3, a bucket or a version is: the contract is
//! [`_core::blob::BlockStore`], the local implementation is [`LocalBlocks`],
//! and the policy — hashing, dedup, blob references, reclamation — is
//! [`Store`].

#[doc(hidden)]
pub extern crate tinio_core as _core;

mod engine;
mod error;
mod local;
mod sweep;

pub use self::{engine::Store, error::Error, local::LocalBlocks};

/// The crate's result alias.
pub type Result<T> = core::result::Result<T, Error>;
```

The crate root is gated, decided with the operator on 2026-10-03: `tinio-blob` is the versioning engine, and the design promises a build that does not need versioning **does not carry it** — a gate on the consumers alone would leave this code compiled into the binary as dead weight. The shape is the one `tinio-sandbox-windows` already uses (a root `#![cfg]` makes it the empty library on a host it is not built for); `default = ["versioning"]` keeps `cargo test -p tinio-blob` meaningful while `--no-default-features` strips the crate to nothing, and `testing` implies `versioning` because the conformance suite asserts the contract the feature gates.

> **Ruling at wave 2 (2026-10-03): the root gate is removed.** The `versioning` feature had exactly one referent — the crate-root attribute — and its off-state compiled an empty crate, so it bought nothing: attribute, feature and the `testing` implication are gone, the crate is built unconditionally, and `testing` stands alone. The ruling concerns `tinio-blob` only; the `versioning` features of the later crates are the later plans' to decide.

`crates/tinio-blob/src/engine.rs`, `local.rs`, `sweep.rs` are created empty in this task (module docs + nothing else) so the tree compiles; Tasks 4–8 fill them, and each entry above joins the `pub use` block in the task that defines its type (Task 4 `local::LocalBlocks`, Task 6 `engine::Store`, Task 8 `sweep::{LiveSet, SweepReport}` — `LiveSet` never lives in `engine`). `use crate::_core::blob::…` is the in-crate path form (`docs/style.md`).

- [x] **Step 2: Write the failing test**

`crates/tinio-blob/src/error.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::storage::{self, Error as StorageError};

    #[test]
    fn a_missing_block_projects_onto_a_not_found_io_error() {
        let err: StorageError = Error::Missing.into();
        let StorageError::Io(io) = err else {
            panic!("expected an I/O projection, got {err:?}");
        };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn a_contract_error_passes_through_the_projection_unchanged() {
        let contract = storage::no_such_key(object::Key::from("missing"));
        let err: StorageError = Error::Contract(contract.clone()).into();
        assert_eq!(err.to_string(), contract.to_string());
    }
}
```

- [x] **Step 3: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib`
Expected: FAIL — `cannot find type Error` / the crate does not compile.

- [x] **Step 4: Write minimal implementation**

Prepend to `crates/tinio-blob/src/error.rs`:

```rust
//! The engine's error type and its projection onto the contract error.

use std::io::{self, Error as IoError};

use crate::_core::{blob, storage};

/// A block-engine failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A block is not present in the store.
    #[error("block not found")]
    Missing,
    /// A blob reference was malformed.
    #[error(transparent)]
    Blob(#[from] blob::ParseError),
    /// The underlying store failed.
    #[error(transparent)]
    Store(Box<dyn std::error::Error + Send + Sync>),
    /// A filesystem operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] IoError),
}

impl From<Error> for storage::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Missing => storage::io(IoError::new(io::ErrorKind::NotFound, err.to_string())),
            Error::Blob(e) => storage::io(IoError::other(e)),
            Error::Store(e) => storage::io(IoError::other(e)),
            Error::Io(e) => storage::io(e),
        }
    }
}
```

The `Store(Box<dyn …>)` arm exists because the engine is generic over a `BlockStore` whose error type is only known at the call site; the engine boxes it (its own error cannot be generic without infecting every signature). Task 5 adds `Error::store(e)` — a constructor that boxes an `impl StdError + Send + Sync + 'static` — and the trait bound `<S as BlockStore>::Error: Into<storage::Error>` is what makes the projection total.

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib`
Expected: PASS — two tests green.

- [x] **Step 6: Prove the crate joins the gates**

Run: `cargo hack check --package tinio-blob --each-feature` — expected: the default graph and the `testing` graph both compile (with `testing` empty for now).
Run: `RUSTDOCFLAGS="-D warnings" cargo doc -p tinio-blob --no-deps` — expected: clean.

- [ ] **Step 7: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob Cargo.toml Cargo.lock
git commit -m "feat(blob): add the tinio-blob crate and its error"
```

---

### Task 4: `LocalBlocks` — staging and publishing

> **Ruling at execution (2026-10-03):** Step 4's "Expected: PASS — three tests green" is impossible as written. `BlockStore` declares six required methods, this task supplies three and Task 5 supplies the rest, so the file cannot compile until Task 5 lands. GREEN was proved with a temporary `todo!()` scaffold for the three Task-5 methods, since removed. Task 5's own GREEN run is the authoritative proof for the completed impl block; see the run ledger.

**Files:**
- Create: `crates/tinio-blob/src/local.rs`
- Modify: `crates/tinio-blob/src/lib.rs` (only if the re-export list needs the staged type)

**Interfaces:**
- Consumes: `_core::blob::{BlockHash, BlockStore, StagedBlock}`, tokio's `fs::File` + `io::AsyncWriteExt`.
- Produces: `LocalBlocks::new(root: impl Into<PathBuf>) -> Self`; `LocalBlocks::block_path(&self, hash: &BlockHash) -> PathBuf` (also used by Task 5's tests); the associated `Staged` type. Layout (documented in the module): blocks at `<root>/<first two hex chars>/<hex>`, staged temps at `<root>/tmp/<uuid>`.

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-blob/src/local.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::blob::{BlockHash, BlockStore, Published, StagedBlock};
    use bytes::Bytes;

    async fn staged(store: &LocalBlocks, bytes: &[u8]) -> <LocalBlocks as BlockStore>::Staged {
        let mut staged = store.stage().await.unwrap();
        staged.write(Bytes::copy_from_slice(bytes)).await.unwrap();
        staged
    }

    #[tokio::test]
    async fn publishing_a_block_makes_it_visible_under_its_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        assert!(!store.contains(&hash).await.unwrap());
        assert_eq!(
            store.publish(staged(&store, b"hello").await, &hash).await.unwrap(),
            Published::Stored
        );
        assert!(store.contains(&hash).await.unwrap());
        assert_eq!(
            std::fs::read(store.block_path(&hash)).unwrap(),
            b"hello",
            "the file is named by the hash and holds the block's bytes"
        );
    }

    #[tokio::test]
    async fn publishing_an_existing_block_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        assert_eq!(
            store.publish(staged(&store, b"hello").await, &hash).await.unwrap(),
            Published::Stored
        );
        assert_eq!(
            store.publish(staged(&store, b"hello").await, &hash).await.unwrap(),
            Published::Existing,
            "the second publish reports the dedup hit"
        );
        assert_eq!(std::fs::read(store.block_path(&hash)).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn a_dropped_stage_removes_its_temp_and_is_never_a_block() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");

        let staged = staged(&store, b"hello").await;
        drop(staged);

        assert!(!store.contains(&hash).await.unwrap(), "nothing is visible before publish");
        assert!(store.list(None, 10).await.unwrap().is_empty(), "a temp is not a block");
        assert!(
            std::fs::read_dir(dir.path().join("tmp")).unwrap().next().is_none(),
            "the dropped stage removed its own temp; only a process that died mid-stage leaves residue"
        );
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib local`
Expected: FAIL — `cannot find type LocalBlocks`.

- [x] **Step 3: Write minimal implementation**

Prepend to `crates/tinio-blob/src/local.rs`:

```rust
//! The local filesystem implementation of [`BlockStore`].
//!
//! Layout under the root directory the store is given: blocks live at
//! `<root>/<aa>/<hex>` where `<aa>` is the address's first two hex characters
//! (a 256-way fan-out, so no directory grows with the store), and blocks being
//! staged live at `<root>/tmp/<uuid>`. `tmp` is reserved: the address fan-out
//! is hex, so it can never collide, and [`list`](BlockStore::list) never
//! descends into it. A **dropped** stage removes its own temp (below); what is
//! left for the caller's cleanup to reap is the residue of a process that died
//! mid-stage (the fs backend's startup repair and sweep, plan 2).

use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

use crate::{
    _core::{
        blob::{BlockHash, BlockStore, Entry, Published, StagedBlock},
        storage, BodyStream, ByteRange,
    },
    Error,
};

/// A block store over a local directory.
///
/// # Examples
///
/// ```rust,no_run
/// use tinio_blob::LocalBlocks;
///
/// let store = LocalBlocks::new("/var/lib/tinio/blobs/demo");
/// # let _ = store;
/// ```
#[derive(Debug, Clone)]
pub struct LocalBlocks {
    root: PathBuf,
}

impl LocalBlocks {
    /// Serve blocks under `root`; `new` creates nothing itself — `stage` creates
    /// `<root>/tmp` and `publish` creates the fan-out directory it needs.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The path a block with this address is published at.
    pub fn block_path(&self, hash: &BlockHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex)
    }

    /// The directory staged blocks are written to.
    pub fn temp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }
}

#[async_trait::async_trait]
impl BlockStore for LocalBlocks {
    type Error = Error;
    type Staged = Staged;

    async fn stage(&self) -> Result<Self::Staged, Self::Error> {
        let dir = self.temp_dir();
        tokio::fs::create_dir_all(&dir).await?;
        let path = dir.join(uuid::Uuid::new_v4().to_string());
        let file = File::create(&path).await?;
        Ok(Staged { path, file: BufWriter::new(file) })
    }

    async fn publish(&self, mut staged: Self::Staged, hash: &BlockHash) -> Result<Published, Self::Error> {
        let target = self.block_path(hash);
        if tokio::fs::try_exists(&target).await? {
            tokio::fs::remove_file(&staged.path).await?;
            return Ok(Published::Existing);
        }
        staged.file.flush().await?;
        staged.file.into_inner().sync_all().await?;
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        match tokio::fs::rename(&staged.path, &target).await {
            Ok(()) => Ok(Published::Stored),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // The temp vanished (a crashed stage's residue was reaped, or a
                // concurrent publish won): a concurrent publish under the same
                // hash stores identical bytes, so a present target is a hit.
                if tokio::fs::try_exists(&target).await? {
                    Ok(Published::Existing)
                } else {
                    Err(Error::Io(err))
                }
            }
            Err(err) => Err(Error::Io(err)),
        }
    }

    async fn contains(&self, hash: &BlockHash) -> Result<bool, Self::Error> {
        Ok(tokio::fs::try_exists(self.block_path(hash)).await?)
    }
}
```

Task 5 adds `open`, `remove` and `list` to this impl block; the `Staged` type is:

```rust
/// A block staged as a file under the store's `tmp/`.
///
/// The handle is an `Option` so [`Drop`] can close it **before** removing the
/// file: on Windows a removal while the handle is open fails, and the workspace
/// already pays that price in `tinio-fs`'s `StagedBody`, whose own `Drop`
/// releases the file first.
#[derive(Debug)]
pub struct Staged {
    path: PathBuf,
    file: Option<BufWriter<File>>,
}

impl Drop for Staged {
    /// Best-effort: a stage that is abandoned removes its own temp, so the
    /// only residue `tmp/` ever holds is a process that died mid-stage.
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = std::fs::remove_file(&self.path);
    }
}

#[async_trait::async_trait]
impl StagedBlock for Staged {
    async fn write(&mut self, chunk: Bytes) -> io::Result<()> {
        self.file
            .as_mut()
            .expect("the file is open until publish consumes the stage")
            .write_all(&chunk)
            .await
    }
}
```

`stage` and `publish` adapt to the `Option`: `stage` builds `Staged { path, file: Some(BufWriter::new(file)) }`, and `publish` takes the handle out first (`let mut file = staged.file.take().expect(…)`) so the flush-then-rename sequence names it explicitly, then lets the stage's `Drop` run (the temp is already renamed away, so its removal is a silent no-op).

`Staged` is not re-exported from `lib.rs` (it appears only as the associated type); if the private-in-public lint objects, re-export it as `pub use self::local::Staged;`.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib local`
Expected: PASS — three tests green.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob/src/local.rs crates/tinio-blob/src/lib.rs
git commit -m "feat(blob): stage and publish blocks on the local filesystem"
```

---

### Task 5: `LocalBlocks` — reading, removing, listing

> **Rulings at execution (2026-10-03), two of them against this task's text:**
> 1. `open` returns `Error::Io(NotFound)`, not `Error::Missing` — the task's own code block and test contradict each other, and the test wins because the trait's documented contract ("an error whose source is an `io::ErrorKind::NotFound` I/O error") is satisfied that way. The `Error::Missing` variant this ruling preserved was deleted later, by the header's consolidated ruling — `Error::Blob`/`Error::Io`/`Error::Storage` are the whole enum, and a missing block reaches callers as `Error::Io(NotFound)` through the same projection.
> 2. **A short read must be an error, not a normal end of stream.** The brief's `read == 0 => Ok(None)` would serve a truncated block silently; since the loop already returns `Ok(None)` when `remaining == 0`, a zero read with bytes still owed is always anomalous. It is now `io::ErrorKind::UnexpectedEof`.
> 3. **`meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)` is not acceptable.** A 1970 timestamp is the one default direction that loses data — the sweep's grace window reads `Entry.published`, so an unreadable mtime would classify a live block as maximally old and hand it to the deleter. It propagates with `?` instead: a sweep that aborts retries, a sweep that deletes referenced blocks does not.
> 4. `list` gained a per-directory sort because `read_dir` order is unspecified — without it a fan-out directory holding more than one block emits addresses out of order and the cursor walk then **skips blocks between pages**, which is the path the sweep deletes along.

**Files:**
- Modify: `crates/tinio-blob/src/local.rs`

**Interfaces:**
- Consumes: Task 4's type; `_core::blob::Entry`; `_core::storage::BodyStream`; `_core::ByteRange`.
- Produces: the remaining `BlockStore` methods. `open` serves the whole block or a resolved range; `list` walks `<root>/<aa>/` directories in ascending address order and pages.

- [x] **Step 1: Write the failing test**

Append to the `tests` module in `crates/tinio-blob/src/local.rs`:

```rust
    async fn read_all(mut body: BodyStream) -> Vec<u8> {
        use futures::StreamExt;
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn opening_a_block_serves_it_whole_or_by_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store.publish(staged(&store, b"hello").await, &hash).await.unwrap();

        assert_eq!(read_all(store.open(&hash, None).await.unwrap()).await, b"hello");
        let one = store.open(&hash, Some(ByteRange::Inclusive(0, 0))).await.unwrap();
        assert_eq!(read_all(one).await, b"h", "bytes=0-0 is one byte, not zero");
        let all = store.open(&hash, Some(ByteRange::From(0))).await.unwrap();
        assert_eq!(read_all(all).await, b"hello", "a range covering the block serves it whole");
        assert!(
            store.open(&hash, Some(ByteRange::From(5))).await.is_err(),
            "a start past the end is InvalidRange"
        );
    }

    #[tokio::test]
    async fn opening_a_missing_block_is_not_found_and_never_a_zero_length_body() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let err = store.open(&BlockHash::of(b"gone"), None).await.unwrap_err();
        let Error::Io(io) = err else { panic!("expected an I/O error, got {err:?}") };
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn list_pages_in_address_order_and_skips_the_temp_directory() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let mut hashes = Vec::new();
        for bytes in [b"a".as_slice(), b"b", b"c"] {
            let hash = BlockHash::of(bytes);
            store.publish(staged(&store, bytes).await, &hash).await.unwrap();
            hashes.push(hash);
        }
        let _temp = staged(&store, b"pending").await;

        let first = store.list(None, 2).await.unwrap();
        assert_eq!(first.len(), 2, "the page is capped at the limit");
        let second = store.list(Some(&first[1].hash), 2).await.unwrap();
        assert_eq!(second.len(), 1, "the walk resumes strictly after the cursor");

        let mut all: Vec<_> = first.into_iter().chain(second).map(|entry| entry.hash).collect();
        all.sort();
        hashes.sort();
        assert_eq!(all, hashes, "every block is listed exactly once, temps excluded");
    }

    #[tokio::test]
    async fn removing_a_block_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlocks::new(dir.path());
        let hash = BlockHash::of(b"hello");
        store.publish(staged(&store, b"hello").await, &hash).await.unwrap();

        store.remove(&hash).await.unwrap();
        assert!(!store.contains(&hash).await.unwrap());
        store.remove(&hash).await.unwrap();
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib local`
Expected: FAIL — `no method named open` (and the same for `list`, `remove`).

- [x] **Step 3: Write minimal implementation**

Add to the `impl BlockStore for LocalBlocks` block in `crates/tinio-blob/src/local.rs`:

```rust
    async fn open(
        &self,
        hash: &BlockHash,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Self::Error> {
        let path = self.block_path(hash);
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(Error::Missing);
            }
            Err(err) => return Err(Error::Io(err)),
        };
        let len = file.metadata().await?.len();
        let (mut file, take) = match range {
            Some(range) => {
                let (start, end) = range.resolve(len).map_err(Error::from)?;
                file.seek(std::io::SeekFrom::Start(start)).await?;
                (file, end - start + 1)
            }
            None => (file, len),
        };
        let stream = futures::stream::try_unfold((file, take), |(mut file, remaining)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let want = remaining.min(64 * 1024) as usize;
            let mut buf = vec![0u8; want];
            let read = file.read(&mut buf).await.map_err(storage::io)?;
            if read == 0 {
                return Ok(None);
            }
            buf.truncate(read);
            Ok(Some((bytes::Bytes::from(buf), (file, remaining - read as u64))))
        });
        Ok(Box::pin(stream))
    }

    async fn remove(&self, hash: &BlockHash) -> Result<(), Self::Error> {
        match tokio::fs::remove_file(self.block_path(hash)).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(Error::Io(err)),
        }
    }

    async fn list(&self, after: Option<&BlockHash>, limit: usize) -> Result<Vec<Entry>, Self::Error> {
        let mut out = Vec::new();
        let mut dirs = match tokio::fs::read_dir(&self.root).await {
            Ok(dirs) => dirs,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(out),
            Err(err) => return Err(Error::Io(err)),
        };
        let mut prefixes = Vec::new();
        while let Some(entry) = dirs.next_entry().await? {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                prefixes.push(name.into_owned());
            }
        }
        prefixes.sort();
        for prefix in prefixes {
            let mut entries = tokio::fs::read_dir(self.root.join(&prefix)).await?;
            while let Some(entry) = entries.next_entry().await? {
                let name = entry.file_name().to_string_lossy().into_owned();
                let Ok(hash) = BlockHash::new(&name) else { continue };
                if after.is_some_and(|after| hash <= *after) {
                    continue;
                }
                let meta = entry.metadata().await?;
                out.push(Entry {
                    hash,
                    len: meta.len(),
                    published: meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                });
                if out.len() == limit {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }
```

Add `impl From<storage::Error> for Error` with a pass-through arm (`Error::Contract(e) => e` — add a `Contract(#[from] storage::Error)` variant to the error enum so `range.resolve(...).map_err(Error::from)` compiles), and update Task 3's projection to unwrap it:

```rust
    /// The contract failed (a range that cannot be satisfied, and the like).
    #[error(transparent)]
    Contract(#[from] storage::Error),
```

with the projection arm `Error::Contract(e) => e` placed first in `impl From<Error> for storage::Error`.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib local`
Expected: PASS — seven tests green.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob/src/local.rs crates/tinio-blob/src/error.rs
git commit -m "feat(blob): read, remove and page blocks on the local filesystem"
```

---

### Task 6: The engine — `Store::put`

**Files:**
- Modify: `crates/tinio-blob/src/engine.rs`
- Modify: `crates/tinio-blob/src/error.rs` (the `store` constructor)

**Interfaces:**
- Consumes: `_core::blob::{BlockStore, BlobRef, StagedBlock}`, `_core::storage::BodyStream`, `futures::StreamExt`, `sha2::{Digest, Sha256}`.
- Produces: `Store::new(blocks: S) -> Self`, `Store::blocks(&self) -> &S`, `Store::put(&self, body: BodyStream) -> Result<BlobRef, Error>`; store errors project through `Error::Storage(err.into())` (the `Error::store` constructor this step drafted was deleted by the header's consolidated ruling).

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-blob/src/engine.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::_core::{
        blob::{BlockHash, BlockRef, BlockStore},
        storage::BodyStream,
        ByteRange,
    };
    use bytes::Bytes;
    use futures::StreamExt;

    fn body(bytes: &'static [u8]) -> BodyStream {
        Box::pin(futures::stream::once(async move { Ok(Bytes::from_static(bytes)) }))
    }

    #[tokio::test]
    async fn putting_a_blob_stores_it_under_its_content_hash() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));

        let blob = store.put(body(b"hello")).await.unwrap();

        assert_eq!(blob.hash(), &BlockHash::of(b"hello"));
        assert_eq!(blob.len(), 5);
        assert_eq!(blob.blocks().len(), 1, "version 1 stores one block per blob");
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
        assert_eq!(blob.blocks(), [BlockRef { hash: BlockHash::of(b""), len: 0 }]);
        assert!(store.blocks().contains(blob.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_body_that_fails_mid_stream_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let failing: BodyStream = Box::pin(futures::stream::iter(vec![
            Ok(Bytes::from_static(b"half")),
            Err(std::io::Error::other("the source died")),
        ]));

        assert!(store.put(failing).await.is_err());
        assert!(
            store.blocks().list(None, 10).await.unwrap().is_empty(),
            "nothing may become visible under a hash from a failed stream"
        );
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib engine`
Expected: FAIL — `cannot find type Store`.

- [x] **Step 3: Write minimal implementation**

Prepend to `crates/tinio-blob/src/engine.rs`:

```rust
//! The engine: every block-store policy lives here.
//!
//! [`Store`] is generic over [`BlockStore`]: it hashes what it writes, skips
//! bytes whose address is already present, returns a [`BlobRef`] describing
//! where the content lives, and sweeps addresses no live reference points at.

use futures::StreamExt;
use sha2::{Digest, Sha256};

use crate::{
    _core::{
        blob::{BlockHash, BlobRef, BlockStore, StagedBlock},
        storage::BodyStream,
        ByteRange,
    },
    Error, LocalBlocks,
};

/// A blob store over a [`BlockStore`].
///
/// # Examples
///
/// ```rust,no_run
/// use tinio_blob::{LocalBlocks, Store};
///
/// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
/// # let _ = store;
/// ```
#[derive(Debug, Clone)]
pub struct Store<S> {
    blocks: S,
}

impl<S: BlockStore> Store<S> {
    /// Serve blobs over `blocks`.
    pub fn new(blocks: S) -> Self {
        Self { blocks }
    }

    /// The underlying block store.
    pub fn blocks(&self) -> &S {
        &self.blocks
    }

    /// Store `body` as a blob: hash it while staging, publish only when the
    /// address is new, and return the blob's reference.
    ///
    /// A stream that fails mid-way publishes nothing: the staged bytes are
    /// dropped and the failure surfaces as the caller's error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use bytes::Bytes;
    /// use tinio_blob::{LocalBlocks, Store};
    /// use tinio_core::BodyStream;
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
    /// let body: BodyStream =
    ///     Box::pin(futures::stream::once(async { Ok(Bytes::from_static(b"hello")) }));
    /// let blob = store.put(body).await?;
    /// assert_eq!(blob.len(), 5);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn put(&self, mut body: BodyStream) -> Result<BlobRef, Error> {
        let mut staged = self.blocks.stage().await.map_err(Error::store)?;
        let mut hasher = Sha256::new();
        let mut len = 0u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(Error::Io)?;
            hasher.update(&chunk);
            len += chunk.len() as u64;
            staged.write(chunk).await.map_err(Error::Io)?;
        }
        let hash = BlockHash::from_digest(hasher.finalize().into());
        self.blocks
            .publish(staged, &hash)
            .await
            .map_err(Error::store)?;
        Ok(BlobRef::single(hash, len))
    }
}

#[cfg(test)]
mod tests { /* as written in Step 1 */ }
```

`Store::new(LocalBlocks::new(...))` in the tests needs `use crate::LocalBlocks;` — the `pub use` in the file above covers it, but the tests also use `tempfile` (a dev-dependency).

Add to `crates/tinio-blob/src/error.rs`:

```rust
impl Error {
    /// Box a caller-side store error, keeping its message and source chain.
    pub(crate) fn store(err: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Store(Box::new(err))
    }
}
```

The `Contract` variant added in Task 5 is what the local implementation maps `range.resolve(...)`'s error through; the staged sink and the body stream both fail as I/O and land in `Error::Io`.

One consequence of content addressing is worth stating where the dedup is written: a repeat `put` still **writes** its bytes to a stage before it can learn the address (the hash is only known once the content has been read), so the dedup saves the publish, the disk space and the second copy — not the write. That is inherent to storing by content hash, and it is why `put` is one streaming pass rather than a `contains` probe followed by a write.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib engine`
Expected: PASS — four tests green.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob/src/engine.rs crates/tinio-blob/src/error.rs
git commit -m "feat(blob): store blobs by content hash with a dedup lookup"
```

---

### Task 7: The engine — `Store::open`

**Files:**
- Modify: `crates/tinio-blob/src/engine.rs`

**Interfaces:**
- Consumes: Tasks 1–6.
- Produces: `Store::open(&self, blob: &BlobRef, range: Option<ByteRange>) -> Result<BodyStream, Error>` — the range is resolved by prefix-summing the block list and stitching the intersecting blocks' sub-ranges (one block in v1, so the loop runs once), the shape the prior art uses (Garage's `body_from_blocks_range`, `docs/superpowers/versioning-storage-prior-art-research.md` is the survey; this plan's own ranged-read study is recorded in the operator's 2026-10-03 ruling).

- [x] **Step 1: Write the failing test**

Append to the `tests` module in `crates/tinio-blob/src/engine.rs`:

```rust
    async fn read_all(mut body: BodyStream) -> Vec<u8> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn opening_a_blob_serves_its_bytes_and_a_range_within_them() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"hello")).await.unwrap();

        assert_eq!(read_all(store.open(&blob, None).await.unwrap()).await, b"hello");
        let range = Some(ByteRange::Inclusive(1, 3));
        assert_eq!(read_all(store.open(&blob, range).await.unwrap()).await, b"ell");
    }

    #[tokio::test]
    async fn opening_an_empty_blob_serves_an_empty_stream() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"")).await.unwrap();

        assert!(read_all(store.open(&blob, None).await.unwrap()).await.is_empty());
    }

    #[tokio::test]
    async fn opening_a_blob_whose_block_is_gone_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let blob = store.put(body(b"hello")).await.unwrap();

        store.blocks().remove(blob.hash()).await.unwrap();

        let err = store.open(&blob, None).await.unwrap_err();
        assert!(matches!(err, Error::Missing), "got {err:?}");
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
                BlockRef { hash: *first.hash(), len: 3 },
                BlockRef { hash: *second.hash(), len: 2 },
            ],
        )
        .unwrap();

        assert_eq!(read_all(store.open(&blob, None).await.unwrap()).await, b"hello");
        let range = Some(ByteRange::Inclusive(1, 3));
        assert_eq!(
            read_all(store.open(&blob, range).await.unwrap()).await,
            b"ell",
            "the range crosses the block boundary, so both blocks contribute"
        );
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib engine`
Expected: FAIL — `no method named open`.

- [x] **Step 3: Write minimal implementation**

Add to `impl<S: BlockStore> Store<S>` in `crates/tinio-blob/src/engine.rs`:

```rust
    /// Open `blob` — or a byte range within it — as a stream.
    ///
    /// The range is resolved against the block list by prefix-summing each
    /// block's length: only the blocks that intersect it are opened, each with
    /// the sub-range it contributes. Version 1 stores one block per blob, so
    /// the loop runs once — it exists so a later chunking policy changes
    /// writers, not this path.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use tinio_blob::{LocalBlocks, Store};
    /// use tinio_core::blob::{BlockHash, BlobRef};
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
    /// let blob = BlobRef::single(BlockHash::of(b"hello"), 5);
    /// let body = store.open(&blob, None).await?;
    /// # let _ = body;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open(
        &self,
        blob: &BlobRef,
        range: Option<ByteRange>,
    ) -> Result<BodyStream, Error> {
        if blob.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let (begin, end) = match range {
            Some(range) => range.resolve(blob.len())?,
            None => (0, blob.len() - 1),
        };
        let mut slices = Vec::new();
        let mut offset = 0u64;
        for block in blob.blocks() {
            if offset > end {
                break;
            }
            if offset + block.len > begin {
                let start = begin.saturating_sub(offset);
                let stop = block.len.min(end - offset + 1);
                slices.push((block.hash, ByteRange::Inclusive(start, stop - 1)));
            }
            offset += block.len;
        }
        let mut parts = Vec::with_capacity(slices.len());
        for (hash, range) in slices {
            parts.push(self.blocks.open(&hash, Some(range)).await.map_err(Error::store)?);
        }
        Ok(Box::pin(futures::stream::iter(parts).flatten()))
    }
```

The intersecting blocks are opened eagerly (one `open` per block, before the first byte streams); with one block per blob that is one open, and a later chunking policy that makes the count large is the point at which pacing them (Garage streams through a channel) becomes worth doing — recorded here so the choice is visible rather than implicit.

**Superseded at execution (2026-10-03).** `Error::Missing` is no longer produced: Task 5's `LocalBlocks::open` returns `Error::Io(io)` with `io.kind() == NotFound` directly (its brief's own code block and test contradicted each other, and the test — matching the trait's documented contract — won). So the engine must **not** pass a store error through by boxing it: `Error::store` would project a `NotFound` to an `io::Error` of kind `Other` and lose it. Instead map each block-store error through the contract projection — `.map_err(|err| Error::Contract(err.into()))?` — and assert the `NotFound` kind through that projection in the test. `Error::store` stays for `put` and `sweep`: a missing block is a contract condition, not a backend failure. **2026-10-06 (review round).** The names moved with the consolidated ruling: the projection target is `Error::Storage` (not `Contract`), and `Error::Store`/`Error::store` no longer exist — `put` and `sweep` project through `Error::Storage(err.into())` as well, so the "stays for `put` and `sweep`" clause is superseded.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib engine`
Expected: PASS — eight tests green.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob/src/engine.rs
git commit -m "feat(blob): open blobs and ranges from the engine"
```

---

### Task 8: The engine — the live-set sweep

**Files:**
- Create: `crates/tinio-blob/src/sweep.rs` (owns `LiveSet`, `SweepReport` and `Store::sweep`)
- Modify: `crates/tinio-blob/src/engine.rs` (its Task 3 skeleton drops the `LiveSet` re-export line; `lib.rs` re-exports `sweep::{LiveSet, SweepReport}` instead)

**Interfaces:**
- Consumes: `_core::blob::{BlockHash, BlockStore}`, `std::time::{Duration, SystemTime}`, `futures`.
- Produces: `LiveSet::from_hashes(impl IntoIterator<Item = BlockHash>) -> Self`, `LiveSet::contains(&self, hash: &BlockHash) -> bool`, `LiveSet::len(&self) -> usize`; `SweepReport { scanned: u64, kept_live: u64, kept_young: u64, removed: u64, bytes: u64 }` (all `pub`, `Default`); `Store::sweep(&self, live: &LiveSet, min_age: Duration, now: SystemTime) -> Result<SweepReport, Error>`.

- [x] **Step 1: Write the failing test**

Append to `crates/tinio-blob/src/sweep.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{blob::BlockStore, storage::BodyStream},
        LocalBlocks, Store,
    };
    use bytes::Bytes;

    fn body(bytes: &'static [u8]) -> BodyStream {
        Box::pin(futures::stream::once(async move { Ok(Bytes::from_static(bytes)) }))
    }

    #[tokio::test]
    async fn a_sweep_removes_unreferenced_blocks_once_they_are_past_the_grace() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let keep = store.put(body(b"keep")).await.unwrap();
        let drop = store.put(body(b"drop")).await.unwrap();

        let live = LiveSet::from_hashes([*keep.hash()]);
        let now = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let report = store
            .sweep(&live, std::time::Duration::from_secs(600), now)
            .await
            .unwrap();

        assert_eq!(report.removed, 1);
        assert_eq!(report.bytes, 4);
        assert!(store.blocks().contains(keep.hash()).await.unwrap());
        assert!(!store.blocks().contains(drop.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_block_published_inside_the_grace_window_survives_a_sweep_that_does_not_know_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        let fresh = store.put(body(b"fresh")).await.unwrap();

        let empty = LiveSet::from_hashes([]);
        let report = store
            .sweep(&empty, std::time::Duration::from_secs(600), std::time::SystemTime::now())
            .await
            .unwrap();

        assert_eq!(report.removed, 0, "a live set computed before the publish must not orphan it");
        assert_eq!(report.kept_young, 1);
        assert!(store.blocks().contains(fresh.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn a_sweep_pages_through_more_blocks_than_one_page_holds() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::new(LocalBlocks::new(dir.path()));
        for bytes in [b"a".as_slice(), b"b", b"c", b"d", b"e"] {
            store.put(body(bytes)).await.unwrap();
        }

        let live = LiveSet::from_hashes([]);
        let now = std::time::SystemTime::now() + std::time::Duration::from_secs(3600);
        let report = store
            .sweep(&live, std::time::Duration::from_secs(600), now)
            .await
            .unwrap();

        assert_eq!(report.scanned, 5);
        assert_eq!(report.removed, 5);
        assert!(store.blocks().list(None, 10).await.unwrap().is_empty());
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --lib sweep`
Expected: FAIL — `cannot find type LiveSet`.

- [x] **Step 3: Write minimal implementation**

`crates/tinio-blob/src/sweep.rs`:

```rust
//! The live-set sweep: reclaim addresses no reference points at.

use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use futures::StreamExt;

use crate::{
    _core::blob::{BlockHash, BlockStore},
    Error,
};

/// A page size for the store walk: bounded memory, and a cursor the walk
/// resumes from, so a store larger than memory still sweeps.
const PAGE: usize = 512;

/// The blocks a sweep must keep: the union of the block lists of every live
/// version. Built by the caller — the engine does not know what a version is.
///
/// # Examples
///
/// ```rust
/// use tinio_blob::LiveSet;
/// use tinio_core::blob::BlockHash;
///
/// let live = LiveSet::from_hashes([BlockHash::of(b"hello")]);
/// assert!(live.contains(&BlockHash::of(b"hello")));
/// assert_eq!(live.len(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct LiveSet {
    hashes: HashSet<BlockHash>,
}

impl LiveSet {
    /// Build the set from the live references' block lists.
    pub fn from_hashes(hashes: impl IntoIterator<Item = BlockHash>) -> Self {
        Self { hashes: hashes.into_iter().collect() }
    }

    /// Whether the sweep must keep this block.
    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.hashes.contains(hash)
    }

    /// How many blocks the set holds.
    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }
}

/// What a sweep did.
///
/// # Examples
///
/// ```rust
/// use tinio_blob::SweepReport;
///
/// let report = SweepReport::default();
/// assert_eq!(report.removed, 0);
/// assert_eq!(report.bytes, 0);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Blocks examined.
    pub scanned: u64,
    /// Blocks kept because a live reference points at them.
    pub kept_live: u64,
    /// Blocks kept because they are younger than the grace.
    pub kept_young: u64,
    /// Blocks removed.
    pub removed: u64,
    /// Bytes removed.
    pub bytes: u64,
}

impl<S: BlockStore> super::Store<S> {
    /// Remove every block that no live reference points at and that was
    /// published at least `min_age` before `now`.
    ///
    /// The grace is what makes the sweep safe against a publish that happened
    /// after the caller computed `live`: a block written moments ago is not yet
    /// referenced, and deleting it would drop content that a version row is
    /// about to name.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::time::{Duration, SystemTime};
    ///
    /// use tinio_blob::{LiveSet, LocalBlocks, Store};
    ///
    /// # async fn example() -> Result<(), tinio_blob::Error> {
    /// let store = Store::new(LocalBlocks::new("/var/lib/tinio/blobs/demo"));
    /// let report = store
    ///     .sweep(&LiveSet::default(), Duration::from_secs(600), SystemTime::now())
    ///     .await?;
    /// assert_eq!(report.kept_young, 0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sweep(&self, live: &LiveSet, min_age: Duration, now: SystemTime) -> Result<SweepReport, Error> {
        let mut report = SweepReport::default();
        let mut cursor = None;
        loop {
            let page = self
                .blocks()
                .list(cursor.as_ref(), PAGE)
                .await
                .map_err(Error::store)?;
            let Some(last) = page.last().map(|entry| entry.hash) else {
                return Ok(report);
            };
            for entry in page {
                report.scanned += 1;
                if live.contains(&entry.hash) {
                    report.kept_live += 1;
                    continue;
                }
                let young = now
                    .duration_since(entry.published)
                    .map(|age| age < min_age)
                    .unwrap_or(true);
                if young {
                    report.kept_young += 1;
                    continue;
                }
                self.blocks().remove(&entry.hash).await.map_err(Error::store)?;
                report.removed += 1;
                report.bytes += entry.len;
            }
            cursor = Some(last);
        }
    }
}
```

The `futures::StreamExt` import is unused here — drop it. `LiveSet` and `SweepReport` both live in `sweep.rs`: there is no `live` module anywhere in the crate, and Task 6's `engine.rs` sketch carries no `LiveSet` line of its own, so `lib.rs`'s `pub use` block gains `sweep::{LiveSet, SweepReport}` here — on top of Task 3's `engine::Store`, `error::Error` and `local::LocalBlocks` — which is exactly the state the Files bullet above records.

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --lib`
Expected: PASS — the whole crate's lib tests, sweep included.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob/src/sweep.rs crates/tinio-blob/src/lib.rs crates/tinio-blob/src/engine.rs
git commit -m "feat(blob): sweep blocks outside the live set, with a grace"
```

---

### Task 9: The conformance suite, and the crate's gates

**Files:**
- Create: `crates/tinio-blob/src/testing.rs`
- Create: `crates/tinio-blob/tests/conformance.rs`
- Modify: `crates/tinio-blob/src/lib.rs` (`#[cfg(feature = "testing")] pub mod testing;`)
- Modify: `specs/001-s3-local-server/tasks.md` (append the plan's row, T149)

**Interfaces:**
- Consumes: every type above.
- Produces: `tinio_blob::testing::assert_block_store(store: &impl BlockStore)` — the suite a future WebDAV/FTP implementation runs to prove it meets the contract, and the reason the contract exists at all (spec: "equivalent capability").

- [x] **Step 1: Write the failing test**

`crates/tinio-blob/tests/conformance.rs`:

```rust
//! The block-store contract, proven against the local implementation.
//!
//! This target is the template a future backend copies: point it at another
//! `BlockStore` and the same rules are asserted.

use tinio_blob::{LocalBlocks, testing};

#[tokio::test]
async fn the_local_block_store_meets_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    testing::assert_block_store(&LocalBlocks::new(dir.path())).await;
}

#[test]
fn the_public_types_cross_threads() {
    tinio_util::testing::assert_send_sync::<tinio_blob::Error>();
    tinio_util::testing::assert_send_sync::<tinio_blob::LocalBlocks>();
    tinio_util::testing::assert_send_sync::<tinio_blob::Store<tinio_blob::LocalBlocks>>();
}
```

The second test is what makes the `tinio-util` dev-dependency (Task 3's manifest) load-bearing: the contract's types are handed to `spawn_blocking` and held across `await` points by plan 2, so "every public type is `Send + Sync`" is a rule worth asserting rather than assuming — the workspace already does this for `ETag` and `Key` (`crates/tinio-core/src/etag.rs:169`, `bucket.rs:136`).

`tempfile` is in `[dev-dependencies]`; `tokio` needs `macros` + `rt` in the dev edge (already listed in Task 3's manifest).

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-blob --test conformance`
Expected: FAIL — `unresolved import tinio_blob::testing` (the feature is off by default; the target must be run with it on, or the module must exist).

Since the suite is behind a non-default feature, the target needs it: add to `crates/tinio-blob/Cargo.toml`

```toml
[[test]]
name = "conformance"
required-features = ["testing"]
```

and run `cargo test -p tinio-blob --features testing --test conformance` — expected FAIL until Step 3.

- [x] **Step 3: Write the suite**

`crates/tinio-blob/src/testing.rs`:

```rust
//! The block-store conformance suite.
//!
//! Behind the non-default `testing` feature (`docs/tests.md`): a `tests/`
//! target cannot cross into `src/`, so a suite a target must share has to be
//! library code behind a feature.

use bytes::Bytes;

use crate::_core::{
    blob::{BlockHash, BlockStore, Published, StagedBlock},
    storage, BodyStream, ByteRange,
};

/// Assert the rules every [`BlockStore`] must satisfy.
///
/// # Panics
///
/// Panics with a rule-naming message when the store breaks one.
pub async fn assert_block_store<S: BlockStore>(store: &S) {
    let hash = BlockHash::of(b"conformance");
    assert!(!store.contains(&hash).await.expect("contains"), "a fresh store holds nothing");

    let mut staged = store.stage().await.expect("stage");
    for chunk in [b"conf".as_slice(), b"orm", b"ance"] {
        staged.write(Bytes::copy_from_slice(chunk)).await.expect("write");
    }
    assert_eq!(
        store.publish(staged, &hash).await.expect("publish"),
        Published::Stored,
        "the first publish stores"
    );
    assert!(store.contains(&hash).await.expect("contains"), "a published block is present");

    let mut staged = store.stage().await.expect("stage");
    staged.write(Bytes::from_static(b"conformance")).await.expect("write");
    assert_eq!(
        store.publish(staged, &hash).await.expect("publish"),
        Published::Existing,
        "a repeated publish is a dedup hit"
    );

    let body = storage::collect_body(store.open(&hash, None).await.expect("open"))
        .await
        .expect("read");
    assert_eq!(body, b"conformance", "a block reads back byte for byte");

    let range = Some(ByteRange::Inclusive(0, 3));
    let body = storage::collect_body(store.open(&hash, range).await.expect("open"))
        .await
        .expect("read");
    assert_eq!(body, b"conf", "a range is resolved within the block");

    let listed = store.list(None, 10).await.expect("list");
    assert_eq!(listed.len(), 1, "the store lists exactly what it holds");
    assert_eq!(listed[0].hash, hash);
    assert_eq!(listed[0].len, 11);

    store.remove(&hash).await.expect("remove");
    assert!(!store.contains(&hash).await.expect("contains"), "a removed block is gone");
    store.remove(&hash).await.expect("remove is idempotent");
    assert!(store.open(&hash, None).await.is_err(), "opening a missing block is an error");
}
```

`crates/tinio-blob/src/lib.rs` gains:

```rust
#[cfg(feature = "testing")]
pub mod testing;
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-blob --features testing --test conformance`
Expected: PASS — one test green; and `cargo test -p tinio-blob` (feature off) still compiles the target out entirely.

- [x] **Step 5: Sweep the crate for public items without an example**

Every public item this crate defines carries a doc comment **and** a runnable `# Examples` block (Global Constraints; the constitution's exempt list is closed and contains no accessor). The earlier tasks wrote examples for the substantial items; this step is the sweep for the rest — read `local.rs`, `engine.rs`, `sweep.rs` and `error.rs` for public items with none (`LocalBlocks::{new, block_path, temp_dir}`, `Store::{new, blocks}`, `LiveSet::{from_hashes, contains, len, is_empty}`, and the trait/type declarations themselves) and add a two-to-four-line example to each, in the shape the earlier ones use. `cargo test -p tinio-blob --doc` proves they compile; a *missing* example is what this step catches, because the doc gate can only see the examples that exist.

- [x] **Step 6: Run the crate's full gates**

Run each, expecting success:
- `cargo fmt --all -- --check`
- `cargo clippy -p tinio-blob --all-targets --all-features -- -D warnings`
- `cargo test -p tinio-blob --all-features`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- `cargo hack check --package tinio-blob --each-feature`

- [x] **Step 7: Append the issue-tracker row**

Append to `specs/001-s3-local-server/tasks.md`, in the shape its existing addenda use:

```markdown
### Addendum 2026-10-03 — S3 versioning, the block engine (design `docs/superpowers/specs/2026-10-03-s3-versioning-design.md`, plan `docs/superpowers/plans/2026-10-03-s3-versioning-p1-block-engine.md`)

Incremental work under the versioning design's plan set; this addendum is its first of three plans. It adds the `tinio-core::blob` contract and the `tinio-blob` crate (cargo feature `versioning`) and changes no S3 surface — plans 2 and 3 own those.

- [X] T149 Implement the `tinio-core::blob` contract and the `tinio-blob` block engine (plan 1, Tasks 1-9)
```

- [ ] **Step 8: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-blob specs/001-s3-local-server/tasks.md Cargo.toml Cargo.lock
git commit -m "feat(blob): prove the block-store contract with a conformance suite"
```

---

## Plan-set ownership

Per `docs/superpowers/README.md`'s one-definer rule, across the three plans of this feature:

- **This plan owns**: the `tinio-core::blob` surface (every type and trait signature above), the `tinio-blob` crate's public surface, the on-disk block layout, and the sweep's grace semantics. Plans 2 and 3 refer to these by the names in **Interfaces** and never restate them.
- **Plan 2 will own**: the `tinio-store` version tables and row encodings, the fs `version/` state machine (supersede, materialize, delete markers, null versions), the retention keys' arithmetic, and the `tinio-mem` parity.
- **Plan 3 will own**: the s3s operation surface and its headers/errors, the capability gating rows, the config keys, the FR-023 read-only amendment, and the cucumber journeys.
- **The spec owns**: the storage model, the version state machine, and the three-level gating model — the plans implement it and do not restate it.

## Self-Review notes (for the executor)

- The plan's first task creates a file whose later steps extend it; run the tasks in order, and do not skip Task 8's re-export move (`LiveSet`/`SweepReport` from `sweep.rs`) when its Step 3 calls for it.
- Every public item in this plan carries its `# Examples` block because the constitution requires one (constitution III, `.specify/memory/constitution.md`); the doc gate in Task 9's Step 6 (`RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `docs/tests.md:33`) proves a *present* example compiles and that its links resolve, but it cannot see one that is absent — catching a missing example is the reviewer's job, not CI's, which is why Step 5 sweeps for them.
- **2026-10-06 (review round).** The erased `Error::Store(Box<dyn …>)` this note recorded was deleted by the header's consolidated ruling: every store error projects into `Error::Storage(storage::Error)` through the `BlockStore::Error: Into<storage::Error>` bound, so the crate now has no deliberate erasure and no boxing on any path.
