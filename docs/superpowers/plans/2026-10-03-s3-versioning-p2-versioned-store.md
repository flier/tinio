# S3 versioning, plan 2 — the versioned store: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Version semantics inside the storage layer — the `tinio-core` version domain types and the `VersionOps` contract, the `tinio-store` version tables, the shared `tinio-version` engine (supersede, materialize, delete markers, null versions) and its `tinio-fs` adapter, retention and block reclamation, and `tinio-mem` parity — with no S3 wire behavior anywhere.

**Architecture:** A version row is `(bucket, key, seq) -> row` in a new table, indexed by `(bucket, key, version_id)` in a second, both written in the same redb transaction that moves a version out of the current slot; the current version's identity lives in the existing `object_meta` row, so the fast path (serve the file at the key path) reads one row. The state machine is the `tinio-version` crate, written against narrow ports it declares itself (`VersionRows`, `CurrentRows`, `ContentPort`) plus the shared blob engine (`tinio_blob::Store<B: BlockStore>` — no block port of its own), with a `tinio-fs` adapter — amended 2026-10-03 (`docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md`, D16/D17/D26): the engine is born decoupled so the next backend inherits it, `VersionRows` and `ContentPort` are replaced by `VersionMeta` and `ObjectContent` when that plan set lands (after plan 3), and `CurrentRows` stays — the staleness gate is the backend's read-path rule, not a contract's. `tinio-mem` implements the same contract over rows.

**Tech Stack:** Rust (edition 2024), `redb` (via `tinio-store`), `tokio`, `async-trait`, `tinio-blob` (plan 1), `uuid`.

**Spec:** `docs/superpowers/specs/2026-10-03-s3-versioning-design.md` — this plan implements its **The storage model**, **The version model**, **Metadata schema**, **Write paths**, **Read paths** (storage half), **Garbage collection and retention** and **The reference implementation (`tinio-mem`)** sections. The S3 wire surface, the capability gating and the read-only amendment are plan 3's.

**Depends on:** plan 1 (`docs/superpowers/plans/2026-10-03-s3-versioning-p1-block-engine.md`) — `tinio_core::blob` and `tinio_blob::{Store, LocalBlocks, LiveSet}` must exist with the signatures that plan pins. **Amendment 2026-10-03 (engine home)**: the state machine is the `tinio-version` crate (D16), not a `tinio-fs` module, and it depends only on the narrow ports it declares (D17); the persistence contracts that later replace those ports are `docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md`. **Amendment 2026-10-03 (plan 1's contract changed after it was executed)**: plan 1's whole-branch review and a four-angle quality pass produced operator-ruled changes worth reading before this plan starts — `BlockStore` gained a **required** `walk(after) -> Result<EntryStream<Self::Error>, Self::Error>` with a new `tinio_core::blob::EntryStream<E>` alias, because paging through `list` restarts from the root and made the sweep Θ(N²) (at 10⁶ blocks, ~7.6 name-parses per block); `list` remains and is now built on the walk. `Error::Missing`, `Error::Store` and `Error::store` were **deleted** — every store error now projects through the existing `Error::Contract(err.into())`, since `BlockStore::Error` is already bound `Into<storage::Error>`. `BlobRef::new` now **rejects a zero-length block in a multi-block list**. And `tinio-blob`'s `versioning` feature and crate-root `#![cfg]` were **removed** by operator ruling (see the feature note below). Consequences for this plan: any `BlockStore` test double written here must implement `walk`, the row codec's invariant re-check must include the zero-length rule, and no code may match on `Error::Missing` or `Error::Store`.

## Global Constraints

- English only in repo artifacts; no hard-wrapped lines in prose (`docs/compress.md`). Every public item carries a runnable `rustdoc` example (constitution III); the gate is `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`.
- Unit tests inline (`#[cfg(test)] mod tests`); conformance legs go in `tinio-util`'s harness, whose suite both backends run (`crates/tinio-util/src/testing.rs:221`).
- Per-crate `Error` with `impl From<Error> for tinio_core::storage::Error`; backends call the contract error's helpers (`storage::io(…)`, `storage::no_such_key(…)`), never build variants.
- redb schema: `FORMAT_VERSION` stays `1` and additive tables need no bump, but **changing the `object_meta` value tuple is a row-format change** — an older database errors at row decode and its remedy is delete-and-rebuild (`crates/tinio-store/src/state.rs:12-20`, the 2026-09-02 ruling).
- Every transaction that touches more than one table does so in **one** `handle.write` closure — redb is single-writer and the repo's atomicity relies on it (`crates/tinio-fs/src/backend/mod.rs:783` is the template).
- Streaming discipline (constitution V): supersede and materialize stream; neither buffers an object whole.
- Git: every commit step asks the operator first (`docs/git.md`); commit messages carry no AI/agent trailers.
- Gate commands: `cargo test -p tinio-fs -p tinio-mem -p tinio-store -p tinio-core -p tinio-util`, `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`, `cargo hack check --package tinio-fs --each-feature`, `cargo fmt`, `cargo clippy`.

## Review Focus

1. **A crash between the version row and the rename.** The row is committed before the key path is overwritten; a crash in that window leaves the old file *and* a row naming the same content. Recovery **adopts** the row — its content is real (the file still at the key path) and it is the version that was current — so the next write must not double-count it (the row already carries the outgoing identity, so a re-supersede lands on the same row), and a read must not serve two "latest" versions.
2. **A suspended-bucket write replacing an existing null version.** It overwrites in place: no version row, no blob, no sequence bump — a store that creates a row here makes AWS's documented behavior wrong in the one case clients test most.
3. **An out-of-band edit between two API writes.** The staleness gate must degrade the current version's identity to `null` and let history keep serving — the transition where the mirror and version contracts meet.
4. **The last version of a key being deleted.** With no version rows left, the key must disappear from the tree and from listings, and the current-version columns must be cleared — a leftover row makes a deleted object reappear.
5. **A retention sweep racing a write.** The sweep expires a noncurrent version while a PUT is superseding the current one: the block both operations touch is shared, and the live set the sweep computed may be one publish stale. **The grace window is NOT the guard here, and plan 2 must not treat it as one** — corrected 2026-10-03 by plan 1's final review, which caught the premise this item was drafted on. The grace protects only a block whose *file* is young, and `LocalBlocks::publish` deliberately does **not** refresh the target's mtime on a dedup hit (`crates/tinio-blob/src/local.rs:159-165`). So a PUT that re-uploads content whose block already exists returns `Published::Existing` against an **old** mtime, and the sweep — whose live set predates that PUT's row — deletes a block the row is about to name. Silent, unrecoverable loss of that version, in exactly the shared-block case this item describes. The guard the design already names is the one to implement: **re-check under the bucket mutation lock, or a mark epoch**. Optional hardening that is *not* a substitute: refreshing the mtime on `Published::Existing` narrows the window for a sweep that has not yet reached the entry, but a sweep that has already read the page is unaffected — do not let it become the story.
6. **A block whose stored bytes are shorter than the row says (the truncation gap — this item needs an owning task, and as of 2026-10-03 nothing in the plan set owns it).** Plan 1's final review found that `LocalBlocks::open` clamps a requested range against the *file's actual length*, so a block truncated **before** the open is served as a short body with no error: `Store::open` derives each sub-range from `BlockRef.len` (the row's claim) while the backend resolves it against what is on disk (the truth), and nothing compares the two. The `UnexpectedEof` guard plan 1 added fires only for truncation *during* a read, not before it. Two consequences to weigh when this gets an owner: a short body against a row-derived `Content-Length` fails the request rather than serving wrong bytes (which is why this is not a plan-1 blocker), but the design's decision to trust the store on the read path and verify in `doctor` is only sound if something *does* verify. **Preferred enforcement point is the engine** — count the bytes each part yields in `Store::open` and error at end-of-stream if fewer than the slice's expected length — because that closes it for every future backend at once and costs nothing on the happy path; the alternative is per-backend in `LocalBlocks`. **Plan 3 has a stake too:** if a version read derives `Content-Length` from the row, a short block body becomes a *valid* chunked 200 rather than a failed request, which is strictly worse. Whichever task takes it must add the test that pins it.

---

### Task 1: `tinio-core` — the version domain types and the `VersionOps` contract

**Files:**
- Create: `crates/tinio-core/src/version.rs`
- Modify: `crates/tinio-core/src/lib.rs` (module + re-export)
- Modify: `crates/tinio-core/src/storage/mod.rs` (`VersionOps` submodule, `Storage` supertrait, re-exports)
- Create: `crates/tinio-core/src/storage/version.rs`
- Modify: `crates/tinio-core/src/object.rs` (`Info` gains the version identity)
- Modify: `crates/tinio-core/src/storage/object.rs` (`delete_object`'s success type)

**Interfaces:**
- Consumes: `bucket::Name`, `object::{Key, Info, Tags}`, `etag::ETag`, `acl::{Acl, OwnerId}`, `checksum`, `ByteRange`, `GetObjectResult`, `BodyStream`, `storage::{Error, ops}`.
- Produces (plan 3 and the backends depend on every name here):
  - `version::Id(String)` — unknown input via `version::id(&str) -> Result<Id, storage::Error>`; `Id::as_str()`; `Display`; `FromStr`; `From<&str>` panicking for literals; `Id::from_wire(&str) -> Option<Id>` reading the literal `null` as `None`.
  - `version::State { Unversioned, Enabled, Suspended }` with `Display` (`Unversioned` / `Enabled` / `Suspended`), `is_enabled()`, `is_suspended()`.
  - `version::Marker` — `{ id: Option<Id>, is_delete_marker: bool }`, returned by the version-creating writes and the deletes; `Marker::none()`.
  - `version::ReadResult` — `{ Object(GetObjectResult), Marker { id: Option<Id>, last_modified: SystemTime } }` — and `version::HeadResult` — `{ Object(object::Info), Marker { … } }`: a version-addressed read of a delete marker is a successful outcome carrying the marker's identity, because the wire answer (plan 3) is a `405` with headers, not a body.
  - `version::Entry` — one listed version: `{ key: object::Key, id: Option<Id>, seq: u64, is_delete_marker: bool, size: u64, last_modified: SystemTime, etag: ETag }`.
  - `version::ListParams { prefix: Option<String>, delimiter: Option<String>, key_marker: Option<String>, version_id_marker: Option<VersionMarker>, max_keys: usize }` where `VersionMarker` is a newtype over a raw string (the wire form, which may be a key's arbitrary version id).
  - `version::Listing { entries: Vec<Entry>, common_prefixes: Vec<String>, is_truncated: bool, next_key_marker: Option<String>, next_version_id_marker: Option<String> }`.
  - `VersionOps` in `storage/version.rs`, aggregated: `pub trait Storage: … + VersionOps`.
  - `object::Info.version_id: Option<version::Id>` (the identity of the returned object: the current version's id, or `None` for `null`).
  - `ObjectOps::delete_object`'s success type becomes `version::Marker` (the marker the delete created, or the version it permanently removed).

- [ ] **Step 1: Write the failing test**

Create `crates/tinio-core/src/version.rs` with the module docs and this test module only:

```rust
//! The version domain: opaque version identifiers, the three bucket states,
//! and the shapes the version operations exchange.
//!
//! A version identifier is opaque to every consumer: the storage layer mints
//! it, the wire echoes it, and **ordering comes from the row's sequence, never
//! from the identifier's shape** (the design's version-ID ruling). `None` in an
//! `Option<Id>` is the `null` version of a bucket that was unversioned when the
//! object was written, or suspended when it was.

use std::str::FromStr;

use derive_more::{AsRef, Deref, Display};

use crate::{
    object,
    storage::{self, error::Error},
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_id_is_opaque_but_never_empty_or_unbounded() {
        assert!(version_id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo").is_ok());
        assert!(version_id("null").is_ok(), "the literal is a legal wire value");
        assert!(version_id("").is_err());
        assert!(version_id(&"x".repeat(1025)).is_err(), "AWS caps the wire form at 1024 bytes");
    }

    #[test]
    fn the_null_version_reads_as_absent() {
        assert_eq!(Id::from_wire("null"), None);
        assert_eq!(Id::from_wire("abc").unwrap().as_str(), "abc");
    }

    #[test]
    fn the_three_bucket_states_round_trip_through_their_wire_names() {
        for state in [State::Unversioned, State::Enabled, State::Suspended] {
            assert_eq!(State::from_str(&state.to_string()).unwrap(), state);
        }
        assert!(State::Enabled.is_enabled() && !State::Suspended.is_enabled());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core --lib version`
Expected: FAIL — `cannot find function version_id`.

- [ ] **Step 3: Write minimal implementation**

Above the test module:

```rust
/// An opaque version identifier.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version;
///
/// let id = version::id("3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY").unwrap();
/// assert_eq!(id.as_str(), "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY");
/// assert_eq!(id.to_string(), id.as_str());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Display, Deref, AsRef)]
#[display("{}", _0)]
#[as_ref(forward)]
pub struct Id(String);

/// Validate and wrap a version identifier from an untrusted source.
pub fn version_id(id: impl Into<String>) -> Result<Id, Error> {
    let id = id.into();
    if id.is_empty() || id.len() > MAX_VERSION_ID_LEN {
        return Err(storage::invalid_version(id));
    }
    Ok(Id(id))
}

/// The wire cap on a version identifier (AWS: 1024 bytes).
pub const MAX_VERSION_ID_LEN: usize = 1024;

impl Id {
    /// The raw wire form.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Read a wire value: the literal `null` is the null version, anything
    /// else must be a valid identifier.
    pub fn from_wire(value: &str) -> Option<Id> {
        if value == "null" {
            return None;
        }
        Self::new(value).ok()
    }

    /// Checked constructor.
    pub fn new(id: &str) -> Result<Id, Error> {
        version_id(id)
    }
}

impl FromStr for Id {
    type Err = Error;

    fn from_str(id: &str) -> Result<Self, Self::Err> {
        Self::new(id)
    }
}

impl From<&str> for Id {
    /// Trusted-input convenience (panics on an invalid identifier — use
    /// [`version::id`] for untrusted input).
    fn from(id: &str) -> Self {
        version_id(id).expect("valid version id")
    }
}

/// The bucket's versioning state.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::State;
///
/// assert_eq!(State::default(), State::Unversioned);
/// assert_eq!(State::Enabled.to_string(), "Enabled");
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Display, FromStr)]
#[display("{}", "Unversioned")]
pub enum State {
    /// The default: every write replaces the previous object.
    #[default]
    Unversioned,
    /// Every write creates a version; the previous becomes noncurrent.
    Enabled,
    /// Writes replace the `null` version in place; deletes insert a `null` marker.
    Suspended,
}

impl State {
    /// Whether writes create versions.
    pub fn is_enabled(self) -> bool {
        self == State::Enabled
    }

    /// Whether writes replace the null version in place.
    pub fn is_suspended(self) -> bool {
        self == State::Suspended
    }
}

impl Display for State {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            State::Unversioned => "Unversioned",
            State::Enabled => "Enabled",
            State::Suspended => "Suspended",
        })
    }
}

/// What a version-creating write or a delete produced.
///
/// # Examples
///
/// ```rust
/// use tinio_core::version::Marker;
///
/// assert_eq!(Marker::none(), Marker { id: None, is_delete_marker: false });
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Marker {
    /// The version the operation created or removed; `None` is the null version.
    pub id: Option<Id>,
    /// Whether the version is a delete marker.
    pub is_delete_marker: bool,
}

impl Marker {
    /// No version was created (an unversioned bucket).
    pub fn none() -> Self {
        Self::default()
    }
}

/// One version as the listing returns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The object key the version belongs to.
    pub key: object::Key,
    /// The version identifier; `None` is the null version.
    pub id: Option<Id>,
    /// The per-key sequence, newest first when sorted descending.
    pub seq: u64,
    /// Whether this version is a delete marker.
    pub is_delete_marker: bool,
    /// The version's content length (0 for a marker).
    pub size: u64,
    /// When the version was written.
    pub last_modified: std::time::SystemTime,
    /// The version's ETag.
    pub etag: crate::etag::ETag,
}

/// The pagination cursor of `ListObjectVersions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionMarker(pub String);

/// `ListObjectVersions`' request parameters.
#[derive(Debug, Clone, Default)]
pub struct ListParams {
    /// Only keys with this prefix.
    pub prefix: Option<String>,
    /// Group keys at this delimiter into common prefixes.
    pub delimiter: Option<String>,
    /// Resume after this key.
    pub key_marker: Option<String>,
    /// Resume after this version of `key_marker`.
    pub version_id_marker: Option<VersionMarker>,
    /// The page size.
    pub max_keys: usize,
}

/// One page of versions.
#[derive(Debug, Clone, Default)]
pub struct Listing {
    /// The page's versions, newest first within each key.
    pub entries: Vec<Entry>,
    /// The delimiter roll-up.
    pub common_prefixes: Vec<String>,
    /// Whether more versions remain.
    pub is_truncated: bool,
    /// The next page's key cursor.
    pub next_key_marker: Option<String>,
    /// The next page's version cursor.
    pub next_version_id_marker: Option<String>,
}

/// A version-addressed read's outcome.
#[derive(Debug)]
pub enum ReadResult {
    /// An object version's content and metadata.
    Object(storage::GetObjectResult),
    /// The version is a delete marker: no content, but an identity.
    Marker {
        /// The marker's identifier (`None` = the null version).
        id: Option<Id>,
        /// When the marker was written.
        last_modified: std::time::SystemTime,
    },
}

/// A version-addressed head's outcome.
#[derive(Debug)]
pub enum HeadResult {
    /// An object version's metadata.
    Object(object::Info),
    /// The version is a delete marker.
    Marker {
        /// The marker's identifier (`None` = the null version).
        id: Option<Id>,
        /// When the marker was written.
        last_modified: std::time::SystemTime,
    },
}
```

`crates/tinio-core/src/storage/error.rs` gains two variants beside the others, each with the repo's `#[error("{0}")]` shape and a constructor helper: `InvalidVersion(String)` (a malformed `versionId`) and `NoSuchVersion(String)` (a well-formed id that names no version — the wire's `404 NoSuchVersion`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-core --lib version && cargo test -p tinio-core --doc`
Expected: PASS.

- [ ] **Step 5: Write the contract test**

Write `crates/tinio-core/src/storage/version.rs` (the module is declared and re-exported in Step 6) — the trait, with no test module of its own:

```rust
//! The version-scoped operations of the storage contract.

use async_trait::async_trait;

use crate::{
    acl, bucket, object,
    storage::{BodyStream, Error, GetObjectResult, Storage},
    version::{self, Id, ListParams, Listing, Marker, State},
    ByteRange,
};

/// The version-scoped operations. Backends that do not version anything answer
/// [`State::Unversioned`] from [`VersionOps::bucket_versioning`] and refuse the
/// rest with [`Error::AccessDenied`]-class errors rather than pretending.
#[async_trait]
pub trait VersionOps: Send + Sync + 'static {
    /// The bucket's versioning state.
    async fn bucket_versioning(&self, bucket: &bucket::Name) -> Result<State, <Self as Storage>::Error>
    where
        Self: Storage,
    {
        let _ = bucket;
        Ok(State::Unversioned)
    }

    /// Set the bucket's versioning state.
    async fn set_bucket_versioning(&self, bucket: &bucket::Name, state: State) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// Read a specific version (`None` = the `null` version) of an object.
    ///
    /// A delete marker is a successful outcome, not an error: the caller needs
    /// the marker's identity to render its own answer (plan 3 maps it to the
    /// `404`/`405` wire rules).
    async fn get_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        range: Option<ByteRange>,
    ) -> Result<version::ReadResult, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The metadata of a specific version, with the same marker rule.
    async fn head_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<version::HeadResult, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Permanently remove one version and answer what was removed.
    async fn delete_object_version(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<Marker, <Self as Storage>::Error>
    where
        Self: Storage;

    /// List versions, newest first within each key.
    async fn list_object_versions(
        &self,
        bucket: &bucket::Name,
        params: ListParams,
    ) -> Result<Listing, <Self as Storage>::Error>
    where
        Self: Storage;

    /// The tags of a specific version (`None` = the `null` version).
    async fn version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<object::Tags, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Replace a specific version's tags.
    async fn set_version_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        tags: object::Tags,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;

    /// The ACL of a specific version.
    async fn version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
    ) -> Result<acl::Acl, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Replace a specific version's ACL.
    async fn set_version_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        version: Option<&Id>,
        acl: &acl::Acl,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;
}
```

and pin the default with a ` ```ignore ` rustdoc example on the trait instead of an in-crate test double: the example shows a `tinio-mem` store answering `Unversioned`, and it is `ignore`d with the reason written into the doc — `tinio-core` cannot take a dev-dependency on `tinio-mem` (the dependency runs the other way, so the example would be a cycle) — which is the same exemption `Storage` uses at `crates/tinio-core/src/storage/mod.rs:87-130`. Do not write a `Bare`-style double in its place: no minimal `BucketOps` body exists to copy, and Task 6's conformance leg — run by both backends — is where the default's behavior is actually proven.

- [ ] **Step 6: Wire the contract**

- `crates/tinio-core/src/storage/mod.rs`: `mod version;` beside the other private submodules; `pub use version::VersionOps;`; `pub trait Storage: Send + Sync + 'static + BucketOps + ObjectOps + MultipartOps + VersionOps`.
- `crates/tinio-core/src/lib.rs`: `pub mod version;` (alphabetically last) and add `version::{Id, State}` to the re-export block.
- `crates/tinio-core/src/object.rs`: `Info` gains `pub version_id: Option<version::Id>`; update every constructor in this crate and let the backends' compile errors point at theirs.
- `crates/tinio-core/src/storage/object.rs`: `delete_object` returns `Result<version::Marker, …>`; the doc explains that an unversioned bucket answers `Marker::none()`.
- **Cargo features** — the boundary, decided with the operator on 2026-10-03: **`tinio-core` is not gated at all**. Its `version` module and `VersionOps` are inert types and one trait, and gating them would not compile: the ungated `object::Info` carries `version_id: Option<version::Id>`, and `Storage` aggregates `VersionOps` (a gated supertrait would need two `Storage` definitions). What the feature gates is the **machinery** — `tinio-blob` (**superseded 2026-10-03: after plan 1 was executed the operator ruled its crate-root gate and `versioning` feature out — the feature had exactly one referent, the crate root, and its off-state compiled an empty crate; so the compile-time level now covers only the crates named after this dash, and `tinio-version`'s own crate-root gate below needs re-deciding against that precedent rather than inherited from plan 1**), `tinio-version` (**settled 2026-10-03, round 38 of the metadata-store set: the operator re-decided the gate the `tinio-blob` note left open — `tinio-version` carries *no* `versioning` feature and no crate-root gate. It is always compiled, because the feature would have exactly one referent (the crate root) and its off-state would be an empty crate, which is the reason `tinio-blob`'s gate was removed; the same ruling covers `tinio-ops`. The compile-time level therefore covers the backends' and the server's own version code only**), `tinio-fs`'s and `tinio-mem`'s version modules together with the write-path branch and the `impl VersionOps` bodies that use them, and `tinio-server`'s gated operation rows (plan 3). `crates/tinio-fs/Cargo.toml` (Task 3) and `crates/tinio-mem/Cargo.toml` (Task 6) therefore declare `[features] versioning = []`, and each backend carries a `#[cfg(not(feature = "versioning"))]` **twin** of its `impl VersionOps` that answers `Ok(State::Unversioned)` from `bucket_versioning` (which the trait already defaults) and `Err(storage::access_denied(…).into())` from the other nine — the contract the trait's own doc states for a backend that does not version, and expressible because both backends' error enums already carry `From<storage::Error>`. Defaults on the trait's methods cannot replace that twin: a default body cannot construct `<Self as Storage>::Error`. `tinio-config` keeps its new keys ungated (an unknown key in a config is a startup error, so a stripped build must still parse them). `cargo hack check --each-feature` is the proof the stripped build compiles.

- [ ] **Step 7: Fix the fallout and run the workspace**

Run: `cargo check --workspace --all-targets`
Expected: errors in `tinio-fs`, `tinio-mem` and `tinio-server` (the two contract changes). Fix the *mechanical* fallout only — `Marker::none()` from `delete_object`, `version_id: None` on `Info` constructors — and leave the behavior for Tasks 4–6. Then `cargo test -p tinio-core`.

- [ ] **Step 8: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-core
git commit -m "feat(core): add the version domain and the VersionOps contract"
```

---

### Task 2: `tinio-store` — the version tables and the current-version columns

**Files:**
- Create: `crates/tinio-store/src/version.rs`
- Create: `crates/tinio-store/src/bucket_versioning.rs`
- Modify: `crates/tinio-store/src/meta.rs` (`Stored` gains two fields; the codec)
- Modify: `crates/tinio-store/src/lib.rs` (`ensure_all` list)

**Interfaces:**
- Consumes: `_core::{version, object, etag, acl, checksum}`, `_core::blob::{BlobRef, BlockRef, BlockHash}`, the `TableDef` pattern (`crates/tinio-store/src/object_part.rs` is the template).
- Produces:
  - `version::Table` — key `(bucket: &str, key: &str, seq: u64)` → row `(id: &str, blob: &str, size: u64, created_nanos: u64, became_noncurrent_nanos: u64, kind: &str, tags: &str, checksum: &str, owner: &str, acl: &str)` where `kind` is `"object"` or `"marker"` and the `id` element is the version's wire form with the literal `null` for a null version (the same encoding `meta::Stored`'s codec uses — one convention, no empty-string sentinel to remember); `blob` is the encoded whole `BlobRef` (the codec below) — the row carries the block list, not a bare content hash, because the design promises chunking later is a policy change, not a format migration. **A delete marker stores the empty blob** (`BlobRef::single(BlockHash::of(b""), 0)`, decided with the operator on 2026-10-03): the engine already treats a zero-byte blob as an ordinary stored blob (plan 1's Review Focus 1 is exactly that case), so the row needs no absent value, `Row::blob() -> BlobRef` stays total, and the only cost is that the empty block is always live while a marker exists. Methods: `put`, `get(bucket, key, seq) -> Option<Row>`, `latest(bucket, key) -> Option<(u64, Row)>`, `for_key(bucket, key, visit)` (descending seq), `remove`, `drain_bucket`, `for_bucket(visit)`; the row's typed accessor `pub fn blob(&self) -> BlobRef` decodes the `blob` element, and every reader builds its `BlobRef` from that accessor, never from a hash.
  - `version_index::Table` — key `(bucket, key, id: &str)` → `seq: u64`: the point-lookup index, written and removed in the same transaction as the row it points at. Methods: `put`, `seq_of(bucket, key, id) -> Option<u64>`, `remove`, `drain_bucket`.
  - `bucket_versioning::Table` — key `bucket: &str` → `state: &str`; `get`, `set`, `drain_bucket`.
  - `encode_blob` / `decode_blob` — the one codec pair for the row's `blob` element, over `tinio_core::blob::{BlobRef, BlockRef, BlockHash}`: `<content-hash-hex>:<total-len>:<block-hash-hex>:<block-len>[,<block-hash-hex>:<block-len>…]`, exactly one block entry in v1, with `decode_blob` re-checking `BlobRef::single`'s invariants (declared length equals the blocks, at least one block, **and — added 2026-10-03 — a zero-length block only as the list's sole entry, the empty blob's shape**) so a decoded row is always a valid `BlobRef`.
  - `meta::Stored` gains `pub version_id: Option<version::Id>` (`None` = the null version) and `pub version_seq: u64` (0 = none), i.e. the value tuple becomes a 10-tuple; the codec functions change with it.

- [ ] **Step 1: Write the failing test**

Append to the new `crates/tinio-store/src/version.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::blob::{self, BlobRef},
        testutil::handle,
    };

    fn row(id: &str, seq: u64) -> Row {
        Row {
            id: id.to_owned(),
            blob: encode_blob(&BlobRef::single(blob::BlockHash::of(b"x"), 1)),
            size: 1,
            created: 0,
            became_noncurrent: 0,
            kind: Kind::Object,
            tags: String::new(),
            checksum: String::new(),
            owner: String::new(),
            acl: String::new(),
            seq,
        }
    }

    #[test]
    fn versions_are_read_newest_first_and_indexed_by_id() {
        let h = handle();
        h.write(|txn| {
            let mut v = Table::open(txn)?;
            let mut i = version_index::Table::open(txn)?;
            for seq in [1u64, 2, 3] {
                v.put("b", "k", &row(&format!("v{seq}"), seq))?;
                i.put("b", "k", &format!("v{seq}"), seq)?;
            }
            Ok(())
        })
        .unwrap();

        let seen: Vec<u64> = h
            .read(|txn| {
                let v = Table::open_readonly(txn)?;
                let mut seqs = Vec::new();
                v.for_key("b", "k", |row| {
                    seqs.push(row.seq);
                    Ok(())
                })?;
                Ok(seqs)
            })
            .unwrap();
        assert_eq!(seen, vec![3, 2, 1], "newest first");
        assert_eq!(h.read(|txn| Ok(version_index::Table::open_readonly(txn)?.seq_of("b", "k", "v2"))).unwrap(), Some(2));
    }

    #[test]
    fn a_bucket_drain_removes_every_row_of_that_bucket_only() {
        let h = handle();
        h.write(|txn| {
            let mut v = Table::open(txn)?;
            v.put("b", "k", &row("v1", 1))?;
            v.put("other", "k", &row("v1", 1))?;
            v.drain_bucket("b")
        })
        .unwrap();
        let left = h
            .read(|txn| {
                let v = Table::open_readonly(txn)?;
                let mut left = Vec::new();
                v.for_bucket(|bucket, _, _| {
                    left.push(bucket.to_owned());
                    Ok(())
                })?;
                Ok(left)
            })
            .unwrap();
        assert_eq!(left, vec!["other".to_owned()]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-store --lib version`
Expected: FAIL — `cannot find type Table`.

- [ ] **Step 3: Write the tables**

Follow `crates/tinio-store/src/object_part.rs` exactly: `pub enum Def {}`, `impl TableDef for Def` with the tuple key/value, `pub type Table<'txn, T = …> = table::Table<'txn, Def, T>`, a `Row { pub … }` struct, two impl blocks (read over `T: ReadableTable`, write over the plain handle), the codec inline in `put`/`get`, and the `_core` wire decoders (`decode_acl_wire`, `decode_owner_wire`, `object::Tags::parse_wire_limited`, `checksum::Recorded::from_wire_opt`) for the row's typed accessors; the `blob` element is written by `encode_blob` and read by `decode_blob`, the module's one codec pair over `tinio_core::blob::{BlobRef, BlockRef, BlockHash}` (one `hash:len` entry in v1 — the seam that keeps chunking a policy change, not a format migration), and `Row::blob(&self) -> BlobRef` is its only reader. Add `version_index.rs` and `bucket_versioning.rs` in the same shape (they are small), and register all three in `crates/tinio-store/src/lib.rs`'s `ensure_all`.

For `meta::Stored`, extend the struct and the 8-tuple codec at `crates/tinio-store/src/meta.rs:31-53`:

```rust
pub struct Stored {
    pub etag: ETag,
    pub size: u64,
    pub mtime: u64,
    pub file_identity: u64,
    pub tags: object::Tags,
    pub checksum: Option<checksum::Recorded>,
    pub owner: Option<OwnerId>,
    pub acl: Acl,
    /// The current version's identifier (`None` = the null version).
    pub version_id: Option<version::Id>,
    /// The current version's sequence (0 = the bucket is unversioned here).
    pub version_seq: u64,
}
```

The value tuple becomes `(etag_hex, size, mtime, identity, tags, checksum, owner, acl, version_id, version_seq)` and its encode/decode functions gain the two elements — the version id's wire form, from `version::Id`'s `as_str` with the literal `null` for `None` (read back through `Id::from_wire`), and the sequence number; every constructor of `Stored` in the workspace then needs the two fields — the compiler lists them, and the two behavioral ones are `crates/tinio-fs/src/backend/objects.rs:645-692` (`record_committed`) and `crates/tinio-mem/src/object.rs:77` (`write_object`), which Task 4 and Task 6 fill in.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-store && cargo test -p tinio-fs -p tinio-mem --lib`
Expected: the store tests pass; the backends' tests still pass once the two new fields are `None`/`0` at their constructors.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-store crates/tinio-fs/src/backend/objects.rs crates/tinio-mem/src/object.rs
git commit -m "feat(store): add the version tables and the current-version columns"
```

---

### Task 3: `tinio-version` — the version state machine, and the fs adapter

**Amended 2026-10-03 (engine home).** The state machine is a crate, not a `tinio-fs` module (`docs/superpowers/specs/2026-10-03-metadata-store-contract-design.md`, D16/D17/D26): `tinio-version` declares the narrow ports it depends on and `tinio-fs` provides an adapter. The ports **mirror the future contracts** so the swap is a type substitution: `VersionRows` (version row + index row + sequence + the bucket's versioning state) → `VersionMeta`; `ContentPort` (read the outgoing content, stage and publish) → `ObjectContent`; `CurrentRows` (the key's current slot) has **no** contract counterpart — `Metadata::get` deliberately does not gate — so it survives the swap as a backend port. No separate block port: the engine holds `tinio_blob::Store<B>` directly (the block primitive is `tinio_core::blob::BlockStore`; fs passes `Store<LocalBlocks>`, a future pg backend passes `Store<PgBlocks>`). Every port method returns `Result<_, storage::Error>` — the engine has one error type and the adapter projects the backend's error in; the swap therefore adds one `.map_err(Into::into)` per contract call site (~20, mechanical) and changes no engine logic. The identity/staleness gate (F01/F04) is the **adapter's**: `CurrentRows::current` applies `entry_matches` and answers a null current when it fails, so the engine only ever sees the gated truth.

Its own tests run against an in-crate port double (rows in a map, an in-memory `BlockStore`, content in a tempdir) so the engine is testable without fs; the adapter's tests live in `crates/tinio-fs/src/version_adapter.rs` and use the fs fixture. `list_object_versions` and the retention sweep stay fs-side (they need the bucket's file truth and the gate) — the engine owns the state machine and the retention arithmetic only. `VersionRows` and `ContentPort` are deleted and replaced by `tinio-core`'s persistence contracts when the metadata-store plan set lands (after plan 3); `CurrentRows` remains, and the engine's own signatures and tests do not change then.

**Files:**
- Create: `crates/tinio-version/src/lib.rs`, `crates/tinio-version/src/ports.rs`, `crates/tinio-version/src/engine.rs`, `crates/tinio-version/src/tests.rs`, `crates/tinio-version/Cargo.toml` (**no features** — the crate is always compiled, per the round-38 ruling recorded under Task 1's feature rule)
- Create: `crates/tinio-fs/src/version_adapter.rs` (the fs port implementation: content key ↔ key path, `tinio_blob::Store<LocalBlocks>` as the block engine, `AtomicWriter`, redb rows, and the F01/F04 gate in `CurrentRows::current`)
- Modify: the workspace manifest, `crates/tinio-fs/src/lib.rs`, `crates/tinio-fs/src/backend/mod.rs` (the `FsStorage` field + accessor), `crates/tinio-fs/Cargo.toml` (`[features] versioning = []`, Task 1's rule)

**Interfaces:**
- Consumes: `tinio_blob::{Store, LocalBlocks, LiveSet}` (directly — the engine holds the blob engine, not a block port), `_core::{blob::BlockStore, storage::Error, version}`, and the three ports it declares: `VersionRows` (the version row, its index row, the per-key sequence, the bucket's versioning state), `CurrentRows` (the key's current slot, already gated by the implementation), `ContentPort` (read the outgoing content, stage and publish) — the fs adapter maps a content key to the key path.
- Produces: `tinio_version::Versions` — the engine, generic over its ports and the block store — with the operations the write, read and list paths call:
  - `pub struct Versions<R, C, B> { rows: R, content: C, blobs: Store<B> }` with `pub fn new(rows: R, content: C, blobs: Store<B>) -> Self`; `rows` is the adapter's type implementing both `VersionRows` and `CurrentRows`, `blobs` is the blob engine over the backend's `BlockStore`, and `content` is the adapter's `ContentPort`. The engine takes content **keys**, never paths (D17).
  - `pub(crate) async fn state(&self, bucket) -> Result<State, Error>` / `set_state(&self, bucket, state)` — carried by `VersionRows` and, after the swap, by `VersionMeta::bucket_state`/`set_bucket_state` (the bucket's versioning state is version-domain state; the `buckets` value tuple stays untouched).
  - `pub(crate) async fn current(&self, bucket, key) -> Result<Current, Error>` — the key's current slot, read through `CurrentRows` (which has already applied the F01/F04 gate, so a stale identity arrives as a null current): `pub(crate) struct Current { pub exists: bool, pub version: Option<version::Id>, pub is_delete_marker: bool }`, where `exists` is true for a file or for a delete marker, and `Current::is_null()` is true exactly when `exists && version.is_none() && !is_delete_marker`.
  - `pub async fn supersede(&self, bucket, key) -> Result<(), Error>` — read the outgoing content through `ContentPort`, publish it through the blob engine, then one `VersionRows` call: insert the version row + index row and stamp `became_noncurrent_at`; the row's identity and sequence are the ones the key's current slot already records (`Stored.version_id`, `Stored.version_seq`), never a newly minted id and never a bumped sequence; aborts if the content's identity changed under the read (the supersede race).
  - `pub async fn materialize(&self, bucket, key, entry: &Row) -> Result<(), Error>` — the row's decoded `entry.blob()` streamed through `ContentPort` (stage + publish) onto the key's content slot.
  - `pub(crate) async fn expire(&self, bucket, key, seq) -> Result<(), Error>` — one transaction: drop the row and its index row.
  - `pub async fn latest(&self, bucket, key) -> Result<Option<(u64, Row)>, Error>` and `pub fn blobs(&self) -> &Store<B>` — the row read the tests, the list path and `delete_object_version` use.
  - `pub(crate) async fn live_blocks(&self, bucket) -> Result<LiveSet, Error>` — the block hashes of every row's decoded `BlobRef` in the bucket, for the sweep.
  - The per-key sequence rule: the null version is seq 0; a version-creating write's new version takes one past the outgoing current's `version_seq` (a null or absent current contributes 0, so the first real version is 1), and the outgoing version keeps the sequence it already had — `next_seq` is minted by Task 4's write branch and written by `record_committed`, never by `supersede`. No per-key version lock exists: the guard is Task 4's `lock_bucket_mutations(bucket)`, held across the whole branch.

- [ ] **Step 1: Write the failing test**

`crates/tinio-version/src/tests.rs` — the state machine over the in-crate port double (rows in a map, an in-memory `BlockStore` under `tinio_blob::Store`, content in a tempdir; the fixture lines below become double construction and seeding):

```rust
use super::*;
use crate::testutil;

#[tokio::test]
async fn a_supersede_moves_the_outgoing_file_into_the_block_store_and_stamps_the_row() {
    let fixture = testutil::storage().await;
    let bucket = testutil::bucket(&fixture, "demo").await;
    let key = object::Key::from("notes.txt");
    let target = fixture.storage.key_path(&bucket, &key).await.unwrap();
    tokio::fs::write(&target, b"first").await.unwrap();
    // The identity the row must carry is the one the key's `object_meta` row already
    // records (`Stored.version_id` / `version_seq`); the write path that plants it is
    // Task 4's, so the test plants it through a local helper that writes
    // `meta::Stored { version_id: Some(version_id(id).unwrap()), version_seq: seq, size: 5, … }`
    // for the key — the shape `record_committed` writes on the way out.
    plant_stored(&fixture, &bucket, &key, "v1", 1).await;

    let versions = fixture.storage.versions(&bucket);
    versions.set_state(&bucket, State::Enabled).await.unwrap();
    versions.supersede(&bucket, &key, &target).await.unwrap();

    let (seq, row) = versions.latest(&bucket, &key).await.unwrap().expect("a version row");
    assert_eq!(row.id, "v1", "the row carries the outgoing identity the meta row recorded, never a freshly minted one");
    assert_eq!(seq, 1, "and the sequence that version already had");
    assert_eq!(row.size, 5);
    assert!(versions.blobs().contains(row.blob().hash()).await.unwrap(), "the content is in the block store");
}

#[tokio::test]
async fn materializing_a_version_writes_its_bytes_back_to_the_key_path() {
    /* supersede "first" as v1, overwrite the key path with "second",
       then materialize v1 and assert the file reads "first" again */
}

#[tokio::test]
async fn the_supersede_race_fails_the_write_instead_of_committing_unknown_bytes() {
    /* supersede with a target file whose identity changes between the
       resolve and the read: the call must answer an error and write no row */
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib version`
Expected: FAIL — `cannot find type Versions`.

- [ ] **Step 3: Write the state machine**

`crates/tinio-version/src/engine.rs`: module docs stating the one rule every backend shares ("the current version is whatever that backend's object store holds") and the protocol order from the spec (**Write paths** steps 1–5), then the `Versions` implementation over its ports. **Amendment 2026-10-03**: the transaction this step used to inline lives in the `VersionRows` implementation (the fs adapter, `crates/tinio-fs/src/version_adapter.rs`), which opens the two redb tables in one closure in the shape `complete_object_state` already uses (`crates/tinio-fs/src/backend/mod.rs:783`); the engine calls `rows.put_version_row(…)`, and the two-table atomicity Review Focus 1 depends on is the adapter's to keep. The sketch that follows is that adapter's body:

```rust
self.handle
    .write(move |txn| {
        let mut rows = version::Table::open(txn)?;
        let mut index = version_index::Table::open(txn)?;
        rows.put(&bucket, &key, &row)?;
        index.put(&bucket, &key, &id_wire, seq)?;
        Ok(())
    })
    .await
```

The `row` that transaction writes is built from the `BlobRef` the block port returned (`blob: encode_blob(&blob)`) and from the identity and sequence the key's current slot already records (`Stored.version_id` / `Stored.version_seq`) — `supersede` never mints either, and the index row is keyed by that same identity.

`supersede` reads the outgoing content through the fs adapter's `ContentPort` (the stream helper at `crates/tinio-fs/src/backend/objects.rs:141-176`) so the block write streams, and re-stats the content after the read: a size/mtime/identity change is the race and answers a dedicated error (`Error::SupersedeRace { key }`) whose projection is an I/O error with `ErrorKind::Other` and a message naming the concurrent modification.

The protocol's one interruption window is step 3's: the row is committed before the rename, so a crash there leaves the old file at the key path **and** a row describing it. Recovery **adopts** that row rather than discarding it — its content is real (the file still at the key path) and it is the version that was current — and because the row carries the identity and sequence the meta row already records, the next supersede of that version rewrites the same row instead of creating a second one for the same content.

The crates land together (Task 1's feature rule): `tinio-version` **carries no feature** and is always compiled, while `tinio-fs`'s and `tinio-mem`'s own version modules carry `[features] versioning = []` with the `#[cfg(not(feature = "versioning"))]` twins; `FsStorage` gains `blobs_dir()` (the `state-dir/blobs` root) and `versions(&self, bucket)` (the adapter's engine); `state_dir()` already exists (`crates/tinio-fs/src/backend/mod.rs:243`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-version` and `cargo test -p tinio-fs --lib version`
Expected: PASS.

- [ ] **Step 5: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-version crates/tinio-fs/src/version_adapter.rs crates/tinio-fs/src/lib.rs crates/tinio-fs/src/backend/mod.rs crates/tinio-fs/Cargo.toml Cargo.toml
git commit -m "feat(version): add the shared version engine and its fs adapter"
```

---

### Task 4: `tinio-fs` — the write, read and delete paths

**Files:**
- Modify: `crates/tinio-fs/src/backend/objects.rs` (`commit_staged`, `record_committed`; new `get_object_version`, `head_object_version`, `delete_object_version`, `delete_object`)
- Modify: `crates/tinio-fs/src/backend/multipart.rs` (`complete_multipart_upload`'s phase 2)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`complete_object_state`)
- Create: `crates/tinio-fs/src/version/list.rs` (`list_object_versions` — fs-side, per this plan's 2026-10-03 amendment; the engine does not own the bucket's file truth)
- Create: `crates/tinio-fs/src/backend/version.rs` (`impl VersionOps for FsStorage`)

**Interfaces:**
- Consumes: Task 3's `Versions` (`state`, `current`, `supersede`, `materialize`, `expire`, `latest`, `blobs`, `live_blocks`), Task 2's tables and the `Row::blob()` accessor, `_core::version`.
- Produces: the fs implementation of `VersionOps`, and the version-aware behavior of the existing object ops:
  - `commit_staged` gains the version-creating branch: under `lock_bucket_mutations`, after `ensure_bucket`/`resolve_key` and **before** `AtomicWriter::commit`, the branch in Step 3 mints the new id and the next sequence, calls `versions.supersede(...)` when a current version exists, and passes both into `record_committed`. The whole branch — state read, current read, supersede, rename, record — runs under that one existing lock, which is why the per-key sequence needs no second guard.
  - `delete_object` (no version) on a versioned bucket: supersede the current version, remove the file, insert the marker row, and answer the marker's `Marker`.
  - `delete_object_version(bucket, key, Some(id))`: drop the row; when the removed version was current, `materialize` the next-newest.
  - `get_object_version` / `head_object_version`: a delete marker answers `ReadResult::Marker { .. }` / `HeadResult::Marker { .. }` — a successful outcome carrying the marker's identity, which plan 3 renders as its `404`/`405` wire rules; object versions stream from `blobs.open(&row.blob(), range)`.
  - `list_object_versions`: walk the bucket's current objects — the same `meta::Store::load_bucket` range scan the object listing uses — and merge each key's version rows into them, newest first per key, honoring `ListParams`. The object walk is the driver because it is what guarantees a key that was never overwritten (a current version with no history rows) still appears: `ListObjectVersions` lists every current version too.

- [ ] **Step 1: Write the failing test**

Append to `crates/tinio-fs/src/version/tests.rs` (the listing tests; the engine's own tests live in `crates/tinio-version/src/tests.rs`):

```rust
#[tokio::test]
async fn overwriting_a_versioned_object_keeps_the_previous_version_readable() {
    /* enable versioning; put "first"; put "second";
       get the current -> "second"; list versions -> two entries, newest first;
       get the older id -> "first" (Review Focus 1's read half) */
}

#[tokio::test]
async fn a_suspended_write_replacing_the_null_version_creates_no_history() {
    /* enable then suspend; put "first" (a null version); put "second";
       assert exactly one version row exists and it is the null version */
}

#[tokio::test]
async fn deleting_the_last_version_removes_the_key_from_the_tree() {
    /* enable; put; delete with the version id; assert the file is gone,
       no rows remain, and list_object_versions is empty (Review Focus 4) */
}

#[tokio::test]
async fn a_simple_delete_on_a_versioned_bucket_inserts_a_marker_and_keeps_the_content() {
    /* enable; put; delete (no version); assert the file is gone, the marker
       is the latest, the old version still reads, and a second delete inserts
       a second marker (AWS's documented stacking) */
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib version`
Expected: FAIL — the behaviors are not implemented.

- [ ] **Step 3: Implement the write path**

In `commit_staged` (`crates/tinio-fs/src/backend/objects.rs:485`), inside the existing lock (the whole branch — the state read, the current read, the supersede, the rename and the record — runs under `lock_bucket_mutations(bucket)`, which is why the per-key sequence needs no second guard), before the rename:

```rust
let versioning = self.versions(bucket);
let state = versioning.state(bucket).await?;
let current = versioning.current(bucket, key).await?;
let marker = match state {
    version::State::Unversioned => version::Marker::none(),
    version::State::Enabled => {
        let id = version::Id::from(uuid::Uuid::new_v4().to_string().as_str());
        if current.exists {
            versioning.supersede(bucket, key, &target).await?;
        }
        version::Marker { id: Some(id), is_delete_marker: false }
    }
    version::State::Suspended => {
        if current.exists && !current.is_null() {
            versioning.supersede(bucket, key, &target).await?;
        }
        version::Marker { id: None, is_delete_marker: false }
    }
};
```

The three arms in prose. **`State::Enabled`**: every write mints a fresh `version::Id` (a uuid v4 string via `version::Id::from(uuid::Uuid::new_v4().to_string().as_str())`) and the response carries it; if a current version exists (a file, or a delete marker with no file), the outgoing one is superseded first; a write onto a *new* key still mints an id (no supersede, since there is nothing to preserve); a write onto an object whose current version is a **delete marker** is a normal versioned write — the marker becomes noncurrent history. **`State::Suspended`**: the write replaces the null version in place when the current version *is* the null version (no version row, no blob, no sequence bump, answer `Marker { id: None, .. }`); when the current version is non-null (or a marker), the outgoing is superseded and the new null version becomes current. **`State::Unversioned`**: `Marker::none()`.

The whole branch is `#[cfg(feature = "versioning")]` (Task 1's feature rule): without the feature the write path keeps today's behavior exactly — `let marker = version::Marker::none();`, no state read, no supersede, no sequence — which is the same shape a bucket that never enabled versioning already takes.

The branch mints the sequence too, and only when it creates a version: `next_seq = current.version_seq + 1` (the `object_meta` row's field), with a null or absent current contributing 0 — the null version is seq 0, so the first real version is 1 — while the suspended in-place replacement creates no version and keeps the stored sequence. The row `supersede` commits before the rename is the Task 3 window: a crash between the two is recovered by adopting that row, never by minting a second identity for the same content.

`record_committed` then stores `version_id: marker.id.clone()` and `version_seq: next_seq` in the row it writes, and `commit_staged` answers an `Info` whose `version_id` is the marker's. `complete_multipart_upload` (`crates/tinio-fs/src/backend/multipart.rs:201`) takes the same branch in its phase 2 and threads the identity through `complete_object_state` (`crates/tinio-fs/src/backend/mod.rs:783`), whose transaction already writes `OBJECT_META` — the version columns join that same tuple.

- [ ] **Step 4: Implement the read, delete and list paths**

`impl VersionOps for FsStorage` in `crates/tinio-fs/src/backend/version.rs` — decided with the operator on 2026-10-03: the block is pinned to its own file rather than left to an executor's judgement about `backend/mod.rs`'s size (that file is already 885 lines, and this trait is ten methods):

- `bucket_versioning` / `set_bucket_versioning` — a point read/write of `bucket_versioning::Table`, in one transaction with nothing else.
- `get_object_version` / `head_object_version` — resolve the row (or the current row when `version` is `None` and the object is not versioned); a delete marker is a **successful** outcome — `ReadResult::Marker { .. }` / `HeadResult::Marker { .. }`, carrying the marker's identity — which plan 3 renders as its `404`/`405` wire rules, never an error class here; an object version opens `blobs.open(&row.blob(), range)`.
- `delete_object_version` — one transaction dropping the row + index row; when it was the latest, the next-newest is materialized **before** the row is dropped (so a crash between them leaves the version recoverable), and the current-version columns follow the version that becomes current (or clear when none is left).
- `delete_object` (simple) — as in Step 3's marker branch, plus the file removal and the marker row.
- `list_object_versions` — the page walk in `crates/tinio-fs/src/version/list.rs` (amended 2026-10-03: it stays fs-side — it needs the bucket's file truth through `meta::Store::load_bucket`'s gate, which the engine deliberately does not own): walk the bucket's current objects and merge each key's version rows into them, newest first per key, apply `prefix`/`delimiter`, and build the cursors.

The block is `#[cfg(feature = "versioning")]`, and its `#[cfg(not(feature = "versioning"))]` twin (Task 1's feature rule) sits beside it in the same file: `bucket_versioning` rides the trait's default (`Unversioned`), and the other nine answer `Err(storage::access_denied(…).into())` — both backends' error enums already carry `From<storage::Error>`, which is what makes the refusal expressible without the gated machinery.

- [ ] **Step 5: Run the tests and the crate gate**

Run: `cargo test -p tinio-fs --lib && cargo test -p tinio-fs`
Expected: PASS — the new legs and every existing fs test.

- [ ] **Step 6: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-fs
git commit -m "feat(fs): make the object paths version aware"
```

---

### Task 5: `tinio-fs` — retention, block reclamation and cleanup

**Files:**
- Modify: `crates/tinio-fs/src/sweep.rs` (two new passes)
- Modify: `crates/tinio-fs/src/cleanup.rs` (a repair stage + the doctor checks)
- Modify: `crates/tinio-fs/src/backend/mod.rs:740` (`remove_bucket_state` — the version rows and the block subtree)
- Modify: `crates/tinio-fs/src/backend/tests.rs:791` (`state_dir_holds_only_redb_tmp_and_multipart` → `…_and_blobs`)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs:446` (`Sweeper::new` is fed the config values and the grace default)
- Modify: `docs/tests.md` (the state-dir classes gain `blobs/`)

**Interfaces:**
- Consumes: `tinio_blob::{LiveSet, SweepReport}`, Task 3's `Versions`, the config keys plan 3 adds (`noncurrent_keep`, `noncurrent_expire_days`) — **this task takes them as `sweep::Options` fields** so it does not depend on the config wiring.
- Produces: `sweep::Options { temp_ttl, multipart_ttl, noncurrent_keep: u64, noncurrent_expire_days: u64, block_grace: Duration }`, where `block_grace` comes from the config key `[s3] block_grace_minutes` — decided with the operator on 2026-10-03: the grace is operator-tunable, and it carries a documented default of **ten minutes** (the Garage precedent the design cites) with a validation floor of one minute, because `Duration::default()` is zero and a zero grace would silently disable the mandatory race guard of Review Focus 5; `sweep::Summary { temp_files, uploads, expired_versions, reclaimed_blocks, reclaimed_bytes }`.

- [ ] **Step 1: Write the failing test**

Append to `crates/tinio-fs/src/sweep.rs`'s tests:

```rust
#[tokio::test]
async fn a_sweep_expires_noncurrent_versions_beyond_the_keep_budget() {
    /* enable versioning; three overwrites; sweep with noncurrent_keep = 1;
       assert one noncurrent version survives and its content still reads */
}

#[tokio::test]
async fn a_sweep_reclaims_the_blocks_of_expired_versions_only() {
    /* after the expiry above, assert the live version's block is present and
       the expired version's block is gone; and that a block published inside
       the grace window survives (Review Focus 5) */
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-fs --lib sweep`
Expected: FAIL — `no field noncurrent_keep`.

- [ ] **Step 3: Implement the passes**

`sweep_once` (`crates/tinio-fs/src/sweep.rs:135`) gains `versions: self.sweep_versions(now)` and `blocks: self.sweep_blocks(now)` beside its two existing passes:

- `sweep_versions` — for each bucket whose state is enabled or suspended: page the version rows; for each key, walk newest-first and expire a noncurrent version when it is beyond `noncurrent_keep` or older than `noncurrent_expire_days` measured from `became_noncurrent_at` (markers count, per the design's retention ruling); each expiry calls `Versions::expire`.
- `sweep_blocks` — for each such bucket: `live = versions.live_blocks(bucket)`, then `blobs.sweep(&live, block_grace, now)`; then reap staged temps older than the grace under `<state-dir>/blobs/<bucket>/tmp`. The grace covers a publish that raced the live-set computation **only when that publish stored a new block** — a dedup hit leaves the existing block's mtime untouched, so the grace does not cover it and the mutation-lock/mark-epoch re-check required by Review Focus 5 is what does. The grace value is the configured `[s3] block_grace_minutes` (default ten minutes, floor one), never an unset `Duration::default()` (zero would disable the guard entirely).

The two new fields join the existing `SmartDefault` shape: `noncurrent_keep` and `noncurrent_expire_days` default to `0` (`#[default(_code = "0")]` — unset means keep every version, the safe reading), and `block_grace` is fed from `[s3] block_grace_minutes` (default 10, validated at one minute or more) with `#[default(_code = "Duration::from_secs(10 * 60)")]` as the `Options`-level fallback, the field's doc naming the Garage precedent and the reason zero is not an option.

The deployed sweeper must actually run these passes: `crates/tinio-cli/src/commands/start/serve.rs:446`'s `Sweeper::new(storage, sweep::Options::default())` becomes the config-fed form — `Sweeper::new(storage, sweep::Options { noncurrent_keep, noncurrent_expire_days, ..sweep::Options::default() })` with the two values read from the resolved config (plan 3's keys) and the grace riding the struct's default — so the CLI's sweeper is the retention pass with the operator's numbers, not the built-ins.

- [ ] **Step 4: Extend cleanup and the state-dir contract**

- `cleanup.rs`: a `repair_block_residue` stage in the ordered list (`crates/tinio-fs/src/cleanup.rs:631-651`) that removes `<state-dir>/blobs/<bucket>/tmp` residue at startup (the same rule as `repair_tmp`), and a doctor check reporting block-store residue (`blocks referenced by no row`, `rows whose block is missing`) — the second is the only place the design's trust-the-store on the read path is compensated.
- **Bucket delete**: the unpublish-then-drain sequence (`data-model.md`, Bucket) gains two things — the bucket's `version` and `version_index` rows drain inside `remove_bucket_state` (`crates/tinio-fs/src/backend/mod.rs:740`, the same write transaction that already drains `OBJECT_META`/`OBJECT_PARTS`/`UPLOADS`/`PARTS`), and the `<state-dir>/blobs/<bucket>/` subtree goes with the bucket directory under that call's mutation lock, so a deleted bucket leaves neither rows nor blocks behind.
- `crates/tinio-fs/src/backend/tests.rs:791`: the closed-set test gains `blobs` and keeps asserting that nothing else appears.
- `docs/tests.md`: the state-dir classes gain `blobs/` (the prose twin of that closed-set test).

- [ ] **Step 5: Run the tests**

Run: `cargo test -p tinio-fs && cargo test -p tinio-server`
Expected: PASS.

- [ ] **Step 6: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-fs crates/tinio-cli/src/commands/start/serve.rs docs/tests.md
git commit -m "feat(fs): expire noncurrent versions and reclaim their blocks"
```

---

### Task 6: `tinio-mem` parity, the conformance legs, and the gates

**Files:**
- Modify: `crates/tinio-mem/src/object.rs`, `crates/tinio-mem/src/storage.rs`, `crates/tinio-mem/src/bucket.rs` (the `impl VersionOps`), `crates/tinio-mem/Cargo.toml` (`[features] versioning = []`, Task 1's rule)
- Create: `crates/tinio-mem/src/version.rs`
- Modify: `crates/tinio-util/src/testing.rs` (a `conformance_versioning` leg)
- Modify: `specs/001-s3-local-server/tasks.md` (rows T150–T151)

**Interfaces:**
- Consumes: Task 2's tables, Task 1's contract, the harness's shape (`crates/tinio-util/src/testing.rs:221`).
- Produces: `tinio-mem`'s version implementation over rows (no block engine: a superseded version's content goes into the `objects` table under its content hash, so mem is content-addressed at whole-blob granularity, as the design states), and one new conformance leg run by both backends.

- [ ] **Step 1: Write the failing test**

In `crates/tinio-mem/src/version.rs`'s tests: the same four journeys Task 4 tests on fs (overwrite keeps the previous readable; a suspended write replacing the null version creates no history; deleting the last version removes the key; a simple delete inserts a marker). Keep them short — the conformance leg is the real oracle:

```rust
#[tokio::test]
async fn the_memory_backend_keeps_the_previous_version_readable() { /* as fs, over rows */ }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-mem --lib version`
Expected: FAIL.

- [ ] **Step 3: Implement the mem backend**

`tinio-mem` already stores a superseded object's bytes in `objects::Table` keyed `(bucket, key)`. Versioning adds: the same `tinio_store::version`/`version_index`/`bucket_versioning` tables (mem already opens the shared tables at `crates/tinio-mem/src/storage.rs:152-162`), a `blobs` table keyed `(bucket, hash_hex) -> Bytes` for superseded content, and the state machine over rows — no I/O, so the move/materialize pair is a row copy. Reuse Task 3's state-machine *policy* only by copying the order, not the code: mem's paths are different enough (no files, no rename, one transaction for everything) that sharing would force the fs module to abstract over I/O it needs synchronously. The module and mem's `impl VersionOps` are `#[cfg(feature = "versioning")]` (Task 1's feature rule), with the same `#[cfg(not(feature = "versioning"))]` refusal twin the fs backend carries in Task 4.

- [ ] **Step 4: Add the conformance leg**

`crates/tinio-util/src/testing.rs`: `async fn conformance_versioning<S: Storage>(storage: &S, b: &bucket::Name)` beside the five existing legs, registered as one more call in `assert_conformance` (`:221`). The leg asserts, through `VersionOps` only: the default state is unversioned; enabling is visible; an overwrite keeps the previous version readable by id; `list_object_versions` returns both, newest first; a simple delete makes the latest a marker and its content unreachable by a plain get; deleting the marker restores the previous version; `?versionId=null` resolves for a suspension-written object. Both backends run it (`crates/tinio-fs/src/backend/objects.rs:1234`, `crates/tinio-mem/src/storage.rs:261`).

- [ ] **Step 5: Run the whole gate**

Run each, expecting success:
- `cargo test -p tinio-fs -p tinio-mem -p tinio-store -p tinio-core -p tinio-util`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
- `cargo hack check --package tinio-fs --each-feature`

- [ ] **Step 6: Append the issue-tracker rows**

```markdown
### Addendum 2026-10-03 — S3 versioning, the versioned store (design `docs/superpowers/specs/2026-10-03-s3-versioning-design.md`, plan `docs/superpowers/plans/2026-10-03-s3-versioning-p2-versioned-store.md`)

Incremental storage-layer work on the versioning design, executed as this plan's Tasks 1-6: the `tinio-core` version domain and the `VersionOps` contract, the `tinio-store` version tables and the two appended `object_meta` elements, the `tinio-version` engine (amended 2026-10-03: a crate on its own ports, with a `tinio-fs` adapter) and the versioned object paths, retention and block reclamation, and `tinio-mem` parity with a conformance leg both backends run. No S3 surface changes — plan 3 owns the wire, the capability rows and the config keys; the `object_meta` tuple change is the row-format change the 2026-09-02 ruling covers (delete-and-rebuild).

- [X] T150 Implement the version domain, the version tables and the `tinio-fs` versioned paths (Tasks 1-4)
- [X] T151 Implement retention, block reclamation, `tinio-mem` parity and the versioning conformance leg (Tasks 5-6)
```

- [ ] **Step 7: Commit**

Ask the operator for the git write, then:

```bash
git add crates/tinio-mem crates/tinio-util specs/001-s3-local-server/tasks.md
git commit -m "feat(mem): implement the version contract and prove both backends"
```

---

## Plan-set ownership

- **This plan owns**: the `tinio-core::version` domain types, the `VersionOps` trait and its signatures, the `tinio-store` version tables and the `object_meta` value tuple, the `tinio-version` engine's protocol and its transaction boundaries (amended 2026-10-03: the engine is the crate's, the transaction boundary is its `VersionRows`/`CurrentRows` implementation's), the retention arithmetic and the block-reclamation pass. Plan 3 refers to these names and never restates them.
- **Plan 1 owns**: `tinio_core::blob` and `tinio_blob`'s surface.
- **Plan 3 owns**: the s3s operations, headers and error mapping, the capability rows and config keys, the FR-023 amendment, and the cucumber journeys.
- **The spec owns**: the storage model and the version state machine.

## Notes for the executor

- The two contract changes in Task 1 (`Info.version_id`, `delete_object`'s return) are the interface churn of this plan: fix the mechanical fallout in the same task, and let Tasks 4–6 give it behavior. Do not "fix" them by adding parallel methods — the design's one-path rule (spec, **Write paths**) depends on one delete entry point.
- Every transaction that moves a version out of the current slot writes the row, its index row and the current-version columns **together**; a two-transaction version of the same step is the bug Review Focus 1 describes.
- `crates/tinio-fs/src/backend/tests.rs:791` and `docs/tests.md`'s state-dir classes are the contract for what may live under `<state-dir>`: `blobs/` joins them in Task 5 (which edits both), and the closed-set test is what keeps the next stray directory from shipping.
