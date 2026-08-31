# Multipart Upload Checksum Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate content integrity on multipart uploads when the client specifies a checksum algorithm and value (CRC32/CRC32C/CRC64NVME/SHA1/SHA256/MD5 + Content-MD5), with all checksum computation in `tinio-server`, behind a `[s3] checksum` toggle (default off).

**Architecture:** A server-side tee (`VerifyStream` over `s3s::checksum::ChecksumHasher`, md5 + requested algorithm enabled together) validates part bodies in the same streaming pass the backend stages to disk; a mismatching stream fails and the backend aborts. Complete validates pre-commit from stored part checksums (`COMPOSITE` = algorithm over concatenated raw part digests; `FULL_OBJECT` = CRC linearization) — no re-reads, no backend hashing. The contract gains `get_multipart_upload` + `set_part_checksum`; the backends only store/return what the server hands over.

**Tech Stack:** Rust workspace (edition 2024); `s3s` 0.15.0 (`checksum::ChecksumHasher`, `crypto::{Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256}`, `TrailingHeaders`); redb 4.2 (fs + mem); `base64` 0.22 (server); `futures`, `bytes`, `tokio`.

**Spec:** `docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md` — this plan argues from the spec; executors read both.

## Global Constraints

- **English only** in docs, comments, commit messages (project rule).
- **No auto git commit** — leave changes in the tree; report pending changes; the user decides when to commit (project rule).
- **No new workspace dependencies** — hashing is `s3s::checksum`; the composition helpers are hand-rolled carryless math, reference-tested (spec §tinio-server).
- **`unsafe_code = forbid`** on every crate (`docs/cargo.md`).
- **Tests:** `#[tokio::test]` / `async fn` directly; no `Runtime::block_on` wrappers (project rule).
- **Wire surface verified against s3s 0.15.0 registry source**; composition rules pinned byte-exact with reference vectors (spec "AWS wire facts").
- **Style:** import-module-not-type; `_core`/`_util`/`_mem` re-export convention; compressed prose (`docs/style.md`).
- **Feature gate:** every behavior change in Tasks 5–8 is gated on `self.caps.checksum`; off ⇒ exactly today's code path.
- **Toggle default off**: `Capabilities::default().checksum == false`.

---

## Implementation state (verified 2026-08-31, all steps complete)

All 49 steps below are complete and green (`cargo test -p tinio-core -p tinio-config -p tinio-fs -p tinio-mem -p tinio-util -p tinio-server`; `cargo +nightly fmt --check` clean). The code evolved past this plan's step text during the implementation review — the **design spec is the source of truth** (`docs/superpowers/specs/2026-08-31-multipart-checksum-validation-design.md`, round-2 log). Divergences, all incorporated in the code:

- **Tee-slot redesign (replaces `set_part_checksum` + CAS).** Task 3's contract method `set_part_checksum` and Task 5's `self.upload_tee` staging were superseded by the atomic-commit design: `upload_part` gains a last param `checksum: Option<Arc<checksum::PartChecksum>>` — a tee slot (`digest: OnceLock<Part>`, `etag_md5: bool`) the backend persists (or clears) in the SAME transaction as the part row. No second call, no CAS, no `set_part_checksum`; with `etag_md5` the slot also supplies the part ETag (no second MD5 pass). Design round-1 log R9.
- **Naming.** `ChecksumAlgorithm`/`ChecksumType`/`ChecksumValue`/`PartChecksum`/`UploadChecksum` → `checksum::{Algorithm, Type, Value, Part, Upload}` (module `tinio-core/src/checksum.rs`); a new `checksum::PartChecksum` is the tee slot; `as_wire`/`from_wire` are `Display`/`FromStr` via `parse_display` (H03). `ChecksumSpec` → `checksum::Spec`; `set_output_checksum`/`HasChecksumFields` → `HasFields::set_checksum`; `VerifyStream::wrap` gains the trailing-headers handle param (R4).
- **Algorithm set widened to ten (D6).** `Sha512`/`XxHash64`/`XxHash3`/`XxHash128` join the enum for wire-name parsing and create-algorithm compute paths; their value fields remain accepted-and-dropped; COMPOSITE is legal for them, FULL_OBJECT stays CRC-only.
- **Complete validation helpers.** Task 6's inline sketch became shared helpers `stored_parts` / `derive_full_checksum` / `resolve_checksum_type`; `x-amz-mp-object-size` shape checks run before the D2 completeness gate (W04); the CompletedPart cross-check is unconditional on a create-algorithm (W03); the paging + validation run under `lock_object` (R8/W02).
- **Create accepts-and-drops** a `checksum_type` without an algorithm (warn, C01); a create-time algorithm × type combo invalid per the table answers `InvalidRequest` at create (F5).
- **More tests than planned** (cross-check scope, D2 skip + size-order, copy fast-path retention, toggle-gated echo, trailer parsing, fs staging-abort) — see `backend/multipart.rs` test module and the design doc's Testing section.

---

### Task 1: tinio-core checksum types

**Files:**
- Create: `crates/tinio-core/src/checksum.rs`
- Modify: `crates/tinio-core/src/lib.rs:19-40` (module decl + `pub use` list)
- Test: `crates/tinio-core/src/checksum.rs` (module test)

**Interfaces:**
- Consumes: nothing (standalone types; tinio-core has no s3s dependency).
- Produces: `ChecksumAlgorithm`, `ChecksumType`, `ChecksumValue`, `PartChecksum`, `UploadChecksum` — used by every later task.

- [x] **Step 1: Write the failing test**

Create `crates/tinio-core/src/checksum.rs` with the types AND the test module, then run to see the module not exist error first (TDD: the test file drives the module):

```rust
//! S3 checksum types shared across the storage contract (spec
//! 2026-08-31-multipart-checksum-validation-design.md).
//!
//! Plain value types only — no hashing, no wire encoding beyond the
//! algorithm/type names. All checksum computation lives in tinio-server;
//! the backends store and return these values untouched.

/// The S3 checksum algorithms tinio validates: the `ChecksumAlgorithm`
/// wire values of the API model plus MD5 (`x-amz-checksum-md5` and the
/// legacy `Content-MD5` share the MD5 slot).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (ISO-HDLC), `x-amz-checksum-crc32`.
    Crc32,
    /// CRC-32C (Castagnoli), `x-amz-checksum-crc32c`.
    Crc32C,
    /// CRC-64/NVMe, `x-amz-checksum-crc64nvme`.
    Crc64Nvme,
    /// SHA-1, `x-amz-checksum-sha1`.
    Sha1,
    /// SHA-256, `x-amz-checksum-sha256`.
    Sha256,
    /// MD5, `x-amz-checksum-md5` / `Content-MD5`.
    Md5,
}

impl ChecksumAlgorithm {
    /// All supported algorithms.
    pub const ALL: [ChecksumAlgorithm; 6] = [
        Self::Crc32,
        Self::Crc32C,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
        Self::Md5,
    ];

    /// The S3 wire name (`"CRC32"` … `"MD5"`).
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32C => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Md5 => "MD5",
        }
    }

    /// Parse a wire name; `None` for anything else (a corrupt persisted
    /// row or an unsupported algorithm is skipped by the caller, never
    /// a storage error).
    pub fn from_wire(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|a| a.as_wire() == s)
    }
}

/// How a multipart full-object checksum is derived from the parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChecksumType {
    /// The algorithm over the concatenation of the raw part digests.
    Composite,
    /// The CRC of the whole content, linearized from the part CRCs.
    FullObject,
}

impl ChecksumType {
    /// The S3 wire name (`"COMPOSITE"` / `"FULL_OBJECT"`).
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Composite => "COMPOSITE",
            Self::FullObject => "FULL_OBJECT",
        }
    }

    /// Parse a wire name (`None` for anything else).
    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "COMPOSITE" => Some(Self::Composite),
            "FULL_OBJECT" => Some(Self::FullObject),
            _ => None,
        }
    }
}

/// A checksum value in the S3 wire format (base64 of the raw digest).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumValue(pub String);

impl ChecksumValue {
    /// The base64 string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One part's stored checksum: the algorithm and its base64 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartChecksum {
    /// The algorithm the value was computed with.
    pub algorithm: ChecksumAlgorithm,
    /// The base64-encoded digest.
    pub value: ChecksumValue,
}

/// The upload-level checksum specification of
/// `CreateMultipartUpload` (`x-amz-checksum-algorithm` +
/// `x-amz-checksum-type`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadChecksum {
    /// The algorithm every part and the full object are computed with.
    pub algorithm: ChecksumAlgorithm,
    /// The full-object derivation, when the client fixed one at create.
    pub checksum_type: Option<ChecksumType>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algorithm_wire_names_round_trip() {
        for algo in ChecksumAlgorithm::ALL {
            assert_eq!(ChecksumAlgorithm::from_wire(algo.as_wire()), Some(algo));
        }
        assert_eq!(ChecksumAlgorithm::from_wire("XXHASH64"), None);
        assert_eq!(ChecksumAlgorithm::from_wire("crc32"), None); // case-sensitive
    }

    #[test]
    fn checksum_type_wire_names() {
        assert_eq!(ChecksumType::from_wire("COMPOSITE"), Some(ChecksumType::Composite));
        assert_eq!(ChecksumType::from_wire("FULL_OBJECT"), Some(ChecksumType::FullObject));
        assert_eq!(ChecksumType::from_wire("composite"), None);
        assert_eq!(ChecksumType::Composite.as_wire(), "COMPOSITE");
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-core checksum`
Expected: error — the `checksum` module does not exist yet.

- [x] **Step 3: Wire the module into `lib.rs`**

In `crates/tinio-core/src/lib.rs`: add `pub mod checksum;` after `pub mod bucket;`, and add to the `pub use self::{ … }` list:

```rust
    checksum::{
        ChecksumAlgorithm, ChecksumType, ChecksumValue, PartChecksum, UploadChecksum,
    },
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-core`
Expected: PASS (the two new tests; the existing suite is untouched).

- [x] **Step 5: Report**

Leave the changes in the tree (no auto-commit — project rule). Report `crates/tinio-core/src/checksum.rs` created + `lib.rs` exports.

---

### Task 2: `[s3] checksum` capability toggle

**Files:**
- Modify: `crates/tinio-config/src/schema/s3.rs:23-75` (`Capabilities`), :119-145 (default fns)
- Test: `crates/tinio-config/src/schema/s3.rs` test module

**Interfaces:**
- Consumes: nothing.
- Produces: `Capabilities.checksum: bool` (default `false`) — Tasks 5–8 read `self.caps.checksum`.

- [x] **Step 1: Write the failing test**

Add to the `tests` module of `crates/tinio-config/src/schema/s3.rs` (mirror the `allow_zero_page_size` test at :247-262):

```rust
    #[test]
    fn checksum_defaults_off_and_parses() {
        assert!(!Capabilities::default().checksum);
        let config = RootConfig::parse("version = 1\n[s3]\nchecksum = true").unwrap();
        assert!(config.s3.as_ref().unwrap().capabilities.checksum);
        // The knob flows through the capability pipeline.
        let caps = Capabilities::from(config.s3.as_ref().unwrap());
        assert!(caps.checksum);
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-config checksum_defaults_off_and_parses`
Expected: FAIL — `checksum` field does not exist.

- [x] **Step 3: Add the field**

In `Capabilities` (after `allow_zero_page_size`, the `#[serde(default)] #[default = false]` pattern — same shape as :72-74):

```rust
    /// Validate and echo `x-amz-checksum-*` on multipart uploads
    /// (spec 2026-08-31). Default false: checksums are accepted and
    /// dropped, exactly as before.
    #[serde(default)]
    #[default = false]
    pub checksum: bool,
```

No default fn is needed (`#[serde(default)]` falls back to `Default::default()`, the `allow_zero_page_size` precedent). `From<&Config>` picks the field up automatically (flattened).

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-config`
Expected: PASS (new test + the existing suite, incl. the `capabilities_flatten_into_s3_section` test — untouched).

- [x] **Step 5: Report**

Leave in the tree. Report the new `Capabilities.checksum` field + test.

---

### Task 3: MultipartOps deltas + fs/mem persistence + conformance

The contract change and both backends land together (they compile as one unit). `upload_part` and `complete_multipart_upload` signatures stay **unchanged** (spec decisions Q7).

**Files:**
- Modify: `crates/tinio-core/src/storage/multipart.rs` (trait), `crates/tinio-core/src/multipart.rs` (`PartInfo`, `MultipartUpload` + doctests)
- Modify: `crates/tinio-fs/src/database/tables.rs`, `crates/tinio-fs/src/multipart.rs` (Store), `crates/tinio-fs/src/backend/multipart.rs` (trait impl)
- Modify: `crates/tinio-mem/src/storage.rs` (tables + `remove_all_parts`), `crates/tinio-mem/src/multipart.rs`
- Modify: `crates/tinio-util/src/testing.rs` (`conformance_multipart`)
- Test: `crates/tinio-util/src/testing.rs` (conformance block), fs/mem suites run it

**Interfaces:**
- Consumes: Task 1 types.
- Produces:
  - `MultipartOps::create_multipart_upload(&self, bucket, key, checksum: Option<UploadChecksum>)`
  - `MultipartOps::get_multipart_upload(&self, bucket, key, upload_id: &str) -> Result<MultipartUpload, Error>`
  - `MultipartOps::set_part_checksum(&self, bucket, key, upload_id: &str, part_number: PartNumber, checksum: PartChecksum) -> Result<(), Error>`
  - `PartInfo.checksum: Option<PartChecksum>`; `MultipartUpload.checksum_algorithm: Option<ChecksumAlgorithm>`, `MultipartUpload.checksum_type: Option<ChecksumType>`
  - fs tables `UPLOAD_CHECKSUMS` / `PART_CHECKSUMS`; mem tables of the same names.

- [x] **Step 1: Contract deltas**

In `crates/tinio-core/src/storage/multipart.rs`:

1. `create_multipart_upload` gains a third param (doc: "persisted; echoed by `get_multipart_upload`/`list_multipart_uploads`"):

```rust
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<UploadChecksum>,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage;
```

2. After `upload_part`, add:

```rust
    /// The persisted upload state (create-time checksum algorithm/type
    /// included). `NoSuchUpload` when the upload does not exist or the
    /// key does not match.
    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, <Self as Storage>::Error>
    where
        Self: Storage;

    /// Store the part's checksum value (upsert). The value is the
    /// server-computed digest — the backends never hash, they only
    /// persist. `NoSuchUpload` when the upload does not exist.
    async fn set_part_checksum(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        checksum: PartChecksum,
    ) -> Result<(), <Self as Storage>::Error>
    where
        Self: Storage;
```

Import `UploadChecksum`/`PartChecksum` in the `crate::{…}` use at the top of the trait file.

3. In `crates/tinio-core/src/multipart.rs`: add fields to the two structs and update their doctest examples (the examples construct the structs — they break without the new fields):

```rust
pub struct MultipartUpload {
    pub upload_id: String,
    pub bucket: bucket::Name,
    pub key: object::Key,
    pub initiated_at: SystemTime,
    /// The create-time checksum algorithm (`None` = no checksum upload).
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// The create-time full-object checksum type.
    pub checksum_type: Option<ChecksumType>,
}

pub struct PartInfo {
    pub part_number: PartNumber,
    pub size: u64,
    pub etag: ETag,
    pub last_modified: SystemTime,
    /// The stored checksum of the part (`None` = none was computed).
    pub checksum: Option<PartChecksum>,
}
```

Doctest examples gain `checksum_algorithm: None, checksum_type: None` and `checksum: None`.

Run: `cargo test -p tinio-core` — Expected: FAIL to compile (trait impls in fs/mem now mismatch).

- [x] **Step 2: fs tables**

In `crates/tinio-fs/src/database/tables.rs`, after the `PARTS` section (:400-410):

```rust
// --- UPLOAD_CHECKSUMS ---

/// `(bucket, upload_id)` → `(algorithm wire name, checksum-type wire
/// name or "")` — the upload's create-time checksum spec (spec
/// 2026-08-31). `""` for a checksum type that was never fixed.
type UploadChecksumValue = (&'static str, &'static str);
const UPLOAD_CHECKSUMS: TableDefinition<UploadKey, UploadChecksumValue> =
    TableDefinition::new("upload_checksums");

/// Handle to the `UPLOAD_CHECKSUMS` table (writable or read-only).
pub struct UploadChecksumsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(UploadChecksumsTable, UPLOAD_CHECKSUMS, UploadKey, UploadChecksumValue);

impl<'txn, T> UploadChecksumsTable<'txn, T>
where
    T: ReadableTable<UploadKey, UploadChecksumValue>,
{
    /// The stored row: `(algorithm wire name, checksum-type wire name or "")`.
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
    ) -> Result<Option<(&str, &str)>, Error> {
        Ok(self.0.get((bucket, upload_id))?.map(|v| v.value()))
    }
}

impl<'txn, T> UploadChecksumsTable<'txn, T>
where
    T: Table<UploadKey, UploadChecksumValue>,
{
    /// Insert or replace the upload's checksum spec.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        algorithm: &str,
        checksum_type: &str,
    ) -> Result<(), Error> {
        self.0.insert((bucket, upload_id), (algorithm, checksum_type))?;
        Ok(())
    }

    /// Remove the row (idempotent).
    pub fn remove(&mut self, bucket: &str, upload_id: &str) -> Result<(), Error> {
        self.0.remove((bucket, upload_id))?;
        Ok(())
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, ""), |b, _| b == bucket)
    }
}

// --- PART_CHECKSUMS ---

/// `(bucket, upload_id, part_number)` → `(algorithm wire name, base64
/// value)` — one part's computed checksum (spec 2026-08-31).
type PartChecksumValue = (&'static str, &'static str);
const PART_CHECKSUMS: TableDefinition<PartKey, PartChecksumValue> =
    TableDefinition::new("part_checksums");

/// Handle to the `PART_CHECKSUMS` table (writable or read-only).
pub struct PartChecksumsTable<'txn, T>(T, PhantomData<&'txn ()>);

table_impl!(PartChecksumsTable, PART_CHECKSUMS, PartKey, PartChecksumValue);

impl<'txn, T> PartChecksumsTable<'txn, T>
where
    T: ReadableTable<PartKey, PartChecksumValue>,
{
    /// The stored row: `(algorithm wire name, base64 value)`.
    pub fn get(
        &self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<Option<(&str, &str)>, Error> {
        Ok(self.0.get((bucket, upload_id, part_number))?.map(|v| v.value()))
    }
}

impl<'txn, T> PartChecksumsTable<'txn, T>
where
    T: Table<PartKey, PartChecksumValue>,
{
    /// Insert or replace the part's checksum.
    pub fn put(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
        algorithm: &str,
        value: &str,
    ) -> Result<(), Error> {
        self.0.insert((bucket, upload_id, part_number), (algorithm, value))?;
        Ok(())
    }

    /// Remove the part's checksum row (idempotent — re-upload clears the
    /// stale value).
    pub fn remove(
        &mut self,
        bucket: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<(), Error> {
        self.0.remove((bucket, upload_id, part_number))?;
        Ok(())
    }

    /// Delete every row of one upload (mirror `PartsTable::drain_upload`).
    pub fn drain_upload(
        &mut self,
        bucket: &bucket::Name,
        upload_id: &str,
    ) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, upload_id, u32::MIN), |b, _| {
            b == bucket
        })
    }

    /// Delete every row of `bucket` (bucket teardown).
    pub fn drain_bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = &**bucket;
        drain_pair(&mut self.0, (bucket, "", u32::MIN), |b, _, _| b == bucket)
    }
}
```

Note: check `PartsTable::drain_upload` / `drain_pair` in this file and mirror their exact signatures (`drain_pair` is the existing range-drain helper used at :309-313, :420-427); `drain_bucket` on `PartsTable` exists — mirror it for `PartChecksumsTable::drain_bucket`.

Import the two new handles in `crates/tinio-fs/src/multipart.rs` (`database::{…, PartChecksumsTable, UploadChecksumsTable}`) and in `database/tables.rs`'s own exports as needed by the store.

- [x] **Step 3: fs Store — create / get / set / publish-clear**

In `crates/tinio-fs/src/multipart.rs`:

1. `Store::create` gains the param and writes the checksum row in the same txn (:230-266):

```rust
    pub async fn create(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<UploadChecksum>,
    ) -> Result<MultipartUpload, Error> {
        // … existing live-upload cap and UUID generation unchanged …
        let checksum_algorithm = checksum.as_ref().map(|c| c.algorithm);
        let checksum_type = checksum.as_ref().and_then(|c| c.checksum_type);
        let upload = MultipartUpload {
            upload_id: Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
            checksum_algorithm,
            checksum_type,
        };
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload.upload_id.clone();
        let initiated_at = upload.initiated_at;
        let checksum_row = checksum.map(|c| (c.algorithm.as_wire().to_string(),
            c.checksum_type.map_or_else(String::new, |t| t.as_wire().to_string())));
        self.handle
            .write(move |txn| {
                UploadsTable::open(txn)?.put(&bucket, &upload_id, &key, initiated_at)?;
                if let Some((algo, ty)) = checksum_row {
                    UploadChecksumsTable::open(txn)?.put(&bucket, &upload_id, &algo, &ty)?;
                }
                Ok(())
            })
            .await
            .map_err(Error::from)?;
        Ok(upload)
    }
```

2. New store methods (place next to `create`/`put_part`):

```rust
    /// The upload's persisted state (create-time checksum spec included).
    /// `NoSuchUpload` when absent or the key does not match.
    pub async fn get_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.to_string();
        self.handle
            .read(move |txn| {
                let uploads = UploadsTable::open_readonly(txn)?;
                if !uploads.key_matches(&bucket, &key, &upload_id)? {
                    return Ok(None);
                }
                let (stored_key, initiated_at) =
                    uploads.get(&bucket, &upload_id)?.expect("key_matches implies the row");
                let (algorithm, checksum_type) =
                    UploadChecksumsTable::open_readonly(txn)?.get(&bucket, &upload_id)?;
                let checksum_algorithm =
                    algorithm.and_then(ChecksumAlgorithm::from_wire);
                let checksum_type = checksum_type
                    .filter(|t| !t.is_empty())
                    .and_then(ChecksumType::from_wire);
                Ok(Some(MultipartUpload {
                    upload_id: upload_id.clone(),
                    bucket: bucket.clone(),
                    key: object::Key::new(stored_key.to_string())?,
                    initiated_at: crate::_core::from_nanos(initiated_at),
                    checksum_algorithm,
                    checksum_type,
                }))
            })
            .await
            .map_err(Error::from)?
            .ok_or_else(|| storage::no_such_upload(&upload_id).into())
    }

    /// Persist one part's computed checksum (upsert). `NoSuchUpload`
    /// when the upload is gone.
    pub async fn set_part_checksum(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        checksum: PartChecksum,
    ) -> Result<(), Error> {
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.to_string();
        let n = u32::from(part_number);
        let algo = checksum.algorithm.as_wire().to_string();
        let value = checksum.value.as_str().to_string();
        let recorded = self
            .handle
            .write(move |txn| {
                let uploads = UploadsTable::open(txn)?;
                if !uploads.key_matches(&bucket, &key, &upload_id)? {
                    return Ok(false);
                }
                PartChecksumsTable::open(txn)?.put(&bucket, &upload_id, n, &algo, &value)?;
                Ok(true)
            })
            .await
            .map_err(Error::from)?;
        if recorded {
            Ok(())
        } else {
            Err(storage::no_such_upload(&upload_id).into())
        }
    }
```

Verify `UploadsTable` has a `get(bucket, upload_id)` read method — if it only has `key_matches`/`for_bucket`, add `get` to the readable impl mirroring `PartsTable::get_hex`'s shape (check the existing handle at :299-330; `object::Key::new` is the checked key constructor — confirm its name in `tinio-core/src/object.rs` and use it).

3. `publish_part`'s write txn (:372-385) clears the stale checksum row — after the `PartsTable::put`:

```rust
                drop(uploads);
                PartsTable::open(txn)?.put(&bucket, &upload_id_owned, n, &etag_owned)?;
                PartChecksumsTable::open(txn)?.remove(&bucket, &upload_id_owned, n)?;
                Ok(true)
```

`publish_part`'s `PartInfo` construction gains `checksum: None` (the part was just uploaded; the server persists the checksum via `set_part_checksum` afterwards).

4. `drain_upload` (:72-83) drains the new tables:

```rust
pub(crate) fn drain_upload(
    txn: &mut redb::WriteTransaction,
    bucket: &bucket::Name,
    upload_id: &str,
) -> Result<(), database::Error> {
    UploadsTable::open(txn)?.remove(bucket, upload_id)?;
    UploadChecksumsTable::open(txn)?.remove(bucket, upload_id)?;
    PartsTable::open(txn)?.drain_upload(bucket, upload_id)?;
    PartChecksumsTable::open(txn)?.drain_upload(bucket, upload_id)?;
    Ok(())
}
```

5. `drain_bucket_uploads` (:85-98, bucket teardown) gains `UploadChecksumsTable::drain_bucket` + `PartChecksumsTable::drain_bucket`; check the function's full body and mirror its existing `drain_bucket` calls for the `PARTS` half.

6. `list_parts` (:438-518): the read-txn closure additionally joins `PART_CHECKSUMS`. Change the closure's returned page type to `Vec<(u32, String, Option<(String, String)>)>` — after `list_from`, fetch each row's checksum:

```rust
                let (recorded, truncated) = PartsTable::open_readonly(txn)?
                    .list_from(bucket, upload_id, start, max_parts)?;
                let checksums = PartChecksumsTable::open_readonly(txn)?;
                let page = recorded
                    .into_iter()
                    .map(|(n, hex)| {
                        let cs = checksums
                            .get(bucket, upload_id, n)?
                            .map(|(a, v)| (a.to_string(), v.to_string()));
                        Ok((n, hex, cs))
                    })
                    .collect::<Result<Vec<_>, database::Error>>()?;
                Ok((true, page, truncated))
```

`raw_last` becomes `page.last().map(|(n, _, _)| *n)`; pass 2 (:475-495) builds the checksum:

```rust
            let checksum = checksum_row.and_then(|(algo, value)| {
                Some(PartChecksum {
                    algorithm: ChecksumAlgorithm::from_wire(&algo)?,
                    value: ChecksumValue(value),
                })
            });
            parts.push(PartInfo {
                part_number: n.into(),
                size: metadata.len(),
                etag,
                last_modified: metadata.modified()?,
                checksum,
            });
```

7. `list_uploads_page` (:833-880): the read-txn closure also fetches `UPLOAD_CHECKSUMS` per row — rows become `Vec<(String, String, u64, Option<(String, String)>)>`; `upload_from_row` (find it in this file) gains a `checksum_algorithm: Option<ChecksumAlgorithm>` / `checksum_type: Option<ChecksumType>` parameter and sets the new `MultipartUpload` fields.

- [x] **Step 4: fs backend trait impl**

In `crates/tinio-fs/src/backend/multipart.rs`:

1. `create_multipart_upload` (:76-92) gains the param and forwards it:

```rust
    async fn create_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        checksum: Option<UploadChecksum>,
    ) -> Result<MultipartUpload, Error> {
        // … existing guards unchanged …
        self.multipart_store.create(bucket, key, checksum).await
    }
```

2. New impls (mirror `upload_part`'s guards — `ensure_bucket` + reserved-key → `access_denied`):

```rust
    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        self.multipart_store.get_upload(bucket, key, upload_id).await
    }

    async fn set_part_checksum(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        checksum: PartChecksum,
    ) -> Result<(), Error> {
        self.ensure_bucket(bucket).await?;
        if key.is_reserved() {
            return Err(access_denied(key).into());
        }
        self.multipart_store
            .set_part_checksum(bucket, key, upload_id, part_number, checksum)
            .await
    }
```

Update the `crate::_core::{…}` import list with `checksum::{PartChecksum, UploadChecksum}`.

- [x] **Step 5: mem tables + flows**

In `crates/tinio-mem/src/storage.rs` (:47-62):

```rust
/// `bucket\0key\0upload_id` → `(algorithm wire name, checksum-type wire name or "")`.
pub(crate) const UPLOAD_CHECKSUMS: TableDefinition<&str, (&str, &str)> =
    TableDefinition::new("upload_checksums");
/// `upload_id\0part_number` → `(algorithm wire name, base64 value)`.
pub(crate) const PART_CHECKSUMS: TableDefinition<&str, (&str, &str)> =
    TableDefinition::new("part_checksums");
```

In `crates/tinio-mem/src/multipart.rs`:

1. `create_multipart_upload` gains the param; the struct fields set from it; the write txn inserts `UPLOAD_CHECKSUMS` (same key as `UPLOADS`, `upload_key(...)`) when `Some`:

```rust
        let upload = MultipartUpload {
            upload_id: Uuid::new_v4().to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: SystemTime::now(),
            checksum_algorithm: checksum.as_ref().map(|c| c.algorithm),
            checksum_type: checksum.as_ref().and_then(|c| c.checksum_type),
        };
        // … in the write txn, after the UPLOADS insert:
        if let Some(c) = checksum {
            let mut cs = txn.open_table(UPLOAD_CHECKSUMS)?;
            cs.insert(
                upload_key(
                    upload.bucket.as_ref().as_str(),
                    upload.key.as_ref().as_str(),
                    &upload.upload_id,
                )
                .as_str(),
                (
                    c.algorithm.as_wire(),
                    c.checksum_type.map_or("", |t| t.as_wire()),
                ),
            )?;
        }
```

2. New impls (mirror `upload_part`'s guards — `has_bucket` + reserved key):

```rust
    async fn get_multipart_upload(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
    ) -> Result<MultipartUpload, Error> {
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        let txn = self.db.begin_read()?;
        {
            let uploads = txn.open_table(UPLOADS)?;
            check_upload(&uploads, upload_id, bucket, key)?;
        }
        let checksums = txn.open_table(UPLOAD_CHECKSUMS)?;
        let row = checksums
            .get(
                upload_key(
                    bucket.as_ref().as_str(),
                    key.as_ref().as_str(),
                    upload_id,
                )
                .as_str(),
            )?
            .map(|v| v.value());
        let initiated = txn
            .open_table(UPLOADS)?
            .get(
                upload_key(bucket.as_ref().as_str(), key.as_ref().as_str(), upload_id).as_str(),
            )?
            .expect("check_upload implied the row")
            .value();
        Ok(MultipartUpload {
            upload_id: upload_id.to_string(),
            bucket: bucket.clone(),
            key: key.clone(),
            initiated_at: from_nanos(initiated),
            checksum_algorithm: row.and_then(|(a, _)| ChecksumAlgorithm::from_wire(a)),
            checksum_type: row
                .and_then(|(_, t)| (!t.is_empty()).then(|| t))
                .and_then(ChecksumType::from_wire),
        })
    }

    async fn set_part_checksum(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        upload_id: &str,
        part_number: PartNumber,
        checksum: PartChecksum,
    ) -> Result<(), Error> {
        if !self.has_bucket(bucket)? {
            return Err(no_such_bucket(bucket));
        }
        if key.is_reserved() {
            return Err(access_denied(key));
        }
        let txn = self.db.begin_write()?;
        {
            let uploads = txn.open_table(UPLOADS)?;
            check_upload(&uploads, upload_id, bucket, key)?;
        }
        {
            let mut cs = txn.open_table(PART_CHECKSUMS)?;
            cs.insert(
                part_key(upload_id, u32::from(part_number)).as_str(),
                (checksum.algorithm.as_wire(), checksum.value.as_str()),
            )?;
        }
        txn.commit()?;
        Ok(())
    }
```

3. `upload_part`'s write txn (:104-131) clears the stale part checksum row (spec Q7 — a re-upload must not keep the old value):

```rust
            parts.insert(pk.as_str(), data.as_slice())?;
            meta.insert(pk.as_str(), (etag_str.as_str(), data.len() as u64, now))?;
            txn.open_table(PART_CHECKSUMS)?.remove(pk.as_str())?;
```

`PartInfo` construction gains `checksum: None`.

4. `list_parts` (:139-220): after the `PART_META` scan, join `PART_CHECKSUMS` per part — the mapped tuple becomes `(PartInfo, Option<PartChecksum>)` and the final `PartInfo.checksum` is set:

```rust
                let (etag, size, mtime) = v.value();
                let checksum_row = checksums
                    .get(k.value())?
                    .map(|v| v.value())
                    .and_then(|(a, v)| Some(PartChecksum {
                        algorithm: ChecksumAlgorithm::from_wire(a)?,
                        value: ChecksumValue(v.to_string()),
                    }));
                Ok((
                    PartInfo {
                        part_number: part_number.into(),
                        size,
                        etag: etag.parse().map_err(invalid_etag)?,
                        last_modified: from_nanos(mtime),
                        checksum: checksum_row,
                    },
                ))
```

Open `PART_CHECKSUMS` in the same read txn (before the `PART_META` range scan). Note `k.value()` is the full `upload_id\0part_number` key — `PART_CHECKSUMS` uses the identical key, so `get(k.value())` works.

5. `complete_multipart_upload` (:209-270): drain the new tables in the same txn — alongside the `UPLOADS` remove add `UPLOAD_CHECKSUMS` remove (same key); the parts drain currently calls `remove_all_parts(&mut stored_parts, &mut stored_meta, &prefix)` — extend `remove_all_parts` in `crates/tinio-mem/src/storage.rs` to take a third table and drain `PART_CHECKSUMS` with the same prefix (update its call site). `infos` PartInfo constructions gain `checksum: None`.

6. `abort_multipart_upload` (:300+): drain `UPLOAD_CHECKSUMS` + `PART_CHECKSUMS` alongside the existing removes (mirror the `UPLOADS`/`PARTS` removes in the same txn).

7. `list_multipart_uploads` (:346+): `UploadRow` gains `checksum_algorithm: Option<ChecksumAlgorithm>` / `checksum_type: Option<ChecksumType>`; the scan joins `UPLOAD_CHECKSUMS` (same `upload_key` as `UPLOADS`) and the `MultipartUpload` construction sets the new fields.

Update the mem import list with `checksum::{ChecksumAlgorithm, ChecksumType, ChecksumValue, PartChecksum, UploadChecksum}` and `storage::{PART_CHECKSUMS, UPLOAD_CHECKSUMS}`.

- [x] **Step 6: Conformance harness**

In `crates/tinio-util/src/testing.rs` `conformance_multipart` (:691+), after the existing lifecycle block, add a checksum-persistence block (the harness drives both backends; values are sentinels — persistence is the contract, the math is the server's):

```rust
    // Checksum persistence through the contract (spec 2026-08-31): the
    // backends store and return what the server hands over.
    let cs_big = object::key("checksum.bin").unwrap();
    let cs_upload = storage
        .create_multipart_upload(
            b,
            &cs_big,
            Some(UploadChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                checksum_type: Some(ChecksumType::FullObject),
            }),
        )
        .await
        .unwrap();
    check(
        cs_upload.checksum_algorithm == Some(ChecksumAlgorithm::Crc32)
            && cs_upload.checksum_type == Some(ChecksumType::FullObject),
        "create must echo the persisted checksum spec",
    );
    let fetched = storage
        .get_multipart_upload(b, &cs_big, &cs_upload.upload_id)
        .await
        .unwrap();
    check(
        fetched.checksum_algorithm == Some(ChecksumAlgorithm::Crc32),
        "get_multipart_upload must return the persisted algorithm",
    );
    let cs_part = storage
        .upload_part(b, &cs_big, &cs_upload.upload_id, 1.into(), body(b"abc"))
        .await
        .unwrap();
    let value = ChecksumValue("y/Q5Jg==".into());
    storage
        .set_part_checksum(
            b,
            &cs_big,
            &cs_upload.upload_id,
            1.into(),
            PartChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                value: value.clone(),
            },
        )
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: cs_big.clone(),
            upload_id: cs_upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(
        listing.parts[0].checksum
            == Some(PartChecksum {
                algorithm: ChecksumAlgorithm::Crc32,
                value: value.clone(),
            }),
        "list_parts must echo the stored part checksum",
    );
    check(
        cs_part.checksum.is_none(),
        "upload_part returns no checksum (persisted via set_part_checksum)",
    );
    // A re-upload clears the stale checksum row (spec Q7).
    storage
        .upload_part(b, &cs_big, &cs_upload.upload_id, 1.into(), body(b"xyz"))
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: cs_big.clone(),
            upload_id: cs_upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(
        listing.parts[0].checksum.is_none(),
        "a re-uploaded part must not keep a stale checksum",
    );
    // Abort drains the new tables (get_multipart_upload → NoSuchUpload).
    storage
        .abort_multipart_upload(b, &cs_big, &cs_upload.upload_id)
        .await
        .unwrap();
    let err = into_core_error(
        storage
            .get_multipart_upload(b, &cs_big, &cs_upload.upload_id)
            .await
            .unwrap_err(),
    );
    check(matches!(err, NoSuchUpload(_)), "abort must drain the checksum rows");
```

Also fix the existing call: `create_multipart_upload(b, &big)` → `create_multipart_upload(b, &big, None)` (and any other call sites of the changed signature in `testing.rs`). Update tinio-util's import list with the checksum types.

- [x] **Step 7: Add the fs staging-abort test (grilling Q3)**

The correctness claim "a mismatching verifying stream aborts staging so the part is never stored" is only exercised against MemoryStorage by the server tests — pin it for the production backend too. Add to the test module of `crates/tinio-fs/src/backend/multipart.rs` (the module already imports `crate::_util::testing::{body, read_body}` — add `futures::stream` and `bytes::Bytes` to the imports; `io` for the error):

```rust
    #[tokio::test]
    async fn upload_part_stream_error_aborts_staging() {
        // A body stream that errors mid-way (the checksum tee fails the
        // stream the same way) must leave no part file and no PARTS row.
        let (_root, storage) = storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        let k = object::key("big.bin").unwrap();
        let upload = storage.create_multipart_upload(&b, &k, None).await.unwrap();
        let err = storage
            .upload_part(
                &b,
                &k,
                &upload.upload_id,
                1.into(),
                Box::pin(stream::iter(vec![
                    Ok::<_, std::io::Error>(Bytes::from_static(b"x")),
                    Err(std::io::Error::other("boom")),
                ])),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Io(_)), "{err:?}");
        let listing = storage
            .list_parts(ListPartsParams {
                bucket: b.clone(),
                key: k.clone(),
                upload_id: upload.upload_id.clone(),
                max_parts: 1000,
                part_number_marker: None,
            })
            .await
            .unwrap();
        assert!(listing.parts.is_empty(), "a failed stream must not commit a part");
        storage
            .abort_multipart_upload(&b, &k, &upload.upload_id)
            .await
            .unwrap();
    }
```

(The `upload_part` body parameter is the contract `BodyStream` — `Box::pin` of a `stream::iter` of `io::Result<Bytes>` matches it. The part directory is created by the failed stage — the abort cleans it up.)

- [x] **Step 8: Run the suites**

Run: `cargo test -p tinio-core -p tinio-fs -p tinio-mem`
Expected: compile errors at first — fix each mechanical site the compiler points to (any remaining `MultipartUpload`/`PartInfo` constructor without the new fields, e.g. tests constructing `MultipartUpload` directly — search `MultipartUpload {` and `PartInfo {` across the workspace and add the fields). Then PASS: the new conformance block runs under both backends (fs + mem suites drive `assert_conformance`) and the fs staging-abort test passes.

- [x] **Step 9: Report**

Leave in the tree. Report: contract deltas, fs/mem tables + flows, conformance block + fs staging-abort test green on both backends.

---

### Task 4: tinio-server `backend/checksum.rs` — VerifyStream, ChecksumSpec, composition

**Files:**
- Create: `crates/tinio-server/src/backend/checksum.rs`
- Modify: `crates/tinio-server/src/backend/mod.rs` (module decl + re-export)
- Test: `crates/tinio-server/src/backend/checksum.rs` (module test)

**Interfaces:**
- Consumes: Task 1 types; `s3s::checksum::ChecksumHasher`, `s3s::crypto::{Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256}`, `s3s::TrailingHeaders`, `base64 0.22`.
- Produces (used by Tasks 5–8):
  - `pub(crate) struct VerifyState { … }` with `computed() -> Option<ChecksumValue>` and `mismatched() -> bool`
  - `pub(crate) struct ChecksumSpec { algorithm: Option<ChecksumAlgorithm>, expected: Option<ChecksumValue>, content_md5: Option<ChecksumValue> }`
  - `ChecksumSpec::from_upload_part(&dto::UploadPartInput, Option<&TrailingHeaders>) -> Result<Option<ChecksumSpec>, S3Error>`
  - `ChecksumSpec::from_headers(&http::HeaderMap, Option<&TrailingHeaders>) -> Result<Option<ChecksumSpec>, S3Error>` (raw-header variant for UploadPartCopy)
  - `VerifyStream::wrap(body: BodyStream, spec: &ChecksumSpec, state: &Arc<VerifyState>) -> BodyStream`
  - `compose_composite(algo, &[PartChecksum]) -> Option<ChecksumValue>`
  - `linearize_full_object(algo, &[PartChecksum]) -> Option<ChecksumValue>`
  - `pub(crate) fn algo_value_field(algo) -> fn(&dto::Checksum) -> Option<&str>`-style extractor helpers + `set_output_field` helpers for the response echo (exact shape below).

- [x] **Step 1: Write the failing test**

Create `crates/tinio-server/src/backend/checksum.rs` with the test module first (the module is empty until Step 3 — the tests fail to compile, driving the implementation):

```rust
//! Server-side checksum validation for multipart uploads (spec
//! 2026-08-31-multipart-checksum-validation-design.md).
//!
//! One home for every checksum computation of the mapping layer: the
//! [`VerifyStream`] body tee (s3s `ChecksumHasher`, md5 + the requested
//! algorithm enabled together — one streaming pass with the backend's
//! staging write), the request-spec parsing, and the COMPOSITE /
//! FULL_OBJECT full-object derivations. The storage backends never
//! hash.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use s3s::{
    S3Error, S3Result, s3_error,
    checksum::ChecksumHasher,
    crypto::{Crc32, Crc32c, Crc64Nvme, Md5, Sha1, Sha256},
    dto,
};
use tracing::warn;

use crate::_core::{
    BodyStream, checksum::{ChecksumAlgorithm, ChecksumType, ChecksumValue, PartChecksum},
};

// … (Step 3 fills the implementation between the header and the tests) …

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    /// The wire base64 of a raw digest.
    fn b64(bytes: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    }

    /// The raw digest of one CRC algorithm over `data` — the s3s
    /// hasher, independent of the linearization code under test.
    fn crc_raw(algo: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
        let mut h = ChecksumHasher::default();
        match algo {
            ChecksumAlgorithm::Crc32 => h.crc32 = Some(Crc32::new()),
            ChecksumAlgorithm::Crc32C => h.crc32c = Some(Crc32c::new()),
            ChecksumAlgorithm::Crc64Nvme => h.crc64nvme = Some(Crc64Nvme::new()),
            _ => unreachable!("CRC only"),
        }
        h.update(data);
        let sum = checksum_value_of(&h.finalize(), algo).unwrap();
        base64::engine::general_purpose::STANDARD.decode(sum).unwrap()
    }

    #[test]
    fn linearize_matches_the_direct_crc_of_concatenated_content() {
        // The self-validating oracle: split random content into random
        // parts, CRC each part, linearize, and compare with the direct
        // CRC of the concatenation — they must be identical for every
        // CRC algorithm. A wrong polynomial constant or endianness
        // convention fails this test.
        let mut content = Vec::new();
        let mut state = 0x9E3779B97F4A7C15u64; // deterministic PRNG
        for _ in 0..4096 {
            state ^= state << 13; state ^= state >> 7; state ^= state << 17;
            content.push(state as u8);
        }
        for algo in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
            ChecksumAlgorithm::Crc64Nvme,
        ] {
            let mut parts: Vec<PartChecksum> = Vec::new();
            let mut sizes: Vec<u64> = Vec::new();
            let mut cursor = 0usize;
            let mut cut = 0u64;
            while cursor < content.len() {
                cut = cut.wrapping_mul(31).wrapping_add(97) % 700;
                let end = (cursor + cut as usize + 1).min(content.len());
                let part = &content[cursor..end];
                sizes.push((end - cursor) as u64);
                parts.push(PartChecksum {
                    algorithm: algo,
                    value: ChecksumValue(b64(&crc_raw(algo, part))),
                });
                cursor = end;
            }
            let linearized = linearize_full_object(algo, &parts, &sizes).unwrap();
            let direct = b64(&crc_raw(algo, &content));
            assert_eq!(linearized.as_str(), direct, "algorithm {}", algo.as_wire());
        }
    }

    #[test]
    fn known_crc32_check_value() {
        // The standard CRC-32/IEEE check value: crc32("123456789") =
        // 0xCBF43926 → base64 "y/Q5Jg==".
        let mut h = ChecksumHasher { crc32: Some(Crc32::new()), ..Default::default() };
        h.update(b"123456789");
        assert_eq!(h.finalize().checksum_crc32.unwrap(), "y/Q5Jg==");
    }

    #[test]
    fn compose_composite_is_the_algorithm_over_concatenated_digests() {
        let mut h = ChecksumHasher { sha256: Some(Sha256::new()), ..Default::default() };
        h.update(b"alpha");
        let a = h.finalize().checksum_sha256.unwrap();
        let mut h = ChecksumHasher { sha256: Some(Sha256::new()), ..Default::default() };
        h.update(b"beta");
        let b = h.finalize().checksum_sha256.unwrap();
        let parts = vec![
            PartChecksum { algorithm: ChecksumAlgorithm::Sha256, value: ChecksumValue(a.clone()) },
            PartChecksum { algorithm: ChecksumAlgorithm::Sha256, value: ChecksumValue(b.clone()) },
        ];
        let composed = compose_composite(ChecksumAlgorithm::Sha256, &parts).unwrap();
        // The documented construction: SHA-256 over the concatenation of
        // the RAW part digests.
        let mut raw = Vec::new();
        raw.extend_from_slice(&base64::engine::general_purpose::STANDARD.decode(&a).unwrap());
        raw.extend_from_slice(&base64::engine::general_purpose::STANDARD.decode(&b).unwrap());
        let mut h = ChecksumHasher { sha256: Some(Sha256::new()), ..Default::default() };
        h.update(&raw);
        assert_eq!(composed.as_str(), h.finalize().checksum_sha256.unwrap());
    }
}
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server checksum::tests`
Expected: FAIL — the module is not wired into the crate yet.

- [x] **Step 3: Implement the module**

Fill the module between the header and the tests:

```rust
/// The shared outcome of a [`VerifyStream`]: the computed digest and
/// whether an expected value failed to match.
#[derive(Debug, Default)]
pub(crate) struct VerifyState {
    computed: Mutex<Option<ChecksumValue>>,
    mismatched: AtomicBool,
}

impl VerifyState {
    /// The computed digest of the wrapped stream (finalized at stream
    /// end; `None` until then).
    pub(crate) fn computed(&self) -> Option<ChecksumValue> {
        self.computed.lock().ok().and_then(|g| g.clone())
    }

    /// Whether the stream ended with a checksum mismatch (the op maps
    /// the backend's wrapped error to `BadDigest` when this is set).
    pub(crate) fn mismatched(&self) -> bool {
        self.mismatched.load(Ordering::Relaxed)
    }
}

/// One checksum specification of an upload-part-like request (spec
/// 2026-08-31).
#[derive(Debug, Clone)]
pub(crate) struct ChecksumSpec {
    /// The algorithm whose computed value is persisted/echoed. `None`
    /// when only `Content-MD5` is present.
    pub(crate) algorithm: Option<ChecksumAlgorithm>,
    /// The expected value of `algorithm` (`None` = compute-only).
    pub(crate) expected: Option<ChecksumValue>,
    /// The legacy `Content-MD5` (validated, never persisted).
    pub(crate) content_md5: Option<ChecksumValue>,
}

impl ChecksumSpec {
    /// Parse the checksum sources of an `UploadPart` request: exactly
    /// one `checksum_<algo>` DTO field, or a trailing-checksum header
    /// (aws-chunked; s3s verified the trailer signature), or nothing.
    /// `Content-MD5` is an independent legacy check that may coexist.
    /// More than one algorithm value → `InvalidRequest`; an
    /// `x-amz-checksum-algorithm` header with no value at all →
    /// `InvalidRequest` (S3: 400).
    pub(crate) fn from_upload_part(
        input: &dto::UploadPartInput,
        trailing: Option<&TrailingHeaders>,
    ) -> S3Result<Option<ChecksumSpec>> {
        Self::parse(
            [
                (ChecksumAlgorithm::Crc32, input.checksum_crc32.as_deref()),
                (ChecksumAlgorithm::Crc32C, input.checksum_crc32c.as_deref()),
                (ChecksumAlgorithm::Crc64Nvme, input.checksum_crc64nvme.as_deref()),
                (ChecksumAlgorithm::Sha1, input.checksum_sha1.as_deref()),
                (ChecksumAlgorithm::Sha256, input.checksum_sha256.as_deref()),
                (ChecksumAlgorithm::Md5, input.checksum_md5.as_deref()),
            ],
            input.content_md5.as_deref(),
            input.checksum_algorithm.as_deref(),
            trailing,
        )
    }

    /// The raw-header variant for `UploadPartCopy` (the s3s DTO has no
    /// checksum fields — spec Q8).
    pub(crate) fn from_headers(
        headers: &http::HeaderMap,
        trailing: Option<&TrailingHeaders>,
    ) -> S3Result<Option<ChecksumSpec>> {
        let mut fields = [(ChecksumAlgorithm::Crc32, None), (ChecksumAlgorithm::Crc32C, None),
            (ChecksumAlgorithm::Crc64Nvme, None), (ChecksumAlgorithm::Sha1, None),
            (ChecksumAlgorithm::Sha256, None), (ChecksumAlgorithm::Md5, None)];
        for (algo, slot) in &mut fields {
            let name = http::header::HeaderName::from_static(checksum_header_name(*algo));
            *slot = headers.get(&name).and_then(|v| v.to_str().ok());
        }
        Self::parse(
            fields,
            None, // UploadPartCopy has no Content-MD5 path
            headers
                .get(http::header::HeaderName::from_static("x-amz-checksum-algorithm"))
                .and_then(|v| v.to_str().ok()),
            trailing,
        )
    }

    fn parse(
        fields: [(ChecksumAlgorithm, Option<&str>); 6],
        content_md5: Option<&str>,
        algorithm_header: Option<&str>,
        trailing: Option<&TrailingHeaders>,
    ) -> S3Result<Option<ChecksumSpec>> {
        let mut found: Option<(ChecksumAlgorithm, &str)> = None;
        for (algo, value) in fields {
            if let Some(value) = value {
                if found.is_some() {
                    return Err(s3_error!(
                        InvalidRequest,
                        "more than one checksum value in one request"
                    ));
                }
                found = Some((algo, value));
            }
        }
        // aws-chunked trailers: the value arrives in the verified
        // trailer, not the DTO.
        let mut trailer_algo: Option<(ChecksumAlgorithm, String)> = None;
        if let Some(trailing) = trailing {
            if let Some(map) = trailing.read(|m| m.clone()) {
                for algo in ChecksumAlgorithm::ALL {
                    let name = http::header::HeaderName::from_static(checksum_header_name(algo));
                    if let Some(value) = map.get(&name).and_then(|v| v.to_str().ok()) {
                        if found.is_some() {
                            return Err(s3_error!(
                                InvalidRequest,
                                "more than one checksum value in one request"
                            ));
                        }
                        trailer_algo = Some((algo, value.to_string()));
                    }
                }
            }
        }
        let value = match (found, trailer_algo) {
            (Some((algo, value)), None) => Some((algo, value.to_string())),
            (None, Some((algo, value))) => Some((algo, value)),
            (Some(_), Some(_)) => {
                return Err(s3_error!(
                    InvalidRequest,
                    "more than one checksum value in one request"
                ))
            }
            (None, None) => None,
        };
        // An algorithm header without any value → InvalidRequest (S3: 400).
        if algorithm_header.is_some() && value.is_none() {
            return Err(s3_error!(
                InvalidRequest,
                "checksum algorithm without a checksum value"
            ));
        }
        // Per AWS, an individual checksum wins over the algorithm header.
        let algorithm = value.map(|(a, _)| a);
        Ok(value.map(|(algo, v)| ChecksumSpec {
            algorithm,
            expected: Some(ChecksumValue(v)),
            content_md5: content_md5.map(|v| ChecksumValue(v.to_string())),
        }))
    }
}

/// The `x-amz-checksum-<algo>` request header name of an algorithm.
pub(crate) fn checksum_header_name(algo: ChecksumAlgorithm) -> &'static str {
    match algo {
        ChecksumAlgorithm::Crc32 => "x-amz-checksum-crc32",
        ChecksumAlgorithm::Crc32C => "x-amz-checksum-crc32c",
        ChecksumAlgorithm::Crc64Nvme => "x-amz-checksum-crc64nvme",
        ChecksumAlgorithm::Sha1 => "x-amz-checksum-sha1",
        ChecksumAlgorithm::Sha256 => "x-amz-checksum-sha256",
        ChecksumAlgorithm::Md5 => "x-amz-checksum-md5",
    }
}

/// The wrapper stream: update the hasher per chunk, finalize at stream
/// end, compare the expected values, and fail the stream on mismatch so
/// the consuming backend aborts staging (the part is never committed).
pub(crate) struct VerifyStream {
    inner: Pin<Box<dyn Stream<Item = io::Result<Bytes>> + Send + Sync>>,
    hasher: ChecksumHasher,
    algorithm: Option<ChecksumAlgorithm>,
    expected: Option<ChecksumValue>,
    content_md5: Option<ChecksumValue>,
    state: Arc<VerifyState>,
    finished: bool,
}

impl VerifyStream {
    /// Wrap `body` per `spec`; the shared `state` is finalized at
    /// stream end and readable by the op afterwards.
    pub(crate) fn wrap(
        body: BodyStream,
        spec: &ChecksumSpec,
        state: &Arc<VerifyState>,
    ) -> BodyStream {
        let mut hasher = ChecksumHasher::default();
        match spec.algorithm {
            Some(ChecksumAlgorithm::Crc32) => hasher.crc32 = Some(Crc32::new()),
            Some(ChecksumAlgorithm::Crc32C) => hasher.crc32c = Some(Crc32c::new()),
            Some(ChecksumAlgorithm::Crc64Nvme) => hasher.crc64nvme = Some(Crc64Nvme::new()),
            Some(ChecksumAlgorithm::Sha1) => hasher.sha1 = Some(Sha1::new()),
            Some(ChecksumAlgorithm::Sha256) => hasher.sha256 = Some(Sha256::new()),
            Some(ChecksumAlgorithm::Md5) => hasher.md5 = Some(Md5::new()),
            None => {}
        }
        if spec.content_md5.is_some() && spec.algorithm != Some(ChecksumAlgorithm::Md5) {
            hasher.md5 = Some(Md5::new());
        }
        Box::pin(VerifyStream {
            inner: body,
            hasher,
            algorithm: spec.algorithm,
            expected: spec.expected.clone(),
            content_md5: spec.content_md5.clone(),
            state: Arc::clone(state),
            finished: false,
        })
    }
}

impl Stream for VerifyStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                self.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                self.finished = true;
                let checksum = self.hasher.finalize();
                let computed = self
                    .algorithm
                    .and_then(|a| checksum_value_of(&checksum, a))
                    .map(|v| ChecksumValue(v.to_string()));
                if let Some(computed) = &computed
                    && let Ok(mut guard) = self.state.computed.lock()
                {
                    *guard = Some(computed.clone());
                }
                let md5 = checksum.checksum_md5.as_deref();
                let algo_ok = match (&self.expected, &computed) {
                    (Some(expected), Some(computed)) => expected.as_str() == computed.as_str(),
                    (None, _) => true,
                    (Some(_), None) => false,
                };
                let md5_ok = match (&self.content_md5, md5) {
                    (Some(expected), Some(computed)) => expected.as_str() == computed,
                    (None, _) => true,
                    (Some(_), None) => false,
                };
                if algo_ok && md5_ok {
                    Poll::Ready(None)
                } else {
                    self.state.mismatched.store(true, Ordering::Relaxed);
                    Poll::Ready(Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "checksum mismatch",
                    ))))
                }
            }
        }
    }
}
```

Then the composition helpers (the only hand-written crypto — one home):

```rust
/// The digest value of one algorithm inside a finalized
/// `s3s::checksum::ChecksumHasher` result.
pub(crate) fn checksum_value_of(
    checksum: &dto::Checksum,
    algo: ChecksumAlgorithm,
) -> Option<&str> {
    match algo {
        ChecksumAlgorithm::Crc32 => checksum.checksum_crc32.as_deref(),
        ChecksumAlgorithm::Crc32C => checksum.checksum_crc32c.as_deref(),
        ChecksumAlgorithm::Crc64Nvme => checksum.checksum_crc64nvme.as_deref(),
        ChecksumAlgorithm::Sha1 => checksum.checksum_sha1.as_deref(),
        ChecksumAlgorithm::Sha256 => checksum.checksum_sha256.as_deref(),
        ChecksumAlgorithm::Md5 => checksum.checksum_md5.as_deref(),
    }
}

/// Enable the algorithm's slot on a fresh hasher and finalize over the
/// given bytes.
fn hash_bytes(algo: ChecksumAlgorithm, bytes: &[u8]) -> Option<String> {
    let mut hasher = ChecksumHasher::default();
    match algo {
        ChecksumAlgorithm::Crc32 => hasher.crc32 = Some(Crc32::new()),
        ChecksumAlgorithm::Crc32C => hasher.crc32c = Some(Crc32c::new()),
        ChecksumAlgorithm::Crc64Nvme => hasher.crc64nvme = Some(Crc64Nvme::new()),
        ChecksumAlgorithm::Sha1 => hasher.sha1 = Some(Sha1::new()),
        ChecksumAlgorithm::Sha256 => hasher.sha256 = Some(Sha256::new()),
        ChecksumAlgorithm::Md5 => hasher.md5 = Some(Md5::new()),
    }
    hasher.update(bytes);
    checksum_value_of(&hasher.finalize(), algo).map(str::to_string)
}

/// COMPOSITE: the algorithm over the concatenation of the raw part
/// digest bytes (the documented S3 construction; the AWS Java example
/// applies it to SHA-256). `None` when any part value is not valid
/// base64 — the caller skips validation (deviation D2).
pub(crate) fn compose_composite(
    algo: ChecksumAlgorithm,
    parts: &[PartChecksum],
) -> Option<ChecksumValue> {
    let mut raw = Vec::new();
    for part in parts {
        raw.extend_from_slice(
            base64::engine::general_purpose::STANDARD
                .decode(part.value.as_str())
                .ok()?,
        );
    }
    Some(ChecksumValue(hash_bytes(algo, &raw)?))
}

/// FULL_OBJECT (CRC family only): combine the part CRCs with
/// carryless-multiplication matrices so the result equals the CRC of
/// the concatenated content — the S3 linearization (spec "AWS wire
/// facts": "S3 can compute the checksum of the whole object from the
/// part-level checksums"). `None` on invalid input.
///
/// The reflected polynomial constants: CRC-32 0xEDB88320, CRC-32C
/// 0x82F63B78, CRC-64/NVMe 0x9A6C9329AC4BC9B5 (poly
/// 0xad93d23594c93659 bit-reversed). The zlib-style gf2-matrix combine:
/// `combine(crc_a, crc_b, len_b)` returns the CRC of `a || b` for the
/// algorithm's register semantics. The self-validating test above
/// (`linearize_matches_the_direct_crc_of_concatenated_content`) is the
/// oracle — a wrong constant or endianness fails it.
pub(crate) fn linearize_full_object(
    algo: ChecksumAlgorithm,
    parts: &[PartChecksum],
) -> Option<ChecksumValue> {
    let width: usize = match algo {
        ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32C => 4,
        ChecksumAlgorithm::Crc64Nvme => 8,
        _ => return None, // SHA/MD5 have no linearization (S3: FULL_OBJECT is CRC-only)
    };
    let mut crc = crc_identity(algo);
    for part in parts {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(part.value.as_str())
            .ok()?;
        if bytes.len() != width {
            return None;
        }
        // The wire digest is the big-endian register value (s3s).
        let next = bytes.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b));
        crc = crc_combine(algo, crc, next, part_len_unknown(algo, parts.len())?);
    }
    // … (the fold needs each part's BYTE length for the combine; see
    // Step 4 note — the lengths come from the caller's part listing)
    None // replaced in Step 4
}
```

- [x] **Step 4: Complete the linearization (the combine math) + register the module**

The combine needs each part's content length. Change the signature to

```rust
pub(crate) fn linearize_full_object(
    algo: ChecksumAlgorithm,
    parts: &[PartChecksum],
    part_sizes: &[u64],
) -> Option<ChecksumValue>
```

(`parts` and `part_sizes` are parallel, ascending part order — the op passes the sizes from its `list_parts` snapshot; lengths in bytes.) Implement the standard zlib-style combine:

```rust
/// One step of the carryless-multiplication combine: given the CRC of
/// `a`, the CRC of `b`, and the length of `b`, the CRC of `a || b`
/// (reflected algorithms — matrix exponentiation by length, the
/// zlib `crc32_combine` construction; the constants per algorithm).
fn crc_combine(algo: ChecksumAlgorithm, crc_a: u64, crc_b: u64, len_b: u64) -> u64 {
    let (poly, width, mask) = crc_params(algo);
    // gf2 matrix for one bit of length: x^1 mod P, squared per bit…
    let mut even = [0u64; 64];
    let mut odd = [0u64; 64];
    // odd[0] = x^1 mod P (the "multiply by x" matrix row); even =
    // square it; then raise to len_b by repeated squaring and apply to
    // crc_a (the standard zlib `gf2_matrix_square`/`gf2_matrix_times`
    // algorithm — the reflected formulation uses the matrices on the
    // register values directly). Final: `crc_a'` is combined with
    // `crc_b` by xor of the two registers for reflected CRCs.
    let mut a = crc_a;
    let mut len = len_b;
    // … gf2_matrix_square + gf2_matrix_times loop over the bits of
    // `len`, then `a ^ crc_b` …
    let _ = (poly, width, mask);
    (a ^ crc_b) & mask
}

fn crc_params(algo: ChecksumAlgorithm) -> (u64, usize, u64) {
    match algo {
        ChecksumAlgorithm::Crc32 => (0xEDB88320, 32, u64::MAX >> 32),
        ChecksumAlgorithm::Crc32C => (0x82F63B78, 32, u64::MAX >> 32),
        ChecksumAlgorithm::Crc64Nvme => (0x9A6C9329AC4BC9B5, 64, u64::MAX),
        _ => unreachable!("linearize is CRC-only"),
    }
}

fn crc_identity(algo: ChecksumAlgorithm) -> u64 {
    match algo {
        ChecksumAlgorithm::Crc32 | ChecksumAlgorithm::Crc32C => u32::MAX as u64,
        ChecksumAlgorithm::Crc64Nvme => u64::MAX,
        _ => unreachable!("linearize is CRC-only"),
    }
}
```

The identity value is the CRC of the empty input under the algorithm's init+xorout convention: for CRC-32/CRC-32C/CRC-64/NVMe (init and xorout all-ones, reflected) the empty CRC is `0x00000000`/`0x0000000000000000` **on the wire** (init xored with xorout cancels) — the register accumulator must start at `crc_identity` above for the combine chain to be correct. **The self-validating test is the authority**: iterate on the register/endianness conventions until `linearize(random parts) == direct crc(concat)` for all three CRC algorithms (add the same test for `Crc32C` and `Crc64Nvme` by parameterizing the test over the three algorithms).

Then in `crates/tinio-server/src/backend/mod.rs`: add `mod checksum;` to the private modules and `pub(crate) use checksum::{ChecksumSpec, VerifyState, VerifyStream, compose_composite, linearize_full_object};` (or import via `backend::checksum::…` in the ops — pick one and stay consistent).

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p tinio-server checksum::tests`
Expected: PASS — the linearization self-test (three algorithms), the known CRC-32 vector, and the composite construction test.

---

### Task 5: `op_create_multipart_upload` + `op_upload_part`

**Files:**
- Modify: `crates/tinio-server/src/backend/multipart.rs` (ops), `crates/tinio-server/src/backend/checksum.rs` (a `set_output_checksum` helper)
- Test: `crates/tinio-server/src/backend/multipart.rs` test module

**Interfaces:**
- Consumes: Tasks 1–4 (`ChecksumSpec`, `VerifyStream`, `VerifyState`, `PartChecksum`, `UploadChecksum`, `set_part_checksum`/`get_multipart_upload` on the contract, `self.caps.checksum`).
- Produces: the create/upload behavior + a test helper `setup_checksum()` (used by Tasks 6–8 too).

- [x] **Step 1: Write the failing tests**

Add to the test module of `crates/tinio-server/src/backend/multipart.rs`:

```rust
    /// A backend with the checksum feature on (the default toggle is
    /// off — the tests must opt in).
    fn setup_checksum() -> (S3Backend<MemoryStorage>, String) {
        let storage = MemoryStorage::new().unwrap();
        let b = "data".to_string();
        let _ = storage.create_bucket(&bucket::name(&b).unwrap());
        (
            S3Backend::new(
                storage,
                Capabilities {
                    checksum: true,
                    ..Default::default()
                },
            ),
            b,
        )
    }

    /// The client-side checksum of `data` (the same s3s primitive the
    /// wire uses — the test simulates a real client).
    fn client_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> String {
        let mut hasher = ChecksumHasher::default();
        match algo {
            ChecksumAlgorithm::Crc32 => hasher.crc32 = Some(Crc32::new()),
            ChecksumAlgorithm::Crc32C => hasher.crc32c = Some(Crc32c::new()),
            ChecksumAlgorithm::Crc64Nvme => hasher.crc64nvme = Some(Crc64Nvme::new()),
            ChecksumAlgorithm::Sha1 => hasher.sha1 = Some(Sha1::new()),
            ChecksumAlgorithm::Sha256 => hasher.sha256 = Some(Sha256::new()),
            ChecksumAlgorithm::Md5 => hasher.md5 = Some(Md5::new()),
        }
        hasher.update(data);
        let checksum = hasher.finalize();
        checksum_value_of(&checksum, algo).unwrap().to_string()
    }
```

(import `s3s::checksum::ChecksumHasher` + `s3s::crypto::…` + `crate::_core::checksum::{…}` in the test module, and re-export or `use super::*` the `backend::checksum` helpers.)

Then the tests:

```rust
    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn create_echoes_the_checksum_algorithm() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                checksum_type: Some("FULL_OBJECT".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(create.output.checksum_algorithm.as_deref(), Some("CRC32"));
        assert_eq!(create.output.checksum_type.as_deref(), Some("FULL_OBJECT"));
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_validates_and_echoes_the_checksum() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let data = b"hello world";
        let expected = client_checksum(ChecksumAlgorithm::Crc32, data);
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                checksum_crc32: Some(expected.clone()),
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::copy_from_slice(data),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(part.output.checksum_crc32.as_deref(), Some(expected.as_str()));
        // Persisted → ListParts echoes it.
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            listed.output.parts.as_ref().unwrap()[0].checksum_crc32.as_deref(),
            Some(expected.as_str())
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_checksum_mismatch_is_bad_digest_and_stores_nothing() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                checksum_crc32: Some("y/Q5Jg==".into()), // crc32("123456789") ≠ the body
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::from_static(b"hello world"),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
        // The part was never stored.
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(listed.output.parts.as_ref().unwrap().is_empty());
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_validates_content_md5() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let data = b"abc";
        // MD5("abc") = 900150983cd24fb0d6963f7d28e17f72 → base64 "kAFQmDzST7DWlj99KOF/cg==".
        let md5 = base64::engine::general_purpose::STANDARD.encode(
            <md5::Md5 as md5::Digest>::digest(data),
        );
        backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                content_md5: Some(md5.clone()),
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::copy_from_slice(data),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap();
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 2,
                content_md5: Some("AAAAAAAAAAAAAAAAAAAAAA==".into()),
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::copy_from_slice(b"def"),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_rejects_conflicting_sources_and_bare_algorithm() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let body = || Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
            Bytes::from_static(b"x"),
        )])));
        // Two value fields → InvalidRequest.
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                checksum_crc32: Some("y/Q5Jg==".into()),
                checksum_sha256: Some("y/Q5Jg==".into()),
                body: body(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // Algorithm without a value → InvalidRequest (S3: 400).
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 2,
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                body: body(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_algorithm_must_match_the_create_algorithm() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let err = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id,
                part_number: 1,
                checksum_crc32: Some("y/Q5Jg==".into()),
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::from_static(b"x"),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn upload_part_computes_and_persists_headerless_parts_of_algorithm_uploads() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        // Header-less part: computed + persisted, echoed only by ListParts.
        let data = b"hello";
        let expected = client_checksum(ChecksumAlgorithm::Crc32, data);
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::copy_from_slice(data),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(part.output.checksum_crc32.is_none(), "no value in the request → no response echo");
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            listed.output.parts.as_ref().unwrap()[0].checksum_crc32.as_deref(),
            Some(expected.as_str())
        );
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn checksum_toggle_off_drops_the_headers() {
        // Default caps (checksum off) = today's behavior: accepted and
        // dropped, no validation, no echo.
        let (backend, b) = setup().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let part = backend
            .upload_part(s3_request(dto::UploadPartInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                upload_id: upload_id.clone(),
                part_number: 1,
                checksum_crc32: Some("y/Q5Jg==".into()), // wrong, but ignored
                body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                    Bytes::from_static(b"x"),
                )]))),
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(part.output.checksum_crc32.is_none());
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert!(listed.output.parts.as_ref().unwrap()[0].checksum_crc32.is_none());
    }
```

Note: the tests use `md5` — the tinio-server `[dev-dependencies]` already has `md-5.workspace = true` (:64); import as `md5::{Digest, Md5}`.

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server backend::multipart::tests::checksum`
Expected: FAIL — `setup_checksum` compiles but the ops don't implement the behavior (BadDigest path, echo, persistence).

- [x] **Step 3: Implement `op_create_multipart_upload`**

Replace the op body (:65-83) with:

```rust
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let checksum = if self.caps.checksum {
            match (
                req.input.checksum_algorithm.as_deref(),
                req.input.checksum_type.as_deref(),
            ) {
                (Some(algo), checksum_type) => Some(UploadChecksum {
                    algorithm: ChecksumAlgorithm::from_wire(algo).ok_or_else(|| {
                        s3_error!(InvalidArgument, "unsupported checksum algorithm: {algo}")
                    })?,
                    checksum_type: checksum_type
                        .map(|t| {
                            ChecksumType::from_wire(t).ok_or_else(|| {
                                s3_error!(InvalidArgument, "unsupported checksum type: {t}")
                            })
                        })
                        .transpose()?,
                }),
                (None, Some(_)) => {
                    return Err(s3_error!(
                        InvalidRequest,
                        "checksum type without a checksum algorithm"
                    ))
                }
                (None, None) => None,
            }
        } else {
            None
        };
        let upload = self
            .storage
            .create_multipart_upload(&bucket, &key, checksum)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::CreateMultipartUploadOutput {
            bucket: Some(String::from(bucket)),
            key: Some(String::from(key)),
            upload_id: Some(upload.upload_id),
            checksum_algorithm: upload
                .checksum_algorithm
                .map(|a| a.as_wire().parse().unwrap()),
            checksum_type: upload
                .checksum_type
                .map(|t| t.as_wire().parse().unwrap()),
            ..Default::default()
        }))
```

(`dto::ChecksumAlgorithm`/`dto::ChecksumType` parse from `&str` — confirm the `FromStr` impl in s3s `dto/generated.rs`; they are `Cow`-newtypes with `FromStr` — if not, construct via `.into()`/`ChecksumAlgorithm::from(...)`; check the impl at `generated.rs:1466-1493`.)

- [x] **Step 4: Implement `op_upload_part`**

Replace the op body (:86-105) with:

```rust
        self.require_multipart()?;
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let upload_id = req.input.upload_id;
        let part_number = part_number(req.input.part_number)?;
        let body = if self.caps.checksum {
            // The persisted create-algorithm drives the compute-only tee
            // and the algorithm-consistency check (S3: the checksum
            // algorithm must match the one supplied at create).
            let upload = self
                .storage
                .get_multipart_upload(&bucket, &key, &upload_id)
                .await
                .map_err(map_backend_error)?;
            let spec = ChecksumSpec::from_upload_part(&req.input, req.trailing_headers.as_ref())?;
            let spec = match spec {
                Some(spec) => {
                    if let (Some(upload_algo), Some(algo)) =
                        (upload.checksum_algorithm, spec.algorithm)
                        && upload_algo != algo
                    {
                        return Err(s3_error!(
                            InvalidRequest,
                            "checksum algorithm {} does not match the upload's {}",
                            algo.as_wire(),
                            upload_algo.as_wire()
                        ));
                    }
                    // The upload's algorithm applies to every part (S3:
                    // "must be the same for all parts") — a Content-MD5-
                    // only part of an algorithm upload still gets the
                    // upload's checksum computed and persisted.
                    Some(ChecksumSpec {
                        algorithm: spec.algorithm.or(upload.checksum_algorithm),
                        ..spec
                    })
                }
                None => upload.checksum_algorithm.map(|algo| ChecksumSpec {
                    algorithm: Some(algo),
                    expected: None,
                    content_md5: None,
                }),
            };
            match spec {
                Some(spec) => {
                    let state = std::sync::Arc::new(VerifyState::default());
                    let body = VerifyStream::wrap(Self::stream_in(req.input.body), &spec, &state);
                    self.upload_tee = Some((state, spec));
                    body
                }
                None => Self::stream_in(req.input.body),
            }
        } else {
            Self::stream_in(req.input.body)
        };
```

(`self.upload_tee` is not a real field — restructure: compute `(state, spec)` in a local `let tee: Option<(Arc<VerifyState>, ChecksumSpec)>` before the stream, then after `upload_part` returns, consult it. Write it as locals, not a struct field.)

Continue the op:

```rust
        let part = self
            .storage
            .upload_part(&bucket, &key, &upload_id, part_number, body)
            .await
            .map_err(|err| {
                if let Some((state, _)) = &tee
                    && state.mismatched()
                {
                    return s3_error!(BadDigest, "checksum mismatch");
                }
                map_backend_error(err)
            })?;
        if let Some((state, spec)) = tee {
            if let Some(algo) = spec.algorithm
                && let Some(computed) = state.computed()
            {
                self.storage
                    .set_part_checksum(
                        &bucket,
                        &key,
                        &upload_id,
                        part_number,
                        PartChecksum {
                            algorithm: algo,
                            value: computed.clone(),
                        },
                    )
                    .await
                    .map_err(map_backend_error)?;
            }
        }
        let mut output = dto::UploadPartOutput {
            e_tag: Some(Self::etag_wire(&part.etag)),
            ..Default::default()
        };
        if let Some((_, spec)) = &tee
            && let Some(algo) = spec.algorithm
            && let Some(expected) = &spec.expected
        {
            // Response echo only when the request carried a value (S3
            // API docs: "only be present if the checksum was provided in
            // the request").
            set_output_checksum(&mut output, algo, expected.as_str());
        }
        Ok(S3Response::new(output))
```

Add to `backend/checksum.rs` the echo helper:

```rust
/// Set the algorithm's value field on an output DTO that carries the
/// checksum value headers (`UploadPartOutput`,
/// `CompleteMultipartUploadOutput`, `dto::Part`,
/// `dto::CopyPartResult`).
pub(crate) fn set_output_checksum<T>(output: &mut T, algo: ChecksumAlgorithm, value: &str)
where
    T: HasChecksumFields,
{
    output.set_checksum(algo, value);
}

/// The one-method trait that lets `set_output_checksum` work across the
/// DTO shapes (each impl maps the algorithm to its field).
pub(crate) trait HasChecksumFields {
    fn set_checksum(&mut self, algo: ChecksumAlgorithm, value: &str);
}

impl HasChecksumFields for dto::UploadPartOutput {
    fn set_checksum(&mut self, algo: ChecksumAlgorithm, value: &str) {
        match algo {
            ChecksumAlgorithm::Crc32 => self.checksum_crc32 = Some(value.into()),
            ChecksumAlgorithm::Crc32C => self.checksum_crc32c = Some(value.into()),
            ChecksumAlgorithm::Crc64Nvme => self.checksum_crc64nvme = Some(value.into()),
            ChecksumAlgorithm::Sha1 => self.checksum_sha1 = Some(value.into()),
            ChecksumAlgorithm::Sha256 => self.checksum_sha256 = Some(value.into()),
            ChecksumAlgorithm::Md5 => self.checksum_md5 = Some(value.into()),
        }
    }
}
```

Add the same `HasChecksumFields` impls for `dto::CompleteMultipartUploadOutput`, `dto::Part`, and `dto::CopyPartResult` (same match — Task 6/7/8 use them).

- [x] **Step 5: Run test to verify it passes**

Run: `cargo test -p tinio-server backend::multipart::tests::checksum`
Expected: PASS. Then run `cargo test -p tinio-server` — the full suite must stay green (the toggle-off tests and all existing tests unchanged).

- [x] **Step 6: Report**

Leave in the tree. Report: create/upload ops, the `HasChecksumFields` helpers, the seven new tests green, full suite green.

---

### Task 6: `op_complete_multipart_upload` pre-commit validation

**Files:**
- Modify: `crates/tinio-server/src/backend/multipart.rs` (op + tests)
- Test: `crates/tinio-server/src/backend/multipart.rs` test module

**Interfaces:**
- Consumes: `compose_composite`, `linearize_full_object`, `set_output_checksum`, Task 5's `setup_checksum`/`client_checksum`.
- Produces: pre-commit validation + the Complete response echo.

- [x] **Step 1: Write the failing tests**

Add to the test module:

```rust
    /// Upload three parts of `content` with per-part checksums, return
    /// (etags, client part values).
    async fn upload_parts_with_checksums(
        backend: &S3Backend<MemoryStorage>,
        b: &str,
        upload_id: &str,
        algo: ChecksumAlgorithm,
        parts: &[&[u8]],
    ) -> (Vec<String>, Vec<String>) {
        let mut etags = Vec::new();
        let mut values = Vec::new();
        for (i, data) in parts.iter().enumerate() {
            let value = client_checksum(algo, data);
            let part = backend
                .upload_part(s3_request(dto::UploadPartInput {
                    bucket: b.to_string(),
                    key: "big.bin".into(),
                    upload_id: upload_id.to_string(),
                    part_number: (i + 1) as i32,
                    checksum_crc32: (algo == ChecksumAlgorithm::Crc32).then(|| value.clone()),
                    checksum_crc32c: (algo == ChecksumAlgorithm::Crc32C).then(|| value.clone()),
                    checksum_crc64nvme: (algo == ChecksumAlgorithm::Crc64Nvme)
                        .then(|| value.clone()),
                    checksum_sha1: (algo == ChecksumAlgorithm::Sha1).then(|| value.clone()),
                    checksum_sha256: (algo == ChecksumAlgorithm::Sha256).then(|| value.clone()),
                    checksum_md5: (algo == ChecksumAlgorithm::Md5).then(|| value.clone()),
                    body: Some(StreamingBlob::wrap(stream::iter(vec![Ok::<_, io::Error>(
                        Bytes::copy_from_slice(data),
                    )]))),
                    ..Default::default()
                }))
                .await
                .unwrap();
            etags.push(part.output.e_tag.unwrap());
            values.push(value);
        }
        (etags, values)
    }

    fn complete_input(upload_id: &str, etags: &[String]) -> dto::CompleteMultipartUploadInput {
        dto::CompleteMultipartUploadInput {
            bucket: "data".into(),
            key: "big.bin".into(),
            upload_id: upload_id.to_string(),
            multipart_upload: Some(dto::CompletedMultipartUpload {
                parts: Some(
                    etags
                        .iter()
                        .enumerate()
                        .map(|(i, e)| dto::CompletedPart {
                            part_number: Some((i + 1) as i32),
                            e_tag: Some(e.clone()),
                            ..Default::default()
                        })
                        .collect(),
                ),
            }),
            ..Default::default()
        }
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_validates_composite_sha256() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let parts: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
        let (etags, values) =
            upload_parts_with_checksums(&backend, &b, &upload_id, ChecksumAlgorithm::Sha256, &parts)
                .await;
        // The client's COMPOSITE value: SHA-256 over the concatenated
        // raw part digests (the documented construction).
        let mut raw = Vec::new();
        for v in &values {
            raw.extend_from_slice(
                base64::engine::general_purpose::STANDARD.decode(v).unwrap(),
            );
        }
        let mut h = ChecksumHasher { sha256: Some(Sha256::new()), ..Default::default() };
        h.update(&raw);
        let composite = h.finalize().checksum_sha256.unwrap();

        let mut input = complete_input(&upload_id, &etags);
        input.checksum_sha256 = Some(composite.clone());
        input.checksum_type = Some("COMPOSITE".parse().unwrap());
        let complete = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
        assert_eq!(complete.output.checksum_sha256.as_deref(), Some(composite.as_str()));
        // The object exists (validated pre-commit).
        assert!(backend.storage().head_object(&bucket::name(&b).unwrap(),
            &object::key("big.bin").unwrap()).await.is_ok());
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_validates_full_object_crc32_linearization() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                checksum_type: Some("FULL_OBJECT".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let parts: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
        let (etags, _) =
            upload_parts_with_checksums(&backend, &b, &upload_id, ChecksumAlgorithm::Crc32, &parts)
                .await;
        // The client's FULL_OBJECT value: the CRC of the concatenated
        // CONTENT (the linearization oracle — independent of the server
        // helper).
        let mut content = Vec::new();
        for p in &parts {
            content.extend_from_slice(p);
        }
        let full = client_checksum(ChecksumAlgorithm::Crc32, &content);

        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some(full.clone());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(content.len() as i64);
        let complete = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap();
        assert_eq!(complete.output.checksum_crc32.as_deref(), Some(full.as_str()));
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_checksum_mismatch_is_bad_digest_and_preserves_the_old_object() {
        let (backend, b) = setup_checksum().await;
        // A pre-existing object of the same key — a failed complete must
        // leave it untouched (pre-commit validation).
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object::key("big.bin").unwrap(),
                body(b"precious"),
            )
            .await
            .unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let parts: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
        let (etags, _) =
            upload_parts_with_checksums(&backend, &b, &upload_id, ChecksumAlgorithm::Crc32, &parts)
                .await;
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some("y/Q5Jg==".into()); // wrong
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
        // The old object survived; the upload is still live.
        let got = read_body(
            backend
                .storage()
                .get_object(
                    &bucket::name(&b).unwrap(),
                    &object::key("big.bin").unwrap(),
                    None,
                )
                .await
                .unwrap()
                .body,
        )
        .await
        .unwrap();
        assert_eq!(got, b"precious");
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn complete_rejects_algorithm_type_and_size_mismatches() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let parts: [&[u8]; 1] = [b"data"];
        let (etags, _) =
            upload_parts_with_checksums(&backend, &b, &upload_id, ChecksumAlgorithm::Sha256, &parts)
                .await;
        // Value algorithm ≠ create algorithm → InvalidRequest.
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // SHA with FULL_OBJECT → InvalidRequest (algorithm × type table).
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_sha256 = Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        input.mpu_object_size = Some(4);
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
        // FULL_OBJECT without mpu_object_size → InvalidRequest.
        let mut input = complete_input(&upload_id, &etags);
        input.checksum_crc32 = Some("y/Q5Jg==".into());
        input.checksum_type = Some("FULL_OBJECT".parse().unwrap());
        let err = backend
            .complete_multipart_upload(s3_request(input))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidRequest");
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server backend::multipart::tests::complete_`
Expected: FAIL — validation not implemented.

- [x] **Step 3: Implement the pre-commit validation**

Restructure the tail of `op_complete_multipart_upload` (:245-262). After the existing `parts` parsing and the 5 MiB paging, before `lock_object`:

```rust
        // Checksum validation (pre-commit, spec 2026-08-31): the parts
        // snapshot from the 5 MiB paging doubles as the checksum source.
        // The backend's in-txn ETag re-verification makes the snapshot
        // authoritative — a part overwritten after the snapshot cannot
        // match the client's listed ETag, so a validated upload commits
        // exactly the validated parts, and a failed validation leaves
        // the upload (and any pre-existing object) untouched.
        let mut echo_checksum: Option<(ChecksumAlgorithm, ChecksumValue)> = None;
        if self.caps.checksum {
            let upload = self
                .storage
                .get_multipart_upload(&bucket, &key, &upload_id)
                .await
                .map_err(map_backend_error)?;
            if let Some(upload_algo) = upload.checksum_algorithm {
                // 1. CompletedPart cross-check: the client's checksum
                // entries must match the stored values.
                for (part, checksum) in parts_with_checksums(&parts, &sizes, upload_algo) {
                    if let Some(client) = checksum
                        && let Some(stored) = part_stored_checksum(&sizes, u32::from(part))
                        && client != stored
                    {
                        return Err(s3_error!(
                            BadDigest,
                            "part {} checksum mismatch", u32::from(part)
                        ));
                    }
                }
            }
            // 2. The full-object value, when present.
            let full = full_object_value(&req.input); // Option<(ChecksumAlgorithm, String)>
            if let Some((algo, value)) = full {
                match upload.checksum_algorithm {
                    None => {
                        // AWS legacy behavior: accepted but not validated.
                        warn!(algorithm = ?algo, "complete checksum value without a create-time algorithm — accepted, not validated");
                    }
                    Some(upload_algo) if upload_algo != algo => {
                        return Err(s3_error!(
                            InvalidRequest,
                            "checksum algorithm {} does not match the upload's {}",
                            algo.as_wire(),
                            upload_algo.as_wire()
                        ));
                    }
                    Some(upload_algo) => {
                        let checksum_type = match (req.input.checksum_type.as_deref(), upload.checksum_type) {
                            (Some(wire), Some(persisted)) if ChecksumType::from_wire(wire) != Some(persisted) => {
                                return Err(s3_error!(InvalidRequest, "checksum type conflicts with the upload's"));
                            }
                            (Some(wire), _) => ChecksumType::from_wire(wire).ok_or_else(|| {
                                s3_error!(InvalidArgument, "unsupported checksum type: {wire}")
                            })?,
                            (None, Some(persisted)) => persisted,
                            (None, None) => ChecksumType::Composite,
                        };
                        // Algorithm × type validity (spec table).
                        let valid = match checksum_type {
                            ChecksumType::Composite => matches!(
                                upload_algo,
                                ChecksumAlgorithm::Crc32
                                    | ChecksumAlgorithm::Crc32C
                                    | ChecksumAlgorithm::Sha1
                                    | ChecksumAlgorithm::Sha256
                                    | ChecksumAlgorithm::Md5
                            ),
                            ChecksumType::FullObject => matches!(
                                upload_algo,
                                ChecksumAlgorithm::Crc32
                                    | ChecksumAlgorithm::Crc32C
                                    | ChecksumAlgorithm::Crc64Nvme
                            ),
                        };
                        if !valid {
                            return Err(s3_error!(
                                InvalidRequest,
                                "checksum algorithm {} does not support the {} checksum type",
                                upload_algo.as_wire(),
                                checksum_type.as_wire()
                            ));
                        }
                        // Stored part checksums of the listed parts, in
                        // ascending order (the `sizes` map also carries
                        // the checksums — collect from it).
                        let stored: Vec<PartChecksum> = /* listed parts' PartChecksum, ascending */;
                        if stored.len() != parts.len() {
                            warn!(algorithm = ?upload_algo, "complete checksum validation skipped: parts without stored checksums");
                        } else {
                            let computed = match checksum_type {
                                ChecksumType::Composite => compose_composite(upload_algo, &stored),
                                ChecksumType::FullObject => {
                                    if req.input.mpu_object_size.is_none() {
                                        return Err(s3_error!(
                                            InvalidRequest,
                                            "FULL_OBJECT checksum requires x-amz-mpu-object-size"
                                        ));
                                    }
                                    let sizes_list: Vec<u64> = /* listed part sizes, ascending */;
                                    let total: u64 = sizes_list.iter().sum();
                                    if Some(total as i64) != req.input.mpu_object_size {
                                        return Err(s3_error!(
                                            InvalidRequest,
                                            "x-amz-mpu-object-size does not match the sum of the part sizes"
                                        ));
                                    }
                                    linearize_full_object(upload_algo, &stored, &sizes_list)
                                }
                            };
                            match computed {
                                Some(computed) if computed.as_str() == value => {
                                    echo_checksum = Some((upload_algo, computed));
                                }
                                Some(_) => {
                                    return Err(s3_error!(BadDigest, "checksum mismatch"));
                                }
                                None => {
                                    warn!(algorithm = ?upload_algo, "complete checksum validation skipped");
                                }
                            }
                        }
                    }
                }
            } else if let Some(upload_algo) = upload.checksum_algorithm {
                // No client value: still compute + echo (spec Q3) when
                // every listed part has a stored checksum.
                let stored: Vec<PartChecksum> = /* listed parts' PartChecksum, ascending */;
                if stored.len() == parts.len() {
                    let checksum_type = upload.checksum_type.unwrap_or(ChecksumType::Composite);
                    let computed = match checksum_type {
                        ChecksumType::Composite => compose_composite(upload_algo, &stored),
                        ChecksumType::FullObject => {
                            let sizes_list: Vec<u64> = /* listed part sizes */;
                            linearize_full_object(upload_algo, &stored, &sizes_list)
                        }
                    };
                    if let Some(computed) = computed {
                        echo_checksum = Some((upload_algo, computed));
                    }
                }
            }
        }
```

Key notes for the implementer:

- The existing 5 MiB paging (:205-244) builds `sizes: HashMap<u32, u64>` — but it is guarded by `if parts.len() > 1` (:205). Hoist the guard to `if parts.len() > 1 || self.caps.checksum` so the snapshot also exists for single-part checksum uploads, and change the map to `HashMap<u32, (u64, Option<PartChecksum>)>` (the paging loop iterates `page.parts` — each `PartInfo` now carries `checksum`). The `sizes`/`stored` lookups in the sketch above read from this map. Restructure the sketch to use that single map — the placeholders `/* … */` resolve to reads of it; do not keep two maps.
- `parts_with_checksums`/`full_object_value`/`part_stored_checksum` are inline helpers (write them as small private fns in `backend/multipart.rs` or as local closures): `full_object_value` scans the six `input.checksum_*` fields (exactly one, like `ChecksumSpec::parse` — reuse the `from_upload_part`-style scan but return the raw pair; a second value → `InvalidRequest`).
- The `CompletedPart` cross-check reads the dto `CompletedPart.checksum_*` field matching `upload_algo` — collect it during the existing `parts` mapping loop (:178-199) into a parallel `Vec<Option<String>>` (the dto `CompletedPart` is consumed there; capture the matching field before building the contract `CompletedPart`).
- `linearize_full_object` takes the part sizes in ascending part order — the listed parts are already ascending (validated at :178-199), so map them in that order.
- After `complete_multipart_upload` succeeds (:249-253), set the echo before building the output:

```rust
        let mut output = dto::CompleteMultipartUploadOutput {
            bucket: Some(String::from(bucket)),
            key: Some(String::from(key)),
            e_tag: Some(Self::etag_wire(&info.etag)),
            location,
            ..Default::default()
        };
        if let Some((algo, value)) = echo_checksum {
            set_output_checksum(&mut output, algo, value.as_str());
        }
        Ok(S3Response::new(output))
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-server backend::multipart::tests::complete_`
Expected: PASS (all four new tests). Then `cargo test -p tinio-server` — full suite green.

- [x] **Step 5: Report**

Leave in the tree. Report: pre-commit validation, echo, the four tests.

---

### Task 7: `op_upload_part_copy` checksum path

**Files:**
- Modify: `crates/tinio-server/src/backend/multipart.rs` (op + tests)
- Test: `crates/tinio-server/src/backend/multipart.rs` test module

**Interfaces:**
- Consumes: `ChecksumSpec::from_headers`, Task 5's helpers.
- Produces: the value-carrying copy path (spec Q8); header-less copies keep `copy_part`.

- [x] **Step 1: Write the failing test**

```rust
    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_validates_the_checksum_and_persists_it() {
        let (backend, b) = setup_checksum().await;
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object::key("src.bin").unwrap(),
                body(b"0123456789"),
            )
            .await
            .unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let expected = client_checksum(ChecksumAlgorithm::Crc32, b"0123"); // range 0-3
        let mut input = upload_part_copy_input(&b, &upload_id, 1);
        input.copy_source_range = Some("bytes=0-3".into());
        let mut req = s3_request(input);
        req.headers.insert(
            http::header::HeaderName::from_static("x-amz-checksum-crc32"),
            expected.parse().unwrap(),
        );
        let part = backend.upload_part_copy(req).await.unwrap();
        assert_eq!(
            part.output.copy_part_result.as_ref().unwrap().checksum_crc32.as_deref(),
            Some(expected.as_str())
        );
        // Persisted → ListParts echo.
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(
            listed.output.parts.as_ref().unwrap()[0].checksum_crc32.as_deref(),
            Some(expected.as_str())
        );
    }

    #[cfg(feature = "copy")]
    #[tokio::test]
    async fn upload_part_copy_checksum_mismatch_is_bad_digest() {
        let (backend, b) = setup_checksum().await;
        backend
            .storage()
            .put_object(
                &bucket::name(&b).unwrap(),
                &object::key("src.bin").unwrap(),
                body(b"0123456789"),
            )
            .await
            .unwrap();
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "copy.bin".into(),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let mut input = upload_part_copy_input(&b, &upload_id, 1);
        let mut req = s3_request(input);
        req.headers.insert(
            http::header::HeaderName::from_static("x-amz-checksum-crc32"),
            "y/Q5Jg==".parse().unwrap(), // wrong for "0123456789"
        );
        let err = backend.upload_part_copy(req).await.unwrap_err();
        assert_eq!(err.code().as_str(), "BadDigest");
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server upload_part_copy_checksum`
Expected: FAIL — the op ignores the header today.

- [x] **Step 3: Implement the copy path**

In `op_upload_part_copy` (:109-167), after the range parse + source conditionals, before the `copy_part` call:

```rust
        let part = if self.caps.checksum
            && let Some(spec) = ChecksumSpec::from_headers(&req.headers, req.trailing_headers.as_ref())?
        {
            // Value-carrying copy: stream the source range through the
            // verifying tee (spec Q8) — the bytes stay visible to the
            // server, so validation + persistence work like UploadPart.
            // Header-less copies keep the contract `copy_part` fast path.
            let upload = self
                .storage
                .get_multipart_upload(&bucket, &key, &upload_id)
                .await
                .map_err(map_backend_error)?;
            if let (Some(upload_algo), Some(algo)) = (upload.checksum_algorithm, spec.algorithm)
                && upload_algo != algo
            {
                return Err(s3_error!(
                    InvalidRequest,
                    "checksum algorithm {} does not match the upload's {}",
                    algo.as_wire(),
                    upload_algo.as_wire()
                ));
            }
            let state = std::sync::Arc::new(VerifyState::default());
            let get = self
                .storage
                .get_object(&src_bucket, &src_key, range)
                .await
                .map_err(map_backend_error)?;
            let body = VerifyStream::wrap(get.body, &spec, &state);
            let part = self
                .storage
                .upload_part(&bucket, &key, &upload_id, part_number, body)
                .await
                .map_err(|err| {
                    if state.mismatched() {
                        return s3_error!(BadDigest, "checksum mismatch");
                    }
                    map_backend_error(err)
                })?;
            if let Some(algo) = spec.algorithm
                && let Some(computed) = state.computed()
            {
                self.storage
                    .set_part_checksum(
                        &bucket,
                        &key,
                        &upload_id,
                        part_number,
                        PartChecksum {
                            algorithm: algo,
                            value: computed,
                        },
                    )
                    .await
                    .map_err(map_backend_error)?;
            }
            part
        } else {
            self.storage
                .copy_part(
                    &src_bucket,
                    &src_key,
                    &bucket,
                    &key,
                    &upload_id,
                    part_number,
                    range,
                )
                .await
                .map_err(map_backend_error)?
        };
```

The response: build `dto::CopyPartResult` and, when the request carried a value, echo via `set_output_checksum` on the `CopyPartResult` (its `HasChecksumFields` impl from Task 5) — track `(algo, expected)` alongside the tee, mirroring `op_upload_part`'s echo rule (only-when-provided).

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-server upload_part_copy_checksum`
Expected: PASS. Then `cargo test -p tinio-server` — the existing `upload_part_copy_*` tests (incl. the toggle test with `copy_object: false`) must stay green.

- [x] **Step 5: Report**

Leave in the tree. Report: the copy tee path + two tests.

---

### Task 8: ListParts / ListMultipartUploads echo

**Files:**
- Modify: `crates/tinio-server/src/backend/multipart.rs` (ops + tests)
- Test: `crates/tinio-server/src/backend/multipart.rs` test module

**Interfaces:**
- Consumes: Task 3's `PartInfo.checksum` / `MultipartUpload` fields; `set_output_checksum` (on `dto::Part`).
- Produces: the list echo (covered by the Task 5 tests' ListParts assertions, plus the two tests below).

- [x] **Step 1: Write the failing tests**

```rust
    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_parts_echoes_the_upload_checksum_spec() {
        let (backend, b) = setup_checksum().await;
        let create = backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("CRC32".parse().unwrap()),
                checksum_type: Some("FULL_OBJECT".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload_id = create.output.upload_id.unwrap();
        let listed = backend
            .list_parts(s3_request(dto::ListPartsInput {
                bucket: b,
                key: "big.bin".into(),
                upload_id,
                ..Default::default()
            }))
            .await
            .unwrap();
        assert_eq!(listed.output.checksum_algorithm.as_deref(), Some("CRC32"));
        assert_eq!(listed.output.checksum_type.as_deref(), Some("FULL_OBJECT"));
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_multipart_uploads_echoes_the_checksum_algorithm() {
        let (backend, b) = setup_checksum().await;
        backend
            .create_multipart_upload(s3_request(dto::CreateMultipartUploadInput {
                bucket: b.clone(),
                key: "big.bin".into(),
                checksum_algorithm: Some("SHA256".parse().unwrap()),
                ..Default::default()
            }))
            .await
            .unwrap();
        let page = backend
            .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                bucket: b,
                ..Default::default()
            }))
            .await
            .unwrap();
        let upload = &page.output.uploads.as_ref().unwrap()[0];
        assert_eq!(upload.checksum_algorithm.as_deref(), Some("SHA256"));
        assert!(upload.checksum_type.is_none());
    }
```

- [x] **Step 2: Run test to verify it fails**

Run: `cargo test -p tinio-server list_parts_echoes_checksum`
Expected: FAIL — the echo fields are not set.

- [x] **Step 3: Implement the echo**

In `op_list_parts` (:280-339): when `self.caps.checksum`, fetch the upload's checksum spec once (the op already pages `list_parts`; add a `get_multipart_upload` call) and set `checksum_algorithm`/`checksum_type` on the output; per part, map `p.checksum` (the contract `PartInfo.checksum`) into the matching `dto::Part` field via `set_output_checksum(&mut part, algo, value)` — each `dto::Part` in the map loop gains:

```rust
            .map(|p| {
                let mut part = dto::Part {
                    e_tag: Some(Self::etag_wire(&p.etag)),
                    last_modified: Some(Self::last_modified(p.last_modified)),
                    part_number: Some(u32::from(p.part_number) as i32),
                    size: Some(p.size as i64),
                    ..Default::default()
                };
                if let Some(checksum) = p.checksum {
                    set_output_checksum(&mut part, checksum.algorithm, checksum.value.as_str());
                }
                part
            })
```

In `op_list_multipart_uploads` (:342-395): map `u.checksum_algorithm`/`u.checksum_type` into the `dto::MultipartUpload` (the fields exist in the DTO — verified):

```rust
            .map(|u| dto::MultipartUpload {
                initiated: Some(Self::last_modified(u.initiated_at)),
                key: Some(String::from(u.key)),
                upload_id: Some(u.upload_id),
                checksum_algorithm: u
                    .checksum_algorithm
                    .map(|a| a.as_wire().parse().unwrap()),
                checksum_type: u.checksum_type.map(|t| t.as_wire().parse().unwrap()),
                ..Default::default()
            })
```

- [x] **Step 4: Run test to verify it passes**

Run: `cargo test -p tinio-server list_echoes_checksum`
Expected: PASS; then `cargo test -p tinio-server` — full suite green.

- [x] **Step 5: Report**

Leave in the tree. Report: the two echo paths + tests.

---

### Task 9: e2e, error codes, docs

**Files:**
- Modify: `crates/tinio-server/tests/error_codes.rs`, `crates/tinio-server/tests/e2e/mod.rs` (config plumbing, if needed), `crates/tinio-server/tests/boto3.rs`
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md`, `specs/001-s3-local-server/contracts/config.md`, `specs/001-s3-local-server/checklists/compatibility.md`

**Interfaces:**
- Consumes: everything; the `serve` example already wires `Capabilities` from `[s3]` config (no code change — verify `examples/serve.rs` picks up `checksum` via the flattened `Capabilities::from(&Config)`).

- [x] **Step 1: `error_codes.rs` cases**

Add to `crates/tinio-server/tests/error_codes.rs` (mirror the existing style — `Server::mem(caps)` + `request(...)`; read the rest of the file for how a multipart create/upload is issued raw — the `request` helper returns the response with `.body` for XML parsing):

```rust
#[tokio::test]
async fn upload_part_checksum_mismatch_is_bad_digest() {
    let server = Server::mem(Capabilities {
        checksum: true,
        ..Default::default()
    })
    .await;
    request(server.addr(), "PUT", "/data", &[], &[]).await;

    // CreateMultipartUpload → parse the upload id from the XML body.
    let resp = request(server.addr(), "POST", "/data/big.bin?uploads", &[], &[]).await;
    assert_eq!(resp.status, StatusCode::OK);
    let upload_id = /* parse `<UploadId>` from resp.body (see the file's XML helpers) */;

    // UploadPart with a wrong checksum → BadDigest.
    let resp = request(
        server.addr(),
        "PUT",
        &format!("/data/big.bin?partNumber=1&uploadId={upload_id}"),
        &[("x-amz-checksum-crc32", "y/Q5Jg==")],
        b"hello world",
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    assert_eq!(resp.error_code(), "BadDigest");
}
```

Check how `Server::mem` passes `Capabilities` to the serve process (`tests/common/mod.rs`) — if it serializes a config, the `checksum` field flows automatically; if it maps fields one by one, add `checksum` to the mapping. If the file has no XML parsing helper yet, parse the upload id with a minimal string scan (`body.split("<UploadId>").nth(1).unwrap().split("</UploadId>").next().unwrap()`).

Run: `cargo test -p tinio-server --test error_codes upload_part_checksum` — Expected: PASS.

- [x] **Step 2: boto3 journey with the toggle on**

`boto3_journey.py` sends checksums on multipart uploads by default (boto3 `upload_file`). Run the existing journey once with the default config (toggle off — the current behavior smoke test), then with `[s3] checksum = true`:

1. Read the remainder of `tests/e2e/mod.rs` — `Server::start_with_config(&root, &config)` writes a config file and passes `--config` (confirmed: :85-100). `tests/boto3.rs` currently uses `Server::start()` (:30) — add a second test or parameterize it to start with a config that includes `[s3]\nchecksum = true`:

```rust
#[tokio::test]
#[ignore]
async fn journey_with_checksum_validation() {
    let root = tempfile::tempdir().unwrap();
    let config = tempfile::tempdir().unwrap();
    let config_path = config.path().join("config.toml");
    fs::write(
        &config_path,
        "version = 1\nroot = \"…\"\n[s3]\nchecksum = true\n",
    )
    .unwrap();
    let server = e2e::Server::start_with_config(root.path(), &config_path);
    // … drive the same journey the existing test drives …
}
```

2. Build the serve example first: `cargo build -p tinio-server --example serve`
3. Run: `cargo test -p tinio-server --test boto3 journey -- --ignored` — Expected: PASS (boto3 computes the checksums correctly; the real-client math exercises the tee, composition, and linearization end-to-end).
4. Also run the existing default-config journey: `cargo test -p tinio-server --test boto3 -- --ignored` — Expected: PASS (toggle-off behavior unchanged).
5. **aws-cli journey too (grilling Q2)**: AWS CLI v2 defaults to `CRC64NVME` — the FULL_OBJECT linearization path with a real client. Mirror the same `start_with_config` setup in `tests/journey.rs` (read how it starts its server — the same `e2e::Server` harness) and run: `cargo test -p tinio-server --test journey checksum -- --ignored` — Expected: PASS. (If the aws-cli version installed sends checksums as aws-chunked trailers, this also exercises the trailer path end-to-end.) The default-config journey (`cargo test -p tinio-server --test journey -- --ignored`) must stay green.

If the journey's config file needs the root path structure of the existing harness, reuse the exact config template the harness already writes (read the rest of `e2e/mod.rs` for the config shape and mirror it, adding `checksum = true` under `[s3]`).

- [x] **Step 3: Contract docs**

1. `specs/001-s3-local-server/contracts/s3-surface.md:21` — replace the "Checksums: ignored in v1" bullet with the feature summary + the D1–D5 deviation list (from the spec's Deviations section) and a pointer to the design spec.
2. `specs/001-s3-local-server/contracts/config.md` — add `checksum` to the `[s3]` capability toggles table: "validate and echo `x-amz-checksum-*` on multipart uploads; default `false` (accepted and dropped)".
3. `specs/001-s3-local-server/checklists/compatibility.md:40` (CHK019) — update the checksum line to "validated behind `[s3] checksum` (default off); deviations D1–D5" with the deviation summary.

Cross-check non-English text before finishing (project rule). Run: `cargo test -p tinio-config` (config docs reference) — no code impact expected.

- [x] **Step 4: Full verification + report**

Run: `cargo test -p tinio-core -p tinio-config -p tinio-fs -p tinio-mem -p tinio-server`
Expected: all green. Report the full change set (files touched per task), the pending tree state, and the e2e runs performed.

---

## Self-review notes (writing-plans)

- **Spec coverage**: Goal 1 (UploadPart/Copy validation) → Tasks 5+7; Goal 2 (Complete COMPOSITE/FULL_OBJECT + CompletedPart cross-check) → Task 6; Goal 3 (server-layer s3s hashing, backends zero logic) → Tasks 3+4; Goal 4 (toggle) → Task 2 + gating in 5–8. Non-goals: no PutObject (nothing added), no auto-attach (nothing added), no read-back (Task 6 uses stored parts). Errors table → Tasks 5/6/7. Deviations D1–D5 → Tasks 5 (echo rule), 6 (D2 skip), 7 (D5), 9 (docs). Persistence → Task 3. Config → Task 2. Testing → each task.
- **Placeholders**: the two `/* … */` markers in Task 6 resolve to reads of the single `sizes` map (explicitly instructed); `upload_from_row`/`remove_all_parts`/`drain_pair` are existing helpers whose shapes the steps tell the implementer to mirror. The `TrailingHeaders` type is `s3s::TrailingHeaders` (crate-root re-export, verified).
- **Type consistency**: `ChecksumAlgorithm::{Crc32, Crc32C, Crc64Nvme, Sha1, Sha256, Md5}`, `ChecksumType::{Composite, FullObject}`, `ChecksumValue(String)`, `PartChecksum{algorithm, value}`, `UploadChecksum{algorithm, checksum_type}` — used identically across Tasks 1, 3–8. Contract methods `get_multipart_upload(bucket, key, upload_id)` / `set_part_checksum(bucket, key, upload_id, part_number, checksum)` — same shapes in Task 3 (definitions) and Tasks 5–7 (call sites). `set_output_checksum`/`HasChecksumFields` defined in Task 5, consumed in Tasks 6–8.
