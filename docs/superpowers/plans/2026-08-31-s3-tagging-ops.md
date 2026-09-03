# S3 Tagging & Remaining Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** End-to-end object + bucket tagging behind a `tagging` capability toggle (default on), plus RenameObject with source/destination conditionals and GetObjectAttributes with multipart part retention.

**Architecture:** The `Tags` type (validated, sorted key→value map with a canonical wire form) lives in `tinio-core`; the storage contract carries tags atomically on every write path (`commit_object`, `copy_object`, `create_multipart_upload`, plus three new tag methods, bucket tag methods, `rename_object`, and `list_object_parts`); the fs/mem backends persist tags by extending their redb rows (object, upload, bucket) — no version bump for the tagging rows; `STATE_VERSION` reverts 2 → 1 (user ruling 2026-09-02: undo the multipart-era bump — additive schema changes carry no version, dev DBs disposable); the S3 interface parses/validates `x-amz-tagging`, implements the six tagging ops, RenameObject, GetObjectAttributes, and wires them through `s3.rs` + `MetricS3`. All conditional evaluation reuses Plan A's shared machinery (`conditions.rs`).

**Tech Stack:** Rust, s3s 0.15.0, redb (fs/mem backends), tinio-config schema, cucumber 0.23 (e2e), tinio-util conformance harness.

**Spec:** `docs/superpowers/specs/2026-08-31-s3-tagging-ops-design.md` — the tagging/RenameObject/GetObjectAttributes spec (approved, grilled 2026-08-31; this plan incorporates the grilling resolutions). Companion: `docs/superpowers/specs/2026-08-31-s3-conditionals-design.md` (the conditional-headers machinery, implemented by Plan A).

**Depends on:** Plan A (`docs/superpowers/plans/2026-08-31-s3-conditionals-cleanup.md`) — Tasks 3, 6, 8, 11 here consume `check_missing`, `check_delete_conditions`, `ConditionalHeaders`, `to_whole_seconds` from Plan A. Execute Plan A first.

## Global Constraints

- **English only** — code, comments, feature files, docs (project rule).
- **No git writes** — never commit/push; leave changes in the tree; report at checkpoints; the user commits (project rule).
- **TDD** — failing test → implement → passing test per task.
- **Async tests** — `#[tokio::test]` directly (project rule).
- **s3s 0.15.0 pinned** — the dto surface is fixed; `GetObjectOutput.tag_count` exists (serialized as `x-amz-tagging-count`), `HeadObjectOutput` has no such field (the header is hand-set via `S3Response.headers`).
- **No version compatibility** — the project is in development with no released data; no migration paths, no compat shims, no schema-version tolerance. **`STATE_VERSION` reverts 2 → 1** (user ruling 2026-09-02: undo the multipart-era bump — additive schema changes (the multipart checksum tables, this plan's tagging rows + `OBJECT_PARTS`) carry no version; the existing open-time gate is untouched, so any stored version other than 1 is refused; a stale same-version DB from a different format may fail at row decode — dev-local disposable state, delete and rebuild by hand). All row changes (object tags + checksum, upload tags, bucket tags, `OBJECT_PARTS`) land in ONE task (Task 3).
- **Backend trust boundary** — the interface validates tags before calling the contract; backends trust `Tags`; no `InvalidTag` in `StorageError`.
- **Existing tests stay green** — `cargo test --workspace` on Windows and WSL2; the conformance harness and both backends' test suites are updated in the same task that changes the contract (compile break otherwise).
- **Test scaffold vocabulary** (existing, reused): `setup()`/`setup_name()`, `s3_request`, `body`, `read_body`, `bucket::name`, `object::key`.

---

### Task 1: `Tags` type + validation + wire form (`tinio-core`)

**Files:**
- Modify: `crates/tinio-core/src/object.rs` (`Tags`, `TagError`; `Info` gains `tags: Tags` — default empty)
- Test: `crates/tinio-core/src/object.rs` test module (or a sibling `tags.rs` test module if the file is already large — follow the existing test layout)

**Interfaces:**
- Consumes: nothing new.
- Produces (used by every later task):
  - `pub struct Tags(BTreeMap<String, String>)` — `Default`, `Clone`, `Debug`, `PartialEq`, `Eq`.
  - `impl Tags { pub fn empty() -> Self; pub fn is_empty(&self) -> bool; pub fn len(&self) -> usize; pub fn iter(&self) -> impl Iterator<Item = (&str, &str)>; pub fn from_pairs(pairs: impl IntoIterator<Item = (String, String)>) -> Result<Self, TagError>; pub fn parse_wire(input: &str) -> Result<Self, TagError>; pub fn to_wire(&self) -> String; }`
  - `#[derive(Debug, thiserror::Error, PartialEq, Eq)] pub enum TagError { TooMany(usize), InvalidKey { key: String }, InvalidValue { value: String }, Duplicate { key: String } }`
  - `object::Info { ..., pub tags: Tags }` (new field; the `..` construction sites in tinio-core/tests and the backends must add `tags: Tags::empty()` / their real value — the compiler will list them).

- [x] **Step 1: Write the failing tests** (in `tinio-core/src/object.rs` test module):

```rust
#[test]
fn tags_validate_count_length_and_charset() {
    // The S3 limits: ≤10 object tags (50 for buckets), key 1..=128 /
    // value 0..=256 UTF-16 units, Unicode letters/digits/space plus
    // + - = . _ : / @ (the S3 Control regex charset).
    assert!(Tags::from_pairs([("a".into(), "1".into())]).is_ok());
    let too_many: Vec<(String, String)> = (0..11).map(|i| (format!("k{i}"), "v".into())).collect();
    assert_eq!(
        Tags::from_pairs(too_many).unwrap_err(),
        TagError::TooMany(11)
    );
    assert_eq!(
        Tags::from_pairs([("".into(), "v".into())]).unwrap_err(),
        TagError::InvalidKey { key: "".into() }
    );
    assert_eq!(
        Tags::from_pairs([("k".into(), "v".repeat(257))]).unwrap_err(),
        TagError::InvalidValue { value: "v".repeat(257) }
    );
    // Empty values are legal (AWS: value min length 0).
    assert!(Tags::from_pairs([("k".into(), String::new())]).is_ok());
    // Unicode letters are legal (AWS tags are Unicode, not ASCII-only).
    assert!(Tags::from_pairs([("键".into(), "值".into())]).is_ok());
    // A character outside the allowed set is rejected.
    assert_eq!(
        Tags::from_pairs([("k".into(), "v&bad".into())]).unwrap_err(),
        TagError::InvalidValue { value: "v&bad".into() }
    );
    assert_eq!(
        Tags::from_pairs([("k".into(), "v".into()), ("k".into(), "w".into())]).unwrap_err(),
        TagError::Duplicate { key: "k".into() }
    );
    // The allowed charset round-trips.
    let tags = Tags::from_pairs([("a+b=c".into(), "x y.z:/@_-".into())]).unwrap();
    assert_eq!(tags.iter().collect::<Vec<_>>(), [("a+b=c", "x y.z:/@_-")]);
}

#[test]
fn tags_wire_form_round_trips() {
    let tags = Tags::from_pairs([
        ("b".into(), "2".into()),
        ("a".into(), "1".into()),
        ("eq".into(), "a=b".into()),
        ("amp".into(), "x&y".into()),
    ])
    .unwrap();
    // Sorted by key; `=` and `&` percent-encoded in values.
    assert_eq!(
        tags.to_wire(),
        "a=1&amp=x%26y&b=2&eq=a%3Db"
    );
    assert_eq!(Tags::parse_wire(&tags.to_wire()).unwrap(), tags);
    // Malformed input is rejected.
    assert!(Tags::parse_wire("k=v&k2").is_err());
    assert!(Tags::parse_wire("k%zz=v").is_err());
    // Percent-encoded allowed chars decode.
    let tags = Tags::parse_wire("a=%2Bb").unwrap();
    assert_eq!(tags.iter().collect::<Vec<_>>(), [("a", "+b")]);
    // Percent-encoded UTF-8 decodes (Unicode tags).
    let tags = Tags::parse_wire("k=%E9%94%AE").unwrap();
    assert_eq!(tags.iter().collect::<Vec<_>>(), [("k", "键")]);
}
```

- [x] **Step 2: Run to verify they fail**

Run: `cargo test -p tinio-core object::tags_`
Expected: FAIL — `Tags` / `TagError` not found.

- [x] **Step 3: Implement** in `tinio-core/src/object.rs` (near `Info`; add `use std::collections::BTreeMap;` at the top):

```rust
/// Object (or bucket) tags — a validated, sorted key→value map with
/// the S3 TagSet rules: ≤10 object tags (50 for buckets, via
/// `from_pairs_limited`), key 1..=128 / value 0..=256 UTF-16 units
/// from the Unicode charset, no duplicate keys. Sorted iteration
/// gives deterministic output and a canonical wire form.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tags(BTreeMap<String, String>);

impl Tags {
    /// The empty tag set.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The number of tags.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Iterate `(key, value)` in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Validate a set of `(key, value)` pairs (the S3 TagSet rules)
    /// with the object-tag count cap (10). Bucket tagging uses
    /// `from_pairs_limited(pairs, 50)`.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, TagError> {
        Self::from_pairs_limited(pairs, 10)
    }

    /// The same validation with an explicit count cap (object 10,
    /// bucket 50 — AWS-verified per-surface limits).
    pub fn from_pairs_limited(
        pairs: impl IntoIterator<Item = (String, String)>,
        limit: usize,
    ) -> Result<Self, TagError> {
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            if map.len() >= limit {
                return Err(TagError::TooMany(map.len() + 1));
            }
            if !valid_key(&key) {
                return Err(TagError::InvalidKey { key });
            }
            if !valid_value(&value) {
                return Err(TagError::InvalidValue { value });
            }
            if map.insert(key.clone(), value).is_some() {
                return Err(TagError::Duplicate { key });
            }
        }
        Ok(Self(map))
    }

    /// Parse the `x-amz-tagging` wire form (`k=v&k2=v2`, percent-decoded;
    /// `+` is a literal plus). Malformed input → error.
    pub fn parse_wire(input: &str) -> Result<Self, TagError> {
        let mut pairs = Vec::new();
        for pair in input.split('&') {
            if pair.is_empty() {
                continue;
            }
            let Some((k, v)) = pair.split_once('=') else {
                return Err(TagError::InvalidKey { key: pair.to_string() });
            };
            pairs.push((percent_decode(k), percent_decode(v)));
        }
        Self::from_pairs(pairs)
    }

    /// The canonical wire form: sorted `k=v&k2=v2` with `%`, `=`, `&`,
    /// `+`, and space percent-encoded in keys and values — the fs/mem
    /// persistence format (parse_wire is its inverse).
    pub fn to_wire(&self) -> String {
        let mut out = String::new();
        for (i, (k, v)) in self.iter().enumerate() {
            if i > 0 {
                out.push('&');
            }
            out.push_str(&percent_encode(k));
            out.push('=');
            out.push_str(&percent_encode(v));
        }
        out
    }
}

/// The allowed tag charset — Unicode letters, numbers, and spaces plus
/// `+ - = . _ : / @` (the S3 Control regex `[\p{L}\p{Z}\p{N}_.:/=+\-@]`,
/// a superset of the EC2 cross-service ASCII restriction).
fn valid_tag_part(s: &str) -> bool {
    s.chars().all(|c| {
        c.is_alphanumeric()
            || c == ' '
            || matches!(c, '+' | '-' | '=' | '.' | '_' | ':' | '/' | '@')
    })
}

/// A tag key: 1..=128 UTF-16 units (AWS counts UTF-16 positions).
fn valid_key(s: &str) -> bool {
    let units = s.encode_utf16().count();
    units >= 1 && units <= 128 && valid_tag_part(s)
}

/// A tag value: 0..=256 UTF-16 units (empty values are legal).
fn valid_value(s: &str) -> bool {
    s.encode_utf16().count() <= 256 && valid_tag_part(s)
}

/// Percent-encode the wire-reserved characters.
fn percent_encode(s: &str) -> String {
    s.chars()
        .flat_map(|c| match c {
            '%' | '=' | '&' | '+' | ' ' => format!("%{:02X}", c as u32).into_bytes(),
            c => c.to_string().into_bytes(),
        })
        .map(char::from)
        .collect()
}

/// Percent-decode `%XX` sequences (`+` stays literal).
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &s[i + 1..i + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TagError {
    #[error("tag count {0} exceeds the maximum of 10")]
    TooMany(usize),
    #[error("invalid tag key {key:?}")]
    InvalidKey { key: String },
    #[error("invalid tag value {value:?}")]
    InvalidValue { value: String },
    #[error("duplicate tag key {key:?}")]
    Duplicate { key: String },
}
```

Note: malformed percent-encoding (`%zz`) is decoded lossily by `percent_decode` — the charset validation in `from_pairs` then rejects the result (the `k%zz=v` case in the test fails at validation, which the test's `is_err` accepts). Keep `parse_wire`'s error mapping as-is if the test passes either way.

- [x] **Step 4: Add `tags` and `checksum` to `Info`** — `crates/tinio-core/src/object.rs:121-130`:

```rust
    /// Object tags (empty when none).
    pub tags: Tags,
    /// The recorded object checksum (validated at write time under the
    /// `checksum` toggle; `None` when the object has none).
    pub checksum: Option<checksum::Recorded>,
```

(The `checksum::Part` type is already in tinio-core — verify it is `Clone`/`Debug`/`PartialEq` (it is used in `PartInfo`); import it in `object.rs`. Add `checksum::Recorded { part: Part, kind: Type }` next to `Part` — reuse the **existing** `checksum::Type` enum (variants `FullObject`/`Composite`, `SNAKE_CASE` Display → `FULL_OBJECT`/`COMPOSITE`) as the kind; **no new kind enum** (user ruling 2026-09-02: a `ChecksumType` enum would duplicate `Type`). Plain value types, consistent with the module's no-hashing rule. The kind is recorded at write time (FULL_OBJECT for plain PUTs, COMPOSITE for multipart completions, the source's kind for copies) so read paths never derive it.)

- [x] **Step 5: Fix the compile ripple** — run `cargo build -p tinio-core` and fix every `Info { .. }` construction site in tinio-core and tinio-util (harness, benches) with `tags: Tags::empty(), checksum: None` (all sites get the defaults until Tasks 3-4 land the real plumbing; Task 3/4 rework the backend sites).

- [x] **Step 6: Run the tests**

Run: `cargo test -p tinio-core`
Expected: PASS.

- [x] **Step 7: Checkpoint** — report; do not commit.

---

### Task 2: Storage contract — tag methods, write-path params, rename, parts

**Files:**
- Modify: `crates/tinio-core/src/storage/object.rs` (tag methods, `commit_object`/`copy_object` tags params, `rename_object`, `list_object_parts`)
- Modify: `crates/tinio-core/src/storage/bucket.rs` (bucket tag methods)
- Modify: `crates/tinio-core/src/storage/multipart.rs` (`create_multipart_upload` tags param)
- Modify: `crates/tinio-core/src/multipart.rs` (`MultipartUpload` gains `tags: Tags`)

**Interfaces:**
- Consumes: `Tags` (Task 1).
- Produces (Tasks 3-10): the exact new/changed signatures below. Every existing caller (tinio-fs, tinio-mem, tinio-util harness, tests) must be updated in the SAME task that changes the contract — Tasks 3-4 fix the backends, Task 10 fixes the harness; until then the workspace will not compile (`cargo check --workspace` is expected to fail after this task until Tasks 3-4 land).

```rust
// storage/object.rs — ObjectOps additions and changes:
async fn get_object_tags(
    &self,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<Tags, <Self as Storage>::Error>;   // NoSuchKey on a missing object

async fn put_object_tags(
    &self,
    bucket: &bucket::Name,
    key: &object::Key,
    tags: &Tags,
) -> Result<(), <Self as Storage>::Error>;    // replace-all; NoSuchKey on a missing object

async fn delete_object_tags(
    &self,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<(), <Self as Storage>::Error>;    // missing object succeeds

// Existing methods gain trailing parameters:
async fn commit_object(&self, bucket, key, staged: StagedBody, tags: Tags) -> Result<object::Info, Self::Error>;
async fn copy_object(&self, bucket, src, dst, range: Option<ByteRange>, tags: Tags, checksum: Option<checksum::Recorded>) -> Result<object::Info, Self::Error>;

// New:
async fn rename_object(&self, bucket, src, dst) -> Result<object::Info, Self::Error>;  // NoSuchKey on missing src
async fn list_object_parts(&self, bucket, key) -> Result<Vec<ObjectPart>, Self::Error>;  // completed-object parts; empty for non-multipart
```

```rust
// storage/bucket.rs — BucketOps additions:
async fn get_bucket_tags(&self, bucket: &bucket::Name) -> Result<Tags, Self::Error>;   // NoSuchBucket on a missing bucket
async fn put_bucket_tags(&self, bucket: &bucket::Name, tags: &Tags) -> Result<(), Self::Error>;  // replace-all
async fn delete_bucket_tags(&self, bucket: &bucket::Name) -> Result<(), Self::Error>;  // missing bucket succeeds
```

```rust
// storage/multipart.rs:
async fn create_multipart_upload(&self, bucket, key, checksum: Option<checksum::Upload>, tags: Tags) -> Result<MultipartUpload, Self::Error>;
// Completion receives the upload tags and the composite checksum — the
// latter computed by the interface (the response-echo value the complete
// path already derives; grilling Q1b), not by the backend:
async fn complete_multipart_upload(&self, bucket, key, upload_id, parts: &[CompletedPart], tags: Tags, checksum: Option<checksum::Recorded>) -> Result<object::Info, Self::Error>;
// multipart.rs:
pub struct MultipartUpload { ..., pub tags: Tags }
```

`ObjectPart` (the completed-object part row, returned by `list_object_parts`): define it next to `PartInfo` in `tinio-core/src/storage/multipart.rs` — `pub struct ObjectPart { pub part_number: PartNumber, pub size: u64, pub checksum: Option<checksum::Part> }` (no etag — AWS `ObjectPart` has none and the dto has no field for it; if a matching type already exists, reuse it).

**Checksum recording** (same task, tinio-core):

- **`stage_body` gains the tee slot** — `storage/object.rs`:

```rust
async fn stage_body(
    &self,
    bucket: &bucket::Name,
    key: &object::Key,
    body: BodyStream,
    checksum: Option<Arc<checksum::PartChecksum>>,  // the tee slot (upload_part pattern)
) -> Result<StagedBody, <Self as Storage>::Error>;
```

(The interface passes the tee when the `checksum` toggle is on and the client sent a single `x-amz-checksum-*` header; the backend computes the digest while staging and fails the staging with the multipart path's checksum-mismatch error when it does not match — mirror `upload_part`'s tee semantics exactly, reusing the same `StorageError` variant the multipart mismatch maps to.)

- **No new compose helper** — the composite is computed **by the interface** at completion, reusing the value the complete path already derives for the response echo (`derive_full_checksum` → `compose_composite`, `tinio-server/src/backend/checksum.rs:504`, called at `multipart.rs:151-152`). It reaches storage via the new `complete_multipart_upload` checksum parameter (grilling Q1b). tinio-core stays hashing-free (the module's stated design: "Plain value types only — no hashing... All checksum computation lives in tinio-server"). `compose_composite` already has its unit test (`checksum.rs:743-767` — the algorithm over the concatenated raw digest bytes); extend only if gaps surface (a part whose base64 fails to decode → `None`).

- [x] **Step 1: Apply the contract changes** — edit the three trait files and `MultipartUpload` per the signatures above (`copy_object` gains `checksum`, `complete_multipart_upload` gains `tags` + `checksum`); add the `ObjectPart` struct. Keep doc comments in the house style (one paragraph each, mirroring the existing `upload_part` doc).

- [x] **Step 2: Fix the immediate compile ripple in tinio-core** — `cargo check -p tinio-core` and fix the in-crate call sites (the default impls in the trait files, tinio-core tests).

- [x] **Step 3: Checkpoint** — report the contract diff; note that the workspace does not compile until Tasks 3-4 (expected). Do not commit.

---

### Task 3: fs backend — row extensions, tags plumbing, parts retention

**Files:**
- Modify: `crates/tinio-fs/src/database/tables.rs` (`MetaValue` +5th element, `UploadValue` +3rd element, bucket row +tags element, new `OBJECT_PARTS` table — `STATE_VERSION` 2→1 revert + comment rewrite, user ruling 2026-09-02)
- Modify: `crates/tinio-fs/src/database/open.rs` (nothing — the version gate and `ensure` handle the new table)
- Modify: `crates/tinio-fs/src/backend/objects.rs` (`commit_object`/`copy_object` tags, `get/put/delete_object_tags`, `rename_object`)
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`complete_object_state` persists parts to `OBJECT_PARTS`, bucket tags in bucket rows)
- Modify: `crates/tinio-fs/src/multipart.rs` (`create`/`upload_from_row` tags, `MultipartUpload.tags`)
- Modify: `crates/tinio-fs/src/backend/bucket.rs` (or wherever bucket ops live — find it) for bucket tags
- Test: `crates/tinio-fs/src/backend/objects.rs` / `multipart.rs` test modules + `crates/tinio-fs/tests/` (layout tests that assert the DB schema may need updating for the new tuple arity and version)

**Interfaces:**
- Consumes: `Tags` (Task 1), the contract (Task 2).
- Produces: the fs implementation the interface calls in Tasks 6-9. Backends trust validated `Tags` only; the canonical wire string is the persisted form.

- [x] **Step 1: Extend the table definitions** in `tables.rs`:

```rust
// The object row gains the tags and checksum wire strings (empty = none).
type MetaValue = (&'static str, u64, u64, u64, &'static str, &'static str);
// (etag, size, mtime nanos, file identity, tags wire, checksum wire)
// Checksum wire: "<algorithm>:<base64 value>:<kind>" — e.g.
// "crc32:NhCmhg==:FULL_OBJECT" (the `Value` base64 wire form; the kind
// is recorded at write time so read paths never derive it).
// The upload row gains the tags wire string.
type UploadValue = (&'static str, u64, &'static str);          // (key, initiated nanos, tags wire)
// The bucket row gains the tags wire string (find the BUCKETS table
// definition and extend its value tuple the same way).
// New: the completed object's part list (GetObjectAttributes).
const OBJECT_PARTS: TableDefinition<ObjectPartKey, ObjectPartValue> =
    TableDefinition::new("object_parts");
// where ObjectPartKey = (&'static str, &'static str, u32) and
// ObjectPartValue = (u64, &'static str, &'static str) — (size, algorithm wire, base64 checksum value or "").
// STATE_VERSION reverts to 1 (user ruling 2026-09-02: additive
// changes carry no bump; stale dev DBs — delete by hand).
```

Follow the existing patterns exactly: the `TableDefinition` const, the handle struct with `get`/`put`/`for_bucket`-style methods, and registration in the open-time `ensure` list (`open.rs:84-89`). Mirror the `UPLOAD_CHECKSUMS` section (`tables.rs:504-562`) for structure. `validate_stored` (`tables.rs:184-193`) must read the 6-tuple and pass the tags + checksum strings through.

- [x] **Step 2: Write the failing backend tests** (fs test module):

```rust
#[tokio::test]
async fn fs_tags_round_trip_and_replace() {
    let (backend, b) = setup().await;  // mirror the fs test scaffold
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    backend.put_object_tags(&b, &object::key("t.txt").unwrap(), &tags).await.unwrap();
    assert_eq!(
        backend.get_object_tags(&b, &object::key("t.txt").unwrap()).await.unwrap(),
        tags
    );
    let replaced = Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
    backend.put_object_tags(&b, &object::key("t.txt").unwrap(), &replaced).await.unwrap();
    assert_eq!(
        backend.get_object_tags(&b, &object::key("t.txt").unwrap()).await.unwrap(),
        replaced
    );
    backend.delete_object_tags(&b, &object::key("t.txt").unwrap()).await.unwrap();
    assert!(backend.get_object_tags(&b, &object::key("t.txt").unwrap()).await.unwrap().is_empty());
    // Missing object: get/put → NoSuchKey, delete succeeds.
    let err = backend.get_object_tags(&b, &object::key("missing.txt").unwrap()).await.unwrap_err();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    backend.delete_object_tags(&b, &object::key("missing.txt").unwrap()).await.unwrap();
}

#[tokio::test]
async fn fs_commit_and_copy_carry_tags() {
    let (backend, b) = setup().await;
    let tags = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let staged = backend.stage_body(&b, &object::key("a.txt").unwrap(), Box::pin(stream::iter([Ok::<_, IoError>(Bytes::from_static(b"hi"))]))).await.unwrap();
    backend.commit_object(&b, &object::key("a.txt").unwrap(), staged, tags.clone()).await.unwrap();
    assert_eq!(backend.head_object(&b, &object::key("a.txt").unwrap()).await.unwrap().tags, tags);
    // Copy carries tags.
    backend.copy_object(&b, &object::key("a.txt").unwrap(), &object::key("b.txt").unwrap(), None, tags.clone(), None).await.unwrap();
    assert_eq!(backend.get_object_tags(&b, &object::key("b.txt").unwrap()).await.unwrap(), tags);
}

#[tokio::test]
async fn fs_complete_retains_object_parts() {
    // Create a multipart upload, upload two parts, complete — then
    // list_object_parts returns the two parts with sizes and per-part
    // checksums, and the completed object row carries the composite
    // checksum passed through the completion params.
    // (mirror the fs multipart test scaffold; assert the OBJECT_PARTS
    // rows, the list_object_parts result, and Info.checksum)
}

#[tokio::test]
async fn fs_object_parts_lifecycle() {
    // (a) overwriting a completed multipart object via commit/copy
    // removes its OBJECT_PARTS rows (the new object has no parts);
    // (b) delete removes the rows; (c) rename migrates them with the
    // record; (d) copy_object never inherits the source's parts.
}
```

(Mirror the existing fs test scaffold — check how the fs tests construct a backend + bucket, and how the multipart tests complete an upload; reuse their helpers verbatim.)

- [x] **Step 3: Run to verify they fail**

Run: `cargo test -p tinio-fs fs_tags_ fs_commit_and_copy_carry_tags fs_complete_retains_object_parts`
Expected: FAIL — the methods do not exist yet.

- [x] **Step 4: Implement the fs plumbing**:
  - `tables.rs`: the tuple extensions + `OBJECT_PARTS` table + version bump (Step 1).
  - `StoredMeta`/`validate_stored`: carry the tags and checksum strings; a checksum-wire parser `(algorithm, value) → Option<checksum::Part>` next to the row types (garbage → `None`, self-healing like the etag).
  - `backend/objects.rs`: `stage_body` accepts the tee slot and computes/validates the digest while staging (mirror `upload_part`'s tee) — `StagedBody` gains the computed `Part` field (`checksum: Option<checksum::Part>`, the kind FULL_OBJECT is set at commit); `commit_object` writes the 6-tuple (tags from the param, checksum from the tee result when present) and **removes any stale `OBJECT_PARTS` rows for the key in the same transaction**; `copy_object` copies the row (tags param wins — the interface already resolved the directive; checksum written from the new `checksum` param — grilling Q3a) and **removes the destination's stale `OBJECT_PARTS` rows** (a copy is single-part); `delete_object` (and `delete_objects`) **removes the key's `OBJECT_PARTS` rows in the same transaction**; new `get_object_tags`/`put_object_tags`/`delete_object_tags` (read/insert/remove the tags element; `NoSuchKey` per contract); `rename_object` (one write txn: remove the src row, insert the dst row with the same etag/size/mtime/identity/tags/checksum **and migrate the src's `OBJECT_PARTS` rows to the dst key** — the object file is content-shared by identity, so no file move; if the layout is per-key files, move the file in the same critical section).
  - `backend/mod.rs` `complete_object_state`: in the completion transaction, write each part's `(size, algorithm wire, base64 value)` into `OBJECT_PARTS` for the completed key (sizes available from the assembled parts; mirror `PART_CHECKSUMS`'s join), **replace any stale `OBJECT_PARTS` rows for the key**, and write the object row's tags + checksum elements **from the new `complete_multipart_upload` params** (the composite is computed by the interface — grilling Q1b — no hashing in the backend).
  - `multipart.rs`: `create` writes the tags wire into the `UPLOADS` row; `upload_from_row` reads it into `MultipartUpload.tags`; `complete_multipart_upload` accepts the new params and hands them to `complete_object_state`.
  - bucket ops file: bucket rows carry tags; `get/put/delete_bucket_tags` (missing bucket → `NoSuchBucket` per contract).
  - `head_object`/`get_object`: parse the tags and checksum elements into `Info` (checksum wire `<algo>:<base64>:<kind>` → `checksum::Recorded`; garbage → `None`, self-healing like the etag).
  - `TinioFs` test fixtures: add `tags: Tags::empty(), checksum: None` where the new params require it.

- [x] **Step 5: Run the fs suite**

Run: `cargo test -p tinio-fs`
Expected: PASS (layout/proptest tests updated for the new arity and version as needed).

- [x] **Step 6: Checkpoint** — report; do not commit.

---

### Task 4: mem backend — same plumbing

**Files:**
- Modify: `crates/tinio-mem/src/storage.rs` (row tuples: `OBJECT_META` +tags element, `UPLOADS` +tags, bucket row +tags, new `OBJECT_PARTS` table)
- Modify: `crates/tinio-mem/src/object.rs` (`commit_object`/`copy_object`/tag methods/`rename_object`/`list_object_parts`)
- Modify: `crates/tinio-mem/src/multipart.rs` (`create`/read tags)
- Modify: `crates/tinio-mem/src/bucket.rs` (or wherever bucket ops live) for bucket tags

**Interfaces:**
- Consumes: the contract (Task 2), `Tags` (Task 1).
- Produces: the mem implementation the interface calls in Tasks 6-9.

- [x] **Step 1: Write the failing tests** — the same tests as Task 3 Step 2 against the mem backend (copy the test bodies; swap the backend constructor for `MemoryStorage::new().unwrap()` + a bucket — mirror the mem test scaffold).

- [x] **Step 2: Run to verify they fail**

Run: `cargo test -p tinio-mem tags_`
Expected: FAIL.

- [x] **Step 3: Implement** — mirror Task 3 exactly over the in-memory redb: extend `OBJECT_META` (etag, size, last-modified, tags wire, checksum wire incl. kind), `UPLOADS` (initiated, tags wire), the bucket row, and add `OBJECT_PARTS`; implement the five object methods, `stage_body` tee (incl. the `StagedBody` checksum field), `rename_object` (one txn: remove src row, insert dst row, migrate OBJECT_PARTS), `list_object_parts`, the OBJECT_PARTS lifecycle (overwrite/delete cleanup, copy never inherits), `complete_multipart_upload` params, bucket tag methods, and `MultipartUpload.tags` plumbing.

- [x] **Step 4: Run the mem suite**

Run: `cargo test -p tinio-mem`
Expected: PASS.

- [x] **Step 5: Workspace compile checkpoint** — `cargo check --workspace` now compiles except the still-unupdated harness call sites (Task 10 fixes those); the tinio-server backend no longer compiles if `commit_object`/`copy_object`/`complete_multipart_upload` call sites lack the new params — fix them with `Tags::empty()` + `None` **temporarily** in tinio-server if the interface tasks (6-9) are not yet landed, or better: land Tasks 5-9 before running the workspace check. Sequence note: execute Tasks 5-9 before expecting `cargo test --workspace` to be green.

- [x] **Step 6: Checkpoint** — report; do not commit.

---

### Task 5: `tagging` capability toggle (tinio-config + e2e tags)

**Files:**
- Modify: `crates/tinio-config/src/schema/s3.rs` (`Capabilities.tagging`)
- Modify: `crates/tinio-config/src/schema/s3.rs` tests (schema validation fixtures)
- Modify: `crates/tinio-e2e/tests/steps/mod.rs:56-67` (`config_from_tags`: `@tagging-off`, `@minimal-caps` clears six)

**Interfaces:**
- Consumes: the existing `Capabilities` struct pattern (`multipart`/`copy_object`/`checksum`).
- Produces: `caps.tagging: bool` (default `true`), consumed by Tasks 6-7 via `Self::require_cap(self.caps.tagging, "...")`.

- [x] **Step 1: Write the failing config test** (tinio-config schema tests — mirror the existing toggle tests):

```rust
#[test]
fn tagging_defaults_on_and_can_be_disabled() {
    // The default config has tagging enabled.
    let caps = Capabilities::default();
    assert!(caps.tagging);
    // A config with tagging: false round-trips.
    let toml = "tagging = false";
    let caps: Capabilities = toml::from_str(toml).unwrap();
    assert!(!caps.tagging);
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-config tagging_defaults_on`
Expected: FAIL — `tagging` field missing.

- [x] **Step 3: Implement** — add to `Capabilities` in `tinio-config/src/schema/s3.rs` (mirror the `checksum` field: `#[serde(default = "default_true")]` or the pattern the file uses):

```rust
    /// Object and bucket tagging (Get/Put/Delete*Tagging). Default on.
    pub tagging: bool,
```

Update the schema validation fixtures/tests that enumerate the fields. In `tinio-e2e/tests/steps/mod.rs` `config_from_tags`:

```rust
    if tagged("tagging-off") {
        caps.tagging = false;
    }
    if tagged("minimal-caps") {
        caps.multipart = false;
        caps.copy_object = false;
        caps.list_objects_v1 = false;
        caps.list_objects_v2 = false;
        caps.delete_objects = false;
        caps.tagging = false;
    }
```

- [x] **Step 4: Run the config + e2e compile tests**

Run: `cargo test -p tinio-config` and `cargo check -p tinio-e2e`
Expected: PASS.

- [x] **Step 5: Checkpoint** — report; do not commit.

---

### Task 6: Interface — object tagging ops + write-path wiring

**Files:**
- Create: `crates/tinio-server/src/backend/tags.rs` (the `x-amz-tagging` parse/validate mapping + dto TagSet conversion)
- Modify: `crates/tinio-server/src/backend/objects.rs` (`op_get_object_tagging` real tags; new `op_put_object_tagging`, `op_delete_object_tagging`; `op_put_object` tagging; `op_copy_object` directive; `op_get_object`/`op_head_object` tagging-count)
- Modify: `crates/tinio-server/src/backend/multipart.rs` (`op_create_multipart_upload` tagging; `op_complete_multipart_upload` passes upload tags)
- Modify: `crates/tinio-server/src/backend/s3.rs` (overrides: `put_object_tagging`, `delete_object_tagging`)
- Modify: `crates/tinio-server/src/metrics.rs` (wrappers + delegation test fixture)
- Modify: `crates/tinio-server/src/backend/mod.rs` (declare/re-export the tags module)

**Interfaces:**
- Consumes: `Tags` (Task 1), `caps.tagging` (Task 5), the contract (Task 2), Plan A's `conditions.rs`.
- Produces: the six-ops-total tagging surface; `x-amz-tagging` on every write path; `x-amz-tagging-count` on GET/HEAD.

- [x] **Step 1: Write the failing tests** (objects.rs test module):

```rust
#[tokio::test]
async fn object_tagging_ops_round_trip() {
    let (backend, b) = setup_name().await;
    backend
        .storage()
        .put_object(&b, &"t.txt".into(), body(b"x"))
        .await
        .unwrap();

    // Put → Get round-trip.
    let tags = vec![dto::Tag { key: Some("env".into()), value: Some("prod".into()) }];
    backend
        .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            tagging: dto::Tagging { tag_set: tags.clone() },
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(got.output.tag_set, tags);

    // Replace-all semantics.
    let other = vec![dto::Tag { key: Some("a".into()), value: Some("1".into()) }];
    backend
        .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            tagging: dto::Tagging { tag_set: other.clone() },
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(got.output.tag_set, other);

    // Delete clears; a missing object answers 404 on get/put and 204 on delete.
    backend
        .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(got.output.tag_set.is_empty());
    let err = backend
        .get_object_tagging(s3_request(dto::GetObjectTaggingInput {
            bucket: b.to_string(),
            key: "missing.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchKey");
    backend
        .delete_object_tagging(s3_request(dto::DeleteObjectTaggingInput {
            bucket: b.to_string(),
            key: "missing.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
}

#[tokio::test]
async fn put_object_tagging_validation_rejects_bad_sets() {
    let (backend, b) = setup_name().await;
    backend
        .storage()
        .put_object(&b, &"t.txt".into(), body(b"x"))
        .await
        .unwrap();
    // Duplicate keys → InvalidTag.
    let err = backend
        .put_object_tagging(s3_request(dto::PutObjectTaggingInput {
            bucket: b.to_string(),
            key: "t.txt".into(),
            tagging: dto::Tagging {
                tag_set: vec![
                    dto::Tag { key: Some("k".into()), value: Some("1".into()) },
                    dto::Tag { key: Some("k".into()), value: Some("2".into()) },
                ],
            },
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "InvalidTag");
}

#[tokio::test]
async fn put_object_validates_and_records_checksums() {
    // The checksum cap defaults OFF — mirror the multipart checksum
    // tests' fixture (`setup_checksum()`: a caps with `checksum: true`).
    let (backend, b) = setup_checksum().await;
    let put = |crc32: Option<String>| {
        backend.put_object(s3_request(dto::PutObjectInput {
            bucket: b.to_string(),
            key: "c.txt".into(),
            body: Some(StreamingBlob::wrap(stream::once(async {
                Ok::<_, io::Error>(Bytes::from_static(b"hello"))
            }))),
            checksum_crc32: crc32,
            ..Default::default()
        }))
    };
    // A mismatching client checksum fails the put (BadDigest) — the
    // checksum cap is enabled in the fixture (it defaults OFF). (dto
    // values are the base64 wire form; "AAAAAAAAAAA=" is base64 of
    // four zero bytes.)
    let err = put(Some("AAAAAAAAAAA=".into())).await.unwrap_err();
    assert_eq!(err.code().as_str(), "BadDigest");
    // A matching one succeeds and is recorded in the object metadata.
    put(Some("NhCmhg==".into())).await.unwrap();  // crc32("hello"), base64 wire form
    let info = backend
        .storage()
        .head_object(&b, &object::key("c.txt").unwrap())
        .await
        .unwrap();
    let recorded = info.checksum.unwrap();
    assert_eq!(recorded.part.value.0, "NhCmhg==");
    assert_eq!(recorded.kind, checksum::Type::FullObject);

    // GET echoes the recorded checksum (grilling Q7) — assert via the
    // dto newtype's accessor (adjust to its exact shape).
    let got = backend
        .get_object(s3_request(dto::GetObjectInput {
            bucket: b.to_string(),
            key: "c.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(got.output.checksum_crc32.as_ref().map(|v| v.as_str()), Some("NhCmhg=="));
}
```

(The crc32 of `"hello"` is 0x3610a686 — base64 `"NhCmhg=="` in the wire form; if the checksum module's crc32 flavor differs from IEEE 802.3/zlib, compute the value with the module's own primitive in the test instead of hard-coding.)

- [x] **Step 2: Run to verify they fail**

Run: `cargo test -p tinio-server object_tagging_`
Expected: FAIL — `put_object_tagging`/`delete_object_tagging` are NotImplemented (default), GetObjectTagging returns the fake empty set.

- [x] **Step 3: Implement `backend/tags.rs`**:

```rust
//! Tagging wire helpers: the `x-amz-tagging` header and the dto TagSet
//! ↔ core `Tags` conversions. The core type validates; these map the
//! wire forms onto it (InvalidTag on any violation).

use s3s::{S3Error, dto, s3_error};
use crate::_core::object::Tags;

/// Parse the `x-amz-tagging` value (URL-encoded `k=v&k2=v2`). s3s
/// surfaces the header as the dto field `tagging: Option<TaggingHeader>`
/// on Put/Copy/CreateMultipartUpload (generated.rs:19376/2806/4063) —
/// NOT a raw header — so the ops pass that field in.
pub(crate) fn parse_tagging_header(
    tagging: Option<&dto::TaggingHeader>,
) -> Result<Option<Tags>, S3Error> {
    let Some(value) = tagging else {
        return Ok(None);
    };
    let text = value.as_str();
    Tags::parse_wire(text)
        .map(Some)
        .map_err(|e| s3_error!(InvalidTag, "{e}"))
}

/// A dto `TagSet` (Put*Tagging body) into core `Tags`. The count cap
/// is per surface: 10 for object tagging, 50 for bucket tagging
/// (AWS-verified per-surface limits).
pub(crate) fn tags_from_tag_set(tag_set: &[dto::Tag], limit: usize) -> Result<Tags, S3Error> {
    let pairs = tag_set.iter().map(|t| {
        let key = t.key.as_ref().map(|k| k.to_string()).unwrap_or_default();
        let value = t.value.as_ref().map(|v| v.to_string()).unwrap_or_default();
        (key, value)
    });
    Tags::from_pairs_limited(pairs, limit).map_err(|e| s3_error!(InvalidTag, "{e}"))
}

/// Core `Tags` into a dto `TagSet` (GetObjectTagging output).
pub(crate) fn tag_set_from_tags(tags: &Tags) -> Vec<dto::Tag> {
    tags.iter()
        .map(|(k, v)| dto::Tag { key: Some(k.into()), value: Some(v.into()) })
        .collect()
}
```

(Check the exact `dto::Tag` field types in s3s 0.15 — `ObjectKey`/`Value` newtypes; `.into()` from `String` is expected, adjust if the types differ. `TaggingHeader` is a string newtype (derefs to `str`) — adjust `parse_tagging_header`'s accessor to its exact shape.)

- [x] **Step 4: Implement the ops** — `objects.rs`:
  - `op_get_object_tagging`: replace the fabricated empty set with `tag_set_from_tags(&head.tags)` — the existence head already runs; keep the 404.
  - New `op_put_object_tagging` / `op_delete_object_tagging` (pattern: bucket/key validation → `require_cap(self.caps.tagging, ...)` → storage call; `put` validates via `tags_from_tag_set(&tag_set, 10)`).
  - `op_put_object`: parse `req.input.tagging` (only when `self.caps.tagging`; accept-and-drop otherwise, per spec) → `commit_object(..., tags)`. Checksum recording: when `self.caps.checksum` and the request carries a single `x-amz-checksum-*` header, build the tee slot (reuse the multipart path's `single_checksum_value` helper from `multipart.rs` — move it to a shared spot if `multipart.rs`'s cfg feature hides it from the objects path, or duplicate the small parse) and pass it to `stage_body`; the tee validates while staging (mismatch → the same error mapping as multipart). Toggle off → today's accept-and-drop, no tee.
  - `op_copy_object`: read `req.input.tagging_directive` (COPY default / REPLACE); COPY → `head_object` the source (already done for conditions — reuse the fetched info's `tags` and recorded `checksum`); REPLACE → `parse_tagging_header(req.input.tagging.as_ref())` (empty when absent). Pass the resolved tags **and the source's recorded checksum** to `copy_object`.
  - `op_get_object`/`op_head_object`: when `self.caps.tagging` and the info has tags — GET sets `tag_count: Some(tags.len() as i32)` on the output (only when non-empty); HEAD sets the response header on the `S3Response` (`let mut resp = S3Response::new(...); resp.headers.insert("x-amz-tagging-count", tags.len().to_string()); resp` — only when non-empty).
  - **Checksum echo (grilling Q7)**: when `info.checksum` is recorded, GET and HEAD set the matching per-algorithm field plus `checksum_type` on the output — `GetObjectOutput`/`HeadObjectOutput` both have all ten `checksum_*` fields + `checksum_type` (no algorithm-generic helper), so extend the `HasFields` macro impl (`backend/checksum.rs:222-241`) to the two outputs and call `set_checksum`; set `checksum_type` from the recorded kind. No `x-amz-checksum-mode` gating. (Nothing is recorded when the `checksum` toggle was off at write time, so no runtime gating is needed here.)
- `multipart.rs`:
  - `op_create_multipart_upload`: parse `req.input.tagging` (when `caps.tagging`) → pass to `create_multipart_upload`.
  - `op_complete_multipart_upload`: extend the existing upload-state fetch condition (`self.caps.checksum || self.caps.tagging`) and pass `upload.tags` plus the composite already computed for the response echo (`echo_checksum`, `multipart.rs:618`) to `complete_multipart_upload`.
- `s3.rs`: add the `put_object_tagging` / `delete_object_tagging` overrides.
- `metrics.rs`: add the two wrappers (mirror `get_object_tagging` at `metrics.rs:1013-1019`) + the delegation-test fixture inputs (`PutObjectTaggingInput { tagging: Tagging { tag_set: vec![] } }`-style minimal inputs).

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS (the two new tests + the existing `get_object_tagging_answers_empty_set` HTTP test — that test's name/assertions now need updating: it asserts the EMPTY set is returned for a tagged-less object, which still holds; keep it).

- [x] **Step 6: Checkpoint** — report; do not commit.

---

### Task 7: Interface — bucket tagging ops

**Files:**
- Modify: `crates/tinio-server/src/backend/buckets.rs` (or wherever bucket ops live — find it; `op_put_bucket_tagging`, `op_get_bucket_tagging`, `op_delete_bucket_tagging`)
- Modify: `crates/tinio-server/src/backend/s3.rs` (three overrides)
- Modify: `crates/tinio-server/src/metrics.rs` (three wrappers + fixture)
- Test: the buckets backend test module

**Interfaces:**
- Consumes: `tags_from_tag_set`/`tag_set_from_tags` (Task 6), `caps.tagging` (Task 5), the bucket tag contract methods (Task 2).
- Produces: the three bucket tagging ops (missing bucket → `NoSuchBucket` on get/put; delete on a missing bucket succeeds).

- [x] **Step 1: Write the failing test** (buckets test module):

```rust
#[tokio::test]
async fn bucket_tagging_ops_round_trip() {
    let (backend, b) = setup_name().await;
    let tags = vec![dto::Tag { key: Some("team".into()), value: Some("core".into()) }];
    backend
        .put_bucket_tagging(s3_request(dto::PutBucketTaggingInput {
            bucket: b.to_string(),
            tagging: dto::Tagging { tag_set: tags.clone() },
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
            bucket: b.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(got.output.tag_set, tags);
    backend
        .delete_bucket_tagging(s3_request(dto::DeleteBucketTaggingInput {
            bucket: b.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    let got = backend
        .get_bucket_tagging(s3_request(dto::GetBucketTaggingInput {
            bucket: b.to_string(),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(got.output.tag_set.is_empty());
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-server bucket_tagging_ops_round_trip`
Expected: FAIL — NotImplemented.

- [x] **Step 3: Implement** — the three ops following the Task 6 pattern (`require_cap(self.caps.tagging, ...)`; `put` validates via `tags_from_tag_set(&tagging.tag_set, 50)` — the bucket cap; `get` returns `tag_set_from_tags`; `delete` calls the contract method), the `s3.rs` overrides, and the `MetricS3` wrappers + delegation-test fixtures.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [x] **Step 5: Checkpoint** — report; do not commit.

---

### Task 8: Interface — RenameObject

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs` (`op_rename_object`)
- Modify: `crates/tinio-server/src/backend/s3.rs` (the `rename_object` override, gated like `copy_object`)
- Modify: `crates/tinio-server/src/metrics.rs` (wrapper + fixture)
- Test: objects.rs test module

**Interfaces:**
- Consumes: Plan A's `ConditionalHeaders`, `check_missing`, `condition_error`; the contract `rename_object` (Task 2).
- Produces: `op_rename_object` — atomic source→destination move under dual per-key locks (sorted order); an existing destination with no destination conditions is **overwritten**; source == destination → 412; `source_if_*` against the source (missing source → 404 `NoSuchKey` — tinio's choice, AWS silent); `destination_if_*` via the shared `check_missing` policy (AWS-verified: missing destination + If-None-Match → proceed, If-Match on missing → 412 — `check_missing` implements exactly this).

- [x] **Step 1: Check the dto field types** — read `s3s-0.15.0/src/dto/generated.rs` around `RenameObjectInput` (lines 20352-20404): confirmed `source_if_match`/`source_if_none_match` alias `String` (not `ETagCondition`); `destination_if_*` are the standard `IfMatch`/`IfNoneMatch` newtypes (plain `If-Match`/`If-None-Match` headers on the wire — no `x-amz-rename-object-destination-*`); still confirm the exact field names before writing the op.

- [x] **Step 2: Write the failing test** (objects.rs test module):

```rust
#[tokio::test]
async fn rename_object_moves_with_conditions() {
    let (backend, b) = setup_name().await;
    let etag = "5d41402abc4b2a76b9719d911017c592";
    backend
        .storage()
        .put_object(&b, &"src.txt".into(), body(b"hello"))
        .await
        .unwrap();

    // Source mismatch → 412; the object stays put.
    let err = backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "src.txt".into(),
            destination_key: "dst.txt".into(),
            source_if_match: Some(r#""deadbeef""#.into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");
    assert!(backend.storage().head_object(&b, &object::key("src.txt").unwrap()).await.is_ok());

    // A matching source condition moves the object.
    backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "src.txt".into(),
            destination_key: "dst.txt".into(),
            source_if_match: Some(format!("\"{etag}\"").into()),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(backend.storage().head_object(&b, &object::key("dst.txt").unwrap()).await.is_ok());
    assert!(matches!(
        backend.storage().head_object(&b, &object::key("src.txt").unwrap()).await.unwrap_err(),
        StorageError::NoSuchKey(_)
    ));

    // A missing source answers NoSuchKey (404).
    let err = backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "absent.txt".into(),
            destination_key: "x.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "NoSuchKey");

    // Destination If-None-Match: * fails against an existing destination.
    backend
        .storage()
        .put_object(&b, &"other.txt".into(), body(b"x"))
        .await
        .unwrap();
    let err = backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "dst.txt".into(),
            destination_key: "other.txt".into(),
            destination_if_none_match: Some("*".into()),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");

    // An existing destination with NO destination conditions is overwritten.
    backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "dst.txt".into(),
            destination_key: "other.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap();

    // Source == destination → 412.
    let err = backend
        .rename_object(s3_request(dto::RenameObjectInput {
            bucket: b.to_string(),
            key: "other.txt".into(),
            destination_key: "other.txt".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code().as_str(), "PreconditionFailed");
}
```

(Adjust the field names/types to what Step 1 found — the `String`-typed source conditions parse with ETag semantics: `dto::ETagCondition::from_str(&s)` → on failure 400 `InvalidArgument`, mirroring `parse_etag_condition_header`.)

- [x] **Step 3: Run to verify it fails**

Run: `cargo test -p tinio-server rename_object_moves_with_conditions`
Expected: FAIL — NotImplemented.

- [x] **Step 4: Implement `op_rename_object`** (in `objects.rs`, `#[cfg(feature = "copy")]` like `op_copy_object`):

```rust
    /// RenameObject — atomically move source → destination with
    /// source and destination conditionals (AWS directory-bucket op;
    /// tinio honors it on the general-purpose model). Dual per-key
    /// locks, acquired in sorted order so two concurrent renames can
    /// never deadlock; the source and destination heads and the move
    /// run inside the critical sections.
    pub(crate) async fn op_rename_object(
        &self,
        req: S3Request<dto::RenameObjectInput>,
    ) -> S3Result<S3Response<dto::RenameObjectOutput>> {
        Self::require_cap(self.caps.copy_object, "RenameObject")?;
        let bucket = self.bucket(req.input.bucket)?;
        let src = self.key(req.input.key)?;
        let dst = self.key(req.input.destination_key)?;
        // Source == destination is degenerate — 412 (grilling Q8c),
        // built with the same 412 constructor the conditional paths
        // use (`condition_error` in conditions.rs) — keep one path.
        if src == dst {
            return Err(condition_error(ConditionFailure::Match, true));
        }
        // Sorted lock acquisition (wire keys) — deadlock-free.
        let (first, second) = if src.as_ref() <= dst.as_ref() {
            (src.clone(), dst.clone())
        } else {
            (dst.clone(), src.clone())
        };
        let _guard_a = self.lock_object(&bucket, &first).await;
        let _guard_b = self.lock_object(&bucket, &second).await;
        // Source conditions (parse the String-typed dto values with
        // ETag semantics; a missing source → NoSuchKey).
        let src_info = self
            .storage
            .head_object(&bucket, &src)
            .await
            .map_err(map_backend_error)?;
        ConditionalHeaders::new(
            parse_rename_etag(req.input.source_if_match.as_deref(), "source If-Match")?.as_ref(),
            parse_rename_etag(req.input.source_if_none_match.as_deref(), "source If-None-Match")?.as_ref(),
            // Source modified-since fields are `Timestamp` aliases
            // (Step 1) — wrap/convert to the `IfModifiedSince`
            // newtypes the evaluator takes.
            req.input.source_if_modified_since.map(dto::IfModifiedSince::from),
            req.input.source_if_unmodified_since.map(dto::IfUnmodifiedSince::from),
        )
        .check(&src_info.etag, src_info.last_modified, true)?;
        // Destination conditions are the plain If-* newtypes on the
        // wire (Step 1) — use them directly, no parsing.
        let dst_conditions = ConditionalHeaders::new(
            req.input.destination_if_match.as_ref(),
            req.input.destination_if_none_match.as_ref(),
            req.input.destination_if_modified_since,
            req.input.destination_if_unmodified_since,
        );
        match self.storage.head_object(&bucket, &dst).await {
            Ok(info) => dst_conditions.check(&info.etag, info.last_modified, true)?,
            Err(err) => {
                let err: StorageError = err.into();
                match err {
                    StorageError::NoSuchKey(_) => dst_conditions.check_missing(true)?,
                    err => return Err(map_backend_error(err)),
                }
            }
        }
        let info = self
            .storage
            .rename_object(&bucket, &src, &dst)
            .await
            .map_err(map_backend_error)?;
        Ok(S3Response::new(dto::RenameObjectOutput {
            e_tag: Some(Self::etag_wire(&info.etag)),
            ..Default::default()
        }))
    }
```

Plus the helper `parse_rename_etag` (in `objects.rs` or `tags.rs`-style): `Option<&str> → Result<Option<dto::ETagCondition>, S3Error>` via `ETagCondition::from_str`, 400 `InvalidArgument` on failure. Add the `s3.rs` override (`#[cfg(feature = "copy")]`), the `MetricS3` wrapper, and the delegation-test fixture input.

- [x] **Step 5: Run the tests**

Run: `cargo test -p tinio-server`
Expected: PASS.

- [x] **Step 6: Checkpoint** — report; do not commit.

---

### Task 9: Interface — GetObjectAttributes

**Files:**
- Modify: `crates/tinio-server/src/backend/objects.rs` (`op_get_object_attributes`)
- Modify: `crates/tinio-server/src/backend/s3.rs` (override)
- Modify: `crates/tinio-server/src/metrics.rs` (wrapper + fixture)
- Test: objects.rs test module

**Interfaces:**
- Consumes: the contract `list_object_parts` (Task 2), `head_object`.
- Produces: `op_get_object_attributes` — ETag / ObjectSize / StorageClass / Checksum / ObjectParts per the requested `object_attributes` list, with pagination (`max_parts` default 1000, `part_number_marker`, `is_truncated`, `next_part_number_marker`, `total_parts_count` — interface-level slicing, grilling Q2a). Checksum = the **recorded** object checksum (`Info.checksum`): plain PUTs recorded at write time (Task 6), multipart composites recorded at completion; absent when the object has none.

- [x] **Step 1: Write the failing op test** (objects.rs test module — multipart flow via the backend ops, mirroring the multipart test scaffold; `compose_composite` itself already has its unit test in `backend/checksum.rs:743-767`):

```rust
#[tokio::test]
async fn get_object_attributes_returns_the_requested_subset() {
    let (backend, b) = setup_name().await;
    backend
        .storage()
        .put_object(&b, &"plain.txt".into(), body(b"hello"))
        .await
        .unwrap();
    // Plain PUT: ETag/ObjectSize/StorageClass; no parts, no checksum.
    let got = backend
        .get_object_attributes(s3_request(dto::GetObjectAttributesInput {
            bucket: b.to_string(),
            key: "plain.txt".into(),
            object_attributes: vec!["ETag".into(), "ObjectSize".into(), "StorageClass".into(), "ObjectParts".into()],
            ..Default::default()
        }))
        .await
        .unwrap();
    let out = got.output;
    assert!(out.e_tag.is_some());
    assert_eq!(out.object_size, Some(5));
    assert!(out.object_parts.is_none());  // non-multipart omits parts

    // Multipart object: ObjectParts lists the retained parts; the
    // Checksum attribute echoes the recorded composite (uploaded with
    // a checksum spec).
    // (create → upload two parts with checksums → complete via the
    // backend ops — fixture with the checksum cap enabled, mirror
    // Task 6's setup_checksum() — then get_object_attributes with
    // ObjectParts and Checksum; assert the parts and the recorded
    // checksum. Also request ObjectParts with max_parts=1 →
    // is_truncated + a next_part_number_marker, covering the
    // pagination path.)
}
```

- [x] **Step 2: Run to verify it fails**

Run: `cargo test -p tinio-server get_object_attributes_`
Expected: FAIL — NotImplemented.

- [x] **Step 3: Implement `op_get_object_attributes`** (objects.rs):

```rust
    /// GetObjectAttributes — the requested subset of ETag / ObjectSize
    /// / StorageClass / Checksum / ObjectParts. The Checksum attribute
    /// echoes the RECORDED object checksum (`Info.checksum`, recorded
    /// at write time under the checksum toggle); ObjectParts comes from
    /// the retained part list, paginated at the interface layer per
    /// `max_parts` (default 1000) and `part_number_marker` (grilling
    /// Q2a); non-multipart objects omit it (AWS).
    pub(crate) async fn op_get_object_attributes(
        &self,
        req: S3Request<dto::GetObjectAttributesInput>,
    ) -> S3Result<S3Response<dto::GetObjectAttributesOutput>> {
        let bucket = self.bucket(req.input.bucket)?;
        let key = self.key(req.input.key)?;
        let info = self
            .storage
            .head_object(&bucket, &key)
            .await
            .map_err(map_backend_error)?;
        let want = |a: &str| {
            req.input
                .object_attributes
                .iter()
                .any(|x| x.as_str() == a)
        };
        let mut out = dto::GetObjectAttributesOutput {
            e_tag: want("ETag").then(|| Self::etag_wire(&info.etag)),
            object_size: want("ObjectSize").then_some(info.size as i64),
            ..Default::default()
        };
        if want("StorageClass") {
            out.storage_class = Some(dto::StorageClass::STANDARD);  // tinio has no tiers
        }
        if want("Checksum")
            && let Some(recorded) = info.checksum.as_ref()
        {
            // dto::Checksum has per-algorithm fields (checksum_crc32,
            // checksum_crc32c, ...) plus checksum_type — no generic
            // algorithm/value pair. Fill via the HasFields matcher and
            // set the kind recorded at write time.
            let mut fields = dto::Checksum::default();
            fields.set_checksum(recorded.part.algorithm, &recorded.part.value.0);
            fields.checksum_type = Some(recorded.kind.wire_name().into());
            out.checksum = Some(fields);
        }
        if want("ObjectParts") {
            let parts = self
                .storage
                .list_object_parts(&bucket, &key)
                .await
                .map_err(map_backend_error)?;
            if !parts.is_empty() {
                // Interface-level pagination: skip parts at/below the
                // marker, keep at most max_parts, flag truncation.
                let marker = req
                    .input
                    .part_number_marker
                    .and_then(|m| m.parse::<u32>().ok());
                let max = req
                    .input
                    .max_parts
                    .and_then(|m| m.parse::<u32>().ok())
                    .unwrap_or(1000);
                let iter = parts
                    .iter()
                    .filter(|p| marker.map_or(true, |m| p.part_number.get() > m));
                let page: Vec<_> = iter.clone().take(max as usize).collect();
                let is_truncated = iter.count() > page.len();
                let next_marker =
                    is_truncated.then(|| page.last().map(|p| p.part_number.get())).flatten();
                out.object_parts = Some(dto::GetObjectAttributesParts {
                    parts: Some(
                        page.iter()
                            .map(|p| {
                                let mut part = dto::ObjectPart {
                                    part_number: Some(p.part_number.get() as i32),
                                    size: Some(p.size as i64),
                                    ..Default::default()
                                };
                                if let Some(c) = p.checksum.as_ref() {
                                    // Per-part checksum_* fields directly;
                                    // no nested container, no type.
                                    part.set_checksum(c.algorithm, &c.value.0);
                                }
                                part
                            })
                            .collect(),
                    ),
                    is_truncated: Some(is_truncated),
                    next_part_number_marker: next_marker.map(|n| n.to_string().into()),
                    total_parts_count: Some(parts.len() as i32),
                    ..Default::default()
                });
            }
        }
        Ok(S3Response::new(out))
    }
```

(Adjust the dto field names to the s3s 0.15 shapes — the parts container is `GetObjectAttributesParts`, `dto::Checksum` has per-algorithm fields (`checksum_crc32`…) + `checksum_type`, and `ObjectPart` carries the per-algorithm `checksum_*` fields directly (no nested container, no `checksum_type`) — read `generated.rs:15494-15537` and the output struct before writing. Extend the `HasFields` macro impl (`backend/checksum.rs:222-241`) to `dto::Checksum` and `dto::ObjectPart`. `part_number.get()` — `PartNumber`'s accessor; check the actual types.) Add the `s3.rs` override, `MetricS3` wrapper, and delegation-test fixture.

- [x] **Step 4: Run the tests**

Run: `cargo test -p tinio-server -p tinio-core`
Expected: PASS.

- [x] **Step 5: Checkpoint** — report; do not commit.

---

### Task 10: Conformance harness — tags, bucket tags, rename, parts

**Files:**
- Modify: `crates/tinio-util/src/...` (the `assert_conformance` suite — find the object/bucket/multipart contract modules and add the new checks; update the harness helpers that call `commit_object`/`copy_object`/`create_multipart_upload` for the new `tags` params)

**Interfaces:**
- Consumes: the contract (Task 2), `Tags` (Task 1).
- Produces: the harness proof that both backends meet the tags/rename/parts contract — this is what `tinio doctor` and the benches run.

- [x] **Step 1: Update the existing harness helpers** — every `commit_object` / `copy_object` / `create_multipart_upload` / `complete_multipart_upload` call in the harness gains `Tags::empty()` (or the scenario's tags) and `None` checksums (or the scenario's values), fixing the Task 2 compile break.

- [x] **Step 2: Add the new contract checks** (in the harness's object/bucket modules, following the existing check style — arrange a bucket + object, assert, clean up):

```rust
// Object tags: put → get round-trip, replace-all, delete, missing-object
// semantics (get/put NoSuchKey, delete succeeds).
// Write-path tags: commit_object carries tags into head/get metadata;
// copy_object carries tags to the destination; a multipart completion
// applies the create-time tags.
// Bucket tags: put/get/delete round-trip; missing bucket semantics.
// rename_object: moves the object (source gone, destination holds the
// content + metadata + OBJECT_PARTS rows); missing source → NoSuchKey.
// list_object_parts: empty for a plain put; the retained parts for a
// completed multipart upload (part number/size/per-part checksum match).
// OBJECT_PARTS lifecycle: overwriting a completed multipart object via
// commit/copy/complete leaves no stale parts; delete leaves none; copy
// never inherits the source's parts.
// Recorded checksums: a plain PUT with a tee carries its validated
// checksum into Info; completion carries the composite passed through
// the contract; copy carries the source's recorded value + kind.
```

- [x] **Step 3: Run the workspace tests**

Run: `cargo test --workspace`
Expected: PASS — this is the first full-green workspace run since Task 2.

- [x] **Step 4: Checkpoint** — report; do not commit.

---

### Task 11: Cucumber — tagging.feature, RenameObject scenarios, GetObjectAttributes scenarios

**Files:**
- Create: `crates/tinio-e2e/tests/features/tagging.feature`
- Modify: `crates/tinio-e2e/tests/features/conditions.feature` (append RenameObject scenarios)
- Create: `crates/tinio-e2e/tests/features/objects.feature` (GetObjectAttributes scenarios)
- Modify: `crates/tinio-e2e/tests/steps/objects.rs` (tagging steps; GetObjectAttributes steps) or a new `tagging.rs` step module (follow the module layout the suite already uses)
- Modify: `crates/tinio-e2e/tests/steps/mod.rs` (declare any new module)

**Interfaces:**
- Consumes: the raw-request steps (errors.rs + conditions.rs from Plan A), `config_from_tags` `@tagging-off` (Task 5), the multipart steps.
- Produces: the executable tagging/RenameObject/attributes surface.

- [x] **Step 1: Write `tagging.feature`**:

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (tagging)
Feature: Object and bucket tagging over real HTTP

  Scenario: Put/Get/Delete object tagging round-trip
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "PUT" request to "/data/a.txt?tagging" with body "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 200
    And the response body contains "<Key>env</Key>"
    When I send a "DELETE" request to "/data/a.txt?tagging"
    Then the response status is 204

  Scenario: PutObject with x-amz-tagging stores the tags
    Given I create bucket "data"
    When I send a "PUT" request to "/data/a.txt" with header "x-amz-tagging" "env=prod&team=core"
    Then the response status is 200
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response body contains "<Key>env</Key>"
    And the response body contains "<Key>team</Key>"

  Scenario: GetObject returns x-amz-tagging-count
    Given I create bucket "data"
    And I send a "PUT" request to "/data/a.txt" with header "x-amz-tagging" "a=1"
    When I send a "GET" request to "/data/a.txt"
    Then the response header "x-amz-tagging-count" is "1"

  Scenario: CopyObject with the default COPY directive carries tags
    Given I create bucket "data"
    And I send a "PUT" request to "/data/a.txt" with header "x-amz-tagging" "env=prod"
    When I send a "PUT" request to "/data/b.txt" with header "x-amz-copy-source" "/data/a.txt"
    Then the response status is 200
    When I send a "GET" request to "/data/b.txt?tagging"
    Then the response body contains "<Key>env</Key>"

  Scenario: CopyObject REPLACE overrides the source tags
    Given I create bucket "data"
    And I send a "PUT" request to "/data/a.txt" with header "x-amz-tagging" "env=prod"
    When I send a "PUT" request to "/data/b.txt" with header "x-amz-copy-source" "/data/a.txt" and header "x-amz-tagging-directive" "REPLACE" and header "x-amz-tagging" "env=dev"
    Then the response status is 200
    When I send a "GET" request to "/data/b.txt?tagging"
    Then the response body contains "<Value>dev</Value>"

  Scenario: Multipart completion carries the create-time tags
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin" with header "x-amz-tagging" "env=prod"
    And I upload part 1 with body "hello"
    When I complete the multipart upload with header "If-None-Match" "*"
    Then the response status is 200
    When I send a "GET" request to "/data/big.bin?tagging"
    Then the response body contains "<Key>env</Key>"

  Scenario: Malformed x-amz-tagging answers InvalidTag
    Given I create bucket "data"
    When I send a "PUT" request to "/data/a.txt" with header "x-amz-tagging" "k=v&broken"
    Then the response status is 400
    And the error code is "InvalidTag"

  @tagging-off
  Scenario: Disabled tagging answers NotImplemented
    Given I create bucket "data"
    When I send a "GET" request to "/data/a.txt?tagging"
    Then the response status is 501
```

(The `I start a multipart upload for {string} with header ...` step and the `the response body contains {string}` step do not exist yet — add them: the former extends the multipart.rs `start_upload` step with an optional header, the latter is a new assertion step in the tagging step module.)

- [x] **Step 2: Write the step additions** — in the tagging step module (or extend objects.rs): `the response body contains {string}` (assert on `world.last.body`), the multipart-start-with-header variant, and the bucket-tagging steps if the feature grows them (the bucket trio can ride the same raw-request steps: `PUT /data?tagging` with the Tagging XML body, `GET /data?tagging`, `DELETE /data?tagging` — reuse the raw-request-with-headers/body steps from Plan A's conditions.rs).

- [x] **Step 3: Write the RenameObject scenarios** — append to `conditions.feature`:

```gherkin
  Scenario: RenameObject moves the object with a source condition
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    And the response header "ETag" is stored
    When I send a "POST" request to "/data/a.txt?x-id=RenameObject" with header "x-amz-destination-key" "data/b.txt" and header "x-amz-source-if-match" "{etag}"
    Then the response status is 200
    And I send a "GET" request to "/data/b.txt"
    Then the response status is 200

  Scenario: RenameObject source mismatch answers 412
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "POST" request to "/data/a.txt?x-id=RenameObject" with header "x-amz-destination-key" "data/b.txt" and header "x-amz-source-if-match" "\"deadbeef\""
    Then the response status is 412

  Scenario: RenameObject overwrites an existing destination
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    And I upload "data/b.txt" with body "old"
    When I send a "POST" request to "/data/a.txt?x-id=RenameObject" with header "x-amz-destination-key" "data/b.txt"
    Then the response status is 200
    And I send a "GET" request to "/data/b.txt"
    Then the response body contains "hello"

  Scenario: RenameObject source equal destination answers 412
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "POST" request to "/data/a.txt?x-id=RenameObject" with header "x-amz-destination-key" "data/a.txt"
    Then the response status is 412
```

(⚠ The wire shape of s3s's RenameObject routing — the `?x-id=RenameObject` query is s3s's operation disambiguator, but confirm against how s3s routes RenameObject (the `x-id` convention s3s uses for non-standard ops) and the actual destination/source header names from `s3s-0.15.0/src/ops/generated.rs` before finalizing the feature.)

- [x] **Step 4: Write the GetObjectAttributes scenarios** — `objects.feature`:

```gherkin
# derived from specs/001-s3-local-server/contracts/s3-surface.md (object attributes)
Feature: GetObjectAttributes

  Scenario: Attributes of a plain object
    Given I create bucket "data"
    And I upload "data/a.txt" with body "hello"
    When I send a "GET" request to "/data/a.txt?attributes"
    Then the response status is 200
    And the response body contains "<ObjectSize>5</ObjectSize>"
    And the response body contains "<ETag>"

  Scenario: ObjectParts pagination truncates per max-parts
    Given I create bucket "data"
    And I start a multipart upload for "data/big.bin"
    And I upload part 1 with body "aaaa"
    And I upload part 2 with body "bbbb"
    And I upload part 3 with body "cccc"
    And I complete the multipart upload with header "If-None-Match" "*"
    When I send a "GET" request to "/data/big.bin?attributes" with header "x-amz-object-attributes" "ObjectParts" and header "x-amz-max-parts" "2"
    Then the response status is 200
    And the response body contains "<IsTruncated>true</IsTruncated>"
    And the response body contains "<NextPartNumberMarker>2</NextPartNumberMarker>"
    And the response body contains "<TotalPartsCount>3</TotalPartsCount>"
    # (client-specified max_parts=2 triggers truncation — no need to upload
    # >1000 parts in e2e)

  @checksum-on
  Scenario: GetObject echoes a recorded checksum
    Given I create bucket "data"
    And I send a "PUT" request to "/data/c.txt" with header "x-amz-checksum-crc32" "NhCmhg=="
    When I send a "GET" request to "/data/c.txt"
    Then the response status is 200
    And the response header "x-amz-checksum-crc32" is "NhCmhg=="
```

(⚠ The GetObjectAttributes wire shape — s3s routes it as `GET /key?attributes` with `x-amz-object-attributes` / `x-amz-max-parts` / `x-amz-part-number-marker` headers (the AWS wire form) — confirm from `s3s-0.15.0/src/ops/generated.rs` (the `get_object_attributes` handler's path/query parsing) before finalizing.)

- [x] **Step 5: Run the e2e suite**

Run: `cargo test -p tinio-e2e --test cucumber`
Expected: PASS.

- [x] **Step 6: Checkpoint** — report; do not commit.

---

### Task 12: Specs & docs — tagging, RenameObject, GetObjectAttributes

**Files:**
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (tagging contract: object + bucket, the capability toggle, RenameObject, GetObjectAttributes)
- Modify: `specs/001-s3-local-server/tasks.md` / `checklists/` (task entries + checklist items; new FR/SC IDs)
- Modify: `crates/tinio-e2e/tests/steps/mod.rs:45` (the "Capability toggles (spec §Tagging, grilling Q4)" comment — the spec section now exists; update the comment to point at it)
- Modify: `docs/superpowers/plans/2026-08-31-cucumber-bdd-migration-design.md`'s `tagging.feature` description (from "GetObjectTagging / DeleteObjects quiet mode" to the real surface) if that plan is still uncommitted

**Interfaces:**
- Consumes: the behavior locked by Tasks 1-11.
- Produces: the spec IDs the feature scenarios carry (apply the new `@FR-xxx`/`@SC-xxx` tags to the tagging/objects feature scenarios).

- [x] **Step 1: Extend `s3-surface.md`** — document: the tagging contract (object tags: get/put/delete, replace-all semantics, validation rules; write-path `x-amz-tagging` on put/copy/multipart; `x-amz-tagging-directive` COPY/REPLACE; `x-amz-tagging-count` on GET/HEAD; bucket tags; the `tagging` capability toggle — off ⇒ six ops NotImplemented, write headers accept-and-drop), RenameObject (source + destination conditionals, destination overwrite, same-key 412, copy gate), GetObjectAttributes (attribute subset, ObjectParts pagination + Checksum retention, GET/HEAD checksum echo), and the OBJECT_PARTS lifecycle (overwrite/delete cleanup, rename migration). Assign new FR/SC IDs following the file's numbering.

- [x] **Step 2: Add task/checklist entries** — `tasks.md` + `checklists/` mirroring Plan A Task 8's pattern.

- [x] **Step 3: Resolve the dangling reference** — update the `steps/mod.rs:45` comment to reference the now-real spec section.

- [x] **Step 4: Tag the feature scenarios** — apply the new spec IDs to `tagging.feature` / `objects.feature` / the RenameObject scenarios.

- [ ] **Step 5: Final verification** — `cargo test --workspace` (Windows + WSL2) and `cargo test -p tinio-e2e --test cucumber`; manual smoke: aws-cli `--tagging` put/copy/multipart round-trip against a dev server (optional, if aws-cli is available). Report all changes; do not commit.

---

## Self-Review Notes

- **Spec coverage**: object tagging end-to-end (Tasks 1-4, 6, 10-12), bucket tagging (Tasks 2-4, 7, 10-12), RenameObject (Tasks 2-4, 8, 11-12), GetObjectAttributes with part retention (Tasks 2-4, 9, 11-12), toggle (Task 5), cucumber + conformance (Tasks 10-11), docs (Task 12). The conditional-headers sections of the spec are owned by Plan A.
- **STATE_VERSION reverts 2→1** (user ruling 2026-09-02) — undo the multipart-era bump; the fs object/upload/bucket/parts row changes ship in Task 3 with no version bump (additive changes carry none); stale dev DBs (another version — refused on open; same version, older format — row-decode errors) are deleted by hand — dev-local state, accepted.
- **`Info.tags` always populated** (spec decision) — head/get/listing carry tags; GetObjectTagging and the count header read the same field; no dedicated tag fetch on read paths.
- **No per-key lock for tagging ops** (spec decision) — `put_object_tags`/`delete_object_tags` are last-writer-wins against concurrent writes.
- **Tagging ops take no lock; RenameObject takes two** (sorted) — the asymmetry is deliberate and documented in each op.
- **Checksum recording is write-time, not read-time** — the object row carries the checksum element (algorithm wire + base64 value + kind); the PUT tee validates under the `checksum` toggle, completion receives the composite from the interface (the response-echo value already computed server-side — grilling Q1b), copy carries the source's recorded value + kind, rename preserves; GetObjectAttributes and the GET/HEAD echo read `Info.checksum` with no read-path computation or kind derivation. Toggle-off PUT keeps today's accept-and-drop (nothing recorded).
- **Placeholder risk (flagged, not silent)**: RenameObject's wire shape and GetObjectAttributes' query shape in the cucumber features must still be confirmed against the s3s router during Tasks 8/11 — the features note the exact source to read (`s3s-0.15.0/src/ops/generated.rs`). The rename destination semantics are no longer a placeholder: AWS-verified and matching the companion spec's `check_missing` (missing + If-None-Match → proceed; If-Match on missing → 412).
- **Plan A is not yet executed** — `conditions.rs` carries only the base evaluator (`any`/`check_missing`/`to_whole_seconds`/`IfRange` absent); execute `2026-08-31-s3-conditionals-cleanup.md` first, per the dependency above.
