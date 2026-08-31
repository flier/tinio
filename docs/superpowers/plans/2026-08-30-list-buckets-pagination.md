# ListBuckets Pagination & Unified Page-Size Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give S3 `ListBuckets` pagination (2025-03 AWS semantics: `continuation-token` / `max-buckets` / `prefix`, `ContinuationToken` when truncated) and unify the listing page-size policy (`< 1` → `InvalidArgument`; ListBuckets/ListObjects clamped to operator-configurable caps, 0 = unlimited).

**Architecture:** Follow the ListObjects precedent end to end: pagination lives in the storage contract (`ListBucketsParams` / `BucketsListing`, page slicing via the shared `paginate_ordered` engine in both backends), the mapping layer validates/clamps/decodes tokens and translates `ListBucketsInput` ↔ `ListBucketsOutput`. The two caps flow `[s3]` config → `Capabilities` (two new `u32` fields) → `S3Backend`; `serve.rs` wires `Capabilities::from(s3)`.

**Tech Stack:** Rust workspace (`tinio-core`, `tinio-mem`, `tinio-fs`, `tinio-config`, `tinio-server`, `tinio-util`), s3s 0.15 DTOs (already parse/serialize the new query params and XML elements — zero s3s changes), redb, tokio, base64 0.22 (already in Cargo.lock at 0.22.1).

**Spec:** `docs/superpowers/specs/2026-08-29-list-buckets-pagination-design.md` (decisions locked by grilling 2026-08-29; review findings of 2026-08-30 incorporated).

## Global Constraints

- **Language**: English only in docs, comments, commits, PRs (CLAUDE.md).
- **No auto-commit**: CLAUDE.md forbids auto-commit/push/merge/rebase/stash — every "Commit" step of the writing-plans template is replaced by a **Report** step (leave changes in tree; the user decides when to commit). Never stage anything.
- **Tests**: `#[tokio::test]` / `async fn` directly (no `Runtime::block_on` wrappers); sync tests `#[test]`. Exception: deliberate runtime shape under test.
- **Wire surface** (s3s 0.15, verified in the registry source): `ListBucketsInput { bucket_region: Option<BucketRegion>, continuation_token: Option<Token>, max_buckets: Option<MaxBuckets>, prefix: Option<Prefix> }` — all `String`/`i32` aliases; `ListBucketsOutput { buckets: Option<Buckets>, continuation_token: Option<NextToken>, owner: Option<Owner>, prefix: Option<Prefix> }` where `Buckets = Vec<Bucket>`. **No `IsTruncated`.**
- **Contract stays permissive**: backends keep the engine's `max = 0` empty-page semantics (no `start_after` marker, untruncated) for direct contract calls; strictness (`< 1` rejection) is wire-level only, in `tinio-server`.
- **Cap 0 is "no clamp"** — never a literal `min(requested, 0)` (would turn the permissive contract's `max = 0` into an empty page under the default config). One home: `clamp_page_size`.
- **Echo rule**: V1/V2/ListParts/ListMultipartUploads response page-size elements echo the **effective** (post-clamp) value — today's behavior, kept.
- **Token**: URL-safe base64 (no padding) of the previous page's last bucket name; undecodable **or** non-UTF-8 → `InvalidArgument`; empty token → skips nothing.
- **Docs**: `specs/001-s3-local-server/contracts/s3-surface.md` and `contracts/config.md` are the prose homes (note: they live under `specs/001-s3-local-server/contracts/`, not `docs/`).
- Doctests and doc examples are code: the `ignore`d example in `tinio-core/src/storage/mod.rs` must not go stale.

---

### Task 1: Storage contract — `ListBucketsParams` / `BucketsListing` + trait signature (tinio-core) + harness call sites (tinio-util)

**Files:**
- Modify: `crates/tinio-core/src/storage/bucket.rs` (new types + trait method + tests)
- Modify: `crates/tinio-core/src/storage/mod.rs` (re-export)
- Modify: `crates/tinio-core/src/lib.rs:31-39` (root re-export, matching `ListObjectsParams`)
- Modify: `crates/tinio-util/src/testing.rs:204-218` (the two `list_buckets()` call sites in `conformance_buckets`)
- Test: `crates/tinio-core/src/storage/bucket.rs` (`#[cfg(test)] mod tests` — new module)

**Interfaces:**
- Produces (used by every later task):
  ```rust
  pub struct ListBucketsParams {
      pub prefix: String,          // only names starting with this are returned
      pub start_after: Option<String>, // resume marker, exclusive
      pub max_buckets: usize,      // page size; 0 = empty page at the contract level
  }
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct BucketsListing {
      pub buckets: Vec<Bucket>,    // name order (lexicographic)
      pub truncated: bool,
      pub next_start_after: Option<String>, // Some iff truncated
  }
  pub trait BucketOps { ... async fn list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, <Self as Storage>::Error> where Self: Storage; }
  ```
  Both types reachable as `tinio_core::ListBucketsParams` / `tinio_core::BucketsListing` (root re-export) and `tinio_core::storage::…`.

- [ ] **Step 1: Write the failing test** — in `crates/tinio-core/src/storage/bucket.rs`, append:

```rust
#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::bucket;

    #[test]
    fn buckets_types_construct() {
        // Mirrors the object listing's `listing_types_construct`: the
        // pagination types are plain data; `max_buckets = 0` is the
        // contract's empty-page request (strictness is wire-level).
        let params = ListBucketsParams {
            prefix: "data".into(),
            start_after: Some("data-a".into()),
            max_buckets: 100,
        };
        assert_eq!(params.max_buckets, 100);
        let listing = BucketsListing {
            buckets: vec![Bucket {
                name: bucket::name("data").unwrap(),
                creation_time: SystemTime::UNIX_EPOCH,
            }],
            truncated: true,
            next_start_after: Some("zeta".into()),
        };
        assert_eq!(listing.buckets.len(), 1);
        assert!(listing.truncated);
        assert_eq!(listing.next_start_after.as_deref(), Some("zeta"));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p tinio-core buckets_types_construct`
Expected: FAIL — `ListBucketsParams` / `BucketsListing` not found.

- [ ] **Step 3: Implement the contract change** — in `crates/tinio-core/src/storage/bucket.rs`, before the `BucketOps` trait, add:

```rust
/// Parameters of a [`BucketOps::list_buckets`] call — the S3 listing
/// semantics (prefix filtering, pagination).
///
/// The page size is permissive (like `ListObjectsParams`): `max_buckets =
/// 0` requests an empty page — the `< 1` rejection is a wire-level policy
/// of the S3 mapping layer.
///
/// # Examples
///
/// ```rust
/// use tinio_core::{ListBucketsParams, bucket};
///
/// let params = ListBucketsParams {
///     prefix: "data".into(),
///     start_after: Some("data-b".into()),
///     max_buckets: 100,
/// };
/// assert_eq!(params.max_buckets, 100);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListBucketsParams {
    /// Only buckets whose name starts with this prefix are returned.
    pub prefix: String,
    /// Resume the listing after this bucket name (exclusive).
    pub start_after: Option<String>,
    /// Maximum number of buckets per page (default 10_000 at the mapping).
    pub max_buckets: usize,
}

/// One page of a [`BucketOps::list_buckets`] listing.
///
/// # Examples
///
/// ```rust
/// use tinio_core::BucketsListing;
///
/// let page = BucketsListing {
///     buckets: vec![],
///     truncated: true,
///     next_start_after: Some("zeta".into()),
/// };
/// assert!(page.truncated);
/// assert_eq!(page.next_start_after.as_deref(), Some("zeta"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketsListing {
    /// Bucket metadata in name order (lexicographic, S3 semantics).
    pub buckets: Vec<Bucket>,
    /// Whether more results exist after this page.
    pub truncated: bool,
    /// Resume marker for the next page (`start_after` of the next call).
    pub next_start_after: Option<String>,
}
```

Replace the trait method (line 45-48):

```rust
    /// List buckets, in name order, per S3 listing semantics: only names
    /// starting with `params.prefix`, a page of at most
    /// `params.max_buckets` entries resuming after `params.start_after`
    /// (exclusive). `truncated` + `next_start_after` mark a page with
    /// more results (`max_buckets = 0` requests an empty, untruncated
    /// page — strictness is a mapping-layer policy).
    async fn list_buckets(
        &self,
        params: ListBucketsParams,
    ) -> Result<BucketsListing, <Self as Storage>::Error>
    where
        Self: Storage;
```

- [ ] **Step 4: Re-export the types** — `crates/tinio-core/src/storage/mod.rs` line 36:

```rust
pub use bucket::{BucketOps, BucketsListing, ListBucketsParams};
```

And `crates/tinio-core/src/lib.rs` root re-export (line 32 block) — add `BucketsListing, ListBucketsParams` next to `ListObjectsParams`:

```rust
    storage::{
        BodyStream, BucketOps, BucketsListing, ByteRange, GetObjectResult, ListBucketsParams,
        ListObjectsParams, ListPartsParams, ListUploadsParams, MultipartOps, ObjectListing,
        ObjectOps, PartsListing, PutObjectResult, Storage, UploadsListing, ...
```

- [ ] **Step 5: Update the `ignore`d doc example** — `crates/tinio-core/src/storage/mod.rs`, the `Storage` trait's `# Examples` block (lines 90-102):

```rust
/// use tinio_core::{bucket, ListBucketsParams};
/// use tinio_mem::MemoryStorage;
/// use tokio::runtime::Runtime;
///
/// let storage = MemoryStorage::new().unwrap();
/// let bucket = bucket::name("data").unwrap();
/// let buckets = Runtime::new()
///     .unwrap()
///     .block_on(async {
///         storage.create_bucket(&bucket).await.unwrap();
///         storage
///             .list_buckets(ListBucketsParams {
///                 prefix: String::new(),
///                 start_after: None,
///                 max_buckets: 10,
///             })
///             .await
///             .unwrap()
///             .buckets
///     });
/// assert_eq!(buckets.len(), 1);
```

- [ ] **Step 6: Update the harness call sites** — `crates/tinio-util/src/testing.rs`, `conformance_buckets` (lines 204-218). Add `ListBucketsParams` to the `crate::_core::{...}` import (line 18-20 block) and replace the two calls:

```rust
    // Start empty.
    let buckets = storage
        .list_buckets(ListBucketsParams {
            prefix: String::new(),
            start_after: None,
            max_buckets: 1000,
        })
        .await
        .unwrap();
    check(
        buckets.buckets.iter().all(|x| x.name != *b),
        "fresh bucket already listed",
    );

    // Create.
    storage.create_bucket(b).await.unwrap();
    let buckets = storage
        .list_buckets(ListBucketsParams {
            prefix: String::new(),
            start_after: None,
            max_buckets: 1000,
        })
        .await
        .unwrap();
    check(
        buckets.buckets.iter().any(|x| x.name == *b),
        "created bucket must be listed",
    );
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p tinio-core`
Expected: PASS (this builds tinio-core's dev-dependencies, including tinio-util — the harness now compiles against the new signature).

- [ ] **Step 8: Report** — summarize the contract change for the user; leave the changes in the tree.

---

### Task 2: Conformance harness — paginated-listing parity check (tinio-util)

**Files:**
- Modify: `crates/tinio-util/src/testing.rs` (`conformance_buckets` — add the parity check after "created bucket must be listed")

**Interfaces:**
- Consumes: `ListBucketsParams` / `BucketsListing` from Task 1 (root re-exports).
- Produces: the harness pins BOTH backends to identical page/marker semantics; Tasks 3/4 run it via `assert_conformance`.

- [ ] **Step 1: Add the parity check** — in `conformance_buckets`, right after the "created bucket must be listed" check (before the `// Head.` comment):

```rust
    // Paginated parity — pins every backend to identical page/marker
    // semantics (the per-backend unit tests alone would not catch a
    // drift): page over ALL buckets with a small page size and assert
    // the union of pages equals the full listing, in name order.
    let extra1 = bucket::name(&unique_bucket("conform")).unwrap();
    let extra2 = bucket::name(&unique_bucket("conform")).unwrap();
    storage.create_bucket(&extra1).await.unwrap();
    storage.create_bucket(&extra2).await.unwrap();
    let full = storage
        .list_buckets(ListBucketsParams {
            prefix: String::new(),
            start_after: None,
            max_buckets: 1000,
        })
        .await
        .unwrap();
    check(
        full.buckets.len() >= 3,
        "the full listing must hold the fixture buckets",
    );
    let mut paged = Vec::new();
    let mut start_after = None;
    loop {
        let page = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: start_after.clone(),
                max_buckets: 2,
            })
            .await
            .unwrap();
        check(
            page.buckets.len() <= 2,
            "a page must not exceed its max_buckets",
        );
        check(
            !page.truncated || page.next_start_after.is_some(),
            "a truncated page must carry a resume marker",
        );
        paged.extend(page.buckets.iter().map(|x| x.name.clone()));
        match page.next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    check(
        paged == full.buckets.iter().map(|x| x.name.clone()).collect::<Vec<_>>(),
        "the paged union must equal the full listing, in name order",
    );
    // Prefix filtering: the fixture bucket's full name matches exactly
    // that bucket (the counter in `unique_bucket` differs for the rest).
    let prefixed = storage
        .list_buckets(ListBucketsParams {
            prefix: b.as_ref().to_string(),
            start_after: None,
            max_buckets: 1000,
        })
        .await
        .unwrap();
    check(
        prefixed.buckets.len() == 1 && prefixed.buckets[0].name == *b,
        "the prefix filter must match exactly the fixture bucket",
    );
    check(
        !prefixed.truncated && prefixed.next_start_after.is_none(),
        "a complete prefixed page must carry no resume marker",
    );
    storage.delete_bucket(&extra1).await.unwrap();
    storage.delete_bucket(&extra2).await.unwrap();
```

- [ ] **Step 2: Build it**

Run: `cargo build -p tinio-util --features testing`
Expected: OK (the check runs when the backends in Tasks 3/4 run `assert_conformance`).

- [ ] **Step 3: Report** — summarize the harness addition; leave changes in the tree.

---

### Task 3: tinio-mem backend — paginated `list_buckets`

**Files:**
- Modify: `crates/tinio-mem/src/bucket.rs` (the `BucketOps` impl + tests)
- Test: same file

**Interfaces:**
- Consumes: `ListBucketsParams` / `BucketsListing` (Task 1), `paginate_ordered` (existing `tinio_core::storage` engine).
- Produces: `MemoryStorage::list_buckets(ListBucketsParams)` — the reference pagination behavior Tasks 4/6/8 assume.

- [ ] **Step 1: Update the existing test** — `list_buckets_is_lexicographic` (line 110-126):

```rust
    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for name in ["zeta", "alpha", "mu-1"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let names: Vec<_> = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            })
            .await
            .unwrap()
            .buckets
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, ["alpha", "mu-1", "zeta"]);
    }
```

- [ ] **Step 2: Write the failing pagination test** — append to the tests module of `crates/tinio-mem/src/bucket.rs`:

```rust
    #[tokio::test]
    async fn list_buckets_paginates_and_filters_prefix() {
        let storage = MemoryStorage::new().unwrap();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let page = |start_after: Option<&str>, max: usize| {
            storage.list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: start_after.map(str::to_string),
                max_buckets: max,
            })
        };
        // Page 1/2/3: two buckets each, the resume marker positions the
        // next page exactly; the final page carries no marker.
        let p1 = page(None, 2).await.unwrap();
        let names: for<'a> fn(&'a crate::_core::BucketsListing) -> Vec<&'a String> = |p| {
            p.buckets.iter().map(|b| b.name.as_ref()).collect()
        };
        assert_eq!(names(&p1), ["alpha-1", "alpha-2"]);
        assert!(p1.truncated);
        let p2 = page(p1.next_start_after.as_deref(), 2).await.unwrap();
        assert_eq!(names(&p2), ["beta-1", "beta-2"]);
        assert!(p2.truncated);
        let p3 = page(p2.next_start_after.as_deref(), 2).await.unwrap();
        assert_eq!(names(&p3), ["mid", "zeta"]);
        assert!(!p3.truncated);
        assert_eq!(p3.next_start_after, None);
        // Exact fill is not truncated.
        let exact = page(None, 6).await.unwrap();
        assert_eq!(exact.buckets.len(), 6);
        assert!(!exact.truncated);
        assert_eq!(exact.next_start_after, None);
        // Prefix filter (applied before the engine's marker skip).
        let prefixed = storage
            .list_buckets(ListBucketsParams {
                prefix: "alpha".into(),
                start_after: None,
                max_buckets: 10,
            })
            .await
            .unwrap();
        assert_eq!(names(&prefixed), ["alpha-1", "alpha-2"]);
        assert!(!prefixed.truncated);
        // Contract-level max_buckets = 0: an empty, untruncated page.
        let empty = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 0,
            })
            .await
            .unwrap();
        assert!(empty.buckets.is_empty());
        assert!(!empty.truncated);
        assert_eq!(empty.next_start_after, None);
    }
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p tinio-mem list_buckets_paginates_and_filters_prefix`
Expected: FAIL — the new signature is not implemented (compile error on `list_buckets`).

- [ ] **Step 4: Implement the backend** — replace `MemoryStorage::list_buckets` (lines 83-98). Add to the imports at the top of the file: `ListBucketsParams, BucketsListing, paginate_ordered` (from `crate::_core::storage` — extend the `_core::{...}` use block):

```rust
    async fn list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, Error> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        // BUCKETS is keyed by name, so iteration is already name order.
        // The prefix filter runs before the shared engine's marker skip;
        // the engine stops one probe entry past the page, so the table
        // is never drained for a small page. A mid-scan table error
        // fails the listing (the error-cell pattern of the object
        // listing, mem/src/object.rs).
        let mut scan_error = None;
        let items = buckets.iter()?.filter_map(|entry| match entry {
            Ok((name, created)) => {
                let name = name.value();
                if name.starts_with(&params.prefix) {
                    Some(Bucket {
                        name: name.into(),
                        creation_time: from_nanos(created.value()),
                    })
                } else {
                    None
                }
            }
            Err(err) => {
                if scan_error.is_none() {
                    scan_error = Some(err.into());
                }
                None
            }
        });
        let (page, truncated, next) = paginate_ordered(
            items,
            params.start_after.as_ref(),
            params.max_buckets,
            // One `String` order per scanned entry — the engine's owned
            // order; immaterial at bucket counts (the S3 account ceiling
            // is ~1,000 buckets).
            |b| b.name.to_string(),
        );
        if let Some(err) = scan_error {
            return Err(err);
        }
        Ok(BucketsListing {
            buckets: page,
            truncated,
            next_start_after: next,
        })
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p tinio-mem`
Expected: PASS (includes `assert_conformance` — the Task 2 parity check runs against the mem backend for the first time).

- [ ] **Step 6: Report** — summarize; leave changes in the tree.

---

### Task 4: tinio-fs backend — prefix-aware root scan + paginated `list_buckets`

**Files:**
- Modify: `crates/tinio-fs/src/backend/mod.rs` (`bucket_names` → prefix-aware)
- Modify: `crates/tinio-fs/src/backend/buckets.rs` (the `BucketOps` impl + tests)
- Modify: `crates/tinio-fs/src/scanner.rs:210` (`bucket_names("")`)
- Modify: `crates/tinio-fs/src/cleanup.rs:235,555` (`bucket_names("")`)
- Modify: `crates/tinio-fs/src/backend/tests.rs` (bucket_names tests + new prefix test)
- Test: `crates/tinio-fs/src/backend/buckets.rs`

**Interfaces:**
- Consumes: `ListBucketsParams` / `BucketsListing` / `paginate_ordered` (Task 1), `bucket::Store::{get_or_record}` (existing).
- Produces: `FsStorage::list_buckets(ListBucketsParams)` with **page-driven creation-time resolution** (only the page's buckets pay `get_or_record`; first-sight recording stays lazy).

- [ ] **Step 1: Update the existing fs tests to the new signature** — in `crates/tinio-fs/src/backend/buckets.rs` tests module:
  - `create_head_list_delete` (line 206): `storage.list_buckets().await.unwrap()` → wrap in `ListBucketsParams` (prefix `String::new()`, `start_after: None`, `max_buckets: 1000`) and read `.buckets` (two call sites: lines 206 and 216 in the `pre_existing…`/`tinio_and_root_files…` tests too).
  - `symlinked_bucket_follows_when_enabled_and_invisible_when_disabled` (lines 446, 466): same conversion, assert on `.buckets`.
  - `pre_existing_directories_are_buckets_with_lazy_creation_time` (line 518): same conversion.
  - `tinio_and_root_files_are_not_buckets` (line 537): same conversion, assert `.buckets.is_empty()`.
  - `many_buckets_list_sorted` (line 612): same conversion, iterate `.buckets`.
  - Add `ListBucketsParams` to the tests' `crate::_core::storage::{...}` import (line 180-182 block, where `ObjectOps` etc. come from).
  - In `crates/tinio-fs/src/backend/tests.rs`: all four `bucket_names()` call sites (lines 51, 189, 219, 266) → `bucket_names("")`.

- [ ] **Step 2: Write the failing pagination test** — append to the tests module of `crates/tinio-fs/src/backend/buckets.rs`:

```rust
    #[tokio::test]
    async fn list_buckets_paginates_and_filters_prefix() {
        let (_root, storage) = storage();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let names: for<'a> fn(&'a crate::_core::BucketsListing) -> Vec<&'a String> = |p| {
            p.buckets.iter().map(|b| b.name.as_ref()).collect()
        };
        let page = |start_after: Option<&str>, max: usize| {
            storage.list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: start_after.map(str::to_string),
                max_buckets: max,
            })
        };
        let p1 = page(None, 2).await.unwrap();
        assert_eq!(names(&p1), ["alpha-1", "alpha-2"]);
        assert!(p1.truncated);
        let p2 = page(p1.next_start_after.as_deref(), 2).await.unwrap();
        assert_eq!(names(&p2), ["beta-1", "beta-2"]);
        assert!(p2.truncated);
        let p3 = page(p2.next_start_after.as_deref(), 2).await.unwrap();
        assert_eq!(names(&p3), ["mid", "zeta"]);
        assert!(!p3.truncated);
        assert_eq!(p3.next_start_after, None);
        // Exact fill is not truncated.
        let exact = page(None, 6).await.unwrap();
        assert_eq!(exact.buckets.len(), 6);
        assert!(!exact.truncated);
        // Prefix filter.
        let prefixed = storage
            .list_buckets(ListBucketsParams {
                prefix: "alpha".into(),
                start_after: None,
                max_buckets: 10,
            })
            .await
            .unwrap();
        assert_eq!(names(&prefixed), ["alpha-1", "alpha-2"]);
        assert!(!prefixed.truncated);
        // Contract-level max_buckets = 0: an empty, untruncated page.
        let empty = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 0,
            })
            .await
            .unwrap();
        assert!(empty.buckets.is_empty());
        assert!(!empty.truncated);
        assert_eq!(empty.next_start_after, None);
    }
```

- [ ] **Step 3: Run it to verify it fails**

Run: `cargo test -p tinio-fs list_buckets_paginates_and_filters_prefix`
Expected: FAIL — the trait method is not implemented (compile error).

- [ ] **Step 4: Make `bucket_names` prefix-aware** — `crates/tinio-fs/src/backend/mod.rs`, replace the `bucket_names` method (lines 512-550) and its doc comment:

```rust
    /// Every bucket of the root matching `prefix`: top-level directories
    /// with valid names (the reserved `.tinio` state dir excluded), in
    /// name order. The scanner, cleanup, and `list_buckets` share this
    /// walk — one source of truth for what a bucket is. The prefix
    /// filter, UTF-8 validity, and name validity all run on the bare
    /// `file_name()` BEFORE any stat — only prefix-matching candidate
    /// names pay the `symlink_metadata` (the bucket-level analogue of
    /// the object walk's subtree pruning; `""` = no filter).
    ///
    /// A symlinked/junction bucket directory is a bucket only when
    /// `follow_symlinks` is enabled (it resolves to its target — the
    /// bucket *is* the target); with following disabled it is invisible.
    pub(crate) async fn bucket_names(&self, prefix: &str) -> Result<Vec<bucket::Name>, Error> {
        let mut out = Vec::new();
        let mut entries = fs::read_dir(self.root()).await?;
        while let Some(entry) = entries.next_entry().await? {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue; // non-UTF8 names cannot be bucket names
            };
            // No I/O before this point: a non-matching name is dropped
            // without a stat.
            if !name.starts_with(prefix) {
                continue;
            }
            let Ok(name) = bucket::name(name) else {
                continue; // invalid names (incl. `.tinio`) are not buckets
            };
            // lstat: a link entry is judged by its resolved target only
            // when following is enabled (a broken link is skipped).
            let lmeta = match fs::symlink_metadata(entry.path()).await {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            let mut is_dir = lmeta.is_dir();
            if self.follow_symlinks && fsutil::is_symlink_or_reparse(&lmeta) {
                is_dir = fs::metadata(entry.path())
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
            }
            if !is_dir {
                continue; // root-level files (and non-dir links) are not buckets
            }
            out.push(name);
        }
        out.sort_by(|a, b| a.as_ref().cmp(b.as_ref()));
        Ok(out)
    }
```

- [ ] **Step 5: Update the other callers** — `scanner.rs:210` `self.storage.bucket_names().await?` → `self.storage.bucket_names("").await?`; same for `cleanup.rs:235` and `cleanup.rs:555` (the scanner keeps the `""` form).

- [ ] **Step 6: Implement the backend** — in `crates/tinio-fs/src/backend/buckets.rs`, replace `FsStorage::list_buckets` (lines 140-162). Extend the `crate::_core::{bucket::{self, Bucket}, storage::{BucketOps, already_exists, no_such_bucket, not_empty}}` import with `ListBucketsParams, BucketsListing, paginate_ordered`:

```rust
    async fn list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, Error> {
        // The prefix-aware root scan runs the name checks and the prefix
        // filter on the bare file names first — only prefix-matching
        // candidates pay a stat (the bucket-level analogue of the object
        // walk's subtree pruning). The dirent sweep itself cannot seek
        // (`read_dir` order is unsorted), so the matching names are
        // collected (O(matches) memory), sorted, and handed to the
        // shared engine for the marker skip, page, and probe.
        let names = self.bucket_names(&params.prefix).await?;
        let (page, truncated, next) = paginate_ordered(
            names,
            params.start_after.as_ref(),
            params.max_buckets,
            |name| name.as_ref().to_string(),
        );
        // Creation times resolve only for the page's buckets — the
        // metadata-per-page analogue of the object listing's page-driven
        // ETag gate (P3). First-sight recording becomes page-driven: a
        // bucket not reached by pagination stays unrecorded until
        // listed; still lazy, no visible behavior change (same total
        // writes as the old load_all + per-miss record).
        let mut buckets = Vec::with_capacity(page.len());
        for name in page {
            let creation_time = self
                .bucket_store
                .get_or_record(&name, SystemTime::now())
                .await?;
            buckets.push(Bucket { name, creation_time });
        }
        Ok(BucketsListing {
            buckets,
            truncated,
            next_start_after: next,
        })
    }
```

- [ ] **Step 7: Add the prefix-filter test for the scan** — in `crates/tinio-fs/src/backend/tests.rs` (after `bucket_names_skips_root_level_files`):

```rust
#[tokio::test]
async fn bucket_names_filters_by_prefix() {
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(root.path(), fs_options()).unwrap();
    for name in ["alpha-1", "alpha-2", "beta-1"] {
        storage.create_bucket(&bucket::name(name).unwrap()).await.unwrap();
    }
    let names = storage
        .bucket_names("alpha")
        .await
        .unwrap()
        .into_iter()
        .map(|n| n.as_ref().to_string())
        .collect::<Vec<_>>();
    assert_eq!(names, ["alpha-1", "alpha-2"], "{names:?}");
    // The empty-prefix form keeps the scanner/cleanup behavior.
    assert_eq!(storage.bucket_names("").await.unwrap().len(), 3);
}
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `cargo test -p tinio-fs`
Expected: PASS (includes `assert_conformance` — the Task 2 parity check now runs against the fs backend too).

- [ ] **Step 9: Report** — summarize; leave changes in the tree.

---

### Task 5: tinio-config — `[s3]` page-size caps + `Capabilities::from`

**Files:**
- Modify: `crates/tinio-config/src/schema/s3.rs` (fields, serde defaults, `From` impl, tests)

**Interfaces:**
- Consumes: nothing new (the `Capabilities` struct already exists, flattened into `s3::Config`).
- Produces: `Capabilities { …, max_buckets: u32 (default 10_000), max_keys: u32 (default 0) }` and `impl From<&s3::Config> for Capabilities` — Task 6 reads the caps, Task 7 wires the `From`.

- [ ] **Step 1: Write the failing tests** — append to the tests module of `crates/tinio-config/src/schema/s3.rs`:

```rust
    #[test]
    fn max_buckets_and_max_keys_defaults() {
        // max_buckets = 10,000 (the AWS documented ceiling);
        // max_keys = 0 = unlimited, preserving current behavior.
        assert_eq!(Capabilities::default().max_buckets, 10_000);
        assert_eq!(Capabilities::default().max_keys, 0);
        let config = RootConfig::parse("version = 1\n[s3]").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 10_000);
        assert_eq!(caps.max_keys, 0);
    }

    #[test]
    fn max_buckets_and_max_keys_parse_and_accept_zero() {
        let config =
            RootConfig::parse("version = 1\n[s3]\nmax_buckets = 3\nmax_keys = 5").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 3);
        assert_eq!(caps.max_keys, 5);
        // 0 is legal and meaningful for both knobs ("no clamp").
        let config = RootConfig::parse("version = 1\n[s3]\nmax_buckets = 0\nmax_keys = 0").unwrap();
        let caps = config.s3.as_ref().unwrap().capabilities;
        assert_eq!(caps.max_buckets, 0);
        assert_eq!(caps.max_keys, 0);
    }

    #[test]
    fn capabilities_from_maps_config() {
        let config =
            RootConfig::parse("version = 1\n[s3]\nmultipart = false\nmax_buckets = 7\nmax_keys = 9")
                .unwrap();
        let caps = Capabilities::from(config.s3.as_ref().unwrap());
        assert!(!caps.multipart);
        assert!(caps.copy_object && caps.list_objects_v1 && caps.list_objects_v2 && caps.delete_objects);
        assert_eq!(caps.max_buckets, 7);
        assert_eq!(caps.max_keys, 9);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p tinio-config max_buckets`
Expected: FAIL — fields `max_buckets` / `max_keys` not found.

- [ ] **Step 3: Implement the knobs** — in `crates/tinio-config/src/schema/s3.rs`, add to `Capabilities` (after `delete_objects`):

```rust
    /// Cap on the ListBuckets page size: larger `max-buckets` requests
    /// are clamped to this value. 0 = unlimited (no clamp). Default
    /// 10,000 — the AWS documented maximum.
    #[serde(default = "max_buckets")]
    #[default = 10000]
    pub max_buckets: u32,

    /// Cap on the ListObjects page size: larger `max-keys` requests are
    /// clamped to this value. 0 = unlimited (no clamp). Default 0 —
    /// unlimited, preserving current behavior (AWS documents no max-keys
    /// cap).
    #[serde(default = "max_keys")]
    #[default = 0]
    pub max_keys: u32,
```

Add the serde default helpers next to `delete_objects()`:

```rust
fn max_buckets() -> u32 {
    Capabilities::default().max_buckets
}

fn max_keys() -> u32 {
    Capabilities::default().max_keys
}
```

Add the conversion (after the `delete_objects` helper):

```rust
impl From<&Config> for Capabilities {
    fn from(config: &Config) -> Self {
        config.capabilities
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-config`
Expected: PASS.

- [ ] **Step 5: Report** — summarize; leave changes in the tree.

---

### Task 6: tinio-server mapping — paginated `op_list_buckets` + unified page-size policy

**Files:**
- Modify: `Cargo.toml` (workspace) + `crates/tinio-server/Cargo.toml` (`base64 = "0.22"` direct dep)
- Modify: `crates/tinio-server/src/backend/mod.rs` (`clamp_page_size` helper + unit test)
- Modify: `crates/tinio-server/src/backend/buckets.rs` (`op_list_buckets` + tests)
- Modify: `crates/tinio-server/src/backend/listing.rs` (`list_page` page-size policy + tests)
- Modify: `crates/tinio-server/src/backend/multipart.rs` (`max-parts` / `max-uploads` policy + tests)

**Interfaces:**
- Consumes: `ListBucketsParams` / `BucketsListing` (Task 1), `Capabilities::{max_buckets, max_keys}` (Task 5), `paginate_ordered` semantics, `s3_error!` (existing), `base64` 0.22.
- Produces: the wire behavior the e2e (Task 8) asserts: `max-buckets`/`max-keys`/`max-parts`/`max-uploads` `< 1` → `InvalidArgument`; ListBuckets clamped to `caps.max_buckets` (default 10,000), ListObjects to `caps.max_keys` (default 0 = no clamp); `ContinuationToken` iff truncated; `Prefix` echoed iff requested.

- [ ] **Step 1: Add the base64 dependency** — workspace `Cargo.toml` `[workspace.dependencies]` (alphabetical, after `base64` is missing — insert before `assert_cmd`? No: alphabetical order puts `base64` first — insert at the top):

```toml
base64 = "0.22"
```

And `crates/tinio-server/Cargo.toml` `[dependencies]` (alphabetical, after `async-trait`):

```toml
base64.workspace = true
```

Run: `cargo build -p tinio-server` — Expected: OK (0.22.1 already in the lock — no new download).

- [ ] **Step 2: Write the failing mapping tests** — in `crates/tinio-server/src/backend/buckets.rs` tests module. Add the helper next to `backend()`:

```rust
    fn backend_with(caps: Capabilities) -> S3Backend<MemoryStorage> {
        S3Backend::new(MemoryStorage::new().unwrap(), caps)
    }

    /// The URL-safe no-pad base64 of a bucket name — the continuation
    /// token a client would send back.
    fn token(name: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(name.as_bytes())
    }
```

The test module imports need `Capabilities` (from `crate::backend` — already re-exported at `crate::backend::Capabilities`) and `s3_error` is not needed in tests. Append the tests:

```rust
    #[tokio::test]
    async fn list_buckets_paginates_and_resumes() {
        let backend = backend();
        let storage = backend.storage();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let names = |out: &dto::ListBucketsOutput| {
            out.buckets
                .as_ref()
                .unwrap()
                .iter()
                .filter_map(|b| b.name.clone())
                .collect::<Vec<_>>()
        };
        let page1 = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page1), ["alpha-1", "alpha-2"]);
        assert_eq!(
            page1.prefix, None,
            "no prefix sent, none echoed"
        );
        let t1 = page1.continuation_token.clone().unwrap();

        let page2 = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(t1),
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page2), ["beta-1", "beta-2"]);
        let t2 = page2.continuation_token.unwrap();

        let page3 = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(t2),
                max_buckets: Some(2),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(names(&page3), ["mid", "zeta"]);
        assert!(
            page3.continuation_token.is_none(),
            "the final page must carry no continuation token"
        );

        // Token exhaustion: a stale-but-decodable token past the end
        // yields an empty page (a plain start_after marker — no error).
        let exhausted = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(token("zzz")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert!(names(&exhausted).is_empty());
        assert!(exhausted.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_prefix_filters_and_echoes() {
        let backend = backend();
        let storage = backend.storage();
        for name in ["alpha-1", "alpha-2", "beta-1"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                prefix: Some("alpha".into()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha-1", "alpha-2"]);
        assert_eq!(
            out.prefix.as_deref(),
            Some("alpha"),
            "the prefix is echoed when the client sent one"
        );
    }

    #[tokio::test]
    async fn list_buckets_rejects_page_size_below_one() {
        let backend = backend();
        for max in [0, -1] {
            let err = backend
                .list_buckets(s3_request(dto::ListBucketsInput {
                    max_buckets: Some(max),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidArgument", "max_buckets = {max}");
        }
    }

    #[tokio::test]
    async fn list_buckets_clamps_to_the_configured_cap() {
        let backend = backend_with(Capabilities {
            max_buckets: 3,
            ..Default::default()
        });
        let storage = backend.storage();
        for name in ["zeta", "alpha-1", "alpha-2", "mid", "beta-1", "beta-2"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        // A max-buckets = 10 request clamps to the cap (3), truncated.
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                max_buckets: Some(10),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 3);
        assert!(out.continuation_token.is_some());
        // The no-parameter default (10,000) clamps to the cap too.
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput::default()))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 3);
        assert!(out.continuation_token.is_some());
    }

    #[tokio::test]
    async fn list_buckets_huge_request_clamps_silently_under_the_default_cap() {
        // The DEFAULT caps (max_buckets = 10,000): a 50,000 request
        // clamps to the AWS ceiling — silently, never an error, and
        // never an empty page.
        let backend = backend();
        let storage = backend.storage();
        storage
            .create_bucket(&bucket::name("data").unwrap())
            .await
            .unwrap();
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                max_buckets: Some(50_000),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["data"]);
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_default_page_size_is_ten_thousand() {
        // The AWS-documented default (10,000) applies when no
        // max-buckets is sent: 10,001 buckets yield a truncated page of
        // 10,000 plus a continuation token.
        let backend = backend();
        let storage = backend.storage();
        for i in 0..10_001 {
            storage
                .create_bucket(&bucket::name(&format!("b-{i}")).unwrap())
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput::default()))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 10_000);
        assert!(out.continuation_token.is_some());
        // The token resumes exactly onto the remaining bucket.
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: out.continuation_token,
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.buckets.as_ref().unwrap().len(), 1);
        assert!(out.continuation_token.is_none());
    }

    #[tokio::test]
    async fn list_buckets_rejects_bad_tokens() {
        let backend = backend();
        // Undecodable base64.
        let err = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some("!!!not-base64!!!".into()),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
        // Base64 of non-UTF-8 bytes.
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0xFF]);
        let err = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(raw),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    #[tokio::test]
    async fn list_buckets_empty_token_is_a_noop() {
        // The empty token decodes to the empty marker — it skips nothing.
        let backend = backend();
        let storage = backend.storage();
        for name in ["alpha", "beta"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(String::new()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["alpha", "beta"]);
        // A stale-but-decodable token resumes like a start_after marker.
        let out = backend
            .list_buckets(s3_request(dto::ListBucketsInput {
                continuation_token: Some(token("alpha")),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        let names: Vec<String> = out
            .buckets
            .unwrap()
            .into_iter()
            .filter_map(|b| b.name)
            .collect();
        assert_eq!(names, ["beta"]);
    }
```

- [ ] **Step 3: Write the failing page-size-policy tests** — `crates/tinio-server/src/backend/listing.rs` tests module. Add `Capabilities` to the `backend::{…}` import. Append:

```rust
    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_rejects_max_keys_below_one() {
        let (backend, b) = setup().await;
        for max_keys in [0, -1] {
            let err = backend
                .list_objects(s3_request(dto::ListObjectsInput {
                    bucket: b.clone(),
                    max_keys: Some(max_keys),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidArgument", "max-keys = {max_keys}");
        }
    }

    #[cfg(feature = "list-v2")]
    #[tokio::test]
    async fn v2_rejects_max_keys_below_one() {
        let (backend, b) = setup().await;
        let err = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: b,
                max_keys: Some(0),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code().as_str(), "InvalidArgument");
    }

    #[cfg(feature = "list-v1")]
    #[tokio::test]
    async fn v1_echoes_the_effective_page_size_after_a_clamp() {
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                max_keys: 2,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        for key in ["a.txt", "b.txt", "c.txt"] {
            storage
                .put_object(&b, &object::key(key).unwrap(), body(key))
                .await
                .unwrap();
        }
        let out = backend
            .list_objects(s3_request(dto::ListObjectsInput {
                bucket: "data".into(),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(
            out.max_keys,
            Some(2),
            "the response echoes the effective (clamped) page size"
        );
        assert_eq!(out.is_truncated, Some(true));
        assert_eq!(out.contents.as_ref().unwrap().len(), 2);
    }

    #[cfg(feature = "list-v2")]
    #[tokio::test]
    async fn v2_echoes_the_effective_page_size_after_a_clamp() {
        let backend = S3Backend::new(
            MemoryStorage::new().unwrap(),
            Capabilities {
                max_keys: 2,
                ..Default::default()
            },
        );
        let storage = backend.storage();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        for key in ["a.txt", "b.txt", "c.txt"] {
            storage
                .put_object(&b, &object::key(key).unwrap(), body(key))
                .await
                .unwrap();
        }
        let out = backend
            .list_objects_v2(s3_request(dto::ListObjectsV2Input {
                bucket: "data".into(),
                max_keys: Some(1000),
                ..Default::default()
            }))
            .await
            .unwrap()
            .output;
        assert_eq!(out.max_keys, Some(2));
        assert_eq!(out.is_truncated, Some(true));
        assert_eq!(out.key_count, Some(2));
    }
```

And `crates/tinio-server/src/backend/multipart.rs` tests module — append:

```rust
    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_parts_rejects_max_parts_below_one() {
        // The rejection is wire-level: it fires before any storage call,
        // so no upload is needed.
        let (backend, b) = setup().await;
        for max_parts in [0, -1] {
            let err = backend
                .list_parts(s3_request(dto::ListPartsInput {
                    bucket: b.clone(),
                    key: "k".into(),
                    upload_id: "u".into(),
                    max_parts: Some(max_parts),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidArgument", "max-parts = {max_parts}");
        }
    }

    #[cfg(feature = "multipart")]
    #[tokio::test]
    async fn list_multipart_uploads_rejects_max_uploads_below_one() {
        let (backend, b) = setup().await;
        for max_uploads in [0, -1] {
            let err = backend
                .list_multipart_uploads(s3_request(dto::ListMultipartUploadsInput {
                    bucket: b.clone(),
                    max_uploads: Some(max_uploads),
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code().as_str(), "InvalidArgument", "max-uploads = {max_uploads}");
        }
    }
```

- [ ] **Step 4: Run the new tests to verify they fail**

Run: `cargo test -p tinio-server list_buckets_` (and the four policy tests by name)
Expected: FAIL — `op_list_buckets` still ignores the inputs; no `InvalidArgument`; no clamp.

- [ ] **Step 5: Add the clamp helper** — `crates/tinio-server/src/backend/mod.rs`, after `normalize_delimiter`:

```rust
/// Clamp a requested page size to the configured cap. `cap = 0` means
/// "no clamp" — a literal `min(requested, 0)` would turn the permissive
/// contract's `max = 0` empty-page semantics on for every uncapped
/// listing (the default `[s3] max_keys` config). One home for the
/// boundary rule, shared by the ListBuckets and ListObjects mappings.
pub(crate) fn clamp_page_size(requested: usize, cap: u32) -> usize {
    if cap == 0 {
        requested
    } else {
        requested.min(cap as usize)
    }
}
```

And its unit test in the `mod tests` block:

```rust
    #[test]
    fn clamp_page_size_zero_cap_is_no_clamp() {
        assert_eq!(clamp_page_size(5, 0), 5);
        assert_eq!(clamp_page_size(10_000, 0), 10_000);
        assert_eq!(clamp_page_size(3, 10_000), 3);
        assert_eq!(clamp_page_size(50_000, 10_000), 10_000);
    }
```

- [ ] **Step 6: Implement `op_list_buckets`** — `crates/tinio-server/src/backend/buckets.rs`. Update the imports: add `s3_error` to the `s3s::{…}` use; add `ListBucketsParams` to the `_core::storage::{…}` use (the file imports `Storage` from there); add `clamp_page_size` to the `backend::{S3Backend, map_backend_error}` use. Add the constant at the top of the file:

```rust
/// The ListBuckets default page size when `max-buckets` is absent — the
/// AWS documented default (2025-03 API).
const DEFAULT_MAX_BUCKETS: i32 = 10_000;
```

Replace `op_list_buckets` (lines 58-79):

```rust
    pub(crate) async fn op_list_buckets(
        &self,
        req: S3Request<dto::ListBucketsInput>,
    ) -> S3Result<S3Response<dto::ListBucketsOutput>> {
        let dto::ListBucketsInput {
            bucket_region: _,
            continuation_token,
            max_buckets,
            prefix,
        } = req.input;
        // AWS documents `max-buckets` as 1..=10,000. The `< 1` rejection
        // is a wire-level policy (the contract keeps the engine's
        // `max = 0` empty-page semantics for direct calls).
        if let Some(max) = max_buckets
            && max < 1
        {
            return Err(s3_error!(InvalidArgument, "max-buckets must be at least 1"));
        }
        // The continuation token is the URL-safe no-pad base64 of the
        // previous page's last bucket name — opaque to clients (AWS:
        // "obfuscated and is not a real bucket"), no server-side token
        // state. Bad base64 AND non-UTF-8 payloads answer
        // InvalidArgument; the empty token decodes to the empty marker,
        // which skips nothing.
        let start_after = match continuation_token {
            Some(token) => {
                let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .decode(token.as_bytes())
                    .map_err(|_| s3_error!(InvalidArgument, "invalid continuation token"))?;
                let name = String::from_utf8(bytes)
                    .map_err(|_| s3_error!(InvalidArgument, "invalid continuation token"))?;
                Some(name)
            }
            None => None,
        };
        // The configured cap clamps the requested page size — and the
        // default (a cap of 5 clamps the no-parameter request to 5).
        let requested = max_buckets.unwrap_or(DEFAULT_MAX_BUCKETS) as usize;
        let effective = clamp_page_size(requested, self.caps.max_buckets);
        let listing = self
            .storage
            .list_buckets(ListBucketsParams {
                prefix: prefix.clone().unwrap_or_default(),
                start_after,
                max_buckets: effective,
            })
            .await
            .map_err(map_backend_error)?;
        let buckets = listing
            .buckets
            .into_iter()
            .map(|b| dto::Bucket {
                name: Some(b.name.to_string()),
                creation_date: Some(Self::last_modified(b.creation_time)),
                ..Default::default()
            })
            .collect();
        // `ContinuationToken` presence is the truncation signal (s3s
        // 0.15 has no `IsTruncated` on this wire); the engine returns
        // the resume marker only when truncated. `Prefix` is echoed iff
        // the client sent one (AWS).
        Ok(S3Response::new(dto::ListBucketsOutput {
            buckets: Some(buckets),
            continuation_token: listing
                .next_start_after
                .map(|name| {
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(name.as_bytes())
                }),
            prefix,
            ..Default::default()
        }))
    }
```

- [ ] **Step 7: Implement the page-size policy in `list_page`** — `crates/tinio-server/src/backend/listing.rs`. Add `s3_error` to the `s3s::{…}` import and `clamp_page_size` to the `backend::{…}` import. Replace line 44:

```rust
        let requested = max_keys.unwrap_or(1000);
        // Unified page-size policy: a page size < 1 is rejected before
        // any storage call (AWS documents no max-keys range; the
        // strictness is deliberate). The configured `[s3] max_keys` cap
        // clamps the requested size (0 = no clamp); the echoed
        // `MaxKeys` element carries the effective value.
        if requested < 1 {
            return Err(s3_error!(InvalidArgument, "max-keys must be at least 1"));
        }
        let max_keys = clamp_page_size(requested as usize, self.caps.max_keys);
```

- [ ] **Step 8: Implement the policy in the multipart listings** — `crates/tinio-server/src/backend/multipart.rs`. Replace line 284:

```rust
        let max_parts = req.input.max_parts.unwrap_or(1000);
        // Unified page-size policy: < 1 is rejected before any storage
        // call. Multipart listings stay uncapped (AWS documents no cap).
        if max_parts < 1 {
            return Err(s3_error!(InvalidArgument, "max-parts must be at least 1"));
        }
        let max_parts = max_parts as usize;
```

And line 338:

```rust
        let max_uploads = req.input.max_uploads.unwrap_or(1000);
        if max_uploads < 1 {
            return Err(s3_error!(InvalidArgument, "max-uploads must be at least 1"));
        }
        let max_uploads = max_uploads as usize;
```

(The `max_parts`/`max_uploads` response elements keep echoing the effective value — the variables are already the validated integers, `as i32` in the outputs unchanged.)

- [ ] **Step 9: Run the tests to verify they pass**

Run: `cargo test -p tinio-server`
Expected: PASS. (The `metrics.rs` test at line 727 calls `list_buckets` with a default DTO input — unaffected: default page size 10,000 on an empty backend returns an empty page.)

- [ ] **Step 10: Report** — summarize; leave changes in the tree.

---

### Task 7: Server startup wiring — `[s3]` reaches the plane (serve.rs)

**Files:**
- Modify: `crates/tinio-server/examples/serve.rs`

**Interfaces:**
- Consumes: `Capabilities::from(&s3::Config)` (Task 5).
- Produces: the running serve binary honors `[s3] max_buckets` — the hard prerequisite of the Task 8 e2e.

- [ ] **Step 1: Wire the caps** — `crates/tinio-server/examples/serve.rs`, replace the plane construction (line 210-211). Right after the scanner/sweep setup (or immediately before `let plane = …`), compute the caps:

```rust
    // `[s3]` capability toggles and page-size caps flow into the plane's
    // `Capabilities` (FR-021); without an `[s3]` section the defaults
    // apply. (The bool toggles used to be dropped here — the plane was
    // built with `Capabilities::default()` even when a config set them.)
    let caps = match config.as_ref().and_then(|c| c.s3.as_ref()) {
        Some(s3) => Capabilities::from(s3),
        None => Capabilities::default(),
    };
```

And replace line 211:

```rust
    let plane =
        DataPlane::new_with_auth(storage, caps, "minioadmin", "minioadmin")
```

- [ ] **Step 2: Build it**

Run: `cargo build -p tinio-server --example serve`
Expected: OK.

- [ ] **Step 3: Report** — summarize; leave changes in the tree.

---

### Task 8: e2e interop — boto3 `list_buckets` paginator over a capped server

**Files:**
- Modify: `e2e/interop/lib.sh` (`start_server` gains an optional `--config` passthrough)
- Modify: `e2e/interop/boto3.sh` (second scenario section)
- Modify: `crates/tinio-server/tests/e2e/mod.rs` (`Server::start_with_config`)
- Modify: `crates/tinio-server/tests/boto3.rs` (new ignored test)
- Create: `crates/tinio-server/tests/boto3_buckets_pagination.py`

**Interfaces:**
- Consumes: the serve wiring (Task 7) — a config `[s3] max_buckets = 3` must reach the running plane.
- Produces: the boto3 paginator proof — every bucket seen exactly once across pages.

- [ ] **Step 1: Extend `start_server`** — `e2e/interop/lib.sh`, replace the function (lines 80-109):

```bash
# Start the server on an ephemeral port and echo the endpoint (after
# polling the log for the readiness marker). The optional third argument
# sets TINIO_SCANNER; any value other than 0/1 falls back to the config
# gate, so passing "" is the unset behavior. The optional fourth argument
# is a config file passed through `--config` ("" = none).
start_server() {
    local root="$1" log="$2" scanner="${3:-}" config="${4:-}"
    ensure_server_binary
    local args=("$SERVER_BIN" "$root" --port 0)
    if [[ -n "$config" ]]; then
        args+=(--config "$config")
    fi
    TINIO_SCANNER="$scanner" "${args[@]}" > "$log" 2>&1 &
    SERVER_PID=$!
    local endpoint=""
    for _ in $(seq 1 50); do
        if grep -q "listening on" "$log" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    # `grep -oE` + `cut` instead of `grep -oP` (BSD grep on macOS lacks -P).
    endpoint="$(grep -oE 'listening on [0-9.:]+' "$log" | head -1 | cut -d' ' -f3)"
    if [[ -z "$endpoint" ]]; then
        echo "server did not start:" >&2
        cat "$log" >&2
        # `return`, not `exit`: this function runs inside a command
        # substitution — an `exit` here would only exit the subshell (its
        # propagation to the parent depends on bash's set -e quirks). The
        # caller's `|| exit 1` after the substitution is the explicit,
        # version-independent failure path.
        return 1
    fi
    echo "$endpoint"
}
```

- [ ] **Step 2: Extend `boto3.sh`** — after the main journey heredoc (before the final `PY` line of the first python block), keep the journey as is; then append a second scenario after the `PY` terminator:

```bash
# ListBuckets pagination: the `[s3] max_buckets = 3` cap forces a small
# page size below the bucket count — the serve-wiring proof that a
# configured cap reaches the running plane. Paginate with the boto3
# list_buckets paginator and assert every bucket is seen exactly once.
cat > "$SCRATCH/paginate.toml" <<'CFG'
version = 1

[s3]
max_buckets = 3
CFG
PAGINATE_ENDPOINT="$(start_server "$SCRATCH/root-paginate" "$SCRATCH/paginate.log" "" "$SCRATCH/paginate.toml")" || exit 1

"$BOTO3_PYTHON" - "$PAGINATE_ENDPOINT" <<'PY'
import sys

import boto3
from botocore.client import Config

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

expected = [f"pag-bucket-{i}" for i in range(7)]
for name in expected:
    s3.create_bucket(Bucket=name)

paginator = s3.get_paginator("list_buckets")
seen = []
for page in paginator.paginate():
    seen.extend(b["Name"] for b in page["Buckets"])
assert sorted(seen) == sorted(expected), f"pagination lost or duplicated a bucket: {seen}"

print("BUCKET PAGINATION OK")
PY
```

- [ ] **Step 3: Add the config variant to the Rust e2e harness** — `crates/tinio-server/tests/e2e/mod.rs`. Extend `start_inner` with a `config: Option<&Path>` parameter and add the public constructor:

```rust
    /// Serve `root` (caller keeps it) with an additional
    /// `--config <path>` — the serve-wiring proof: a configured
    /// `[s3] max_buckets` must reach the running plane.
    pub fn start_with_config(root: &Path, config: &Path) -> Self {
        Self::start_inner(root, None, None, Some(config))
    }
```

and in `start_inner` (line 84):

```rust
    fn start_inner(
        root: &Path,
        scanner: Option<bool>,
        dir: Option<tempfile::TempDir>,
        config: Option<&Path>,
    ) -> Self {
        let mut cmd = Command::new(serve_bin());
        cmd.arg(root).arg("--port").arg("0");
        if let Some(config) = config {
            cmd.arg("--config").arg(config);
        }
        if let Some(s) = scanner {
            cmd.env("TINIO_SCANNER", if s { "1" } else { "0" });
        }
        ...
```

Update the two existing callers (`start` line 74 and `start_at` line 82) to pass `None` as the fourth argument.

- [ ] **Step 4: Create the python script** — `crates/tinio-server/tests/boto3_buckets_pagination.py`:

```python
"""ListBuckets pagination (2025-03 semantics) against a serve endpoint
whose `[s3] max_buckets = 3` cap forces a small page size: create more
buckets than one page, paginate with the boto3 list_buckets paginator,
assert every bucket is seen exactly once. Driven by the Rust test
tests/boto3.rs: `python3 boto3_buckets_pagination.py <endpoint>`.
Best-effort client per FR-025 (targeted/manual, NOT CI-gated).
"""

import sys

import boto3
from botocore.client import Config

endpoint = sys.argv[1]
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{endpoint}",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

expected = [f"pag-bucket-{i}" for i in range(7)]
for name in expected:
    s3.create_bucket(Bucket=name)

paginator = s3.get_paginator("list_buckets")
seen = []
for page in paginator.paginate():
    seen.extend(b["Name"] for b in page["Buckets"])
assert sorted(seen) == sorted(expected), f"pagination lost or duplicated a bucket: {seen}"

print("BUCKET PAGINATION OK")
```

- [ ] **Step 5: Add the Rust test** — `crates/tinio-server/tests/boto3.rs`, after `journey`:

```rust
#[test]
#[ignore = "requires the tinio-e2e venv with boto3 (see TROUBLESHOOTING.md §2)"]
fn list_buckets_pagination() {
    let python = e2e::boto3_python();
    assert!(
        python.exists(),
        "boto3 venv python not found at {} — create it and install boto3:\n\
         python3 -m venv <target>/tinio-e2e-venv && <venv>/pip install boto3\n\
         (or point TINIO_BOTO3_PYTHON at your own venv python; \
         see e2e/interop/TROUBLESHOOTING.md §2)",
        python.display()
    );
    // The `[s3] max_buckets = 3` cap forces pagination below the
    // account's bucket count — the serve-wiring proof that a configured
    // cap reaches the running plane's `Capabilities`.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "version = 1\n\n[s3]\nmax_buckets = 3\n").unwrap();
    let server = e2e::Server::start_with_config(&root, &config);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/boto3_buckets_pagination.py");
    e2e::boto3(server.endpoint(), &script)
        .assert()
        .success()
        .stdout(contains("BUCKET PAGINATION OK"));
}
```

- [ ] **Step 6: Build the tests**

Run: `cargo test -p tinio-server --test boto3 --no-run`
Expected: OK (the new test compiles; it is `#[ignore]`d, so the plain run skips it).

- [ ] **Step 7: Run the e2e (manual, venv required)**

Run: `cargo test -p tinio-server --test boto3 -- --ignored list_buckets_pagination` (and/or `bash e2e/interop/boto3.sh`)
Expected: `BUCKET PAGINATION OK` (both ports). If the venv is missing, report the provisioning step (`python3 -m venv target/tinio-e2e-venv && …/pip install boto3`) and leave the test as the manual proof.

- [ ] **Step 8: Report** — summarize; leave changes in the tree.

---

### Task 9: Docs — `s3-surface.md` ops table + behavior note; `contracts/config.md` `[s3]` sample + rules

**Files:**
- Modify: `specs/001-s3-local-server/contracts/s3-surface.md` (ops table line 11 + behavior notes)
- Modify: `specs/001-s3-local-server/contracts/config.md` (`[s3]` sample + validation-rules list)

- [ ] **Step 1: Update the ops table** — `specs/001-s3-local-server/contracts/s3-surface.md`, the Buckets row:

```markdown
| Buckets | `CreateBucket` (FR-002, FR-012), `DeleteBucket` (only empty → else `BucketNotEmpty`), `HeadBucket`, `ListBuckets` (2025-03 pagination semantics: `continuation-token` / `max-buckets` / `prefix`; a `ContinuationToken` is returned when more buckets remain; CreationDate from the `BUCKETS` table of `meta.redb`), `GetBucketLocation` (returns `us-east-1`) |
```

- [ ] **Step 2: Add the behavior note** — after the "Capability toggles" bullet (line 40) or next to the Listing behavior notes, add:

```markdown
- **Listing page size**: every listing page size < 1 (`max-buckets`, `max-keys`, `max-parts`, `max-uploads`) answers `InvalidArgument`. `[s3] max_buckets` (default 10,000; 0 = unlimited) clamps ListBuckets page sizes — including the no-parameter default; `[s3] max_keys` (default 0 = unlimited) clamps ListObjects page sizes; multipart listings are uncapped. V1/V2/ListParts/ListMultipartUploads responses echo the effective (clamped) page size.
```

- [ ] **Step 3: Update the `[s3]` sample** — `specs/001-s3-local-server/contracts/config.md`, after the `delete_objects = true` line:

```toml
max_buckets = 10000   # ListBuckets page-size cap (0 = unlimited; larger max-buckets requests are clamped — the AWS documented ceiling)
max_keys = 0          # ListObjects page-size cap (0 = unlimited, the default — preserves current behavior)
```

- [ ] **Step 4: Add the validation-rules entry** — in the same file's `## Validation rules` list, after the `[s3]` capability-groups bullet (line 120):

```markdown
- `[s3] max_buckets` / `max_keys`: the ListBuckets / ListObjects page-size caps, `u32` (0 = unlimited, legal and meaningful — no range validation). `max_buckets` defaults to 10,000 (the AWS documented maximum); `max_keys` defaults to 0 (unlimited, preserving current behavior). Multipart listings have no caps (AWS documents none).
```

- [ ] **Step 5: Cross-check** — the docs are English-only (CLAUDE.md): re-read the two files for stray non-English text.

- [ ] **Step 6: Report** — summarize; leave changes in the tree.

---

## Self-review notes (checked against the spec)

- **Spec coverage**: contract types + trait + re-exports (T1); backends mem/fs with prefix filter + marker resume + probe + `max = 0` empty page (T3/T4); harness parity (T2); mapping validate/decode/clamp/map/echo + token errors (T6); unified `< 1` policy across all four surfaces + echo rule (T6); caps in config + `Capabilities::from` (T5); serve wiring (T7); e2e boto3 paginator, both ports (T8); docs (T9). No `IsTruncated` (s3s cannot carry it), no feature gate, no multipart caps, `bucket-region` ignored, `Owner` not emitted — all non-goals respected.
- **Type consistency**: `ListBucketsParams { prefix: String, start_after: Option<String>, max_buckets: usize }` and `BucketsListing { buckets: Vec<Bucket>, truncated: bool, next_start_after: Option<String> }` are used identically in T1→T4 and T6; `Capabilities::{max_buckets, max_keys}` (u32) in T5→T7; `clamp_page_size(requested: usize, cap: u32) -> usize` in T6.
- **Placeholders**: none — every step carries its exact code.
- **Deviation from the writing-plans template**: commit steps are replaced by report steps (CLAUDE.md forbids auto-commits; the user decides when to commit).
