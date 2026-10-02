# Known-Issues Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the six findings of audit run-2 — one parse authority for the copy/rename source, a loopback-only development credential, a bounded Select evaluation, a bounded HTTP method label, and a descriptor-bound Linux jail exposure bind.

**Architecture:** One small module (`tinio-auth/src/source.rs`) owns both source headers, with `s3s::dto::CopySource` as the copy-format authority. `tinio-server`'s identity module owns the fallback predicate; `serve.rs` enforces it. The Select engine gains a per-stream name index, a per-row JSON index and one per-request evaluation budget. `metrics.rs` gains a method-label whitelist. The Linux jail holds a no-follow descriptor per exposure and binds through `open_tree` + `move_mount`.

**Tech Stack:** Rust (edition 2024), s3s 0.17, tokio, cucumber 0.23 (`tinio-e2e`), rustix 1.1 (`mount` feature) for the sandbox.

**Spec:** `docs/superpowers/specs/2026-10-02-known-issues-remediation-design.md` — every decision below is that spec's. Fingerprints are audit run-2 artifacts; the code carries no audit reference.

## Global Constraints

- English only — code, comments, feature files, docs (project rule).
- No git writes — never `git add`/`commit`/`push`/stage; leave every change in the tree and report at each checkpoint (project rule).
- TDD — write the failing test, run it, implement, run it green (each task's steps).
- Async tests are `#[tokio::test]` / `async fn` directly; no `Runtime::block_on` (project rule).
- Test placement follows `docs/tests.md` §Placement: unit tests inline beside the subject; product-surface flows in cucumber when the migration rule admits them.
- s3s 0.17 is pinned; its dto surface is the API. Do not "fix" s3s.
- No version-compatibility shims: pre-release project, the P2 startup refusal lands directly.
- Existing tests stay green; only the metrics tests that label with `"FAM"`/`"INC"` change (Task 4).
- Cucumber scenarios reuse existing `@FR-*` tags (no new FR/SC IDs).

## Review Focus

Checked against the spec; each line's test sits in the owning task.

1. A source header whose first segment names an existing bucket (`/data/secret.txt`) must be denied, not treated as a phantom — the exact shape the old parser authorized (Task 1).
2. A percent-encoded key or a literal `+` in a source header must resolve to the same key the handler acts on (Task 1).
3. A loopback start with no `[auth]` must still start (with the warning), and a non-loopback start with explicit credentials must still start (Task 2).
4. A legitimate Select query under the budget must stream unchanged; a hostile one must end with the in-stream `S3QueryError`, not a panic or a silent truncation (Task 3).
5. An exposure whose ancestor is a symlink must still be refused (policy), and the forced-fallback leg must confine identically to the `move_mount` leg (Task 5).

---

### Task 1: One source-key authority (P1, P6)

**Files:**
- Create: `crates/tinio-auth/src/source.rs`
- Modify: `crates/tinio-auth/src/lib.rs` (add `pub mod source;` and re-export `source::{copy_source, copy_source_of, rename_source}`)
- Modify: `crates/tinio-auth/src/access.rs` (delete `parse_source`; both call sites use `crate::source`)
- Modify: `crates/tinio-server/src/backend/objects.rs` (rename op + copy helper)
- Test: inline `#[cfg(test)] mod tests` in `source.rs`; cucumber `crates/tinio-e2e/tests/features/conditions.feature`

**Interfaces:**
- Consumes: `s3s::dto::CopySource`, `crate::_core::{bucket, object}`.
- Produces (used by `access.rs` and `objects.rs`):
  - `pub fn copy_source(raw: &str) -> Option<(bucket::Name, object::Key)>`
  - `pub fn copy_source_of(src: &s3s::dto::CopySource) -> Option<(bucket::Name, object::Key)>`
  - `pub fn rename_source(raw: &str) -> Option<object::Key>`

- [x] **Step 1: Write the failing tests** in `crates/tinio-auth/src/source.rs`

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_source_strips_the_leading_slash_and_the_version_id() {
        let (b, k) = copy_source("/data/a/b.txt?versionId=7").expect("parsed");
        assert_eq!(&*b, "data");
        assert_eq!(&*k, "a/b.txt");
    }

    #[test]
    fn the_acl_parse_agrees_with_the_dto_the_handler_reads() {
        for raw in ["/data/a%20b.txt", "/data/a+b.txt", "/data/%E2%9C%93.txt"] {
            let dto = s3s::dto::CopySource::parse(raw).unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!(copy_source(raw), copy_source_of(&dto), "diverged on {raw}");
        }
    }

    #[test]
    fn copy_source_rejects_arns_and_malformed_values() {
        assert!(copy_source("arn:aws:s3:us-east-1:1:accesspoint/ap/object/k").is_none());
        assert!(copy_source("no-slash").is_none());
        assert!(copy_source("").is_none());
    }

    #[test]
    fn rename_source_is_a_key_of_the_request_bucket() {
        let k = rename_source("/a/b.txt").expect("parsed");
        assert_eq!(&*k, "a/b.txt");
        // A bucket-prefixed value is a KEY here, not a bucket split — the
        // exact divergence this task removes.
        let k = rename_source("/data/secret.txt").expect("parsed");
        assert_eq!(&*k, "data/secret.txt");
    }

    #[test]
    fn copy_source_of_maps_the_dto_the_handler_already_has() {
        let dto = s3s::dto::CopySource::parse("/data/a.txt?versionId=3").expect("dto");
        let (b, k) = copy_source_of(&dto).expect("mapped");
        assert_eq!((&*b, &*k), ("data", "a.txt"));
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio-auth source:: -v`
Expected: FAIL — `source.rs` does not exist / functions not found.

- [x] **Step 3: Write the module**

```rust
//! The one parse authority for `x-amz-copy-source` / `x-amz-rename-source`.

use crate::_core::{bucket, object};
use s3s::dto::CopySource;

/// The object a copy source names. `None` for a malformed value or an ARN
/// form — path-style only (SC-002), as the handler already enforces.
pub fn copy_source(raw: &str) -> Option<(bucket::Name, object::Key)> {
    copy_source_of(&CopySource::parse(raw).ok()?)
}

/// The same mapping for the dto s3s already parsed (the handler's input).
pub fn copy_source_of(src: &CopySource) -> Option<(bucket::Name, object::Key)> {
    let CopySource::Bucket { bucket: b, key, .. } = src else {
        return None; // access-point / outpost ARN: unsupported
    };
    Some((bucket::name(b.as_ref()).ok()?, object::key(key.as_ref()).ok()?))
}

/// The source key of the request's own bucket (RenameObject).
pub fn rename_source(raw: &str) -> Option<object::Key> {
    object::key(raw.strip_prefix('/').unwrap_or(raw)).ok()
}
```

Add to `lib.rs`: `pub mod source;` and `source::{copy_source, copy_source_of, rename_source}` in the `pub use` block.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-auth source:: -v`
Expected: PASS (5 tests).

- [x] **Step 5: Point the ACL at the shared parser**

In `crates/tinio-auth/src/access.rs`: delete `parse_source` (the `fn parse_source` near line 641). At the two call sites:

```rust
// source_gate (~line 232): the source is (request bucket, key) for RenameObject.
let (src_bucket, src_key) = match if request.op == "RenameObject" {
    request.rename_source.and_then(source::rename_source).map(|key| (dst_bucket.clone(), key))
} else {
    request.copy_source.and_then(source::copy_source)
} {
    Some(pair) => pair,
    None => return Err(denied()),
};

// the reads_source arm (~line 212): UploadPartCopy is copy-source only.
let (src_bucket, src_key) = match request.copy_source.and_then(source::copy_source) {
    Some(pair) => pair,
    None => return Err(denied()),
};
```

Update the `access.rs` unit tests that call `parse_source` directly (lines ~987, ~991, ~1035, ~1038, ~1043) to `source::copy_source` / `source::rename_source`.

- [x] **Step 6: Point the handler at the shared parser**

In `crates/tinio-server/src/backend/objects.rs`: replace `rename_source_key` (definition near line 1304 and its call at ~1192) with `tinio_auth::source::rename_source`, mapping `None` to the existing `InvalidArgument` error. Replace the shared copy-extraction helper `copy_source` in `crates/tinio-server/src/backend/mod.rs` with `tinio_auth::source::copy_source_of`; its two callers are `objects.rs` (`op_copy_object`) and `multipart.rs:434` (`op_upload_part_copy`).

- [x] **Step 7: Cucumber regressions**

Append to `crates/tinio-e2e/tests/features/conditions.feature` (fixture users are `root` and `user-b`; see `acl.feature`):

```gherkin
  @acl @FR-031
  Scenario: A bucket-WRITE principal cannot rename with a bucket-prefixed source
    When I send a "PUT" request to "/data" signed as "root" with headers
      | x-amz-grant-write | id="8b3873bb48d62959c9316b8bc5a1c11dcb3e14dce9426cc76ce9c67e00523b6c" |
    Then the response status is 200
    When I send a "PUT" request to "/data/secret.txt" signed as "root" with headers and body "top-secret"
      | x-amz-acl | private |
    Then the response status is 200
    When I send a "PUT" request to "/data/stolen.txt?renameObject" signed as "user-b" with headers
      | x-amz-rename-source | /data/secret.txt |
    Then the response status is 403
    And the error code is "AccessDenied"
    When I send a "GET" request to "/data/secret.txt" signed as "root"
    Then the response status is 200
    And the object body is "top-secret"

  @acl @FR-031
  Scenario: A bucket-READ grantee cannot copy a private object with a versionId suffix
    When I send a "PUT" request to "/data" signed as "root" with headers
      | x-amz-acl | public-read |
    Then the response status is 200
    When I send a "PUT" request to "/data/secret.txt" signed as "root" with headers and body "top-secret"
      | x-amz-acl | private |
    Then the response status is 200
    When I send a "PUT" request to "/out" signed as "user-b"
    Then the response status is 200
    When I send a "PUT" request to "/out/leak.txt" signed as "user-b" with headers and body ""
      | x-amz-copy-source | /data/secret.txt?versionId=x |
    Then the response status is 403
    And the error code is "AccessDenied"
```

The `root` access key is `ROOTAK` and `user-b` is `USERB`, both already configured by the harness (`acl.feature`); the grant is made at bucket creation because `PutBucketAcl` requires a `Content-MD5`.

- [x] **Step 8: Run the crate suites**

Run: `cargo test -p tinio-auth -p tinio-server` then `cargo build -p tinio && cargo test -p tinio-e2e --test cucumber -- conditions.feature`
Expected: PASS; the two new scenarios green.

- [x] **Step 9: Report**

Stop. Report the diff and the test output. Changes stay unstaged (`docs/git.md`).

---

### Task 2: Loopback-only development credential (P2)

**Files:**
- Modify: `crates/tinio-server/src/identity.rs` (add `uses_builtin_fallback`)
- Modify: `crates/tinio-cli/src/commands/start/serve.rs` (refusal + loopback warning)
- Modify: `README.md`, `crates/tinio-config/src/schema/auth.rs` (document the loopback-only fallback)
- Test: `crates/tinio/tests/credential_policy.rs`, modelled on `crates/tinio/tests/lifecycle.rs`

**Interfaces:**
- Consumes: `tinio_server::identity::{root_auth_pair, uses_builtin_fallback}`; `resolution.address()` / `resolution.config`.
- Produces: `pub fn uses_builtin_fallback(config: &Config) -> bool`.

- [x] **Step 1: Write the failing test**

```rust
// crates/tinio/tests/credential_policy.rs
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tinio"))
}

fn temp_root() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let cfg = dir.path().join("cfg.toml");
    (dir, root, cfg)
}

/// Spawn and read stderr until the line the test waits for, then kill.
fn read_until(args: &[&str], want: &str) -> String {
    let mut child = bin().args(args).stderr(Stdio::piped()).spawn().expect("spawn");
    let mut seen = String::new();
    if let Some(err) = child.stderr.take() {
        for line in BufReader::new(err).lines().map_while(Result::ok) {
            seen.push_str(&line);
            seen.push('\n');
            if line.contains(want) {
                break;
            }
        }
    }
    let _ = child.kill();
    let _ = child.wait();
    seen
}

#[test]
fn the_builtin_pair_is_refused_on_a_non_loopback_bind() {
    let (_d, root, cfg) = temp_root();
    std::fs::write(&cfg, "version = 1\n").expect("config");
    let out = bin()
        .args(["server", root.to_str().unwrap(), "--config", cfg.to_str().unwrap(),
               "--host", "0.0.0.0", "--port", "0"])
        .env_remove("TINIO_ACCESS_KEY")
        .env_remove("TINIO_SECRET_KEY")
        .output()
        .expect("run");
    assert!(!out.status.success(), "a non-loopback fallback start must fail");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("auth"), "the error names the missing [auth] pair: {err}");
}

#[test]
fn the_builtin_pair_starts_on_loopback() {
    let (_d, root, cfg) = temp_root();
    std::fs::write(&cfg, "version = 1\n").expect("config");
    let err = read_until(
        &["server", root.to_str().unwrap(), "--config", cfg.to_str().unwrap(),
          "--host", "127.0.0.1", "--port", "0"],
        "loopback",
    );
    assert!(err.contains("loopback"), "a loopback fallback start warns: {err}");
}

#[test]
fn explicit_credentials_start_on_a_non_loopback_bind() {
    let (_d, root, cfg) = temp_root();
    std::fs::write(&cfg, "version = 1\n[auth]\naccess_key = \"AK\"\nsecret_key = \"SK\"\n")
        .expect("config");
    let mut child = bin()
        .args(["server", root.to_str().unwrap(), "--config", cfg.to_str().unwrap(),
               "--host", "0.0.0.0", "--port", "0"])
        .env_remove("TINIO_ACCESS_KEY")
        .env_remove("TINIO_SECRET_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let exited = child.try_wait().expect("try_wait");
    let _ = child.kill();
    let _ = child.wait();
    assert!(exited.is_none(), "an explicit [auth] must not refuse a non-loopback bind: {exited:?}");
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p tinio --test credential_policy -v`
Expected: FAIL — the non-loopback start currently succeeds with a warning only.

- [x] **Step 3: Add the predicate and the enforcement**

```rust
// crates/tinio-server/src/identity.rs, beside root_auth_pair
/// True when the resolved pair is the built-in fallback (no `[auth]`).
/// The CLI refuses a non-loopback bind in that state; the server itself
/// stays agnostic.
pub fn uses_builtin_fallback(config: &Config) -> bool {
    config.auth.is_none()
}
```

```rust
// crates/tinio-cli/src/commands/start/serve.rs, immediately after `let bind = resolution.address()?;`
if identity::uses_builtin_fallback(&resolution.config) {
    if bind.ip().is_loopback() {
        eprintln!("tinio: warning: serving the built-in minioadmin pair on loopback");
    } else {
        return Err(Error::Config(_config::Error::invalid_value(
            "auth",
            "no [auth] pair: a non-loopback bind needs explicit credentials",
        )));
    }
}
```

Use the same `Error::Config(_config::Error::invalid_value(...))` shape already used in `resolve/mod.rs`; add the `identity` and `_config` imports if `serve.rs` lacks them (`rg -n "identity::|_config::" crates/tinio-cli/src/commands/start/serve.rs`). `warn_if_non_loopback(address)` stays as-is for configured credentials.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio --test credential_policy -v`
Expected: PASS (2 tests).

- [x] **Step 5: Document the residual**

`README.md`: one line beside the `serve` default-credential note — the built-in `minioadmin` pair is loopback-only; a non-loopback bind refuses to start without an `[auth]` pair (or `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY`). `crates/tinio-config/src/schema/auth.rs`: the same sentence in the `[auth]` doc comment.

- [x] **Step 6: Report**

Stop. Report the diff and the test output. Changes stay unstaged.

---

### Task 3: Bounded Select evaluation (P3)

**Files:**
- Modify: `crates/tinio-select/src/row.rs` (`NameHit`, `NameIndex`, `Columns::index`)
- Modify: `crates/tinio-select/src/csv.rs` (build the index once per stream)
- Modify: `crates/tinio-select/src/engine.rs` (`EVAL_CELL_CAP`, `Engine.spent`, `RowCtx.spent`, `csv_field`, `like_match`, `project`)
- Test: inline unit tests; extend `crates/tinio-select/benches/select_scan.rs`

**Interfaces:**
- Consumes: the existing `Columns`, `Record`, `RowCtx`, `Engine`, `like_match`.
- Produces:
  - `pub enum NameHit { One(usize), Ambiguous, None }`
  - `pub struct NameIndex` with `pub fn build(names: &[String]) -> Self` and `pub fn get(&self, name: &str, style: NameStyle) -> NameHit`
  - `Columns { fields: Vec<Field>, names: Rc<Vec<String>>, index: Rc<NameIndex> }`; `Columns::new(fields, names, index)`
  - `pub(crate) const EVAL_CELL_CAP: u64 = 10_000_000`
  - the counter is charged only for possibly-superlinear work: `json_lookup` (the entries it scans), `like_match` (cells) and the headerless `is_positional_names` walk (`names.len()`); the O(1) indexed lookup and the per-record projection list are not charged

- [x] **Step 1: Write the failing tests** in `crates/tinio-select/src/row.rs`

```rust
#[test]
fn the_index_distinguishes_a_hit_a_duplicate_and_a_miss() {
    let names = vec!["a".to_string(), "A".to_string(), "b".to_string()];
    let ix = NameIndex::build(&names);
    assert!(matches!(ix.get("a", NameStyle::Bare), NameHit::Ambiguous));
    assert_eq!(ix.get("a", NameStyle::Quoted), NameHit::One(0));
    assert_eq!(ix.get("b", NameStyle::Bare), NameHit::One(2));
    assert_eq!(ix.get("c", NameStyle::Bare), NameHit::None);
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-select row:: -v`
Expected: FAIL — `NameIndex` not defined.

- [x] **Step 3: Add the index and carry it on `Columns`**

```rust
// row.rs
/// One header lookup's outcome; `Ambiguous` is a value, not an error, so the
/// caller keeps its existing per-style rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameHit { One(usize), Ambiguous, None }

/// Header names resolved once per stream: one pass at build time, O(1) per
/// reference afterwards.
#[derive(Debug, Default)]
pub struct NameIndex {
    exact: std::collections::HashMap<String, NameHit>,
    folded: std::collections::HashMap<String, NameHit>,
}

impl NameIndex {
    pub fn build(names: &[String]) -> Self {
        let mut exact = std::collections::HashMap::new();
        let mut folded = std::collections::HashMap::new();
        for (i, n) in names.iter().enumerate() {
            insert(&mut exact, n.clone(), i);
            insert(&mut folded, n.to_ascii_lowercase(), i);
        }
        Self { exact, folded }
    }
    pub fn get(&self, name: &str, style: NameStyle) -> NameHit {
        let key = if style.exact() { name.to_string() } else { name.to_ascii_lowercase() };
        let map = if style.exact() { &self.exact } else { &self.folded };
        map.get(&key).copied().unwrap_or(NameHit::None)
    }
}

fn insert(map: &mut std::collections::HashMap<String, NameHit>, key: String, i: usize) {
    match map.get(&key) {
        Some(NameHit::One(_)) => { map.insert(key, NameHit::Ambiguous); }
        Some(_) => {}
        None => { map.insert(key, NameHit::One(i)); }
    }
}
```

`Columns` gains `pub index: Rc<NameIndex>`; `Columns::new(fields, names, index)`; `Columns::from_names(fields, names)` builds `NameIndex::build(&names)`.

- [x] **Step 4: Build the index once per stream in `csv.rs`**

Where `csv.rs` chooses `names` (the header set or the positional set, ~lines 225-246), build `let index = Rc::new(NameIndex::build(&names));` once beside the `Rc<Vec<String>>` it already caches, and pass it to `Columns::new(fields, names, index)` at line 246 and to the `from_names`-style sites (`:273`, `:411`) via `Columns::new`.

- [x] **Step 5: Make `csv_field` a lookup and thread the JSON index**

```rust
// engine.rs — csv_field keeps the positional branch, then:
match index.get(name, style) {
    NameHit::One(i) => Ok(Some(fields.get(i).cloned().unwrap_or(Field::Missing))),
    NameHit::Ambiguous => Err(Error::Ambiguous(name.into())),
    NameHit::None => {
        if is_positional_names(names) { Ok(Some(Field::Missing)) }
        else { Err(Error::MissingHeader(name.into())) }
    }
}
```

`column_value` / `parquet_field` call the new signature (`parquet_field` delegates to `csv_field`; `column_value` passes `&cols.index`). JSON keeps its existing search order unchanged: `json_lookup` is called on nested maps (`json_root_field` / `json_dot_step`), so no single per-row index is correct — instead `json_lookup` takes the `&Cell<u64>` and charges one per entry it scans before its existing case rules run.

- [x] **Step 6: Add the per-request budget**
```rust
// engine.rs
pub(crate) const EVAL_CELL_CAP: u64 = 10_000_000;

pub struct Engine {
    // existing fields …
    spent: std::cell::Cell<u64>,
}
```

`RowCtx` gains `spent: &'a std::cell::Cell<u64>` (set from `&self.spent` at both construction sites). Charge only possibly-superlinear work: `json_lookup` charges the entries it scans, `like_match` charges `tokens.len() as u64 * (m as u64 + 1)`, and the headerless `is_positional_names` fallback walk charges `names.len()` before it runs; each returns `Error::Value("SELECT expression too expensive".into())` once the total exceeds `EVAL_CELL_CAP`. Do **not** charge the O(1) indexed lookup or the per-record projection list — a legitimate wide `SELECT` must stream, not abort near the cap. `project()` re-checks the running total once per record through a zero charge (`charge(spent, 0)`), a defensive re-check rather than a priced step — the reference count is no longer charged. Pin each charged path with the shipped tests: `the_budget_rejects_a_like_expression_at_the_cap`, `the_budget_rejects_a_missing_reference_over_a_wide_name_set` and `the_budget_rejects_a_json_entry_scan_at_the_cap`, plus `a_wide_named_projection_streams_on_a_spent_budget` for the deliberately uncharged wide projection.

Pin it with the module's existing helpers (`parse` + `Engine::new` + `next` + `csv`):

```rust
#[test]
fn the_budget_rejects_a_like_expression_at_the_cap() {
    // Seeded just under the cap, one pair wider than the slack trips it: the
    // LIKE DP is priced, the reference count is not.
    let mut engine = Engine::new(
        parse("SELECT s.a FROM S3Object s WHERE s.a LIKE '%a%a%a%a%a%a%'").expect("plan"),
    );
    engine.spent.set(EVAL_CELL_CAP - 4);
    let err = engine.next(csv(&["aaaaaaa"], &["a"])).unwrap_err();
    assert!(matches!(err, Error::Value(_)), "{err:?}");
}
```

- [x] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p tinio-select -v`
Expected: PASS, including the pre-existing LIKE-cap test.
- [x] **Step 8: Extend the growth bench**

Add the audit's two shapes to `crates/tinio-select/benches/select_scan.rs` as `--test` cases: repeated named references over a wide header (must not grow with reference count) and a one-row-per-record stream (budget must trip with `S3QueryError`). Run: `cargo bench --profile ci -p tinio-select -- --test`.

- [x] **Step 9: Report**

Stop. Report the diff and the bench output. Changes stay unstaged.

---

### Task 4: Bounded HTTP method label (P4)

**Files:**
- Modify: `crates/tinio-server/src/metrics.rs` (`method_label`, `record_http_request`, and the two tests that label with `"FAM"`/`"INC"`)
- Test: inline unit test in `metrics.rs`

**Interfaces:**
- Produces: `fn method_label(method: &str) -> &'static str`

- [x] **Step 1: Write the failing test**

```rust
#[test]
fn unknown_methods_share_one_label() {
    assert_eq!(method_label("FAM"), "OTHER");
    assert_eq!(method_label("PATCH"), "OTHER");
    assert_eq!(method_label("GET"), "GET");
    assert_eq!(method_label("OPTIONS"), "OPTIONS");
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-server metrics:: -v`
Expected: FAIL — `method_label` not defined.

- [x] **Step 3: Add the whitelist**

```rust
/// The six verbs the data plane serves; any other HTTP token collapses to
/// one label so a client cannot grow the metric families.
fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "PUT" => "PUT",
        "POST" => "POST",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

pub fn record_http_request(method: &str, status: u16, duration: Duration) {
    #[cfg(test)]
    let _g = test_lock::writer();
    let method = method_label(method);
    record(&HTTP_REQUESTS, &[method, &status.to_string()], &HTTP_DURATION, method, duration);
}
```

Update the two tests that pass `"FAM"` / `"INC"` (~lines 533, 681) to a whitelisted verb or to assert the `OTHER` collapse.

- [x] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p tinio-server metrics:: -v`
Expected: PASS.

- [x] **Step 5: Report**

Stop. Report the diff and the test output. Changes stay unstaged.

---

### Task 5: Descriptor-bound jail exposure (P5)

**Files:**
- Modify: `crates/tinio-sandbox-linux/src/enter.rs` (`ExposureFd`, `validate_exposures`, the mount plan, `apply_mount` / `apply_bind`)
- Modify: `crates/tinio-sandbox-linux/tests/jail.rs` (forced-fallback leg)
- Modify: `docs/sandbox.md` (`/proc` prerequisite + the test hook)
- Test: `crates/tinio-sandbox-linux/tests/jail.rs`

**Interfaces:**
- Consumes: `rustix::fs::{open, fstat, OFlags}`, `rustix::mount::{open_tree, move_mount, OpenTreeFlags, MoveMountFlags}`, `rustix::fd::OwnedFd`.
- Produces: `struct ExposureFd(OwnedFd)`; `open_exposures` returns the descriptors in exposure order.

**Note (spec §5 correction, verified):** `move_mount(MOVE_MOUNT_F_EMPTY_PATH)` takes a **mount** descriptor; a plain directory's `O_PATH` fd is not one. The clone is obtained with `open_tree(OPEN_TREE_CLONE | AT_RECURSIVE)` — the recursive form the old `MS_BIND|MS_REC` bind had. The `/proc/self/fd/<n>` fallback keeps `MS_BIND | MS_REC`.

- [x] **Step 1: Write the failing test leg** in `crates/tinio-sandbox-linux/tests/jail.rs`

Give `child_environment` one more parameter and add the env-extended spawn twin:

```rust
fn child_environment(
    command: &mut Command,
    root: &Path,
    state: Option<&Path>,
    home: &Path,
    access: &str,
    probe: SocketAddr,
    extra: &[(&str, &str)],   // new
) {
    // … the existing five .env(...) calls …
    for (k, v) in extra {
        command.env(k, v);
    }
}

/// `run_child` keeps its signature (it passes `&[]`); this is the twin the hook leg uses.
fn run_child_with_env(
    root: &Path,
    state: Option<&Path>,
    home: &Path,
    access: &str,
    probe: SocketAddr,
    extra: &[(&str, &str)],
) -> Option<String> {
    // the body of `run_child`, threading `extra` into `child_environment`
}

#[test]
fn the_forced_proc_fallback_confines_identically() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The sibling memory-space leg's fixture: served root with
    // bucket/obj.txt, a scratch HOME, a parent-held loopback probe listener.
    // Extract it into `fn jail_fixture(dir: &tempfile::TempDir) -> (PathBuf, PathBuf, SocketAddr, Option<PathBuf>)`
    // if it is written inline, and have both legs call it.
    let (root, home, probe, state) = jail_fixture(&dir);
    let log = run_child_with_env(
        &root, state.as_deref(), &home, "read-write", probe,
        &[("TINIO_SANDBOX_FORCE_PROC_FALLBACK", "1")],
    )
    .expect("this host can confine");
    assert!(log.contains("read-ok:"), "{log}");
    assert!(log.contains("host-etc-visible: false"), "{log}");
    assert!(log.contains("old-root-visible: false"), "{log}");
    assert!(log.contains("host-home-visible: false"), "{log}");
    assert!(log.contains("outbound-connect: refused"), "{log}");
}
```

The ancestor-symlink policy keeps its existing oracle: the `refuse_symlink_exposure` / `validate_exposures` tests in `enter.rs` (the `pub(crate)` surface at `enter.rs:1518`); Step 5 runs them.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p tinio-sandbox-linux --test jail the_forced_proc_fallback_confines_identically -v`
Expected: FAIL — the hook is not read and no `ExposureFd` exists.

- [x] **Step 3: Capture the descriptors with the check**

**Note (spec §5 correction, verified):** the shipped shape is **two functions, not one**, and `O_NOFOLLOW` does not do what this step first claimed. `validate_exposures` keeps its path policy and its **pre-unshare** position, but it returns `Result<()>` and opens nothing; a separate `open_exposures` opens the descriptors **after** `enter_mount_namespace()`, because a mount is bindable only from the namespace that owns it — a descriptor opened before the `unshare` belongs to the namespace this process left and both `open_tree` and the `/proc/self/fd` bind refuse it with `EINVAL` (measured). And the leaf is **not** refused by the open: `O_PATH | O_NOFOLLOW` opens the link itself and returns a descriptor for the *link* (measured: `O_PATH|O_NOFOLLOW` opens the link, while `O_RDONLY|O_NOFOLLOW` answers `ELOOP`), so the policy walk is re-run after the fallback open. This is the shipped split:

```rust
/// The validated exposure: the descriptor is the object, the path was only
/// the way to it.
struct ExposureFd(rustix::fd::OwnedFd);

/// Open every validated exposure, in exposure order — after
/// `enter_mount_namespace`, whose namespace owns the descriptors the binds need.
fn open_exposures(sandbox: &Sandbox) -> Result<Vec<ExposureFd>> {
    sandbox
        .exposures()
        .iter()
        .map(|exposure| open_exposure(&exposure.host))
        .collect()
}

/// One `openat2` resolution with `RESOLVE_NO_SYMLINKS` is the whole ancestor
/// policy on the primary arm; the fallback's open refuses nothing, so the walk
/// runs again after it.
fn open_exposure(path: &std::path::Path) -> Result<ExposureFd> {
    let refuse = |err: rustix::io::Errno| {
        let reason = if err == rustix::io::Errno::LOOP { SYMLINK_EXPOSURE } else { "cannot open" };
        refused(EXPOSE_STEP, Error::BadPath { path: path.to_path_buf(), reason })
    };
    match rustix::fs::openat2(
        rustix::fs::CWD,
        path,
        OFlags::PATH | OFlags::CLOEXEC,
        Mode::empty(),
        rustix::fs::ResolveFlags::NO_SYMLINKS,
    ) {
        Ok(descriptor) => Ok(ExposureFd(descriptor)),
        Err(err) if err == rustix::io::Errno::NOSYS || err == rustix::io::Errno::INVAL => {
            let descriptor = rustix::fs::open(
                path,
                OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(refuse)?;
            // The leaf `O_PATH | O_NOFOLLOW` accepted as the link's own
            // descriptor, and the components this open had to walk.
            refuse_symlink_exposure(path)?;
            Ok(ExposureFd(descriptor))
        }
        Err(err) => Err(refuse(err)),
    }
}
```

`validate_exposures` keeps the existing `refuse_symlink_exposure` (ancestor policy) and existence check, and still returns `Result<()>`; `enter` now calls `open_exposures(sandbox)?` after `enter_mount_namespace()` and holds the `Vec` for the run.

- [x] **Step 4: Bind from the descriptor**

The mount list is built in `plan.rs` as: the jail-root tmpfs `Mount` first, then one `Mount` per `sandbox.exposures()` entry (plan.rs:214-223), then the optional device `Mount`s — so parallel vectors cannot be zipped by index without an explicit link. Add `exposure: Option<usize>` to `Mount` (plan.rs:43) and set it in that loop (`enumerate()` → `Some(i)`), leaving the tmpfs and device mounts `None`; update the three in-crate `Mount` literals (`enter.rs:1118`, `:2226`, `:2292`) and the `plan` doc example/assertions. `open_exposures` returns `Vec<ExposureFd>` in exposure order, and `apply_bind` takes that whole slice and resolves `entry.exposure` itself (the device and tmpfs mounts keep the path-based `mount_bind_recursive`). **Note (M2 correction, verified):** the link is a **checked lookup, not an index** — the shipped `apply_bind` resolves `entry.exposure` with `descriptors.get(index)`, a miss becoming the crate's ordinary `bind exposure` refusal naming both the mount and the descriptor count rather than a panic inside confinement (design §11). The guard sits in the `match entry.exposure` immediately above the `bind_exposure` call the snippet adds, so the snippet's function is reached only with a resolved descriptor:

```rust
match entry.exposure {
    Some(index) => {
        let descriptor = descriptors.get(index).ok_or_else(|| {
            fail(
                "bind exposure",
                io::Error::other(format!(
                    "mount {} names exposure {index}, but only {} descriptor(s) were opened",
                    entry.source.display(),
                    descriptors.len(),
                )),
            )
        })?;
        bind_exposure(&descriptor.0, &entry.target)?;
    }
    None => mount_bind_recursive(&entry.source, &entry.target).map_err(step("mount --bind"))?,
}
```

Then add:

```rust
// enter.rs imports: rustix::{fd::OwnedFd, fs::OFlags, io::Errno,
//                      mount::{move_mount, open_tree, MoveMountFlags, OpenTreeFlags}};

/// Bind the validated object: a recursive clone of the descriptor's mount
/// moved onto the target, or the descriptor's `/proc` link where `open_tree`
/// is unavailable. `move_mount` takes a *mount* descriptor, which is why the
/// clone (not the bare `O_PATH` fd) is what it receives.
fn bind_exposure(fd: &OwnedFd, target: &Path) -> Result<()> {
    if std::env::var_os("TINIO_SANDBOX_FORCE_PROC_FALLBACK").is_none() {
        let flags = OpenTreeFlags::OPEN_TREE_CLONE
            | OpenTreeFlags::AT_EMPTY_PATH
            | OpenTreeFlags::AT_RECURSIVE;
        match open_tree(fd, "", flags) {
            Ok(tree) => {
                return move_mount(
                    &tree,
                    "",
                    rustix::fs::CWD,
                    target,
                    MoveMountFlags::MOVE_MOUNT_F_EMPTY_PATH,
                )
                .map_err(step("move_mount"));
            }
            Err(Errno::NOSYS) => {} // no open_tree: fall through to /proc
            Err(e) => return Err(step("open_tree")(e)),
        }
    }
    let link = format!("/proc/self/fd/{}", std::os::fd::AsRawFd::as_raw_fd(fd));
    mount_bind_recursive(Path::new(&link), target).map_err(step("mount --bind"))
}
```

Descriptors are dropped after `apply_bind` returns and on every error path (they are owned by the `Vec` in `enter`).

- [x] **Step 5: Run the jail suite**

Run: `cargo test -p tinio-sandbox-linux --test jail -v`
Expected: PASS, both the `move_mount` legs and the forced-fallback leg (on WSL2; a host without unprivileged userns may refuse before the bind by design).

- [x] **Step 6: Document**

`docs/sandbox.md`: the bind is descriptor-bound (`open_tree` + `move_mount`); the `/proc/self/fd` fallback is for kernels without `open_tree` and requires `/proc`; `TINIO_SANDBOX_FORCE_PROC_FALLBACK=1` forces it and is read in release builds too, selecting between two descriptor-bound paths.

- [x] **Step 7: Report**

Stop. Report the diff and the test output. Changes stay unstaged.

---

### Task 6: Gates and audit reproductions

**Files:** none (verification only).

- [x] **Step 1: Workspace gates**

```sh
cargo check --profile ci --workspace --all-targets
cargo clippy --profile ci --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

Expected: all Finished, zero warnings.

- [x] **Step 2: Touched-crate suites**

```sh
cargo test -p tinio-auth -p tinio-server -p tinio-select -p tinio-cli -p tinio
cargo test -p tinio-sandbox-linux --test jail
```

Expected: green (the pre-existing host-environmental failures on this Windows host are unchanged and named in the audit report).

- [x] **Step 3: Re-run the audit reproductions**

In the WSL2 sandbox used by audit run-2 (`wsl -d Ubuntu-26.04 -e bash -lc "bash /var/tmp/sa-run2/run.sh …"`), re-run the P1 copy-`versionId` and P2 default-credential probes, and the P3 growth harness. Expected: each negative result is gone; each positive control is unchanged.

- [x] **Step 4: Report**

Stop. Report every command's output and the tree diff. Changes stay unstaged; the operator decides commits.
Evidence: `.superpowers/sdd/2026-10-02-known-issues-remediation/task-6-gates.md` and
`task-6-reproductions.md`.
