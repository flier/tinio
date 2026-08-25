# Design: tinio-fs path mapping on strict-path

**Date**: 2026-08-25
**Status**: implemented — decisions locked by grilling (2026-08-25)
**Scope**: `crates/tinio-fs/src/path.rs` + call sites (`bucket_path`, `key_path`, `state_dir`)

## Goal

Path mapping is **strict-path-first**: containment, symlink resolution, Windows 8.3/ADS, and canonicalize proofs come from `strict-path`. Tinio only **supplements** rules the crate lacks, or that conflict with its Windows reserved-name policy.

## Non-goals

- No new S3 error codes beyond existing `InvalidPath`/`AccessDenied` mapping.
- I/O-time `follow_symlinks` checks in `objects.rs`/listing stay; path-layer enforcement is an earlier gate.
- `StrictPath` stays out of the public contract (callers keep `PathBuf`).

## Architecture

```
existing root / bucket_dir
        │
        ▼
 PathBoundary::try_new(...)          ← base (strict-path)
        │
        ├─ tinio supplements (before join, zero FS I/O)
        │    • object::key / bucket::name defensive re-check (FR-006)
        │    • .tinio → AccessDenied (FR-020)
        │    • empty interior segments a//b, a\\b (mirror alias refusal)
        │    • Windows only: WINDOWS_INVALID + windows_aliasing (reject;
        │      no reliance on \\?\ preservation)
        │
        └─ strict_join(candidate)    ← base proof
             → PathBuf via interop_path / unstrict for std FS APIs
```

### Coverage matrix

| Scenario | Owner |
|----------|--------|
| `..` / absolute / controls / `.` | Supplement: `object::key` (before any FS); also caught by `strict_join` if reached |
| Symlink / junction escape | Base: `strict_join` when `enforce_boundary` |
| Linux `/proc` magic symlinks | Base: soft-canonicalize / proc-canonicalize |
| Windows 8.3 short names | Base |
| NTFS ADS | Base (+ Windows `:` also refused by supplement charset) |
| `CON`/`NUL`/…, trailing `.`/` ` | Supplement: **reject** (opposite of soft-canonicalize `\\?\` preserve) |
| `<>"|?*\` on Windows | Supplement |
| Empty interior `a//b` / `a\\b` | Supplement |
| `.tinio` at any depth | Supplement → `AccessDenied` |
| FR-006 zero FS side effects on bad keys | Supplement runs first; only then `try_new` / `strict_join` |

### Unix vs Windows

- Same base path on both OSes (`PathBoundary` + `strict_join`).
- Charset/device supplements are Windows-only.
- Unix allows keys Windows would reject (`CON`, `:`, `*`, …) when they pass `object::key`.

### `follow_symlinks`

Policy ([s3-surface.md](../../../specs/001-s3-local-server/contracts/s3-surface.md)): default `false` refuses symlink resolution; opt-in `true` may follow links **outside** the storage root.

- `enforce_boundary = !follow_symlinks` (default: enforce).
- `false`: still run tinio supplements, then **plain** `bucket_dir.join(key)` (no escape→`Err`). Escape stays an I/O-time concern under `follow_symlinks = true`.
- `FsStorage` passes the flag into `key_path`.

## API

```rust
pub fn state_dir(root: &Path) -> Result<PathBuf, Error>;
pub fn bucket_path(root: &Path, name: &bucket::Name) -> Result<PathBuf, Error>;
pub fn key_path(bucket_dir: &Path, key: &object::Key, enforce_boundary: bool) -> Result<PathBuf, Error>;
```

- External type stays `PathBuf`; backend/write/listing/cleanup stable aside from the new `key_path` arg and `state_dir` becoming fallible.
- `PathBoundary::try_new` requires the restriction dir to exist (created/verified by callers). Missing → `InvalidPath`.
- No process-wide boundary cache (stale proof if dir replaced).
- Order: supplements first (zero FS), then `try_new`, then `strict_join` when enforcing.

### Call sites

- `FsStorage` object paths: `enforce_boundary = !self.follow_symlinks`.
- Cleanup / scanner / doctor: always `enforce_boundary = true` (never address outside bucket/root).
- `bucket_path`/`state_dir`: `PathBoundary` on root; `.tinio` bucket-name rejection stays a supplement.

### `is_contained`

Test helper only (or removed); production containment is the `StrictPath` proof.

## Error mapping

| Source | Tinio error |
|--------|-------------|
| Supplement: bad key / empty segments / Win charset·alias | `InvalidPath` |
| Supplement: `.tinio` | `AccessDenied` |
| `StrictPathError::PathEscapesBoundary` | `InvalidPath` |
| `PathResolutionError` / `InvalidRestriction` | `InvalidPath` |

## Dependencies

- Pin `strict-path` in `[workspace.dependencies]` (`major.minor` per `docs/cargo.md`).
- `tinio-fs`: `strict-path.workspace = true`.

## Tests

Real directories (`tempfile`); `PathBoundary::try_new` requires existence.

- Existing: nested keys, folder markers, empty segments, `.tinio` → `AccessDenied`, no escape for safe keys.
- Unix (`cfg(unix)`): in-bucket symlink to outside → `Err` when `enforce_boundary`; allowed / non-escaping failure mode when `false`.
- Windows (`cfg(windows)`): `NUL`, `a.`, invalid chars → `Err` via supplement.
- `bucket_path`: normal name Ok; `.tinio` Err.

## Docs

Update [fs-backend.md](../../../specs/001-s3-local-server/fs-backend.md) §1: strict-path base + tinio supplements; document `follow_symlinks` ↔ `enforce_boundary` — **done** (2026-08-25).

## Resolved choices

- `enforce_boundary == false` → supplements + plain `Path::join` (no `strict_join`).
- `state_dir` fallible via `PathBoundary::try_new(root)` + `strict_join(STATE_DIR_NAME)`.

## Grilled decisions (2026-08-25)

- **bucket directories follow `follow_symlinks`** (`bucket_path` itself takes no flag and always proves containment): under `false`, a symlinked/junction bucket directory is refused by the containment proof (`NoSuchBucket` everywhere); under `true`, `FsStorage::bucket_dir` **resolves the link to its canonical target** — the bucket *is* the target (a legit way to put a bucket on another volume) — and every proof and walk addresses the resolved path. Documented in fs-backend.md §1.
- **Windows 8.3 shapes refused by supplement**: `strict-path` only expands 8.3 names of existing components; a new 8.3-shaped key (`PROGRA~1`, `FILE~1.TXT`, `VERYLO~10`) can alias a later out-of-band sibling → `windows_aliasing` rejects the shape (base ≤ 6 chars + `~` + 1..=4 nonzero digits + optional ≤ 3-char extension).
- **Boundary cache with LRU bound**: `BoundaryCache` (moka `Cache`, cap 256) on `FsStorage`; free fns stay per-call (uncached public API), with `map_bucket_path`/`map_key_path` the single implementation behind both. Unix only: dev+inode identity check per lookup rebuilds on dir replacement. Windows never cached (no stable file identity — file-index APIs nightly-gated, creation FILETIME unchanged on recreation); every call rebuilds (correct, one extra canonicalize).
- **Error mapping**: `PathEscapesBoundary` → `InvalidPath`; `PathResolutionError`/`InvalidRestriction` → `Io` (real FS conditions — a deleted bucket mid-op is not a bad key).
- **state_dir override (FR-023) not boundary-validated**: admin/CLI config, out of path.rs scope.
- **`is_contained` kept** as `debug_assert!` invariant (both branches — the `enforce_boundary == false` plain-join path has no proof) + test helper; `#[cfg(any(test, debug_assertions))]`.
- **strict-path `junctions` feature enabled** on tinio-fs (Windows junction escape detection; the `junction` dep is Windows-target-gated in strict-path).
- **Pins**: `strict-path = "0.2"`, `moka = "0.12"` in `[workspace.dependencies]` (major.minor per docs/cargo.md); features enabled on the crate.
- **Lexical return** (proof only, not a rewrite): `strict_join` returns the canonicalized path; tinio discards it and returns `bucket_dir.join(key)`, so in-bucket links stay visible to the I/O-time symlink policy (non-goal "checks remain" holds) and listing/write semantics unchanged. Stale cached boundary fails safe (canonicalize of replaced dir does not `starts_with` old boundary).
