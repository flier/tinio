//! Bucket/key → filesystem path mapping (task T037).
//!
//! Buckets map to top-level subdirectories of the storage root, keys to
//! paths relative to the bucket. Traversal and absolute paths are rejected
//! **before any filesystem access** (FR-006); the reserved `.tinio` segment
//! is rejected at any depth (FR-020); platform charset restrictions follow
//! the host OS (Windows-invalid characters rejected on Windows only).
//!
//! Containment and symlink/junction escapes are proven by `strict-path`
//! (`PathBoundary` + `strict_join`, a canonicalize + boundary check); tinio
//! only **supplements** rules the crate does not provide: the checked
//! constructor re-check, `.tinio` refusal, empty-interior-segment refusal,
//! and the Windows charset / device-aliasing / 8.3-shape refusal. The
//! returned path is the **lexical** join — the strict-path proof is a gate,
//! not a rewrite — so the I/O-time symlink policy in `objects.rs` (a link
//! inside a bucket is refused even when contained) keeps its authority.
//!
//! Inputs arrive pre-validated as [`bucket::Name`] / [`object::Key`] (the
//! contract is called with checked constructors only); the mapping here is a
//! second, path-specific line of defense that can never escape the storage
//! root.

// `Component` is used only by the cfg-gated [`is_contained`].
#[cfg(unix)]
use std::io::Result as IoResult;
#[cfg(any(test, debug_assertions))]
use std::path::Component;
use std::{
    io::Error as IoError,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use moka::sync::Cache;
use strict_path::{PathBoundary, StrictPathError};
#[cfg(unix)]
use tokio::fs;
use tokio::task::spawn_blocking;

#[cfg(windows)]
use crate::_core::storage::Error::InvalidBucketName;
use crate::{
    _core::{bucket, object, storage},
    Error,
    backend::invalid_path,
    fsutil,
};

/// The reserved state-directory name — never served, never listed
/// (FR-020). One source of truth with the contract crate's reserved
/// segment.
pub const STATE_DIR_NAME: &str = object::RESERVED_SEGMENT;

/// The temporary-file subdirectory inside the state dir.
pub const TMP_DIR_NAME: &str = "tmp";

/// The multipart parts root inside the state dir.
pub const MULTIPART_DIR_NAME: &str = "multipart";

/// Upper bound on cached [`PathBoundary`] entries: the storage root plus
/// one per bucket directory. Bounded so a long-running server with
/// churning bucket directories cannot grow the cache without bound.
#[cfg(unix)]
const BOUNDARY_CACHE_CAP: u64 = 256;

/// Windows path characters that are never legal in a single path component.
///
/// Rejected on Windows only (the mapping follows the host OS charset, per
/// fs-backend.md §1); on other platforms they are legal file-name
/// characters. `\` is included because Windows treats it as a path
/// separator — a key `a\b` would alias the file of the key `a/b`.
#[cfg(windows)]
const WINDOWS_INVALID: &[char] = &['<', '>', ':', '"', '|', '?', '*', '\\'];

/// Whether a path segment aliases another file on Windows: a reserved
/// device name (`CON`, `NUL`, `COM1`..`LPT9` — with or without an
/// extension, case-insensitive), a trailing dot/space (Windows strips
/// both), or an 8.3 short-name shape. `CON` would open the console device
/// instead of a file; `a.` / `a ` collide with `a`.
#[cfg(windows)]
fn windows_aliasing(seg: &str) -> bool {
    let base = seg.rsplit_once('.').map(|(base, _)| base).unwrap_or(seg);
    if matches!(
        base.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    ) {
        return true;
    }
    seg.ends_with('.') || seg.ends_with(' ') || is_83_aliasing(seg)
}

/// Whether a segment matches the Windows 8.3 short-name shape
/// (`PROGRA~1`, `FILE~1.TXT`, `VERYLO~10`).
///
/// Opening an 8.3-shaped name resolves to an existing entry's short name
/// when 8.3 generation is enabled, aliasing two distinct keys to one
/// path. `strict-path` only expands 8.3 names of *existing* components; a
/// *new* 8.3-shaped key can alias a later out-of-band sibling (SC-006
/// serves out-of-band files), so the shape is refused outright. Windows
/// generates short names from the first ≤ 6 characters plus `~` plus up
/// to 4 digits and an extension of ≤ 3 characters — anything outside that
/// shape is a literal long name and safe.
#[cfg(windows)]
fn is_83_aliasing(seg: &str) -> bool {
    let lower = seg.to_ascii_lowercase();
    let stem = match lower.rsplit_once('.') {
        Some((stem, ext)) if !ext.is_empty() && ext.len() <= 3 => stem,
        Some(_) => return false, // extension > 3 chars: never a short name
        None => lower.as_str(),
    };
    if stem.is_empty() || stem.contains('.') {
        return false; // short names never contain a dot before the extension
    }
    let Some(tilde) = stem.rfind('~') else {
        return false;
    };
    let base = &stem[..tilde];
    let digits = &stem[tilde + 1..];
    let ok_digits = (1..=4).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit());
    // Short-name numbering starts at 1 — a `~0` name is a literal long
    // name and safe.
    let nonzero = digits.parse::<u32>().is_ok_and(|v| v >= 1);
    !base.is_empty() && base.len() <= 6 && ok_digits && nonzero
}

/// The filesystem identity of a directory — the cache-freshness check of
/// [`BoundaryCache`]: a cached canonical proof stays valid only while the
/// directory is the same object (a replaced directory — `rm` + `mkdir`
/// under the same name — gets a new identity and rebuilds).
///
/// Unix only: the dev+inode pair is the reliable identity. The cache is
/// disabled on Windows by design — every call rebuilds the proof (correct,
/// one extra canonicalize per mapping); the creation FILETIME would not
/// distinguish a recreated directory, and the object-file identity
/// (volume serial + file index, [`crate::fsutil::file_identity`]) is a
/// separate concern from the boundary cache.
#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirId {
    dev: u64,
    ino: u64,
}

#[cfg(unix)]
impl DirId {
    /// Test-only: the production cache-freshness check goes through
    /// [`Self::of_async`] (item 7a — the freshness stat must not run
    /// sync on the request threads).
    #[cfg(test)]
    fn of(dir: &Path) -> IoResult<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(dir)?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }

    /// The async form used by the boundary cache (item 7a — the
    /// freshness stat must not run sync on the request threads).
    async fn of_async(dir: &Path) -> IoResult<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = fs::metadata(dir).await?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
}

/// Map a `strict-path` failure onto the crate error (design Q4b): an
/// escape is a path violation ([`Error::InvalidPath`]); a restriction or
/// resolution failure is a filesystem condition ([`Error::Io`]).
fn map_boundary_error(err: StrictPathError) -> Error {
    match err {
        StrictPathError::PathEscapesBoundary { attempted_path, .. } => invalid_path(attempted_path),
        other => Error::Io(IoError::other(other)),
    }
}

/// Map a boundary failure to `NoSuchKey` when the bucket directory
/// itself is gone — the object cannot exist, and the old lexical join +
/// stat answered `NoSuchKey` (a racing `delete_bucket` or out-of-band
/// removal between the caller's `ensure_bucket` and the proof must not
/// turn into a 500). Other failures pass through unchanged. The
/// racing-delete policy has ONE home (F44) in its two forms: the sync
/// probe serves the offline [`key_path`]; the async probe (item 7a —
/// never a sync stat on the request threads, F22) serves
/// [`prove_key_contained`]. A probe error is treated as "the bucket is
/// still there" — the original error passes through (F11).
fn missing_bucket_boundary_sync(err: Error, bucket_dir: &Path, key: &object::Key) -> Error {
    if !bucket_dir.exists() {
        return Error::Storage(storage::no_such_key(key));
    }
    err
}

/// The async form of [`missing_bucket_boundary_sync`] (F22 — the probe
/// runs through `tokio::fs`, never a blocking stat on a request thread).
async fn missing_bucket_boundary_async(err: Error, bucket_dir: &Path, key: &object::Key) -> Error {
    if fsutil::is_absent(bucket_dir).await.unwrap_or(false) {
        return Error::Storage(storage::no_such_key(key));
    }
    err
}

/// A bounded cache of validated path boundaries keyed by directory path.
///
/// Each entry stores the boundary's filesystem identity; a lookup
/// re-stats the directory and rebuilds the boundary when the identity
/// changed (a replaced directory invalidates the cached proof). Unix
/// only: Windows has no stable file identity (see [`DirId`]), so the
/// cache never hits there and every call rebuilds the proof.
#[derive(Debug, Clone)]
pub(crate) struct BoundaryCache(#[cfg(unix)] Cache<PathBuf, (PathBoundary, DirId)>);

impl BoundaryCache {
    pub(crate) fn new() -> Self {
        #[cfg(unix)]
        {
            Self(Cache::new(BOUNDARY_CACHE_CAP))
        }
        #[cfg(not(unix))]
        {
            Self()
        }
    }

    /// The validated boundary of `dir`, rebuilt when the directory was
    /// replaced (identity change) or evicted. **Async (item 7a)**: the
    /// freshness stat runs through `tokio::fs` and the rebuild's
    /// canonicalize runs on the blocking pool — the boundary resolution
    /// never executes sync filesystem calls on the request threads
    /// (the old per-object-op sync stat/canonicalize is gone).
    async fn boundary(&self, dir: &Path) -> Result<PathBoundary, Error> {
        #[cfg(unix)]
        {
            if let Some((boundary, id)) = self.0.get(dir)
                && DirId::of_async(dir).await.ok() == Some(id)
            {
                return Ok(boundary);
            }
        }
        let dir = dir.to_path_buf();
        // The task closure owns its path copy — the unix bookkeeping
        // below uses the original (a `move` closure would move it).
        let dir_task = dir.clone();
        let boundary = spawn_blocking(move || PathBoundary::try_new(&dir_task))
            .await
            .map_err(IoError::other)
            .map_err(Error::Io)?
            .map_err(map_boundary_error)?;
        #[cfg(unix)]
        {
            let id = DirId::of_async(&dir).await.map_err(Error::Io)?;
            self.0.insert(dir, (boundary.clone(), id));
        }
        Ok(boundary)
    }
}

/// The sync, uncached boundary — the public mapping functions only
/// (test/offline surface; the `FsStorage` path goes through the async
/// cached form, item 7a).
fn boundary_uncached(dir: &Path) -> Result<PathBoundary, Error> {
    PathBoundary::try_new(dir).map_err(map_boundary_error)
}

/// The containment proof for `candidate` inside `boundary`: `strict_join`
/// canonicalizes and rejects escapes. The validated path is deliberately
/// discarded — the callers return the **lexical** join and leave the
/// I/O-time symlink policy (objects.rs) as the authority on in-bucket
/// links.
fn prove_contained(boundary: &PathBoundary, candidate: &str) -> Result<(), Error> {
    let _ = boundary
        .strict_join(candidate)
        .map_err(map_boundary_error)?;
    Ok(())
}

/// The state directory of a storage root: `<root>/.tinio/`.
///
/// Fallible since the mapping proves containment: a pre-existing
/// `<root>/.tinio` symlink (or junction) resolving outside the root is
/// refused. Read-only mode relocates it (FR-023); the relocation is a
/// caller-provided state-dir override on [`FsStorage`](crate::FsStorage),
/// not a path-mapping concern.
///
/// # Errors
///
/// `InvalidPath` when `<root>/.tinio` resolves outside `root`;
/// `Io` when `root` is missing or cannot be resolved.
pub fn state_dir(root: &Path) -> Result<PathBuf, Error> {
    let boundary: PathBoundary = PathBoundary::try_new(root).map_err(map_boundary_error)?;
    prove_contained(&boundary, STATE_DIR_NAME)?;
    Ok(root.join(STATE_DIR_NAME))
}

/// The bucket directory of a bucket: `<root>/<bucket>`.
///
/// The containment proof always runs in this raw form — a symlinked
/// bucket directory resolving outside `root` is refused. (The
/// `FsStorage` backend adds the follow policy: with `follow_symlinks`
/// enabled its [`bucket_dir`](crate::FsStorage) resolves the link to its
/// canonical target — the bucket *is* the target — and operates through
/// it.)
///
/// # Errors
///
/// `InvalidPath` when the name is a reserved `.tinio` collision (defensive
/// — names are pre-validated by [`bucket::name`]) or when the bucket
/// directory resolves outside `root`.
pub fn bucket_path(root: &Path, name: &bucket::Name) -> Result<PathBuf, Error> {
    let path = bucket_path_lexical(root, name)?;
    prove_contained(&boundary_uncached(root)?, name)?;
    Ok(path)
}

/// The lexical bucket mapping: the validation supplements (the reserved
/// `.tinio` refusal and the Windows charset/aliasing refusal — F21) and
/// the plain join. NO containment proof — the sync [`bucket_path`] and
/// async [`map_bucket_path`] add their own; the create path
/// ([`FsStorage::create_bucket`](crate::FsStorage)) cannot prove
/// a directory that does not exist yet, and the listing walk applies its
/// own symlink policy.
pub(crate) fn bucket_path_lexical(root: &Path, name: &bucket::Name) -> Result<PathBuf, Error> {
    if name.as_ref() == STATE_DIR_NAME {
        return Err(invalid_path(name.as_ref()));
    }
    #[cfg(windows)]
    {
        refuse_windows_bucket_name(name)?;
    }
    Ok(root.join(&**name))
}

/// Map `name` to `<root>/<name>`: the `.tinio` supplement, then the
/// strict-path containment proof, returning the **lexical** join.
/// Async (item 7a) — the boundary resolution runs off the request
/// threads; the public sync [`bucket_path`] is the uncached offline
/// form.
pub(crate) async fn map_bucket_path(
    cache: &BoundaryCache,
    root: &Path,
    name: &bucket::Name,
) -> Result<PathBuf, Error> {
    let path = bucket_path_lexical(root, name)?;
    prove_contained(&cache.boundary(root).await?, name)?;
    Ok(path)
}

/// The Windows reserved-name / charset refusal for BUCKET names (F21) —
/// the bucket-side mirror of the key-side check in
/// [`map_key_path_lexical`]. Without it a `con`/`nul`/`aux`/`com1`…
/// bucket passes name validation and fails materialization with an
/// opaque error (canonicalizing `root\con` resolves the console device
/// outside the boundary); the clean refusal answers `InvalidBucketName`.
#[cfg(windows)]
fn refuse_windows_bucket_name(name: &bucket::Name) -> Result<(), Error> {
    let seg = name.as_ref();
    if seg.chars().any(|c| WINDOWS_INVALID.contains(&c)) || windows_aliasing(seg) {
        return Err(Error::Storage(InvalidBucketName(name.to_string())));
    }
    Ok(())
}

/// The object file path of a key: `<root>/<bucket>/<key>`.
///
/// The key is validated **before any filesystem access**: traversal
/// (`..`), absolute paths, drive-letter paths, and control characters are
/// rejected by the [`object::key`] constructor (defensive re-check here),
/// and a reserved `.tinio` segment at any depth is refused (FR-020 — this
/// also protects nested roots: an outer server never maps into an inner
/// root's state). Only then, when `enforce_boundary` is set, does the
/// strict-path containment proof run.
///
/// The returned path is the **lexical** join — the proof is a gate, not a
/// rewrite — so a link inside the bucket stays visible to the I/O-time
/// symlink policy (`objects.rs` refuses it even though it is contained).
///
/// # Errors
///
/// - `InvalidPath` — traversal/absolute/control-character keys (defensive;
///   the caller should have rejected them already), empty interior
///   segments, Windows charset/aliasing violations, or a key path that
///   resolves outside the bucket directory.
/// - `AccessDenied` — a `.tinio` segment at any depth (FR-020).
/// - `NoSuchKey` — the bucket directory vanished before the containment
///   proof (a racing `delete_bucket` / out-of-band removal); the object
///   cannot exist.
pub fn key_path(
    bucket_dir: &Path,
    key: &object::Key,
    enforce_boundary: bool,
) -> Result<PathBuf, Error> {
    let path = map_key_path_lexical(bucket_dir, key)?;
    if enforce_boundary {
        let boundary = match boundary_uncached(bucket_dir) {
            Ok(boundary) => boundary,
            Err(err) => return Err(missing_bucket_boundary_sync(err, bucket_dir, key)),
        };
        if let Err(err) = prove_contained(&boundary, key) {
            return Err(missing_bucket_boundary_sync(err, bucket_dir, key));
        }
    }
    Ok(path)
}

/// The lexical half of [`map_key_path`]: the validation supplements and
/// the plain join, with **no filesystem access** — the defensive
/// constructor re-check, the reserved `.tinio` refusal (FR-020), the
/// Windows charset/aliasing refusal, and `bucket_dir.join(key)`. The
/// containment proof is a separate step ([`prove_key_contained`]) so the
/// object-op resolution can refuse a key before any syscall (P5).
pub(crate) fn map_key_path_lexical(bucket_dir: &Path, key: &object::Key) -> Result<PathBuf, Error> {
    // Defensive re-validation: the contract is only ever called with
    // validated keys, but re-running the checked constructor keeps the
    // escape-proof property local to this module, always in sync with
    // the authoritative rule set.
    if object::key(key.as_ref()).is_err() {
        return Err(invalid_path(key.as_ref()));
    }
    if key.is_reserved() {
        return Err(storage::access_denied(key).into());
    }
    #[cfg(windows)]
    {
        if key
            .split(['/', '\\'])
            .any(|seg| seg.chars().any(|c| WINDOWS_INVALID.contains(&c)) || windows_aliasing(seg))
        {
            return Err(invalid_path(key.as_ref()));
        }
    }
    let path = bucket_dir.join(&**key);
    // Same gate as [`is_contained`] (the reference must not survive a
    // release build that compiled the function out).
    #[cfg(any(test, debug_assertions))]
    debug_assert!(
        is_contained(bucket_dir, &path),
        "key path escapes the bucket directory: {path:?}"
    );
    Ok(path)
}

/// The containment-proof half of [`map_key_path`] over an already
/// mapped key: the `strict-path` boundary of `bucket_dir` plus the
/// `strict_join` check (canonicalize — filesystem access). Runs after
/// the symlink policy in the object-op resolution so a link inside the
/// bucket answers `AccessDenied` first (s3-surface.md).
///
/// # Errors
///
/// - `InvalidPath` — a key path that resolves outside the bucket
///   directory.
/// - `NoSuchKey` — the bucket directory vanished before the proof (a
///   racing `delete_bucket` / out-of-band removal); the object cannot
///   exist.
pub(crate) async fn prove_key_contained(
    cache: &BoundaryCache,
    bucket_dir: &Path,
    key: &object::Key,
) -> Result<(), Error> {
    let boundary = match cache.boundary(bucket_dir).await {
        Ok(boundary) => boundary,
        Err(err) => return Err(missing_bucket_boundary_async(err, bucket_dir, key).await),
    };
    if let Err(err) = prove_contained(&boundary, key) {
        return Err(missing_bucket_boundary_async(err, bucket_dir, key).await);
    }
    Ok(())
}

/// Map `key` to `bucket_dir.join(key)` — the lexical mapping plus the
/// containment proof when `enforce_boundary` is set; the async
/// implementation behind the `FsStorage` cached path (item 7a; the
/// public sync [`key_path`] is the uncached offline form).
pub(crate) async fn map_key_path(
    cache: &BoundaryCache,
    bucket_dir: &Path,
    key: &object::Key,
    enforce_boundary: bool,
) -> Result<PathBuf, Error> {
    let path = map_key_path_lexical(bucket_dir, key)?;
    if enforce_boundary {
        prove_key_contained(cache, bucket_dir, key).await?;
    }
    Ok(path)
}

/// Whether `path` stays within `base` (no `..` components, not absolute).
///
/// The escape-proof check backing [`key_path`]'s debug assertion; kept for
/// tests. The production containment proof is the strict-path boundary
/// check. Dead outside tests and debug builds (the only callers are the
/// `debug_assert` in [`map_key_path_lexical`] and the test module).
#[cfg(any(test, debug_assertions))]
fn is_contained(base: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(base) else {
        return false;
    };
    !rel.is_absolute() && rel.components().all(|c| matches!(c, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;
    use std::{
        fs,
        io::{Error as IoError, ErrorKind},
    };

    use super::*;
    use crate::_core::{bucket, object, storage::Error as StorageError};

    fn bucket_dir(root: &Path, name: &str) -> PathBuf {
        let dir = root.join(name);
        fs::create_dir(&dir).unwrap();
        dir
    }

    #[test]
    fn state_dir_maps_default() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(state_dir(root.path()).unwrap(), root.path().join(".tinio"));
    }

    #[test]
    fn state_dir_refuses_symlinked_state_dir() {
        // A pre-existing `<root>/.tinio` symlink resolving outside the
        // root must be refused, not followed by `database::open`.
        #[cfg(unix)]
        {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            symlink(outside.path(), root.path().join(".tinio")).unwrap();
            assert!(state_dir(root.path()).is_err());
        }
    }

    #[test]
    fn bucket_path_maps_top_level() {
        let root = tempfile::tempdir().unwrap();
        let name = bucket::name("my-bucket").unwrap();
        assert_eq!(
            bucket_path(root.path(), &name).unwrap(),
            root.path().join("my-bucket")
        );
    }

    #[test]
    fn bucket_path_defends_reserved_name() {
        // The checked constructor already refuses a leading dot, so the
        // `.tinio` branch of `bucket_path` is unreachable through the
        // public constructor — defense in depth against a future change.
        assert!(bucket::name(".tinio").is_err());
    }

    #[cfg(windows)]
    #[test]
    fn bucket_path_refuses_windows_reserved_device_names() {
        // "con" passes the S3 name grammar but aliases the console device
        // on Windows — the platform charset refusal must reject it before
        // any filesystem access.
        let root = tempfile::tempdir().unwrap();
        let name = bucket::name("con").unwrap();
        let err = bucket_path_lexical(root.path(), &name).unwrap_err();
        assert!(matches!(
            err,
            Error::Storage(StorageError::InvalidBucketName(_))
        ));
    }

    #[tokio::test]
    async fn map_key_path_refuses_a_missing_bucket_directory() {
        let root = tempfile::tempdir().unwrap();
        let cache = BoundaryCache::new();
        let key = object::key("a.txt").unwrap();
        // A vanished bucket dir (racing delete_bucket / out-of-band
        // removal) cannot prove containment — the object cannot exist.
        let missing = root.path().join("nope");
        let err = map_key_path(&cache, &missing, &key, true)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(StorageError::NoSuchKey(_))));
        // With a real bucket dir the same call proves containment and
        // returns the lexical join.
        let dir = bucket_dir(root.path(), "data");
        assert_eq!(
            map_key_path(&cache, &dir, &key, true).await.unwrap(),
            dir.join("a.txt")
        );
    }

    #[test]
    fn bucket_path_refuses_symlinked_bucket() {
        // A bucket directory that is a link resolves outside the root —
        // refused even though `follow_symlinks` might allow key escape.
        #[cfg(unix)]
        {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            symlink(outside.path(), root.path().join("linked")).unwrap();
            let name = bucket::name("linked").unwrap();
            assert!(bucket_path(root.path(), &name).is_err());
        }
    }

    #[test]
    fn key_path_maps_nested_keys() {
        let root = tempfile::tempdir().unwrap();
        let dir = bucket_dir(root.path(), "my-bucket");
        let key = object::key("dir/file.txt").unwrap();
        assert_eq!(
            key_path(&dir, &key, true).unwrap(),
            dir.join("dir/file.txt")
        );
        // Folder markers map to directories.
        let marker = object::key("dir/").unwrap();
        assert_eq!(key_path(&dir, &marker, true).unwrap(), dir.join("dir"));
    }

    #[test]
    fn key_path_rejects_empty_segments_that_alias() {
        // `a//b` / `a\\b` would alias a single-separator key on the
        // filesystem — the mirror cannot represent both, so the aliasing
        // key is refused at the contract boundary (`object::key`, all
        // backends agree). The mapping's own re-check stays as
        // defense-in-depth; folder markers (`dir/`, `a/b/`) keep their
        // trailing empty segment. Supplements run before any filesystem
        // access, so `enforce_boundary = false` exercises the same rule
        // with no real directories.
        let bucket_dir = Path::new("/srv/data/b");
        for evil in ["a//b", "a//", "a///b", "a//b/", r"a\\b", r"a/\b", r"a\/b"] {
            assert!(
                object::key(evil).is_err(),
                "{evil:?} must be rejected by the contract"
            );
        }
        for ok in ["a/b", "a/b/", "dir/", "a"] {
            let key = object::key(ok).unwrap();
            assert!(
                key_path(bucket_dir, &key, false).is_ok(),
                "{ok:?} must be accepted"
            );
        }
    }

    #[test]
    fn key_path_rejects_reserved_segment_at_any_depth() {
        let bucket_dir = Path::new("/srv/data/b");
        for key in [".tinio/state", "a/.tinio/x", "a/b/.tinio/c/d", ".tinio"] {
            let key = object::key(key).unwrap();
            let err = key_path(bucket_dir, &key, false).unwrap_err();
            assert!(
                matches!(err, Error::Storage(StorageError::AccessDenied(_))),
                "{key:?} must be AccessDenied, got {err:?}"
            );
        }
    }

    #[test]
    fn key_path_mapping_never_escapes_bucket() {
        // Property-style sweep over representative safe keys: the joined
        // path always stays inside the bucket directory. (`:` is rejected
        // on Windows only — platform charset, covered by its own rules.)
        let bucket_dir = Path::new("/srv/data/b");
        for key in [
            "a",
            "dir/file.txt",
            "dir/sub/file.txt",
            "with space.txt",
            "ümlaut.txt",
            "0-._~!$&'()+,;=@x",
        ] {
            let key = object::key(key).unwrap();
            let path = key_path(bucket_dir, &key, false).unwrap();
            assert!(is_contained(bucket_dir, &path), "{key:?} → {path:?}");
        }
    }

    #[test]
    fn key_path_proves_boundary_on_existing_bucket() {
        let root = tempfile::tempdir().unwrap();
        let dir = bucket_dir(root.path(), "b");
        // A key whose path stays inside the bucket passes the proof.
        let key = object::key("a/x.txt").unwrap();
        assert_eq!(key_path(&dir, &key, true).unwrap(), dir.join("a/x.txt"));
        // A key through a link resolving outside the bucket is refused.
        #[cfg(unix)]
        {
            let outside = tempfile::tempdir().unwrap();
            symlink(outside.path(), dir.join("link")).unwrap();
            let key = object::key("link/x.txt").unwrap();
            assert!(key_path(&dir, &key, true).is_err());
            // `enforce_boundary = false` returns the plain join (the
            // follow_symlinks path — I/O-time checks stay authoritative).
            assert!(key_path(&dir, &key, false).is_ok());
        }
    }

    #[test]
    fn key_path_keeps_lexical_path_for_in_bucket_link() {
        // The proof passes for a link resolving *inside* the bucket, and
        // the returned path stays lexical — the I/O-time symlink policy
        // (objects.rs) must still see the link component.
        #[cfg(unix)]
        {
            let root = tempfile::tempdir().unwrap();
            let dir = bucket_dir(root.path(), "b");
            fs::create_dir(dir.join("real")).unwrap();
            symlink(dir.join("real"), dir.join("link")).unwrap();
            let key = object::key("link/x.txt").unwrap();
            let path = key_path(&dir, &key, true).unwrap();
            assert_eq!(path, dir.join("link/x.txt"));
        }
    }

    #[tokio::test]
    async fn boundary_cache_rebuilds_on_directory_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BoundaryCache::new();
        assert!(cache.boundary(dir.path()).await.is_ok());
        #[cfg(unix)]
        {
            let before = DirId::of(dir.path()).unwrap();
            // Replacement = a live directory renamed over the path: its
            // inode is still allocated, so the identity is guaranteed to
            // change. rm + mkdir under the same name can reuse the freed
            // inode on ext4 — the identity then matches and the cache
            // (correctly) treats the path as unchanged.
            let repl = tempfile::tempdir().unwrap();
            fs::remove_dir(dir.path()).unwrap();
            fs::rename(repl.path(), dir.path()).unwrap();
            assert_ne!(before, DirId::of(dir.path()).unwrap());
            assert!(cache.boundary(dir.path()).await.is_ok());
        }
    }

    #[test]
    fn is_contained_checks() {
        let base = Path::new("/srv/data/b");
        assert!(is_contained(base, &base.join("a/b.txt")));
        assert!(!is_contained(base, &base.join("../x")));
        assert!(!is_contained(base, Path::new("/etc/passwd")));
        // The base directory is trivially contained in itself.
        assert!(is_contained(base, base));
    }

    #[cfg(windows)]
    #[test]
    fn windows_aliasing_covers_83_shapes() {
        for evil in ["progra~1", "PROGRA~1", "file~1.txt", "verylo~10", "a~1234"] {
            assert!(windows_aliasing(evil), "{evil:?} must be refused");
        }
        for ok in [
            "program files",
            "abcdefgh~1",  // base > 6 chars: never a generated short name
            "a.b~1",       // dot in the stem: not 8.3-shaped
            "file~1.long", // extension > 3 chars
            "file~12345",  // > 4 digits
            "file~0",      // short-name numbering starts at 1
        ] {
            assert!(!windows_aliasing(ok), "{ok:?} must be allowed");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_aliasing_covers_reserved_device_names() {
        // Reserved device names (with or without an extension, any case)
        // alias the console/device files — `CON` would open the console
        // instead of a file.
        for evil in [
            "con", "CON", "Con.txt", "nul", "nul.log", "aux", "prn", "com1", "com9", "lpt1", "lpt9",
        ] {
            assert!(windows_aliasing(evil), "{evil:?} must be refused");
        }
        for ok in ["console", "com10", "com0", "lpt10", "coner", "null"] {
            assert!(!windows_aliasing(ok), "{ok:?} must be allowed");
        }
    }

    #[test]
    fn map_boundary_error_projects_both_error_shapes() {
        // `PathEscapesBoundary` is a client error (`InvalidPath`); any
        // other boundary failure (a missing/invalid boundary directory)
        // is an IO error — the two map to different wire statuses.
        let escape = StrictPathError::PathEscapesBoundary {
            attempted_path: PathBuf::from("/etc/passwd"),
            restriction_boundary: PathBuf::from("/srv/data/b"),
        };
        assert!(matches!(map_boundary_error(escape), Error::InvalidPath(_)));
        let broken = StrictPathError::InvalidRestriction {
            restriction: PathBuf::from("/gone"),
            source: IoError::new(ErrorKind::NotFound, "gone"),
        };
        assert!(matches!(map_boundary_error(broken), Error::Io(_)));
    }

    #[tokio::test]
    async fn missing_bucket_boundary_maps_to_no_such_key() {
        // A boundary failure while the bucket directory is gone is
        // `NoSuchKey` (the object cannot exist); with the directory
        // present the original error passes through unchanged (F11 — a
        // probe error is not "gone").
        let root = tempfile::tempdir().unwrap();
        let key = object::key("a.txt").unwrap();
        let boundary_err = Error::Io(IoError::new(ErrorKind::PermissionDenied, "probe"));

        let missing = root.path().join("gone-bucket");
        let err = missing_bucket_boundary_sync(boundary_err, &missing, &key);
        assert!(matches!(err, Error::Storage(StorageError::NoSuchKey(_))));

        let present = bucket_dir(root.path(), "b");
        let err = missing_bucket_boundary_sync(
            Error::Io(IoError::new(ErrorKind::PermissionDenied, "probe")),
            &present,
            &key,
        );
        assert_eq!(err.to_string(), "I/O error: probe");

        let err = missing_bucket_boundary_async(
            Error::Io(IoError::new(ErrorKind::PermissionDenied, "probe")),
            &missing,
            &key,
        )
        .await;
        assert!(matches!(err, Error::Storage(StorageError::NoSuchKey(_))));
        let err = missing_bucket_boundary_async(
            Error::Io(IoError::new(ErrorKind::PermissionDenied, "probe")),
            &present,
            &key,
        )
        .await;
        assert_eq!(err.to_string(), "I/O error: probe");
    }

    #[tokio::test]
    async fn key_path_on_a_missing_bucket_answers_no_such_key() {
        // The containment proof on a vanished bucket directory must not
        // surface as a boundary IO error — the racing-delete policy
        // (F44) answers `NoSuchKey` for the sync and async forms alike.
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("gone-bucket");
        let key = object::key("a.txt").unwrap();
        let err = key_path(&missing, &key, true).unwrap_err();
        assert!(matches!(err, Error::Storage(StorageError::NoSuchKey(_))));

        let cache = BoundaryCache::new();
        let err = map_key_path(&cache, &missing, &key, true)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Storage(StorageError::NoSuchKey(_))));
    }
}
