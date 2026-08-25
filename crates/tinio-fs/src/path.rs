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

use std::{
    io,
    path::{Component, Path, PathBuf},
};

#[cfg(unix)]
use moka::sync::Cache;
use strict_path::{PathBoundary, StrictPathError};
use tinio_core::{bucket, object, storage};

use crate::backend::invalid_path;

/// The reserved state-directory name — never served, never listed
/// (FR-020). One source of truth with the contract crate's reserved
/// segment.
pub const STATE_DIR_NAME: &str = tinio_core::object::RESERVED_SEGMENT;

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
    fn of(dir: &Path) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(dir)?;
        Ok(Self {
            dev: meta.dev(),
            ino: meta.ino(),
        })
    }
}

/// Map a `strict-path` failure onto the crate error (design Q4b): an
/// escape is a path violation ([`Error::InvalidPath`]); a restriction or
/// resolution failure is a filesystem condition ([`Error::Io`]).
fn map_boundary_error(err: StrictPathError) -> crate::Error {
    match err {
        StrictPathError::PathEscapesBoundary { attempted_path, .. } => invalid_path(attempted_path),
        other => crate::Error::Io(io::Error::other(other)),
    }
}

/// Map a boundary failure to `NoSuchKey` when the bucket directory
/// itself is gone — the object cannot exist, and the old lexical join +
/// stat answered `NoSuchKey` (a racing `delete_bucket` or out-of-band
/// removal between the caller's `ensure_bucket` and the proof must not
/// turn into a 500). Other failures pass through unchanged.
fn missing_bucket_boundary(
    err: crate::Error,
    bucket_dir: &Path,
    key: &object::Key,
) -> crate::Error {
    if !bucket_dir.exists() {
        return crate::Error::Storage(storage::no_such_key(key));
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
    /// replaced (identity change) or evicted.
    fn boundary(&self, dir: &Path) -> Result<PathBoundary, crate::Error> {
        #[cfg(unix)]
        {
            if let Some((boundary, id)) = self.0.get(dir) {
                if DirId::of(dir).ok() == Some(id) {
                    return Ok(boundary);
                }
            }
        }
        let boundary = PathBoundary::try_new(dir).map_err(map_boundary_error)?;
        #[cfg(unix)]
        {
            let id = DirId::of(dir).map_err(crate::Error::Io)?;
            self.0.insert(dir.to_path_buf(), (boundary.clone(), id));
        }
        Ok(boundary)
    }
}

/// The validated boundary of `dir`, either from `cache` or built fresh.
fn boundary_for(cache: Option<&BoundaryCache>, dir: &Path) -> Result<PathBoundary, crate::Error> {
    match cache {
        Some(cache) => cache.boundary(dir),
        None => PathBoundary::try_new(dir).map_err(map_boundary_error),
    }
}

/// The containment proof for `candidate` inside `boundary`: `strict_join`
/// canonicalizes and rejects escapes. The validated path is deliberately
/// discarded — the callers return the **lexical** join and leave the
/// I/O-time symlink policy (objects.rs) as the authority on in-bucket
/// links.
fn prove_contained(boundary: &PathBoundary, candidate: &str) -> Result<(), crate::Error> {
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
pub fn state_dir(root: &Path) -> Result<PathBuf, crate::Error> {
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
pub fn bucket_path(root: &Path, name: &bucket::Name) -> Result<PathBuf, crate::Error> {
    map_bucket_path(None, root, name)
}

/// Map `name` to `<root>/<name>`: the `.tinio` supplement, then the
/// strict-path containment proof, returning the **lexical** join.
pub(crate) fn map_bucket_path(
    cache: Option<&BoundaryCache>,
    root: &Path,
    name: &bucket::Name,
) -> Result<PathBuf, crate::Error> {
    if name.as_ref() == STATE_DIR_NAME {
        return Err(invalid_path(name.as_ref()));
    }
    prove_contained(&boundary_for(cache, root)?, name)?;
    Ok(root.join(&**name))
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
) -> Result<PathBuf, crate::Error> {
    map_key_path(None, bucket_dir, key, enforce_boundary)
}

/// Map `key` to `bucket_dir.join(key)` — the single implementation behind
/// the public [`key_path`] and the `FsStorage` cached path.
pub(crate) fn map_key_path(
    cache: Option<&BoundaryCache>,
    bucket_dir: &Path,
    key: &object::Key,
    enforce_boundary: bool,
) -> Result<PathBuf, crate::Error> {
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
    if enforce_boundary {
        let boundary = match boundary_for(cache, bucket_dir) {
            Ok(boundary) => boundary,
            Err(err) => return Err(missing_bucket_boundary(err, bucket_dir, key)),
        };
        if let Err(err) = prove_contained(&boundary, key) {
            return Err(missing_bucket_boundary(err, bucket_dir, key));
        }
    }
    debug_assert!(
        is_contained(bucket_dir, &path),
        "key path escapes the bucket directory: {path:?}"
    );
    Ok(path)
}

/// Whether `path` stays within `base` (no `..` components, not absolute).
///
/// The escape-proof check backing [`key_path`]'s debug assertion; kept for
/// tests. The production containment proof is the strict-path boundary
/// check.
#[cfg(any(test, debug_assertions))]
fn is_contained(base: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(base) else {
        return false;
    };
    !rel.is_absolute() && rel.components().all(|c| matches!(c, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinio_core::storage::Error as StorageError;
    use tinio_core::{bucket, object};

    fn bucket_dir(root: &Path, name: &str) -> PathBuf {
        let dir = root.join(name);
        std::fs::create_dir(&dir).unwrap();
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
            std::os::unix::fs::symlink(outside.path(), root.path().join(".tinio")).unwrap();
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

    #[test]
    fn bucket_path_refuses_symlinked_bucket() {
        // A bucket directory that is a link resolves outside the root —
        // refused even though `follow_symlinks` might allow key escape.
        #[cfg(unix)]
        {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(outside.path(), root.path().join("linked")).unwrap();
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
                matches!(err, crate::Error::Storage(StorageError::AccessDenied(_))),
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
            std::os::unix::fs::symlink(outside.path(), dir.join("link")).unwrap();
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
            std::fs::create_dir(dir.join("real")).unwrap();
            std::os::unix::fs::symlink(dir.join("real"), dir.join("link")).unwrap();
            let key = object::key("link/x.txt").unwrap();
            let path = key_path(&dir, &key, true).unwrap();
            assert_eq!(path, dir.join("link/x.txt"));
        }
    }

    #[test]
    fn boundary_cache_rebuilds_on_directory_replacement() {
        // The cache keeps a proof valid while the directory identity is
        // unchanged; replacing the directory (new identity) rebuilds.
        // Unix only for the identity assertion (dev+ino): Windows has no
        // stable identity and never hits the cache (see [`DirId`]).
        let dir = tempfile::tempdir().unwrap();
        let cache = BoundaryCache::new();
        assert!(cache.boundary(dir.path()).is_ok());
        #[cfg(unix)]
        {
            let before = DirId::of(dir.path()).unwrap();
            std::fs::remove_dir(dir.path()).unwrap();
            std::fs::create_dir(dir.path()).unwrap();
            assert_ne!(before, DirId::of(dir.path()).unwrap());
            assert!(cache.boundary(dir.path()).is_ok());
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
}
