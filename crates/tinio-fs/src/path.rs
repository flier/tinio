//! Bucket/key → filesystem path mapping (task T037).
//!
//! Buckets map to top-level subdirectories of the storage root, keys to
//! paths relative to the bucket. Traversal and absolute paths are rejected
//! **before any filesystem access** (FR-006); the reserved `.tinio` segment
//! is rejected at any depth (FR-020); platform charset restrictions follow
//! the host OS (Windows-invalid characters rejected on Windows only).
//!
//! Inputs arrive pre-validated as [`bucket::Name`] / [`object::Key`] (the
//! contract is called with checked constructors only); the mapping here is a
//! second, path-specific line of defense that can never escape the storage
//! root.

use std::path::{Component, Path, PathBuf};

use tinio_core::{bucket, object, object::Key, storage};

use crate::backend::invalid_path;

/// The reserved state-directory name — never served, never listed
/// (FR-020). One source of truth with the contract crate's reserved
/// segment.
pub const STATE_DIR_NAME: &str = tinio_core::object::RESERVED_SEGMENT;

/// The temporary-file subdirectory inside the state dir.
pub const TMP_DIR_NAME: &str = "tmp";

/// The ETag metadata store root inside the state dir.
pub const META_DIR_NAME: &str = "meta";

/// The multipart parts root inside the state dir.
pub const MULTIPART_DIR_NAME: &str = "multipart";

/// The bucket-creation-times file inside the state dir.
pub const BUCKETS_FILE: &str = "buckets.json";

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
/// extension, case-insensitive) or a trailing dot/space (Windows strips
/// both). `CON` would open the console device instead of a file; `a.` /
/// `a ` collide with `a`.
#[cfg(windows)]
fn windows_aliasing(seg: &str) -> bool {
    let base = seg
        .rsplit_once('.')
        .map(|(base, _)| base)
        .unwrap_or(seg);
    if matches!(
        base.to_ascii_uppercase().as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "COM1" | "COM2" | "COM3" | "COM4" | "COM5" | "COM6"
            | "COM7" | "COM8" | "COM9" | "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5"
            | "LPT6" | "LPT7" | "LPT8" | "LPT9"
    ) {
        return true;
    }
    seg.ends_with('.') || seg.ends_with(' ')
}

/// The state directory of a storage root: `<root>/.tinio/`.
///
/// Read-only mode relocates it (FR-023); the relocation is a
/// caller-provided state-dir override on [`FsStorage`](crate::FsStorage),
/// not a path-mapping concern.
pub fn state_dir(root: &Path) -> PathBuf {
    root.join(STATE_DIR_NAME)
}

/// The bucket directory of a bucket: `<root>/<bucket>`.
///
/// # Errors
///
/// `InvalidPath` when the name is a reserved `.tinio` collision or cannot
/// map to a safe path component (defensive — names are pre-validated by
/// [`bucket::name`]).
pub fn bucket_path(root: &Path, name: &bucket::Name) -> Result<PathBuf, crate::Error> {
    if name.as_ref() == STATE_DIR_NAME {
        return Err(invalid_path(name.as_ref()));
    }
    Ok(root.join(&**name))
}

/// The object file path of a key: `<root>/<bucket>/<key>`.
///
/// The key is validated **before any filesystem access**: traversal
/// (`..`), absolute paths, drive-letter paths, and control characters are
/// rejected by the [`object::key`] constructor (defensive re-check here),
/// and a reserved `.tinio` segment at any depth is refused (FR-020 — this
/// also protects nested roots: an outer server never maps into an inner
/// root's state).
///
/// # Errors
///
/// - `InvalidPath` — traversal/absolute/control-character keys (defensive;
///   the caller should have rejected them already).
/// - `AccessDenied` — a `.tinio` segment at any depth (FR-020).
pub fn key_path(bucket_dir: &Path, key: &Key) -> Result<PathBuf, crate::Error> {
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
        if key.split(['/', '\\']).any(|seg| {
            seg.chars().any(|c| WINDOWS_INVALID.contains(&c)) || windows_aliasing(seg)
        }) {
            return Err(invalid_path(key.as_ref()));
        }
    }
    let path = bucket_dir.join(&**key);
    debug_assert!(
        is_contained(bucket_dir, &path),
        "key path escapes the bucket directory: {path:?}"
    );
    Ok(path)
}

/// Whether `path` stays within `base` (no `..` components, not absolute).
///
/// The escape-proof check backing [`key_path`]'s debug assertion; exposed
/// for tests.
pub fn is_contained(base: &Path, path: &Path) -> bool {
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

    #[test]
    fn bucket_path_maps_top_level() {
        let root = Path::new("/srv/data");
        let name = bucket::name("my-bucket").unwrap();
        assert_eq!(
            bucket_path(root, &name).unwrap(),
            Path::new("/srv/data/my-bucket")
        );
    }

    #[test]
    fn key_path_maps_nested_keys() {
        let root = Path::new("/srv/data");
        let bucket_dir = root.join("my-bucket");
        let key = object::key("dir/file.txt").unwrap();
        assert_eq!(
            key_path(&bucket_dir, &key).unwrap(),
            Path::new("/srv/data/my-bucket/dir/file.txt")
        );
        // Folder markers map to directories.
        let marker = object::key("dir/").unwrap();
        assert_eq!(
            key_path(&bucket_dir, &marker).unwrap(),
            Path::new("/srv/data/my-bucket/dir")
        );
    }

    #[test]
    fn key_path_rejects_reserved_segment_at_any_depth() {
        let bucket_dir = Path::new("/srv/data/b");
        for key in [".tinio/state", "a/.tinio/x", "a/b/.tinio/c/d", ".tinio"] {
            let key = object::key(key).unwrap();
            let err = key_path(bucket_dir, &key).unwrap_err();
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
            let path = key_path(bucket_dir, &key).unwrap();
            assert!(is_contained(bucket_dir, &path), "{key:?} → {path:?}");
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
}
