//! Object key validation and object metadata.

use std::time::SystemTime;

use derive_more::{AsRef, Deref, Display, Into};
use garde::Error as GardeError;

use crate::{
    ETag,
    storage::{self, Error::*},
};

/// The reserved state-directory segment: never a bucket, never part of a
/// key (FR-020). The fs backend's state dir is named after this constant.
pub const RESERVED_SEGMENT: &str = ".tinio";

/// A validated object key (FR-006/FR-020).
///
/// Constructed via [`key`], which rejects traversal (`..`),
/// absolute paths, control characters, dot segments, empty interior
/// segments (`a//b`, `a\\b` — a filesystem mirror cannot represent
/// distinct keys that map to one path), and the empty key — before any
/// filesystem access. Folder markers (`dir/`) are legal keys.
/// The reserved `.tinio` segment is *syntactically* valid but flagged by
/// [`Key::is_reserved`]; backends refuse writes to reserved keys with
/// `AccessDenied` (FR-020).
///
/// Deref/AsRef expose the inner string; `"dir/file.txt".into()` builds a
/// key from a trusted literal (panics on invalid input).
///
/// # Examples
///
/// ```rust
/// use tinio_core::{
///     object,
///     storage::{self, Error::*},
/// };
///
/// let k = object::key("dir/file.txt").unwrap();
/// assert_eq!(k.as_ref(), "dir/file.txt");
/// assert!(!k.is_reserved());
/// assert!(!k.is_folder_marker());
/// assert!(object::key("dir/").unwrap().is_folder_marker());
///
/// for bad in ["../evil", "/abs", "a\x00b"] {
///     assert!(matches!(object::key(bad), Err(InvalidKey(_))));
/// }
///
/// let reserved = object::key("a/.tinio/b").unwrap(); // syntactically legal
/// assert!(reserved.is_reserved());
///
/// // Trusted-literal convenience.
/// let from_literal: object::Key = "dir/file.txt".into();
/// assert_eq!(from_literal, k);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Display, Deref, AsRef, Into)]
#[display("{}", _0)]
#[deref(forward)]
pub struct Key(String);

impl Key {
    /// Whether the key contains a reserved `.tinio` segment at any depth
    /// (FR-020).
    pub fn is_reserved(&self) -> bool {
        is_reserved_key(&self.0)
    }

    /// Whether this key is a folder marker (`dir/`): legal to store as a
    /// directory, never an object (get/head report `NoSuchKey`).
    pub fn is_folder_marker(&self) -> bool {
        self.0.ends_with('/')
    }
}

/// Validate and wrap an object key from an untrusted source (FR-006).
pub fn key(key: impl Into<String>) -> Result<Key, storage::Error> {
    let key = key.into();
    if validate_object_key(&key).is_err() {
        return Err(InvalidKey(key));
    }
    Ok(Key(key))
}

impl From<&str> for Key {
    /// Trusted-input convenience (panics on invalid keys — use [`key`] for
    /// untrusted input).
    fn from(key: &str) -> Self {
        self::key(key).expect("valid object key")
    }
}

impl From<String> for Key {
    fn from(key: String) -> Self {
        self::key(key).expect("valid object key")
    }
}

/// Metadata of a stored object (key, size, mtime, ETag).
///
/// ETags are stored without the surrounding quotes S3 headers use: single
/// uploads carry the content MD5 hex, multipart uploads the composed
/// `"<md5hex>-N"` form (FR-022). The quote wrapping is a protocol-layer
/// concern.
///
/// # Examples
///
/// ```rust
/// use std::time::SystemTime;
///
/// use tinio_core::object::Info;
///
/// let info = Info {
///     key: "dir/file.txt".into(),
///     size: 4,
///     last_modified: SystemTime::UNIX_EPOCH,
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
/// };
/// assert_eq!(info.size, 4);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Info {
    /// Object key (validated per FR-006/FR-020).
    pub key: Key,
    /// Object size in bytes.
    pub size: u64,
    /// Last modification time (the filesystem mtime — actual file state).
    pub last_modified: SystemTime,
    /// ETag: content MD5 hex, or the composed `"<md5hex>-N"` for multipart.
    pub etag: ETag,
}

/// Whether a key contains a reserved `.tinio` segment at any depth
/// (FR-020), treating both `/` and Windows' `\` as separators.
pub fn is_reserved_key(key: &str) -> bool {
    key.split(['/', '\\']).any(|seg| seg == RESERVED_SEGMENT)
}

fn validate_object_key(key: &str) -> garde::Result {
    if key.is_empty() {
        return Err(GardeError::new("empty key"));
    }
    if key.starts_with('/') || key.starts_with('\\') {
        // `\` is a path separator on Windows — a backslash-absolute key
        // would map outside the storage root there.
        return Err(GardeError::new(format!("{key:?}: absolute path")));
    }
    if key.len() >= 2 && key.as_bytes()[0].is_ascii_alphabetic() && key.as_bytes()[1] == b':' {
        // Drive-letter absolute/relative path (`C:\foo`, `C:foo`) — escapes
        // the storage root on Windows.
        return Err(GardeError::new(format!("{key:?}: drive-letter path")));
    }
    if key.contains("..") {
        return Err(GardeError::new(format!("{key:?}: traversal sequence")));
    }
    if key.split(['/', '\\']).any(|seg| seg == ".") {
        return Err(GardeError::new(format!("{key:?}: dot segment")));
    }
    // Empty interior segments (`a//b`, `a\\b`, `a/\b`) alias a
    // single-separator key on a filesystem mirror — the mirror cannot
    // represent distinct keys that map to one OS path, so they are
    // refused at the contract boundary (every backend agrees;
    // fs-backend.md). `\` is a separator on Windows, matching the
    // reserved/dot-segment splits above. The trailing empty segment of
    // a folder marker (`dir/`) is the one legal empty segment.
    let segs: Vec<&str> = key.split(['/', '\\']).collect();
    if segs.len() > 1 && segs[..segs.len() - 1].iter().any(|seg| seg.is_empty()) {
        return Err(GardeError::new(format!("{key:?}: empty interior segment")));
    }
    if key.chars().any(|c| c.is_control()) {
        return Err(GardeError::new(format!("{key:?}: control character")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_util::testing::assert_send_sync;

    #[test]
    fn object_key_validates_and_exposes() {
        let k = key("dir/file.txt").unwrap();
        assert_eq!(k.as_ref(), "dir/file.txt");
        assert_eq!(k.to_string(), "dir/file.txt");
        assert_eq!(key("dir/file.txt").unwrap(), k);
        assert_eq!(String::from(k.clone()), "dir/file.txt");
        assert_eq!(&*k, "dir/file.txt");
        assert!(!k.is_folder_marker());
        assert!(key("dir/").unwrap().is_folder_marker());
        assert!(key("").is_err());
    }

    #[test]
    fn object_key_from_trusted_literals() {
        let key: Key = "dir/file.txt".into();
        assert_eq!(key.as_ref(), "dir/file.txt");
        let key2 = Key::from("dir/file.txt".to_string());
        assert_eq!(key, key2);
    }

    #[test]
    #[should_panic]
    fn object_key_from_invalid_panics() {
        let _: Key = "../evil".into();
    }

    #[test]
    fn empty_key_rejected() {
        assert!(validate_object_key("").is_err());
    }

    #[test]
    fn object_info_carries_etag_size_mtime() {
        let t = SystemTime::UNIX_EPOCH;
        let o = Info {
            key: "dir/file.txt".into(),
            size: 5,
            last_modified: t,
            etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
        };
        assert_eq!(o.size, 5);
        assert_eq!(o.last_modified, t);
        assert_eq!(o.etag.as_str(), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn object_types_are_send_sync_and_static() {
        assert_send_sync::<Key>();
        assert_send_sync::<Info>();
    }
}
