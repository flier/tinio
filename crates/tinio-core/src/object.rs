//! Object key validation and object metadata.

use std::{collections::BTreeMap, time::SystemTime};

use derive_more::{AsRef, Deref, Display, Into};
use garde::Error as GardeError;
use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

use crate::{
    ETag, checksum,
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

/// Metadata of a stored object (key, size, mtime, ETag, tags, checksum).
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
/// use tinio_core::object::{Info, Tags};
///
/// let info = Info {
///     key: "dir/file.txt".into(),
///     size: 4,
///     last_modified: SystemTime::UNIX_EPOCH,
///     etag: "d41d8cd98f00b204e9800998ecf8427e".into(),
///     tags: Tags::empty(),
///     checksum: None,
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
    /// Object tags (empty when none).
    pub tags: Tags,
    /// The recorded object checksum (validated at write time under the
    /// `checksum` toggle; `None` when the object has none).
    pub checksum: Option<checksum::Recorded>,
}

/// The per-surface tag-count caps (S3): object tags ≤ 10, bucket tags
/// ≤ 50. `from_pairs`/`parse_wire` apply the object cap; bucket tagging
/// (and the fs/mem row reads) call the `_limited` forms with
/// `BUCKET_TAGS_MAX`.
pub const OBJECT_TAGS_MAX: usize = 10;
pub const BUCKET_TAGS_MAX: usize = 50;

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
    /// with the object-tag count cap ([`OBJECT_TAGS_MAX`]). Bucket
    /// tagging uses `from_pairs_limited(pairs, BUCKET_TAGS_MAX)`.
    pub fn from_pairs(pairs: impl IntoIterator<Item = (String, String)>) -> Result<Self, TagError> {
        Self::from_pairs_limited(pairs, OBJECT_TAGS_MAX)
    }

    /// The same validation with an explicit count cap (the
    /// [`OBJECT_TAGS_MAX`] object / [`BUCKET_TAGS_MAX`] bucket caps —
    /// AWS-verified per-surface limits).
    pub fn from_pairs_limited(
        pairs: impl IntoIterator<Item = (String, String)>,
        limit: usize,
    ) -> Result<Self, TagError> {
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            if map.len() >= limit {
                return Err(TagError::TooMany {
                    count: map.len() + 1,
                    limit,
                });
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
    /// `+` is a literal plus) under the object cap — see
    /// [`Tags::parse_wire_limited`]. Malformed input → error.
    pub fn parse_wire(input: &str) -> Result<Self, TagError> {
        Self::parse_wire_limited(input, OBJECT_TAGS_MAX)
    }

    /// The same wire parse with an explicit count cap — the fs/mem
    /// disk-read parse (the object cap of [`Tags::parse_wire`] cannot
    /// read a 50-tag bucket row): decode + validate via
    /// [`Tags::from_pairs_limited`]. Malformed input → error; the read
    /// side maps errors to the empty set — rows are API-written, so a
    /// garbage wire self-heals to missing, exactly like the
    /// invalid-etag rows.
    pub fn parse_wire_limited(input: &str, limit: usize) -> Result<Self, TagError> {
        let mut pairs = Vec::new();
        for pair in input.split('&') {
            if pair.is_empty() {
                continue;
            }
            let Some((k, v)) = pair.split_once('=') else {
                return Err(TagError::InvalidKey {
                    key: pair.to_string(),
                });
            };
            pairs.push((percent_decode(k), percent_decode(v)));
        }
        Self::from_pairs_limited(pairs, limit)
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

/// The allowed tag charset — Unicode letters, numbers, and separators
/// plus `+ - = . _ : / @` (the S3 Control regex `[\p{L}\p{Z}\p{N}_.:/=+\-@]`,
/// a superset of the EC2 cross-service ASCII restriction). Matched by
/// Unicode general category: std's `is_alphanumeric`/`is_whitespace`
/// straddle category boundaries (Other_Alphabetic marks, control
/// whitespace) and would admit or drop characters the regex does not.
fn valid_tag_part(s: &str) -> bool {
    s.chars()
        .all(|c| in_lzn(c) || matches!(c, '+' | '-' | '=' | '.' | '_' | ':' | '/' | '@'))
}

/// Whether `c`'s general category falls in the locked regex's
/// `\p{L}` ∪ `\p{Z}` ∪ `\p{N}` groups (UAX #44 letters, separators,
/// numbers).
fn in_lzn(c: char) -> bool {
    use GeneralCategory as Gc;
    matches!(
        c.general_category(),
        Gc::UppercaseLetter
            | Gc::LowercaseLetter
            | Gc::TitlecaseLetter
            | Gc::ModifierLetter
            | Gc::OtherLetter
            | Gc::SpaceSeparator
            | Gc::LineSeparator
            | Gc::ParagraphSeparator
            | Gc::DecimalNumber
            | Gc::LetterNumber
            | Gc::OtherNumber
    )
}

/// A tag key: 1..=128 UTF-16 units (AWS counts UTF-16 positions).
fn valid_key(s: &str) -> bool {
    let units = s.encode_utf16().count();
    (1..=128).contains(&units) && valid_tag_part(s)
}

/// A tag value: 0..=256 UTF-16 units (empty values are legal).
fn valid_value(s: &str) -> bool {
    s.encode_utf16().count() <= 256 && valid_tag_part(s)
}

/// The hex digits of the wire `%XX` encoding (percent_encode's
/// per-byte lookup).
const HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encode the wire-reserved characters (`%`, `=`, `&`, `+`,
/// space). Everything else — the Unicode charset included — passes
/// through untouched.
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '%' | '=' | '&' | '+' | ' ' => {
                out.push('%');
                out.push(HEX_DIGITS[(c as u32 >> 4) as usize] as char);
                out.push(HEX_DIGITS[(c as u32 & 0xF) as usize] as char);
            }
            c => out.push(c),
        }
    }
    out
}

/// Percent-decode `%XX` sequences (`+` stays literal). The two hex
/// bytes are read as raw bytes — never sliced out of the `&str` — so a
/// `%` followed by a raw non-ASCII char cannot hit a mid-char boundary
/// and panic: the bytes fail UTF-8 or hex validation and the `%` passes
/// through, leaving the charset check in `from_pairs` to reject the
/// input.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(byte) = hex {
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
    #[error("tag count {count} exceeds the maximum of {limit}")]
    TooMany { count: usize, limit: usize },
    #[error("invalid tag key {key:?}")]
    InvalidKey { key: String },
    #[error("invalid tag value {value:?}")]
    InvalidValue { value: String },
    #[error("duplicate tag key {key:?}")]
    Duplicate { key: String },
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
            tags: Tags::empty(),
            checksum: None,
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

    #[test]
    fn tags_validate_count_length_and_charset() {
        // The S3 limits: ≤10 object tags (50 for buckets), key 1..=128 /
        // value 0..=256 UTF-16 units, Unicode letters/digits/space plus
        // + - = . _ : / @ (the S3 Control regex charset).
        assert!(Tags::from_pairs([("a".into(), "1".into())]).is_ok());
        let too_many: Vec<(String, String)> =
            (0..11).map(|i| (format!("k{i}"), "v".into())).collect();
        assert_eq!(
            Tags::from_pairs(too_many).unwrap_err(),
            TagError::TooMany {
                count: 11,
                limit: 10
            }
        );
        // The bucket cap is 50 — `from_pairs_limited` reports the
        // surface's own limit, never the object cap of 10.
        let fifty: Vec<(String, String)> = (0..50).map(|i| (format!("k{i}"), "v".into())).collect();
        assert!(Tags::from_pairs_limited(fifty, BUCKET_TAGS_MAX).is_ok());
        let fifty_one: Vec<(String, String)> =
            (0..51).map(|i| (format!("k{i}"), "v".into())).collect();
        assert_eq!(
            Tags::from_pairs_limited(fifty_one, BUCKET_TAGS_MAX).unwrap_err(),
            TagError::TooMany {
                count: 51,
                limit: 50
            }
        );
        assert_eq!(
            Tags::from_pairs([("".into(), "v".into())]).unwrap_err(),
            TagError::InvalidKey { key: "".into() }
        );
        assert_eq!(
            Tags::from_pairs([("k".into(), "v".repeat(257))]).unwrap_err(),
            TagError::InvalidValue {
                value: "v".repeat(257)
            }
        );
        // Empty values are legal (AWS: value min length 0).
        assert!(Tags::from_pairs([("k".into(), String::new())]).is_ok());
        // Unicode letters are legal (AWS tags are Unicode, not ASCII-only).
        assert!(Tags::from_pairs([("键".into(), "值".into())]).is_ok());
        // The whole `\p{Z}` separator class is legal — not just the
        // literal space: NBSP (Zs) and the line separator (Zl) pass,
        // exactly as the locked regex reads.
        assert!(Tags::from_pairs([("k".into(), "\u{a0}x".into())]).is_ok());
        assert!(Tags::from_pairs([("k".into(), "x\u{2028}".into())]).is_ok());
        // `\p{N}` covers the other-number classes (² is No, not Nd).
        assert!(Tags::from_pairs([("k".into(), "²".into())]).is_ok());
        // Characters OUTSIDE the general categories stay rejected even
        // when std's alphabetic/numeric approximations would admit them:
        // Other_Alphabetic marks (U+0345, a non-letter) and control
        // whitespace (tab is White_Space but not `\p{Z}`).
        assert_eq!(
            Tags::from_pairs([("k".into(), "x\u{0345}".into())]).unwrap_err(),
            TagError::InvalidValue {
                value: "x\u{0345}".into()
            }
        );
        assert_eq!(
            Tags::from_pairs([("k".into(), "x\ty".into())]).unwrap_err(),
            TagError::InvalidValue {
                value: "x\ty".into()
            }
        );
        // A character outside the allowed set is rejected.
        assert_eq!(
            Tags::from_pairs([("k".into(), "v&bad".into())]).unwrap_err(),
            TagError::InvalidValue {
                value: "v&bad".into()
            }
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
            ("amp".into(), "x+y".into()),
        ])
        .unwrap();
        // Sorted by key; `=` and `+` percent-encoded in values.
        assert_eq!(tags.to_wire(), "a=1&amp=x%2By&b=2&eq=a%3Db");
        assert_eq!(Tags::parse_wire(&tags.to_wire()).unwrap(), tags);
        // Malformed input is rejected.
        assert!(Tags::parse_wire("k=v&k2").is_err());
        assert!(Tags::parse_wire("k%zz=v").is_err());
        // A `%` followed by a raw non-ASCII char never panics (the hex
        // read must not slice mid-char) — the `%` passes through and the
        // charset check rejects the value.
        assert!(Tags::parse_wire("k=%键").is_err());
        // Percent-encoded allowed chars decode.
        let tags = Tags::parse_wire("a=%2Bb").unwrap();
        assert_eq!(tags.iter().collect::<Vec<_>>(), [("a", "+b")]);
        // Percent-encoded UTF-8 decodes (Unicode tags).
        let tags = Tags::parse_wire("k=%E9%94%AE").unwrap();
        assert_eq!(tags.iter().collect::<Vec<_>>(), [("k", "键")]);
        // Unicode tags round-trip through the wire form.
        let tags = Tags::from_pairs([("键".into(), "值".into())]).unwrap();
        assert_eq!(Tags::parse_wire(&tags.to_wire()).unwrap(), tags);
        // The cap-parameterized parse reads a bucket-size wire under
        // `BUCKET_TAGS_MAX` and rejects the same wire under the object
        // cap — the fs/mem row-read discipline.
        let many = (0..20)
            .map(|i| format!("k{i}=v"))
            .collect::<Vec<_>>()
            .join("&");
        assert_eq!(
            Tags::parse_wire_limited(&many, BUCKET_TAGS_MAX)
                .unwrap()
                .len(),
            20
        );
        assert!(Tags::parse_wire_limited(&many, OBJECT_TAGS_MAX).is_err());
    }
}
