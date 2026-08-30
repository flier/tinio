//! Validated ETag wire-format and content helpers.

use std::{num::ParseIntError, ops::Deref, str::FromStr};

use bytes::Bytes;
use derive_more::Display;
use md5::{Digest, Md5};

use crate::{multipart::PartInfo, storage};

/// A validated ETag.
///
/// Internally stores the raw 16-byte MD5 digest (not hex). Multipart uploads
/// also record the part count for the composed `MD5-of-MD5s-N` form (FR-022).
/// Wire-format hex encoding is a protocol-layer concern — use [`ETag::as_str`] or
/// [`std::fmt::Display`] when emitting S3 headers.
///
/// Construct from untrusted wire input via [`ETag::new`]; from content via
/// [`ETag::from_content`]; from assembled parts via
/// [`ETag::composed_from_parts`].
///
/// # Examples
///
/// ```rust
/// use tinio_core::ETag;
///
/// let single = ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap();
/// assert_eq!(single.as_str(), "d41d8cd98f00b204e9800998ecf8427e");
///
/// let from_content = ETag::from_content(b"");
/// assert_eq!(from_content, ETag::EMPTY);
/// assert_eq!(from_content, single);
///
/// let multipart = ETag::new("d41d8cd98f00b204e9800998ecf8427e-3").unwrap();
/// assert_eq!(multipart.as_str(), "d41d8cd98f00b204e9800998ecf8427e-3");
///
/// assert!(ETag::new("not-a-hex-etag").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Display)]
#[display("{}", self.as_str())]
pub enum ETag {
    /// Content MD5 for a single upload.
    Single([u8; 16]),
    /// Composed multipart ETag `MD5-of-MD5s-N`.
    Composed([u8; 16], u32),
}

/// Wire-format [`ETag`] parse failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wire format is neither single nor composed multipart.
    #[error("invalid ETag format")]
    InvalidFormat,
    /// Multipart part-count suffix is not a valid integer.
    #[error("invalid ETag part count")]
    PartCount {
        #[source]
        source: ParseIntError,
    },
}

impl FromStr for ETag {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = s.as_bytes();
        if bytes.len() == 32 && bytes.iter().all(u8::is_ascii_hexdigit) {
            let mut digest = [0u8; 16];
            hex::decode_to_slice(bytes, &mut digest).expect("32 hex digits decode to 16 bytes");
            return Ok(Self::Single(digest));
        }
        if bytes.len() > 33
            && bytes[32] == b'-'
            && bytes[..32].iter().all(u8::is_ascii_hexdigit)
            && bytes[33..].iter().all(u8::is_ascii_digit)
        {
            let mut digest = [0u8; 16];
            hex::decode_to_slice(&bytes[..32], &mut digest)
                .expect("32 hex digits decode to 16 bytes");
            let parts = s[33..]
                .parse()
                .map_err(|source| Error::PartCount { source })?;
            if parts == 0 {
                return Err(Error::InvalidFormat);
            }
            return Ok(Self::Composed(digest, parts));
        }
        Err(Error::InvalidFormat)
    }
}

impl Deref for ETag {
    type Target = [u8; 16];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Single(digest) | Self::Composed(digest, _) => digest,
        }
    }
}

impl AsRef<[u8]> for ETag {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl ETag {
    /// ETag of empty content (`MD5("")` = `d41d8cd98f00b204e9800998ecf8427e`).
    pub const EMPTY: Self = Self::Single([
        0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8, 0x42,
        0x7e,
    ]);

    fn md5(data: &[u8]) -> [u8; 16] {
        let mut hasher = Md5::new();
        hasher.update(data);
        hasher.finalize().into()
    }

    /// Validate wire-format input and store the raw digest (plus multipart
    /// suffix when present).
    pub fn new(etag: &str) -> Result<Self, storage::Error> {
        Ok(etag.parse()?)
    }

    /// Content MD5 for a single upload (16 raw bytes).
    pub fn from_content(data: &[u8]) -> Self {
        Self::Single(Self::md5(data))
    }

    /// Composed multipart ETag `MD5-of-MD5s-N` (raw digest + `-N` suffix).
    ///
    /// Follows the AWS composition: the MD5 of the *raw* 16-byte part
    /// digests concatenated, then `-N` (hex-string joins are not
    /// interoperable with real S3 clients).
    ///
    /// Returns `None` on an empty part list — a multipart upload must have
    /// at least one part.
    pub fn composed_from_parts(parts: &[PartInfo]) -> Option<Self> {
        if parts.is_empty() {
            return None;
        }
        let mut joined = Vec::with_capacity(parts.len() * 16);
        for p in parts {
            joined.extend_from_slice(p.etag.deref());
        }
        Some(Self::Composed(Self::md5(&joined), parts.len() as u32))
    }

    /// Wire-format hex (plus `-N` suffix for multipart).
    pub fn as_str(&self) -> String {
        match self {
            Self::Single(digest) => hex::encode(digest),
            Self::Composed(digest, parts) => format!("{}-{parts}", hex::encode(digest)),
        }
    }
}

impl From<ETag> for Bytes {
    fn from(etag: ETag) -> Self {
        Bytes::copy_from_slice(etag.deref())
    }
}

impl From<&str> for ETag {
    /// Trusted-input convenience (panics on invalid ETags — use
    /// [`ETag::new`] for untrusted input).
    fn from(etag: &str) -> Self {
        etag.parse().expect("valid etag")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::_util::testing::assert_send_sync;

    #[test]
    fn etag_validates_formats() {
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(ETag::new(hex).unwrap().as_str(), hex);
        assert_eq!(
            ETag::new(&format!("{hex}-3")).unwrap().as_str(),
            format!("{hex}-3")
        );
        assert_eq!(ETag::new(&hex.to_uppercase()).unwrap().as_str(), hex);
        let from_literal: ETag = hex.into();
        assert_eq!(from_literal.as_str(), hex);
        assert_eq!(from_literal.len(), 16);
        let bytes: Bytes = from_literal.clone().into();
        assert_eq!(bytes.len(), 16);
        assert_eq!(ETag::from_content(b""), ETag::EMPTY);
        assert_eq!(ETag::EMPTY, ETag::new(hex).unwrap());
        assert!(ETag::new("").is_err());
        assert!(ETag::new("short").is_err());
        assert!(ETag::new(&format!("{hex}-")).is_err());
        assert!(ETag::new(&format!("{hex}-x")).is_err());
        assert!(ETag::new(&format!("zzzz{hex}")).is_err());
    }

    #[test]
    fn etag_is_send_sync_and_static() {
        assert_send_sync::<ETag>();
    }

    #[test]
    fn etag_composed_deref_and_edge_cases() {
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        let single = ETag::new(hex).unwrap();
        let composed = ETag::new(&format!("{hex}-3")).unwrap();
        // Deref/AsRef on the composed variant yields the same raw digest.
        assert_eq!(single.as_ref(), composed.as_ref());
        assert_eq!(composed.len(), 16);
        // `-0` part count is not a valid multipart ETag.
        assert!(ETag::new(&format!("{hex}-0")).is_err());
        // An empty part list cannot compose an ETag.
        assert!(ETag::composed_from_parts(&[]).is_none());
    }
}
