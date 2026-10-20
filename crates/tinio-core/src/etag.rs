//! Validated ETag wire-format and content helpers.

use std::{num::ParseIntError, ops::Deref, str::FromStr};

use bytes::Bytes;
use derive_more::{Deref, Display};
use md5::{Digest, Md5};

use crate::multipart::PartInfo;

/// A validated ETag.
///
/// Internally stores the raw 16-byte MD5 digest (not hex). Multipart uploads
/// also record the part count for the composed `MD5-of-MD5s-N` form (FR-022).
/// Wire-format hex encoding is a protocol-layer concern — use [`ETag::as_str`] or
/// [`std::fmt::Display`] when emitting S3 headers.
///
/// Parse untrusted wire input with [`FromStr`]; build from content via
/// [`ETag::from_content`]; from assembled parts via
/// [`ETag::composed_from_parts`].
///
/// # Examples
///
/// ```rust
/// use tinio_core::ETag;
///
/// let single = "d41d8cd98f00b204e9800998ecf8427e".parse::<ETag>().unwrap();
/// assert_eq!(single.as_str(), "d41d8cd98f00b204e9800998ecf8427e");
///
/// let from_content = ETag::from_content(b"");
/// assert_eq!(from_content, ETag::EMPTY);
/// assert_eq!(from_content, single);
///
/// let multipart = "d41d8cd98f00b204e9800998ecf8427e-3"
///     .parse::<ETag>()
///     .unwrap();
/// assert_eq!(multipart.as_str(), "d41d8cd98f00b204e9800998ecf8427e-3");
///
/// assert!("not-a-hex-etag".parse::<ETag>().is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Display, Deref)]
#[display("{}", self.as_str())]
pub enum ETag {
    /// Content MD5 for a single upload.
    Single([u8; 16]),
    /// Composed multipart ETag `MD5-of-MD5s-N`.
    Composed(#[deref] [u8; 16], u32),
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
            // The length/hex test above is the decode's whole precondition,
            // so the only failure left is the format this branch excluded.
            hex::decode_to_slice(bytes, &mut digest).map_err(|_| Error::InvalidFormat)?;
            return Ok(Self::Single(digest));
        }
        if bytes.len() > 33
            && bytes[32] == b'-'
            && bytes[..32].iter().all(u8::is_ascii_hexdigit)
            && bytes[33..].iter().all(u8::is_ascii_digit)
        {
            let mut digest = [0u8; 16];
            hex::decode_to_slice(&bytes[..32], &mut digest).map_err(|_| Error::InvalidFormat)?;
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

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, time::SystemTime};

    use super::*;
    use crate::{_util::testing::assert_send_sync, multipart};

    #[test]
    fn etag_validates_formats() {
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(hex.parse::<ETag>().unwrap().as_str(), hex);
        assert_eq!(
            format!("{hex}-3").parse::<ETag>().unwrap().as_str(),
            format!("{hex}-3")
        );
        assert_eq!(hex.to_uppercase().parse::<ETag>().unwrap().as_str(), hex);
        let from_literal = hex.parse::<ETag>().expect("a valid etag");
        assert_eq!(from_literal.as_str(), hex);
        assert_eq!(from_literal.len(), 16);
        let bytes: Bytes = from_literal.clone().into();
        assert_eq!(bytes.len(), 16);
        assert_eq!(ETag::from_content(b""), ETag::EMPTY);
        assert_eq!(ETag::EMPTY, hex.parse::<ETag>().unwrap());
        assert!("".parse::<ETag>().is_err());
        assert!("short".parse::<ETag>().is_err());
        assert!(format!("{hex}-").parse::<ETag>().is_err());
        assert!(format!("{hex}-x").parse::<ETag>().is_err());
        assert!(format!("zzzz{hex}").parse::<ETag>().is_err());
    }

    #[test]
    fn etag_is_send_sync_and_static() {
        assert_send_sync::<ETag>();
    }

    #[test]
    fn etag_composed_deref_and_edge_cases() {
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        let single = hex.parse::<ETag>().unwrap();
        let composed = format!("{hex}-3").parse::<ETag>().unwrap();
        // Deref/AsRef on the composed variant yields the same raw digest.
        assert_eq!(single.as_ref(), composed.as_ref());
        assert_eq!(composed.len(), 16);
        // `-0` part count is not a valid multipart ETag.
        assert!(format!("{hex}-0").parse::<ETag>().is_err());
        // An empty part list cannot compose an ETag.
        assert!(ETag::composed_from_parts(&[]).is_none());
    }

    #[test]
    fn etag_composes_parts_and_rejects_an_overflowing_part_count() {
        // The AWS composition: the MD5 over the RAW 16-byte part digests
        // concatenated, then `-N`. The vector is the published one for
        // ("hello", "world") — joining the part etags as HEX strings
        // would compose a different digest and break every client that
        // re-verifies the multipart etag.
        let parts = [
            PartInfo {
                part_number: multipart::part_number(1).expect("a valid part number"),
                size: 5,
                etag: ETag::from_content(b"hello"),
                last_modified: SystemTime::UNIX_EPOCH,
                checksum: None,
            },
            PartInfo {
                part_number: multipart::part_number(2).expect("a valid part number"),
                size: 5,
                etag: ETag::from_content(b"world"),
                last_modified: SystemTime::UNIX_EPOCH,
                checksum: None,
            },
        ];
        let composed = ETag::composed_from_parts(&parts).expect("two parts compose");
        assert_eq!(composed.as_str(), "065947336a2f2a95ba8899f3675c3be6-2");
        // The suffix counts PARTS (the composed shape carries the count
        // as a field), and the emitted form is what a client echoes back
        // — it must re-parse to the same value.
        assert_eq!(
            composed,
            "065947336a2f2a95ba8899f3675c3be6-2"
                .parse::<ETag>()
                .unwrap()
        );

        // A suffix that overflows `u32` is the only failing parse: the
        // format checks reject non-digits and `-0` first, so the count
        // parse is the single caller of `Error::PartCount` — it must
        // stay that error and never degrade to `InvalidFormat` (the S3
        // layer answers a different code for each).
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        let overflowing = format!("{hex}-{}", u64::from(u32::MAX) + 1);
        let err = overflowing.parse::<ETag>().unwrap_err();
        assert_eq!(err.to_string(), "invalid ETag part count");
        assert!(err.source().is_some(), "the parse error is its source");
    }
}
