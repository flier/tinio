//! S3 checksum types shared across the storage contract (spec
//! 2026-08-31-multipart-checksum-validation-design.md).
//!
//! Plain value types only — no hashing, no wire encoding beyond the
//! algorithm/type names. All checksum computation lives in tinio-server;
//! the backends store and return these values untouched.

use std::sync::OnceLock;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use derive_more::{AsRef, Deref, Into};
use parse_display::{Display, FromStr};

use crate::storage::{self, invalid_checksum};

/// The S3 checksum algorithms tinio validates: every `ChecksumAlgorithm`
/// wire value of the API model (`x-amz-checksum-*`), plus MD5 (the
/// legacy `Content-MD5` shares the MD5 slot).
///
/// # Examples
///
/// ```rust
/// use tinio_core::checksum;
///
/// assert_eq!(checksum::Algorithm::Crc32.to_string(), "CRC32");
/// assert_eq!("SHA512".parse(), Ok(checksum::Algorithm::Sha512));
/// assert_eq!("XXHASH64".parse(), Ok(checksum::Algorithm::XxHash64));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, FromStr)]
#[display(style = "UPPERCASE")]
pub enum Algorithm {
    /// CRC-32 (ISO-HDLC), `x-amz-checksum-crc32`.
    Crc32,
    /// CRC-32C (Castagnoli), `x-amz-checksum-crc32c`.
    Crc32C,
    /// CRC-64/NVMe, `x-amz-checksum-crc64nvme`.
    Crc64Nvme,
    /// SHA-1, `x-amz-checksum-sha1`.
    Sha1,
    /// SHA-256, `x-amz-checksum-sha256`.
    Sha256,
    /// SHA-512, `x-amz-checksum-sha512`.
    Sha512,
    /// MD5, `x-amz-checksum-md5` / `Content-MD5`.
    Md5,
    /// XXHASH64, `x-amz-checksum-xxhash64`.
    XxHash64,
    /// XXHASH3, `x-amz-checksum-xxhash3`.
    XxHash3,
    /// XXHASH128, `x-amz-checksum-xxhash128`.
    XxHash128,
}

impl Algorithm {
    /// All supported algorithms.
    pub const ALL: [Algorithm; 10] = [
        Self::Crc32,
        Self::Crc32C,
        Self::Crc64Nvme,
        Self::Sha1,
        Self::Sha256,
        Self::Sha512,
        Self::Md5,
        Self::XxHash64,
        Self::XxHash3,
        Self::XxHash128,
    ];

    /// The `&'static str` wire name (`"CRC32"` … `"XXHASH128"`) — the
    /// allocation-free form for the backends' persisted rows.
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32C => "CRC32C",
            Self::Crc64Nvme => "CRC64NVME",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Sha512 => "SHA512",
            Self::Md5 => "MD5",
            Self::XxHash64 => "XXHASH64",
            Self::XxHash3 => "XXHASH3",
            Self::XxHash128 => "XXHASH128",
        }
    }
}

/// How a multipart full-object checksum is derived from the parts.
///
/// # Examples
///
/// ```rust
/// use tinio_core::checksum;
///
/// assert_eq!(checksum::Type::Composite.to_string(), "COMPOSITE");
/// assert_eq!("FULL_OBJECT".parse(), Ok(checksum::Type::FullObject));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, FromStr)]
#[display(style = "SNAKE_CASE")]
pub enum Type {
    /// The algorithm over the concatenation of the raw part digests.
    Composite,
    /// The CRC of the whole content, linearized from the part CRCs.
    FullObject,
}

impl Type {
    /// The algorithm × type validity table (spec 2026-08-31): which
    /// algorithms a full-object derivation accepts — COMPOSITE is every
    /// algorithm but CRC64NVME (which is always FULL_OBJECT), FULL_OBJECT
    /// is the CRC family. One home for the table — the create and
    /// complete validations share it.
    pub fn supports(self, algorithm: Algorithm) -> bool {
        match self {
            Type::Composite => matches!(
                algorithm,
                Algorithm::Crc32
                    | Algorithm::Crc32C
                    | Algorithm::Sha1
                    | Algorithm::Sha256
                    | Algorithm::Sha512
                    | Algorithm::Md5
                    | Algorithm::XxHash64
                    | Algorithm::XxHash3
                    | Algorithm::XxHash128
            ),
            Type::FullObject => matches!(
                algorithm,
                Algorithm::Crc32 | Algorithm::Crc32C | Algorithm::Crc64Nvme
            ),
        }
    }
}

/// Parse a persisted algorithm wire name.
pub fn algorithm(s: &str) -> Result<Algorithm, storage::Error> {
    s.parse().map_err(|_| invalid_checksum(s))
}

/// Parse a persisted type wire name. Empty is unset (create did not fix a type).
pub fn stored_type(s: &str) -> Result<Option<Type>, storage::Error> {
    if s.is_empty() {
        Ok(None)
    } else {
        s.parse().map_err(|_| invalid_checksum(s)).map(Some)
    }
}

/// A checksum value in the S3 wire format (base64 of the raw digest).
#[derive(Debug, Clone, PartialEq, Eq, Display, Deref, AsRef, Into)]
#[display("{0}")]
pub struct Value(pub String);

impl Value {
    /// The raw digest bytes (the base64 wire form decoded).
    pub fn raw(&self) -> Option<Vec<u8>> {
        STANDARD.decode(&self.0).ok()
    }

    /// The raw 16-byte MD5 digest — `None` when the value is not a
    /// valid MD5 wire form (the tee's MD5 slot doubles as a part ETag,
    /// and both backends decode it the same way).
    pub fn md5_raw(&self) -> Option<[u8; 16]> {
        self.raw()?.try_into().ok()
    }
}

/// One part's stored checksum: the algorithm and its base64 value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Part {
    /// The algorithm the value was computed with.
    pub algorithm: Algorithm,
    /// The base64-encoded digest.
    pub value: Value,
}

/// The server-computed digest of a part body: the verify tee fills
/// [`Self::digest`] at stream end, and the backend reads it at commit
/// time so the part row and its checksum row commit in ONE transaction
/// (the two-phase `set_part_checksum` CAS is gone). `etag_md5` tells
/// the backend the tee computed MD5 over the body — a part's ETag IS
/// its content MD5, so the backend uses the slot value as the ETag
/// instead of hashing the bytes a second time.
#[derive(Debug, Default)]
pub struct PartChecksum {
    /// The digest (set once at stream end; empty when the part carried
    /// no checksum algorithm).
    pub digest: OnceLock<Part>,
    /// The tee computed MD5 — the digest may serve as the part ETag.
    pub etag_md5: bool,
}

impl Part {
    /// Parse a persisted part-checksum row: the algorithm wire name and
    /// the base64 value (the backends store the wire strings untouched).
    pub fn from_wire(algo: &str, value: String) -> Result<Self, storage::Error> {
        Ok(Self {
            algorithm: algorithm(algo)?,
            value: Value(value),
        })
    }
}

/// The upload-level checksum specification of
/// `CreateMultipartUpload` (`x-amz-checksum-algorithm` +
/// `x-amz-checksum-type`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Upload {
    /// The algorithm every part and the full object are computed with.
    pub algorithm: Algorithm,
    /// The full-object derivation, when the client fixed one at create.
    pub r#type: Option<Type>,
}

impl Upload {
    /// The persisted row of the upload-level spec: `(algorithm, type)`
    /// wire names (`""` marks a type that was never fixed).
    pub fn to_wire(&self) -> (String, String) {
        (
            self.algorithm.to_string(),
            self.r#type.map_or_else(String::new, |t| t.to_string()),
        )
    }

    /// Parse a persisted upload-spec row (empty type = unfixed).
    pub fn from_wire(algo: &str, ty: &str) -> Result<Self, storage::Error> {
        Ok(Self {
            algorithm: algorithm(algo)?,
            r#type: stored_type(ty)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn algorithm_wire_names_round_trip() {
        for algo in Algorithm::ALL {
            assert_eq!(algo.to_string().parse(), Ok(algo));
        }
        assert!(Algorithm::from_str("BLAKE3").is_err());
        assert!(Algorithm::from_str("crc32").is_err()); // case-sensitive
    }

    #[test]
    fn checksum_type_wire_names() {
        assert_eq!(Type::from_str("COMPOSITE"), Ok(Type::Composite));
        assert_eq!(Type::from_str("FULL_OBJECT"), Ok(Type::FullObject));
        assert!(Type::from_str("composite").is_err());
        assert_eq!(Type::Composite.to_string(), "COMPOSITE");
        assert_eq!(Type::FullObject.to_string(), "FULL_OBJECT");
    }

    #[test]
    fn persisted_wire_names_are_storage_errors() {
        assert!(matches!(
            algorithm("BLAKE3"),
            Err(storage::Error::InvalidChecksum(_))
        ));
        assert_eq!(stored_type("").unwrap(), None);
        assert_eq!(stored_type("FULL_OBJECT").unwrap(), Some(Type::FullObject));
        assert!(matches!(
            stored_type("composite"),
            Err(storage::Error::InvalidChecksum(_))
        ));
    }
}
