//! The one parse authority for `x-amz-copy-source` / `x-amz-rename-source`.

use s3s::dto::CopySource;

use crate::_core::{bucket, object};

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
    Some((
        bucket::name(b.as_ref()).ok()?,
        object::key(key.as_ref()).ok()?,
    ))
}

/// The source key of the request's own bucket (RenameObject).
pub fn rename_source(raw: &str) -> Option<object::Key> {
    object::key(raw.strip_prefix('/').unwrap_or(raw)).ok()
}

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
    fn copy_source_decodes_each_encoded_spelling() {
        // The concrete `(bucket, key)` per encoded spelling, not a comparison
        // against `copy_source_of` — `copy_source` is *defined* as that same
        // mapping, so the two sides never diverge on anything but parse
        // nondeterminism. A change in the percent-decoding (s3s's
        // `urlencoding::decode`) or in the split rule (the first `/`) reddens
        // the literal it touches.
        //
        // `+` is pinned as a literal plus, not a space: s3s 0.17's parse
        // calls `urlencoding::decode` (2.1.3), which decodes `%XX` only and
        // documents that an unencoded `+` is preserved — so this is decidable
        // from the shipped dependency, not guessed.
        for (raw, want) in [
            ("/data/a%20b.txt", ("data", "a b.txt")),
            ("/data/a+b.txt", ("data", "a+b.txt")),
            ("/data/%E2%9C%93.txt", ("data", "✓.txt")),
        ] {
            let (b, k) = copy_source(raw).unwrap_or_else(|| panic!("{raw}: unparsed"));
            assert_eq!((&*b, &*k), want, "{raw}");
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
        let dto = CopySource::parse("/data/a.txt?versionId=3").expect("dto");
        let (b, k) = copy_source_of(&dto).expect("mapped");
        assert_eq!((&*b, &*k), ("data", "a.txt"));
    }
}
