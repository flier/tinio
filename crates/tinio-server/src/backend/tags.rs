//! Tagging wire helpers: the `x-amz-tagging` header and the dto TagSet
//! ↔ core `Tags` conversions. The core type validates; these map the
//! wire forms onto it (InvalidTag on any violation).

use s3s::{S3Error, dto, s3_error};

use crate::_core::object::Tags;

/// Parse the `x-amz-tagging` value (URL-encoded `k=v&k2=v2`). s3s
/// surfaces the header as the dto field `tagging: Option<TaggingHeader>`
/// on Put/Copy/CreateMultipartUpload (generated.rs:19376/2806/4063) —
/// NOT a raw header — so the ops pass that field in.
pub(crate) fn parse_tagging_header(
    tagging: Option<&dto::TaggingHeader>,
) -> Result<Option<Tags>, S3Error> {
    let Some(value) = tagging else {
        return Ok(None);
    };
    let text = value.as_str();
    Tags::parse_wire(text)
        .map(Some)
        .map_err(|e| s3_error!(InvalidTag, "{e}"))
}

/// A dto `TagSet` (Put*Tagging body) into core `Tags`. The count cap
/// is per surface: 10 for object tagging, 50 for bucket tagging
/// (AWS-verified per-surface limits).
pub(crate) fn tags_from_tag_set(tag_set: &[dto::Tag], limit: usize) -> Result<Tags, S3Error> {
    let pairs = tag_set.iter().map(|t| {
        let key = t.key.as_ref().map(|k| k.to_string()).unwrap_or_default();
        let value = t.value.as_ref().map(|v| v.to_string()).unwrap_or_default();
        (key, value)
    });
    Tags::from_pairs_limited(pairs, limit).map_err(|e| s3_error!(InvalidTag, "{e}"))
}

/// Core `Tags` into a dto `TagSet` (GetObjectTagging output).
pub(crate) fn tag_set_from_tags(tags: &Tags) -> Vec<dto::Tag> {
    tags.iter()
        .map(|(k, v)| dto::Tag {
            key: Some(k.into()),
            value: Some(v.into()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use s3s::S3Error;

    use super::*;

    fn tag(key: &str, value: &str) -> dto::Tag {
        dto::Tag {
            key: Some(key.into()),
            value: Some(value.into()),
        }
    }

    fn invalid_tag(err: S3Error) -> bool {
        err.code() == &s3s::S3ErrorCode::InvalidTag
    }

    #[test]
    fn parse_tagging_header_none_and_valid() {
        assert!(matches!(parse_tagging_header(None), Ok(None)));
        let header = dto::TaggingHeader::from("a=1&b=2");
        let tags = parse_tagging_header(Some(&header)).unwrap().unwrap();
        assert_eq!(tags.iter().collect::<Vec<_>>(), [("a", "1"), ("b", "2")]);
    }

    #[test]
    fn parse_tagging_header_rejects_malformed_and_overflow() {
        // A missing `=` (no separator) is not a `k=v` pair.
        let bad = dto::TaggingHeader::from("no-equals");
        assert!(invalid_tag(parse_tagging_header(Some(&bad)).unwrap_err()));
        // A garbage percent-escape is a malformed value.
        let bad = dto::TaggingHeader::from("k=%zz&");
        assert!(invalid_tag(parse_tagging_header(Some(&bad)).unwrap_err()));
        // Over the object cap (10) -> TooMany -> InvalidTag.
        let too_many = (0..11)
            .map(|i| format!("k{i}=v"))
            .collect::<Vec<_>>()
            .join("&");
        let bad = dto::TaggingHeader::from(too_many.as_str());
        assert!(invalid_tag(parse_tagging_header(Some(&bad)).unwrap_err()));
    }

    #[test]
    fn tags_from_tag_set_applies_the_surface_cap() {
        // The object cap (10) is the default boundary; the bucket cap (50)
        // only applies when the caller passes BUCKET_TAGS_MAX.
        let obj: Vec<dto::Tag> = (0..10).map(|i| tag(&format!("k{i}"), "v")).collect();
        assert!(tags_from_tag_set(&obj, 10).is_ok());

        let eleven: Vec<dto::Tag> = (0..11).map(|i| tag(&format!("k{i}"), "v")).collect();
        assert!(invalid_tag(tags_from_tag_set(&eleven, 10).unwrap_err()));
        let eleven_ok = tags_from_tag_set(&eleven, 50).unwrap();
        assert_eq!(eleven_ok.len(), 11);

        // A duplicate key is rejected.
        let dup = vec![tag("a", "1"), tag("a", "2")];
        assert!(invalid_tag(tags_from_tag_set(&dup, 10).unwrap_err()));

        // A missing value serializes to empty — legal (AWS: value min
        // length 0); a missing key serializes to empty — NOT a legal key.
        let empty_value = vec![dto::Tag {
            key: Some("k".into()),
            value: None,
        }];
        let tags = tags_from_tag_set(&empty_value, 10).unwrap();
        assert_eq!(tags.iter().collect::<Vec<_>>(), [("k", "")]);
        let empty_key = vec![dto::Tag {
            key: None,
            value: None,
        }];
        assert!(invalid_tag(tags_from_tag_set(&empty_key, 10).unwrap_err()));
    }

    #[test]
    fn tag_set_from_tags_round_trips() {
        let tags = Tags::from_pairs([("b".into(), "2".into()), ("a".into(), "1".into())]).unwrap();
        let set = tag_set_from_tags(&tags);
        assert_eq!(set.len(), 2);
        assert_eq!(set[0].key.as_deref(), Some("a"));
        assert_eq!(set[0].value.as_deref(), Some("1"));
        assert_eq!(set[1].key.as_deref(), Some("b"));

        // The empty tag set round-trips to an empty TagSet.
        assert!(tag_set_from_tags(&Tags::empty()).is_empty());
    }

    #[test]
    fn tag_set_from_tags_round_trips_through_from_tag_set() {
        // tags -> dto::TagSet -> core Tags preserves the set.
        let original = Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
        let set = tag_set_from_tags(&original);
        let back = tags_from_tag_set(&set, 10).unwrap();
        assert_eq!(back, original);
    }
}
