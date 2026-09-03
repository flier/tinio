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
