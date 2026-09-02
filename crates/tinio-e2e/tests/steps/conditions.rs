//! Request-condition steps (RFC 7232 + copy-source-range validation),
//! ported from the conditional-request and copy-range legs of
//! `tinio-server/tests/coverage_gaps.rs`. The conditional PUTs themselves
//! go through the raw with-headers step in errors.rs; this module adds
//! the UploadPartCopy step the copy-range scenario needs (the source and
//! range ride in `x-amz-copy-source*` headers, so a dedicated step keeps
//! the dynamic upload id out of the feature text).

use cucumber::{given, when};

/// UploadPartCopy: copies `source` (the wire `x-amz-copy-source` value,
/// e.g. "/src/key.bin") into part `n` of the scenario's multipart upload,
/// restricted to the closed `bytes=first-last` range. The copied part's
/// ETag is extracted from the `<CopyPartResult>` body and recorded for
/// `I complete the multipart upload`.
#[given(regex = r#"I upload part copy (\d+) of "([^"]+)" with range "([^"]+)""#)]
#[when(regex = r#"I upload part copy (\d+) of "([^"]+)" with range "([^"]+)""#)]
async fn upload_part_copy(world: &mut super::World, part: u32, source: String, range: String) {
    world.last = world
        .client
        .request(
            "PUT",
            &format!(
                "/{}?partNumber={part}&uploadId={}",
                world.mp.upload_key, world.mp.upload_id
            ),
            &[
                ("x-amz-copy-source", &source),
                ("x-amz-copy-source-range", &range),
            ],
            &[],
        )
        .await;
    if world.last.status == 200 {
        let etag = super::common::extract(
            &String::from_utf8_lossy(&world.last.body),
            "<ETag>",
            "</ETag>",
        );
        assert!(!etag.is_empty(), "copy part answers an ETag");
        super::multipart::record_part(world, part, etag);
    }
}
