//! Multipart steps (T032): the minimal set the error-code scenarios need,
//! grown with the coverage-gap scenarios (EntityTooSmall, part-number-
//! marker validation, completion with the recorded parts). The full
//! multipart feature suite grows this module in Task 8.

use std::{
    collections::HashMap,
    time::SystemTime,
};

use cucumber::{gherkin::Step, given, then, when};

use crate::_core::{etag::ETag, multipart::PartInfo};

use super::common::{deterministic_bytes, md5_hex};

/// The scenario's multipart-upload state (world.mp): one rebuild per
/// started upload ([`start_upload_with`]), so no upload-scoped field can
/// leak across uploads — a new upload never inherits a stale part.
#[derive(Debug, Default)]
pub struct MultipartState {
    /// UploadId of the scenario's multipart upload.
    pub upload_id: String,
    /// The key the scenario's multipart upload was started for.
    pub upload_key: String,
    /// The uploaded parts (part number, wire ETag), in upload order —
    /// recorded by the part-upload steps, echoed back by the completion
    /// step.
    pub parts: Vec<(u32, String)>,
    /// The parts the last completion step echoed (the composed-ETag
    /// assertion derives from exactly these).
    pub last_completed: Vec<(u32, String)>,
    /// The size of each recorded part (the completion checksum step
    /// derives `x-amz-mp-object-size` from these).
    pub part_sizes: HashMap<u32, u64>,
    /// The size of the last part upload (the part-size record).
    pub last_part_size: u64,
    /// The body of the last part upload (the part-ETag assertion hashes
    /// it).
    pub last_part_body: Vec<u8>,
}

#[given(expr = "I start a multipart upload for {string}")]
async fn start_upload(world: &mut super::World, key: String) {
    start_upload_with(world, key, None).await;
}

/// A create with a `x-amz-checksum-algorithm` header (the checksum
/// scenarios): the upload's algorithm is pinned for the later part and
/// completion checksum validations.
#[given(expr = "I start a multipart upload for {string} with checksum-algorithm {word}")]
async fn start_upload_algo(world: &mut super::World, key: String, algo: String) {
    start_upload_with(world, key, Some(&algo)).await;
}

/// One create: `POST /{key}?uploads`, optionally carrying the
/// `x-amz-checksum-algorithm` header. Keep the UploadId and the key for
/// the part-upload steps; a new upload rebuilds the whole upload state.
async fn start_upload_with(world: &mut super::World, key: String, algo: Option<&str>) {
    let headers: &[(&str, &str)] = match algo {
        Some(algo) => &[("x-amz-checksum-algorithm", algo)],
        None => &[],
    };
    world.last = world
        .client
        .request("POST", &format!("/{key}?uploads"), headers, &[])
        .await;
    let upload_id = super::common::extract(
        &String::from_utf8_lossy(&world.last.body),
        "<UploadId>",
        "</UploadId>",
    );
    world.mp = MultipartState {
        upload_key: key,
        upload_id,
        ..Default::default()
    };
}

#[when(regex = r#"I upload part (\d+) with body "([^"]*)" and checksum-crc32 "([^"]+)""#)]
async fn upload_part_checksum(world: &mut super::World, part: u32, body: String, crc32: String) {
    let body = body.into_bytes();
    upload_part(world, part, &[("x-amz-checksum-crc32", &crc32)], &body, true).await;
}

/// A part upload with a `Content-MD5` header (the checksum scenarios).
#[given(expr = "I upload part {int} with body {string} and content-md5 {string}")]
#[when(expr = "I upload part {int} with body {string} and content-md5 {string}")]
async fn upload_part_md5(world: &mut super::World, part: u32, body: String, md5: String) {
    let body = body.into_bytes();
    upload_part(world, part, &[("Content-MD5", &md5)], &body, true).await;
}

/// A part upload with an explicit text body and no checksum headers (the
/// part re-upload scenario).
#[given(expr = "I upload part {int} with body {string}")]
#[when(expr = "I upload part {int} with body {string}")]
async fn upload_part_body(world: &mut super::World, part: u32, body: String) {
    let body = body.into_bytes();
    upload_part(world, part, &[], &body, true).await;
}

/// A part upload without checksum headers, with a body of `n`
/// deterministic bytes (the same size always produces the same bytes
/// within a run). The non-final-part minimum-size check is size-based, so
/// the byte values never matter.
#[given(expr = "I upload part {int} with {int} bytes")]
#[when(expr = "I upload part {int} with {int} bytes")]
#[then(expr = "I upload part {int} with {int} bytes")]
async fn upload_part_bytes(world: &mut super::World, part: u32, n: u64) {
    let body = deterministic_bytes(n);
    upload_part(world, part, &[], &body, true).await;
}

/// The parts 1..=`last` of the scenario's upload, each with `n`
/// deterministic bytes (the ListParts pagination walk). Only the last
/// part's body is retained — the part-ETag assertion reads it back.
#[given(expr = "I upload parts 1 through {int} with {int} bytes each")]
async fn upload_parts_range(world: &mut super::World, last: u32, n: u64) {
    let body = deterministic_bytes(n);
    for part in 1..=last {
        upload_part(world, part, &[], &body, part == last).await;
    }
}

/// One part upload: `PUT /{key}?partNumber={n}&uploadId={id}` with
/// `headers`, recording the response and — on success — the part for the
/// completion step (one home for the part-upload block). The body is
/// retained for the part-ETag assertion only when `retain` is set — the
/// part-range step uploads many parts but reads back only the last one's
/// body.
async fn upload_part(
    world: &mut super::World,
    part: u32,
    headers: &[(&str, &str)],
    body: &[u8],
    retain: bool,
) {
    world.last = world
        .client
        .request(
            "PUT",
            &format!(
                "/{}?partNumber={part}&uploadId={}",
                world.mp.upload_key, world.mp.upload_id
            ),
            headers,
            body,
        )
        .await;
    world.mp.last_part_size = body.len() as u64;
    if retain {
        world.mp.last_part_body = body.to_vec();
    }
    record_part_on_success(world, part);
}

#[given(expr = "I list the parts of the multipart upload")]
#[when(expr = "I list the parts of the multipart upload")]
#[then(expr = "I list the parts of the multipart upload")]
async fn list_parts(world: &mut super::World) {
    do_list_parts(world, None, None).await;
}

/// A ListParts request with an explicit `part-number-marker` — negative
/// values are rejected up front with InvalidArgument (the coverage-gap
/// scenario's wire shape). The `$` anchor keeps this from
/// prefix-matching the marker-and-max-parts variant below.
#[given(regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+)$"#)]
#[when(regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+)$"#)]
#[then(regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+)$"#)]
async fn list_parts_marker(world: &mut super::World, marker: i64) {
    do_list_parts(world, Some(marker), None).await;
}

/// A ListParts request with an explicit `max-parts` — values below one
/// are rejected up front with InvalidArgument. `$`-anchored like the
/// marker variant (the combined variant below extends both).
#[given(regex = r#"I list the parts of the multipart upload with max-parts (-?\d+)$"#)]
#[when(regex = r#"I list the parts of the multipart upload with max-parts (-?\d+)$"#)]
#[then(regex = r#"I list the parts of the multipart upload with max-parts (-?\d+)$"#)]
async fn list_parts_max(world: &mut super::World, max: i64) {
    do_list_parts(world, None, Some(max)).await;
}

/// A ListParts request with both a marker and a page size (the
/// pagination walk).
#[given(
    regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+) and max-parts (-?\d+)$"#
)]
#[when(
    regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+) and max-parts (-?\d+)$"#
)]
#[then(
    regex = r#"I list the parts of the multipart upload with part-number-marker (-?\d+) and max-parts (-?\d+)$"#
)]
async fn list_parts_marker_max(world: &mut super::World, marker: i64, max: i64) {
    do_list_parts(world, Some(marker), Some(max)).await;
}

/// One ListParts request against the scenario's upload.
async fn do_list_parts(world: &mut super::World, marker: Option<i64>, max: Option<i64>) {
    let mut path = format!("/{}?uploadId={}", world.mp.upload_key, world.mp.upload_id);
    if let Some(marker) = marker {
        path += &format!("&part-number-marker={marker}");
    }
    if let Some(max) = max {
        path += &format!("&max-parts={max}");
    }
    world.last = world.client.request("GET", &path, &[], &[]).await;
}

/// Complete the scenario's multipart upload with every part recorded by
/// the part-upload steps (number + wire ETag, in upload order) — the
/// `<CompleteMultipartUpload>` XML echoes them back, so the server can
/// match each part.
#[given(expr = "I complete the multipart upload")]
#[when(expr = "I complete the multipart upload")]
#[then(expr = "I complete the multipart upload")]
async fn complete_upload(world: &mut super::World) {
    let parts = world.mp.parts.clone();
    complete_with(world, &parts, &[("Content-Type", "application/xml")]).await;
}

/// Complete the scenario's upload with the recorded parts plus the
/// conditional-write headers of a data table (the conditions.feature
/// FR-028 legs: If-Match / If-None-Match ride on the completion POST —
/// errors.rs's table handling applies the `{etag}` substitution).
#[given(regex = r#"I complete the multipart upload with headers$"#)]
#[when(regex = r#"I complete the multipart upload with headers$"#)]
#[then(regex = r#"I complete the multipart upload with headers$"#)]
async fn complete_with_conditional_headers(world: &mut super::World, step: &Step) {
    let mut headers = super::errors::table_headers(world, step);
    headers.insert(0, ("Content-Type".into(), "application/xml".into()));
    let refs: Vec<(&str, &str)> = headers
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    let parts = world.mp.parts.clone();
    complete_with(world, &parts, &refs).await;
}

/// Complete with the last `n` recorded parts (a subset completion): the
/// server must assemble exactly the listed parts and compose the ETag
/// over them (`MD5-of-MD5s-N` with N = the listed count).
#[given(expr = "I complete the multipart upload with the last {int} parts")]
#[when(expr = "I complete the multipart upload with the last {int} parts")]
#[then(expr = "I complete the multipart upload with the last {int} parts")]
async fn complete_last_parts(world: &mut super::World, n: usize) {
    let start = world.mp.parts.len().saturating_sub(n);
    let parts = world.mp.parts[start..].to_vec();
    complete_with(world, &parts, &[("Content-Type", "application/xml")]).await;
}

/// Complete with the recorded parts, one part's ETag replaced by a value
/// that matches nothing (InvalidPart).
#[given(expr = "I complete the multipart upload with a mismatched etag for part {int}")]
#[when(expr = "I complete the multipart upload with a mismatched etag for part {int}")]
#[then(expr = "I complete the multipart upload with a mismatched etag for part {int}")]
async fn complete_mismatched_etag(world: &mut super::World, part: u32) {
    let mut parts = world.mp.parts.clone();
    if let Some(entry) = parts.iter_mut().find(|(n, _)| *n == part) {
        entry.1 = "00000000000000000000000000000000".into();
    }
    complete_with(world, &parts, &[("Content-Type", "application/xml")]).await;
}

/// Complete with the recorded parts plus a part number that was never
/// uploaded (InvalidPart).
#[given(expr = "I complete the multipart upload with an extra part {int}")]
#[when(expr = "I complete the multipart upload with an extra part {int}")]
#[then(expr = "I complete the multipart upload with an extra part {int}")]
async fn complete_extra_part(world: &mut super::World, part: u32) {
    let mut parts = world.mp.parts.clone();
    parts.push((part, "00000000000000000000000000000000".into()));
    complete_with(world, &parts, &[("Content-Type", "application/xml")]).await;
}

/// Complete the scenario's upload under a different key (NoSuchUpload).
#[given(expr = "I complete the multipart upload for {string}")]
#[when(expr = "I complete the multipart upload for {string}")]
#[then(expr = "I complete the multipart upload for {string}")]
async fn complete_for_key(world: &mut super::World, key: String) {
    let parts = world.mp.parts.clone();
    complete_with_key(
        world,
        &key,
        &parts,
        &[("Content-Type", "application/xml")],
    )
    .await;
}

/// Complete with a client-side full-object checksum: the headers carry
/// `x-amz-checksum-crc32`, `x-amz-checksum-type: FULL_OBJECT` and the
/// `x-amz-mp-object-size` derived from the listed parts (a wrong value
/// must fail pre-commit with BadDigest).
#[given(expr = "I complete the multipart upload with checksum-crc32 {string}")]
#[when(expr = "I complete the multipart upload with checksum-crc32 {string}")]
#[then(expr = "I complete the multipart upload with checksum-crc32 {string}")]
async fn complete_with_checksum(world: &mut super::World, crc32: String) {
    let size: u64 = world
        .mp
        .parts
        .iter()
        .map(|(n, _)| world.mp.part_sizes.get(n).copied().unwrap_or(0))
        .sum();
    let size_text = size.to_string();
    let parts = world.mp.parts.clone();
    let headers = [
        ("Content-Type", "application/xml"),
        ("x-amz-checksum-crc32", &crc32),
        ("x-amz-checksum-type", "FULL_OBJECT"),
        ("x-amz-mp-object-size", &size_text),
    ];
    complete_with(world, &parts, &headers).await;
}

/// Complete with TWO checksum fields on every part (F01): with the
/// checksum toggle off the entries are accepted and dropped — a second
/// field must not answer InvalidRequest (off ⇒ v1's pass-through).
#[given(expr = "I complete the multipart upload with two checksum fields on every part")]
#[when(expr = "I complete the multipart upload with two checksum fields on every part")]
#[then(expr = "I complete the multipart upload with two checksum fields on every part")]
async fn complete_with_two_checksums(world: &mut super::World) {
    let body = completion_body(
        |(n, etag)| {
            format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag><ChecksumCRC32>y/Q5Jg==</ChecksumCRC32><ChecksumSHA256>DUoRhQ==</ChecksumSHA256></Part>"
            )
        },
        &world.mp.parts,
    );
    let key = world.mp.upload_key.clone();
    send_complete(
        world,
        &key,
        &body,
        &[("Content-Type", "application/xml")],
    )
    .await;
}

/// Abort the scenario's multipart upload.
#[given(expr = "I abort the multipart upload")]
#[when(expr = "I abort the multipart upload")]
#[then(expr = "I abort the multipart upload")]
async fn abort_upload(world: &mut super::World) {
    world.last = world
        .client
        .request(
            "DELETE",
            &format!("/{}?uploadId={}", world.mp.upload_key, world.mp.upload_id),
            &[],
            &[],
        )
        .await;
}

/// The last response was a parts listing with exactly `n` `<Part>`
/// entries (the optional plural matches both "1 part" and "2 parts").
#[then(expr = "the parts listing shows {int} part(s)")]
async fn parts_listing_shows(world: &mut super::World, n: u64) {
    let text = String::from_utf8_lossy(&world.last.body);
    let count = super::common::count_tag(&world.last.body, "<Part>") as u64;
    assert_eq!(count, n, "part count mismatch in listing: {text}");
}

/// The last response was a ListMultipartUploads listing with exactly `n`
/// `<Upload>` entries.
#[then(expr = "the uploads listing shows {int} upload(s)")]
async fn uploads_listing_shows(world: &mut super::World, n: u64) {
    let text = String::from_utf8_lossy(&world.last.body);
    let count = super::common::count_tag(&world.last.body, "<Upload>") as u64;
    assert_eq!(count, n, "upload count mismatch in listing: {text}");
}

/// The last part upload's ETag equals the MD5 of the uploaded body.
#[then("the part ETag matches the MD5 of the uploaded body")]
async fn part_etag_md5(world: &mut super::World) {
    let etag = world
        .last
        .header("etag")
        .expect("part upload answers an ETag")
        .to_string();
    assert_eq!(
        etag,
        format!("\"{}\"", md5_hex(&world.mp.last_part_body)),
        "part ETag mismatch"
    );
}

/// The last response's ETag equals the composed multipart form of the
/// parts the last completion echoed: `MD5-of-MD5s-N` (the concatenated
/// raw part digests, md5'd, with the part count suffix — the AWS
/// reference composition).
#[then("the object ETag matches the composed multipart form")]
async fn composed_etag(world: &mut super::World) {
    let etag = world
        .last
        .header("etag")
        .expect("response answers an ETag")
        .to_string();
    assert_eq!(
        etag,
        composed(&world.mp.last_completed),
        "composed ETag mismatch"
    );
}

/// The `MD5-of-MD5s-N` composition of `parts` (number, wire ETag) —
/// derived by the production composer: the recorded wire ETags parse
/// back into the core type, so the composition formula has ONE home
/// (the assertion still pins the server's full wiring — the parts it
/// echoes, their order, and the suffix — against that composer).
fn composed(parts: &[(u32, String)]) -> String {
    let infos: Vec<PartInfo> = parts
        .iter()
        .map(|(n, wire)| PartInfo {
            part_number: (*n).into(),
            size: 0,
            etag: ETag::new(wire.trim_matches('"')).expect("recorded part etag"),
            last_modified: SystemTime::UNIX_EPOCH,
            checksum: None,
        })
        .collect();
    let etag = ETag::composed_from_parts(&infos).expect("a completion carries parts");
    format!("\"{}\"", etag.as_str())
}

/// The `<CompleteMultipartUpload>` body whose `<Part>` elements come
/// from `render` — one home for the completion body shape.
fn completion_body(
    render: impl Fn(&(u32, String)) -> String,
    parts: &[(u32, String)],
) -> String {
    let parts_xml: String = parts.iter().map(render).collect();
    format!("<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>")
}

/// The standard completion body of `parts` (number + wire ETag).
fn completion_xml(parts: &[(u32, String)]) -> String {
    completion_body(
        |(n, etag)| format!("<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"),
        parts,
    )
}

/// One completion request against `key`'s upload.
async fn send_complete(
    world: &mut super::World,
    key: &str,
    body: &str,
    headers: &[(&str, &str)],
) {
    world.last = world
        .client
        .request(
            "POST",
            &format!("/{key}?uploadId={}", world.mp.upload_id),
            headers,
            body.as_bytes(),
        )
        .await;
}

/// Complete with `parts` (number + wire ETag) and `headers`, recording
/// the echoed parts for the composed-ETag assertion.
async fn complete_with_key(
    world: &mut super::World,
    key: &str,
    parts: &[(u32, String)],
    headers: &[(&str, &str)],
) {
    let body = completion_xml(parts);
    send_complete(world, key, &body, headers).await;
    world.mp.last_completed = parts.to_vec();
}

/// [`complete_with_key`] against the scenario's upload.
async fn complete_with(
    world: &mut super::World,
    parts: &[(u32, String)],
    headers: &[(&str, &str)],
) {
    let key = world.mp.upload_key.clone();
    complete_with_key(world, &key, parts, headers).await;
}

/// Record the just-uploaded part (number + the response's ETag header)
/// when the upload succeeded — the completion step completes with exactly
/// these parts. A failed upload (e.g. a checksum mismatch) records
/// nothing.
fn record_part_on_success(world: &mut super::World, part: u32) {
    if world.last.status != 200 {
        return;
    }
    let etag = world
        .last
        .header("etag")
        .expect("part upload answers an ETag")
        .to_string();
    record_part(world, part, etag);
    world.mp.part_sizes.insert(part, world.mp.last_part_size);
}

/// Record an uploaded part (number, wire ETag) for the completion step;
/// re-uploading a part replaces its earlier entry.
pub(super) fn record_part(world: &mut super::World, part: u32, etag: String) {
    world.mp.parts.retain(|(n, _)| *n != part);
    world.mp.parts.push((part, etag));
}
