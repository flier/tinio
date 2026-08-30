//! Conformance test harness for backend implementations (task T014).
//!
//! Every `tinio-core::Storage` implementation MUST pass
//! [`assert_conformance`]. Backend crates enable the `testing` feature in
//! their dev-dependencies and run the suite against a freshly constructed
//! backend:
//!
//! ```toml
//! [dev-dependencies]
//! tinio-util = { workspace = true, features = ["testing"] }
//! ```
//!
//! ```rust,ignore
//! #[tokio::test]
//! async fn conformance() {
//!     let backend = MyBackend::new(...);
//!     testing::assert_conformance(&backend).await;
//! }
//! ```
//!
//! The suite covers: bucket lifecycle and naming errors, object put/get
//! round-trips (byte-identical, ETag = content MD5), key validation and
//! `.tinio` reservation (FR-006/FR-020), folder markers, idempotent deletes,
//! listing with prefix/delimiter/pagination, byte ranges, and the full
//! multipart lifecycle with composed-ETag verification (FR-014/FR-022).
//! [`tinio_mem::MemoryStorage`] — the in-memory reference backend —
//! backs conformance tests in the `tinio-mem` crate.
//!
//! This module is a deliberately panicking test harness (assert-style);
//! unwraps here are assertion plumbing, not library error handling.

use std::{
    io,
    process::id,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures::{StreamExt, stream};
use tokio::time::{Instant, sleep};

use crate::_core::{
    BodyStream, ByteRange, CompletedPart, ETag, ListObjectsParams, ListPartsParams,
    ListUploadsParams, Storage, bucket, multipart, object, storage, storage::Error::*,
};

/// Produce a unique bucket name for the harness (fresh backends may already
/// hold fixtures).
pub fn unique_bucket(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{n}-{}", id())
}

/// Wrap bytes into a one-chunk body stream.
pub fn body<C: Into<Vec<u8>>>(bytes: C) -> BodyStream {
    let data: Vec<u8> = bytes.into();
    Box::pin(stream::iter(vec![Ok(Bytes::from(data))]))
}

/// Read a body stream to the end.
pub async fn read_body(mut body: BodyStream) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}

/// Assert that a type is `Send + Sync + 'static` (every contract type is).
pub fn assert_send_sync<T: Send + Sync + 'static>() {}

/// Build a validated [`ETag`] from a wire-format hex string (harness helper).
pub fn etag(hex: &str) -> ETag {
    ETag::new(hex).expect("valid etag")
}

/// Reference multipart ETag for the conformance parts
/// (`part-one-`, `part-two-`, `part-three`) under the AWS composition:
/// MD5(raw(part1) || raw(part2) || raw(part3))-3, computed externally.
const EXPECTED_COMPOSED_ETAG: &str = "aed23cbfc502f1e851e828efe2ca50d0-3";

fn check(cond: bool, msg: &str) {
    assert!(cond, "conformance violation: {msg}");
}

/// Run the full conformance suite against a backend. Panics on the first
/// violation (test-harness semantics).
pub async fn assert_conformance<S: Storage>(storage: &S) {
    let b = bucket::name(unique_bucket("conform")).unwrap();
    conformance_buckets(storage, &b).await;
    conformance_objects(storage, &b).await;
    conformance_copy(storage, &b).await;
    conformance_listing(storage, &b).await;
    conformance_multipart(storage, &b).await;
}

async fn conformance_copy<S: Storage>(storage: &S, b: &bucket::Name) {
    storage.create_bucket(b).await.unwrap();

    // CopyObject: the destination holds the source's bytes, and its ETag
    // is the content MD5 (a full copy of a single-form source may reuse
    // the source ETag — either way the wire value is the content MD5).
    let data = b"copy me, byte for byte";
    let src = object::key("src.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    storage
        .put_object(b, &src, body(data.to_vec()))
        .await
        .unwrap();
    let put = storage.copy_object(b, &src, b, &dst).await.unwrap();
    check(
        put.etag == ETag::from_content(data),
        "copy ETag must be the content MD5",
    );
    let get = storage.get_object(b, &dst, None).await.unwrap();
    check(get.info.size == data.len() as u64, "copy size must match");
    check(
        read_body(get.body).await.unwrap() == data,
        "copy content must be byte-identical",
    );

    // Copy of a missing source is NoSuchKey.
    let missing = object::key("ghost.bin").unwrap();
    let err = into_core_error(storage.copy_object(b, &missing, b, &dst).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "copy of a missing source must be NoSuchKey",
    );

    // Copy into a folder-marker destination creates the marker (the
    // destination is a directory, never an object).
    let marker = object::key("copied-dir/").unwrap();
    storage.copy_object(b, &src, b, &marker).await.unwrap();
    let err = into_core_error(storage.get_object(b, &marker, None).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "a copied folder marker is not an object",
    );
    storage.delete_object(b, &marker).await.unwrap();

    // UploadPartCopy: the part holds the source's bytes (optionally a
    // byte range); the part ETag is the content MD5 of the part bytes.
    let upload = storage.create_multipart_upload(b, &src).await.unwrap();
    let part = storage
        .copy_part(
            b,
            &src,
            b,
            &src,
            &upload.upload_id,
            multipart::part_number(1).unwrap(),
            Some(ByteRange::Inclusive(2, 9)),
        )
        .await
        .unwrap();
    check(
        part.etag == ETag::from_content(&data[2..=9]),
        "part-copy ETag must be the range's content MD5",
    );
    check(part.size == 8, "part-copy size must match the range");
    let part = storage
        .copy_part(
            b,
            &src,
            b,
            &src,
            &upload.upload_id,
            multipart::part_number(2).unwrap(),
            None,
        )
        .await
        .unwrap();
    check(
        part.etag == ETag::from_content(data),
        "full part-copy ETag must be the content MD5",
    );
    let parts = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: src.clone(),
            upload_id: upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(parts.parts.len() == 2, "part copies must be listed");
    storage
        .abort_multipart_upload(b, &src, &upload.upload_id)
        .await
        .unwrap();

    // Cleanup.
    storage.delete_object(b, &src).await.unwrap();
    storage.delete_object(b, &dst).await.unwrap();
    storage.delete_bucket(b).await.unwrap();
}

async fn conformance_buckets<S: Storage>(storage: &S, b: &bucket::Name) {
    // Start empty.
    let buckets = storage.list_buckets().await.unwrap();
    check(
        buckets.iter().all(|x| x.name != *b),
        "fresh bucket already listed",
    );

    // Create.
    storage.create_bucket(b).await.unwrap();
    let buckets = storage.list_buckets().await.unwrap();
    check(
        buckets.iter().any(|x| x.name == *b),
        "created bucket must be listed",
    );

    // Head.
    let head = storage.head_bucket(b).await.unwrap();
    check(head.name == *b, "head_bucket returns the bucket name");

    // Duplicate create.
    let err = into_core_error(storage.create_bucket(b).await.unwrap_err());
    check(
        matches!(err, AlreadyExists(_)),
        "duplicate create must be AlreadyExists",
    );

    // Invalid bucket names are rejected at the checked constructor.
    for bad in ["ab", "Big", "bad name", "a..b", "-lead"] {
        check(
            bucket::name(bad).is_err(),
            "invalid bucket name must be rejected by bucket::name",
        );
    }

    // Delete missing bucket.
    let missing = bucket::name(unique_bucket("missing")).unwrap();
    let err = into_core_error(storage.delete_bucket(&missing).await.unwrap_err());
    check(
        matches!(err, NoSuchBucket(_)),
        "deleting a missing bucket must be NoSuchBucket",
    );

    // Delete non-empty (a folder marker counts as content).
    let marker = object::key("dir/").unwrap();
    storage.put_object(b, &marker, body("")).await.unwrap();
    let err = into_core_error(storage.delete_bucket(b).await.unwrap_err());
    check(
        matches!(err, NotEmpty(_)),
        "deleting a non-empty bucket must be NotEmpty",
    );
    storage.delete_object(b, &marker).await.unwrap();

    // Delete empty.
    storage.delete_bucket(b).await.unwrap();
    let err = into_core_error(storage.head_bucket(b).await.unwrap_err());
    check(
        matches!(err, NoSuchBucket(_)),
        "head_bucket after delete must be NoSuchBucket",
    );
}

async fn conformance_objects<S: Storage>(storage: &S, b: &bucket::Name) {
    storage.create_bucket(b).await.unwrap();

    // Zero-byte round-trip.
    let empty = object::key("empty").unwrap();
    let put = storage.put_object(b, &empty, body("")).await.unwrap();
    check(
        put.etag == ETag::EMPTY,
        "zero-byte object ETag must be the MD5 of empty content",
    );
    let head = storage.head_object(b, &empty).await.unwrap();
    check(head.size == 0, "zero-byte object size must be 0");

    // Any Range on a zero-byte object is unsatisfiable (AWS answers 416).
    for range in [
        ByteRange::From(0),
        ByteRange::Inclusive(0, 0),
        ByteRange::Suffix(1),
    ] {
        let err = into_core_error(
            storage
                .get_object(b, &empty, Some(range))
                .await
                .unwrap_err(),
        );
        check(
            matches!(err, InvalidRange { .. }),
            "range on a zero-byte object must be InvalidRange",
        );
    }

    // Byte-identical round-trip with ETag = content MD5.
    let data = b"hello tinio, streaming bodies work";
    let hello = object::key("hello.txt").unwrap();
    storage
        .put_object(b, &hello, body(data.to_vec()))
        .await
        .unwrap();
    let head = storage.head_object(b, &hello).await.unwrap();
    check(
        head.etag == ETag::from_content(data),
        "single-upload ETag must be the content MD5",
    );
    check(head.size == data.len() as u64, "size must match");
    let get = storage.get_object(b, &hello, None).await.unwrap();
    check(get.served_range.is_none(), "full read has no served range");
    let got = read_body(get.body).await.unwrap();
    check(got == data, "get must return the object byte-identical");

    // The two-phase write (stage + commit) equals a direct put.
    let staged = storage
        .stage_body(b, &hello, body(data.to_vec()))
        .await
        .unwrap();
    let put = storage.commit_object(b, &hello, staged).await.unwrap();
    check(
        put.etag == ETag::from_content(data),
        "staged commit ETag must be the content MD5",
    );
    let head = storage.head_object(b, &hello).await.unwrap();
    check(
        head.size == data.len() as u64,
        "staged commit must overwrite",
    );

    // Missing object.
    let missing = object::key("missing").unwrap();
    let err = into_core_error(storage.head_object(b, &missing).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "missing object must be NoSuchKey",
    );

    // Invalid keys are rejected at the checked constructor (FR-006) — before
    // any backend call.
    for bad in ["../evil", "/abs", "a\x00b", "a/../b", "a/./b", ""] {
        check(
            object::key(bad).is_err(),
            "invalid key must be rejected by object::key",
        );
    }

    // Reserved .tinio segment at ANY depth (FR-020): writes AccessDenied,
    // reads NoSuchKey. The keys are syntactically valid — the rejection is
    // the backend's duty.
    for key in [".tinio", ".tinio/x", "a/.tinio", "a/.tinio/b"] {
        let reserved = object::key(key).unwrap();
        check(reserved.is_reserved(), "reserved key must be flagged");
        let err = into_core_error(
            storage
                .put_object(b, &reserved, body("x"))
                .await
                .unwrap_err(),
        );
        check(
            matches!(err, AccessDenied(_)),
            "write to a reserved key must be AccessDenied",
        );
        let err = into_core_error(storage.get_object(b, &reserved, None).await.unwrap_err());
        check(
            matches!(err, NoSuchKey(_)),
            "read of a reserved key must be NoSuchKey",
        );
    }

    // Folder markers (s3-surface.md): PUT creates, GET/HEAD → NoSuchKey,
    // DELETE idempotent.
    let marker = object::key("marker/").unwrap();
    storage.put_object(b, &marker, body("")).await.unwrap();
    let err = into_core_error(storage.get_object(b, &marker, None).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "GET of a folder marker must be NoSuchKey",
    );
    storage.delete_object(b, &marker).await.unwrap();
    storage.delete_object(b, &marker).await.unwrap(); // idempotent

    // Idempotent object delete (S3 semantics: 204 always).
    let never = object::key("never-existed").unwrap();
    storage.delete_object(b, &never).await.unwrap();

    // Byte ranges.
    let digits = object::key("digits").unwrap();
    let data = b"0123456789";
    storage
        .put_object(b, &digits, body(data.to_vec()))
        .await
        .unwrap();
    let get = storage
        .get_object(b, &digits, Some(ByteRange::Inclusive(2, 5)))
        .await
        .unwrap();
    check(get.served_range == Some((2, 5)), "inclusive range served");
    check(
        read_body(get.body).await.unwrap() == b"2345",
        "inclusive range content",
    );
    let get = storage
        .get_object(b, &digits, Some(ByteRange::From(7)))
        .await
        .unwrap();
    check(
        read_body(get.body).await.unwrap() == b"789",
        "open-ended range content",
    );
    let get = storage
        .get_object(b, &digits, Some(ByteRange::Suffix(3)))
        .await
        .unwrap();
    check(
        read_body(get.body).await.unwrap() == b"789",
        "suffix range content",
    );

    // Cleanup.
    storage.delete_object(b, &empty).await.unwrap();
    storage.delete_object(b, &hello).await.unwrap();
    storage.delete_object(b, &digits).await.unwrap();
    storage.delete_bucket(b).await.unwrap();
}

async fn conformance_listing<S: Storage>(storage: &S, b: &bucket::Name) {
    storage.create_bucket(b).await.unwrap();
    for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "dir/e.txt"] {
        storage
            .put_object(b, &object::key(key).unwrap(), body(format!("{key}!")))
            .await
            .unwrap();
    }

    // Full listing.
    let page = storage
        .list_objects(list_params(b, "", None, None, 1000))
        .await
        .unwrap();
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_ref()).collect();
    check(
        keys == ["a.txt", "b.txt", "dir/c.txt", "dir/e.txt", "dir/sub/d.txt"],
        &format!("full listing must be lexicographic: {keys:?}"),
    );

    // Prefix filtering.
    let page = storage
        .list_objects(list_params(b, "dir/", None, None, 1000))
        .await
        .unwrap();
    check(
        page.objects.iter().all(|o| o.key.starts_with("dir/")),
        "prefix filter must be applied",
    );

    // Delimiter grouping.
    let page = storage
        .list_objects(list_params(b, "", Some("/"), None, 1000))
        .await
        .unwrap();
    let keys: Vec<_> = page.objects.iter().map(|o| o.key.as_ref()).collect();
    check(
        keys == ["a.txt", "b.txt"],
        "delimiter must roll up nested keys",
    );
    check(
        page.common_prefixes == ["dir/"],
        "common prefixes must be returned",
    );

    // Pagination.
    let page = storage
        .list_objects(list_params(b, "", None, None, 2))
        .await
        .unwrap();
    check(
        page.objects.len() == 2 && page.truncated,
        "max_keys must truncate",
    );
    let resume = page.next_start_after.clone().expect("resume marker");
    let page2 = storage
        .list_objects(list_params(b, "", None, Some(&resume), 1000))
        .await
        .unwrap();
    let all: Vec<_> = page
        .objects
        .iter()
        .chain(&page2.objects)
        .map(|o| o.key.as_ref())
        .collect();
    check(all.len() == 5, "paginated listing must cover everything");
    check(!page2.truncated, "second page must be the last");

    // Listing a missing bucket.
    let missing = bucket::name(unique_bucket("missing")).unwrap();
    let err = into_core_error(
        storage
            .list_objects(list_params(&missing, "", None, None, 1000))
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchBucket(_)),
        "listing a missing bucket must be NoSuchBucket",
    );

    for key in ["a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt", "dir/e.txt"] {
        storage
            .delete_object(b, &object::key(key).unwrap())
            .await
            .unwrap();
    }
    storage.delete_bucket(b).await.unwrap();
}

fn list_params(
    bucket: &bucket::Name,
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
    max_keys: usize,
) -> ListObjectsParams {
    ListObjectsParams {
        bucket: bucket.clone(),
        prefix: prefix.into(),
        delimiter: delimiter.map(str::to_string),
        start_after: start_after.map(str::to_string),
        max_keys,
    }
}

async fn conformance_multipart<S: Storage>(storage: &S, b: &bucket::Name) {
    storage.create_bucket(b).await.unwrap();

    // Reserved keys are refused at multipart creation too (FR-020) — the
    // multipart path must not be a backdoor for materializing `.tinio`.
    let reserved = object::key("a/.tinio/b").unwrap();
    let err = into_core_error(
        storage
            .create_multipart_upload(b, &reserved)
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, AccessDenied(_)),
        "multipart create on a reserved key must be AccessDenied",
    );

    // Full lifecycle: create → upload 3 parts → list → complete.
    let big = object::key("big.bin").unwrap();
    let upload = storage.create_multipart_upload(b, &big).await.unwrap();
    check(!upload.upload_id.is_empty(), "upload id must be non-empty");

    let parts_data: [&[u8]; 3] = [b"part-one-", b"part-two-", b"part-three"];
    let mut parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part = storage
            .upload_part(
                b,
                &big,
                &upload.upload_id,
                ((i + 1) as u32).into(),
                body(data.to_vec()),
            )
            .await
            .unwrap();
        check(
            part.etag == ETag::from_content(data),
            "part ETag must be the part's content MD5",
        );
        parts.push(part);
    }
    check(
        parts
            .iter()
            .map(|p| u32::from(p.part_number))
            .collect::<Vec<_>>()
            == [1, 2, 3],
        "part numbers must be preserved",
    );

    // List parts.
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: big.clone(),
            upload_id: upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(listing.parts.len() == 3, "list_parts must return 3 parts");
    check(!listing.truncated, "part listing not truncated");

    // Complete → composed ETag `MD5-of-MD5s-N` and byte-exact assembly.
    let completed_parts: Vec<CompletedPart> = parts
        .iter()
        .map(|p| CompletedPart {
            part_number: p.part_number,
            etag: p.etag.clone(),
        })
        .collect();
    let completed = storage
        .complete_multipart_upload(b, &big, &upload.upload_id, &completed_parts)
        .await
        .unwrap();
    let expected = b"part-one-part-two-part-three";
    check(
        completed.size == expected.len() as u64,
        "assembled object size",
    );
    // The composed ETag is verified against a hard-coded reference value
    // (AWS composition: MD5 of the raw part digests concatenated, then -N)
    // — not against the same function the backend uses, which would let a
    // composition bug pass both sides.
    check(
        completed.etag.as_str() == EXPECTED_COMPOSED_ETAG,
        &format!(
            "composed ETag must be MD5-of-MD5s-N: {} != {}",
            completed.etag, EXPECTED_COMPOSED_ETAG
        ),
    );
    let get = storage.get_object(b, &big, None).await.unwrap();
    check(
        read_body(get.body).await.unwrap() == expected,
        "assembled object must be byte-exact",
    );

    // Missing uploads.
    let err = into_core_error(
        storage
            .abort_multipart_upload(b, &big, "no-such-upload")
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchUpload(_)),
        "abort of a missing upload must be NoSuchUpload",
    );

    // Complete with no parts is an error.
    let empty_mp = object::key("empty-mp.bin").unwrap();
    let u = storage.create_multipart_upload(b, &empty_mp).await.unwrap();
    let err = into_core_error(
        storage
            .complete_multipart_upload(b, &empty_mp, &u.upload_id, &[])
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoParts),
        "complete with no parts must be NoParts",
    );
    storage
        .abort_multipart_upload(b, &empty_mp, &u.upload_id)
        .await
        .unwrap();

    // A bucket with in-progress uploads is not empty.
    let busy = object::key("busy.bin").unwrap();
    let busy_upload = storage.create_multipart_upload(b, &busy).await.unwrap();
    let err = into_core_error(storage.delete_bucket(b).await.unwrap_err());
    check(
        matches!(err, NotEmpty(_)),
        "deleting a bucket with in-progress uploads must be NotEmpty",
    );
    storage
        .abort_multipart_upload(b, &busy, &busy_upload.upload_id)
        .await
        .unwrap();

    // Abort removes parts and leaves no object.
    let abort_bin = object::key("abort.bin").unwrap();
    let upload2 = storage
        .create_multipart_upload(b, &abort_bin)
        .await
        .unwrap();
    storage
        .upload_part(b, &abort_bin, &upload2.upload_id, 1.into(), body(b"x"))
        .await
        .unwrap();
    storage
        .abort_multipart_upload(b, &abort_bin, &upload2.upload_id)
        .await
        .unwrap();
    let err = into_core_error(storage.head_object(b, &abort_bin).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "aborted upload must leave no object",
    );

    // List uploads.
    let pending = object::key("pending.bin").unwrap();
    let upload3 = storage.create_multipart_upload(b, &pending).await.unwrap();
    let listing = storage
        .list_multipart_uploads(ListUploadsParams {
            bucket: b.clone(),
            prefix: String::new(),
            delimiter: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: 1000,
        })
        .await
        .unwrap();
    check(
        listing
            .uploads
            .iter()
            .any(|u| u.upload_id == upload3.upload_id),
        "in-progress upload must be listed",
    );
    storage
        .abort_multipart_upload(b, &pending, &upload3.upload_id)
        .await
        .unwrap();

    storage.delete_object(b, &big).await.unwrap();
    storage.delete_bucket(b).await.unwrap();
}

/// Convert a backend error into the contract error for assertions.
fn into_core_error<E: Into<storage::Error>>(err: E) -> storage::Error {
    err.into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_buckets_are_unique() {
        assert_ne!(unique_bucket("x"), unique_bucket("x"));
    }

    #[test]
    fn etag_helper_builds_validated_etag() {
        let hex = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(etag(hex).as_str(), hex);
    }
}

/// Poll `cond` until true or a 10 s deadline passes (the test runners'
/// workers are asynchronous, so assertions must wait). The shared home of
/// the helper formerly duplicated across tinio-fs testutil and the
/// tinio-server pipeline tests (F30).
pub async fn wait_for(mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(Instant::now() < deadline, "condition not met within 10 s");
        sleep(Duration::from_millis(2)).await;
    }
}

/// An owned `Write` sink for the fmt layers under test (the log.rs and
/// pipeline.rs test pattern — F32: one definition, three copies removed).
#[derive(Clone, Default)]
pub struct SharedBuf(pub Arc<Mutex<Vec<u8>>>);

impl io::Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
