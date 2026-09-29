//! Conformance test harness for backend implementations (task T014).
//!
//! Every `tinio-core::Storage` implementation MUST pass
//! [`assert_conformance`]. Crates enable the `testing` feature in their
//! dev-dependencies: backend crates run the suite against a freshly constructed
//! backend, and any test target may use the shared helpers beside it — the
//! facade's doctor legs consume [`json_keys`]:
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
//! multipart lifecycle with composed-ETag verification (FR-014/FR-022);
//! the tagging contract — object and bucket tag sets, the tagged write
//! paths (commit/copy/multipart completion) and recorded checksums;
//! the bucket CORS trio with zero-rule normalization and idempotent
//! delete; the ACL contract — object and bucket ACL round-trips
//! (replace-all, owner preserved), owner/ACL recording on the write
//! paths (commit, copy with the caller's pair, multipart completion
//! from the upload state), rename preservation, and the `list_buckets`
//! owner filter (filter-then-paginate) (spec 2026-09-05);
//! rename_object; and the completed-object part listing with its
//! OBJECT_PARTS lifecycle (spec 2026-08-31).
//! `tinio_mem::MemoryStorage` — the in-memory reference backend, named rather
//! than linked because `tinio-util` does not depend on `tinio-mem` — backs
//! conformance tests in the `tinio-mem` crate.
//!
//! This module is a deliberately panicking test harness (assert-style);
//! unwraps here are assertion plumbing, not library error handling.
//!
//! Three names here are not the conformance suite's and are shared across crates
//! beyond it: [`wait_timeout_secs`] (the readiness budget every spawned-child
//! harness reads), [`wait_for`] (its async poll) and [`wait_for_exit`] — the one
//! spelling of "every spawned process is waited for or killed on every path",
//! which the two launcher platforms' legs need and only a crate both of them
//! build can hold.
//!
//! Two more are the spawned-child half: [`kill_and_reap`] (re-exported from
//! [`crate::process`], where it is defined outside this feature because a
//! production caller needs it too) and [`PipedChild`], the child whose two pipes
//! are drained into shared buffers for the legs that read them. `PipedChild` is
//! deliberately **not** the whole spawn harness: the readiness event the server
//! legs decode off its stdout is `tinio_sandbox::Event`'s, and a test device that
//! would put the sandbox stack on every `testing` build stays in the test tree.
//! What is here is the half that needs nothing but `std`.
//!
//! The recording half is **one device** and not a pair of helpers: [`capture`],
//! the [`Capture`] it answers with, the [`Recording`] guard whose two entry
//! points ([`Capture::record`] and [`Capture::record_every_thread`]) decide
//! whether a recording owns the calling thread or every thread, and [`recorder`],
//! the process-wide lock the every-thread callers take first. [`record_events`]
//! is the thin wrapper over that device which a leg runs a body through, and
//! [`ThreadLocalBuf`] is the sink a single-thread recording writes through. Each
//! name carries one decision — which sink a process that forks can be recorded
//! through, the `with_ansi(false)` that keeps a captured field greppable, the one
//! level a recording runs at — and a crate behind a `#![cfg(target_os = "…")]`
//! root cannot take any of them from another such crate, so it takes them from
//! here instead of spelling a second copy of each.
//!
//! Four more are the rules a suite is least able to keep on its own, each
//! spelled once per platform crate before it landed here: [`host_cannot`], with
//! [`ALLOW_SKIP`] and [`skip_allowed`], the one arbitration of a host
//! precondition that may be skipped under; [`assert_step`], shared §7's check in
//! its rendered-field form; [`wait_for_path`], the bounded poll on a file another
//! process produces, beside [`poll_exit`]; and [`Untouchable`], the reader that
//! fails a test which reads it. None of them names a domain type or a platform,
//! which is what lets them live in the shared module at all.
//!
//! Four fixtures are the same idea one step down: [`conform_owner`],
//! [`conform_other_owner`], [`conform_group_read_grant`] and [`checksum_tee`]
//! are the ACL and checksum-slot literals the conformance blocks build on, and
//! the fs and mem suites hold the same four as `owner_id`, `other_owner_id`,
//! `all_users_read_grant` and `checksum_tee` — thin delegations there, so a
//! literal three suites agree on is written down once.

use std::{
    cell::RefCell,
    env,
    io::{self, BufRead, BufReader, Read},
    mem,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio, id},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures::{StreamExt, stream};
// `Instant` is std's here, because the module now carries a sync poll
// (`wait_for_exit`) beside the async one; tokio's timer-aware instant is named
// for its crate so the two cannot be swapped by a `use` nobody looked at.
use tokio::time::{Instant as TokioInstant, sleep};
use tracing::{Level, callsite::rebuild_interest_cache};
use tracing_subscriber::fmt::MakeWriter;

use crate::_core::{
    BodyStream, ByteRange, CompletedPart, ETag, ListBucketsParams, ListObjectsParams,
    ListPartsParams, ListUploadsParams, MultipartOps, Storage, acl,
    acl::{Acl, AclGrants},
    bucket,
    checksum::{self, Algorithm, Type},
    cors, multipart,
    multipart::MIN_PART_BYTES,
    object, storage,
    storage::Error::*,
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

/// The key set of one JSON object, sorted.
///
/// The shared half of a "the JSON shape is the schema's" leg: both `tinio-cli`'s report tests
/// and the facade's process legs compare a key set against `doctor-report.schema.json`'s
/// `additionalProperties: false`, and one implementation keeps the two comparisons the same one.
pub fn json_keys(value: &serde_json::Value) -> Vec<String> {
    let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
    keys.sort();
    keys
}

/// Build a validated [`ETag`] from a wire-format hex string (harness helper).
pub fn etag(hex: &str) -> ETag {
    ETag::new(hex).expect("valid etag")
}

/// The harness's fixed canonical owner id (64 lowercase hex) — the
/// recorded owner element the ACL blocks write and assert. The fs and mem
/// suites delegate to it as their `owner_id`.
pub fn conform_owner() -> acl::OwnerId {
    acl::OwnerId::new("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
        .expect("64 lowercase hex")
}

/// A second canonical owner id, distinct from [`conform_owner`]. The fs and
/// mem suites delegate to it as their `other_owner_id`.
pub fn conform_other_owner() -> acl::OwnerId {
    acl::OwnerId::new("ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100")
        .expect("64 lowercase hex")
}

/// The standard group-read grant (AllUsers READ) — the ACL-block
/// fixture grant both backends share. The fs suite delegates to it as its
/// `all_users_read_grant`.
pub fn conform_group_read_grant() -> acl::Grant {
    acl::Grant {
        grantee: acl::Grantee::Group(acl::GroupUri(acl::GROUP_ALL_USERS.into())),
        permission: acl::Permission::Read,
    }
}

/// The server's checksum tee slot with `algorithm`/`value` pre-loaded —
/// the digest the write paths record atomically with the row (the
/// harness sets the slot itself: the backends never hash). The fs and mem
/// suites delegate to it as their own `checksum_tee`.
pub fn checksum_tee(algorithm: checksum::Algorithm, value: &str) -> Arc<checksum::PartChecksum> {
    let slot = Arc::new(checksum::PartChecksum::default());
    let _ = slot.digest.set(checksum::Part {
        algorithm,
        value: checksum::Value(value.into()),
    });
    slot
}

/// Reference multipart ETag for the conformance parts (5 MiB of `a`,
/// 5 MiB of `b`, `part-three` — non-final parts must clear the 5 MiB
/// minimum the backends enforce at complete) under the AWS composition:
/// MD5(raw(part1) || raw(part2) || raw(part3))-3, computed externally.
const EXPECTED_COMPOSED_ETAG: &str = "a123aabc080721b488eb77490b99befe-3";

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

/// Drive one create → upload-part → complete round trip of `key` — the
/// shared scaffold of the fs/mem suites' object-attributes tests. The
/// upload carries no tags or checksum spec; one part of body `b"mp"` is
/// uploaded and completed (a composed single-part object whose retained
/// part list has one row).
pub async fn complete_single_part<S>(storage: &S, b: &bucket::Name, key: &object::Key)
where
    S: Storage + MultipartOps,
{
    let upload = storage
        .create_multipart_upload(
            b,
            key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let part = storage
        .upload_part(b, key, &upload.upload_id, 1.into(), body(b"mp"), None)
        .await
        .unwrap();
    storage
        .complete_multipart_upload(
            b,
            key,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .unwrap();
}

async fn conformance_copy<S: Storage>(storage: &S, b: &bucket::Name) {
    storage
        .create_bucket(b, None, &Acl::default_private(None))
        .await
        .unwrap();

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
    let put = storage
        .copy_object(
            b,
            &src,
            b,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
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
    let err = into_core_error(
        storage
            .copy_object(
                b,
                &missing,
                b,
                &dst,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
                None,
            )
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchKey(_)),
        "copy of a missing source must be NoSuchKey",
    );

    // The copy's metadata is the caller's, never the source's: the
    // `tags` parameter records on the destination, and a tagged source
    // copied without them arrives untagged (the interface sends the
    // source's set only under the COPY directive).
    let tagged_src = object::key("copy-tagged-src.bin").unwrap();
    let src_tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let copy_tags = object::Tags::from_pairs([("tier".into(), "gold".into())]).unwrap();
    let src_checksum = checksum::Recorded {
        part: checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("y/Q5Jg==".into()),
        },
        kind: Type::FullObject,
    };
    let staged = storage
        .stage_body(
            b,
            &tagged_src,
            body(data.to_vec()),
            Some(checksum_tee(Algorithm::Crc32, "y/Q5Jg==")),
        )
        .await
        .unwrap();
    storage
        .commit_object(
            b,
            &tagged_src,
            staged,
            src_tags,
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let bare_dst = object::key("copy-bare-dst.bin").unwrap();
    let bare = storage
        .copy_object(
            b,
            &tagged_src,
            b,
            &bare_dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    check(
        bare.tags.is_empty(),
        "a copy without tags must not inherit the source's",
    );
    let head = storage.head_object(b, &bare_dst).await.unwrap();
    check(
        head.tags.is_empty() && head.checksum.is_none(),
        "an untagged copy must record none of the source's metadata",
    );
    let carried_dst = object::key("copy-carried-dst.bin").unwrap();
    let carried = storage
        .copy_object(
            b,
            &tagged_src,
            b,
            &carried_dst,
            copy_tags.clone(),
            None,
            &Acl::default_private(None),
            Some(src_checksum.clone()),
        )
        .await
        .unwrap();
    check(
        carried.tags == copy_tags,
        "the copy's tags must record on the destination",
    );
    let head = storage.head_object(b, &carried_dst).await.unwrap();
    check(head.tags == copy_tags, "head must serve the copied tags");
    check(
        head.checksum == Some(src_checksum.clone()),
        "the carried checksum — value and kind — must record on the destination",
    );
    check(
        head.etag == ETag::from_content(data),
        "the copy ETag must be the content MD5",
    );

    // The copy's owner is the caller's and its ACL is the caller's —
    // never the source's (AWS: the destination takes the request's ACL
    // or the default; the copy's owner = the requester).
    let src_owner = conform_owner();
    let requester = conform_other_owner();
    let src_grants = vec![conform_group_read_grant()];
    let dst_grants = vec![
        acl::Grant {
            grantee: acl::Grantee::Canonical(requester.clone()),
            permission: acl::Permission::ReadAcp,
        },
        conform_group_read_grant(),
    ];
    let src_acled = object::key("copy-owned-src.bin").unwrap();
    let staged = storage
        .stage_body(b, &src_acled, body(data.to_vec()), None)
        .await
        .unwrap();
    storage
        .commit_object(
            b,
            &src_acled,
            staged,
            object::Tags::empty(),
            Some(&src_owner),
            &Acl {
                owner: Some(src_owner.clone()),
                grants: src_grants.clone(),
            },
        )
        .await
        .unwrap();
    let owned_dst = object::key("copy-owned-dst.bin").unwrap();
    let copied = storage
        .copy_object(
            b,
            &src_acled,
            b,
            &owned_dst,
            object::Tags::empty(),
            Some(&requester),
            &Acl {
                owner: Some(requester.clone()),
                grants: dst_grants.clone(),
            },
            None,
        )
        .await
        .unwrap();
    check(
        copied.owner.as_ref() == Some(&requester) && copied.acl.grants == dst_grants,
        "the copy must record the caller's owner and its own ACL",
    );
    let head = storage.head_object(b, &owned_dst).await.unwrap();
    check(
        head.owner.as_ref() == Some(&requester) && head.acl.grants == dst_grants,
        "the copy's head must echo the caller's owner and ACL",
    );
    let head_src = storage.head_object(b, &src_acled).await.unwrap();
    check(
        head_src.owner.as_ref() == Some(&src_owner) && head_src.acl.grants == src_grants,
        "the source's owner and ACL must be untouched by the copy",
    );

    // Copy into a folder-marker destination creates the marker (the
    // destination is a directory, never an object).
    let marker = object::key("copied-dir/").unwrap();
    storage
        .copy_object(
            b,
            &src,
            b,
            &marker,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    let err = into_core_error(storage.get_object(b, &marker, None).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "a copied folder marker is not an object",
    );
    storage.delete_object(b, &marker).await.unwrap();

    // UploadPartCopy: the part holds the source's bytes (optionally a
    // byte range); the part ETag is the content MD5 of the part bytes.
    let upload = storage
        .create_multipart_upload(
            b,
            &src,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
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
    storage.delete_object(b, &tagged_src).await.unwrap();
    storage.delete_object(b, &bare_dst).await.unwrap();
    storage.delete_object(b, &carried_dst).await.unwrap();
    storage.delete_object(b, &src_acled).await.unwrap();
    storage.delete_object(b, &owned_dst).await.unwrap();
    storage.delete_bucket(b).await.unwrap();
}

/// One ListBuckets request of the conformance fixture — the
/// `prefix: String::new()`-heavy literal built at every pagination
/// step below.
fn page(prefix: &str, start_after: Option<String>, max_buckets: usize) -> ListBucketsParams {
    ListBucketsParams {
        prefix: prefix.to_string(),
        start_after,
        max_buckets,
    }
}

async fn conformance_buckets<S: Storage>(storage: &S, b: &bucket::Name) {
    // Start empty.
    let buckets = storage
        .list_buckets(page("", None, usize::MAX), None, None)
        .await
        .unwrap();
    check(
        buckets.buckets.iter().all(|x| x.name != *b),
        "fresh bucket already listed",
    );

    // Create.
    storage
        .create_bucket(b, None, &Acl::default_private(None))
        .await
        .unwrap();
    let buckets = storage
        .list_buckets(page("", None, usize::MAX), None, None)
        .await
        .unwrap();
    check(
        buckets.buckets.iter().any(|x| x.name == *b),
        "created bucket must be listed",
    );

    // Paginated parity — pins every backend to identical page/marker
    // semantics (the per-backend unit tests alone would not catch a
    // drift): page over ALL buckets with a small page size and assert
    // the union of pages equals the full listing, in name order.
    let extra1 = bucket::name(unique_bucket("conform")).unwrap();
    let extra2 = bucket::name(unique_bucket("conform")).unwrap();
    storage
        .create_bucket(&extra1, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .create_bucket(&extra2, None, &Acl::default_private(None))
        .await
        .unwrap();
    let full = storage
        .list_buckets(page("", None, usize::MAX), None, None)
        .await
        .unwrap();
    check(
        full.buckets.len() >= 3,
        "the full listing must hold the fixture buckets",
    );
    let mut paged = Vec::new();
    let mut start_after = None;
    loop {
        let page = storage
            .list_buckets(page("", start_after.clone(), 2), None, None)
            .await
            .unwrap();
        check(
            page.buckets.len() <= 2,
            "a page must not exceed its max_buckets",
        );
        check(
            !page.truncated || page.next_start_after.is_some(),
            "a truncated page must carry a resume marker",
        );
        check(
            page.next_start_after.is_none() || page.truncated,
            "a resume marker without truncation must not be emitted",
        );
        paged.extend(page.buckets.iter().map(|x| x.name.clone()));
        match page.next_start_after {
            Some(next) => start_after = Some(next),
            None => break,
        }
    }
    check(
        paged
            == full
                .buckets
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<_>>(),
        "the paged union must equal the full listing, in name order",
    );
    // Edge pages: an exact fill (max = the full count) is not
    // truncated; a marker past the end yields an empty, untruncated
    // page; the contract-level `max_buckets = 0` asks for an empty,
    // untruncated page.
    let exact = storage
        .list_buckets(page("", None, full.buckets.len()), None, None)
        .await
        .unwrap();
    check(
        exact.buckets.len() == full.buckets.len()
            && !exact.truncated
            && exact.next_start_after.is_none(),
        "an exact fill must return the full listing, untruncated",
    );
    let last = full
        .buckets
        .last()
        .expect("fixture buckets")
        .name
        .to_string();
    let exhausted = storage
        .list_buckets(page("", Some(last), 1000), None, None)
        .await
        .unwrap();
    check(
        exhausted.buckets.is_empty()
            && !exhausted.truncated
            && exhausted.next_start_after.is_none(),
        "a marker past the end must yield an empty, untruncated page",
    );
    let empty = storage
        .list_buckets(page("", None, 0), None, None)
        .await
        .unwrap();
    check(
        empty.buckets.is_empty() && !empty.truncated && empty.next_start_after.is_none(),
        "max_buckets = 0 must yield an empty, untruncated page",
    );
    // Prefix filtering: the fixture bucket's full name matches exactly
    // that bucket (the counter in `unique_bucket` differs for the rest).
    let prefixed = storage
        .list_buckets(page(b.as_ref(), None, 1000), None, None)
        .await
        .unwrap();
    check(
        prefixed.buckets.len() == 1 && prefixed.buckets[0].name == *b,
        "the prefix filter must match exactly the fixture bucket",
    );
    check(
        !prefixed.truncated && prefixed.next_start_after.is_none(),
        "a complete prefixed page must carry no resume marker",
    );
    storage.delete_bucket(&extra1).await.unwrap();
    storage.delete_bucket(&extra2).await.unwrap();

    // Prefix + marker: a marker inside a prefix-filtered stream
    // positions the next page (exclusive-after). The pair shares the
    // `pg` prefix (the `conform` prefix would match the fixture bucket
    // too); the resume target is read from the listing, never assumed
    // from creation order.
    let p1 = bucket::name(unique_bucket("pg")).unwrap();
    let p2 = bucket::name(unique_bucket("pg")).unwrap();
    storage
        .create_bucket(&p1, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .create_bucket(&p2, None, &Acl::default_private(None))
        .await
        .unwrap();
    let prefixed = storage
        .list_buckets(page("pg", None, 1000), None, None)
        .await
        .unwrap();
    check(
        prefixed.buckets.len() == 2,
        "the pg pair must be the only prefix matches",
    );
    let resumed = storage
        .list_buckets(
            page("pg", Some(prefixed.buckets[0].name.to_string()), 1000),
            None,
            None,
        )
        .await
        .unwrap();
    check(
        resumed.buckets.len() == 1
            && resumed.buckets[0].name == prefixed.buckets[1].name
            && !resumed.truncated
            && resumed.next_start_after.is_none(),
        "a marker inside the filtered stream must resume exactly",
    );
    storage.delete_bucket(&p1).await.unwrap();
    storage.delete_bucket(&p2).await.unwrap();

    // The owner filter runs during the walk, BEFORE pagination (review
    // P2#4): seed an interleaved A/B pair on the `own` prefix (the
    // `{i}a`/`{i}b` suffix of the shared-`id()` pair interleaves the
    // names lexically — a raw-walk page would pair an A with a B),
    // page the A-owned sub-collection with a size
    // smaller than its count, and assert the continuation-token math
    // applies to the FILTERED set: no B-owned leak, the paged union
    // equals the full filtered listing, the last page carries no token.
    let owner_a = conform_owner();
    let owner_b = conform_other_owner();
    let (mut a_buckets, mut b_buckets) = (Vec::new(), Vec::new());
    for i in 0..4 {
        a_buckets.push(bucket::name(format!("own-{}-{i}a", id())).unwrap());
        b_buckets.push(bucket::name(format!("own-{}-{i}b", id())).unwrap());
    }
    for (a_name, b_name) in a_buckets.iter().zip(&b_buckets) {
        storage
            .create_bucket(
                a_name,
                Some(&owner_a),
                &Acl::default_private(Some(owner_a.clone())),
            )
            .await
            .unwrap();
        storage
            .create_bucket(
                b_name,
                Some(&owner_b),
                &Acl::default_private(Some(owner_b.clone())),
            )
            .await
            .unwrap();
    }
    let full = storage
        .list_buckets(page("own", None, usize::MAX), Some(&owner_a), None)
        .await
        .unwrap();
    check(
        full.buckets.len() == a_buckets.len() && !full.truncated && full.next_start_after.is_none(),
        "the full filtered listing must hold exactly the A-owned set",
    );
    let mut paged = Vec::new();
    let mut start_after = None;
    let mut pages = 0;
    loop {
        let p = storage
            .list_buckets(page("own", start_after.clone(), 2), Some(&owner_a), None)
            .await
            .unwrap();
        check(
            p.buckets.iter().all(|x| a_buckets.contains(&x.name)),
            "a filtered page must not leak another owner's bucket",
        );
        paged.extend(p.buckets.iter().map(|x| x.name.clone()));
        pages += 1;
        match p.next_start_after {
            Some(next) => {
                check(p.truncated, "a resume marker must mark the page truncated");
                start_after = Some(next);
            }
            None => {
                check(!p.truncated, "the final filtered page must be untruncated");
                break;
            }
        }
    }
    check(
        pages >= 2,
        "the page size must force at least one continuation token",
    );
    check(
        paged
            == full
                .buckets
                .iter()
                .map(|x| x.name.clone())
                .collect::<Vec<_>>(),
        "the paged union must equal the full filtered listing, in name order",
    );
    // The mirror principal: B's listing holds exactly its own buckets.
    let b_full = storage
        .list_buckets(page("own", None, usize::MAX), Some(&owner_b), None)
        .await
        .unwrap();
    check(
        b_full.buckets.len() == b_buckets.len()
            && b_full.buckets.iter().all(|x| b_buckets.contains(&x.name)),
        "the other owner's listing must hold exactly its own buckets",
    );
    for n in a_buckets.iter().chain(&b_buckets) {
        storage.delete_bucket(n).await.unwrap();
    }
    let legacy = storage
        .list_buckets(page("own", None, usize::MAX), None, None)
        .await
        .unwrap();
    check(
        legacy.buckets.is_empty(),
        "the filtered seed buckets must be gone",
    );

    // Bucket ACL: the write path records the caller-provided owner and
    // grant set, and put_bucket_acl replaces the grants, never the
    // owner (the bucket counterpart of the object block).
    let acl_bucket = bucket::name(unique_bucket("conform")).unwrap();
    let acl_owner = conform_owner();
    let acl_grants = vec![conform_group_read_grant()];
    storage
        .create_bucket(
            &acl_bucket,
            Some(&acl_owner),
            &Acl {
                owner: Some(acl_owner.clone()),
                grants: acl_grants.clone(),
            },
        )
        .await
        .unwrap();
    let got = storage.get_bucket_acl(&acl_bucket).await.unwrap();
    check(
        got.owner.as_ref() == Some(&acl_owner) && got.grants == acl_grants,
        "create_bucket must record the owner and grant set",
    );
    storage
        .put_bucket_acl(&acl_bucket, &AclGrants::new())
        .await
        .unwrap();
    let got = storage.get_bucket_acl(&acl_bucket).await.unwrap();
    check(
        got.grants.is_empty() && got.owner.as_ref() == Some(&acl_owner),
        "put_bucket_acl must replace the grants and preserve the owner",
    );
    storage
        .put_bucket_acl(&acl_bucket, &acl_grants)
        .await
        .unwrap();
    let got = storage.get_bucket_acl(&acl_bucket).await.unwrap();
    check(
        got.grants == acl_grants && got.owner.as_ref() == Some(&acl_owner),
        "put_bucket_acl must round-trip the new grant set",
    );
    let err = into_core_error(
        storage
            .get_bucket_acl(&bucket::name(unique_bucket("missing")).unwrap())
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchBucket(_)),
        "the ACL of a missing bucket must be NoSuchBucket",
    );
    let err = into_core_error(
        storage
            .put_bucket_acl(
                &bucket::name(unique_bucket("missing")).unwrap(),
                &acl_grants,
            )
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchBucket(_)),
        "put_bucket_acl on a missing bucket must be NoSuchBucket",
    );
    storage.delete_bucket(&acl_bucket).await.unwrap();

    // Head.
    let head = storage.head_bucket(b).await.unwrap();
    check(head.name == *b, "head_bucket returns the bucket name");

    // Duplicate create.
    let err = into_core_error(
        storage
            .create_bucket(b, None, &Acl::default_private(None))
            .await
            .unwrap_err(),
    );
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

    // Bucket tags: put/get round-trip, replace-all, delete — and the
    // missing-bucket semantics (get/put NoSuchBucket, delete succeeds;
    // an untagged bucket reads back as the empty set).
    let tags = object::Tags::from_pairs([
        ("owner".into(), "team-x".into()),
        ("env".into(), "prod".into()),
    ])
    .unwrap();
    let replaced = object::Tags::from_pairs([("tier".into(), "gold".into())]).unwrap();
    storage.put_bucket_tags(b, &tags).await.unwrap();
    let got = storage.get_bucket_tags(b).await.unwrap();
    check(got == tags, "bucket tags must round-trip");
    storage.put_bucket_tags(b, &replaced).await.unwrap();
    let got = storage.get_bucket_tags(b).await.unwrap();
    check(got == replaced, "put_bucket_tags must replace, not merge");
    storage.delete_bucket_tags(b).await.unwrap();
    let got = storage.get_bucket_tags(b).await.unwrap();
    check(got.is_empty(), "delete_bucket_tags must clear the set");
    storage.delete_bucket_tags(b).await.unwrap(); // idempotent

    // The bucket cap is 50 tags — a set past the object-tag cap of 10
    // (built with the bucket constructor) round-trips, so the read-side
    // parse honors the bucket cap, not the object cap.
    let wide: Vec<(String, String)> = (0..12)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    let wide = object::Tags::from_pairs_limited(wide, 50).unwrap();
    storage.put_bucket_tags(b, &wide).await.unwrap();
    let got = storage.get_bucket_tags(b).await.unwrap();
    check(
        got == wide,
        "a bucket tag set past 10 tags must read back intact",
    );
    let full: Vec<(String, String)> = (0..50)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();
    let full = object::Tags::from_pairs_limited(full, 50).unwrap();
    storage.put_bucket_tags(b, &full).await.unwrap();
    let got = storage.get_bucket_tags(b).await.unwrap();
    check(got == full, "a 50-tag bucket set must round-trip");
    storage.delete_bucket_tags(b).await.unwrap();

    // Missing bucket.
    let err = into_core_error(storage.get_bucket_tags(&missing).await.unwrap_err());
    check(
        matches!(err, NoSuchBucket(_)),
        "get_bucket_tags on a missing bucket must be NoSuchBucket",
    );
    let err = into_core_error(storage.put_bucket_tags(&missing, &tags).await.unwrap_err());
    check(
        matches!(err, NoSuchBucket(_)),
        "put_bucket_tags on a missing bucket must be NoSuchBucket",
    );
    storage.delete_bucket_tags(&missing).await.unwrap(); // idempotent

    // Bucket CORS: put/get round-trip, re-put replaces, delete clears —
    // with the zero-rule normalization (a `cors::Config::default()` through
    // the storage layer is "no config", op-review G2) and the
    // missing-bucket get semantics.
    let config = cors::Config {
        rules: vec![cors::Rule {
            id: Some("one".into()),
            allowed_methods: vec!["GET".into()],
            allowed_origins: vec!["*".into()],
            allowed_headers: None,
            expose_headers: None,
            max_age_seconds: None,
        }],
    };
    storage.put_bucket_cors(b, &config).await.unwrap();
    let got = storage.get_bucket_cors(b).await.unwrap();
    check(
        got == Some(config.clone()),
        "bucket CORS config must round-trip",
    );
    storage
        .put_bucket_cors(b, &cors::Config::default())
        .await
        .unwrap();
    let got = storage.get_bucket_cors(b).await.unwrap();
    check(
        got.is_none(),
        "a zero-rule CORS config must store as no config",
    );
    storage.put_bucket_cors(b, &config).await.unwrap();
    let got = storage.get_bucket_cors(b).await.unwrap();
    check(
        got == Some(config.clone()),
        "a re-put CORS config must round-trip",
    );
    let err = into_core_error(storage.get_bucket_cors(&missing).await.unwrap_err());
    check(
        matches!(err, NoSuchBucket(_)),
        "get_bucket_cors on a missing bucket must be NoSuchBucket",
    );
    storage.delete_bucket_cors(b).await.unwrap();
    let got = storage.get_bucket_cors(b).await.unwrap();
    check(got.is_none(), "delete_bucket_cors must clear the config");
    storage.delete_bucket_cors(b).await.unwrap(); // idempotent

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
    storage
        .create_bucket(b, None, &Acl::default_private(None))
        .await
        .unwrap();

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
        .stage_body(b, &hello, body(data.to_vec()), None)
        .await
        .unwrap();
    let put = storage
        .commit_object(
            b,
            &hello,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
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

    // Object tags: put/get round-trip, replace-all, and delete — with
    // the missing-object semantics (get/put NoSuchKey, delete succeeds;
    // an untagged object reads back as the empty set).
    let tags = object::Tags::from_pairs([
        ("env".into(), "prod".into()),
        ("team".into(), "core".into()),
        ("owner".into(), "harness".into()),
    ])
    .unwrap();
    let replaced = object::Tags::from_pairs([("tier".into(), "gold".into())]).unwrap();
    storage.put_object_tags(b, &hello, &tags).await.unwrap();
    let got = storage.get_object_tags(b, &hello).await.unwrap();
    check(got == tags, "object tags must round-trip");
    let head = storage.head_object(b, &hello).await.unwrap();
    check(head.tags == tags, "head must serve a tag-only write");
    storage.put_object_tags(b, &hello, &replaced).await.unwrap();
    let got = storage.get_object_tags(b, &hello).await.unwrap();
    check(got == replaced, "put_object_tags must replace, not merge");
    storage.delete_object_tags(b, &hello).await.unwrap();
    let got = storage.get_object_tags(b, &hello).await.unwrap();
    check(got.is_empty(), "delete_object_tags must clear the set");
    storage.delete_object_tags(b, &hello).await.unwrap(); // idempotent
    let err = into_core_error(storage.get_object_tags(b, &missing).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the tags of a missing object must be NoSuchKey",
    );
    let err = into_core_error(
        storage
            .put_object_tags(b, &missing, &tags)
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchKey(_)),
        "tagging a missing object must be NoSuchKey",
    );
    storage.delete_object_tags(b, &missing).await.unwrap(); // idempotent

    // The tagged write path: a staged commit records its tags with the
    // object — head and get serve the same set.
    let tagged = object::key("tagged.bin").unwrap();
    let write_tags = object::Tags::from_pairs([
        ("env".into(), "prod".into()),
        ("owner".into(), "harness".into()),
    ])
    .unwrap();
    let staged = storage
        .stage_body(b, &tagged, body(data.to_vec()), None)
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &tagged,
            staged,
            write_tags.clone(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        committed.tags == write_tags,
        "a commit must record its tags",
    );
    let head = storage.head_object(b, &tagged).await.unwrap();
    check(
        head.tags == write_tags,
        "head must serve the committed tags",
    );
    let get = storage.get_object(b, &tagged, None).await.unwrap();
    check(
        get.info.tags == write_tags,
        "get must serve the committed tags",
    );

    // Object ACL (spec 2026-09-05): the write paths record the
    // caller-provided owner and grant set, a put_object_acl replaces
    // the grants never the owner, and a plain put_object records the
    // default-private ACL with no owner (review B4 — a no-identity
    // write records an empty owner wire, never the default owner).
    let acl_owner = conform_owner();
    let acl_grants = vec![conform_group_read_grant()];
    let acl_key = object::key("acl.bin").unwrap();
    let staged = storage
        .stage_body(b, &acl_key, body(data.to_vec()), None)
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &acl_key,
            staged,
            object::Tags::empty(),
            Some(&acl_owner),
            &Acl {
                owner: Some(acl_owner.clone()),
                grants: acl_grants.clone(),
            },
        )
        .await
        .unwrap();
    check(
        committed.owner.as_ref() == Some(&acl_owner) && committed.acl.grants == acl_grants,
        "a commit must record the caller-provided owner and grant set",
    );
    let got = storage.get_object_acl(b, &acl_key).await.unwrap();
    check(
        got.owner.as_ref() == Some(&acl_owner) && got.grants == acl_grants,
        "get_object_acl must recombine the recorded owner and grant set",
    );
    let head = storage.head_object(b, &acl_key).await.unwrap();
    check(
        head.owner.as_ref() == Some(&acl_owner) && head.acl.grants == acl_grants,
        "head must echo the recorded owner and grant set",
    );
    storage
        .put_object_acl(b, &acl_key, &AclGrants::new())
        .await
        .unwrap();
    let got = storage.get_object_acl(b, &acl_key).await.unwrap();
    check(
        got.grants.is_empty() && got.owner.as_ref() == Some(&acl_owner),
        "put_object_acl must replace the grants and preserve the owner",
    );
    storage
        .put_object_acl(b, &acl_key, &acl_grants)
        .await
        .unwrap();
    let got = storage.get_object_acl(b, &acl_key).await.unwrap();
    check(
        got.grants == acl_grants && got.owner.as_ref() == Some(&acl_owner),
        "put_object_acl must round-trip the new grant set",
    );
    let plain = object::key("acl-plain.bin").unwrap();
    storage.put_object(b, &plain, body(b"x")).await.unwrap();
    let got = storage.get_object_acl(b, &plain).await.unwrap();
    check(
        got.owner.is_none() && got.grants.is_empty(),
        "a plain put_object must record the default-private ACL with no owner",
    );
    let err = into_core_error(storage.get_object_acl(b, &missing).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the ACL of a missing object must be NoSuchKey",
    );

    // Recorded checksums: a plain PUT staged with the server's tee slot
    // records the validated digest as FULL_OBJECT — the kind is fixed by
    // the write path — and head/get echo the record. A commit staged
    // without a tee records none.
    let recorded = object::key("recorded.bin").unwrap();
    let expected = checksum::Recorded {
        part: checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("y/Q5Jg==".into()),
        },
        kind: Type::FullObject,
    };
    let staged = storage
        .stage_body(
            b,
            &recorded,
            body(b"abc"),
            Some(checksum_tee(Algorithm::Crc32, "y/Q5Jg==")),
        )
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &recorded,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        committed.checksum == Some(expected.clone()),
        "a tee-staged commit must record its digest as FULL_OBJECT",
    );
    let head = storage.head_object(b, &recorded).await.unwrap();
    check(
        head.checksum == Some(expected.clone()),
        "head must echo the recorded checksum",
    );
    let get = storage.get_object(b, &recorded, None).await.unwrap();
    check(
        get.info.checksum == Some(expected),
        "get must echo the recorded checksum",
    );
    let staged = storage
        .stage_body(b, &recorded, body(b"abc"), None)
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &recorded,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        committed.checksum.is_none(),
        "a commit staged without a tee must record no checksum",
    );

    // Completed-object parts: a plain put has none, and the listing of a
    // missing object is NoSuchKey (T2-B — the listing mirrors head's
    // existence gate).
    let listed = storage.list_object_parts(b, &hello).await.unwrap();
    check(listed.is_empty(), "a plain put must have no parts");
    let err = into_core_error(storage.list_object_parts(b, &missing).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the parts of a missing object must be NoSuchKey",
    );

    // rename_object: an atomic move — the source is gone, and the
    // destination holds the bytes and the object's metadata (tags,
    // recorded checksum, ETag — a rename is not a fresh object). An
    // existing destination is overwritten.
    let mv_src = object::key("mv-src.bin").unwrap();
    let mv_dst = object::key("mv-dst.bin").unwrap();
    let mv_data = b"rename me, metadata included";
    let mv_tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let mv_checksum = checksum::Recorded {
        part: checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("y/Q5Jg==".into()),
        },
        kind: Type::FullObject,
    };
    let mv_owner = conform_owner();
    let mv_grants = vec![conform_group_read_grant()];
    let staged = storage
        .stage_body(
            b,
            &mv_src,
            body(mv_data.to_vec()),
            Some(checksum_tee(Algorithm::Crc32, "y/Q5Jg==")),
        )
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &mv_src,
            staged,
            mv_tags.clone(),
            Some(&mv_owner),
            &Acl {
                owner: Some(mv_owner.clone()),
                grants: mv_grants.clone(),
            },
        )
        .await
        .unwrap();
    let renamed = storage.rename_object(b, &mv_src, &mv_dst).await.unwrap();
    check(
        renamed.key == mv_dst,
        "rename must return the destination info",
    );
    check(
        renamed.tags == mv_tags && renamed.checksum == Some(mv_checksum.clone()),
        "rename must move the tags and the recorded checksum",
    );
    check(
        renamed.owner.as_ref() == Some(&mv_owner) && renamed.acl.grants == mv_grants,
        "rename must move the owner and the ACL",
    );
    check(renamed.etag == committed.etag, "rename must move the ETag");
    let head = storage.head_object(b, &mv_dst).await.unwrap();
    check(
        head.size == mv_data.len() as u64
            && head.tags == mv_tags
            && head.checksum == Some(mv_checksum.clone())
            && head.owner.as_ref() == Some(&mv_owner)
            && head.acl.grants == mv_grants,
        "the destination must hold the moved metadata",
    );
    let get = storage.get_object(b, &mv_dst, None).await.unwrap();
    check(
        read_body(get.body).await.unwrap() == mv_data,
        "the destination must hold the bytes",
    );
    let err = into_core_error(storage.head_object(b, &mv_src).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the rename source must be gone",
    );
    // An overwriting rename carries the source's bytes and metadata.
    let mv_over = object::key("mv-over.bin").unwrap();
    storage
        .put_object(b, &mv_over, body(b"old content"))
        .await
        .unwrap();
    let renamed = storage.rename_object(b, &mv_dst, &mv_over).await.unwrap();
    check(
        renamed.tags == mv_tags
            && renamed.checksum == Some(mv_checksum.clone())
            && renamed.owner.as_ref() == Some(&mv_owner)
            && renamed.acl.grants == mv_grants,
        "an overwriting rename must carry the source metadata",
    );
    let get = storage.get_object(b, &mv_over, None).await.unwrap();
    check(
        read_body(get.body).await.unwrap() == mv_data,
        "an overwriting rename must replace the destination bytes",
    );
    let head = storage.head_object(b, &mv_over).await.unwrap();
    check(
        head.etag == committed.etag,
        "the overwritten object keeps the moved ETag",
    );
    // Missing source.
    let nowhere = object::key("mv-nowhere.bin").unwrap();
    let err = into_core_error(
        storage
            .rename_object(b, &missing, &nowhere)
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchKey(_)),
        "renaming a missing source must be NoSuchKey",
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
    storage.delete_object(b, &tagged).await.unwrap();
    storage.delete_object(b, &recorded).await.unwrap();
    storage.delete_object(b, &acl_key).await.unwrap();
    storage.delete_object(b, &plain).await.unwrap();
    storage.delete_object(b, &mv_over).await.unwrap();
    storage.delete_bucket(b).await.unwrap();
}

async fn conformance_listing<S: Storage>(storage: &S, b: &bucket::Name) {
    storage
        .create_bucket(b, None, &Acl::default_private(None))
        .await
        .unwrap();
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

    // The listing's Info echoes the recorded owner (the access layer's
    // owner-population read): a commit with an owner is served by the
    // listing; a plain put records none.
    let own_key = object::key("own.txt").unwrap();
    let listed_owner = conform_owner();
    let staged = storage
        .stage_body(b, &own_key, body(b"owned"), None)
        .await
        .unwrap();
    storage
        .commit_object(
            b,
            &own_key,
            staged,
            object::Tags::empty(),
            Some(&listed_owner),
            &Acl::default_private(Some(listed_owner.clone())),
        )
        .await
        .unwrap();
    let page = storage
        .list_objects(list_params(b, "", None, None, 1000))
        .await
        .unwrap();
    let own = page
        .objects
        .iter()
        .find(|o| o.key == own_key)
        .expect("the owned key must be listed");
    check(
        own.owner.as_ref() == Some(&listed_owner),
        "the listing must echo the recorded owner",
    );

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
    storage.delete_object(b, &own_key).await.unwrap();
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
    storage
        .create_bucket(b, None, &Acl::default_private(None))
        .await
        .unwrap();

    // Reserved keys are refused at multipart creation too (FR-020) — the
    // multipart path must not be a backdoor for materializing `.tinio`.
    let reserved = object::key("a/.tinio/b").unwrap();
    let err = into_core_error(
        storage
            .create_multipart_upload(
                b,
                &reserved,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, AccessDenied(_)),
        "multipart create on a reserved key must be AccessDenied",
    );

    // Full lifecycle: create → upload 3 parts → list → complete.
    let big = object::key("big.bin").unwrap();
    let upload = storage
        .create_multipart_upload(
            b,
            &big,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(!upload.upload_id.is_empty(), "upload id must be non-empty");

    // Non-final parts must be >= the 5 MiB minimum the backends enforce
    // at complete (the authoritative rule); the final part may be small.
    let min = MIN_PART_BYTES as usize;
    let parts_data: [Vec<u8>; 3] = [vec![b'a'; min], vec![b'b'; min], b"part-three".to_vec()];
    let mut parts = Vec::new();
    for (i, data) in parts_data.iter().enumerate() {
        let part = storage
            .upload_part(
                b,
                &big,
                &upload.upload_id,
                ((i + 1) as u32).into(),
                body(data.clone()),
                None,
            )
            .await
            .unwrap();
        check(
            part.etag == ETag::from_content(data.as_slice()),
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
        .complete_multipart_upload(b, &big, &upload.upload_id, &completed_parts, None)
        .await
        .unwrap();
    let expected = parts_data.concat();
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

    // The non-final minimum is the S3 rule the backends enforce
    // authoritatively at complete — its failure shape (one byte under →
    // PartTooSmall) is asserted here once, against both backends; the
    // backends' own suites keep only their mechanism-specific angles.
    let min_key = object::key("min.bin").unwrap();
    let failing = storage
        .create_multipart_upload(
            b,
            &min_key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let under = storage
        .upload_part(
            b,
            &min_key,
            &failing.upload_id,
            1.into(),
            body(vec![b'u'; min - 1]),
            None,
        )
        .await
        .unwrap();
    let small = storage
        .upload_part(
            b,
            &min_key,
            &failing.upload_id,
            2.into(),
            body(b"x".to_vec()),
            None,
        )
        .await
        .unwrap();
    let err = into_core_error(
        storage
            .complete_multipart_upload(
                b,
                &min_key,
                &failing.upload_id,
                &[
                    CompletedPart {
                        part_number: under.part_number,
                        etag: under.etag,
                    },
                    CompletedPart {
                        part_number: small.part_number,
                        etag: small.etag,
                    },
                ],
                None,
            )
            .await
            .unwrap_err(),
    );
    check(
        matches!(
            err,
            PartTooSmall {
                part_number: 1,
                min_bytes,
                actual,
            } if min_bytes == MIN_PART_BYTES && actual == min as u64 - 1
        ),
        "a non-final part one byte under the minimum must fail the complete",
    );
    storage
        .abort_multipart_upload(b, &min_key, &failing.upload_id)
        .await
        .unwrap();
    // The client list's last entry is the final part — it has no
    // minimum: a single small part still completes.
    let single_key = object::key("single.bin").unwrap();
    let single_upload = storage
        .create_multipart_upload(
            b,
            &single_key,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let only = storage
        .upload_part(
            b,
            &single_key,
            &single_upload.upload_id,
            1.into(),
            body(b"tiny".to_vec()),
            None,
        )
        .await
        .unwrap();
    storage
        .complete_multipart_upload(
            b,
            &single_key,
            &single_upload.upload_id,
            &[CompletedPart {
                part_number: only.part_number,
                etag: only.etag,
            }],
            None,
        )
        .await
        .unwrap();
    storage.delete_object(b, &single_key).await.unwrap();

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
    let u = storage
        .create_multipart_upload(
            b,
            &empty_mp,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let err = into_core_error(
        storage
            .complete_multipart_upload(b, &empty_mp, &u.upload_id, &[], None)
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
    let busy_upload = storage
        .create_multipart_upload(
            b,
            &busy,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
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
        .create_multipart_upload(
            b,
            &abort_bin,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    storage
        .upload_part(
            b,
            &abort_bin,
            &upload2.upload_id,
            1.into(),
            body(b"x"),
            None,
        )
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
    let upload3 = storage
        .create_multipart_upload(
            b,
            &pending,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
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

    // Checksum persistence through the contract (spec 2026-08-31): the
    // backends store and return what the server hands over.
    let cs_big = object::key("checksum.bin").unwrap();
    let cs_spec = checksum::Upload {
        algorithm: Algorithm::Crc32,
        r#type: Some(Type::FullObject),
    };
    let cs_upload = storage
        .create_multipart_upload(
            b,
            &cs_big,
            Some(cs_spec.clone()),
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        cs_upload.checksum == Some(cs_spec.clone()),
        "create must echo the persisted checksum spec",
    );
    let fetched = storage
        .get_multipart_upload(b, &cs_big, &cs_upload.upload_id)
        .await
        .unwrap();
    check(
        fetched.checksum.as_ref().map(|c| c.algorithm) == Some(Algorithm::Crc32),
        "get_multipart_upload must return the persisted algorithm",
    );
    // The tee slot (what the server hands the backend): the digest
    // commits atomically with the part row.
    let value = checksum::Value("y/Q5Jg==".into());
    let slot = checksum_tee(Algorithm::Crc32, "y/Q5Jg==");
    let cs_part = storage
        .upload_part(
            b,
            &cs_big,
            &cs_upload.upload_id,
            1.into(),
            body(b"abc"),
            Some(slot),
        )
        .await
        .unwrap();
    check(
        cs_part.checksum
            == Some(checksum::Part {
                algorithm: Algorithm::Crc32,
                value: value.clone(),
            }),
        "upload_part must return the checksum committed with the part",
    );
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: cs_big.clone(),
            upload_id: cs_upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(
        listing.parts[0].checksum
            == Some(checksum::Part {
                algorithm: Algorithm::Crc32,
                value: value.clone(),
            }),
        "list_parts must echo the stored part checksum",
    );
    // A re-upload without a tee slot clears the stale checksum row
    // (the atomic commit writes-or-clears in the part's transaction).
    storage
        .upload_part(
            b,
            &cs_big,
            &cs_upload.upload_id,
            1.into(),
            body(b"xyz"),
            None,
        )
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: cs_big.clone(),
            upload_id: cs_upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(
        listing.parts[0].checksum.is_none(),
        "a re-uploaded part must not keep a stale checksum",
    );
    // A re-upload with a NEW slot overwrites the row — the two rows
    // commit in one transaction, so no CAS can ever go stale.
    let new_slot = checksum_tee(Algorithm::Crc32, "AAAAAAAAAAAAAAAAAAAAAA==");
    storage
        .upload_part(
            b,
            &cs_big,
            &cs_upload.upload_id,
            1.into(),
            body(b"abc"),
            Some(new_slot),
        )
        .await
        .unwrap();
    let listing = storage
        .list_parts(ListPartsParams {
            bucket: b.clone(),
            key: cs_big.clone(),
            upload_id: cs_upload.upload_id.clone(),
            max_parts: 1000,
            part_number_marker: None,
        })
        .await
        .unwrap();
    check(
        listing.parts[0].checksum
            == Some(checksum::Part {
                algorithm: Algorithm::Crc32,
                value: checksum::Value("AAAAAAAAAAAAAAAAAAAAAA==".into()),
            }),
        "a re-uploaded part's checksum row follows the new content",
    );
    // Abort drains the new tables (get_multipart_upload → NoSuchUpload).
    storage
        .abort_multipart_upload(b, &cs_big, &cs_upload.upload_id)
        .await
        .unwrap();
    let err = into_core_error(
        storage
            .get_multipart_upload(b, &cs_big, &cs_upload.upload_id)
            .await
            .unwrap_err(),
    );
    check(
        matches!(err, NoSuchUpload(_)),
        "abort must drain the checksum rows",
    );
    // The completed object's retained parts (GetObjectAttributes
    // ObjectParts): big.bin's composition rows are served in part-number
    // order with the uploaded sizes — no per-part checksums (its parts
    // were uploaded without the tee).
    let rows = storage.list_object_parts(b, &big).await.unwrap();
    check(
        rows.iter()
            .map(|p| u32::from(p.part_number))
            .collect::<Vec<_>>()
            == [1, 2, 3],
        "the retained parts must be the completion's, in number order",
    );
    check(
        rows.iter().map(|p| p.size).collect::<Vec<_>>() == [min as u64, min as u64, 10],
        "the retained part sizes must match the uploaded parts",
    );
    check(
        rows.iter().all(|p| p.checksum.is_none()),
        "parts uploaded without the tee must carry no checksum",
    );

    // Multipart write path: the create-time tags — persisted in the
    // upload state, passed back through the completion — land on the
    // object, and the composite the interface computed records with it
    // (kind COMPOSITE), echoed by head and get.
    let attrs = object::key("attrs.bin").unwrap();
    let create_tags = object::Tags::from_pairs([
        ("env".into(), "prod".into()),
        ("owner".into(), "harness".into()),
    ])
    .unwrap();
    let upload = storage
        .create_multipart_upload(
            b,
            &attrs,
            None,
            create_tags.clone(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        upload.tags == create_tags,
        "the upload state must persist the create-time tags",
    );
    let first = storage
        .upload_part(
            b,
            &attrs,
            &upload.upload_id,
            1.into(),
            body(vec![b'q'; min]),
            Some(checksum_tee(Algorithm::Crc32, "y/Q5Jg==")),
        )
        .await
        .unwrap();
    let second = storage
        .upload_part(
            b,
            &attrs,
            &upload.upload_id,
            2.into(),
            body(b"tail"),
            Some(checksum_tee(Algorithm::Crc32, "NhCmhg==")),
        )
        .await
        .unwrap();
    let composite = checksum::Recorded {
        part: checksum::Part {
            algorithm: Algorithm::Crc32,
            value: checksum::Value("AAAAAAAAAAAAAAAAAAAAAA==".into()),
        },
        kind: Type::Composite,
    };
    let completed = storage
        .complete_multipart_upload(
            b,
            &attrs,
            &upload.upload_id,
            &[
                CompletedPart {
                    part_number: first.part_number,
                    etag: first.etag.clone(),
                },
                CompletedPart {
                    part_number: second.part_number,
                    etag: second.etag.clone(),
                },
            ],
            Some(composite.clone()),
        )
        .await
        .unwrap();
    check(
        completed.tags == create_tags,
        "the completion must apply the create-time tags",
    );
    check(
        completed.checksum == Some(composite.clone()),
        "the completion must record the interface's composite",
    );
    let head = storage.head_object(b, &attrs).await.unwrap();
    check(
        head.tags == create_tags,
        "head must serve the completed tags",
    );
    check(
        head.checksum == Some(composite.clone()),
        "head must echo the recorded composite",
    );
    let get = storage.get_object(b, &attrs, None).await.unwrap();
    check(
        get.info.checksum == Some(composite.clone()),
        "get must echo the recorded composite",
    );
    // The retained rows carry each part's tee checksum and uploaded size.
    let rows = storage.list_object_parts(b, &attrs).await.unwrap();
    check(
        rows.iter().map(|p| p.size).collect::<Vec<_>>() == [min as u64, 4],
        "the retained part sizes must match the uploaded parts",
    );
    check(
        rows[0].checksum
            == Some(checksum::Part {
                algorithm: Algorithm::Crc32,
                value: checksum::Value("y/Q5Jg==".into()),
            })
            && rows[1].checksum
                == Some(checksum::Part {
                    algorithm: Algorithm::Crc32,
                    value: checksum::Value("NhCmhg==".into()),
                }),
        "the retained rows must carry the per-part tee checksums",
    );

    // OBJECT_PARTS lifecycle: the retained list is always exactly the
    // last write's — a copy never inherits the source's rows (nor keeps
    // the overwritten destination's), a re-completion replaces them, and
    // commit and delete leave none behind.
    let overwrite = object::key("overwrite.bin").unwrap();
    let upload = storage
        .create_multipart_upload(
            b,
            &overwrite,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let part = storage
        .upload_part(
            b,
            &overwrite,
            &upload.upload_id,
            1.into(),
            body(b"tiny"),
            None,
        )
        .await
        .unwrap();
    storage
        .complete_multipart_upload(
            b,
            &overwrite,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .unwrap();
    let rows = storage.list_object_parts(b, &overwrite).await.unwrap();
    check(
        rows.len() == 1 && u32::from(rows[0].part_number) == 1,
        "a completion must retain its own parts",
    );
    storage
        .copy_object(
            b,
            &attrs,
            b,
            &overwrite,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    let rows = storage.list_object_parts(b, &overwrite).await.unwrap();
    check(
        rows.is_empty(),
        "a copy must not inherit the source's parts — nor keep the destination's",
    );
    storage.delete_object(b, &overwrite).await.unwrap();

    // A rename moves the rows with the record — the moved object keeps
    // its tags, its recorded composite, and its retained parts.
    let moved = object::key("moved.bin").unwrap();
    let renamed = storage.rename_object(b, &attrs, &moved).await.unwrap();
    check(
        renamed.tags == create_tags && renamed.checksum == Some(composite.clone()),
        "rename must move the completed object's metadata",
    );
    let rows = storage.list_object_parts(b, &moved).await.unwrap();
    check(
        rows.len() == 2 && rows[0].checksum.is_some() && rows[1].checksum.is_some(),
        "the retained parts must migrate with the record",
    );
    let err = into_core_error(storage.head_object(b, &attrs).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the rename source must be gone",
    );
    let err = into_core_error(storage.list_object_parts(b, &attrs).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "a renamed-away object's parts must answer NoSuchKey",
    );

    // A re-completion over the moved object replaces its rows (no
    // accumulation) and its metadata (the completion's own tags and
    // checksum, not the moved ones).
    let upload = storage
        .create_multipart_upload(
            b,
            &moved,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let part = storage
        .upload_part(b, &moved, &upload.upload_id, 1.into(), body(b"again"), None)
        .await
        .unwrap();
    storage
        .complete_multipart_upload(
            b,
            &moved,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .unwrap();
    let rows = storage.list_object_parts(b, &moved).await.unwrap();
    check(
        rows.len() == 1 && u32::from(rows[0].part_number) == 1,
        "a re-completion must replace the retained rows",
    );
    let head = storage.head_object(b, &moved).await.unwrap();
    check(
        head.tags.is_empty() && head.checksum.is_none(),
        "a re-completion must replace the moved metadata",
    );

    // A plain commit over the multipart object replaces the rest of the
    // multipart state — tags, recorded checksum, retained parts.
    let commit_tags = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
    let staged = storage
        .stage_body(b, &moved, body(b"flat"), None)
        .await
        .unwrap();
    let committed = storage
        .commit_object(
            b,
            &moved,
            staged,
            commit_tags.clone(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    check(
        committed.tags == commit_tags,
        "a commit over a multipart object must record its own tags",
    );
    let head = storage.head_object(b, &moved).await.unwrap();
    check(
        head.tags == commit_tags && head.checksum.is_none(),
        "a commit over a multipart object must replace tags and checksum",
    );
    let rows = storage.list_object_parts(b, &moved).await.unwrap();
    check(
        rows.is_empty(),
        "a commit over a completed multipart object must clear its parts",
    );

    // Delete drains the rows with the object.
    storage.delete_object(b, &moved).await.unwrap();
    let err = into_core_error(storage.list_object_parts(b, &moved).await.unwrap_err());
    check(
        matches!(err, NoSuchKey(_)),
        "the parts of a deleted object must be gone with it",
    );

    // The completion applies the create-time owner/ACL: the CMU
    // records the pair in the upload state and the completion consumes
    // it (the tags precedent — never re-ferried through the interface),
    // so the completed object's owner is the CMU's, not the
    // completing principal's (the contract has no such parameter).
    let owned = object::key("owned.bin").unwrap();
    let cmu_owner = conform_owner();
    let cmu_grants = vec![conform_group_read_grant()];
    let upload = storage
        .create_multipart_upload(
            b,
            &owned,
            None,
            object::Tags::empty(),
            Some(&cmu_owner),
            &Acl {
                owner: Some(cmu_owner.clone()),
                grants: cmu_grants.clone(),
            },
        )
        .await
        .unwrap();
    check(
        upload.owner.as_ref() == Some(&cmu_owner) && upload.acl.grants == cmu_grants,
        "the upload state must carry the create-time owner and grant set",
    );
    let part = storage
        .upload_part(b, &owned, &upload.upload_id, 1.into(), body(b"tiny"), None)
        .await
        .unwrap();
    let completed = storage
        .complete_multipart_upload(
            b,
            &owned,
            &upload.upload_id,
            &[CompletedPart {
                part_number: part.part_number,
                etag: part.etag,
            }],
            None,
        )
        .await
        .unwrap();
    check(
        completed.owner.as_ref() == Some(&cmu_owner) && completed.acl.grants == cmu_grants,
        "the completion must apply the create-time owner and grant set",
    );
    let got = storage.get_object_acl(b, &owned).await.unwrap();
    check(
        got.owner.as_ref() == Some(&cmu_owner) && got.grants == cmu_grants,
        "the completed object's ACL must come from the upload state",
    );
    storage.delete_object(b, &owned).await.unwrap();

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

/// The wait timeout in seconds — `TINIO_TEST_WAIT_TIMEOUT_SECS` overrides the
/// 60 s default (read once per call; unset or unparsable → default).
/// A knob for slow runners: fs-drain waits (tombstone reclaim under CI
/// load / Defender scans / contended machines) starve without it.
///
/// Public so a harness that owns its own poll loop — one whose failure path
/// must kill a child and quote its pipes, which `wait_for` cannot express —
/// still honours this one knob and default instead of growing a second.
pub fn wait_timeout_secs() -> u64 {
    env::var("TINIO_TEST_WAIT_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(60)
}

/// Poll `cond` until true or the deadline passes (the test runners'
/// workers are asynchronous, so assertions must wait; the poll only
/// costs on failure). The shared home of the helper formerly duplicated
/// across tinio-fs testutil and the tinio-server pipeline tests (F30).
pub async fn wait_for(mut cond: impl FnMut() -> bool) {
    let secs = wait_timeout_secs();
    let deadline = TokioInstant::now() + Duration::from_secs(secs);
    while !cond() {
        assert!(
            TokioInstant::now() < deadline,
            "condition not met within {secs} s"
        );
        sleep(Duration::from_millis(2)).await;
    }
}

/// End the confined grandchild a `kill` leaves behind on macOS.
///
/// **A launcher leg's own cleanup, and macOS's alone.** The launcher is what a
/// leg kills; the confined server is the launcher's *child*, and on macOS
/// nothing forwards the launcher's death to it — no job object, as Windows has
/// — so the server survives, keeps the served root open, and the next leg reads
/// to EOF and hangs instead of reporting (measured on macOS 15.8, 2026-09-25).
/// The pattern is the leg's own served root, which is unique to it and is named
/// in both processes' argv.
///
/// **A caller must have confined something before it sweeps.** This is a
/// `pkill -f` over the whole process table, so a leg that spawned no confined
/// server would be matching a pattern it never created.
///
/// **The `cfg` is why the sweep is not a no-op elsewhere.** Every platform but
/// macOS needs no sweep — the process a kill ends *is* the server — so on any
/// other target this function does not exist and a call site cannot forget the
/// gate.
///
/// **It lives here for [`wait_for_exit`]'s reason**: the rule was spelled once
/// in `tinio-server`'s e2e module and once in the e2e crate's CLI harness, each
/// documented as unavoidable because two `tests/` trees are not importable from
/// each other. A crate both of them already dev-depend on is importable from
/// both, so the two copies become one definer and two re-exports.
#[cfg(target_os = "macos")]
pub fn sweep_confined_grandchild(root: &Path) {
    // A root that is not UTF-8 cannot be matched by a command line, and the legs
    // that call this build theirs from a scratch directory of their own — so the
    // guard is a fact about the fixture rather than a silent skip.
    let Some(root) = root.to_str() else {
        return;
    };
    let _ = Command::new("pkill")
        .args(["-f", &format!("tinio server {root}")])
        .status();
}

/// The guard a harness's spawn sites go through, so that a test process which dies without
/// unwinding takes its servers with it: a job object on Windows, the parent-death signal on Linux.
///
/// **Re-exported rather than defined here**, for [`kill_and_reap`]'s reason and one more: the
/// module is deliberately outside this feature so that its own tests run in the default graph
/// (`crate::job`), while every harness that spawns a server reaches `testing` already. A caller
/// therefore spells one path — `tinio_util::testing::job::spawn` — whichever half of the two it
/// stands on. The mechanism, its per-thread caveat on Linux and the target that has none are the
/// module's own doc.
pub use crate::job;
/// Kill `child` and reap it, ignoring both answers — the silent sibling of [`wait_for_exit`], for
/// every path that has already decided what to report.
///
/// **Re-exported rather than defined here**, because it needs nothing this feature provides: the
/// definition is [`crate::process`]'s, beside nothing else, so a production caller (the daemon
/// parent that kills a child it has just reported as unready) reaches it without turning `testing`
/// — and `tinio-core` behind it — on. A test tree still spells it
/// `tinio_util::testing::kill_and_reap`, which is where the rule's other readers live.
pub use crate::process::kill_and_reap;

/// Wait for `child` to exit, or kill it and fail the test rather than block on
/// it.
///
/// **Every spawned process is waited for or killed on every path, red ones
/// included**, and this is the one function that rule is spelled in — which is
/// why it is shared rather than copied per suite. The failing path kills before
/// it panics: a child left running keeps `target/<profile>/deps/<suite>-<hash>`
/// open for the rest of the run, which breaks a later build with `LNK1104` on
/// Windows, and it keeps writing its own libtest output into the parent's.
///
/// **It lives here because the launcher platforms build one crate each.** The
/// rule was `tinio-sandbox-windows::testing`'s, and that crate's root is
/// `#![cfg(target_os = "windows")]` — so a macOS leg could not import it and
/// re-spelled the deadline/poll/kill rule for itself (the server's macOS e2e leg
/// and the macOS crate's own suites), which is the drift one definer exists to
/// remove. This module already owns the two knobs such a wait is written
/// against — [`wait_timeout_secs`] and [`wait_for`]'s poll — so the rule joins
/// them, and `tinio-sandbox-windows::testing` re-exports the name so its own
/// callers (and the server harness's wrapper) keep working unchanged.
///
/// `try_wait` polls rather than `wait_with_output` waiting, which is the price
/// of the bound: nothing here needs the child's captured output, since a child
/// that inherits this process's streams has already written it where the reader
/// of a red log will see it. `what` names the process whose exit is being waited
/// for, because a suite has more than one.
pub fn wait_for_exit(child: &mut Child, within: Duration, what: &str) -> ExitStatus {
    poll_exit(child, within).unwrap_or_else(|| {
        kill_and_reap(child);
        panic!(
            "{what} did not exit within {within:?} — whatever it did is in the output above; \
             it has been killed"
        )
    })
}

/// The bounded `try_wait` poll both wait shapes are written on: `Some` when the child exits inside
/// `within`, `None` when the deadline passes with it not seen to exit.
///
/// **Re-exported rather than defined here**, for [`kill_and_reap`]'s reason one step further: this
/// poll needs nothing the `testing` feature provides — `std` and a child handle are the whole of it
/// — so its home is `crate::process`, beside the kill, where a caller that must not turn the
/// feature on can still reach it. `tinio-util`'s own job guard is the first such caller: its module
/// is deliberately outside this feature so that its tests run in the default graph, and this is the
/// bounded wait those tests are written against.
///
/// **Why one poll serves two endings lives with the definition** (`crate::process::poll_exit`):
/// the argument is about the shipped module's surface, so it is written once, there.
pub use crate::process::poll_exit;

/// Whether `path` exists before `within` passes, polling every `poll` — the sync sibling of
/// [`wait_for`] and one family with [`poll_exit`].
///
/// **It waits on an event another process produces** — a marker file a spawned child writes, the
/// state file a launcher publishes — so the answer is a `bool` and the failure path stays with the
/// caller. [`wait_for`] cannot serve those callers: its panic is all a suite gets back, and a leg
/// whose child is still alive has to kill it before it fails (the rule [`wait_for_exit`] states),
/// which is why the verdict is returned rather than asserted here.
///
/// **Three suites spelled this loop with an interval of their own** — `tinio-sandbox-macos`'s
/// handover, `tinio-sandbox-windows`'s signals and `tinio-server`'s macOS e2e — so the interval is a
/// parameter rather than a constant: it is the resolution of the window the caller writes down, and
/// the three legitimately differ. The final `exists()` re-read after the loop is deliberate, so a
/// path that appeared inside the last sleep is still answered for.
pub fn wait_for_path(path: &Path, within: Duration, poll: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(poll);
    }
    path.exists()
}

/// Shared §7's check, as one assertion: the event carries `step` as a rendered
/// field.
///
/// **A rendered field and not a substring of the whole line**, which is the
/// distinction the core's own recorder test states at length: the operation's
/// name appears inside the message and inside any error the same call returns,
/// so `contains("jail_root")` would pass for an event whose `step` field said
/// something else. The log is in the failure message, because a recording that
/// came out empty is the other half of what can go wrong here.
///
/// **`with_ansi(false)` on the recording subscriber is the prerequisite this
/// assertion rests on**, and it is measured rather than assumed: the default
/// subscriber interleaves escape sequences into the field *syntax* — `step=`
/// arrives as `\x1b[3mstep\x1b[0m\x1b[2m=\x1b[0m` — so `step="…"` never appears
/// as such without it. The value substrings an earlier generation of these tests
/// checked survived that colouring; the field names do not.
pub fn assert_step(logged: &str, step: &str) {
    assert!(
        logged.contains(&format!("step=\"{step}\"")),
        "no `step=\"{step}\"` in:\n{logged}"
    );
}

/// The recorded line carrying `needle` is logged at `level`: **the level check a recording can no
/// longer make by filtering.**
///
/// **Why it is an assertion and not the recording's own filter.** The device records at `DEBUG` — one
/// level for one device, because a sibling crate's leg asserts a `debug!` event — so a recording no
/// longer *selects* the level `serve`'s subscriber enables, and a leg whose event was moved to
/// `debug!` would still be recorded and still pass. This puts that property back and makes it
/// stronger: the line's own level is read rather than inferred from what a filter let through, so a
/// **downgrade** to `debug!` and a **promotion** away from the level `serve` enables both fail it,
/// where the filter could only ever catch the downgrade.
///
/// **The level is read off the line the needle is on, never off the whole recording.** A recording
/// holds every line its body emitted, so a text-wide search would answer a question about a different
/// event than the caller asked. The rendered level is a bare token at the head of the line
/// (`… ERROR <target>: <message>`, measured on `tracing-subscriber` 0.3.23 with `with_ansi(false)`),
/// which is why this is a `contains` on that line and not a parse — and why `with_ansi(false)` is the
/// prerequisite [`assert_step`] documents.
pub fn assert_recorded_at(logged: &str, needle: &str, level: Level) {
    let line = logged
        .lines()
        .find(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no recorded line carries {needle:?} in:\n{logged}"));
    assert!(
        line.contains(level.as_str()),
        "the line carrying {needle:?} is not at {level}: {line}"
    );
}

/// No recorded line is at `level` or a more severe one: **"a subscriber at this level prints
/// nothing"**, the assertion a leg whose subject is a *silent* path needs, and [`assert_recorded_at`]'s
/// inverse.
///
/// **Why it cannot be `logged.is_empty()`.** The device records at `DEBUG` (one level for one device),
/// so a path an `INFO` subscriber prints nothing for still leaves its `debug!` lines in the buffer.
/// What an operator sees is decided by the *level* of those lines rather than by their absence, so that
/// is what is asserted — the same move [`assert_recorded_at`] makes, in the other direction: a `warn!`
/// quietly demoted to `debug!` no longer empties the buffer, and this is what still fails for it. The
/// doc there carries why the recording cannot make the check by filtering.
///
/// **The tokens are spelled out rather than derived from `Level`'s own ordering**, which is *inverted
/// on purpose*: `tracing` orders levels by verbosity (`ERROR < WARN < INFO < DEBUG < TRACE`), so
/// `rendered >= level` in Rust would mean "quieter than or equal to", the opposite of what a reader of
/// this assertion takes it for.
///
/// The rendered level is a bare token at the head of each line (`… ERROR <target>: …`, measured), so a
/// line whose message happens to spell a level name is answered for as if it were that level. Every
/// caller so far asserts about a path whose message is the caller's own prose; a needle with a level
/// name in it is what [`assert_recorded_at`] is for.
pub fn assert_recorded_below(logged: &str, level: Level) {
    const SEEN: [&str; 5] = ["TRACE", "DEBUG", "INFO", "WARN", "ERROR"];
    let visible: &[&str] = match level {
        Level::TRACE => &SEEN[..],
        Level::DEBUG => &SEEN[1..],
        Level::INFO => &SEEN[2..],
        Level::WARN => &SEEN[3..],
        Level::ERROR => &SEEN[4..],
    };
    for line in logged.lines() {
        assert!(
            !visible.iter().any(|token| line.contains(token)),
            "a line is at {level} or above, which a subscriber at {level} prints: {line}\nin:\n{logged}"
        );
    }
}

/// A reader that fails the test if anything reads from it.
///
/// **An EOF-returning reader cannot express this rule**: `io::empty()` and a
/// `Cursor` over an empty buffer behave identically whether or not they are read,
/// so a test built on one passes for a rule that reads stdin and one that does
/// not. This reader is hostile to being read at all, which is the property under
/// test — `adopt` must not read a stdin nothing launched it with, because an
/// operator's terminal blocks rather than answering.
pub struct Untouchable;

impl Read for Untouchable {
    fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
        panic!("`adopt` read stdin without a listener element in argv");
    }
}

/// The one way past a precondition this host cannot meet, and it is a deliberate
/// opt-in: `TINIO_SANDBOX_ALLOW_SKIP`.
///
/// **A suite that certifies a confinement does not get to skip in silence.** The
/// rule, the variable and its spelling are P1's and Task 11's
/// (`crates/tinio-sandbox-linux/tests/jail.rs`,
/// `crates/tinio-server/tests/sandbox_e2e_windows.rs`), kept identical so one habit
/// covers all of them. It covers the *host's* preconditions alone and nothing the
/// product does: a failure after the first spawn is a suite's subject, and no value
/// of this variable makes it pass.
pub const ALLOW_SKIP: &str = "TINIO_SANDBOX_ALLOW_SKIP";

/// Whether the operator has deliberately opted out of a precondition this host
/// cannot meet (see [`ALLOW_SKIP`]).
pub fn skip_allowed() -> bool {
    env::var(ALLOW_SKIP).is_ok_and(|value| !value.is_empty())
}

/// `what` cannot run on this host: **fail here, or let the operator opt out and let
/// the caller return early**.
///
/// **This is the one arbitration of [`ALLOW_SKIP`] every suite that skips shares**,
/// and the three details that make a skip honest are its shape rather than its
/// callers':
///
/// - **The assertion is evaluated before the `eprintln!`.** A skip printed by
///   libtest is swallowed without `--nocapture`, so a host that cannot run the leg
///   and did not opt out has to read as a *failure* — otherwise the suite reports a
///   green run that certified nothing, which is exactly the defect this rule exists
///   for.
/// - **The panic names the variable and the remedy.** `what` and `reason` are the
///   caller's (which leg, which capability of the machine answered no), and the
///   message always carries [`ALLOW_SKIP`]'s name plus whatever other way out the
///   caller knows, `remedy`, appended verbatim.
/// - **The skip branch prints and returns**, so the caller's shape is
///   `host_cannot(…); return;` — or `host_cannot(…); false` where the leg's own
///   answer is the `bool`: the leg then asserts nothing about the product, which
///   is what the operator asked for.
///
/// **It answers `()` rather than a `bool` that is `false` on every returning
/// path.** The panic is the other path, so a returned value carries no
/// information, and one that looked like an answer would invite a caller to
/// write `if host_cannot(…) { … }` — the skip inverted, silently.
pub fn host_cannot(what: &str, reason: &str, remedy: &str) {
    assert!(
        skip_allowed(),
        "this host cannot run {what} ({reason}), so this leg would assert nothing. Set \
         {ALLOW_SKIP}=1 to pass deliberately on a host that genuinely cannot{remedy}"
    );
    eprintln!("skipping: this host cannot run {what} ({reason}) and {ALLOW_SKIP} is set{remedy}");
}

/// The lines a child has written to one pipe so far, shared with that pipe's draining thread (the
/// only writer).
type Lines = Arc<Mutex<Vec<String>>>;

/// Drain `pipe` line by line into `lines` until EOF (the child exits). A pipe nobody reads fills up
/// and blocks the child, so both pipes are always drained, whether or not the leg looks at them.
fn drain(pipe: impl Read + Send + 'static, lines: Lines) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut reader = BufReader::new(pipe);
        let mut raw = Vec::new();
        while reader.read_until(b'\n', &mut raw).is_ok_and(|n| n > 0) {
            // Lossy: a diagnostic that is not UTF-8 drops bytes from the record, never the rest of
            // the pipe.
            let line = String::from_utf8_lossy(&raw);
            lines
                .lock()
                .expect("the pipe buffer")
                .push(line.trim_end().to_string());
            raw.clear();
        }
    })
}

/// A spawned child with both pipes drained into shared buffers, killed and reaped when it drops.
///
/// **Both pipes are drained whether or not the leg reads them**, and a thread each is why: a pipe
/// nobody reads fills up and blocks the child, so a harness that wanted only stderr would hang the
/// server it is observing once that server logged more than a pipe holds. The buffers are the two
/// the leg reads through [`stdout_lines`](Self::stdout_lines), [`stderr_text`](Self::stderr_text)
/// and [`stderr_containing`](Self::stderr_containing).
///
/// **Killed and reaped on every path out of the leg, the panic path included**, because `Drop`
/// runs [`stop`](Self::stop) — an assertion failure must not leave a server holding its listener,
/// its jail and the served root's state lock for the rest of the run. An explicit call is only
/// about ordering: it gets the complete pipes (their write ends close at the child's death, so the
/// drains finish) before the leg's final assertions.
///
/// **Why the std half is shared and the rest is not.** Two `tests/` trees cannot import from each
/// other, and both spawn the real `tinio server` and read its pipes: the read-only suite
/// (`tinio-server/tests/serve_read_only.rs`) and the sandbox-channel harness beside it
/// (`tests/support/serve_spawn.rs`) held a byte-identical copy of this — the buffers, the drain,
/// the exit wait and the kill — and the copies were equal only while an author remembered the
/// other. What both of them add on top, and what stays in those trees, is the decoding of the
/// readiness event, which is `tinio_sandbox::Event`'s: it would cost every `testing` build the
/// whole sandbox stack to hold it here, for a type only those two binaries read.
///
/// The child inherits this process's environment; a harness that must remove or set a variable does
/// it on the [`Command`] before [`spawn`](Self::spawn).
///
/// **The spawn goes through the job guard** ([`crate::job::spawn`]), which is what makes the legs
/// that hold one of these — `tinio-server`'s `serve_read_only` and the sandbox-channel harness
/// beside it — take their servers with them when the test process dies without unwinding. The
/// guard is the backstop for that path; the `Drop` below stays what reaps on every path that
/// returns. What the guard covers, per target, is its module's doc.
pub struct PipedChild {
    child: Child,
    stdout: Lines,
    stderr: Lines,
    /// The draining threads, taken by `stop`.
    drains: Vec<JoinHandle<()>>,
}

impl PipedChild {
    /// Spawn `cmd` and start the two draining threads.
    ///
    /// **Both pipes are set here rather than by the caller**, because draining them is this type's
    /// contract: a caller that forgot `Stdio::piped()` would get a child whose output nobody reads,
    /// which is the hang the type exists to prevent. A spawn failure panics — every caller is a
    /// test or a harness, and a binary that cannot be spawned is the leg's own precondition.
    ///
    /// **The child is spawned through the job guard** ([`crate::job::spawn`]) rather than through
    /// the [`Command`] directly, which is the one spelling every harness's spawn site uses: the
    /// guard is what covers a test process that dies without unwinding, and a leg that spawns a
    /// server any other way is exactly the orphan it exists to prevent. The call is made here, on
    /// the caller's thread, because the guard is only as wide as the thread it was taken on.
    pub fn spawn(cmd: &mut Command) -> Self {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = job::spawn(cmd).unwrap_or_else(|e| {
            panic!("cannot spawn {}: {e}", cmd.get_program().to_string_lossy())
        });
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let drains = vec![
            drain(
                child.stdout.take().expect("stdout is piped"),
                Arc::clone(&stdout),
            ),
            drain(
                child.stderr.take().expect("stderr is piped"),
                Arc::clone(&stderr),
            ),
        ];
        Self {
            child,
            stdout,
            stderr,
            drains,
        }
    }

    /// The child itself, for the polls a leg writes against its own readiness protocol
    /// (`try_wait`, a signal, a pid).
    pub fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    /// Wait for the child to exit on its own — the legs that expect a failure, never a server —
    /// and return its status.
    ///
    /// Bounded by [`wait_timeout_secs`], and the deadline kills before it panics: a child that
    /// never exits is the failure mode every `PipedChild` caller has, and the stderr quoted beside
    /// the message is the evidence it left.
    pub async fn exited(&mut self) -> ExitStatus {
        let timeout = wait_timeout_secs();
        let deadline = Instant::now() + Duration::from_secs(timeout);
        loop {
            if let Some(status) = self.child.try_wait().expect("the child's status") {
                // Complete the pipes before the caller reads them: the child is gone, so its ends
                // are closed and the drains cannot block.
                self.join_drains();
                return status;
            }
            if Instant::now() >= deadline {
                self.stop();
                panic!(
                    "the child did not exit within {timeout} s (this leg expects a failure); \
                     stderr:\n{}",
                    self.stderr_text()
                );
            }
            sleep(Duration::from_millis(2)).await;
        }
    }

    /// Kill and reap the child now, then join the draining threads (their pipes reach EOF at the
    /// child's death). `Drop` runs the same path, so an assertion failure later still reaps — an
    /// explicit call just gets the complete pipes before the leg's final assertions.
    pub fn stop(&mut self) {
        kill_and_reap(&mut self.child);
        self.join_drains();
    }

    /// Join the draining threads, taking the handles so a second `stop` cannot join them twice.
    pub fn join_drains(&mut self) {
        for handle in self.drains.drain(..) {
            let _ = handle.join();
        }
    }

    /// Everything the child has written to stdout so far.
    pub fn stdout_lines(&self) -> Vec<String> {
        self.stdout.lock().expect("the stdout buffer").clone()
    }

    /// Everything the child has written to stderr so far, line-joined.
    pub fn stderr_text(&self) -> String {
        self.stderr.lock().expect("the stderr buffer").join("\n")
    }

    /// The child's stderr once it carries `needle`, waiting (bounded) while it does not. A separate
    /// thread drains the pipe, so a line the child wrote is not necessarily recorded the instant
    /// this thread looks — a live child's stderr has no EOF to synchronize on.
    pub async fn stderr_containing(&self, needle: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(wait_timeout_secs());
        loop {
            let text = self.stderr_text();
            if text.contains(needle) || Instant::now() >= deadline {
                return text;
            }
            sleep(Duration::from_millis(2)).await;
        }
    }
}

impl Drop for PipedChild {
    fn drop(&mut self) {
        // See [`PipedChild::stop`] and the type's doc.
        self.stop();
    }
}

/// An owned `Write` sink for the fmt layers under test (the log.rs and
/// pipeline.rs test pattern — F32: one definition, three copies removed).
///
/// **Right where the test reaps every thread it starts**, which is the shape both of its callers
/// have. A recording whose process **forks** underneath it cannot use one: take [`ThreadLocalBuf`],
/// which carries the measurement and the lock-free alternative.
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

thread_local! {
    /// What this thread is recording, or `None` when it is not recording at all.
    ///
    /// **A thread-local rather than a shared buffer behind a lock, and the reason is a fork** — see
    /// [`ThreadLocalBuf`] for what the fork does to the other shape and what this one gives up for it.
    static THREAD_BUFFER: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

/// The sink a recording writes through when the process can **fork** underneath it: the lock-free twin
/// of [`SharedBuf`].
///
/// **Thread-local rather than one buffer behind a `Mutex`, and the reason is a fork.** An event written
/// through a `Mutex` takes a critical section a sibling thread may have held at the instant the fork
/// happened, and the child inherits it locked with nobody left to unlock it: on Linux the in-place
/// confinement forks inside its availability probe, and the first shape of that crate's recorder — the
/// one this type replaces — put the forked child on that lock until the probe's five-second deadline,
/// measured as the probe's own test failing in five seconds. A thread-local takes no lock at all on the
/// write path, and the forking case does not even reach the buffer: a forking test is not a recording
/// one, so the child inherits `None` and its event is dropped without allocating.
///
/// **What that gives up, against [`SharedBuf`]**: a recording reads the thread that made it and no
/// other, so an event emitted on a worker thread is dropped rather than collected, where a shared
/// buffer would have collected it. Choose by what the test does with threads — one that joins every
/// thread it starts can share a buffer; one whose process forks underneath it cannot.
///
/// **The state is one `thread_local` for the module rather than a field**, so the sink is zero-sized and
/// `MakeWriter`'s per-event writer costs nothing to create; anything shared here would be the lock this
/// type exists to avoid.
pub struct ThreadLocalBuf;

impl ThreadLocalBuf {
    /// Start recording on this thread, dropping whatever it held.
    ///
    /// **Emptied at the start of a recording and not only at the end**, because a body that panics never
    /// reaches [`take`](Self::take): the buffer it filled stays where it is, and the next recording on
    /// this thread is what clears it, so a failed test cannot bleed into a later one.
    pub fn clear() {
        THREAD_BUFFER.with(|buffer| *buffer.borrow_mut() = Some(Vec::new()));
    }

    /// Stop recording: what this thread logged since [`clear`](Self::clear), as the subscriber rendered
    /// it.
    ///
    /// Empty rather than a panic when this thread recorded nothing (a nested recording takes the buffer
    /// first), because "the capture came out empty" is a failure the caller's own assertion has to state.
    pub fn take() -> String {
        THREAD_BUFFER.with(|buffer| {
            let text = buffer.borrow_mut().take().unwrap_or_default();
            String::from_utf8(text).expect("the capture is UTF-8")
        })
    }

    /// Whether this thread has a recording open — **the question [`Capture`]'s write path asks first**,
    /// and it answers without taking any lock, which is what keeps a recording fork-safe.
    pub fn is_recording() -> bool {
        THREAD_BUFFER.with(|buffer| buffer.borrow().is_some())
    }

    /// What this thread has logged since [`clear`](Self::clear), without taking it: the non-taking twin
    /// of [`take`](Self::take), for a caller that polls a recording still in progress.
    pub fn peek() -> String {
        THREAD_BUFFER.with(|buffer| {
            String::from_utf8_lossy(buffer.borrow().as_deref().unwrap_or_default()).into_owned()
        })
    }
}

impl io::Write for ThreadLocalBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        THREAD_BUFFER.with(|buffer| {
            if let Some(text) = buffer.borrow_mut().as_mut() {
                text.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for ThreadLocalBuf {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        ThreadLocalBuf
    }
}

/// Everything `body` logged, as the subscriber rendered it, beside what `body` returned.
///
/// **A recording installs its own subscriber because the crate under test installs none** (shared §7):
/// the process that owns the runtime decides where events go, so a test that wants to read its own
/// crate's events has to be the one that captures them. What it installs is fixed rather than
/// parameterised — ANSI off (load-bearing: the default subscriber wraps each field *name* and the equals
/// sign after it in escapes, so no captured line would carry `step="…"`) and `DEBUG` (§7's `Info` would
/// leave every `debug!` site with no test that could fail if it were deleted) — because a knob here is a
/// way for two callers to disagree about what a captured line looks like.
///
/// **The buffer is emptied at both ends rather than dropped at the start**: a panic in the body leaves it
/// in place, and the next recording on this thread clears it, so a failed test cannot bleed into a later
/// one. The pair is the body's value and the text, in that order, so a caller that wants only the log
/// writes `let (_, logged) = record_events(…)`.
///
/// **It is the device's single-thread entry point and nothing more** ([`capture`] carries the install
/// and the level): a leg that has to order two threads' events, or to poll a recording while it is
/// still open, takes the device and [`Capture::record_every_thread`] directly.
pub fn record_events<T>(body: impl FnOnce() -> T) -> (T, String) {
    let capture = capture();
    let _recording = capture.record();
    let value = body();
    (value, capture.text())
}

/// The recording device a test binary's legs share, installed once for the process.
///
/// **One subscriber for the whole process, as the global default, installed with `try_init` and never
/// `init`** (shared §7), and that is a measured correction rather than a preference. `tracing` caches
/// each callsite's `Interest` in a **process-global** cell, written both when a callsite is first hit
/// and when any dispatch is created — and while exactly one dispatch is registered, which is the whole
/// of a `with_default` recording, `tracing-core`'s `Rebuilder::JustOne` consults *the calling thread's*
/// default rather than the registry. A thread-scoped subscriber is therefore invisible to every other
/// thread, so a parallel test that happens to be the first to hit the same callsite writes `never` into
/// the shared cell and the event the recording exists to observe is dropped in silence: one run in
/// thirty, and only when the other thread wins that race. A **global** default is visible from every
/// thread, so the cell can only ever say "interested". **A crate whose test modules share callsites with
/// the tests around them is that shape**, and the sandbox crates are: `resolve_model` is reached by the
/// refusal tests as well as by the recording one, and `jail_root` by every `jail.rs` test.
///
/// **A second global installer in the same test binary silently loses this device**, and it is the one
/// failure its `try_init` cannot turn into a loud one: whoever installs a process default first wins, so
/// a device installed after another one's subscriber stays empty and every assertion reading it fails
/// for a recording that never happened. `tinio-sandbox-core` is the binary where that bites — its
/// `launcher/adopt.rs` tests install a structured recorder of their own as the process default — so that
/// crate's `error.rs` recorder test stays thread-scoped (`with_default`) instead of taking this device.
/// **A crate whose test binary installs a default of its own cannot use this one.**
///
/// **The level is `DEBUG` rather than §7's `Info`**, because the per-item half is a core operation too:
/// a capture at `Info` would leave every `debug!` site — Linux's `jail_root` is one, and its own test
/// asserts it — with no test that could fail if it were deleted. What a recording gives up for that is
/// the ability to *select* a level, so a leg that has to hold an event at the level `serve`'s subscriber
/// enables asserts that level on the recorded line ([`assert_recorded_at`]) instead of relying on a
/// filter to drop the event.
///
/// **Which threads a recording owns is its entry point's decision**, and it is the only point the two
/// entry points differ on: [`Capture::record`] owns the calling thread alone, through
/// [`ThreadLocalBuf`], and [`Capture::record_every_thread`] owns every thread, through the one shared
/// buffer the [`Recording`] guard closes. A recording reads only what it owns, so another test's events
/// never reach an assertion that did not ask for them.
///
/// **`try_init` and never `init`** (shared §7): a subscriber the caller already installed wins, and if
/// one ever is, the capture stays empty and the assertions fail loudly rather than passing on a
/// recording that never happened.
pub fn capture() -> Capture {
    static CAPTURE: OnceLock<Capture> = OnceLock::new();
    CAPTURE
        .get_or_init(|| {
            let capture = Capture::default();
            let _ = tracing_subscriber::fmt()
                .with_writer(capture.clone())
                // **Load-bearing for a field assertion, and measured rather than
                // assumed**: the default subscriber wraps each field *name* and the
                // equals sign after it in ANSI escapes, so a captured line carries
                // `step` only as coloured spans and never as `step="…"`.
                .with_ansi(false)
                .with_max_level(Level::DEBUG)
                .try_init();
            // **`set_global_default` stores the dispatch without re-evaluating the
            // callsite cache** — only `Dispatch::new` does that, and it runs before
            // the default is visible — so a callsite another test already hit stays
            // cached `never`. Rebuilding here, with the global default in place, is
            // what turns those cells back to "interested".
            rebuild_interest_cache();
            capture
        })
        .clone()
}

/// The recorder: the buffer an every-thread recording writes into, and whether one is open.
#[derive(Clone, Default)]
pub struct Capture(Arc<Shared>);

/// [`Capture`]'s shared half, which the capture's own lock protects where it needs protecting.
#[derive(Default)]
struct Shared {
    /// The buffer an **every-thread** recording writes into. A single-thread recording never touches it:
    /// its lines go to the calling thread's [`ThreadLocalBuf`].
    text: Mutex<Vec<u8>>,
    /// Whether an every-thread recording is open right now.
    ///
    /// An atomic rather than a flag beside the buffer's lock, because the write path asks this question
    /// before it is willing to take that lock: a recording whose process forks underneath it must not
    /// enter a critical section a sibling thread held at the instant of the fork ([`ThreadLocalBuf`]
    /// carries that measurement).
    every_thread: AtomicBool,
}

impl Capture {
    /// Own this thread's events until the guard drops.
    ///
    /// **The exact filter is the thread's own buffer**, which is what keeps another test's event — the
    /// subscriber is the process's — out of this recording, and what lets two tests record at once.
    ///
    /// **An every-thread recording must not be open when this one opens**: it claims *every* thread's
    /// lines, so this recording would read an empty buffer while its own lines landed in the other one.
    /// The assertion is what says so, and the pair of entry points is small enough for that to stay a
    /// rule rather than a hope — the every-thread callers take [`recorder`] first as well.
    pub fn record(&self) -> Recording {
        assert!(
            !self.0.every_thread.load(Ordering::Acquire),
            "one recording at a time: an every-thread recording is open, so this thread's lines are \
             not this recording's to read"
        );
        ThreadLocalBuf::clear();
        Recording {
            capture: self.clone(),
            every_thread: false,
        }
    }

    /// Own **every** thread's events until the guard drops.
    ///
    /// The mode an ordering assertion needs rather than a convenience: the two events it compares are
    /// written by two threads, so a recording that owned one of them would have nothing to order.
    ///
    /// **Every other test's events land in the buffer too** while one of these is open — the lines a test
    /// that is *not* recording has nowhere else to go, where a single-thread recording keeps its own
    /// ([`Capture::record`]'s doc). That is why its callers take [`recorder`] first and name the package
    /// or the step their assertion is about.
    pub fn record_every_thread(&self) -> Recording {
        assert!(
            self.0
                .every_thread
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok(),
            "one recording at a time, and this capture already has an every-thread one"
        );
        self.0.text.lock().expect("the capture mutex").clear();
        Recording {
            capture: self.clone(),
            every_thread: true,
        }
    }

    /// What the recording has written so far, without taking it: the reader a caller polls with when the
    /// event it waits for is the signal itself rather than one of the assertions (`tests/profile.rs`'s
    /// `wait_for`).
    pub fn peek(&self) -> String {
        if ThreadLocalBuf::is_recording() {
            ThreadLocalBuf::peek()
        } else {
            String::from_utf8_lossy(&self.0.text.lock().expect("the capture mutex")).into_owned()
        }
    }

    /// What the recording wrote, as UTF-8 — and takes it, so the next recording starts from an empty
    /// buffer.
    pub fn text(&self) -> String {
        if ThreadLocalBuf::is_recording() {
            ThreadLocalBuf::take()
        } else {
            let text = mem::take(&mut *self.0.text.lock().expect("the capture mutex"));
            String::from_utf8(text).expect("the capture is UTF-8")
        }
    }
}

impl io::Write for Capture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // This thread's own recording first, and **without taking the shared lock**: a recording whose
        // process forks underneath it must not enter a critical section the child inherits held (see
        // [`ThreadLocalBuf`]). The shared buffer is reached only by threads that are not recording, and
        // the callers that fork under a recording are not those.
        if ThreadLocalBuf::is_recording() {
            io::Write::write(&mut ThreadLocalBuf, buf)?;
        } else if self.0.every_thread.load(Ordering::Acquire) {
            self.0
                .text
                .lock()
                .expect("the capture mutex")
                .extend_from_slice(buf);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// One recording's lifetime: the guard both of [`Capture`]'s recording entry points answer with.
///
/// A guard rather than a matching call at the end, so that a body which panics releases the capture
/// instead of leaving the every-thread flag set — a later read would then answer from a buffer no
/// recording owns, and a later write would land in it. **A single-thread recording needs no release**:
/// its buffer is the thread's own, and the clear-at-the-start rule is what keeps a failed test from
/// bleeding into the next one on that thread.
pub struct Recording {
    capture: Capture,
    every_thread: bool,
}

impl Drop for Recording {
    fn drop(&mut self) {
        if self.every_thread {
            self.capture.0.every_thread.store(false, Ordering::Release);
        }
    }
}

/// **One every-thread recording at a time across a test binary's tests.**
///
/// The shared buffer is process-wide and holds one recording, so two tests that assert on it must not
/// overlap: [`Capture::record_every_thread`]'s "one at a time" is an *assertion*, and an assertion is
/// the wrong shape for a rule two tests have to keep — the assert fires before the buffer is cleared, so
/// an overlap makes the loser panic on the other test's timing without clearing the winner's events,
/// where the failure this device prevents is one test answering from another's verdict. It is taken once
/// per test rather than once per recording, so a nested recording inside one test is still what the
/// assertion catches.
///
/// **What it cannot cover is the other tests' events**, which the lock has no hold on: a test that never
/// records still emits, which is why an assertion names the package or the step it is about rather than
/// counting on a buffer only its own test wrote (`tests/profile.rs`'s ordering test names the profile it
/// reads the two deletes of).
///
/// Poisoning is not a second failure: the test that panicked while recording has already failed this
/// binary, and the next reader takes the buffer rather than reporting a failure that hides the first.
pub fn recorder() -> MutexGuard<'static, ()> {
    static RECORDER: Mutex<()> = Mutex::new(());
    RECORDER
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Trusted-literal tag set (panics on invalid tags — tests and fixtures).
///
/// Ident keys/values stringify (`env => prod` → `env=prod`); a literal
/// value keeps its spelling (`z => 1`, `env => "a-b"`).
///
/// # Examples
///
/// ```
/// use tinio_util::testing::tags;
///
/// let t = tags! { env => prod };
/// assert_eq!(t.iter().collect::<Vec<_>>(), [("env", "prod")]);
/// let t = tags! { env => prod, z => 1 };
/// assert_eq!(t.iter().collect::<Vec<_>>(), [("env", "prod"), ("z", "1")]);
/// ```
#[macro_export]
macro_rules! tags {
    () => {
        $crate::_core::object::Tags::empty()
    };
    ($($key:ident => $value:tt),+ $(,)?) => {
        $crate::_core::object::Tags::from_pairs([
            $((
                ::core::stringify!($key).to_string(),
                $crate::tags!(@val $value),
            )),+
        ])
        .expect("tags!: valid S3 tag set")
    };
    (@val $v:ident) => {
        ::core::stringify!($v).to_string()
    };
    (@val $v:literal) => {
        ::std::string::ToString::to_string(&$v)
    };
}
pub use tags;

#[cfg(test)]
mod tags_tests {
    use crate::_core::object::Tags;

    /// The three arms: the empty set, ident stringification, and a
    /// literal value keeping its spelling (the doc example covers idents
    /// only — the quoted-literal arm is the one that breaks without the
    /// `@val` two-arm split).
    #[test]
    fn builds_a_set_from_idents_literals_and_quoted_values() {
        assert_eq!(tags! {}, Tags::empty());
        assert_eq!(
            tags! { env => prod, z => 1, label => "a-b" }
                .iter()
                .collect::<Vec<_>>(),
            [("env", "prod"), ("label", "a-b"), ("z", "1")]
        );
    }
}
