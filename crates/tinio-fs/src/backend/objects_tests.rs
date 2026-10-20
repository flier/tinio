#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(windows)]
use std::os::windows::fs::symlink_dir;
use std::{io::Error as IoError, thread, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use md5::{Digest, Md5};
#[cfg(test)]
use tinio_util::testing::object_key;
use tokio::{fs, time::timeout};

use super::*;
#[cfg(unix)]
use crate::_core::multipart;
// MultipartOps' direct callers here are the #[cfg(unix)] fast-path
// tests — cfg-gated like the import above, or windows clippy reports
// it unused while the unix build cannot resolve the trait methods.
#[cfg(unix)]
use crate::_core::storage::MultipartOps;
// The parts-table proof reads a table directly; the file-level import is
// the copy fast path's, unix-gated, so the test module carries its own.
use crate::_store_redb::object_part;
// The ranged-stream tests assert the shared stream's chunking.
use crate::_util::fsutil::CHUNK_SIZE;
// The versioning leg is registered in the runner below, not inside
// `assert_conformance`: a backend compiled without its versioning
// machinery refuses every version operation by contract, so the shared
// suite cannot demand it. The gate carries the import with the call.
#[cfg(feature = "versioning")]
use crate::_util::testing::{conformance_versioning, unique_bucket};
#[cfg(unix)]
use crate::FsOptions;
use crate::{
    _core::{
        acl::Acl,
        object,
        storage::{BucketOps, Error as StorageError},
    },
    _util::testing::{assert_conformance, body, complete_single_part, etag, read_body},
    database,
    testutil::{
        all_users_read_grant, bucket, checksum_tee, fs_options, md5_wire, owner_id, storage,
    },
};
#[tokio::test]
async fn conformance_green() {
    let (_root, storage) = storage();
    assert_conformance(&storage).await;
    #[cfg(feature = "versioning")]
    {
        // The versioning leg runs beside the suite on its own bucket: the
        // harness keeps the bucket it created to itself, and the leg
        // leaves its bucket versioned.
        let b = bucket::name(unique_bucket("conform-versioning")).unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        conformance_versioning(&storage, &b).await;
    }
}

#[tokio::test]
async fn put_get_head_delete_round_trip() {
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("dir/a.txt").unwrap();

    let put = storage.put_object(&b, &k, body(b"hello")).await.unwrap();
    assert_eq!(put.etag, etag("5d41402abc4b2a76b9719d911017c592"));

    // The file physically appears in the directory.
    assert_eq!(
        fs::read(root.path().join("data/dir/a.txt")).await.unwrap(),
        b"hello"
    );

    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(head.size, 5);
    assert_eq!(head.etag, put.etag);

    let get = storage.get_object(&b, &k, None).await.unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"hello");
    assert!(get.served_range.is_none());
    assert_eq!(get.info.etag, put.etag);

    storage.delete_object(&b, &k).await.unwrap();
    let err = storage.head_object(&b, &k).await.unwrap_err();
    assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn copy_object_fast_path_preserves_content_and_etag() {
    use crate::_core::acl::Acl;
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let b2 = bucket::name("other").unwrap();
    storage
        .create_bucket(&b2, None, &Acl::default_private(None))
        .await
        .unwrap();
    let src = object::key("src.bin").unwrap();
    let dst = object::key("dir/dst.bin").unwrap();
    storage
        .put_object(&b, &src, body(b"cross-bucket copy"))
        .await
        .unwrap();
    let put = storage
        .copy_object(
            &b,
            &src,
            &b2,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(put.etag, ETag::from_content(b"cross-bucket copy"));
    let get = storage.get_object(&b2, &dst, None).await.unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"cross-bucket copy");
    assert_eq!(get.info.etag, put.etag);
}

#[cfg(unix)]
#[tokio::test]
async fn copy_of_a_multipart_source_yields_the_content_md5() {
    use crate::_core::{CompletedPart, acl::Acl, multipart::MIN_PART_BYTES};
    // The fast path reuses the source ETag only in its SINGLE form:
    // a composed source's copy is a fresh single-part object whose
    // canonical ETag is the content MD5 of the copied bytes — never
    // the stale `MD5-of-MD5s-N` form (the old stream path's re-hash
    // behavior, preserved).

    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("big.bin").unwrap();
    let dst = object::key("copy.bin").unwrap();
    let upload = storage
        .create_multipart_upload(
            &b,
            &k,
            None,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    // The non-final part must meet the backend's 5 MiB minimum at
    // complete (the shared `check_part_minimum`); only the final
    // part may be small.
    let min = MIN_PART_BYTES as usize;
    let mut concat = vec![b'a'; min];
    concat.extend_from_slice(b"part-two-");
    let p1 = storage
        .upload_part(
            &b,
            &k,
            &upload.upload_id,
            multipart::part_number(1).unwrap(),
            body(vec![b'a'; min]),
            None,
        )
        .await
        .unwrap();
    let p2 = storage
        .upload_part(
            &b,
            &k,
            &upload.upload_id,
            multipart::part_number(2).unwrap(),
            body(b"part-two-"),
            None,
        )
        .await
        .unwrap();
    let completed = [
        CompletedPart {
            part_number: p1.part_number,
            etag: p1.etag,
        },
        CompletedPart {
            part_number: p2.part_number,
            etag: p2.etag,
        },
    ];
    let info = storage
        .complete_multipart_upload(&b, &k, &upload.upload_id, &completed, None)
        .await
        .unwrap();
    assert!(matches!(info.etag, ETag::Composed(_, 2)));
    let put = storage
        .copy_object(
            &b,
            &k,
            &b,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(put.etag, ETag::from_content(&concat));
    let head = storage.head_object(&b, &dst).await.unwrap();
    assert_eq!(head.etag, ETag::from_content(&concat));
}

#[tokio::test]
async fn copy_of_a_missing_source_is_no_such_key() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let missing = object::key("ghost.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    let err = storage
        .copy_object(
            &b,
            &missing,
            &b,
            &dst,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
}

#[tokio::test]
async fn missing_bucket_is_no_such_bucket() {
    let (_root, storage) = storage();
    let ghost = bucket::name("ghost").unwrap();
    let err: StorageError = storage
        .put_object(&ghost, &object_key("a"), body(b"x"))
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchBucket(_)));
}

#[tokio::test]
async fn get_ranges() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("digits").unwrap();
    storage
        .put_object(&b, &k, body(b"0123456789"))
        .await
        .unwrap();

    let get = storage
        .get_object(&b, &k, Some(ByteRange::Inclusive(2, 5)))
        .await
        .unwrap();
    assert_eq!(get.served_range, Some((2, 5)));
    assert_eq!(read_body(get.body).await.unwrap(), b"2345");

    let get = storage
        .get_object(&b, &k, Some(ByteRange::From(7)))
        .await
        .unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"789");

    let get = storage
        .get_object(&b, &k, Some(ByteRange::Suffix(3)))
        .await
        .unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"789");

    let err: StorageError = storage
        .get_object(&b, &k, Some(ByteRange::From(99)))
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::InvalidRange { .. }));
}

#[tokio::test]
async fn get_stream_chunk_sequence() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("chunked.bin").unwrap();
    let payload: Vec<u8> = (0..(2 * CHUNK_SIZE + 1234) as u32)
        .map(|i| (i % 251) as u8)
        .collect();
    storage
        .put_object(&b, &k, body(payload.clone()))
        .await
        .unwrap();

    let get = storage.get_object(&b, &k, None).await.unwrap();
    let mut stream = get.body;
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk.unwrap());
    }
    let sizes: Vec<usize> = chunks.iter().map(|chunk| chunk.len()).collect();
    assert_eq!(sizes, vec![CHUNK_SIZE, CHUNK_SIZE, 1234]);
    assert_eq!(chunks.concat(), payload);
}

#[tokio::test]
async fn get_stream_stops_at_the_range_end() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("ranged.bin").unwrap();
    let payload: Vec<u8> = (0..(CHUNK_SIZE + 100) as u32)
        .map(|i| (i % 251) as u8)
        .collect();
    storage
        .put_object(&b, &k, body(payload.clone()))
        .await
        .unwrap();

    let get = storage
        .get_object(
            &b,
            &k,
            Some(ByteRange::Inclusive(
                CHUNK_SIZE as u64 - 10,
                CHUNK_SIZE as u64 + 4,
            )),
        )
        .await
        .unwrap();
    assert_eq!(
        read_body(get.body).await.unwrap(),
        &payload[CHUNK_SIZE - 10..CHUNK_SIZE + 5]
    );
}

#[tokio::test]
async fn streamed_chunks_own_their_memory() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("owned.bin").unwrap();
    let payload = [vec![0xAB; CHUNK_SIZE], vec![0xCD; 100]].concat();
    storage.put_object(&b, &k, body(payload)).await.unwrap();

    let get = storage.get_object(&b, &k, None).await.unwrap();
    let mut stream = get.body;
    let first = stream.next().await.unwrap().unwrap();
    // Pull the second chunk before inspecting the first.
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(first.len(), CHUNK_SIZE);
    assert!(first.iter().all(|&byte| byte == 0xAB));
    assert_eq!(second.len(), 100);
    assert!(second.iter().all(|&byte| byte == 0xCD));
}

#[tokio::test]
async fn delete_of_a_directory_without_slash_is_204() {
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    // 'dir' exists as a directory (a folder marker was PUT).
    storage
        .put_object(&b, &object_key("dir/"), body(b""))
        .await
        .unwrap();
    // DELETE 'dir' (no trailing slash): idempotent 204, the
    // directory itself is untouched.
    storage
        .delete_object(&b, &object::key("dir").unwrap())
        .await
        .unwrap();
    assert!(root.path().join("data/dir").is_dir());
    storage
        .delete_object(&b, &object::key("dir").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn folder_marker_semantics() {
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let marker = object::key("dir/").unwrap();

    // PUT creates the directory, idempotently.
    storage.put_object(&b, &marker, body(b"")).await.unwrap();
    storage.put_object(&b, &marker, body(b"")).await.unwrap();
    assert!(root.path().join("data/dir").is_dir());

    // GET/HEAD → NoSuchKey.
    let err: StorageError = storage
        .get_object(&b, &marker, None)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));

    // DELETE removes the empty directory, idempotently.
    storage.delete_object(&b, &marker).await.unwrap();
    storage.delete_object(&b, &marker).await.unwrap();
    assert!(!root.path().join("data/dir").exists());

    // A non-empty directory is left in place.
    storage.put_object(&b, &marker, body(b"")).await.unwrap();
    storage
        .put_object(&b, &object_key("dir/file.txt"), body(b"x"))
        .await
        .unwrap();
    storage.delete_object(&b, &marker).await.unwrap();
    assert!(root.path().join("data/dir").is_dir());
    assert!(root.path().join("data/dir/file.txt").is_file());
}

#[tokio::test]
async fn reserved_keys_denied() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    for key in [".tinio", ".tinio/x", "a/.tinio", "a/.tinio/b"] {
        let k = object::key(key).unwrap();
        let err: StorageError = storage
            .put_object(&b, &k, body(b"x"))
            .await
            .unwrap_err()
            .into();
        assert!(matches!(err, StorageError::AccessDenied(_)), "{key}");
        let err: StorageError = storage.get_object(&b, &k, None).await.unwrap_err().into();
        assert!(matches!(err, StorageError::NoSuchKey(_)), "{key}");
    }
}

#[tokio::test]
async fn reserved_key_refused_with_follow_enabled() {
    // FR-020 in BOTH follow modes (P5): the follow-enabled resolve
    // used to skip the lexical mapping entirely — a `.tinio` PUT
    // slipped through and wrote `<bucket>/.tinio/x`. The lexical
    // validation now runs before the follow shortcut, refusing
    // AccessDenied like every other op (contract doc: "refuse writes
    // whose key is reserved ... with AccessDenied").
    use crate::FsOptions;
    let (root, _) = storage();
    let b = bucket::name("data").unwrap();
    fs::create_dir(root.path().join("data")).await.unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    let k = object::key(".tinio/x").unwrap();
    let err: StorageError = storage
        .put_object(&b, &k, body(b"x"))
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
    assert!(
        !root.path().join("data/.tinio").exists(),
        "the reserved segment must never be written"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn lexical_validation_precedes_the_symlink_walk() {
    // P5 ordering pin: the pure lexical validation runs before the
    // symlink walk, so a key the mapping refuses (Windows-invalid
    // chars) answers InvalidKey even when a path component is a link
    // — the documented order ("rejected before any filesystem
    // access", path.rs), where the walk-first code answered
    // AccessDenied (the walk syscalled first).
    use crate::FsOptions;
    let (root, _) = storage();
    let b = bucket::name("data").unwrap();
    fs::create_dir(root.path().join("data")).await.unwrap();
    let outside = tempfile::tempdir().unwrap();
    symlink_dir(outside.path(), root.path().join("data/a")).unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: false,
            ..fs_options()
        },
    )
    .unwrap();
    let k = object::key("a/b<c").unwrap();
    let err: StorageError = storage
        .put_object(&b, &k, body(b"x"))
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::InvalidKey(_)), "{err:?}");
}

#[tokio::test]
async fn head_of_folder_marker_is_no_such_key() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    storage
        .put_object(&b, &object_key("dir/"), body(b""))
        .await
        .unwrap();
    let err: StorageError = storage
        .head_object(&b, &object::key("dir/").unwrap())
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
}

#[tokio::test]
async fn commit_after_bucket_deleted_is_no_such_bucket() {
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"hello"), None, None)
        .await
        .unwrap();
    storage.delete_bucket(&b).await.unwrap();
    let err: StorageError = storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchBucket(_)), "{err:?}");
    assert!(!root.path().join("data").exists());
    let tmp = root.path().join(".tinio/tmp");
    let mut entries = fs::read_dir(&tmp).await.unwrap();
    assert!(
        entries.next_entry().await.unwrap().is_none(),
        "no temp files may remain"
    );
}

#[tokio::test]
async fn dropped_staged_body_leaves_no_temp() {
    // A rejected conditional PUT (412) drops the staged body without
    // a commit — the full body must not stay in `tmp/` for the sweep.
    use crate::_core::storage::ObjectOps;
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("big.bin").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"x".repeat(1024)), None, None)
        .await
        .unwrap();
    drop(staged); // the server's precondition-failure path
    // The removal is async (item 7a — the unlink runs on the
    // blocking pool); wait for it.
    let tmp = root.path().join(".tinio/tmp");
    timeout(Duration::from_secs(5), async {
        loop {
            let mut entries = fs::read_dir(&tmp).await.unwrap();
            if entries.next_entry().await.unwrap().is_none() {
                return;
            }
            task::yield_now().await;
        }
    })
    .await
    .expect("the dropped staged body's temp removal");
}

#[tokio::test]
async fn out_of_band_change_served_immediately() {
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    // Hand-dropped file (SC-006).
    fs::write(root.path().join("data/dropped.txt"), b"out-of-band")
        .await
        .unwrap();
    let k = object::key("dropped.txt").unwrap();
    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(head.size, 11);
    assert_eq!(head.etag, etag(&hex::encode(Md5::digest(b"out-of-band"))));
    let get = storage.get_object(&b, &k, None).await.unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"out-of-band");
}

#[tokio::test]
async fn interrupted_upload_leaves_no_object() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("partial").unwrap();
    let stream = stream::iter(vec![
        Ok(Bytes::from_static(b"data")),
        Err(IoError::other("boom")),
    ]);
    let err = storage
        .put_object(&b, &k, Box::pin(stream))
        .await
        .unwrap_err();
    // The body's failure reaches this crate as the engine's own
    // `storage::Error` — the staging that streams it is the engine's now
    // (spec E8), and the engine answers the contract error. The refusal is
    // the same I/O failure on the wire (`storage::Error::Io` either way),
    // one nesting deeper at this crate's boundary than the bare
    // `Error::Io` the pre-engine staging produced.
    let refused: StorageError = err.into();
    assert!(
        matches!(refused, StorageError::Io(_)),
        "an interrupted upload is an I/O failure: {refused:?}"
    );
    let err = storage.head_object(&b, &k).await.unwrap_err();
    assert!(matches!(err.into(), StorageError::NoSuchKey(_)));
}

#[cfg(unix)]
#[tokio::test]
async fn symlinks_rejected_when_disabled() {
    use crate::FsOptions;
    let (root, _) = storage();
    let b = bucket::name("data").unwrap();
    fs::create_dir(root.path().join("data")).await.unwrap();
    fs::write(root.path().join("outside.txt"), b"secret")
        .await
        .unwrap();
    symlink(
        root.path().join("outside.txt"),
        root.path().join("data/link.txt"),
    )
    .unwrap();

    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: false,
            state_dir: None,
            ..fs_options()
        },
    )
    .unwrap();
    let k = object::key("link.txt").unwrap();
    let err: StorageError = storage.head_object(&b, &k).await.unwrap_err().into();
    assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
    let err: StorageError = storage.get_object(&b, &k, None).await.unwrap_err().into();
    assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
    // The PUT path rejects the same way (the stage gate refuses
    // before the body is staged).
    let err: StorageError = storage
        .put_object(&b, &k, body(b"x"))
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
    // DELETE through the link is refused the same way (it would
    // otherwise remove a file outside the storage root).
    let err: StorageError = storage.delete_object(&b, &k).await.unwrap_err().into();
    assert!(matches!(err, StorageError::AccessDenied(_)), "{err:?}");
    assert!(root.path().join("outside.txt").exists());

    // With following enabled, the link is served.
    drop(storage);
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(head.size, 6);
    // ... and DELETE resolves through it (the follow policy).
    storage.delete_object(&b, &k).await.unwrap();
    assert!(!root.path().join("outside.txt").exists());
}

#[tokio::test]
async fn commit_object_writes_to_bucket_target_at_rename() {
    // Same race as multipart complete: a followed bucket symlink
    // retargeted between commit's resolve and the rename must not
    // leave the object on the stale path (stage_body is a validation
    // gate; commit re-resolves under the mutation lock).
    use crate::{
        FsOptions,
        testutil::{link_dir, retarget_bucket_during_commit, wait_for_lock_waiter},
    };

    let root = tempfile::tempdir().unwrap();
    let target_a = tempfile::tempdir().unwrap();
    let target_b = tempfile::tempdir().unwrap();
    let link = root.path().join("data");
    link_dir(target_a.path(), &link);
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            follow_symlinks: true,
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("data").unwrap();
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"hello"), None, None)
        .await
        .unwrap();
    let storage2 = storage.clone();
    let b2 = b.clone();
    let k2 = k.clone();
    retarget_bucket_during_commit(
        &storage,
        &b,
        &link,
        target_b.path(),
        wait_for_lock_waiter(),
        move || async move {
            storage2
                .commit_object(
                    &b2,
                    &k2,
                    staged,
                    object::Tags::empty(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap()
        },
    )
    .await;
    assert!(
        target_b.path().join("a.txt").exists(),
        "object must land under the bucket target at rename"
    );
    assert!(
        !target_a.path().join("a.txt").exists(),
        "stale pre-lock path must not receive the object"
    );
    let get = storage.get_object(&b, &k, None).await.unwrap();
    assert_eq!(read_body(get.body).await.unwrap(), b"hello");
}

// --- tags + recorded checksums + retained parts (spec 2026-08-31) ---

#[tokio::test]
async fn fs_object_tags_round_trip_and_replace() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("t.txt").unwrap();
    storage.put_object(&b, &k, body(b"x")).await.unwrap();
    assert!(
        storage.get_object_tags(&b, &k).await.unwrap().is_empty(),
        "an untagged object answers the empty set"
    );

    // Put → Get round-trip (replace-all, no merge).
    let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    storage.put_object_tags(&b, &k, &tags).await.unwrap();
    assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), tags);
    let replaced = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
    storage.put_object_tags(&b, &k, &replaced).await.unwrap();
    assert_eq!(storage.get_object_tags(&b, &k).await.unwrap(), replaced);
    // head/get carry the same tags (Info.tags — one source of truth).
    assert_eq!(storage.head_object(&b, &k).await.unwrap().tags, replaced);

    // Delete clears; the row's other elements survive the tag write.
    storage.delete_object_tags(&b, &k).await.unwrap();
    assert!(storage.get_object_tags(&b, &k).await.unwrap().is_empty());
    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(head.size, 1, "the etag/size row must survive tag ops");
    assert!(head.tags.is_empty());

    // Missing object: get/put → NoSuchKey, delete succeeds.
    let missing = object::key("missing.txt").unwrap();
    let err: StorageError = storage
        .get_object_tags(&b, &missing)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    let err: StorageError = storage
        .put_object_tags(&b, &missing, &tags)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    storage.delete_object_tags(&b, &missing).await.unwrap();
    // A missing bucket answers NoSuchBucket (get and delete alike).
    let ghost = bucket::name("ghost").unwrap();
    let err: StorageError = storage
        .get_object_tags(&ghost, &k)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchBucket(_)));
    let err: StorageError = storage
        .delete_object_tags(&ghost, &k)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchBucket(_)));
}

#[tokio::test]
async fn fs_commit_and_copy_carry_tags() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let a = object::key("a.txt").unwrap();
    let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    // The commit records the tags with the object — no post-commit
    // tag window.
    let staged = storage
        .stage_body(&b, &a, body(b"hi"), None, None)
        .await
        .unwrap();
    let info = storage
        .commit_object(
            &b,
            &a,
            staged,
            tags.clone(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    assert_eq!(info.etag, ETag::from_content(b"hi"));
    assert_eq!(info.tags, tags);
    assert_eq!(storage.head_object(&b, &a).await.unwrap().tags, tags);

    // A copy is a fresh object whose tags are the caller's — never
    // the source's.
    let dst = object::key("b.txt").unwrap();
    let copy_tags = object::Tags::from_pairs([("env".into(), "dev".into())]).unwrap();
    storage
        .copy_object(
            &b,
            &a,
            &b,
            &dst,
            copy_tags.clone(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert_eq!(storage.get_object_tags(&b, &dst).await.unwrap(), copy_tags);
    assert_eq!(storage.get_object_tags(&b, &a).await.unwrap(), tags);
}

#[tokio::test]
async fn fs_commit_records_the_stage_tee_checksum() {
    // spec 2026-08-31: a plain PUT under the checksum toggle records
    // the tee's validated digest as the object's FULL_OBJECT checksum
    // — the backend never re-hashes.
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("c.txt").unwrap();
    let staged = storage
        .stage_body(
            &b,
            &k,
            body(b"hello"),
            Some(checksum_tee(checksum::Algorithm::Crc32, "NhCmhg==")),
            None,
        )
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let head = storage.head_object(&b, &k).await.unwrap();
    let recorded = head.checksum.expect("the tee digest must be recorded");
    assert_eq!(recorded.part.algorithm, checksum::Algorithm::Crc32);
    assert_eq!(recorded.part.value.0, "NhCmhg==");
    assert_eq!(recorded.kind, checksum::Type::FullObject);
    // A put without a tee records none.
    storage
        .put_object(&b, &object::key("plain.txt").unwrap(), body(b"x"))
        .await
        .unwrap();
    let head = storage
        .head_object(&b, &object::key("plain.txt").unwrap())
        .await
        .unwrap();
    assert!(head.checksum.is_none());
}

#[tokio::test]
async fn fs_content_recompute_preserves_tags_and_checksum() {
    // The row's tags/checksum elements are NOT recomputable from the
    // object file (spec 2026-08-31): a content-derived rewrite — an
    // out-of-band file change forces an ETag recompute on the next
    // read — carries them over. Only an API write clears them.
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    let tags = object::Tags::from_pairs([("env".into(), "prod".into())]).unwrap();
    let staged = storage
        .stage_body(
            &b,
            &k,
            body(b"hello"),
            Some(checksum_tee(checksum::Algorithm::Md5, &md5_wire(b"hello"))),
            None,
        )
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            tags.clone(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();

    // Out-of-band in-place rewrite (same size — only the identity
    // gate and mtime detect it). The sleep lands the rewrite in a
    // later Windows FILETIME tick (~16 ms granularity).
    thread::sleep(Duration::from_millis(30));
    fs::write(root.path().join("data/a.txt"), b"world")
        .await
        .unwrap();
    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(
        head.etag,
        ETag::from_content(b"world"),
        "the etag recomputes"
    );
    assert_eq!(
        head.tags, tags,
        "the API-written tags survive the recompute"
    );
    assert_eq!(
        head.checksum.as_ref().map(|c| c.part.value.0.as_str()),
        Some(md5_wire(b"hello").as_str()),
        "the recorded checksum survives the recompute"
    );
}

#[tokio::test]
async fn fs_object_parts_lifecycle() {
    // The OBJECT_PARTS lifecycle (spec 2026-08-31): an overwrite via
    // commit removes the rows, delete removes them, rename migrates
    // them with the record, and copy never inherits them.
    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("mp.bin").unwrap();
    complete_single_part(&storage, &b, &k).await;
    assert_eq!(storage.list_object_parts(&b, &k).await.unwrap().len(), 1);

    // (a) An overwriting commit is a fresh single-part object: its
    // parts rows are gone.
    let staged = storage
        .stage_body(&b, &k, body(b"plain"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    assert!(
        storage.list_object_parts(&b, &k).await.unwrap().is_empty(),
        "an overwrite must not leave the completed object's parts"
    );

    // (c) rename migrates the parts rows with the record.
    complete_single_part(&storage, &b, &k).await;
    let moved = object::key("moved.bin").unwrap();
    storage.rename_object(&b, &k, &moved).await.unwrap();
    assert!(
        root.path().join("data/moved.bin").exists(),
        "the file moves"
    );
    assert!(
        !root.path().join("data/mp.bin").exists(),
        "the source file is gone"
    );
    assert_eq!(
        storage.list_object_parts(&b, &moved).await.unwrap().len(),
        1
    );
    let err: StorageError = storage.list_object_parts(&b, &k).await.unwrap_err().into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    // A rename over an existing destination replaces it (the dst's
    // own stale rows die).
    let dst = object::key("dst.bin").unwrap();
    complete_single_part(&storage, &b, &dst).await;
    storage.rename_object(&b, &moved, &dst).await.unwrap();
    assert_eq!(storage.list_object_parts(&b, &dst).await.unwrap().len(), 1);

    // (d) copy_object never inherits the source's parts.
    let copy = object::key("copy.bin").unwrap();
    storage
        .copy_object(
            &b,
            &dst,
            &b,
            &copy,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
            None,
        )
        .await
        .unwrap();
    assert!(
        storage
            .list_object_parts(&b, &copy)
            .await
            .unwrap()
            .is_empty(),
        "a copy is single-part: the source's rows must not follow"
    );

    // (b) delete removes the rows — proved straight in the database
    // (the object is gone, so the parts list answers NoSuchKey).
    storage.delete_object(&b, &dst).await.unwrap();
    let err: StorageError = storage
        .list_object_parts(&b, &dst)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    drop(storage); // release the redb file lock
    let db = database::open(&root.path().join(".tinio")).unwrap().db;
    let txn = redb::ReadableDatabase::begin_read(&db).unwrap();
    let parts = object_part::Table::open_readonly(&txn).unwrap();
    assert!(
        parts.list(&b, &dst).unwrap().is_empty(),
        "delete must drain the rows"
    );
    assert!(
        parts.list(&b, &copy).unwrap().is_empty(),
        "copy never wrote rows"
    );
    assert!(
        parts.list(&b, &moved).unwrap().is_empty(),
        "rename migrated the rows away"
    );
}

// --- owner + ACL (spec 2026-09-05) ---

#[tokio::test]
async fn fs_object_acl_replace_all() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    storage.put_object(&b, &k, body(b"x")).await.unwrap();
    assert_eq!(
        storage.get_object_acl(&b, &k).await.unwrap(),
        Acl::default_private(None),
        "an unwritten ACL answers the private default"
    );

    // Put → Get round-trip (replace-all, no merge).
    let grants = vec![all_users_read_grant()];
    storage.put_object_acl(&b, &k, &grants).await.unwrap();
    assert_eq!(storage.get_object_acl(&b, &k).await.unwrap().grants, grants);
    storage.put_object_acl(&b, &k, &Vec::new()).await.unwrap();
    assert_eq!(
        storage.get_object_acl(&b, &k).await.unwrap().grants,
        Vec::new()
    );

    // Missing object: get/put → NoSuchKey.
    let missing = object::key("missing.txt").unwrap();
    let err: StorageError = storage
        .get_object_acl(&b, &missing)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
    let err: StorageError = storage
        .put_object_acl(&b, &missing, &grants)
        .await
        .unwrap_err()
        .into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
}

#[tokio::test]
async fn fs_commit_records_owner_and_acl() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    let owner = owner_id();
    let grants = vec![all_users_read_grant()];
    let write_acl = Acl {
        owner: Some(owner.clone()),
        grants: grants.clone(),
    };
    let staged = storage
        .stage_body(&b, &k, body(b"hello"), None, None)
        .await
        .unwrap();
    let info = storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &write_acl,
        )
        .await
        .unwrap();
    assert_eq!(info.owner.as_ref(), Some(&owner));
    assert_eq!(info.acl.grants, grants);
    // head/get echo the recorded owner and ACL.
    let head = storage.head_object(&b, &k).await.unwrap();
    assert_eq!(head.owner.as_ref(), Some(&owner));
    assert_eq!(head.acl.grants, grants);
    // get_object_acl recombines the owner element with the grants.
    let got = storage.get_object_acl(&b, &k).await.unwrap();
    assert_eq!(got.owner.as_ref(), Some(&owner));
    assert_eq!(got.grants, grants);
    // A put_object_acl replaces the grants, never the owner.
    let replaced = Vec::new();
    storage.put_object_acl(&b, &k, &replaced).await.unwrap();
    let got = storage.get_object_acl(&b, &k).await.unwrap();
    assert_eq!(got.owner.as_ref(), Some(&owner));
    assert_eq!(got.grants, replaced);
}

#[tokio::test]
async fn fs_copy_records_the_callers_owner_and_acl() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let src = object::key("src.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    storage
        .put_object(&b, &src, body(b"copy me"))
        .await
        .unwrap();
    let owner = owner_id();
    let grants = vec![all_users_read_grant()];
    let info = storage
        .copy_object(
            &b,
            &src,
            &b,
            &dst,
            object::Tags::empty(),
            Some(&owner),
            &Acl {
                owner: Some(owner.clone()),
                grants: grants.clone(),
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(info.owner.as_ref(), Some(&owner));
    assert_eq!(info.acl.grants, grants);
    // The copy's ACL is the caller's — never the source's.
    let head = storage.head_object(&b, &dst).await.unwrap();
    assert_eq!(head.owner.as_ref(), Some(&owner));
    assert_eq!(head.acl.grants, grants);
    let head_src = storage.head_object(&b, &src).await.unwrap();
    assert_eq!(head_src.owner, None, "the source is untouched");
    assert_eq!(head_src.acl.grants, Vec::new());
}

#[tokio::test]
async fn fs_rename_moves_owner_and_acl() {
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    let src = object::key("src.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    let owner = owner_id();
    let grants = vec![all_users_read_grant()];
    let staged = storage
        .stage_body(&b, &src, body(b"rename me"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &src,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl {
                owner: Some(owner.clone()),
                grants: grants.clone(),
            },
        )
        .await
        .unwrap();
    // A rename is not a fresh object: the owner and ACL move with
    // the record.
    let renamed = storage.rename_object(&b, &src, &dst).await.unwrap();
    assert_eq!(renamed.owner.as_ref(), Some(&owner));
    assert_eq!(renamed.acl.grants, grants);
    let head = storage.head_object(&b, &dst).await.unwrap();
    assert_eq!(head.owner.as_ref(), Some(&owner));
    assert_eq!(head.acl.grants, grants);
    let got = storage.get_object_acl(&b, &dst).await.unwrap();
    assert_eq!(got.owner.as_ref(), Some(&owner));
    assert_eq!(got.grants, grants);
}

// --- unix mode + ownership hardening (spec 2026-09-05 §5a) ---
//
// The chown-asserting tests run only as root (a foreign-uid chown
// would otherwise fail with EPERM — the fail-closed test runs only
// when NOT root, where that EPERM is exactly the failure it needs).
// Their real run is the Task 16 WSL2 gate; here they typecheck on the
// linux target and the mode tests run everywhere.

#[cfg(unix)]
#[tokio::test]
async fn fs_object_file_mode_0600() {
    use std::{fs::metadata, os::unix::fs::PermissionsExt};

    let (root, storage) = storage();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    storage.put_object(&b, &k, body(b"x")).await.unwrap();
    let mode = metadata(root.path().join("data/a.txt"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "a fresh object file must be 0600");
}

#[cfg(unix)]
#[tokio::test]
async fn fs_object_chowned_to_mapped_owner_uid() {
    use std::{collections::HashMap, fs as std_fs, os::unix::fs::MetadataExt};

    use crate::{_core::acl::Acl, testutil};
    if testutil::euid() != 0 {
        eprintln!("skipped: the chown tests need root");
        return;
    }
    let uid = 2345;
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), uid)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("data").unwrap();
    storage
        .create_bucket(&b, Some(&owner), &Acl::default_private(Some(owner.clone())))
        .await
        .unwrap();
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"x"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap();
    let metadata = std_fs::metadata(root.path().join("data/a.txt")).unwrap();
    assert_eq!(metadata.uid(), uid, "the file must land chowned");
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_unmapped_owner_file_stays_server_user_owned() {
    use std::{
        fs as std_fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };

    use crate::{_core::acl::Acl, testutil};
    let (_root, storage) = storage();
    let b = bucket(&storage).await;
    // An owner with no configured uid: no chown — the file stays
    // server-user-owned, still 0600.
    let owner = testutil::owner_id();
    let k = object::key("unmapped.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"x"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap();
    let path = storage.root().join("data/unmapped.txt");
    let metadata = std_fs::metadata(&path).unwrap();
    assert_eq!(metadata.uid(), testutil::euid());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    // The anonymous path (owner None) is the same.
    let k2 = object::key("anon.txt").unwrap();
    storage.put_object(&b, &k2, body(b"x")).await.unwrap();
    let metadata = std_fs::metadata(storage.root().join("data/anon.txt")).unwrap();
    assert_eq!(metadata.uid(), testutil::euid());
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_rename_preserves_ownership() {
    use std::{collections::HashMap, fs as std_fs, os::unix::fs::MetadataExt};

    use crate::{_core::acl::Acl, testutil};
    if testutil::euid() != 0 {
        eprintln!("skipped: the chown tests need root");
        return;
    }
    let uid = 3456;
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), uid)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"x"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap();
    // The inode moves — the uid is preserved, no chown.
    let moved = object::key("b.txt").unwrap();
    storage.rename_object(&b, &k, &moved).await.unwrap();
    let metadata = std_fs::metadata(root.path().join("data/b.txt")).unwrap();
    assert_eq!(metadata.uid(), uid);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_copy_overwrite_rechown_to_new_owner() {
    use std::{collections::HashMap, fs as std_fs, os::unix::fs::MetadataExt};

    use crate::{_core::acl::Acl, testutil};
    if testutil::euid() != 0 {
        eprintln!("skipped: the chown tests need root");
        return;
    }
    let uid_a = 4567;
    let uid_b = 5678;
    let owner_a = testutil::owner_id();
    let owner_b = testutil::other_owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner_a.clone(), uid_a), (owner_b.clone(), uid_b)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket(&storage).await;
    let src = object::key("src.bin").unwrap();
    let dst = object::key("dst.bin").unwrap();
    let staged = storage
        .stage_body(&b, &src, body(b"x"), None, None)
        .await
        .unwrap();
    storage
        .commit_object(
            &b,
            &src,
            staged,
            object::Tags::empty(),
            Some(&owner_a),
            &Acl::default_private(Some(owner_a.clone())),
        )
        .await
        .unwrap();
    // An overwriting copy records the NEW owner's uid.
    storage
        .copy_object(
            &b,
            &src,
            &b,
            &dst,
            object::Tags::empty(),
            Some(&owner_b),
            &Acl::default_private(Some(owner_b.clone())),
            None,
        )
        .await
        .unwrap();
    let metadata = std_fs::metadata(root.path().join("data/dst.bin")).unwrap();
    assert_eq!(metadata.uid(), uid_b);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_prefix_dir_chowned_to_bucket_owner() {
    use std::{
        collections::HashMap,
        fs::metadata,
        os::unix::fs::{MetadataExt, PermissionsExt},
    };

    use crate::{_core::acl::Acl, testutil};
    if testutil::euid() != 0 {
        eprintln!("skipped: the chown tests need root");
        return;
    }
    let uid = 6789;
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), uid)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("data").unwrap();
    storage
        .create_bucket(&b, Some(&owner), &Acl::default_private(Some(owner.clone())))
        .await
        .unwrap();
    // A non-bucket-owner PutObject (owner None) creating prefix dirs:
    // the dirs take the bucket owner's mapped uid, 0700.
    let k = object::key("dir/sub/f.txt").unwrap();
    storage.put_object(&b, &k, body(b"x")).await.unwrap();
    let dir = metadata(root.path().join("data/dir")).unwrap();
    assert_eq!(dir.uid(), uid, "the prefix dir must be bucket-owner-owned");
    assert_eq!(dir.permissions().mode() & 0o777, 0o700);
    let sub = metadata(root.path().join("data/dir/sub")).unwrap();
    assert_eq!(sub.uid(), uid);
    assert_eq!(sub.permissions().mode() & 0o777, 0o700);
    // The object file itself is server-user-owned (its owner is
    // unmapped) and 0600.
    let file = metadata(root.path().join("data/dir/sub/f.txt")).unwrap();
    assert_eq!(file.uid(), testutil::euid());
    assert_eq!(file.permissions().mode() & 0o777, 0o600);
}

#[cfg(unix)]
#[tokio::test]
async fn fs_prefix_dir_chown_failure_removes_dirs_and_errors() {
    use std::{collections::HashMap, fs};

    use crate::{_core::acl::Acl, testutil};
    // Review round 1: a prefix-dir chown failure is fail-closed —
    // the write removes the components IT created (deepest first)
    // and errors, so a wrongly-owned 0700 prefix dir never strands
    // the mapped owner's own 0600 objects while the request
    // reported success. Runs UNPRIVILEGED (the chown to uid 0 fails
    // with EPERM — the failure the test needs); as root the chown
    // succeeds, so the test skips.
    if testutil::euid() == 0 {
        eprintln!("skipped: the fail-closed tests need an UNPRIVILEGED process");
        return;
    }
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), 0)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket::name("data").unwrap();
    // The bucket ROW records a mapped owner, but the bucket dir is
    // hand-created (a pre-existing dir is never touched — an
    // unprivileged create_bucket with a mapped owner would itself
    // fail closed, see the buckets test).
    storage
        .bucket_store()
        .record_full(
            &b,
            SystemTime::now(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap();
    fs::create_dir(root.path().join("data")).unwrap();
    // A non-bucket-owner PutObject creating a two-level prefix: the
    // first created dir's chown fails — the created chain is
    // removed and the PUT errors.
    let k = object::key("dir/sub/f.txt").unwrap();
    let err = storage.put_object(&b, &k, body(b"x")).await.unwrap_err();
    // The engine's bodies answer `storage::Error`, so the file layer's EPERM
    // reaches this crate's boundary nested under `Storage` — the same `Io`
    // (a 500) on the wire, one nesting deeper than the pre-engine staging.
    let refused: StorageError = err.into();
    assert!(matches!(refused, StorageError::Io(_)), "{refused:?}");
    assert!(
        !root.path().join("data/dir").exists(),
        "the failed-hardening prefix chain must be removed"
    );
    assert!(
        root.path().join("data").is_dir(),
        "the pre-existing bucket dir is never touched"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn fs_chown_failure_after_rename_removes_file_and_errors() {
    use std::collections::HashMap;

    use crate::{_core::acl::Acl, testutil};
    if testutil::euid() == 0 {
        eprintln!("skipped: the fail-closed test needs an UNPRIVILEGED process");
        return;
    }
    // The map points at uid 0 — an unprivileged process can never
    // chown to root: the chown fails (EPERM) after the rename
    // landed, and the fail-closed path must remove the file and
    // error the request.
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), 0)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket(&storage).await;
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"x"), None, None)
        .await
        .unwrap();
    let err = storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err:?}");
    assert!(
        !root.path().join("data/a.txt").exists(),
        "a wrongly-owned 0600 file must not be left behind"
    );
    let err: StorageError = storage.head_object(&b, &k).await.unwrap_err().into();
    assert!(matches!(err, StorageError::NoSuchKey(_)));
}

#[cfg(all(unix, feature = "versioning"))]
#[tokio::test]
async fn fs_chown_failure_on_a_versioned_overwrite_keeps_the_current_version() {
    use std::collections::HashMap;

    use crate::{
        _core::{acl::Acl, storage::VersionOps, version::State},
        testutil,
    };
    if testutil::euid() == 0 {
        eprintln!("skipped: the fail-closed test needs an UNPRIVILEGED process");
        return;
    }
    // uid 0 is unreachable for this process, so the second write's chown
    // fails. The first version must stay current: the refusal lands before
    // any version row moves.
    let owner = testutil::owner_id();
    let root = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(
        root.path(),
        FsOptions {
            owner_uids: HashMap::from([(owner.clone(), 0)]),
            ..fs_options()
        },
    )
    .unwrap();
    let b = bucket(&storage).await;
    storage
        .set_bucket_versioning(&b, State::Enabled)
        .await
        .unwrap();
    let k = object::key("a.txt").unwrap();
    let staged = storage
        .stage_body(&b, &k, body(b"first"), None, None)
        .await
        .unwrap();
    let first = storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            None,
            &Acl::default_private(None),
        )
        .await
        .unwrap();
    let first_id = first
        .version_id
        .expect("an enabled write mints a version id");

    let staged = storage
        .stage_body(&b, &k, body(b"second"), None, None)
        .await
        .unwrap();
    let err = storage
        .commit_object(
            &b,
            &k,
            staged,
            object::Tags::empty(),
            Some(&owner),
            &Acl::default_private(Some(owner.clone())),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Io(_)), "{err:?}");

    let got = storage.get_object(&b, &k, None).await.unwrap();
    assert_eq!(read_body(got.body).await.unwrap(), b"first");
    assert_eq!(got.info.version_id.as_ref(), Some(&first_id));
    assert!(
        storage
            .get_object_version(&b, &k, Some(&first_id), None)
            .await
            .is_ok(),
        "the current version stays addressable"
    );
}
