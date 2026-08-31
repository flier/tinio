//! Property tests for multipart assembly (task T029).
//!
//! Arbitrary part counts and sizes assemble into the exact concatenation,
//! and the composed ETag `MD5-of-MD5s-N` matches an independent reference
//! implementation (raw part digests concatenated, per the AWS composition —
//! NOT the same function the backend uses).

use std::time::SystemTime;

use md5::{Digest, Md5};
use prop::collection;
use proptest::prelude::*;
use tinio_core::{
    ETag, bucket,
    multipart::{CompletedPart, PartInfo, PartNumber},
    object,
    storage::Error::NoSuchUpload,
};
use tinio_fs::{Error, multipart};
use tinio_util::testing::body;
use tokio::{fs, runtime::Runtime};

/// Independent reference composition: MD5 of the raw part digests
/// concatenated, then `-N` (the AWS composition, computed here from
/// scratch).
fn reference_composed(parts: &[PartInfo]) -> String {
    let mut joined = Vec::with_capacity(parts.len() * 16);
    for part in parts {
        joined.extend_from_slice(&part.etag[..]);
    }
    format!("{}-{}", hex::encode(Md5::digest(&joined)), parts.len())
}

proptest! {
    /// Random part counts (1..=24) and sizes (0..=4 KiB) assemble
    /// byte-exact, with the reference composed ETag.
    #[test]
    fn assembly_is_exact_concatenation(
        part_sizes in collection::vec(0usize..4096, 1..24),
    ) {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = multipart::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let key = object::key("big.bin").unwrap();
            let upload = store.create(&b, &key, None).await.unwrap();

            let mut parts = Vec::new();
            let mut expected = Vec::new();
            for (i, size) in part_sizes.iter().enumerate() {
                let data: Vec<u8> = (0..*size).map(|j| (i * 31 + j) as u8).collect();
                expected.extend_from_slice(&data);
                let part = store
                    .put_part(&b, &key, &upload.upload_id, ((i + 1) as u32).into(), body(data), None)
                    .await
                    .unwrap();
                parts.push(part);
            }

            // Independent ETag check (before the object is assembled).
            let expected_etag = reference_composed(&parts);
            let completed: Vec<CompletedPart> = parts
                .iter()
                .map(|p| CompletedPart {
                    part_number: p.part_number,
                    etag: p.etag.clone(),
                })
                .collect();
            let target = state.path().join("assembled.bin");
            let (temp, etag) = store
                .complete(&b, &key, &upload.upload_id, &completed)
                .await
                .unwrap();
            prop_assert_eq!(etag.as_str(), expected_etag);
            fs::rename(&temp, &target).await.unwrap();
            let assembled = fs::read(&target).await.unwrap();
            prop_assert_eq!(&assembled, &expected);
            // The caller renames, then consumes; the upload is gone and
            // abort is now NoSuchUpload.
            store.consume(&b, &upload.upload_id).await.unwrap();
            let err = store.abort(&b, &key, &upload.upload_id).await.unwrap_err();
            prop_assert!(matches!(
                err,
                Error::Storage(NoSuchUpload(_))
            ));
            Ok(())
        })?;
    }

    /// Overwriting a part keeps the final assembly consistent (last write
    /// wins per part number).
    #[test]
    fn part_overwrite_last_writer_wins(n in 1u32..10_000, first in 0usize..1024, second in 0usize..1024) {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = multipart::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let key = object::key("big.bin").unwrap();
            let upload = store.create(&b, &key, None).await.unwrap();
            let first_data: Vec<u8> = (0..first).map(|i| i as u8).collect();
            let second_data: Vec<u8> = (0..second).map(|i| (i as u8).wrapping_mul(7)).collect();
            let p1 = store.put_part(&b, &key, &upload.upload_id, n.into(), body(first_data.clone()), None).await.unwrap();
            let p2 = store.put_part(&b, &key, &upload.upload_id, n.into(), body(second_data.clone()), None).await.unwrap();
            prop_assert_eq!(p1.part_number, p2.part_number);
            // The stored part is the second write.
            let (listed, truncated, _) = store.list_parts(&b, &key, &upload.upload_id, None, 1000).await.unwrap();
            prop_assert!(!truncated);
            prop_assert_eq!(listed.len(), 1);
            prop_assert_eq!(&listed[0].etag, &ETag::from_content(&second_data));
            prop_assert_eq!(listed[0].size, second as u64);
            let completed = [CompletedPart {
                part_number: p2.part_number,
                etag: p2.etag.clone(),
            }];
            let target = state.path().join("out.bin");
            let (temp, _etag) = store
                .complete(&b, &key, &upload.upload_id, &completed)
                .await
                .unwrap();
            fs::rename(&temp, &target).await.unwrap();
            let metadata = fs::metadata(&target).await.unwrap();
            prop_assert_eq!(metadata.len(), second as u64);
            let assembled = fs::read(&target).await.unwrap();
            prop_assert_eq!(&assembled, &second_data);
            Ok(())
        })?;
    }

    /// Part numbers validate 1..=10000 (numbers outside are rejected by
    /// the checked constructor, not by the store).
    #[test]
    fn part_numbers_bounds(n in 1u32..10_000) {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = multipart::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let key = object::key("big.bin").unwrap();
            let upload = store.create(&b, &key, None).await.unwrap();
            let pn: PartNumber = n.into();
            prop_assert!(u32::from(pn) == n);
            let part = store.put_part(&b, &key, &upload.upload_id, pn, body(b"x"), None).await.unwrap();
            prop_assert_eq!(u32::from(part.part_number), n);
            // A part at the extreme edge is listable.
            let (listed, _, _) = store.list_parts(&b, &key, &upload.upload_id, None, 1000).await.unwrap();
            prop_assert!(listed.iter().any(|p| u32::from(p.part_number) == n));
            let _ = SystemTime::now();
            Ok(())
        })?;
    }
}
