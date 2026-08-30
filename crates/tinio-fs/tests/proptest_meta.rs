//! Property tests for the ETag metadata store (task T028).
//!
//! Randomized keys/ETags/sizes round-trip through the store; size/mtime
//! mismatches invalidate the entry (recompute needed, FR-022); concurrent
//! writers never produce torn JSON (atomic temp+rename under the
//! in-process lock).

use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use prop::collection;
use proptest::prelude::*;
use tinio_core::{ETag, bucket, object};
use tinio_fs::meta;
use tokio::runtime::Runtime;

proptest! {
    /// Arbitrary (valid) keys and ETag values round-trip exactly.
    #[test]
    fn entries_round_trip(
        segs in collection::vec("[a-zA-Z0-9_ -]{1,12}", 1..4),
        etag_hex in "[0-9a-f]{32}",
        size in 0u64..(1 << 40),
        secs in 0u64..1_000_000_000,
    ) {
        let key = object::key(segs.join("/")).unwrap();
        let etag = ETag::new(&etag_hex).unwrap();
        let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(secs);
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            store.set(&b, &key, &etag, size, mtime, 0).await.unwrap();
            let record = store.get(&b, &key).await.unwrap().unwrap();
            prop_assert_eq!(&record.key, &key);
            prop_assert_eq!(&record.etag, &etag);
            prop_assert_eq!(record.size, size);
            prop_assert!(record.matches(size, mtime));
            prop_assert!(!record.matches(size + 1, mtime));
            prop_assert!(!record.matches(size, mtime + Duration::from_secs(1)));
            Ok(())
        })?;
    }
}

proptest! {
    /// Composed multipart ETags round-trip through the stored wire form.
    #[test]
    fn composed_etags_round_trip(hex in "[0-9a-f]{32}", parts in 1u32..10_000) {
        let etag = ETag::new(&format!("{hex}-{parts}")).unwrap();
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let key = object::key("big.bin").unwrap();
            store.set(&b, &key, &etag, 1, SystemTime::UNIX_EPOCH, 0).await.unwrap();
            let record = store.get(&b, &key).await.unwrap().unwrap();
            prop_assert_eq!(record.etag, etag);
            Ok(())
        })?;
    }
}

proptest! {
    /// Concurrent writers of complete JSON payloads never tear: the final
    /// file is exactly one writer's payload.
    #[test]
    fn concurrent_writes_never_torn(payloads in collection::vec("[a-zA-Z0-9]{1,200}", 4..12)) {
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let key = object::key("shared.txt").unwrap();
            let mut handles = Vec::new();
            for payload in &payloads {
                let store = store.clone();
                let b = b.clone();
                let key = key.clone();
                let payload = payload.clone();
                handles.push(tokio::spawn(async move {
                    let etag = ETag::from_content(payload.as_bytes());
                    store.set(&b, &key, &etag, payload.len() as u64, SystemTime::UNIX_EPOCH, 0).await.unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            let record = store.get(&b, &key).await.unwrap().unwrap();
            // The surviving entry is exactly one writer's (payload length
            // and etag agree).
            let ok = payloads.iter().any(|p| {
                p.len() as u64 == record.size && record.etag == ETag::from_content(p.as_bytes())
            });
            prop_assert!(ok, "final record {:?} matches no writer", record);
            Ok(())
        })?;
    }
}

proptest! {
    /// Concurrent writers on distinct keys never lose an entry: every
    /// committed write is visible afterwards (the redb single writer
    /// serializes them).
    #[test]
    fn concurrent_distinct_key_writes_all_persist(
        keys in collection::vec("[a-z0-9]{1,16}", 1..16),
    ) {
        // Deduplicate: "distinct keys" is the property under test.
        let keys: Vec<String> = {
            let mut seen = HashSet::new();
            keys.into_iter()
                .filter(|k| seen.insert(k.clone()))
                .collect()
        };
        let runtime = Runtime::new().unwrap();
        runtime.block_on(async {
            let state = tempfile::tempdir().unwrap();
            let store = meta::store(state.path()).unwrap();
            let b = bucket::name("data").unwrap();
            let mut handles = Vec::new();
            for (i, key) in keys.iter().enumerate() {
                let store = store.clone();
                let b = b.clone();
                let key = object::key(format!("{key}.txt")).unwrap();
                handles.push(tokio::spawn(async move {
                    let etag = ETag::new(&format!("{i:032x}")).unwrap();
                    store
                        .set(&b, &key, &etag, i as u64, SystemTime::UNIX_EPOCH, 0)
                        .await
                        .unwrap();
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            let records = store.walk(&b).await.unwrap();
            prop_assert_eq!(records.len(), keys.len(), "every write persists");
            Ok(())
        })?;
    }
}
