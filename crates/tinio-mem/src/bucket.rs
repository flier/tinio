//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Bucket lifecycle over the `buckets` table; the empty-check and removal of
//! `delete_bucket` are one atomic write transaction (see [`crate::storage`]).

use std::ops::Bound;

use async_trait::async_trait;
use redb::{ReadableDatabase, ReadableTable};

use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, bucket, from_nanos, now_nanos,
        object, paginate_ordered,
    },
    Error,
    error::{already_exists, no_such_bucket, not_empty},
    storage::{BUCKETS, MemoryStorage, OBJECTS, UPLOADS, band_start},
};

#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            let mut buckets = txn.open_table(BUCKETS)?;
            if buckets.get(name.as_ref().as_str())?.is_some() {
                return Err(already_exists(name));
            }
            buckets.insert(name.as_ref().as_str(), (now_nanos(), ""))?;
        }
        txn.commit()?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &bucket::Name) -> Result<(), Error> {
        let txn = self.db.begin_write()?;
        {
            // The empty-check and the removal are one atomic write
            // transaction (redb serializes writers), so a concurrent
            // put_object can never slip an object in between.
            let objects = txn.open_table(OBJECTS)?;
            let prefix = format!("{}\0", name.as_ref().as_str());
            let mut range = objects.range(prefix.as_str()..)?;
            if let Some(entry) = range.next() {
                let (k, _) = entry?;
                if k.value().starts_with(&prefix) {
                    return Err(not_empty(name));
                }
            }
            // In-progress multipart uploads are bucket-level state — S3
            // answers BucketNotEmpty for them too. The compound UPLOADS key
            // (`bucket\0...`) makes this a bounded range probe; the first
            // key must belong to THIS bucket (`\0` sorts below every
            // bucket-name character, so the probe's first entry can be a
            // later bucket's upload — the OBJECTS probe above guards, the
            // UPLOADS probe must too).
            let uploads = txn.open_table(UPLOADS)?;
            let upload_prefix = format!("{}\0", name.as_ref().as_str());
            let mut uploads_range = uploads.range(upload_prefix.as_str()..)?;
            if let Some(entry) = uploads_range.next() {
                let (k, _) = entry?;
                if k.value().starts_with(&upload_prefix) {
                    return Err(not_empty(name));
                }
            }
            let mut buckets = txn.open_table(BUCKETS)?;
            if buckets.remove(name.as_ref().as_str())?.is_none() {
                return Err(no_such_bucket(name));
            }
        }
        txn.commit()?;
        Ok(())
    }

    async fn head_bucket(&self, name: &bucket::Name) -> Result<Bucket, Error> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        buckets
            .get(name.as_ref().as_str())?
            .map(|g| Bucket {
                name: name.clone(),
                creation_time: from_nanos(g.value().0),
            })
            .ok_or_else(|| no_such_bucket(name))
    }

    async fn list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, Error> {
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        // BUCKETS is keyed by name, so iteration is already name order.
        // The range starts at the later of the key-prefix band and the
        // resume marker (T06 — a deep resume never re-reads the skipped
        // rows; the same seek as the object listing, mem/src/object.rs),
        // and `take_while` stops at the first non-matching name — the
        // prefix band is contiguous, so the scan never runs past it (an
        // `Err` row passes through to the error cell; only a
        // non-matching name ends the band). The engine stops one probe
        // entry past the page; the sync scan runs inline on the async
        // executor by design (mem is the reference backend, rows are
        // owned copies, and the redb read txn is MVCC — no lock is
        // held). A mid-scan table error fails the listing (the
        // error-cell pattern of the object listing, mem/src/object.rs).
        // F05: the lazy scan only touches the rows the engine visits
        // (page + one probe), so a corrupt row BEYOND the page is never
        // reached — the eager `collect` this replaced failed the whole
        // listing on any error row in the band. Documented shift, not a
        // bug: the full-band validation pass is exactly the cost P05
        // removed; revisit if mem gains production use.
        let mut scan_error = None;
        let start = band_start(&params.prefix, params.start_after.as_deref());
        let items = buckets
            .range::<&str>((start, Bound::Unbounded))?
            .take_while(|entry| {
                entry
                    .as_ref()
                    .map(|(name, _)| name.value().starts_with(&params.prefix))
                    .unwrap_or(true)
            })
            .filter_map(|entry| match entry {
                Ok((name, created)) => {
                    let name = name.value();
                    if !name.starts_with(&params.prefix) {
                        return None;
                    }
                    Some(Bucket {
                        name: name.into(),
                        creation_time: from_nanos(created.value().0),
                    })
                }
                Err(err) => {
                    if scan_error.is_none() {
                        scan_error = Some(err.into());
                    }
                    None
                }
            });
        let (page, truncated, next) = paginate_ordered(
            items,
            None, // the scan applied the marker skip
            params.max_buckets,
            // One `String` order per scanned entry — the engine's owned
            // order; immaterial at bucket counts (the S3 account ceiling
            // is ~1,000 buckets).
            |b| b.name.to_string(),
        );
        if let Some(err) = scan_error {
            return Err(err);
        }
        Ok(BucketsListing {
            buckets: page,
            truncated,
            next_start_after: next,
        })
    }

    async fn get_bucket_tags(&self, name: &bucket::Name) -> Result<object::Tags, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`; the row IS the bucket in mem, written
        // at create, so the fs backend's row-less pre-existing bucket has
        // no mem equivalent). The tags come from the row's tags element,
        // empty when the wire is domain-invalid (self-healing, cap 50).
        let txn = self.db.begin_read()?;
        let buckets = txn.open_table(BUCKETS)?;
        let Some(guard) = buckets.get(name.as_ref().as_str())? else {
            return Err(no_such_bucket(name));
        };
        let (_, tags_wire) = guard.value();
        Ok(
            object::Tags::parse_wire_limited(tags_wire, object::BUCKET_TAGS_MAX)
                .unwrap_or_default(),
        )
    }

    async fn put_bucket_tags(&self, name: &bucket::Name, tags: &object::Tags) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`).
        if !self
            .rewrite_bucket_tags_element(name, &tags.to_wire())
            .await?
        {
            return Err(no_such_bucket(name));
        }
        Ok(())
    }

    async fn delete_bucket_tags(&self, name: &bucket::Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete leniency, mirroring the fs backend's
        // row-only clear). A live row keeps its creation time and loses
        // its tags; a row-less bucket has nothing to clear.
        self.rewrite_bucket_tags_element(name, "").await?;
        Ok(())
    }
}

impl MemoryStorage {
    /// The tag-write transaction of the bucket trio — the shared body of
    /// `put_bucket_tags` and `delete_bucket_tags`: one read-modify-write
    /// transaction replaces the row's tags element with `tags_wire`,
    /// keeping the creation time verbatim. Returns whether the row
    /// existed (the caller maps: put → `NoSuchBucket`, delete → no-op).
    async fn rewrite_bucket_tags_element(
        &self,
        name: &bucket::Name,
        tags_wire: &str,
    ) -> Result<bool, Error> {
        let txn = self.db.begin_write()?;
        let existed = {
            let mut buckets = txn.open_table(BUCKETS)?;
            let Some(created) = buckets
                .get(name.as_ref().as_str())?
                .map(|guard| guard.value().0)
            else {
                return Ok(false);
            };
            buckets.insert(name.as_ref().as_str(), (created, tags_wire))?;
            true
        };
        txn.commit()?;
        Ok(existed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{MultipartOps, ObjectOps, bucket, object, storage::Error::*},
        _util::testing::body,
    };

    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for name in ["zeta", "alpha", "mu-1"] {
            storage
                .create_bucket(&bucket::name(name).unwrap())
                .await
                .unwrap();
        }
        let names: Vec<_> = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            })
            .await
            .unwrap()
            .buckets
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, ["alpha", "mu-1", "zeta"]);
    }

    #[tokio::test]
    async fn delete_empty_bucket_succeeds_when_a_later_bucket_has_objects() {
        let storage = MemoryStorage::new().unwrap();
        let alpha = bucket::name("alpha").unwrap();
        let zeta = bucket::name("zeta").unwrap();
        storage.create_bucket(&alpha).await.unwrap();
        storage.create_bucket(&zeta).await.unwrap();
        storage
            .put_object(&zeta, &object::key("a.txt").unwrap(), body(b"x".to_vec()))
            .await
            .unwrap();
        storage.delete_bucket(&alpha).await.unwrap();
        assert!(matches!(
            storage.head_bucket(&alpha).await.unwrap_err(),
            Error::Storage(NoSuchBucket(_))
        ));
        storage.head_bucket(&zeta).await.unwrap();
    }

    #[tokio::test]
    async fn delete_bucket_with_in_progress_uploads_is_not_empty() {
        let storage = MemoryStorage::new().unwrap();
        let bucket = bucket::name("data").unwrap();
        storage.create_bucket(&bucket).await.unwrap();
        let key = object::key("pending.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty())
            .await
            .unwrap();
        let part = storage
            .upload_part(
                &bucket,
                &key,
                &upload.upload_id,
                1.into(),
                body(b"part".to_vec()),
                None,
            )
            .await
            .unwrap();
        assert!(matches!(
            storage.delete_bucket(&bucket).await.unwrap_err(),
            Error::Storage(NotEmpty(_))
        ));
        // The upload stays intact and usable after the failed delete.
        let completed = storage
            .complete_multipart_upload(
                &bucket,
                &key,
                &upload.upload_id,
                &[crate::_core::CompletedPart {
                    part_number: part.part_number,
                    etag: part.etag.clone(),
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(completed.size, 4);
        // A bucket with an upload but no parts yet is also not empty.
        let idle = storage
            .create_multipart_upload(
                &bucket,
                &object::key("idle.bin").unwrap(),
                None,
                object::Tags::empty(),
            )
            .await
            .unwrap();
        assert!(matches!(
            storage.delete_bucket(&bucket).await.unwrap_err(),
            Error::Storage(NotEmpty(_))
        ));
        storage
            .abort_multipart_upload(&bucket, &object::key("idle.bin").unwrap(), &idle.upload_id)
            .await
            .unwrap();
        storage.delete_object(&bucket, &key).await.unwrap();
        storage.delete_bucket(&bucket).await.unwrap();
    }

    #[tokio::test]
    async fn mem_bucket_tags_round_trip_and_replace() {
        let storage = MemoryStorage::new().unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        assert!(
            storage.get_bucket_tags(&b).await.unwrap().is_empty(),
            "an untagged bucket answers the empty set"
        );

        // Put → Get round-trip (replace-all, no merge).
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        storage.put_bucket_tags(&b, &tags).await.unwrap();
        assert_eq!(storage.get_bucket_tags(&b).await.unwrap(), tags);
        let replaced = object::Tags::from_pairs([("team".into(), "edge".into())]).unwrap();
        storage.put_bucket_tags(&b, &replaced).await.unwrap();
        assert_eq!(storage.get_bucket_tags(&b).await.unwrap(), replaced);

        // head_bucket still reports the creation time (the row's other
        // element survives the tag writes).
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(
            head.creation_time <= std::time::SystemTime::now(),
            "the creation time must survive bucket tagging"
        );

        // Delete clears.
        storage.delete_bucket_tags(&b).await.unwrap();
        assert!(storage.get_bucket_tags(&b).await.unwrap().is_empty());

        // Missing bucket: get/put → NoSuchBucket; delete succeeds
        // (idempotent, like the object tagging delete).
        let ghost = bucket::name("ghost").unwrap();
        let err: Error = storage.get_bucket_tags(&ghost).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
        let err: Error = storage.put_bucket_tags(&ghost, &tags).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
        storage.delete_bucket_tags(&ghost).await.unwrap();
    }

    #[tokio::test]
    async fn mem_garbage_bucket_tags_self_heal() {
        // The read-side tolerance ruling: a bucket row whose tags element
        // is domain-invalid serves the empty set (mirroring the fs
        // cap-50 `parse_wire_limited` discipline). Rows are API-written; the
        // garbage below is a direct database write (tampering).
        let storage = MemoryStorage::new().unwrap();
        let b = bucket::name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        {
            let txn = storage.db.begin_write().unwrap();
            txn.open_table(BUCKETS)
                .unwrap()
                .insert(b.as_ref().as_str(), (1u64, "team=%zz&"))
                .unwrap();
            txn.commit().unwrap();
        }
        assert!(storage.get_bucket_tags(&b).await.unwrap().is_empty());
    }
}
