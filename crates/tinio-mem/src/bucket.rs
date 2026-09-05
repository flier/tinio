//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Bucket lifecycle over the `buckets` table; the empty-check and removal of
//! `delete_bucket` are one atomic write transaction (see [`crate::storage`]).

use std::time::SystemTime;

use async_trait::async_trait;

#[cfg(test)]
#[cfg(test)]
use crate::_core::bucket::name;
use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, bucket::Name, cors, object,
        paginate_ordered,
    },
    _store::{bucket, objects, upload},
    Error,
    error::{already_exists, no_such_bucket, not_empty},
    storage::MemoryStorage,
};

#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(&self, name: &Name) -> Result<(), Error> {
        self.db.write(|txn| {
            let mut buckets = bucket::Table::open(txn)?;
            if buckets.get(name.as_ref().as_str())?.is_some() {
                return Err(already_exists(name));
            }
            buckets.put(name.as_ref().as_str(), SystemTime::now())?;
            Ok(())
        })
    }

    async fn delete_bucket(&self, name: &Name) -> Result<(), Error> {
        self.db.write(|txn| {
            {
                // The empty-check and the removal are one atomic write
                // transaction (redb serializes writers), so a concurrent
                // put_object can never slip an object in between.
                let objects = objects::Table::open(txn)?;
                if objects.has_bucket(name.as_ref().as_str())? {
                    return Err(not_empty(name));
                }
            }
            {
                // In-progress multipart uploads are bucket-level state —
                // S3 answers BucketNotEmpty for them too. The shared
                // `upload::Table::has_bucket` first-match probe on the
                // `(bucket, "")` lower bound.
                let uploads = upload::Table::open(txn)?;
                if uploads.has_bucket(name.as_ref().as_str())? {
                    return Err(not_empty(name));
                }
            }
            {
                let mut buckets = bucket::Table::open(txn)?;
                if buckets.get(name.as_ref().as_str())?.is_none() {
                    return Err(no_such_bucket(name));
                }
                buckets.remove(name.as_ref().as_str())?;
            }
            Ok(())
        })
    }

    async fn head_bucket(&self, name: &Name) -> Result<Bucket, Error> {
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            buckets
                .get(name.as_ref().as_str())?
                .map(|creation_time| Bucket {
                    name: name.clone(),
                    creation_time,
                })
                .ok_or_else(|| no_such_bucket(name))
        })
    }

    async fn list_buckets(&self, params: ListBucketsParams) -> Result<BucketsListing, Error> {
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            // BUCKETS is keyed by name, so the shared `for_each` walk is
            // already name order; the prefix filter and the exclusive-after
            // marker run in the shared pager. The walk materializes the
            // name list (the F05 note of the lazy scan this replaces: it
            // only touched the rows the engine visits — immaterial at
            // bucket counts, the S3 account ceiling is ~1,000 buckets).
            let mut items: Vec<Bucket> = Vec::new();
            buckets.for_each(|name, creation_time| {
                if name.starts_with(&params.prefix) {
                    items.push(Bucket {
                        name: name.into(),
                        creation_time,
                    });
                }
                Ok(())
            })?;
            let (page, truncated, next) = paginate_ordered(
                items,
                params.start_after.as_ref(),
                params.max_buckets,
                // One `String` order per scanned entry — the engine's owned
                // order; immaterial at bucket counts (the S3 account ceiling
                // is ~1,000 buckets).
                |b| b.name.to_string(),
            );
            Ok(BucketsListing {
                buckets: page,
                truncated,
                next_start_after: next,
            })
        })
    }

    async fn get_bucket_tags(&self, name: &Name) -> Result<object::Tags, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`; the row IS the bucket in mem, written
        // at create, so the fs backend's row-less pre-existing bucket has
        // no mem equivalent). The tags come from the row's tags element,
        // empty when the wire is domain-invalid (self-healing, cap 50).
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            let Some(row) = buckets.row(name.as_ref().as_str())? else {
                return Err(no_such_bucket(name));
            };
            Ok(object::Tags::from_wire_limited(
                &row.tags,
                object::BUCKET_TAGS_MAX,
            ))
        })
    }

    async fn put_bucket_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`).
        if !self.rewrite_bucket_element(name, Some(&tags.to_wire()), None).await? {
            return Err(no_such_bucket(name));
        }
        Ok(())
    }

    async fn delete_bucket_tags(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete leniency, mirroring the fs backend's
        // row-only clear). A live row keeps its creation time and loses
        // its tags; a row-less bucket has nothing to clear.
        self.rewrite_bucket_element(name, Some(""), None).await?;
        Ok(())
    }

    async fn get_bucket_cors(&self, name: &Name) -> Result<Option<cors::CorsConfig>, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`). The configuration comes from the
        // row's CORS element, None when it is the `''` wire (a bucket
        // that was cleared or never configured) or domain-invalid
        // (self-healing, the store's `decode_cors_wire`).
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            let Some(row) = buckets.row(name.as_ref().as_str())? else {
                return Err(no_such_bucket(name));
            };
            Ok(bucket::decode_cors_wire(&row.cors))
        })
    }

    async fn put_bucket_cors(&self, name: &Name, config: &cors::CorsConfig) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`). A zero-rule config normalizes to the
        // `''` wire by the codec itself (op-review G2 — "no
        // configuration").
        if !self.rewrite_bucket_element(name, None, Some(&config.to_wire())).await? {
            return Err(no_such_bucket(name));
        }
        Ok(())
    }

    async fn delete_bucket_cors(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: `NoSuchBucket` when the bucket is missing — the
        // delete-tagging idempotent leniency does NOT extend here (the
        // CORS spec mandates `NoSuchBucket` for a missing bucket); a
        // bucket that simply has no configuration clears to the same
        // state (idempotent).
        if !self.rewrite_bucket_element(name, None, Some("")).await? {
            return Err(no_such_bucket(name));
        }
        Ok(())
    }
}

impl MemoryStorage {
    /// The wire-element write transaction of the bucket trio — the shared
    /// body of the tags and CORS put/delete operations: one
    /// read-modify-write transaction replaces the tagged elements with
    /// the given wires (`None` = keep the row's value verbatim) and sets
    /// every other element verbatim. Returns whether the row existed (the
    /// caller maps to the per-operation missing-bucket semantics).
    async fn rewrite_bucket_element(
        &self,
        name: &Name,
        tags_wire: Option<&str>,
        cors_wire: Option<&str>,
    ) -> Result<bool, Error> {
        self.db.write(|txn| {
            let mut buckets = bucket::Table::open(txn)?;
            let Some(row) = buckets.row(name.as_ref().as_str())? else {
                return Ok(false);
            };
            let tags = tags_wire.map(str::to_string).unwrap_or(row.tags);
            let cors = cors_wire.map(str::to_string).unwrap_or(row.cors);
            buckets.put_full(
                name.as_ref().as_str(),
                &bucket::BucketRow { tags, cors, ..row },
            )?;
            Ok(true)
        })
    }
}

#[cfg(test)]
mod tests {
    // Raw-transaction test calls (`db().begin_read`) take the trait; the
    // `super::*` glob shadows the import name.
    #[allow(unused_imports)]
    use redb::ReadableDatabase;

    use super::*;
    use crate::{
        _core::{MultipartOps, ObjectOps, object, storage::Error::*},
        _util::testing::body,
    };

    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for n in ["zeta", "alpha", "mu-1"] {
            storage.create_bucket(&name(n).unwrap()).await.unwrap();
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
        let alpha = name("alpha").unwrap();
        let zeta = name("zeta").unwrap();
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
        let bucket = name("data").unwrap();
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
        let b = name("data").unwrap();
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
        let ghost = name("ghost").unwrap();
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
        let b = name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        {
            let mut txn = storage.db.db().begin_write().unwrap();
            bucket::Table::open(&mut txn)
                .unwrap()
                .insert(b.as_ref().as_str(), (1u64, "team=%zz&", "", "", ""))
                .unwrap();
            txn.commit().unwrap();
        }
        assert!(storage.get_bucket_tags(&b).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn mem_bucket_cors_round_trip_and_replace() {
        let storage = MemoryStorage::new().unwrap();
        let b = name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        assert!(
            storage.get_bucket_cors(&b).await.unwrap().is_none(),
            "an unconfigured bucket answers None"
        );

        let config = cors::CorsConfig {
            rules: vec![
                cors::CorsRule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: Some(vec!["x-amz-*".into()]),
                    expose_headers: Some(vec!["ETag".into()]),
                    max_age_seconds: Some(60),
                },
                cors::CorsRule {
                    id: None,
                    allowed_methods: vec!["PUT".into(), "DELETE".into()],
                    allowed_origins: vec!["https://example.com".into()],
                    allowed_headers: None,
                    expose_headers: None,
                    max_age_seconds: None,
                },
            ],
        };
        // The 5-tuple discipline, written first: the CORS writes must not
        // clobber a stored tag set.
        let tags = object::Tags::from_pairs([("team".into(), "core".into())]).unwrap();
        storage.put_bucket_tags(&b, &tags).await.unwrap();

        // Put → Get round-trip (replace-all, no merge; order + fields kept).
        storage.put_bucket_cors(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_cors(&b).await.unwrap(),
            Some(config.clone())
        );
        assert_eq!(
            storage.get_bucket_tags(&b).await.unwrap(),
            tags,
            "the CORS writes must preserve the tags element"
        );

        // op-review G2: a zero-rule config through the whole backend must
        // be indistinguishable from "no configuration".
        storage
            .put_bucket_cors(&b, &cors::CorsConfig::default())
            .await
            .unwrap();
        assert_eq!(storage.get_bucket_cors(&b).await.unwrap(), None);
        storage.put_bucket_cors(&b, &config).await.unwrap();
        assert_eq!(
            storage.get_bucket_cors(&b).await.unwrap(),
            Some(config.clone())
        );

        // head_bucket still reports the creation time (the row's other
        // elements survive the CORS writes).
        let head = storage.head_bucket(&b).await.unwrap();
        assert!(
            head.creation_time <= std::time::SystemTime::now(),
            "the creation time must survive bucket CORS writes"
        );

        // Delete clears; the delete is idempotent on the stored config.
        storage.delete_bucket_cors(&b).await.unwrap();
        assert_eq!(storage.get_bucket_cors(&b).await.unwrap(), None);
        storage.delete_bucket_cors(&b).await.unwrap();

        // Missing bucket: get/put/delete → NoSuchBucket (the CORS spec —
        // unlike the delete-bucket-tags idempotent Ok).
        let ghost = name("ghost").unwrap();
        let err: Error = storage.get_bucket_cors(&ghost).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
        let err: Error = storage.put_bucket_cors(&ghost, &config).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
        let err: Error = storage.delete_bucket_cors(&ghost).await.unwrap_err();
        assert!(matches!(err, Error::Storage(NoSuchBucket(_))));
    }

    #[tokio::test]
    async fn mem_garbage_bucket_cors_self_heal() {
        // The read-side tolerance ruling: a bucket row whose CORS element
        // is domain-invalid serves "no configuration" (mirroring the fs
        // store's self-healing decode). Rows are API-written; the garbage
        // below is a direct database write (tampering).
        let storage = MemoryStorage::new().unwrap();
        let b = name("data").unwrap();
        storage.create_bucket(&b).await.unwrap();
        {
            let mut txn = storage.db.db().begin_write().unwrap();
            bucket::Table::open(&mut txn)
                .unwrap()
                .insert(b.as_ref().as_str(), (1u64, "", "", "", "a,b,c"))
                .unwrap();
            txn.commit().unwrap();
        }
        assert!(storage.get_bucket_cors(&b).await.unwrap().is_none());
    }
}
