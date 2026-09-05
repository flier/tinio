//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Bucket lifecycle over the `buckets` table; the empty-check and removal of
//! `delete_bucket` are one atomic write transaction (see [`crate::storage`]).

use std::time::SystemTime;

use async_trait::async_trait;

#[cfg(test)]
use crate::_core::bucket::name;
use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, acl, bucket::Name, cors, object,
        paginate_ordered,
    },
    _store::{bucket, decode_acl_wire, decode_owner_wire, objects, upload},
    Error,
    error::{already_exists, no_such_bucket, not_empty},
    storage::MemoryStorage,
};

#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(
        &self,
        name: &Name,
        owner: Option<&acl::OwnerId>,
        acl: &acl::Acl,
    ) -> Result<(), Error> {
        self.db.write(|txn| {
            let mut buckets = bucket::Table::open(txn)?;
            if buckets.get(name.as_ref().as_str())?.is_some() {
                return Err(already_exists(name));
            }
            let owner_wire = owner.map_or_else(String::new, |o| o.as_str().to_string());
            let acl_wire = acl.to_grants_wire();
            buckets.put_full(
                name.as_ref().as_str(),
                &bucket::BucketRow {
                    owner: owner_wire,
                    acl: acl_wire,
                    ..bucket::BucketRow::at(SystemTime::now())
                },
            )?;
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

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
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
                // The walk-time owner filter (P2#4): before pagination, so
                // the continuation-token math applies to the filtered set.
                let owned = owner.map_or(true, |o| {
                    buckets
                        .row(name)
                        .ok()
                        .flatten()
                        .map(|row| decode_owner_wire(&row.owner).as_ref() == Some(o))
                        .unwrap_or(false)
                });
                if name.starts_with(&params.prefix) && owned {
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
        // no mem equivalent). The tags come from the table's tags
        // accessor, empty when the wire is domain-invalid (self-healing,
        // cap 50).
        self.db.read(|txn| {
            bucket::Table::open_readonly(txn)?
                .tags(name.as_ref().as_str())?
                .ok_or_else(|| no_such_bucket(name))
        })
    }

    async fn put_bucket_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`).
        self.db.write(|txn| {
            if bucket::Table::open(txn)?
                .put_tags(name.as_ref().as_str(), tags)?
                .is_none()
            {
                return Err(no_such_bucket(name));
            }
            Ok(())
        })
    }

    async fn delete_bucket_tags(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete leniency, mirroring the fs backend's
        // row-only clear). A live row keeps its creation time and loses
        // its tags; a row-less bucket has nothing to clear.
        self.db.write(|txn| {
            bucket::Table::open(txn)?.clear_tags(name.as_ref().as_str())?;
            Ok(())
        })
    }

    async fn get_bucket_cors(&self, name: &Name) -> Result<Option<cors::Config>, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`). The configuration comes from the
        // table's CORS accessor, None when it is the `''` wire (a bucket
        // that was cleared or never configured) or domain-invalid
        // (self-healing).
        self.db.read(|txn| {
            bucket::Table::open_readonly(txn)?
                .cors(name.as_ref().as_str())?
                .ok_or_else(|| no_such_bucket(name))
        })
    }

    async fn put_bucket_cors(&self, name: &Name, config: &cors::Config) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`). A zero-rule config normalizes to the
        // `''` wire by the codec itself (op-review G2 — "no
        // configuration").
        self.db.write(|txn| {
            if bucket::Table::open(txn)?
                .put_cors(name.as_ref().as_str(), config)?
                .is_none()
            {
                return Err(no_such_bucket(name));
            }
            Ok(())
        })
    }

    async fn delete_bucket_cors(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: `NoSuchBucket` when the bucket is missing — the
        // delete-tagging idempotent leniency does NOT extend here (the
        // CORS spec mandates `NoSuchBucket` for a missing bucket); a
        // bucket that simply has no configuration clears to the same
        // state (idempotent).
        self.db.write(|txn| {
            if bucket::Table::open(txn)?
                .clear_cors(name.as_ref().as_str())?
                .is_none()
            {
                return Err(no_such_bucket(name));
            }
            Ok(())
        })
    }

    async fn get_bucket_acl(&self, name: &Name) -> Result<acl::Acl, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`; the row IS the bucket in mem, written
        // at create). The ACL recombines the stored owner element with
        // the stored grant set (the row form keeps them apart); a
        // domain-invalid owner/ACL wire self-heals to `None`/the private
        // default (the shared decode-home rule).
        self.db.read(|txn| {
            let buckets = bucket::Table::open_readonly(txn)?;
            buckets
                .row(name.as_ref().as_str())?
                .map(|row| acl::Acl {
                    owner: decode_owner_wire(&row.owner),
                    grants: decode_acl_wire(&row.acl).grants,
                })
                .ok_or_else(|| no_such_bucket(name))
        })
    }

    async fn put_bucket_acl(&self, name: &Name, grants: &acl::AclGrants) -> Result<(), Error> {
        // Replace-all: the grant set replaces the row's ACL element; the
        // creation time, tags and owner element are preserved — a put
        // never changes the owner (contract ruling). `NoSuchBucket` when
        // the row is missing.
        let grants_wire = acl::Acl {
            owner: None,
            grants: grants.clone(),
        }
        .to_grants_wire();
        self.db.write(|txn| -> Result<bool, Error> {
            let mut buckets = bucket::Table::open(txn)?;
            let Some(mut row) = buckets.row(name.as_ref().as_str())? else {
                return Ok(false);
            };
            row.acl = grants_wire;
            buckets.put_full(name.as_ref().as_str(), &row)?;
            Ok(true)
        })
        .map(|found| {
            if found {
                Ok(())
            } else {
                Err(no_such_bucket(name))
            }
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        _core::{MultipartOps, ObjectOps, object, storage::Error::*},
        _util::testing::body,
    };

    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for n in ["zeta", "alpha", "mu-1"] {
            storage.create_bucket(&name(n).unwrap(), None, &acl::Acl::default_private(None)).await.unwrap();
        }
        let names: Vec<_> = storage
            .list_buckets(ListBucketsParams {
                prefix: String::new(),
                start_after: None,
                max_buckets: 1000,
            }, None)
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
        storage.create_bucket(&alpha, None, &acl::Acl::default_private(None)).await.unwrap();
        storage.create_bucket(&zeta, None, &acl::Acl::default_private(None)).await.unwrap();
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
        storage.create_bucket(&bucket, None, &acl::Acl::default_private(None)).await.unwrap();
        let key = object::key("pending.bin").unwrap();
        let upload = storage
            .create_multipart_upload(&bucket, &key, None, object::Tags::empty(), None, &acl::Acl::default_private(None))
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
                None,
                &acl::Acl::default_private(None),
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
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
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
    async fn mem_bucket_cors_round_trip_and_replace() {
        let storage = MemoryStorage::new().unwrap();
        let b = name("data").unwrap();
        storage.create_bucket(&b, None, &acl::Acl::default_private(None)).await.unwrap();
        assert!(
            storage.get_bucket_cors(&b).await.unwrap().is_none(),
            "an unconfigured bucket answers None"
        );

        let config = cors::Config {
            rules: vec![
                cors::Rule {
                    id: Some("one".into()),
                    allowed_methods: vec!["GET".into()],
                    allowed_origins: vec!["*".into()],
                    allowed_headers: Some(vec!["x-amz-*".into()]),
                    expose_headers: Some(vec!["ETag".into()]),
                    max_age_seconds: Some(60),
                },
                cors::Rule {
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
            .put_bucket_cors(&b, &cors::Config::default())
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
}
