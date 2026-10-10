//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Every body is the shared ops engine's ([`crate::_ops`]): the row plane is
//! the crate's metadata handle and the content plane is the accounting
//! decorator, so the whole bucket group is the engine's with no mem-specific
//! arm. What stays here is the error projection.
//!
//! The one step only this backend can take — the bucket teardown, which must
//! stay **one** write transaction with its emptiness probes (ruling
//! 2026-10-08: two transactions would let a bucket re-created between them
//! lose the new bucket's rows and bytes) — is the `Hooks` implementation
//! beside the receiver ([`crate::storage::MemHooks`]). mem takes no version
//! plane (spec E16): its version group and its version-creating writes stay
//! mem-local.

use async_trait::async_trait;

#[cfg(test)]
use crate::_core::bucket::name;
use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, acl,
        acl::{Acl, AclGrants},
        bucket::Name,
        cors, object, storage,
    },
    _ops::Hooks,
    _store_redb::{object_meta, scan::has_prefix_pair, upload},
    Error,
    storage::MemoryStorage,
};

#[async_trait]
impl BucketOps for MemoryStorage {
    async fn create_bucket(
        &self,
        name: &Name,
        owner: Option<&acl::OwnerId>,
        acl: &Acl,
    ) -> Result<(), Error> {
        // The engine body: the content plane's create (a no-op for a store
        // whose bucket IS its row) then the create-or-refuse row write (D22).
        self.ops()
            .create_bucket(name, owner, acl)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket(&self, name: &Name) -> Result<(), Error> {
        // The engine's probes (empty content, no in-progress upload) then the
        // hook, which owns the whole teardown: mem's ONE write transaction
        // (ruling 2026-10-08) with the probes re-run inside it, so a concurrent
        // write cannot slip between the check and the removal. The hook's
        // refusals are the answer — `NoSuchBucket` here, exactly as the
        // contract's `remove_if_empty` would project it.
        let ops = self.ops();
        match ops.delete_bucket(name).await {
            // The engine's emptiness probe is the content plane's (byte rows,
            // the contract's own rule), while mem's emptiness authority is its
            // ROW planes — for mem the row is the object. The two differ in one
            // state: a bucket whose rows are gone while byte rows linger (the
            // part bytes a failed `remove_many` orphaned). The teardown sweeps
            // that residue, so it must stay deletable instead of refusing
            // forever and stranding the bytes it meters.
            Err(storage::Error::NotEmpty(_)) if self.rows_are_empty(name).await? => ops
                .hooks
                .before_bucket_delete(name)
                .await
                .map_err(Error::from),
            other => other.map_err(Error::from),
        }
    }

    async fn head_bucket(&self, name: &Name) -> Result<Bucket, Error> {
        // The engine's: the recorded creation time, and `NoSuchBucket` for a
        // row-less bucket — the row IS the bucket in mem (`BucketRow::
        // Guaranteed`, the receiver's declared policy).
        self.ops().head_bucket(name).await.map_err(Error::from)
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // The engine's: BUCKETS is keyed by name, so the row walk is already
        // name order, and the prefix filter, the owner filter (with the
        // effective-owner comparison) and the continuation marker are the
        // shared bodies' — the same ones fs takes where its sweep feeds them.
        self.ops()
            .list_buckets(params, owner, lazy_default)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_tags(&self, name: &Name) -> Result<object::Tags, Error> {
        self.ops().get_bucket_tags(name).await.map_err(Error::from)
    }

    async fn put_bucket_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        self.ops()
            .put_bucket_tags(name, tags)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_tags(&self, name: &Name) -> Result<(), Error> {
        self.ops()
            .delete_bucket_tags(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_cors(&self, name: &Name) -> Result<Option<cors::Config>, Error> {
        self.ops().get_bucket_cors(name).await.map_err(Error::from)
    }

    async fn put_bucket_cors(&self, name: &Name, config: &cors::Config) -> Result<(), Error> {
        self.ops()
            .put_bucket_cors(name, config)
            .await
            .map_err(Error::from)
    }

    async fn delete_bucket_cors(&self, name: &Name) -> Result<(), Error> {
        self.ops()
            .delete_bucket_cors(name)
            .await
            .map_err(Error::from)
    }

    async fn get_bucket_acl(&self, name: &Name) -> Result<Acl, Error> {
        self.ops().get_bucket_acl(name).await.map_err(Error::from)
    }

    async fn put_bucket_acl(&self, name: &Name, grants: &AclGrants) -> Result<(), Error> {
        self.ops()
            .put_bucket_acl(name, grants)
            .await
            .map_err(Error::from)
    }
}

impl MemoryStorage {
    /// Whether mem's row planes hold neither an object row nor an upload row
    /// for `name` — mem's emptiness authority (the row IS the object), the
    /// probe the teardown re-runs inside its own transaction.
    async fn rows_are_empty(&self, name: &Name) -> Result<bool, Error> {
        let b = name.as_ref().as_str().to_owned();
        self.db.read(move |txn| -> Result<bool, Error> {
            let objects = object_meta::Table::open_readonly(txn)?;
            if has_prefix_pair::<_, _, Error>(&*objects, (b.as_str(), ""), |owner, _| {
                owner == b.as_str()
            })? {
                return Ok(false);
            }
            Ok(!upload::Table::open_readonly(txn)?.has_bucket(&b)?)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use crate::{
        _core::{CompletedPart, MultipartOps, ObjectOps, acl::Acl, object, storage::Error::*},
        _store_redb::bucket,
        _util::testing::body,
        testutil::{other_owner_id, owner_id},
    };

    #[tokio::test]
    async fn list_buckets_is_lexicographic() {
        let storage = MemoryStorage::new().unwrap();
        for n in ["zeta", "alpha", "mu-1"] {
            storage
                .create_bucket(&name(n).unwrap(), None, &Acl::default_private(None))
                .await
                .unwrap();
        }
        let names: Vec<_> = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap()
            .buckets
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, ["alpha", "mu-1", "zeta"]);
    }

    /// The teardown's whole stored state (ruling 2026-10-08): the bucket row,
    /// the emptiness probes and every plane the bucket still holds — the object,
    /// upload and part rows of both the metadata and the content side, the
    /// version plane, its index, the state row and the history blobs — retire in
    /// ONE transaction, and the rows the probes cannot see (a failed part
    /// `remove_many`'s leftover) go with them.
    #[cfg(feature = "versioning")]
    mod versioned {
        use redb::ReadTransaction;

        use super::*;
        use crate::{
            _core::{storage::VersionOps, version::State},
            _store_redb::{
                bucket_versioning, object_meta, object_part, objects, part, part_checksum,
                part_data, part_meta,
                scan::{has_prefix_pair, has_prefix_triple},
                upload as upload_row, upload_checksum, version as stored, version_index,
            },
            MemoryOptions,
            version::blobs,
        };

        /// Whether any pair-keyed row of `bucket` survives in `table`.
        fn any_pair<V, T>(table: &T, bucket: &str) -> bool
        where
            V: redb::Value + 'static,
            T: redb::ReadableTable<(&'static str, &'static str), V>,
        {
            has_prefix_pair::<V, T, Error>(table, (bucket, ""), |owner, _| owner == bucket).unwrap()
        }

        /// Whether any `(bucket, upload_id, part_number)` row of `bucket`
        /// survives in `table`.
        fn any_triple<V, T>(table: &T, bucket: &str) -> bool
        where
            V: redb::Value + 'static,
            T: redb::ReadableTable<(&'static str, &'static str, u32), V>,
        {
            has_prefix_triple::<V, T, Error>(table, (bucket, "", 0), |owner, _, _| owner == bucket)
                .unwrap()
        }

        /// How many `(bucket, upload_id, part_number)` rows the bucket still
        /// names (the boundary rule is `scan`'s: the first foreign key ends the
        /// range).
        fn count_triple<V, T>(table: &T, bucket: &str) -> u64
        where
            V: redb::Value + 'static,
            T: redb::ReadableTable<(&'static str, &'static str, u32), V>,
        {
            let mut rows = 0u64;
            for item in table.range((bucket, "", 0)..).unwrap() {
                let (key, _) = item.unwrap();
                let (owner, _, _) = key.value();
                if owner != bucket {
                    break;
                }
                rows += 1;
            }
            rows
        }

        /// How many `(bucket, key, version_id)` rows of `version_index` the
        /// bucket still names (its third element is the id, so neither the pair
        /// nor the u32-triple probe fits it).
        fn index_rows(txn: &ReadTransaction, bucket: &str) -> u64 {
            let table = version_index::Table::open_readonly(txn).unwrap();
            let mut rows = 0u64;
            for item in table.range((bucket, "", "")..).unwrap() {
                let (key, _) = item.unwrap();
                let (owner, _, _) = key.value();
                if owner != bucket {
                    break;
                }
                rows += 1;
            }
            rows
        }

        /// What the post-teardown sweep reads back: the state row, the version
        /// rows, the index rows, and one flag per swept table.
        struct Leftovers {
            state: bool,
            version_rows: u64,
            index_rows: u64,
            pair: [bool; 5],
            triple: [bool; 5],
        }

        #[tokio::test]
        async fn deleting_a_bucket_with_history_retires_every_plane() {
            let storage = MemoryStorage::with_options(MemoryOptions {
                max_object_bytes: None,
                max_total_bytes: Some(4096),
            })
            .unwrap();
            let doomed = name("doomed").unwrap();
            storage
                .create_bucket(&doomed, None, &Acl::default_private(None))
                .await
                .unwrap();
            storage
                .set_bucket_versioning(&doomed, State::Enabled)
                .await
                .unwrap();
            let key = object::key("history.bin").unwrap();
            storage
                .put_object(&doomed, &key, body(b"first!".to_vec()))
                .await
                .unwrap();
            storage
                .put_object(&doomed, &key, body(b"second!".to_vec()))
                .await
                .unwrap();
            // The delete moves the current version into the history and leaves a
            // marker: the bucket holds no object row, and the version plane holds
            // a version row, its index entry, a history blob and the state row.
            storage.delete_object(&doomed, &key).await.unwrap();
            // A failed part `remove_many`'s leftover: the upload row is gone (so
            // the occupancy probes find nothing) while the part planes and their
            // metered bytes stay behind — the residue the probes cannot see, and
            // the one this teardown has to sweep.
            let pending = object::key("pending.bin").unwrap();
            let upload = storage
                .create_multipart_upload(
                    &doomed,
                    &pending,
                    None,
                    object::Tags::empty(),
                    None,
                    &Acl::default_private(None),
                )
                .await
                .unwrap();
            storage
                .upload_part(
                    &doomed,
                    &pending,
                    &upload.upload_id,
                    1.into(),
                    body(b"orphan!".to_vec()),
                    None,
                )
                .await
                .unwrap();
            let bucket = doomed.as_ref().as_str().to_owned();
            let upload_id = upload.upload_id.clone();
            storage
                .db
                .write(|txn| -> Result<(), Error> {
                    upload_row::Table::open(txn)?.remove(&bucket, &upload_id)?;
                    Ok(())
                })
                .unwrap();
            let (left_data, left_meta) = storage
                .db
                .read(|txn| -> Result<(u64, u64), Error> {
                    Ok((
                        count_triple(&*part_data::Table::open_readonly(txn)?, &bucket),
                        count_triple(&*part_meta::Table::open_readonly(txn)?, &bucket),
                    ))
                })
                .unwrap();
            assert_eq!(
                (left_data, left_meta),
                (1, 1),
                "the failed remove_many left the part rows behind"
            );
            assert!(
                storage.total_bytes() > 0,
                "the history and the part bytes are metered"
            );

            storage.delete_bucket(&doomed).await.unwrap();

            assert!(
                matches!(
                    storage.head_bucket(&doomed).await.unwrap_err(),
                    Error::Storage(NoSuchBucket(_))
                ),
                "the bucket row died with the teardown"
            );
            assert_eq!(storage.total_bytes(), 0, "every stored byte was released");
            let Leftovers {
                state,
                version_rows,
                index_rows: index_left,
                pair: pair_left,
                triple: triple_left,
            } = storage
                .db
                .read(|txn| -> Result<Leftovers, Error> {
                    let state = bucket_versioning::Table::open_readonly(txn)?
                        .get(&bucket)?
                        .is_some();
                    let mut version_rows = 0u64;
                    stored::Table::open_readonly(txn)?.for_bucket_of(&bucket, |_, _| {
                        version_rows += 1;
                        Ok(())
                    })?;
                    let index_left = index_rows(txn, &bucket);
                    let pair_left = [
                        any_pair(&*object_meta::Table::open_readonly(txn)?, &bucket),
                        any_pair(&*objects::Table::open_readonly(txn)?, &bucket),
                        any_pair(&*upload_row::Table::open_readonly(txn)?, &bucket),
                        any_pair(&*upload_checksum::Table::open_readonly(txn)?, &bucket),
                        any_pair(&*blobs::Table::open_readonly(txn)?, &bucket),
                    ];
                    let triple_left = [
                        any_triple(&*object_part::Table::open_readonly(txn)?, &bucket),
                        any_triple(&*part::Table::open_readonly(txn)?, &bucket),
                        any_triple(&*part_checksum::Table::open_readonly(txn)?, &bucket),
                        any_triple(&*part_data::Table::open_readonly(txn)?, &bucket),
                        any_triple(&*part_meta::Table::open_readonly(txn)?, &bucket),
                    ];
                    Ok(Leftovers {
                        state,
                        version_rows,
                        index_rows: index_left,
                        pair: pair_left,
                        triple: triple_left,
                    })
                })
                .unwrap();
            assert!(!state, "the version state row died with the bucket");
            assert_eq!(version_rows, 0, "no version row survives the teardown");
            assert_eq!(
                index_left, 0,
                "no version index entry survives the teardown"
            );
            for (left, table) in pair_left.iter().zip([
                "object_meta",
                "objects",
                "uploads",
                "upload_checksums",
                "blobs",
            ]) {
                assert!(!left, "no {table} row survives the teardown");
            }
            for (left, table) in triple_left.iter().zip([
                "object_parts",
                "parts",
                "part_checksums",
                "part_data",
                "part_meta",
            ]) {
                assert!(!left, "no {table} row survives the teardown");
            }
        }
    }

    #[tokio::test]
    async fn delete_empty_bucket_succeeds_when_a_later_bucket_has_objects() {
        let storage = MemoryStorage::new().unwrap();
        let alpha = name("alpha").unwrap();
        let zeta = name("zeta").unwrap();
        storage
            .create_bucket(&alpha, None, &Acl::default_private(None))
            .await
            .unwrap();
        storage
            .create_bucket(&zeta, None, &Acl::default_private(None))
            .await
            .unwrap();
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
        storage
            .create_bucket(&bucket, None, &Acl::default_private(None))
            .await
            .unwrap();
        let key = object::key("pending.bin").unwrap();
        let upload = storage
            .create_multipart_upload(
                &bucket,
                &key,
                None,
                object::Tags::empty(),
                None,
                &Acl::default_private(None),
            )
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
                &[CompletedPart {
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
                &Acl::default_private(None),
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
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
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
            head.creation_time <= SystemTime::now(),
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
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
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
            head.creation_time <= SystemTime::now(),
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
    async fn mem_bucket_acl_self_heals_on_garbage() {
        // The read-side tolerance ruling: a bucket row whose owner/ACL
        // wires are domain-invalid serves `None`/the private default
        // (mirroring the fs decode-helper discipline). Rows are
        // API-written; the garbage below is a direct database write
        // (tampering).
        let storage = MemoryStorage::new().unwrap();
        let b = name("data").unwrap();
        storage
            .create_bucket(&b, None, &Acl::default_private(None))
            .await
            .unwrap();
        {
            let mut txn = storage.db.db().unwrap().begin_write().unwrap();
            bucket::Table::open(&mut txn)
                .unwrap()
                .insert(
                    b.as_ref().as_str(),
                    (1u64, "", "##bad-owner", "grants=%zz", ""),
                )
                .unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(
            storage.get_bucket_acl(&b).await.unwrap(),
            Acl::default_private(None),
            "garbage owner/ACL wires serve the private default with no owner"
        );
    }

    #[tokio::test]
    async fn mem_list_buckets_filters_by_owner_during_the_walk() {
        let storage = MemoryStorage::new().unwrap();
        let owner_a = owner_id();
        let owner_b = other_owner_id();
        for (bn, owner) in [
            ("alpha-a", &owner_a),
            ("alpha-b", &owner_a),
            ("beta-b", &owner_b),
        ] {
            storage
                .create_bucket(
                    &name(bn).unwrap(),
                    Some(owner),
                    &Acl::default_private(Some(owner.clone())),
                )
                .await
                .unwrap();
        }
        async fn list_filtered(
            storage: &MemoryStorage,
            owner: &acl::OwnerId,
            start_after: Option<String>,
            max: usize,
        ) -> BucketsListing {
            storage
                .list_buckets(
                    ListBucketsParams {
                        prefix: String::new(),
                        start_after,
                        max_buckets: max,
                    },
                    Some(owner),
                    None,
                )
                .await
                .unwrap()
        }
        // The filtered listing holds exactly A's buckets.
        let full = list_filtered(&storage, &owner_a, None, 1000).await;
        let names: Vec<&str> = full
            .buckets
            .iter()
            .map(|x| x.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["alpha-a", "alpha-b"], "{names:?}");
        // The continuation-token math applies to the FILTERED set: a
        // page of 1 over the filtered walk yields exactly A's two
        // buckets across two pages — B's bucket never leaks in.
        let page1 = list_filtered(&storage, &owner_a, None, 1).await;
        assert_eq!(page1.buckets.len(), 1);
        assert!(page1.truncated);
        let resume = page1.next_start_after.clone();
        assert!(
            resume.is_some(),
            "a truncated page must carry a resume marker"
        );
        let page2 = list_filtered(&storage, &owner_a, resume, 1).await;
        assert_eq!(page2.buckets.len(), 1);
        assert!(!page2.truncated);
        assert_eq!(page2.buckets[0].name.as_ref(), "alpha-b");
        // The lazy-default leg (contract amendment): a legacy row (no
        // recorded owner element) matches the `lazy_default` principal
        // and nobody else — the expected-owner comparison is on the
        // EFFECTIVE owner.
        storage
            .create_bucket(&name("legacy").unwrap(), None, &Acl::default_private(None))
            .await
            .unwrap();
        let lazy = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                Some(&owner_b),
                Some(&owner_b),
            )
            .await
            .unwrap();
        let names: Vec<&str> = lazy
            .buckets
            .iter()
            .map(|x| x.name.as_ref().as_str())
            .collect();
        assert_eq!(names, ["beta-b", "legacy"], "{names:?}");
        // The strict compare (`lazy_default = None`) excludes the
        // legacy row even for the lazy-default principal.
        let strict = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                Some(&owner_b),
                None,
            )
            .await
            .unwrap();
        assert_eq!(strict.buckets.len(), 1, "{:?}", strict.buckets);
        // The legacy listing (`None`, `None`) still shows everything.
        let legacy = storage
            .list_buckets(
                ListBucketsParams {
                    prefix: String::new(),
                    start_after: None,
                    max_buckets: 1000,
                },
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(legacy.buckets.len(), 4);
    }
}
