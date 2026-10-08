//! The `BucketOps` implementation for [`MemoryStorage`].
//!
//! Bucket lifecycle over the shared [`BucketMeta`] contract: every bucket
//! row read and write delegates to the crate's metadata handle, and the
//! contract's error becomes mem's at the boundary. The decisions that must
//! be atomic — the create-or-refuse, the empty-bucket delete, the element
//! setters' presence outcome — are taken inside the contract's own
//! transaction, never as a caller-side pre-read.
//!
//! `delete_bucket` keeps the version state's teardown beside the contract
//! delete: the bucket's version rows, their index, its state row and its
//! blobs die with the bucket (spec 2026-10-03).

use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use redb::ReadableTable;

#[cfg(test)]
use crate::_core::bucket::name;
#[cfg(feature = "versioning")]
use crate::version;
use crate::{
    _core::{
        Bucket, BucketOps, BucketsListing, ListBucketsParams, acl,
        acl::{Acl, AclGrants},
        bucket,
        bucket::Name,
        content::ContentKey,
        cors,
        meta::{BucketMeta, bucket as row},
        multipart, object, paginate_ordered,
        storage::not_empty,
    },
    _store_redb::{
        Error as StoreError, bucket as bucket_row, object_meta, object_part, objects, part,
        part_checksum, part_data, part_meta,
        scan::{drain_pair, drain_triple, for_each_pair, has_prefix_pair},
        upload, upload_checksum,
    },
    Error,
    error::no_such_bucket,
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
        // The create-time row and the already-exists refusal are ONE contract
        // transaction (`BucketMeta::create`, D22): a concurrent create on the
        // same name cannot be raced, and the refusal rides out as
        // `AlreadyExists`. The owner element rides the row beside the grants
        // (the row form keeps the two apart).
        let mut recorded = row::Row::at(SystemTime::now());
        recorded.owner = owner.cloned();
        recorded.acl = Acl {
            owner: None,
            grants: acl.grants.clone(),
        };
        self.metadata().create(name, &recorded).await?;
        Ok(())
    }

    async fn delete_bucket(&self, name: &Name) -> Result<(), Error> {
        // The bucket row, the emptiness probes, the row's removal and the
        // bucket's whole stored state are ONE transaction. Elided the
        // contract's `remove_if_empty` + `drain_bucket` to make the pair
        // atomic (ruling 2026-10-08): with two transactions, a bucket of the
        // same name re-created in the window between them would lose the new
        // bucket's version rows and blobs. This is mem's deliberate deviation —
        // P2's "second consumer proves the contract is expressible" — and the
        // probes below are the contract's semantics, spelled once here.
        //
        // The accounting lock is held across the removal and the release so the
        // bytes this teardown retires and the tracked total move together (the
        // lock is not reentrant: no locking accounting entry point may run
        // inside it).
        let guard = self.lock_accounting().await;
        let name = name.clone();
        let b = name.as_ref().as_str().to_owned();
        // The decorator's released-but-present marks: a key whose bytes a
        // failed removal already released must not join this release again.
        let content = Arc::clone(self.content());
        let marks = name.clone();
        let purge = name.clone();
        let released = self.db.write(move |txn| -> Result<u64, Error> {
            // The bucket row is the bucket: its absence is the contract's
            // `Error::Refused(NoSuchBucket)` projection.
            if bucket_row::Table::open(txn)?.row(&b)?.is_none() {
                return Err(no_such_bucket(&name));
            }
            // The two occupancy probes in the deleting transaction — one
            // key-range seek each, never a walk — so a concurrent write cannot
            // slip between the check and the removal. S3 answers BucketNotEmpty
            // for object rows and in-progress uploads alike.
            let objects = {
                let table = object_meta::Table::open(txn)?;
                has_prefix_pair::<_, _, Error>(&*table, (b.as_str(), ""), |owner, _| {
                    owner == b.as_str()
                })?
            };
            if objects || upload::Table::open(txn)?.has_bucket(&b)? {
                return Err(Error::from(not_empty(&name)));
            }
            bucket_row::Table::open(txn)?.remove(&b)?;
            // The bucket's stored state dies with it (spec 2026-10-03): the
            // object and upload planes' rows, mem's own part planes, and — under
            // the `versioning` feature — the version plane and the history
            // blobs, all in this same transaction. Whatever rows the probes
            // above cannot see (a failed `remove_many`'s leftover) are swept
            // by the bucket prefix here, and their metered bytes join the
            // release: the tracked total drops by exactly what this transaction
            // removes.
            object_meta::Table::open(txn)?.drain_bucket(&b)?;
            object_part::Table::open(txn)?.drain_bucket(&b)?;
            upload::Table::open(txn)?.drain_bucket(&b)?;
            upload_checksum::Table::open(txn)?.drain_bucket(&b)?;
            part::Table::open(txn)?.drain_bucket(&b)?;
            part_checksum::Table::open(txn)?.drain_bucket(&b)?;
            // The content plane's two byte tables: `objects` (an object's
            // bytes) and `part_data` (a part's). The occupancy probe reads the
            // object ROWS and the upload ROWS, so a byte row those planes no
            // longer name — what a failed `remove_many` leaves behind — is
            // invisible to it: it would become unreachable bytes the tracked
            // total still counts. The lengths summed here are the same bytes
            // the decorator counted when they were published, so the release is
            // exact. `part_meta` carries no bytes of its own (the size/mtime
            // pair), so it is drained without a sum.
            let mut released = {
                let mut total = 0u64;
                for_each_pair::<_, _, _, Error>(
                    &*objects::Table::open(txn)?,
                    (b.as_str(), ""),
                    |owner, _| owner == b.as_str(),
                    |_, key, value| {
                        let marked = object::key(key).is_ok_and(|key| {
                            content.released_undead(&marks, &ContentKey::Object(key))
                        });
                        if !marked {
                            total += value.len() as u64;
                        }
                        Ok(())
                    },
                )?;
                // `part_data` is keyed `(bucket, upload_id, part_number)` and
                // the redb crate has no triple walk that hands the value over:
                // the walk below is the bucket prefix with the first foreign key
                // ending it (`scan`'s boundary rule), the same shape
                // `part_data::Table::total_len` uses per upload. The handle
                // drops before the drain runs (redb asserts when a page is freed
                // under a live read reference — the rule `scan::drain_pair`
                // documents).
                let table = part_data::Table::open(txn)?;
                for item in table
                    .range((b.as_str(), "", 0)..)
                    .map_err(StoreError::from)?
                {
                    let (key, value) = item.map_err(StoreError::from)?;
                    let (owner, upload_id, number) = key.value();
                    if owner != b.as_str() {
                        break;
                    }
                    let marked = multipart::part_number(number).is_ok_and(|number| {
                        content.released_undead(
                            &marks,
                            &ContentKey::Part {
                                upload_id: upload_id.to_owned(),
                                number,
                            },
                        )
                    });
                    if !marked {
                        total += value.value().len() as u64;
                    }
                }
                total
            };
            drain_pair(
                &mut *objects::Table::open(txn)?,
                (b.as_str(), ""),
                |owner, _| owner == b.as_str(),
            )?;
            drain_triple(
                &mut *part_data::Table::open(txn)?,
                (b.as_str(), "", 0),
                |owner, _, _| owner == b.as_str(),
            )?;
            drain_triple(
                &mut *part_meta::Table::open(txn)?,
                (b.as_str(), "", 0),
                |owner, _, _| owner == b.as_str(),
            )?;
            // The version plane's history blobs are metered like the rest.
            #[cfg(feature = "versioning")]
            let version_bytes = version::drain_bucket_in_txn(txn, &name)?;
            #[cfg(not(feature = "versioning"))]
            let version_bytes = 0u64;
            released += version_bytes;
            Ok(released)
        })?;
        self.release_total_unlocked(-(released as i64));
        // The marks this teardown consumed are spent, and one that matched no
        // row named bytes already gone: the bucket's marks leave with it.
        self.content().purge_released_undead(&purge);
        drop(guard);
        Ok(())
    }

    async fn head_bucket(&self, name: &Name) -> Result<Bucket, Error> {
        // The recorded creation time is the row's own (`BucketMeta::row`;
        // a row-less bucket is `NoSuchBucket`, the row IS the bucket in mem).
        match self.metadata().row(name).await? {
            Some(recorded) => Ok(Bucket {
                name: name.clone(),
                creation_time: recorded.created,
            }),
            None => Err(no_such_bucket(name)),
        }
    }

    async fn list_buckets(
        &self,
        params: ListBucketsParams,
        owner: Option<&acl::OwnerId>,
        lazy_default: Option<&acl::OwnerId>,
    ) -> Result<BucketsListing, Error> {
        // BUCKETS is keyed by name, so the contract's whole-table walk is
        // already name order; the prefix filter and the exclusive-after
        // marker run in the shared pager. The walk materializes the name
        // list (the F05 note of the lazy scan this replaces: it only touched
        // the rows the engine visits — immaterial at bucket counts, the S3
        // account ceiling is ~1,000 buckets).
        let candidates: Vec<(String, SystemTime)> = self
            .metadata()
            .load_all()
            .await?
            .into_iter()
            .filter(|(name, _)| name.starts_with(&params.prefix))
            .collect();
        // The `owner` filter applies DURING the walk, before pagination
        // (contract review P2#4 — the continuation-token math applies to the
        // filtered set): the candidate names collect first, then ONE contract
        // read resolves every candidate's recorded owner element, and only
        // the principal's buckets reach the pager (the fs backend's two-phase
        // shape). The comparison uses the EFFECTIVE owner — the recorded
        // element, or `lazy_default` for a row whose wire is empty (the
        // uniform lazy-default semantics, review B4 — such rows belong to the
        // default principal); `lazy_default = None` is the strict compare.
        // `None` owner = no filter, the legacy listing.
        let mut items: Vec<Bucket> = Vec::new();
        match owner {
            Some(owner) => {
                // The stored names are the ones `create_bucket` validated, so a
                // stored name that does not convert is a hand-edited store: report
                // it rather than panicking on the way out of a listing.
                let names: Vec<Name> = candidates
                    .iter()
                    .map(|(name, _)| bucket::name(name.clone()))
                    .collect::<Result<Vec<Name>, _>>()?;
                let recorded = self.metadata().owners(&names).await?;
                for ((name, creation_time), recorded_owner) in candidates.into_iter().zip(recorded)
                {
                    if recorded_owner.as_ref().or(lazy_default) == Some(owner) {
                        items.push(Bucket {
                            name: bucket::name(name)?,
                            creation_time,
                        });
                    }
                }
            }
            None => {
                for (name, creation_time) in candidates {
                    items.push(Bucket {
                        name: bucket::name(name)?,
                        creation_time,
                    });
                }
            }
        }
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
    }

    async fn get_bucket_tags(&self, name: &Name) -> Result<object::Tags, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`; the row IS the bucket in mem, written
        // at create, so the fs backend's row-less pre-existing bucket has
        // no mem equivalent). The tags are the row's own element, empty
        // when the wire is absent or domain-invalid (self-healing, cap 50
        // — the shared decode-home rule).
        match self.metadata().row(name).await? {
            Some(recorded) => Ok(recorded.tags),
            None => Err(no_such_bucket(name)),
        }
    }

    async fn put_bucket_tags(&self, name: &Name, tags: &object::Tags) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`), answered by the contract's guarded
        // setter: `Ok(false)` is the row-less bucket, decided inside the
        // writing transaction.
        if self.metadata().set_tags(name, tags).await? {
            return Ok(());
        }
        Err(no_such_bucket(name))
    }

    async fn delete_bucket_tags(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: idempotent — a missing bucket is Ok (the
        // contract's delete leniency, mirroring the fs backend's
        // row-only clear). A live row keeps its creation time and loses
        // its tags; a row-less bucket has nothing to clear.
        self.metadata().clear_tags(name).await?;
        Ok(())
    }

    async fn get_bucket_cors(&self, name: &Name) -> Result<Option<cors::Config>, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`). The configuration is the row's own
        // element, None when it is the `''` wire (a bucket that was cleared
        // or never configured) or domain-invalid (self-healing).
        match self.metadata().row(name).await? {
            Some(recorded) => Ok(recorded.cors),
            None => Err(no_such_bucket(name)),
        }
    }

    async fn put_bucket_cors(&self, name: &Name, config: &cors::Config) -> Result<(), Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`), answered by the contract's guarded
        // setter. A zero-rule config normalizes to the `''` wire by the
        // codec itself (op-review G2 — "no configuration").
        if self.metadata().set_cors(name, config).await? {
            return Ok(());
        }
        Err(no_such_bucket(name))
    }

    async fn delete_bucket_cors(&self, name: &Name) -> Result<(), Error> {
        // S3 semantics: `NoSuchBucket` when the bucket is missing — the
        // delete-tagging idempotent leniency does NOT extend here (the
        // CORS spec mandates `NoSuchBucket` for a missing bucket); a
        // bucket that simply has no configuration clears to the same
        // state (idempotent).
        if self.metadata().clear_cors(name).await? {
            return Ok(());
        }
        Err(no_such_bucket(name))
    }

    async fn get_bucket_acl(&self, name: &Name) -> Result<Acl, Error> {
        // Existence is the `BUCKETS` row (`NoSuchBucket` when missing —
        // mirroring `head_bucket`; the row IS the bucket in mem, written
        // at create). The ACL recombines the stored owner element with
        // the stored grant set (the row form keeps them apart); a
        // domain-invalid owner/ACL wire self-heals to `None`/the private
        // default (the shared decode-home rule).
        match self.metadata().row(name).await? {
            Some(recorded) => Ok(Acl {
                owner: recorded.owner,
                grants: recorded.acl.grants,
            }),
            None => Err(no_such_bucket(name)),
        }
    }

    async fn put_bucket_acl(&self, name: &Name, grants: &AclGrants) -> Result<(), Error> {
        // Replace-all: the grant set replaces the row's ACL element; the
        // creation time, tags and owner element are preserved — a put
        // never changes the owner (contract ruling). `NoSuchBucket` when
        // the row is missing.
        //
        // The owner element is read first and handed back to the contract's
        // guarded setter, which replaces both elements inside ONE writing
        // transaction. `set_grants_or_create` is the fs first-sight path and
        // would create a row here; mem must answer `NoSuchBucket` instead, so
        // the row-missing outcome is the setter's `Ok(false)` after the
        // owner read.
        let Some(recorded) = self.metadata().row(name).await? else {
            return Err(no_such_bucket(name));
        };
        if !self
            .metadata()
            .set_acl(name, recorded.owner.as_ref(), grants)
            .await?
        {
            return Err(no_such_bucket(name));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
