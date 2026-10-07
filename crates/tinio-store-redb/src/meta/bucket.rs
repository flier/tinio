//! `BucketMeta` over the shared `buckets` table — Task 5's implementation.
//!
//! One `BUCKETS` row is the five-tuple `(created-at unix nanos, tags wire,
//! owner wire, acl grants wire, cors wire)`; the wire codecs are
//! [`crate::_core::meta::bucket`]'s, so the decode here cannot drift from
//! the encode. Every decision that must be atomic (the create-or-refuse,
//! the existence-conditioned setters, the empty-bucket delete) is taken
//! **inside** the one transaction that writes it (design items 9/10, D22):
//! a missing row answers `Ok(false)` from the setter, never a pre-read.

use std::time::SystemTime;

use async_trait::async_trait;
use redb::WriteTransaction;

use crate::{
    _core::{
        acl, bucket, cors,
        meta::{BucketMeta, bucket as row},
        object, storage,
    },
    Error, RedbMetadata,
    bucket::{BucketRow, Table as BucketTable},
    object_meta::Table as MetaTable,
    scan,
    upload::Table as UploadTable,
};

/// The low-level row a contract row writes: the same five elements
/// [`row::Row::to_raw`] encodes, moved into the table layer's row type.
fn stored_row(row: &row::Row) -> BucketRow {
    let wires = row.to_raw();
    BucketRow {
        created: row.created,
        tags: wires.tags,
        owner: wires.owner,
        acl: wires.grants,
        cors: wires.cors,
    }
}

/// The existence-conditioned setter tail: a write that changed nothing (or
/// found no row) aborts its transaction, and one read breaks the tie
/// between "the row is there and already said this" (`Ok(true)`) and
/// "there is no row" (`Ok(false)`) — the tie-break tinio-fs's CORS
/// rewrite used before its facade migrated here, so re-setting an
/// identical tag set/ACL/CORS still costs no commit and no fsync.
async fn recorded(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    changed: bool,
) -> Result<bool, Error> {
    if changed {
        return Ok(true);
    }
    Ok(store.created_at(bucket).await?.is_some())
}

/// The existence-conditioned setter's write half: the table op reports
/// whether it changed the row, `write_if` drops the transaction without a
/// commit when it did not (the no-fsync arm), and [`recorded`] breaks the
/// tie between "the row is there and already said this" and "there is no
/// row". One home for the three steps the four setters share; only the
/// table call differs.
async fn set_if<F>(store: &RedbMetadata, bucket: &bucket::Name, op: F) -> Result<bool, Error>
where
    F: FnOnce(&mut WriteTransaction) -> Result<Option<()>, Error> + Send + 'static,
{
    let changed = store.driver.write_if(op).await?.is_some();
    recorded(store, bucket, changed).await
}

#[async_trait]
impl BucketMeta for RedbMetadata {
    async fn created_at(&self, bucket: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        let bucket = bucket.to_string();
        self.driver
            .read(move |txn| BucketTable::open_readonly(txn)?.get(&bucket))
    }

    async fn load_many(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<SystemTime>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        self.driver.read(move |txn| {
            let table = BucketTable::open_readonly(txn)?;
            names.iter().map(|name| table.get(name)).collect()
        })
    }

    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        // A whole-table walk: the decode and the allocation run on the
        // blocking pool (the driver's own rule for traversals).
        self.driver
            .read_blocking(|txn| {
                let table = BucketTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_each(|name, created| {
                    out.push((name.to_owned(), created));
                    Ok(())
                })?;
                Ok(out)
            })
            .await
    }

    async fn row(&self, bucket: &bucket::Name) -> Result<Option<row::Row>, Error> {
        let bucket = bucket.to_string();
        self.driver.read(move |txn| {
            Ok(BucketTable::open_readonly(txn)?
                .row(&bucket)?
                .map(|stored| {
                    row::Row::from_raw(
                        storage::to_nanos(stored.created),
                        &stored.tags,
                        &stored.owner,
                        &stored.acl,
                        &stored.cors,
                    )
                }))
        })
    }

    async fn owners(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<acl::OwnerId>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        self.driver.read(move |txn| {
            let table = BucketTable::open_readonly(txn)?;
            names
                .iter()
                .map(|name| {
                    Ok(table
                        .row(name)?
                        .and_then(|stored| row::decode_owner(&stored.owner)))
                })
                .collect()
        })
    }

    async fn record(&self, bucket: &bucket::Name, created: SystemTime) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| BucketTable::open(txn)?.put(&bucket, created))
            .await
    }

    async fn create(&self, bucket: &bucket::Name, row: &row::Row) -> Result<(), Error> {
        let name = bucket.clone();
        let key = bucket.to_string();
        let stored = stored_row(row);
        self.driver
            .write(move |txn| {
                let mut table = BucketTable::open(txn)?;
                if table.exists(&key)? {
                    return Err(Error::Refused(storage::already_exists(&name)));
                }
                table.put_full(&key, &stored)
            })
            .await
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| BucketTable::open(txn)?.get_or_insert(&bucket, now))
            .await
    }

    async fn record_full(&self, bucket: &bucket::Name, row: &row::Row) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let stored = stored_row(row);
        self.driver
            .write(move |txn| BucketTable::open(txn)?.put_full(&bucket, &stored))
            .await
    }

    async fn set_tags(&self, bucket: &bucket::Name, tags: &object::Tags) -> Result<bool, Error> {
        let row = bucket.to_string();
        let tags = tags.clone();
        set_if(self, bucket, move |txn| {
            Ok(BucketTable::open(txn)?
                .put_tags(&row, &tags)?
                .and_then(|changed| changed.then_some(())))
        })
        .await
    }

    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), Error> {
        let row = bucket.to_string();
        let tags = tags.clone();
        // One transaction: the table's create-or-rewrite decides presence
        // and writes inside it, and a no-op (an identical wire) aborts
        // rather than committing — `Ok(false)` is exactly "nothing to
        // write", so the `write_if` abort is the no-fsync arm.
        self.driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                Ok(BucketTable::open(txn)?
                    .put_tags_or_create(&row, &tags, now)?
                    .then_some(()))
            })
            .await
            .map(|_| ())
    }

    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let row = bucket.to_string();
        set_if(self, bucket, move |txn| {
            Ok(BucketTable::open(txn)?
                .clear_tags(&row)?
                .and_then(|changed| changed.then_some(())))
        })
        .await
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, Error> {
        let row = bucket.to_string();
        let owner_wire = row::encode_owner(owner);
        let grants_wire = row::encode_grants(&acl::Acl {
            owner: None,
            grants: grants.clone(),
        });
        let changed = self
            .driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                let mut table = BucketTable::open(txn)?;
                let Some(mut stored) = table.row(&row)? else {
                    return Ok(None);
                };
                if stored.owner == owner_wire && stored.acl == grants_wire {
                    return Ok(None);
                }
                stored.owner = owner_wire;
                stored.acl = grants_wire;
                table.put_full(&row, &stored)?;
                Ok(Some(()))
            })
            .await?
            .is_some();
        recorded(self, bucket, changed).await
    }

    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &acl::AclGrants,
        now: SystemTime,
    ) -> Result<(), Error> {
        let row = bucket.to_string();
        let grants_wire = row::encode_grants(&acl::Acl {
            owner: None,
            grants: grants.clone(),
        });
        // One transaction: the row is read (or inserted), the grant element
        // replaced and the owner element carried over — all inside the one
        // closure, so the owner's writer cannot slip between a read and a
        // write. An unchanged wire aborts (no commit, no fsync).
        self.driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                let mut table = BucketTable::open(txn)?;
                let Some(mut stored) = table.row(&row)? else {
                    // First sight: the row is recorded WITH the grant set,
                    // so no reader sees it without them — and with no owner
                    // (a bucket never sighted through the API has none).
                    table.put_full(
                        &row,
                        &BucketRow {
                            acl: grants_wire,
                            ..BucketRow::at(now)
                        },
                    )?;
                    return Ok(Some(()));
                };
                if stored.acl == grants_wire {
                    return Ok(None);
                }
                stored.acl = grants_wire;
                table.put_full(&row, &stored)?;
                Ok(Some(()))
            })
            .await
            .map(|_| ())
    }

    async fn set_cors(&self, bucket: &bucket::Name, config: &cors::Config) -> Result<bool, Error> {
        let row = bucket.to_string();
        let config = config.clone();
        set_if(self, bucket, move |txn| {
            Ok(BucketTable::open(txn)?
                .put_cors(&row, &config)?
                .and_then(|changed| changed.then_some(())))
        })
        .await
    }

    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let row = bucket.to_string();
        set_if(self, bucket, move |txn| {
            Ok(BucketTable::open(txn)?
                .clear_cors(&row)?
                .and_then(|changed| changed.then_some(())))
        })
        .await
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| BucketTable::open(txn)?.remove(&bucket))
            .await
    }

    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let name = bucket.clone();
        let key = bucket.to_string();
        self.driver
            .write(move |txn| {
                if BucketTable::open(txn)?.row(&key)?.is_none() {
                    return Err(Error::Refused(storage::no_such_bucket(&name)));
                }
                // The two occupancy probes in the deleting transaction:
                // one key-range seek each (never a walk), so a concurrent
                // write cannot slip between the check and the removal.
                let objects = {
                    let table = MetaTable::open(txn)?;
                    scan::has_prefix_pair::<_, _, Error>(
                        &table.0,
                        (key.as_str(), ""),
                        |owner, _| owner == key,
                    )?
                };
                let uploads = UploadTable::open(txn)?.has_bucket(&key)?;
                if objects || uploads {
                    return Err(Error::Refused(storage::not_empty(&name)));
                }
                BucketTable::open(txn)?.remove(&key)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use tinio_util::testing::metadata::assert_bucket_meta_conformance;

    use crate::{
        _core::{
            acl::{Acl, AclGrants, OwnerId},
            bucket, cors,
            etag::ETag,
            meta::{BucketMeta, bucket as row},
            object, storage,
        },
        Error, RedbMetadata,
        object_meta::{self, Stored},
    };

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// A grant set (the owner's own default grants).
    fn grants() -> AclGrants {
        Acl::default_private(Some(owner())).grants
    }

    fn owner() -> OwnerId {
        OwnerId::new("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap()
    }

    fn tags(pairs: &[(&str, &str)]) -> object::Tags {
        object::Tags::from_pairs(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        )
        .unwrap()
    }

    /// The tag set as owned pairs — [`object::Tags::iter`] yields borrowed
    /// `(&str, &str)` tuples, so a leg comparing against `Vec<(String,
    /// String)>` needs this side (the shared suite's `pairs` helper).
    fn pairs(tags: &object::Tags) -> Vec<(String, String)> {
        tags.iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    /// One valid object row to plant for the occupancy probe.
    fn held_row() -> Stored {
        Stored {
            etag: ETag::new("d41d8cd98f00b204e9800998ecf8427e").unwrap(),
            size: 0,
            mtime: 0,
            file_identity: 0,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: Acl::default_private(None),
            version_id: None,
            version_seq: 0,
            is_delete_marker: false,
        }
    }

    /// The suite's bucket leg against a fresh in-memory implementation.
    #[tokio::test]
    async fn bucket_meta_conforms() {
        let store = RedbMetadata::in_memory().unwrap();
        assert_bucket_meta_conformance(&store).await;
    }

    /// The refusals D22 pins that the bucket leg alone cannot reach:
    /// `create` answers `AlreadyExists` for a recorded name, and
    /// `remove_if_empty` answers `NoSuchBucket` for a missing row and
    /// `NotEmpty` while another category's row still names the bucket.
    #[tokio::test]
    async fn refusals_project_onto_the_contract_error() {
        let store = RedbMetadata::in_memory().unwrap();
        let name = bucket::name("refusing").unwrap();
        store.create(&name, &row::Row::at(t(1))).await.unwrap();
        let refused: storage::Error = store
            .create(&name, &row::Row::at(t(2)))
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refused, storage::Error::AlreadyExists(_)),
            "{refused:?}"
        );

        let key = name.to_string();
        let planted = key.clone();
        store
            .driver()
            .write(move |txn| -> Result<(), Error> {
                object_meta::Table::open(txn)?.put(&planted, "held.txt", &held_row())
            })
            .await
            .unwrap();
        let refused: storage::Error = store.remove_if_empty(&name).await.unwrap_err().into();
        assert!(
            matches!(refused, storage::Error::NotEmpty(_)),
            "an object row keeps the bucket non-empty, got {refused:?}"
        );
        store
            .driver()
            .write(move |txn| -> Result<(), Error> {
                object_meta::Table::open(txn)?.remove(&key, "held.txt")
            })
            .await
            .unwrap();

        store.remove_if_empty(&name).await.unwrap();
        let refused: storage::Error = store.remove_if_empty(&name).await.unwrap_err().into();
        assert!(
            matches!(refused, storage::Error::NoSuchBucket(_)),
            "a removed row refuses as NoSuchBucket, got {refused:?}"
        );
    }

    /// The create-or-update tag write is ONE transaction in both arms: a
    /// first-sight record and a replace are each one write transaction, and
    /// the record lands WITH the tags (a reader never sees a bare row).
    /// The write-transaction count is the driver's own (`write_lock_stats`,
    /// one per `timed_write`, commit or abort).
    #[tokio::test]
    async fn set_tags_or_create_records_and_replaces_in_one_transaction() {
        let store = RedbMetadata::in_memory().unwrap();
        let name = bucket::name("first-sight-tags").unwrap();
        let writes = || store.driver().write_lock_stats().count;

        // First sight: one write transaction, the row recorded with the
        // tags at `now`.
        let before = writes();
        store
            .set_tags_or_create(&name, &tags(&[("env", "prod")]), t(7))
            .await
            .unwrap();
        assert_eq!(
            writes() - before,
            1,
            "the first-sight tag record is one write"
        );
        let recorded = store.row(&name).await.unwrap().expect("the recorded row");
        assert_eq!(recorded.created, t(7), "the first-sight row carries `now`");
        assert_eq!(
            pairs(&recorded.tags),
            vec![("env".to_owned(), "prod".to_owned())]
        );
        assert!(recorded.cors.is_none(), "a fresh row has no other element");

        // Replace: one write transaction, the creation time and the other
        // elements ride.
        let config = cors::Config {
            rules: vec![cors::Rule {
                id: Some("kept".to_owned()),
                allowed_methods: vec!["GET".to_owned()],
                allowed_origins: vec!["*".to_owned()],
                allowed_headers: None,
                expose_headers: None,
                max_age_seconds: None,
            }],
        };
        store.set_cors(&name, &config).await.unwrap();
        let before = writes();
        store
            .set_tags_or_create(&name, &tags(&[("team", "core")]), t(9))
            .await
            .unwrap();
        assert_eq!(writes() - before, 1, "the tag replace is one write");
        let replaced = store.row(&name).await.unwrap().unwrap();
        assert_eq!(replaced.created, t(7), "the recorded time is preserved");
        assert_eq!(
            pairs(&replaced.tags),
            vec![("team".to_owned(), "core".to_owned())]
        );
        assert_eq!(
            replaced.cors,
            Some(config),
            "the other elements are preserved"
        );

        // An identical set is a no-op body (the transaction still opens and
        // aborts): the row is unchanged.
        let before = writes();
        store
            .set_tags_or_create(&name, &tags(&[("team", "core")]), t(11))
            .await
            .unwrap();
        assert_eq!(
            writes() - before,
            1,
            "an identical set is still one transaction"
        );
        assert_eq!(store.row(&name).await.unwrap().unwrap(), replaced);
    }

    /// The grants create-or-update write is ONE transaction in both arms and
    /// preserves the recorded owner element: the first-sight row carries the
    /// grants and no owner, a later write replaces only the grants.
    #[tokio::test]
    async fn set_grants_or_create_preserves_the_owner_element() {
        let store = RedbMetadata::in_memory().unwrap();
        let name = bucket::name("first-sight-grants").unwrap();
        let writes = || store.driver().write_lock_stats().count;
        let held = owner();

        // First sight: one write transaction, the grants recorded, no owner.
        let before = writes();
        store
            .set_grants_or_create(&name, &grants(), t(3))
            .await
            .unwrap();
        assert_eq!(
            writes() - before,
            1,
            "the first-sight grant record is one write"
        );
        let recorded = store.row(&name).await.unwrap().expect("the recorded row");
        assert_eq!(recorded.created, t(3));
        assert_eq!(recorded.acl.grants, grants());
        assert_eq!(
            recorded.owner, None,
            "a bucket never sighted through the API carries no owner"
        );

        // A recorded owner (the create-time ACL row) rides the later grants
        // write: the row read and the write are the same transaction, so the
        // owner's writer cannot slip between them.
        let mut with_owner = recorded.clone();
        with_owner.owner = Some(held.clone());
        store.record_full(&name, &with_owner).await.unwrap();
        let empty: AclGrants = Vec::new();
        let before = writes();
        store
            .set_grants_or_create(&name, &empty, t(5))
            .await
            .unwrap();
        assert_eq!(writes() - before, 1, "the grant replace is one write");
        let replaced = store.row(&name).await.unwrap().unwrap();
        assert_eq!(
            replaced.owner,
            Some(held),
            "the recorded owner is carried over"
        );
        assert!(replaced.acl.grants.is_empty(), "the grants are replaced");
        assert_eq!(replaced.created, t(3), "the recorded time is preserved");

        // An identical set is a no-op body: the row is unchanged.
        let before = writes();
        store
            .set_grants_or_create(&name, &empty, t(13))
            .await
            .unwrap();
        assert_eq!(
            writes() - before,
            1,
            "an identical set is still one transaction"
        );
        assert_eq!(store.row(&name).await.unwrap().unwrap(), replaced);
    }
}
