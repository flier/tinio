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
        acl, bucket, cors, lifecycle,
        meta::{BucketMeta, bucket as row},
        object, object_lock, storage,
        version::State,
    },
    Error, RedbMetadata,
    bucket::{BucketRow, Table as BucketTable},
    bucket_lifecycle::Table as LifecycleTable,
    bucket_object_lock::Table as ObjectLockTable,
    bucket_versioning::Table as VersioningTable,
    object_meta::Table as MetaTable,
    upload::Table as UploadTable,
    upload_object_lock::Table as UploadLockTable,
    version::Table as VersionTable,
    version_object_lock::Table as VersionLockTable,
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

/// The shared body of a bucket-row write: refuse a taken name (writing
/// nothing) and insert the row, then clear the lifecycle row a bucket that
/// vanished out of band would otherwise leave behind (LC19). The caller's own
/// transaction adds whatever else the write implies — the Object Lock clear
/// for an ordinary create, the versioning state and the configuration for
/// `create_bucket_with_lock`.
fn insert_bucket_row(
    txn: &mut WriteTransaction,
    key: &str,
    name: &bucket::Name,
    stored: &BucketRow,
) -> Result<(), Error> {
    {
        let mut table = BucketTable::open(txn)?;
        if table.exists(key)? {
            return Err(Error::Refused(storage::already_exists(name)));
        }
        table.put_full(key, stored)?;
    }
    // LC19: a table of its own does not inherit the bucket row's upsert, so a
    // row left behind by a bucket that vanished out of band would otherwise
    // expire the new bucket's objects.
    LifecycleTable::open(txn)?.drain_bucket(key)
}

/// Whether `bucket` still holds any row whose presence refuses its teardown:
/// the occupancy predicate `remove_if_empty` applies, in the caller's own
/// transaction. One key-range seek per plane, never a walk — a consumer that
/// elides the contract's two-transaction pair (tinio-mem) asks this so its
/// emptiness decision cannot drift from the redb store's.
pub fn bucket_occupied(txn: &mut WriteTransaction, bucket: &str) -> Result<bool, Error> {
    let objects = MetaTable::open(txn)?.has_bucket(bucket)?;
    let uploads = UploadTable::open(txn)?.has_bucket(bucket)?;
    let versions = VersionTable::open(txn)?.has_bucket(bucket)?;
    let version_locks = VersionLockTable::open(txn)?.has_bucket(bucket)?;
    let upload_locks = UploadLockTable::open(txn)?.has_bucket(bucket)?;
    Ok(objects || uploads || versions || version_locks || upload_locks)
}

impl RedbMetadata {
    /// Create a bucket with Object Lock enabled (OL13) in ONE write
    /// transaction: the bucket row, its `Enabled` versioning state and
    /// `config`.
    ///
    /// The compensating composition (the ops engine's
    /// `create_bucket_with_lock`) exists for a backend whose three facts are
    /// three writes; a store whose rows share this database commits them
    /// together, so no failure can leave a partial lock-enabled bucket. A
    /// taken name is `AlreadyExists`, and the existence check and the three
    /// inserts are the same transaction — the refusal writes nothing. The
    /// same create-shape rule as [`BucketMeta::create`] applies to the
    /// configuration tables: a stale lifecycle row of a name that vanished
    /// out of band is cleared (LC19), and the lock row is overwritten by
    /// `config` rather than inherited.
    pub async fn create_bucket_with_lock(
        &self,
        bucket: &bucket::Name,
        row: &row::Row,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        let name = bucket.clone();
        let key = bucket.to_string();
        let stored = stored_row(row);
        let config = config.clone();
        self.driver
            .write(move |txn| {
                insert_bucket_row(txn, &key, &name, &stored)?;
                VersioningTable::open(txn)?.set(&key, State::Enabled)?;
                ObjectLockTable::open(txn)?.set(&key, &config)
            })
            .await
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

/// Read the lifecycle row, upsert `rules`, validate, and write, in the
/// caller's transaction. `create_at` records a first-sight bucket row when
/// the store has none (the lazy policy); without it a missing bucket is
/// [`lifecycle::Merged::Absent`] and nothing is written. A refusal writes
/// neither row.
fn merge_row(
    txn: &mut WriteTransaction,
    row: &str,
    rules: &[lifecycle::Rule],
    create_at: Option<SystemTime>,
) -> Result<lifecycle::Merged, Error> {
    let present = BucketTable::open(txn)?.exists(row)?;
    if !present && create_at.is_none() {
        return Ok(lifecycle::Merged::Absent);
    }
    let current = LifecycleTable::open(txn)?
        .get(row)?
        .unwrap_or(lifecycle::Config { rules: Vec::new() });
    let config = match current.apply_rules(rules) {
        Ok(config) => config,
        Err(invalid) => return Ok(lifecycle::Merged::Refused(invalid)),
    };
    if let Some(now) = create_at.filter(|_| !present) {
        BucketTable::open(txn)?.put_full(row, &BucketRow::at(now))?;
        // OL4: the first-sight insert is a bucket-row write.
        ObjectLockTable::open(txn)?.drain_bucket(row)?;
    }
    LifecycleTable::open(txn)?.set(row, &config)?;
    Ok(lifecycle::Merged::Stored)
}

/// The existence-conditioned setter's write half: the table op reports
/// whether it changed the row, `write_if` drops the transaction without a
/// commit when it did not (the no-fsync arm), and [`recorded`] breaks the
/// tie between "the row is there and already said this" and "there is no
/// row". One home for the three steps the six setters share; only the
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
            .write(move |txn| {
                BucketTable::open(txn)?.put(&bucket, created)?;
                // OL4: a bucket-row writer clears the Object Lock
                // configuration in the same write. The row's other elements
                // are cleared here too, so the configuration cannot describe a
                // bucket state this write replaced.
                ObjectLockTable::open(txn)?.drain_bucket(&bucket)
            })
            .await
    }

    async fn create(&self, bucket: &bucket::Name, row: &row::Row) -> Result<(), Error> {
        let name = bucket.clone();
        let key = bucket.to_string();
        let stored = stored_row(row);
        self.driver
            .write(move |txn| {
                insert_bucket_row(txn, &key, &name, &stored)?;
                // OL4, the Object Lock twin of the LC19 rule: a stale
                // configuration never protects the new bucket's writes.
                ObjectLockTable::open(txn)?.drain_bucket(&key)
            })
            .await
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<(SystemTime, bool), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| {
                let (created, inserted) = BucketTable::open(txn)?.get_or_insert(&bucket, now)?;
                // OL4: only a first-sight insert is a bucket-row write here;
                // an existing row (and the configuration beside it) rides.
                if inserted {
                    ObjectLockTable::open(txn)?.drain_bucket(&bucket)?;
                }
                Ok((created, inserted))
            })
            .await
    }

    async fn record_full(&self, bucket: &bucket::Name, row: &row::Row) -> Result<(), Error> {
        let bucket = bucket.to_string();
        let stored = stored_row(row);
        self.driver
            .write(move |txn| {
                BucketTable::open(txn)?.put_full(&bucket, &stored)?;
                // LC19, the `create` drain's twin: this is the lazy policy's
                // re-create (a store whose buckets outlive their rows adopts
                // an existing record the same way).
                LifecycleTable::open(txn)?.drain_bucket(&bucket)?;
                // OL4: every bucket-row writer clears the Object Lock
                // configuration beside the LC19 rule.
                ObjectLockTable::open(txn)?.drain_bucket(&bucket)
            })
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
                // OL4: a first-sight insert is a bucket-row writer and clears
                // a stale Object Lock configuration; an existing row rides.
                let present = BucketTable::open(txn)?.exists(&row)?;
                let changed = BucketTable::open(txn)?.put_tags_or_create(&row, &tags, now)?;
                if !present {
                    ObjectLockTable::open(txn)?.drain_bucket(&row)?;
                }
                Ok(changed.then_some(()))
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
                    // (a bucket never sighted through the API has none). It
                    // is a bucket-row write, so a stale Object Lock
                    // configuration is cleared beside it (OL4).
                    table.put_full(
                        &row,
                        &BucketRow {
                            acl: grants_wire,
                            ..BucketRow::at(now)
                        },
                    )?;
                    // OL4: drop the bucket-table handle before opening the
                    // configuration table in the same transaction (redb
                    // hands one transaction's tables out one at a time).
                    drop(table);
                    ObjectLockTable::open(txn)?.drain_bucket(&row)?;
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

    async fn lifecycle(&self, bucket: &bucket::Name) -> Result<Option<lifecycle::Config>, Error> {
        let bucket = bucket.to_string();
        self.driver
            .read(move |txn| LifecycleTable::open_readonly(txn)?.get(&bucket))
    }

    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
        // A whole-table walk: the decode and the allocation run on the
        // blocking pool (the driver's own rule for traversals).
        self.driver
            .read_blocking(|txn| {
                let table = LifecycleTable::open_readonly(txn)?;
                let mut out = Vec::new();
                table.for_each(|bucket, config| {
                    match bucket::name(bucket.to_owned()) {
                        Ok(name) => out.push((name, config)),
                        Err(err) => tracing::warn!(
                            %err,
                            bucket,
                            "discarding a lifecycle row under an invalid bucket name"
                        ),
                    }
                    Ok(())
                })?;
                Ok(out)
            })
            .await
    }

    async fn set_lifecycle(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<bool, Error> {
        let row = bucket.to_string();
        let config = config.clone();
        // The bucket row is the presence decision, taken inside the writing
        // transaction: a missing bucket writes nothing and answers `false`.
        set_if(self, bucket, move |txn| {
            let present = BucketTable::open(txn)?.exists(&row)?;
            if !present {
                return Ok(None);
            }
            LifecycleTable::open(txn)?.set(&row, &config)?;
            Ok(Some(()))
        })
        .await
    }

    async fn merge_lifecycle(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        let row = bucket.to_string();
        let rules = rules.to_vec();
        // One transaction: the row this reads is the row the upsert writes,
        // so a configuration stored between the two cannot be overwritten by
        // the snapshot (LC13).
        self.driver
            .write(move |txn| merge_row(txn, &row, &rules, None))
            .await
    }

    async fn merge_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
        now: SystemTime,
    ) -> Result<lifecycle::Merged, Error> {
        let row = bucket.to_string();
        let rules = rules.to_vec();
        self.driver
            .write(move |txn| merge_row(txn, &row, &rules, Some(now)))
            .await
    }

    async fn set_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
        now: SystemTime,
    ) -> Result<(), Error> {
        let row = bucket.to_string();
        let config = config.clone();
        // One transaction: a first-sight bucket row is recorded WITH the
        // configuration, so no reader sees a bare row, and an existing row's
        // elements ride untouched.
        self.driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                let present = {
                    let mut table = BucketTable::open(txn)?;
                    let present = table.exists(&row)?;
                    if !present {
                        table.put_full(&row, &BucketRow::at(now))?;
                    }
                    present
                };
                if !present {
                    // OL4: the first-sight insert is a bucket-row write, so
                    // a stale Object Lock configuration is cleared in the
                    // same transaction.
                    ObjectLockTable::open(txn)?.drain_bucket(&row)?;
                }
                LifecycleTable::open(txn)?.set(&row, &config)?;
                Ok(Some(()))
            })
            .await
            .map(|_| ())
    }

    async fn clear_lifecycle(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let row = bucket.to_string();
        set_if(self, bucket, move |txn| {
            let present = BucketTable::open(txn)?.exists(&row)?;
            if !present {
                return Ok(None);
            }
            LifecycleTable::open(txn)?.drain_bucket(&row)?;
            Ok(Some(()))
        })
        .await
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| {
                BucketTable::open(txn)?.remove(&bucket)?;
                // LC19: the rule dies with the bucket, in the same write.
                LifecycleTable::open(txn)?.drain_bucket(&bucket)?;
                // OL4: the Object Lock configuration dies with it too.
                ObjectLockTable::open(txn)?.drain_bucket(&bucket)
            })
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
                // The occupancy probes in the deleting transaction:
                // one key-range seek each (never a walk), so a concurrent
                // write cannot slip between the check and the removal. The
                // object-row half is the table layer's own probe — the same
                // one `ObjectMeta::has_objects` answers from. OL8: a
                // bucket's emptiness decision includes its version rows and
                // its lock rows, so a teardown cannot strand either.
                if bucket_occupied(txn, &key)? {
                    return Err(Error::Refused(storage::not_empty(&name)));
                }
                BucketTable::open(txn)?.remove(&key)?;
                // LC19: the rule dies with the bucket, in the same write.
                LifecycleTable::open(txn)?.drain_bucket(&key)?;
                // OL4: the Object Lock configuration leaves with it.
                ObjectLockTable::open(txn)?.drain_bucket(&key)
            })
            .await
    }

    async fn object_lock(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, Error> {
        let name = bucket.clone();
        let bucket = bucket.to_string();
        self.driver.read(move |txn| {
            // The lifecycle read's presence rule: a missing bucket row is
            // NoSuchBucket, never "no configuration" (OL4).
            if !BucketTable::open_readonly(txn)?.exists(&bucket)? {
                return Err(Error::Refused(storage::no_such_bucket(&name)));
            }
            ObjectLockTable::open_readonly(txn)?.get(&bucket)
        })
    }

    async fn set_object_lock(
        &self,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<bool, Error> {
        let name = bucket.clone();
        let bucket = bucket.to_string();
        let config = config.clone();
        // The bucket row is the presence decision, taken inside the writing
        // transaction: a missing bucket writes nothing and answers `false`
        // (the `set_lifecycle` rule).
        self.driver
            .write(move |txn| -> Result<bool, Error> {
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Ok(false);
                }
                // OL13: a configuration is only valid on an `Enabled` bucket.
                // The check runs in the transaction that writes it, so a
                // concurrent suspend — which takes the same write lock and
                // reads the configuration — cannot interleave between the
                // caller's read and this write and leave an enabled
                // configuration on a `Suspended` bucket.
                let state = VersioningTable::open(txn)?
                    .get(&bucket)?
                    .unwrap_or_default();
                if state != State::Enabled {
                    return Err(Error::Refused(storage::invalid_bucket_state(&name)));
                }
                ObjectLockTable::open(txn)?.set(&bucket, &config)?;
                Ok(true)
            })
            .await
    }

    async fn clear_object_lock(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let bucket = bucket.to_string();
        // Same presence outcome as `clear_lifecycle`: a bucket that stores no
        // configuration still answers `Ok(true)`.
        self.driver
            .write(move |txn| -> Result<bool, Error> {
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Ok(false);
                }
                ObjectLockTable::open(txn)?.drain_bucket(&bucket)?;
                Ok(true)
            })
            .await
    }
}

#[cfg(test)]
mod tests;
