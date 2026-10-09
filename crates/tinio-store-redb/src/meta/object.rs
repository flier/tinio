//! The `ObjectMeta` home over [`crate::RedbMetadata`] — Task 6's implementation.
//!
//! One read or write transaction per method, over the `object_meta` row
//! ([`crate::object_meta`]) and the retained `object_parts` list
//! ([`crate::object_part`]). Three rules the file exists to carry:
//!
//! * **The version columns stay in the table layer.** A stored row is the
//!   eleven-element tuple; the contract's [`Row`] carries the content-derived and
//!   recorded elements only, because version identity is version-domain state
//!   (D37) and rides the version row plus the key's current slot. The writers
//!   here therefore re-read the row they replace **inside their own
//!   transaction** and carry its `version_id` and `version_seq` over — the
//!   object-row writers preserve the stored version columns, and only the
//!   version domain's slot advance and the delete path change them — while the
//!   recorded delete-marker flag is cleared by every object write, the rule the
//!   row format documents.
//! * **A write is a fresh object.** [`ObjectMeta::put`] and
//!   [`ObjectMeta::put_batch`] clear the key's retained part list in the same
//!   transaction (review item 5); [`ObjectMeta::put_preserving`] is the read-path
//!   heal and leaves it standing, because it refreshes an object rather than
//!   replacing it.
//! * **The self-heal asymmetry, both halves.** Point reads and
//!   [`ObjectMeta::walk_healing`] treat a domain-invalid row as absent (never a
//!   failure); [`ObjectMeta::walk`] fails on it, so the reclamation and doctor
//!   callers cannot skip one.
//!
//! The strict traversal itself lives here as [`for_bucket_strict`]: review round
//! 2, item 28 relocated it, and `crates/tinio-fs/src/database/tables.rs` with
//! it, from tinio-fs, so the engine and the fs facade run one rule. The error
//! is the caller's, because the shared error has no corrupt-row variant and
//! tinio-fs keeps its own `CorruptMeta`; that is why the helper takes the
//! constructor: [`ObjectMeta::walk`] passes the shared corruption arm, and the fs
//! facade passes `corrupt_meta`.

use std::ops::Deref;

use async_trait::async_trait;
use redb::{ReadableTable, WriteTransaction};

use crate::{
    _core::{
        acl, bucket,
        etag::{self, ETag},
        meta::{
            Metadata, ObjectMeta,
            object::{Identity, PartRow, Preserved, Row},
        },
        object, storage,
    },
    Error, RedbMetadata,
    bucket::Table as BucketTable,
    object_meta::{self, Table as MetaTable},
    object_part::{self, Table as PartTable},
    scan::for_each_pair,
    table::{self, TableDef},
};

/// The contract row of one stored row — the three version-domain columns
/// (`version_id`, `version_seq`, `is_delete_marker`) stay in the table layer by
/// design (D37).
fn contract_row(stored: &object_meta::Stored) -> Row {
    Row {
        etag: stored.etag.clone(),
        size: stored.size,
        mtime: storage::from_nanos(stored.mtime),
        identity: stored.file_identity,
        tags: stored.tags.clone(),
        checksum: stored.checksum.clone(),
        owner: stored.owner.clone(),
        acl: stored.acl.clone(),
    }
}

/// The table-layer row a contract write stores: the row's elements plus the
/// version columns of the row it replaces (`None` — a first write — contributes
/// no version identity and sequence `0`) and the delete-marker flag cleared:
/// every object write clears it.
fn stored_row(row: &Row, previous: Option<&object_meta::Stored>) -> object_meta::Stored {
    object_meta::Stored {
        etag: row.etag.clone(),
        size: row.size,
        mtime: storage::to_nanos(row.mtime),
        file_identity: row.identity,
        tags: row.tags.clone(),
        checksum: row.checksum.clone(),
        owner: row.owner.clone(),
        acl: row.acl.clone(),
        version_id: previous.and_then(|stored| stored.version_id.clone()),
        version_seq: previous.map_or(0, |stored| stored.version_seq),
        is_delete_marker: false,
    }
}

/// Write one object row into an **already-open** `object_meta` table — the
/// write half every object writer shares: read the row being replaced
/// **inside the writer's own transaction** (never a stale pre-read) and store
/// [`stored_row`]'s transformation of it, so the stored version columns ride
/// the write and the delete-marker flag is cleared. A write is a fresh object,
/// so each caller also clears the key's retained part list beside this call;
/// that step is its own, because the four callers differ there (a replacement
/// list, a loop of keys, or the bare removal). [`write_row`] opens the table
/// and calls this for the three single-row writers; [`ObjectMeta::put_batch`]
/// opens once and calls this per entry, so a batch pays one open rather than
/// one per entry.
fn write_row_into(
    table: &mut MetaTable<'_>,
    bucket: &str,
    key: &str,
    row: &Row,
) -> Result<(), Error> {
    let previous = table.get(bucket, key)?;
    table.put(bucket, key, &stored_row(row, previous.as_ref()))
}

/// Write one object row through its own table handle: open `object_meta`, then
/// [`write_row_into`]. [`ObjectMeta::put`], [`ObjectMeta::put_guarded`] and
/// [`ObjectMeta::put_with_parts`] each pay that one open per call, which is
/// correct and cheap for them.
fn write_row(txn: &mut WriteTransaction, bucket: &str, key: &str, row: &Row) -> Result<(), Error> {
    write_row_into(&mut MetaTable::open(txn)?, bucket, key, row)
}

/// The recorded elements a content-derived rewrite carries over — the
/// contract's own absent-row value ([`Preserved::none`]) when there is no
/// previous row.
fn preserved_of(previous: Option<&object_meta::Stored>) -> Preserved {
    previous.map_or_else(Preserved::none, |stored| Preserved {
        tags: stored.tags.clone(),
        checksum: stored.checksum.clone(),
        owner: stored.owner.clone(),
        acl: stored.acl.clone(),
    })
}

/// The retained part row of one contract part.
fn part_row(part: &PartRow) -> object_part::Stored {
    object_part::Stored {
        part_number: part.number,
        size: part.size,
        checksum: part.checksum.clone(),
    }
}

/// The failure behind a row [`object_meta::validate`] rejected: the etag is the
/// one element that decides the whole row, so the failure is that element's own
/// parse failure — the only `?` in `validate`'s body.
///
/// A rejected row's etag never parses, so the `Ok` arm answers the canonical
/// malformed-etag failure ([`etag::Error::InvalidFormat`], the same
/// [`storage::Error`] variant the failing arm carries) rather than panicking on
/// a row that cannot be served either way (style.md §Panics).
fn invalid_etag(etag: &str) -> storage::Error {
    match ETag::new(etag) {
        Ok(_) => etag::Error::InvalidFormat.into(),
        Err(rejected) => rejected,
    }
}

/// The strict traversal's corrupt-row outcome: the row named, the validation
/// failure carried. It is its own variant so a caller can tell a rejected row
/// from a redb failure (tinio-fs maps it onto its fs-local `CorruptMeta`).
fn corrupt_row(key: &str, source: storage::Error) -> Error {
    Error::CorruptRow {
        key: key.to_owned(),
        source,
    }
}

/// Strict per-bucket traversal: every row of `bucket` decoded, in ascending key
/// order; a domain-invalid key or content identity fails through `corrupt` — the
/// caller's error ([`ObjectMeta::walk`] passes the shared corruption arm, the fs
/// facade passes its own `CorruptMeta`).
///
/// Relocated here from `crates/tinio-fs/src/database/tables.rs` (review round 2,
/// item 28): the engine's own [`ObjectMeta::walk`] and the fs facade's orphan
/// scan run this one rule. Validation happens **before** the visit — a corrupt
/// delete-marker row fails too — and the delete-marker filter stays at the call
/// site that needs it, because the object-file orphan passes must never see a
/// marker's row while the contract's own traversals yield every row.
///
/// # Errors
///
/// Fails with the error `corrupt` builds for the first domain-invalid key or row
/// of the bucket; a range-read failure lifts through the caller's conversion
/// from [`Error`].
pub fn for_bucket_strict<T, C, F, E>(
    table: &table::Table<'_, object_meta::Def, T>,
    bucket: &str,
    corrupt: C,
    mut visit: F,
) -> Result<(), E>
where
    T: ReadableTable<<object_meta::Def as TableDef>::Key, <object_meta::Def as TableDef>::Value>,
    C: Fn(&str, storage::Error) -> E,
    F: FnMut(object::Key, object_meta::Stored) -> Result<(), E>,
    E: From<Error>,
{
    for_each_pair::<_, _, _, E>(
        table.deref(),
        (bucket, ""),
        |name, _| name == bucket,
        |_, raw_key, value| {
            let key = object::key(raw_key).map_err(|err| corrupt(raw_key, err))?;
            let Some(stored) = object_meta::validate(value) else {
                return Err(corrupt(raw_key, invalid_etag(value.0)));
            };
            visit(key, stored)
        },
    )
}

/// Whether the key has a row — the tie-break an existence-conditioned setter
/// reads after a no-op write ([`ObjectMeta::set_tags`], [`ObjectMeta::clear_tags`],
/// [`ObjectMeta::set_acl`]). The write itself already decided inside its
/// transaction; this read only separates "present and already said this" from
/// "no row".
async fn present(
    store: &RedbMetadata,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<bool, Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();
    store
        .driver
        .read(move |txn| Ok(MetaTable::open_readonly(txn)?.get(&bucket, &key)?.is_some()))
}

#[async_trait]
impl ObjectMeta for RedbMetadata {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<Row>, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            Ok(MetaTable::open_readonly(txn)?
                .get(&bucket, &key)?
                .as_ref()
                .map(contract_row))
        })
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let row = row.clone();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                write_row(txn, &bucket, &key, &row)?;
                PartTable::open(txn)?.remove_key(&bucket, &key)
            })
            .await
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
    ) -> Result<(), <Self as Metadata>::Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let row = row.clone();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                write_row(txn, &bucket, &key, &row)?;
                PartTable::open(txn)?.remove_key(&bucket, &key)
            })
            .await
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &Row,
        parts: &[PartRow],
    ) -> Result<(), <Self as Metadata>::Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let row = row.clone();
        let parts: Vec<object_part::Stored> = parts.iter().map(part_row).collect();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                // The parts write is a guarded write too: the bucket row is
                // probed in this transaction, first, so a concurrent
                // `remove_if_empty` cannot be raced into leaving object and
                // part rows behind (the shape `put_guarded` rules out).
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                write_row(txn, &bucket, &key, &row)?;
                let mut parts_table = PartTable::open(txn)?;
                parts_table.remove_key(&bucket, &key)?;
                for part in &parts {
                    parts_table.put(&bucket, &key, part)?;
                }
                Ok(())
            })
            .await
    }

    async fn parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Vec<PartRow>, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        self.driver.read(move |txn| {
            Ok(PartTable::open_readonly(txn)?
                .list(&bucket, &key)?
                .into_iter()
                .map(|stored| PartRow {
                    number: stored.part_number,
                    size: stored.size,
                    checksum: stored.checksum,
                })
                .collect())
        })
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &Identity,
    ) -> Result<Preserved, <Self as Metadata>::Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let key = key.to_string();
        let identity = identity.clone();
        self.driver
            .write(move |txn| -> Result<Preserved, Error> {
                // The preserving write is a guarded write too: the bucket row
                // is probed in this transaction, first, exactly as
                // `put_guarded` probes it.
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                let mut table = MetaTable::open(txn)?;
                let previous = table.get(&bucket, &key)?;
                let preserved = preserved_of(previous.as_ref());
                let version_id = previous
                    .as_ref()
                    .and_then(|stored| stored.version_id.clone());
                let version_seq = previous.as_ref().map_or(0, |stored| stored.version_seq);
                table.put(
                    &bucket,
                    &key,
                    &object_meta::Stored {
                        etag: identity.etag.clone(),
                        size: identity.size,
                        mtime: storage::to_nanos(identity.mtime),
                        file_identity: identity.file_identity,
                        tags: preserved.tags.clone(),
                        checksum: preserved.checksum.clone(),
                        owner: preserved.owner.clone(),
                        acl: preserved.acl.clone(),
                        version_id,
                        version_seq,
                        is_delete_marker: false,
                    },
                )?;
                Ok(preserved)
            })
            .await
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), <Self as Metadata>::Error> {
        if from == to {
            return Ok(());
        }
        let bucket = bucket.to_string();
        let from = from.to_string();
        let to = to.to_string();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                let stored = MetaTable::open(txn)?.get(&bucket, &from)?;
                let Some(stored) = stored else {
                    return Ok(());
                };
                let parts = PartTable::open(txn)?.list(&bucket, &from)?;
                {
                    // Every removal precedes every insert into `object_parts`:
                    // the other order frees a page of the table and then writes
                    // it again in the same transaction, tripping redb 4.2.0's
                    // page-manager read-ref assertion (the rule and the panic
                    // shape are recorded on `Driver::write`). `put_with_parts`
                    // runs the same remove-then-insert order.
                    let mut parts_table = PartTable::open(txn)?;
                    parts_table.remove_key(&bucket, &to)?;
                    parts_table.remove_key(&bucket, &from)?;
                    for part in &parts {
                        parts_table.put(&bucket, &to, part)?;
                    }
                }
                // `object_meta` follows the same remove-then-insert order as the
                // parts table above; `from` and `to` differ (the equal case
                // returned early) and `from` exists (it was read above).
                let mut table = MetaTable::open(txn)?;
                table.remove(&bucket, &from)?;
                table.put(&bucket, &to, &stored)
            })
            .await
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, <Self as Metadata>::Error> {
        let bucket_key = bucket.to_string();
        let key_name = key.to_string();
        let tags = tags.clone();
        let changed = self
            .driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                Ok(MetaTable::open(txn)?
                    .put_tags(&bucket_key, &key_name, &tags)?
                    .and_then(|changed| changed.then_some(())))
            })
            .await?
            .is_some();
        if changed {
            return Ok(true);
        }
        present(self, bucket, key).await
    }

    async fn clear_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        let bucket_key = bucket.to_string();
        let key_name = key.to_string();
        let cleared = self
            .driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                Ok(MetaTable::open(txn)?
                    .clear_tags(&bucket_key, &key_name)?
                    .and_then(|cleared| cleared.then_some(())))
            })
            .await?
            .is_some();
        if cleared {
            return Ok(true);
        }
        present(self, bucket, key).await
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let tags = tags.clone();
        self.driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                Ok(MetaTable::open(txn)?
                    .put_tags(&bucket, &key, &tags)?
                    .filter(|changed| *changed)
                    .map(|_| ()))
            })
            .await
            .map(|_| ())
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&acl::OwnerId>,
        grants: &acl::AclGrants,
    ) -> Result<bool, <Self as Metadata>::Error> {
        let bucket_key = bucket.to_string();
        let key_name = key.to_string();
        let owner = owner.cloned();
        let grants = grants.clone();
        let recorded = self
            .driver
            .write_if(move |txn| -> Result<Option<()>, Error> {
                let mut table = MetaTable::open(txn)?;
                let Some(mut stored) = table.get(&bucket_key, &key_name)? else {
                    return Ok(None);
                };
                if stored.owner.as_ref() == owner.as_ref() && stored.acl.grants == *grants {
                    return Ok(None);
                }
                stored.owner = owner.clone();
                stored.acl = acl::Acl {
                    owner: None,
                    grants: grants.clone(),
                };
                table.put(&bucket_key, &key_name, &stored)?;
                Ok(Some(()))
            })
            .await?;
        if recorded.is_some() {
            return Ok(true);
        }
        present(self, bucket, key).await
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, Row)],
    ) -> Result<(), <Self as Metadata>::Error> {
        let refusal = storage::no_such_bucket(bucket);
        let bucket = bucket.to_string();
        let rows: Vec<(String, Row)> = rows
            .iter()
            .map(|(key, row)| (key.to_string(), row.clone()))
            .collect();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                // The batch is a guarded write too: the bucket row is probed in
                // this transaction, first, exactly as `put_guarded` probes it.
                if !BucketTable::open(txn)?.exists(&bucket)? {
                    return Err(Error::Refused(refusal));
                }
                // One `object_meta` handle for the whole batch: the row writes
                // open the table once here rather than once per entry, and the
                // handle is dropped before the parts table opens below.
                {
                    let mut table = MetaTable::open(txn)?;
                    for (key, row) in &rows {
                        write_row_into(&mut table, &bucket, key, row)?;
                    }
                }
                let mut parts = PartTable::open(txn)?;
                for (key, _) in &rows {
                    parts.remove_key(&bucket, key)?;
                }
                Ok(())
            })
            .await
    }

    async fn remove(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<bool, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let removed = self
            .driver
            .write_if(move |txn| -> Result<Option<bool>, Error> {
                let present = {
                    let table = MetaTable::open(txn)?;
                    table.get(&bucket, &key)?.is_some()
                };
                if !present {
                    return Ok(None);
                }
                MetaTable::open(txn)?.remove(&bucket, &key)?;
                PartTable::open(txn)?.remove_key(&bucket, &key)?;
                Ok(Some(true))
            })
            .await?;
        Ok(removed.unwrap_or(false))
    }

    async fn remove_many(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<(), <Self as Metadata>::Error> {
        if keys.is_empty() {
            return Ok(());
        }
        let bucket = bucket.to_string();
        let keys: Vec<String> = keys.iter().map(ToString::to_string).collect();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                {
                    let mut table = MetaTable::open(txn)?;
                    for key in &keys {
                        table.remove(&bucket, key)?;
                    }
                }
                let mut parts = PartTable::open(txn)?;
                for key in &keys {
                    parts.remove_key(&bucket, key)?;
                }
                Ok(())
            })
            .await
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<Row>>, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        let keys: Vec<String> = keys.iter().map(ToString::to_string).collect();
        self.driver.read(move |txn| {
            let table = MetaTable::open_readonly(txn)?;
            keys.iter()
                .map(|key| Ok(table.get(&bucket, key)?.as_ref().map(contract_row)))
                .collect()
        })
    }

    async fn walk(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Row)>, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        // A whole-bucket walk: the decode and the allocation run on the
        // blocking pool (the driver's own rule for traversals).
        self.driver
            .read_blocking(move |txn| {
                let table = MetaTable::open_readonly(txn)?;
                let mut walked = Vec::new();
                for_bucket_strict::<_, _, _, Error>(
                    &table,
                    &bucket,
                    corrupt_row,
                    |key, stored| {
                        walked.push((key, contract_row(&stored)));
                        Ok(())
                    },
                )?;
                Ok(walked)
            })
            .await
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<Row>)>, <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        // A whole-bucket walk: the decode and the allocation run on the
        // blocking pool (the driver's own rule for traversals).
        self.driver
            .read_blocking(move |txn| {
                let mut walked = Vec::new();
                MetaTable::open_readonly(txn)?.for_bucket_gated(&bucket, |key, stored| {
                    walked.push((key, stored.as_ref().map(contract_row)));
                    Ok(())
                })?;
                Ok(walked)
            })
            .await
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), <Self as Metadata>::Error> {
        let bucket = bucket.to_string();
        self.driver
            .write(move |txn| -> Result<(), Error> {
                MetaTable::open(txn)?.drain_bucket(&bucket)?;
                PartTable::open(txn)?.drain_bucket(&bucket)
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use tinio_util::testing::metadata::assert_object_meta_conformance;

    use super::*;
    use crate::_core::{
        meta::{BucketMeta, bucket as bucket_row},
        multipart::ObjectPart,
    };

    const ETAG: &str = "d41d8cd98f00b204e9800998ecf8427e";
    const VERSION_WIRE: &str = "00000000-0000-0000-0000-000000000000";

    fn bucket_name(value: &str) -> bucket::Name {
        bucket::name(value).unwrap()
    }

    fn key(value: &str) -> object::Key {
        object::key(value).unwrap()
    }

    fn row(size: u64) -> Row {
        Row {
            etag: ETag::new(ETAG).unwrap(),
            size,
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(3),
            identity: 7,
            tags: object::Tags::empty(),
            checksum: None,
            owner: None,
            acl: acl::Acl::default_private(None),
        }
    }

    fn identity(size: u64) -> Identity {
        Identity {
            etag: ETag::new(ETAG).unwrap(),
            size,
            mtime: SystemTime::UNIX_EPOCH + Duration::from_secs(11),
            file_identity: 9,
        }
    }

    fn part(number: u32) -> PartRow {
        PartRow {
            number,
            size: 5,
            checksum: None,
        }
    }

    async fn bucket_with(store: &RedbMetadata, value: &str) -> bucket::Name {
        let bucket = bucket_name(value);
        BucketMeta::create(store, &bucket, &bucket_row::Row::at(SystemTime::UNIX_EPOCH))
            .await
            .unwrap();
        bucket
    }

    async fn plant_raw(
        store: &RedbMetadata,
        bucket: &str,
        key: &str,
        etag: &str,
        seq: u64,
        marker: bool,
    ) {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let etag = etag.to_owned();
        store
            .driver()
            .write(move |txn| -> Result<(), Error> {
                MetaTable::open(txn)?.insert(
                    (bucket.as_str(), key.as_str()),
                    (
                        etag.as_str(),
                        5,
                        0,
                        1,
                        "",
                        "",
                        "",
                        "",
                        VERSION_WIRE,
                        seq,
                        marker,
                    ),
                )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    fn raw_columns(store: &RedbMetadata, bucket: &str, key: &str) -> (String, u64, bool) {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        store
            .driver()
            .read(move |txn| -> Result<(String, u64, bool), Error> {
                let table = MetaTable::open_readonly(txn)?;
                let guard = table
                    .deref()
                    .get((bucket.as_str(), key.as_str()))?
                    .expect("the raw row exists");
                let (_, _, _, _, _, _, _, _, version_id, version_seq, marker) = guard.value();
                Ok((version_id.to_owned(), version_seq, marker))
            })
            .unwrap()
    }

    /// The binding leg: every `ObjectMeta` method against the shared suite.
    #[tokio::test]
    async fn object_meta_conforms() {
        let store = RedbMetadata::in_memory().unwrap();
        assert_object_meta_conformance(&store).await;
    }

    #[tokio::test]
    async fn a_corrupt_row_fails_the_strict_walk_and_is_the_healing_walks_decision() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-corrupt").await;
        store.put(&bucket, &key("good.txt"), &row(3)).await.unwrap();
        plant_raw(&store, "conf-corrupt", "bad.txt", "not-an-etag", 0, false).await;

        let err = store.walk(&bucket).await.unwrap_err();
        assert!(
            matches!(&err, Error::CorruptRow { key, .. } if key == "bad.txt"),
            "the strict walk reports the rejected row as itself, got {err:?}"
        );
        assert!(
            !matches!(err, Error::Storage(_)),
            "a corrupt row is not a storage failure"
        );

        let healed = store.walk_healing(&bucket).await.unwrap();
        assert_eq!(healed.len(), 2, "one slot per row");
        assert_eq!(&*healed[0].0, "bad.txt");
        assert!(healed[0].1.is_none(), "a corrupt row is the heal decision");
        assert_eq!(&*healed[1].0, "good.txt");
        assert_eq!(healed[1].1.as_ref().map(|row| row.size), Some(3));

        assert!(store.get(&bucket, &key("bad.txt")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_delete_marker_row_is_walked_and_an_object_write_clears_it() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-marker").await;
        plant_raw(&store, "conf-marker", "gone.txt", ETAG, 0, true).await;

        assert_eq!(store.walk(&bucket).await.unwrap().len(), 1);
        assert!(store.walk_healing(&bucket).await.unwrap()[0].1.is_some());
        assert!(raw_columns(&store, "conf-marker", "gone.txt").2);

        store.put(&bucket, &key("gone.txt"), &row(1)).await.unwrap();
        assert!(!raw_columns(&store, "conf-marker", "gone.txt").2);
    }

    #[tokio::test]
    async fn put_guarded_refuses_a_missing_bucket_and_projects_the_refusal() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_name("conf-guarded");
        let key = key("a.txt");

        let refusal: storage::Error = store
            .put_guarded(&bucket, &key, &row(1))
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refusal, storage::Error::NoSuchBucket(_)),
            "{refusal:?}"
        );
        assert!(store.get(&bucket, &key).await.unwrap().is_none());

        bucket_with(&store, "conf-guarded").await;
        store.put_guarded(&bucket, &key, &row(1)).await.unwrap();
        assert_eq!(
            store.get(&bucket, &key).await.unwrap().map(|row| row.size),
            Some(1)
        );
    }

    /// The parts write probes the bucket row in its own transaction, exactly as
    /// `put_guarded` does: a concurrent `remove_if_empty` cannot be raced into
    /// leaving object and part rows behind for a bucket that is gone (permanent
    /// capacity loss — the shape `concurrent_put_and_delete_never_orphans`
    /// rules out).
    #[tokio::test]
    async fn put_with_parts_refuses_a_bucket_with_no_row() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_name("conf-parts-refusal");
        let key = key("a.txt");

        let refusal: storage::Error = store
            .put_with_parts(&bucket, &key, &row(1), &[part(1), part(2)])
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refusal, storage::Error::NoSuchBucket(_)),
            "{refusal:?}"
        );
        assert!(
            store.get(&bucket, &key).await.unwrap().is_none(),
            "a refused parts write leaves no object row"
        );
        assert!(
            store.parts(&bucket, &key).await.unwrap().is_empty(),
            "and no part rows"
        );

        bucket_with(&store, "conf-parts-refusal").await;
        store
            .put_with_parts(&bucket, &key, &row(1), &[part(1), part(2)])
            .await
            .unwrap();
        assert_eq!(
            store.get(&bucket, &key).await.unwrap().map(|row| row.size),
            Some(1)
        );
        assert_eq!(store.parts(&bucket, &key).await.unwrap().len(), 2);
    }

    /// The batch write probes the bucket row as well: `put_batch` is the other
    /// writer that lands object rows for a bucket that may already be gone (the
    /// multipart completion's tail), so it must not outlive its bucket either.
    #[tokio::test]
    async fn put_batch_refuses_a_bucket_with_no_row() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_name("conf-batch-refusal");
        let key = key("a.txt");

        let refusal: storage::Error = store
            .put_batch(&bucket, &[(key.clone(), row(1))])
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refusal, storage::Error::NoSuchBucket(_)),
            "{refusal:?}"
        );
        assert!(
            store.get(&bucket, &key).await.unwrap().is_none(),
            "a refused batch leaves no object row"
        );

        bucket_with(&store, "conf-batch-refusal").await;
        store
            .put_batch(&bucket, &[(key.clone(), row(1))])
            .await
            .unwrap();
        assert_eq!(
            store.get(&bucket, &key).await.unwrap().map(|row| row.size),
            Some(1)
        );
    }

    #[tokio::test]
    async fn a_write_preserves_the_stored_version_columns_and_clears_the_parts() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-write").await;
        let key = key("versioned.txt");

        store
            .put_with_parts(&bucket, &key, &row(1), &[part(1), part(2)])
            .await
            .unwrap();
        assert_eq!(store.parts(&bucket, &key).await.unwrap().len(), 2);
        plant_raw(&store, "conf-write", "versioned.txt", ETAG, 7, false).await;

        store.put(&bucket, &key, &row(2)).await.unwrap();
        let (version_id, version_seq, marker) = raw_columns(&store, "conf-write", "versioned.txt");
        assert!(
            !version_id.is_empty(),
            "the stored version identity rides the write"
        );
        assert_eq!(
            version_seq, 7,
            "the object-row writers preserve the sequence"
        );
        assert!(!marker, "an object write clears the delete marker");
        assert!(
            store.parts(&bucket, &key).await.unwrap().is_empty(),
            "a write is a fresh object"
        );
    }

    #[tokio::test]
    async fn put_preserving_carries_the_recorded_elements_and_leaves_the_parts() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-heal").await;
        let key = key("healed.txt");
        let tags = object::Tags::from_pairs([("team".to_owned(), "red".to_owned())]).unwrap();

        store
            .put_with_parts(&bucket, &key, &row(1), &[part(1)])
            .await
            .unwrap();
        assert!(
            ObjectMeta::set_tags(&store, &bucket, &key, &tags)
                .await
                .unwrap()
        );

        let preserved = store
            .put_preserving(&bucket, &key, &identity(4))
            .await
            .unwrap();
        assert_eq!(preserved.tags, tags);
        let healed = store.get(&bucket, &key).await.unwrap().unwrap();
        assert_eq!(healed.size, 4);
        assert_eq!(healed.tags, tags, "the recorded tag set is carried over");
        assert_eq!(
            store.parts(&bucket, &key).await.unwrap().len(),
            1,
            "a heal leaves the retained list"
        );
    }

    /// The preserving write — the heal path — probes the bucket row in its own
    /// transaction, exactly as `put_guarded` does: an identity heal must not
    /// resurrect rows for a bucket that is gone.
    #[tokio::test]
    async fn put_preserving_refuses_a_bucket_with_no_row() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_name("conf-heal-refusal");
        let key = key("a.txt");

        let refusal: storage::Error = store
            .put_preserving(&bucket, &key, &identity(4))
            .await
            .unwrap_err()
            .into();
        assert!(
            matches!(refusal, storage::Error::NoSuchBucket(_)),
            "{refusal:?}"
        );
        assert!(
            store.get(&bucket, &key).await.unwrap().is_none(),
            "a refused heal leaves no row"
        );

        bucket_with(&store, "conf-heal-refusal").await;
        let preserved = store
            .put_preserving(&bucket, &key, &identity(4))
            .await
            .unwrap();
        assert_eq!(preserved.tags, object::Tags::empty());
        assert_eq!(
            store.get(&bucket, &key).await.unwrap().map(|row| row.size),
            Some(4)
        );
    }

    /// `move_` moves the row and its retained parts together. The parts-table
    /// removals run before its inserts inside the transaction (a page freed
    /// after an insert into the same table in one transaction trips redb's
    /// page-manager assertion — the shape `put_with_parts` also uses).
    #[tokio::test]
    async fn move_moves_the_row_and_its_parts_in_one_transaction() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-move").await;
        let from = key("from.txt");
        let to = key("to.txt");

        ObjectMeta::put_with_parts(&store, &bucket, &from, &row(1), &[part(1), part(2)])
            .await
            .unwrap();
        ObjectMeta::move_(&store, &bucket, &from, &to)
            .await
            .unwrap();

        assert!(
            ObjectMeta::get(&store, &bucket, &from)
                .await
                .unwrap()
                .is_none()
        );
        let moved = ObjectMeta::get(&store, &bucket, &to)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.size, 1);
        assert_eq!(
            ObjectMeta::parts(&store, &bucket, &to).await.unwrap().len(),
            2
        );
        assert!(
            ObjectMeta::parts(&store, &bucket, &from)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A stored part number outside the API range is served by `parts()` as
    /// the raw row it is (the backend stores a `u32`), and only the
    /// projection into the API's `ObjectPart` refuses it: a row a foreign
    /// writer planted must not panic the read (FIX 1's split — the row is
    /// the backend's, the projection is the consumer's).
    #[tokio::test]
    async fn parts_serves_an_out_of_range_number_and_the_projection_refuses_it() {
        let store = RedbMetadata::in_memory().unwrap();
        let bucket = bucket_with(&store, "conf-part-range").await;
        let key = key("parts.bin");
        let plant_bucket = bucket.to_string();
        let plant_key = key.to_string();
        store
            .driver()
            .write(move |txn| -> Result<(), Error> {
                PartTable::open(txn)?.put(
                    &plant_bucket,
                    &plant_key,
                    &object_part::Stored {
                        part_number: 0,
                        size: 5,
                        checksum: None,
                    },
                )
            })
            .await
            .unwrap();

        let served = ObjectMeta::parts(&store, &bucket, &key).await.unwrap();
        assert_eq!(served.len(), 1, "the raw row is served, not dropped");
        let row = served.into_iter().next().unwrap();
        assert_eq!(row.number, 0, "the stored number rides the row untouched");
        assert!(
            ObjectPart::try_from(row).is_err(),
            "the projection into the S3 shape refuses the out-of-range number"
        );
    }
}
