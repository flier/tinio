//! ObjectMeta over the object_meta and object_parts relations.
//!
//! The object row carries the contract's eight elements and no version
//! columns: version identity is version-domain state (VersionMeta's), so a
//! content rewrite that goes through this plane can never un-version an
//! object. The retained part list is the object_parts relation, written and
//! cleared only beside the row it belongs to — one transaction, or one
//! statement.
//!
//! The self-heal asymmetry lives here: a point read decodes a domain-invalid
//! ETag to absent and serves nothing, the strict walk fails on it, and the
//! healing walk reports None and deletes the row (a failed heal write is
//! logged and the read still succeeds).

use std::collections::HashMap;

use async_trait::async_trait;
use sqlx::{query_file_as, query_scalar};
use time::OffsetDateTime;

use crate::{
    _core::{
        acl::{Acl, AclGrants, OwnerId},
        bucket, checksum,
        meta::{
            ObjectMeta,
            object::{Identity, PartRow, Preserved, Row as ObjectRow},
        },
        object,
    },
    Error, PgTx, begin,
    meta::{acl_composite, assert_bucket_row, project},
    store::PgStore,
    timestamp::{from_timestamp, to_timestamp},
};

/// The seven value columns every object statement selects, in the order
/// `sql/meta/object/*.sql` spells them: the contract's four derived elements,
/// then tags, checksum and the ACL composite (which carries the owner).
///
/// The field order is the column order -- the macro builds the struct from the
/// statement's own columns -- so the decode is a typed conversion, not a
/// positional read.
struct Stored {
    etag: String,
    size: i64,
    mtime: OffsetDateTime,
    identity: i64,
    tags: object::Tags,
    checksum: String,
    acl: Acl,
}

/// The same seven columns preceded by the stored key: what the batch load and
/// the two walkers read.
struct StoredKeyed {
    key: String,
    etag: String,
    size: i64,
    mtime: OffsetDateTime,
    identity: i64,
    tags: object::Tags,
    checksum: String,
    acl: Acl,
}

/// One `object_parts` row: the part's number, size, algorithm and value.
struct StoredPart {
    number: i64,
    size: i64,
    algorithm: String,
    value: String,
}

/// An object row's checksum column: the recorded checksum's wire, or the
/// empty sentinel for none.
fn checksum_wire(row: &ObjectRow) -> String {
    row.checksum
        .as_ref()
        .map(checksum::Recorded::to_wire)
        .unwrap_or_default()
}

impl Stored {
    /// The contract row, or `None` for a domain-invalid ETag -- the point
    /// read's self-heal. The row is consumed: its tag set and ACL move into
    /// the contract row rather than being copied.
    fn decode(self) -> Option<ObjectRow> {
        object_row(
            &self.etag,
            self.size,
            self.mtime,
            self.identity,
            self.tags,
            &self.checksum,
            self.acl,
        )
    }
}

impl StoredKeyed {
    /// The stored key with the contract row it decodes to.
    fn decode(self) -> (String, Option<ObjectRow>) {
        (
            self.key,
            object_row(
                &self.etag,
                self.size,
                self.mtime,
                self.identity,
                self.tags,
                &self.checksum,
                self.acl,
            ),
        )
    }
}

/// Decode one object row from the stored columns: the four derived elements
/// and the checksum are the wire/instant values, the tags and ACL arrive as
/// the contract types. Shared with the bucket-wide version listing, whose
/// join reads the same seven columns: one definer for the decode keeps a
/// listing's elements and a walk's row the same row.
///
/// The two `bigint` elements are converted, never cast: a stored size or
/// identity outside the contract's `u64` is a row the store cannot serve a
/// length for, so it reads as absent exactly as a domain-invalid ETag does —
/// an `as` would answer `0` for 2^64 and a negative size as a length near
/// `u64::MAX`, i.e. a corrupt row served as a legitimate one.
pub(crate) fn object_row(
    etag: &str,
    size: i64,
    mtime: OffsetDateTime,
    identity: i64,
    tags: object::Tags,
    checksum: &str,
    acl: Acl,
) -> Option<ObjectRow> {
    ObjectRow::from_values(
        etag,
        u64::try_from(size).ok()?,
        from_timestamp(mtime),
        u64::try_from(identity).ok()?,
        tags,
        checksum,
        acl,
    )
}

/// One object row, upserted: every column but the key is refreshed from the
/// contract row, so an overwrite is a fresh object. Shared with the version
/// plane's restore, which writes the restored version's row the same way.
pub(crate) async fn upsert_object(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    row: &ObjectRow,
) -> Result<(), Error> {
    let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
    sqlx::query_file!(
        "sql/meta/object/upsert_object.sql",
        bucket,
        key,
        row.etag.as_str(),
        row.size as i64,
        to_timestamp(row.mtime),
        row.identity as i64,
        (&row.tags) as _,
        checksum_wire(row),
        acl as _
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Write one object row and its retained parts together: the row itself —
/// upserted from `row`, or deleted when it is `None` — then the part list that
/// replaces whatever the object held before, because an overwrite is a fresh
/// object. Answers whether the row write touched a row, so a caller reporting
/// an idempotent removal reads the delete's own count rather than re-spelling
/// the statement.
async fn write_object_row(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    key: &object::Key,
    row: Option<&ObjectRow>,
    parts: &[PartRow],
) -> Result<bool, Error> {
    let written = match row {
        Some(row) => {
            upsert_object(tx, bucket, key, row).await?;
            true
        }
        None => {
            sqlx::query_file!("sql/meta/object/delete_object_row.sql", bucket, key)
                .execute(&mut **tx)
                .await?
                .rows_affected()
                > 0
        }
    };
    sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
        .execute(&mut **tx)
        .await?;
    if !parts.is_empty() {
        let (numbers, sizes, algorithms, values) = part_arrays(parts);
        sqlx::query_file!(
            "sql/meta/object/put_with_parts.sql",
            bucket,
            key,
            &numbers,
            &sizes,
            &algorithms,
            &values
        )
        .execute(&mut **tx)
        .await?;
    }
    Ok(written)
}

/// The retained part list's element vectors, in the order the relation stores
/// them, so a list is written with one unnest statement.
///
/// A part number that appears twice keeps its **last** row: the relation is
/// keyed by `(bucket, key, number)`, so the unnest insert would abort the
/// transaction on the duplicate, while the reference store's part table
/// overwrites and answers the last write. The batch writer's last-row-wins
/// rule, resolved before the statement rather than by it.
fn part_arrays(parts: &[PartRow]) -> (Vec<i64>, Vec<i64>, Vec<String>, Vec<String>) {
    let mut last: HashMap<u32, usize> = HashMap::with_capacity(parts.len());
    for (index, part) in parts.iter().enumerate() {
        last.insert(part.number, index);
    }
    let mut numbers = Vec::with_capacity(last.len());
    let mut sizes = Vec::with_capacity(last.len());
    let mut algorithms = Vec::with_capacity(last.len());
    let mut values = Vec::with_capacity(last.len());
    for (index, part) in parts.iter().enumerate() {
        if last.get(&part.number) != Some(&index) {
            continue;
        }
        let (size, algorithm, value) = part.to_raw();
        numbers.push(i64::from(part.number));
        sizes.push(size as i64);
        algorithms.push(algorithm);
        values.push(value);
    }
    (numbers, sizes, algorithms, values)
}

#[async_trait]
impl ObjectMeta for PgStore {
    async fn get(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
    ) -> Result<Option<ObjectRow>, Error> {
        let row = query_file_as!(Stored, "sql/meta/object/get.sql", bucket, key)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.and_then(Stored::decode))
    }

    async fn put(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        write_object_row(&mut tx, bucket, key, Some(row), &[]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn put_guarded(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
    ) -> Result<(), Error> {
        // The bucket row stays locked across the upsert, so a concurrent
        // remove_if_empty cannot commit an empty-bucket delete underneath it.
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        write_object_row(&mut tx, bucket, key, Some(row), &[]).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn put_with_parts(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        row: &ObjectRow,
        parts: &[PartRow],
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        write_object_row(&mut tx, bucket, key, Some(row), parts).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Vec<PartRow>, Error> {
        let rows = query_file_as!(StoredPart, "sql/meta/object/parts.sql", bucket, key)
            .fetch_all(self.pool())
            .await?;
        let mut parts = Vec::with_capacity(rows.len());
        for row in rows {
            // The stored `bigint` elements are converted, never cast: a part
            // number the contract's `u32` cannot name (2^32 would answer part
            // 0, a number no client ever wrote) or a negative size (which `as`
            // would serve as a length near `u64::MAX`) is a row this read
            // cannot serve at all, so it refuses rather than answering a
            // legitimate-looking part.
            let (Ok(number), Ok(size)) = (u32::try_from(row.number), u64::try_from(row.size))
            else {
                return Err(Error::Corrupt {
                    bucket: bucket.clone(),
                    key: key.to_string(),
                });
            };
            parts.push(PartRow::from_raw(number, size, &row.algorithm, &row.value));
        }
        Ok(parts)
    }

    async fn put_preserving(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        identity: &Identity,
    ) -> Result<Preserved, Error> {
        let mut tx = begin(self.pool()).await?;
        // A content rewrite materializes a row, so it carries the same
        // in-transaction bucket-row assertion the other guarded writers do.
        assert_bucket_row(&mut tx, bucket).await?;
        let existing = query_file_as!(Stored, "sql/meta/object/put_preserving.sql", bucket, key)
            .fetch_optional(&mut *tx)
            .await?;
        let had_row = existing.is_some();
        // No previous row is the contract's own zero state: `Preserved::none`,
        // the one home of that value (redb's own preserving write answers it
        // too).
        let preserved = existing
            .and_then(Stored::decode)
            .map(|row| row.preserved())
            .unwrap_or_else(Preserved::none);
        if had_row {
            // Only the content-derived elements are refreshed: the recorded
            // ones (tags, checksum, owner, ACL) ride the stored row untouched.
            sqlx::query_file!(
                "sql/meta/object/put_preserving_refresh.sql",
                bucket,
                key,
                identity.etag.as_str(),
                identity.size as i64,
                to_timestamp(identity.mtime),
                identity.file_identity as i64
            )
            .execute(&mut *tx)
            .await?;
        } else {
            let row = ObjectRow {
                etag: identity.etag.clone(),
                size: identity.size,
                mtime: identity.mtime,
                identity: identity.file_identity,
                tags: preserved.tags.clone(),
                checksum: preserved.checksum.clone(),
                owner: preserved.owner.clone(),
                acl: preserved.acl.clone(),
            };
            upsert_object(&mut tx, bucket, key, &row).await?;
        }
        tx.commit().await?;
        Ok(preserved)
    }

    async fn move_(
        &self,
        bucket: &bucket::Name,
        from: &object::Key,
        to: &object::Key,
    ) -> Result<(), Error> {
        // A same-key move is a no-op: the destination delete below would
        // otherwise remove the very row and parts being moved, the write
        // would match nothing, and the call would report success having
        // destroyed the key.
        if from == to {
            return Ok(());
        }
        let mut tx = begin(self.pool()).await?;
        // `FOR UPDATE` is the row lock PG12's design pins for the move, and it
        // covers **both** keys: locking only the source leaves a concurrent
        // write to the destination free to commit between the delete below and
        // the moved row's write, which the write then answers with a raw
        // unique-violation instead of the reference store's last-writer-wins
        // overwrite. The two locks are taken in key order, so two moves in
        // opposite directions cannot deadlock on each other.
        let locked = sqlx::query_scalar!(
            r#"SELECT key FROM object_meta WHERE bucket = $1 AND key IN ($2, $3) ORDER BY key COLLATE "C" FOR UPDATE"#,
            bucket,
            from,
            to
        )
        .fetch_all(&mut *tx)
        .await?;
        let source: &str = from.as_ref();
        if !locked.iter().any(|key| key.as_str() == source) {
            // A from with no row is a no-op; the drop would roll back anyway,
            // and the explicit rollback releases the connection now.
            tx.rollback().await?;
            return Ok(());
        }
        // Each statement binds exactly the placeholders it mentions: an unused
        // parameter leaves PostgreSQL unable to infer its type.
        sqlx::query_file!("sql/meta/object/delete_object_row.sql", bucket, to)
            .execute(&mut *tx)
            .await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, to)
            .execute(&mut *tx)
            .await?;
        // The moved row and its parts are written as upserts, not as
        // `UPDATE … SET key`: a destination the lock could not cover — a key
        // with no row of its own, which `FOR UPDATE` has nothing to lock —
        // may still be materialized by a concurrent writer, and an upsert
        // settles that the way the reference store does (the writer that
        // commits last stands) rather than failing the move.
        sqlx::query_file!("sql/meta/object/move_object_row.sql", bucket, from, to)
            .execute(&mut *tx)
            .await?;
        sqlx::query_file!("sql/meta/object/delete_object_row.sql", bucket, from)
            .execute(&mut *tx)
            .await?;
        sqlx::query_file!("sql/meta/object/move_object_parts.sql", bucket, from, to)
            .execute(&mut *tx)
            .await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, from)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<bool, Error> {
        let written = sqlx::query!(
            "UPDATE object_meta SET tags = $3 WHERE bucket = $1 AND key = $2",
            bucket,
            key,
            tags as _
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn clear_tags(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, Error> {
        let written = sqlx::query!(
            "UPDATE object_meta SET tags = '{}' WHERE bucket = $1 AND key = $2",
            bucket,
            key
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn ensure_tags(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        tags: &object::Tags,
    ) -> Result<(), Error> {
        // The WHERE makes the update conditional on the stored wire actually
        // differing: an identical set rewrites no row and commits nothing.
        sqlx::query_file!("sql/meta/object/ensure_tags.sql", bucket, key, tags as _)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        key: &object::Key,
        owner: Option<&OwnerId>,
        grants: &AclGrants,
    ) -> Result<bool, Error> {
        let acl = acl_composite(owner, grants);
        let written = sqlx::query!(
            "UPDATE object_meta SET acl = $3 WHERE bucket = $1 AND key = $2",
            bucket,
            key,
            acl as _
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn put_batch(
        &self,
        bucket: &bucket::Name,
        rows: &[(object::Key, ObjectRow)],
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        // The batch is a guarded write like `put_guarded`, `put_with_parts` and
        // `put_preserving`: the reference store probes the bucket row first and
        // refuses a bucket with no row, so a batch into one must not plant
        // object rows for a bucket the store has no row for.
        assert_bucket_row(&mut tx, bucket).await?;
        let mut wires = Vec::with_capacity(rows.len());
        let mut etags = Vec::with_capacity(rows.len());
        let mut sizes = Vec::with_capacity(rows.len());
        let mut mtimes = Vec::with_capacity(rows.len());
        let mut identities = Vec::with_capacity(rows.len());
        let mut tags = Vec::with_capacity(rows.len());
        let mut checksums = Vec::with_capacity(rows.len());
        let mut acls = Vec::with_capacity(rows.len());
        // One statement cannot touch a key twice, so a key that appears twice in
        // the batch is written once — from its last row, which is the write the
        // per-row loop would have left standing.
        let mut last: HashMap<&object::Key, usize> = HashMap::with_capacity(rows.len());
        for (index, (key, _row)) in rows.iter().enumerate() {
            last.insert(key, index);
        }
        for (index, (key, row)) in rows.iter().enumerate() {
            if last.get(key) != Some(&index) {
                continue;
            }
            wires.push(key.to_string());
            etags.push(row.etag.as_str().to_owned());
            sizes.push(row.size as i64);
            mtimes.push(to_timestamp(row.mtime));
            identities.push(row.identity as i64);
            tags.push(row.tags.clone());
            checksums.push(checksum_wire(row));
            acls.push(acl_composite(row.owner.as_ref(), &row.acl.grants));
        }
        // One statement for the whole batch, the unnest shape the parts path
        // writes with: the rows land together instead of one statement apiece.
        sqlx::query_file!(
            "sql/meta/object/put_batch.sql",
            bucket,
            &wires,
            &etags,
            &sizes,
            &mtimes,
            &identities,
            &tags as _,
            &checksums,
            &acls as _
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM object_parts WHERE bucket = $1 AND key = ANY($2)",
            bucket,
            &wires
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove(&self, bucket: &bucket::Name, key: &object::Key) -> Result<bool, Error> {
        let mut tx = begin(self.pool()).await?;
        let removed = write_object_row(&mut tx, bucket, key, None, &[]).await?;
        // The key's version slot is **not** this call's. `current_slots` is the
        // version plane's own state: a slot with no object row is a recorded
        // state the contract admits (a delete marker is exactly a recorded
        // slot whose row carries no bytes), and the calls that retire one are
        // the version plane's own `advance_slot` and `restore_slot` plus the
        // bucket teardown — never a row removal (the 2026-10-13 ruling, which
        // the fs and redb stores already follow). Removing the row here
        // therefore leaves the slot recorded, and a later write to the key
        // advances from the sequence that slot names.
        tx.commit().await?;
        Ok(removed)
    }

    async fn remove_many(&self, bucket: &bucket::Name, keys: &[object::Key]) -> Result<(), Error> {
        let wires: Vec<String> = keys.iter().map(ToString::to_string).collect();
        let mut tx = begin(self.pool()).await?;
        sqlx::query!(
            "DELETE FROM object_meta WHERE bucket = $1 AND key = ANY($2)",
            bucket,
            &wires
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "DELETE FROM object_parts WHERE bucket = $1 AND key = ANY($2)",
            bucket,
            &wires
        )
        .execute(&mut *tx)
        .await?;
        // Every key's version slot stays, exactly as `remove` leaves it: the
        // slot is the version plane's own state, and a batch removal is a row
        // removal like any other (the 2026-10-13 ruling). This is the path
        // `Cleanup` uses to retire many keys at once, and the slots it leaves
        // recorded are the version plane's to retire.
        tx.commit().await?;
        Ok(())
    }

    async fn load(
        &self,
        bucket: &bucket::Name,
        keys: &[object::Key],
    ) -> Result<Vec<Option<ObjectRow>>, Error> {
        let wires: Vec<String> = keys.iter().map(ToString::to_string).collect();
        let rows = query_file_as!(StoredKeyed, "sql/meta/object/load.sql", bucket, &wires)
            .fetch_all(self.pool())
            .await?;
        let mut found: HashMap<String, ObjectRow> = HashMap::new();
        for row in rows {
            let (stored, decoded) = row.decode();
            // A row whose ETag does not decode is no answer: it is left out of
            // the map, so it reads exactly as an absent key does.
            if let Some(decoded) = decoded {
                found.insert(stored, decoded);
            }
        }
        Ok(project(keys.iter().map(|key| key.as_ref()), &mut found))
    }

    async fn walk(&self, bucket: &bucket::Name) -> Result<Vec<(object::Key, ObjectRow)>, Error> {
        let rows = query_file_as!(StoredKeyed, "sql/meta/object/walk.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        let mut walked = Vec::with_capacity(rows.len());
        for row in rows {
            let (stored, decoded) = row.decode();
            let key = object::key(&stored).map_err(|_| Error::Corrupt {
                bucket: bucket.clone(),
                key: stored.clone(),
            })?;
            let decoded = decoded.ok_or_else(|| Error::Corrupt {
                bucket: bucket.clone(),
                key: key.to_string(),
            })?;
            walked.push((key, decoded));
        }
        Ok(walked)
    }

    async fn walk_healing(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Vec<(object::Key, Option<ObjectRow>)>, Error> {
        let rows = query_file_as!(StoredKeyed, "sql/meta/object/walk.sql", bucket)
            .fetch_all(self.pool())
            .await?;
        let mut walked = Vec::with_capacity(rows.len());
        let mut corrupt = Vec::new();
        for row in rows {
            let (stored, decoded) = row.decode();
            // An unparsable key cannot be reported as a contract key. It is
            // still healed below; the traversal itself does not fail.
            let Ok(key) = object::key(&stored) else {
                tracing::warn!(
                    bucket = %bucket,
                    key = %stored,
                    "heal dropped an unparsable key"
                );
                corrupt.push(stored);
                continue;
            };
            if decoded.is_none() {
                corrupt.push(stored);
            }
            walked.push((key, decoded));
        }
        if !corrupt.is_empty() {
            // The heal write is logged and the read still succeeds, so a store
            // is never made unreadable by its own repair.
            if let Err(err) = self.heal_corrupt(bucket, &corrupt).await {
                tracing::warn!(bucket = %bucket, error = %err, "heal write failed");
            }
        }
        Ok(walked)
    }

    async fn has_objects(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        // The teardown's metadata-plane probe, the exact predicate the
        // bucket teardown's own statement carries (`teardown_bucket.sql`'s
        // `blocked.objects`): one `exists` over the bucket's rows, decoding
        // nothing — a delete marker's row, a corrupt-etag row and a
        // domain-invalid key row all count as rows.
        let present = query_scalar!(
            r#"SELECT exists(SELECT 1 FROM object_meta WHERE bucket = $1) AS "present!""#,
            bucket
        )
        .fetch_one(self.pool())
        .await?;
        Ok(present)
    }

    async fn drain_bucket(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        sqlx::query!("DELETE FROM object_meta WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        sqlx::query!("DELETE FROM object_parts WHERE bucket = $1", bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }
}

impl PgStore {
    /// Delete the corrupt rows the traversal listed — those a locked re-read
    /// still cannot decode — together with the retained part lists that belong
    /// to them, in one transaction.
    ///
    /// The re-read takes each listed key's row lock and re-decodes it, so the
    /// delete only reaches a row that is **still** corrupt: a row a concurrent
    /// writer (`put_guarded`, `put_with_parts`, `put_preserving`) committed
    /// between the traversal's read and this write decodes, and is left in
    /// place instead of being deleted from under the writer that just
    /// acknowledged it. A writer that arrives while this transaction holds the
    /// lock waits for it, and its own upsert then lands on the row this delete
    /// removed.
    ///
    /// The parts go with the row: a row-only heal leaves an `object_parts` list
    /// that no row clears again, and the next object written at the key
    /// inherits a part list that is not its own.
    async fn heal_corrupt(&self, bucket: &bucket::Name, listed: &[String]) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        let rows = query_file_as!(StoredKeyed, "sql/meta/object/lock_keys.sql", bucket, listed)
            .fetch_all(&mut *tx)
            .await?;
        let mut still: Vec<String> = Vec::with_capacity(rows.len());
        for row in rows {
            let (stored, decoded) = row.decode();
            if decoded.is_none() {
                still.push(stored);
            }
        }
        if !still.is_empty() {
            sqlx::query!(
                "DELETE FROM object_meta WHERE bucket = $1 AND key = ANY($2)",
                bucket,
                &still
            )
            .execute(&mut *tx)
            .await?;
            sqlx::query!(
                "DELETE FROM object_parts WHERE bucket = $1 AND key = ANY($2)",
                bucket,
                &still
            )
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "object_tests.rs"]
mod tests;
