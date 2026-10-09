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
use sqlx::query_file_as;
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
/// the contract types.
fn object_row(
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
        size as u64,
        from_timestamp(mtime),
        identity as u64,
        tags,
        checksum,
        acl,
    )
}

/// One object row, upserted: every column but the key is refreshed from the
/// contract row, so an overwrite is a fresh object.
async fn upsert_object(
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

/// The retained part list's element vectors, in the order the relation stores
/// them, so a list is written with one unnest statement.
fn part_arrays(parts: &[PartRow]) -> (Vec<i64>, Vec<i64>, Vec<String>, Vec<String>) {
    let mut numbers = Vec::with_capacity(parts.len());
    let mut sizes = Vec::with_capacity(parts.len());
    let mut algorithms = Vec::with_capacity(parts.len());
    let mut values = Vec::with_capacity(parts.len());
    for part in parts {
        let (size, algorithm, value) = part.to_raw();
        numbers.push(part.number as i64);
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
        upsert_object(&mut tx, bucket, key, row).await?;
        // An overwrite is a fresh object: the parts of the object it replaces
        // must not survive it, and they retire in the same transaction.
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
            .execute(&mut *tx)
            .await?;
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
        // The column list is upsert_object's: the guard is the lock, not a
        // second copy of the write.
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        upsert_object(&mut tx, bucket, key, row).await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
            .execute(&mut *tx)
            .await?;
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
        upsert_object(&mut tx, bucket, key, row).await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
            .execute(&mut *tx)
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
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn parts(&self, bucket: &bucket::Name, key: &object::Key) -> Result<Vec<PartRow>, Error> {
        let rows = query_file_as!(StoredPart, "sql/meta/object/parts.sql", bucket, key)
            .fetch_all(self.pool())
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                PartRow::from_raw(
                    row.number as u32,
                    row.size as u64,
                    &row.algorithm,
                    &row.value,
                )
            })
            .collect())
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
        // otherwise remove the very row and parts being moved, the UPDATE
        // would match nothing, and the call would report success having
        // destroyed the key.
        if from == to {
            return Ok(());
        }
        let mut tx = begin(self.pool()).await?;
        // `FOR UPDATE` is the row lock PG12's design pins for the move: the
        // destination is deleted before the source key is written, so a source
        // row removed between this read and that write would otherwise destroy
        // the destination and report success.
        let source = sqlx::query_scalar!(
            "SELECT 1 FROM object_meta WHERE bucket = $1 AND key = $2 FOR UPDATE",
            bucket,
            from
        )
        .fetch_optional(&mut *tx)
        .await?;
        if source.is_none() {
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
        sqlx::query!(
            "UPDATE object_meta SET key = $3 WHERE bucket = $1 AND key = $2",
            bucket,
            from,
            to
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "UPDATE object_parts SET key = $3 WHERE bucket = $1 AND key = $2",
            bucket,
            from,
            to
        )
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
        let mut wires = Vec::with_capacity(rows.len());
        for (key, row) in rows {
            upsert_object(&mut tx, bucket, key, row).await?;
            wires.push(key.to_string());
        }
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
        let removed = sqlx::query_file!("sql/meta/object/delete_object_row.sql", bucket, key)
            .execute(&mut *tx)
            .await?;
        sqlx::query_file!("sql/meta/object/clear_object_parts.sql", bucket, key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(removed.rows_affected() > 0)
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
            // One delete for every corrupt row. A write that fails is logged
            // and the read still succeeds, so a store is never made unreadable
            // by its own repair.
            if let Err(err) = sqlx::query!(
                "DELETE FROM object_meta WHERE bucket = $1 AND key = ANY($2)",
                bucket,
                &corrupt
            )
            .execute(self.pool())
            .await
            {
                tracing::warn!(bucket = %bucket, error = %err, "heal write failed");
            }
        }
        Ok(walked)
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

#[cfg(test)]
#[path = "object_tests.rs"]
mod tests;
