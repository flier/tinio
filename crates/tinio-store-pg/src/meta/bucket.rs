//! BucketMeta over the buckets relation.
//!
//! The stored value is the contract's row laid out as four columns: (created
//! timestamptz, tags jsonb, ACL composite, CORS wire). The ACL composite
//! carries the owner and the grant set, so the owner has no column of its own;
//! the mapping lives on tinio_core::acl::Acl itself (behind that crate's
//! `sqlx` feature) and crate::_core::meta::bucket::decode_cors owns the CORS
//! wire, so the row this plane writes is the row every backend's reads expect.

use std::{collections::HashMap, time::SystemTime};

use async_trait::async_trait;
use sqlx::{query, query_as, query_file, query_file_as, query_file_scalar};
use time::OffsetDateTime;

use crate::{
    _core::{
        acl::{Acl, AclGrants, OwnerId},
        bucket, cors, lifecycle,
        meta::{
            BucketMeta,
            bucket::{Row as BucketRow, decode_cors},
        },
        object, object_lock, storage,
        version::State,
    },
    Error, PgTx, begin,
    meta::{acl_composite, assert_bucket_row, bucket_row_present, project, version::StoredState},
    store::PgStore,
    timestamp::{from_timestamp, to_timestamp},
};

/// The `(name, created)` pair the two readers fill their lookup map from.
struct Named {
    name: String,
    created: OffsetDateTime,
}

/// The `(name, acl)` pair `owners` reads. The owner is the ACL composite's own
/// attribute, so the row carries the contract type and the owner arrives
/// already healed by the composite's decode -- there is no second heal here.
struct Owned {
    name: String,
    acl: Acl,
}

/// The four stored columns `row` hands to `BucketRow::from_values`.
struct Stored {
    created: OffsetDateTime,
    tags: object::Tags,
    acl: Acl,
    cors: String,
}

/// One row of `lock_get.sql`: the configuration's enabled bit and rule wire,
/// both NULL when the bucket stores no configuration (the left join's arm).
/// The presence of the outer row is the bucket's, so an absent bucket and a
/// bucket with no configuration cannot be confused.
struct StoredConfig {
    enabled: Option<bool>,
    rule: Option<String>,
}

/// A row's CORS column: the config's wire, or the empty sentinel for none.
fn cors_wire(cors: Option<&cors::Config>) -> String {
    cors.map(|cors| cors.to_wire()).unwrap_or_default()
}

/// Clear the bucket's Object Lock configuration inside the caller's
/// transaction (OL4): a writer that installs or replaces a bucket row clears a
/// stale configuration in the same write, so a rule never protects a bucket
/// state that write replaced. A bucket's other element writers keep it.
pub(in crate::meta) async fn clear_config(
    tx: &mut PgTx,
    bucket: &bucket::Name,
) -> Result<(), Error> {
    sqlx::query!("DELETE FROM bucket_object_lock WHERE bucket = $1", bucket)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// The shared body of a bucket-row write: insert the row (a taken name is
/// `AlreadyExists`, writing nothing) and clear any Object Lock configuration a
/// row left behind out of band would otherwise inherit (OL4). The caller's own
/// transaction carries whatever else the write implies — the versioning state
/// and the Object Lock write for `create_bucket_with_lock`.
async fn insert_bucket_row(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    row: &BucketRow,
) -> Result<(), Error> {
    let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
    let inserted = query_file!(
        "sql/meta/bucket/create.sql",
        bucket,
        to_timestamp(row.created),
        (&row.tags) as _,
        acl as _,
        cors_wire(row.cors.as_ref())
    )
    .execute(&mut **tx)
    .await?;
    if inserted.rows_affected() == 0 {
        return Err(Error::AlreadyExists(bucket.clone()));
    }
    clear_config(tx, bucket).await
}

#[async_trait]
impl BucketMeta for PgStore {
    async fn created_at(&self, bucket: &bucket::Name) -> Result<Option<SystemTime>, Error> {
        let created = query_file_scalar!("sql/meta/bucket/bucket_created_at.sql", bucket)
            .fetch_optional(self.pool())
            .await?;
        Ok(created.map(from_timestamp))
    }

    async fn load_many(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<SystemTime>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        let rows = query_as!(
            Named,
            "SELECT name, created FROM buckets WHERE name = ANY ($1)",
            &names
        )
        .fetch_all(self.pool())
        .await?;
        let mut found: HashMap<String, SystemTime> = HashMap::new();
        for row in rows {
            found.insert(row.name, from_timestamp(row.created));
        }
        Ok(project(
            buckets.iter().map(|bucket| bucket.as_ref()),
            &mut found,
        ))
    }

    async fn load_all(&self) -> Result<Vec<(String, SystemTime)>, Error> {
        let rows = query_as!(
            Named,
            "SELECT name, created FROM buckets ORDER BY name COLLATE \"C\""
        )
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.name, from_timestamp(row.created)))
            .collect())
    }

    async fn row(&self, bucket: &bucket::Name) -> Result<Option<BucketRow>, Error> {
        let row = query_as!(
            Stored,
            "SELECT created, tags AS \"tags: object::Tags\", acl AS \"acl: Acl\", cors FROM buckets WHERE name = $1",
            bucket
        )
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(|row| {
            BucketRow::from_values(
                from_timestamp(row.created),
                row.tags,
                row.acl,
                decode_cors(&row.cors),
            )
        }))
    }

    async fn owners(&self, buckets: &[bucket::Name]) -> Result<Vec<Option<OwnerId>>, Error> {
        let names: Vec<String> = buckets.iter().map(ToString::to_string).collect();
        let rows = query_as!(
            Owned,
            "SELECT name, acl AS \"acl: Acl\" FROM buckets WHERE name = ANY ($1)",
            &names
        )
        .fetch_all(self.pool())
        .await?;
        let mut found: HashMap<String, OwnerId> = HashMap::new();
        for row in rows {
            // A bucket whose owner element is absent -- or was healed away by
            // the composite's read -- answers None, exactly as a bucket with no
            // row does: the answer has one element per requested name either
            // way.
            if let Some(owner) = row.acl.owner {
                found.insert(row.name, owner);
            }
        }
        Ok(project(
            buckets.iter().map(|bucket| bucket.as_ref()),
            &mut found,
        ))
    }

    async fn record(&self, bucket: &bucket::Name, created: SystemTime) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        query_file!("sql/meta/bucket/record.sql", bucket, to_timestamp(created))
            .execute(&mut *tx)
            .await?;
        // OL4: a bucket-row writer clears the Object Lock configuration in the
        // same write; the row's other elements are cleared too, so the
        // configuration cannot describe a bucket state this write replaced.
        clear_config(&mut tx, bucket).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn create(&self, bucket: &bucket::Name, row: &BucketRow) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        insert_bucket_row(&mut tx, bucket, row).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<(SystemTime, bool), Error> {
        let mut tx = begin(self.pool()).await?;
        let result = query_file!(
            "sql/meta/bucket/record_if_absent.sql",
            bucket,
            to_timestamp(now)
        )
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() > 0 {
            // OL4: only a first-sight insert is a bucket-row write here; an
            // existing row (and the configuration beside it) rides.
            clear_config(&mut tx, bucket).await?;
        }
        let created = query_file_scalar!("sql/meta/bucket/bucket_created_at.sql", bucket)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((from_timestamp(created), result.rows_affected() > 0))
    }

    async fn record_full(&self, bucket: &bucket::Name, row: &BucketRow) -> Result<(), Error> {
        let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
        let mut tx = begin(self.pool()).await?;
        query_file!(
            "sql/meta/bucket/record_full.sql",
            bucket,
            to_timestamp(row.created),
            (&row.tags) as _,
            acl as _,
            cors_wire(row.cors.as_ref())
        )
        .execute(&mut *tx)
        .await?;
        // OL4: this is the lazy policy's re-create (a store whose buckets
        // outlive their rows adopts an existing record the same way).
        clear_config(&mut tx, bucket).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn set_tags(&self, bucket: &bucket::Name, tags: &object::Tags) -> Result<bool, Error> {
        let written = query!(
            "UPDATE buckets SET tags = $2 WHERE name = $1",
            bucket,
            tags as _
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn set_tags_or_create(
        &self,
        bucket: &bucket::Name,
        tags: &object::Tags,
        now: SystemTime,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        // OL4: the first-sight insert is a bucket-row writer and clears a
        // stale Object Lock configuration; an existing row rides. The answer is
        // the write's own effect (`xmax = 0` names a row this statement
        // inserted), never a pre-read: under Read Committed a concurrent
        // `create_bucket_with_lock` can commit its row and configuration
        // between a presence read and the insert, and the pre-read would then
        // clear a configuration that is not stale.
        let inserted = query_file_scalar!(
            "sql/meta/bucket/set_tags_or_create.sql",
            bucket,
            to_timestamp(now),
            tags as _
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        if inserted == Some(true) {
            clear_config(&mut tx, bucket).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn clear_tags(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let written = query!("UPDATE buckets SET tags = '{}' WHERE name = $1", bucket)
            .execute(self.pool())
            .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn set_acl(
        &self,
        bucket: &bucket::Name,
        owner: Option<&OwnerId>,
        grants: &AclGrants,
    ) -> Result<bool, Error> {
        let acl = acl_composite(owner, grants);
        let written = query!(
            "UPDATE buckets SET acl = $2 WHERE name = $1",
            bucket,
            acl as _
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn set_grants_or_create(
        &self,
        bucket: &bucket::Name,
        grants: &AclGrants,
        now: SystemTime,
    ) -> Result<(), Error> {
        let acl = acl_composite(None, grants);
        let mut tx = begin(self.pool()).await?;
        // OL4: the same write-effect rule `set_tags_or_create` states: only a
        // row this statement inserted is first sight, so a concurrent
        // `create_bucket_with_lock` cannot lose its configuration to a
        // presence read taken before its commit.
        let inserted = query_file_scalar!(
            "sql/meta/bucket/set_grants_or_create.sql",
            bucket,
            to_timestamp(now),
            acl as _
        )
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        if inserted == Some(true) {
            clear_config(&mut tx, bucket).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn set_cors(&self, bucket: &bucket::Name, config: &cors::Config) -> Result<bool, Error> {
        let written = query!(
            "UPDATE buckets SET cors = $2 WHERE name = $1",
            bucket,
            config.to_wire()
        )
        .execute(self.pool())
        .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn clear_cors(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let written = query!("UPDATE buckets SET cors = '' WHERE name = $1", bucket)
            .execute(self.pool())
            .await?;
        Ok(written.rows_affected() > 0)
    }

    async fn lifecycle(&self, bucket: &bucket::Name) -> Result<Option<lifecycle::Config>, Error> {
        super::lifecycle::get(self, bucket).await
    }

    async fn lifecycles(&self) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
        super::lifecycle::list(self).await
    }

    async fn set_lifecycle(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
    ) -> Result<bool, Error> {
        super::lifecycle::set(self, bucket, config).await
    }

    async fn merge_lifecycle(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
    ) -> Result<lifecycle::Merged, Error> {
        super::lifecycle::merge(self, bucket, rules).await
    }

    async fn merge_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        rules: &[lifecycle::Rule],
        now: SystemTime,
    ) -> Result<lifecycle::Merged, Error> {
        super::lifecycle::merge_or_create(self, bucket, rules, now).await
    }

    async fn set_lifecycle_or_create(
        &self,
        bucket: &bucket::Name,
        config: &lifecycle::Config,
        now: SystemTime,
    ) -> Result<(), Error> {
        super::lifecycle::set_or_create(self, bucket, config, now).await
    }

    async fn clear_lifecycle(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        super::lifecycle::clear(self, bucket).await
    }

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        query!("DELETE FROM buckets WHERE name = $1", bucket)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), Error> {
        // Lock the bucket, then delete it in the next statement. That
        // statement's snapshot includes every object and upload committed
        // while this waited. Those writers take the same lock, so they cannot
        // insert until this transaction ends. One statement cannot do this:
        // its emptiness check would keep the snapshot from before the wait.
        let mut tx = begin(self.pool()).await?;
        assert_bucket_row(&mut tx, bucket).await?;
        let removed = sqlx::query_file_scalar!("sql/meta/bucket/remove_if_empty.sql", bucket)
            .fetch_optional(&mut *tx)
            .await?;
        if removed.is_none() {
            tx.rollback().await?;
            return Err(Error::NotEmpty(bucket.clone()));
        }
        tx.commit().await?;
        Ok(())
    }

    /// The bucket's Object Lock configuration, or `None` when the bucket
    /// stores none. A missing bucket is `NoSuchBucket`, the lifecycle read's
    /// presence rule (OL4).
    async fn object_lock(
        &self,
        bucket: &bucket::Name,
    ) -> Result<Option<object_lock::Config>, Error> {
        let row = query_file_as!(StoredConfig, "sql/meta/bucket/lock_get.sql", bucket)
            .fetch_optional(self.pool())
            .await?;
        let Some(row) = row else {
            return Err(Error::NoSuchBucket(bucket.clone()));
        };
        // A corrupt rule decodes to no rule while the enabled bit — the fact
        // that drives enforcement — survives (OL4).
        Ok(row.enabled.map(|enabled| {
            object_lock::Config::from_rule_wire(enabled, row.rule.as_deref().unwrap_or_default())
        }))
    }

    /// Store the bucket's configuration. A missing bucket writes nothing and
    /// answers `false`; the bucket row's lock is taken in the same
    /// transaction, so the presence decision cannot race a delete.
    ///
    /// OL13: the same transaction reads the versioning state and refuses a
    /// bucket that is not `Enabled`. A concurrent suspend takes the bucket
    /// row's lock and reads the configuration (R21), so the two writers
    /// serialize on one row and cannot commit the enabled configuration on a
    /// `Suspended` bucket.
    async fn set_object_lock(
        &self,
        bucket: &bucket::Name,
        config: &object_lock::Config,
    ) -> Result<bool, Error> {
        let mut tx = begin(self.pool()).await?;
        if !bucket_row_present(&mut tx, bucket).await? {
            tx.rollback().await?;
            return Ok(false);
        }
        let state = query_file_as!(StoredState, "sql/meta/version/bucket_state.sql", bucket)
            .fetch_optional(&mut *tx)
            .await?;
        if state.map(|state| state.state) != Some(State::Enabled) {
            tx.rollback().await?;
            return Err(Error::Storage(storage::invalid_bucket_state(bucket)));
        }
        query_file!(
            "sql/meta/bucket/lock_upsert.sql",
            bucket,
            config.enabled,
            config.rule_wire()
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Remove the bucket's configuration. The answer is the bucket's presence,
    /// exactly as the lifecycle clear answers it: a bucket that stores no
    /// configuration is still `true`, and only a missing bucket is `false`.
    async fn clear_object_lock(&self, bucket: &bucket::Name) -> Result<bool, Error> {
        let mut tx = begin(self.pool()).await?;
        if !bucket_row_present(&mut tx, bucket).await? {
            tx.rollback().await?;
            return Ok(false);
        }
        clear_config(&mut tx, bucket).await?;
        tx.commit().await?;
        Ok(true)
    }
}

impl PgStore {
    /// Create a bucket with Object Lock enabled: the bucket row, its
    /// `Enabled` versioning state and the configuration commit as one
    /// transaction (OL13), so a crash can leave no partial lock-enabled
    /// bucket. `AlreadyExists` when the name is taken; the caller supplies a
    /// request-validated configuration.
    ///
    /// # Errors
    ///
    /// `AlreadyExists` when the bucket row is there already, and the driver's
    /// answer when a statement fails.
    pub async fn create_bucket_with_lock(
        &self,
        bucket: &bucket::Name,
        row: &BucketRow,
        config: &object_lock::Config,
    ) -> Result<(), Error> {
        let mut tx = begin(self.pool()).await?;
        insert_bucket_row(&mut tx, bucket, row).await?;
        query_file!(
            "sql/meta/version/set_bucket_state.sql",
            bucket,
            State::Enabled as _
        )
        .execute(&mut *tx)
        .await?;
        query_file!(
            "sql/meta/bucket/lock_upsert.sql",
            bucket,
            config.enabled,
            config.rule_wire()
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
