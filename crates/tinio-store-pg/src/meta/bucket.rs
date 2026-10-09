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
        bucket, cors,
        meta::{
            BucketMeta,
            bucket::{Row as BucketRow, decode_cors},
        },
        object,
    },
    Error, begin,
    meta::{acl_composite, project},
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

/// A row's CORS column: the config's wire, or the empty sentinel for none.
fn cors_wire(cors: Option<&cors::Config>) -> String {
    cors.map(|cors| cors.to_wire()).unwrap_or_default()
}

/// `remove_if_empty`'s one-statement verdict.
struct Verdict {
    present: bool,
    removed: bool,
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
        query_file!("sql/meta/bucket/record.sql", bucket, to_timestamp(created))
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn create(&self, bucket: &bucket::Name, row: &BucketRow) -> Result<(), Error> {
        let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
        let inserted = query_file!(
            "sql/meta/bucket/create.sql",
            bucket,
            to_timestamp(row.created),
            (&row.tags) as _,
            acl as _,
            cors_wire(row.cors.as_ref())
        )
        .execute(self.pool())
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(Error::AlreadyExists(bucket.clone()));
        }
        Ok(())
    }

    async fn record_if_absent(
        &self,
        bucket: &bucket::Name,
        now: SystemTime,
    ) -> Result<SystemTime, Error> {
        let mut tx = begin(self.pool()).await?;
        query_file!(
            "sql/meta/bucket/record_if_absent.sql",
            bucket,
            to_timestamp(now)
        )
        .execute(&mut *tx)
        .await?;
        let created = query_file_scalar!("sql/meta/bucket/bucket_created_at.sql", bucket)
            .fetch_one(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(from_timestamp(created))
    }

    async fn record_full(&self, bucket: &bucket::Name, row: &BucketRow) -> Result<(), Error> {
        let acl = acl_composite(row.owner.as_ref(), &row.acl.grants);
        query_file!(
            "sql/meta/bucket/record_full.sql",
            bucket,
            to_timestamp(row.created),
            (&row.tags) as _,
            acl as _,
            cors_wire(row.cors.as_ref())
        )
        .execute(self.pool())
        .await?;
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
        query_file!(
            "sql/meta/bucket/set_tags_or_create.sql",
            bucket,
            to_timestamp(now),
            tags as _
        )
        .execute(self.pool())
        .await?;
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
        query_file!(
            "sql/meta/bucket/set_grants_or_create.sql",
            bucket,
            to_timestamp(now),
            acl as _
        )
        .execute(self.pool())
        .await?;
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

    async fn remove(&self, bucket: &bucket::Name) -> Result<(), Error> {
        query!("DELETE FROM buckets WHERE name = $1", bucket)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn remove_if_empty(&self, bucket: &bucket::Name) -> Result<(), Error> {
        // ONE statement: the bucket row is locked, then deleted only while it
        // is still empty. A concurrent delete that already removed the row
        // leaves the lock empty, so the loser is NoSuchBucket rather than
        // NotEmpty. A concurrent put_guarded holds the same lock until its
        // object row commits, and this delete re-checks emptiness after it.
        let answer = query_file_as!(Verdict, "sql/meta/bucket/remove_if_empty.sql", bucket)
            .fetch_one(self.pool())
            .await?;
        if !answer.present {
            return Err(Error::NoSuchBucket(bucket.clone()));
        }
        if !answer.removed {
            return Err(Error::NotEmpty(bucket.clone()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::slice::from_ref;

    use crate::{
        _core::{
            acl::{Acl, AclGrants, GROUP_ALL_USERS, Grant, Grantee, GroupUri, OwnerId, Permission},
            meta::{BucketMeta, bucket::Row as BucketRow},
        },
        Error,
        tests::fixtures::{bucket_tags, name, owner, scratch},
        timestamp::{unix_micros, unix_seconds as at},
    };

    #[tokio::test]
    async fn the_creation_time_round_trips_with_sub_second_precision() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-micros");
        let created = unix_micros(1_700_000_000_123_456);
        BucketMeta::create(&store, &data, &BucketRow::at(created))
            .await
            .unwrap();
        assert_eq!(
            BucketMeta::created_at(&store, &data).await.unwrap(),
            Some(created),
            "timestamptz keeps the sub-second digits"
        );
        assert_eq!(
            BucketMeta::row(&store, &data)
                .await
                .unwrap()
                .unwrap()
                .created,
            created
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn the_bucket_row_round_trips_and_orders_by_bytes() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-data");
        let other = name("plane-bucket-other");
        let mut row = BucketRow::at(at(10));
        row.tags = bucket_tags(&[("env", "prod")]);
        BucketMeta::create(&store, &data, &row).await.unwrap();
        BucketMeta::create(&store, &other, &BucketRow::at(at(20)))
            .await
            .unwrap();
        assert_eq!(
            BucketMeta::created_at(&store, &data).await.unwrap(),
            Some(at(10))
        );
        let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert_eq!(reloaded.tags, row.tags);
        assert_eq!(reloaded.created, at(10));
        let all = BucketMeta::load_all(&store).await.unwrap();
        assert_eq!(
            all.iter().map(|(name, _)| name.clone()).collect::<Vec<_>>(),
            vec![data.to_string(), other.to_string()],
            "the listing orders the names by their UTF-8 bytes"
        );
        let many = BucketMeta::load_many(&store, &[other.clone(), name("plane-bucket-absent")])
            .await
            .unwrap();
        assert_eq!(many, vec![Some(at(20)), None]);
        assert_eq!(
            BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
            vec![None]
        );
        assert!(matches!(
            BucketMeta::create(&store, &data, &row).await,
            Err(Error::AlreadyExists(_))
        ));
        db.drop_db().await;
    }

    #[tokio::test]
    async fn the_element_setters_leave_a_missing_row_alone_and_the_creators_land() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-setters");
        assert!(
            !BucketMeta::set_tags(&store, &data, &bucket_tags(&[("a", "b")]))
                .await
                .unwrap()
        );
        assert!(!BucketMeta::clear_tags(&store, &data).await.unwrap());
        assert!(
            !BucketMeta::set_acl(&store, &data, None, &AclGrants::new())
                .await
                .unwrap()
        );
        assert!(!BucketMeta::clear_cors(&store, &data).await.unwrap());
        BucketMeta::set_tags_or_create(&store, &data, &bucket_tags(&[("a", "b")]), at(1))
            .await
            .unwrap();
        assert_eq!(
            BucketMeta::created_at(&store, &data).await.unwrap(),
            Some(at(1))
        );
        BucketMeta::set_grants_or_create(
            &store,
            &data,
            &Acl::default_private(Some(owner())).grants,
            at(2),
        )
        .await
        .unwrap();
        // The creation time is the first writer's, never the second's.
        assert_eq!(
            BucketMeta::created_at(&store, &data).await.unwrap(),
            Some(at(1))
        );
        assert_eq!(
            BucketMeta::row(&store, &data).await.unwrap().unwrap().tags,
            bucket_tags(&[("a", "b")])
        );
        // record clears the recorded elements; record_if_absent keeps the stamp.
        BucketMeta::record(&store, &data, at(5)).await.unwrap();
        let recorded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert_eq!(recorded.created, at(5));
        assert!(recorded.tags.is_empty());
        assert!(recorded.owner.is_none());
        assert_eq!(
            BucketMeta::record_if_absent(&store, &data, at(9))
                .await
                .unwrap(),
            at(5)
        );
        db.drop_db().await;
    }

    #[tokio::test]
    async fn acl_elements_round_trip_and_the_bucket_lifecycle_is_idempotent() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-acl");
        BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        let owner = owner();
        let grants = Acl::default_private(Some(owner.clone())).grants;
        assert!(
            BucketMeta::set_acl(&store, &data, Some(&owner), &grants)
                .await
                .unwrap()
        );
        let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert_eq!(reloaded.owner.as_ref(), Some(&owner));
        assert_eq!(
            reloaded.acl.grants, grants,
            "the decoded ACL carries the grants; the owner rides the composite attribute"
        );
        assert_eq!(
            BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
            vec![Some(owner)]
        );
        assert!(
            BucketMeta::set_tags(&store, &data, &bucket_tags(&[("x", "y")]))
                .await
                .unwrap()
        );
        assert!(BucketMeta::clear_tags(&store, &data).await.unwrap());
        assert!(
            BucketMeta::row(&store, &data)
                .await
                .unwrap()
                .unwrap()
                .tags
                .is_empty()
        );
        // remove_if_empty is the one-statement decision: it removes a bucket
        // with nothing in it and refuses an absent one.
        BucketMeta::remove_if_empty(&store, &data).await.unwrap();
        assert!(
            BucketMeta::created_at(&store, &data)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            BucketMeta::remove_if_empty(&store, &data).await,
            Err(Error::NoSuchBucket(_))
        ));
        BucketMeta::remove(&store, &data).await.unwrap();
        BucketMeta::remove(&store, &data).await.unwrap();
        db.drop_db().await;
    }

    /// Every ACL shape the contract can hold round-trips through the stored
    /// composite: an owner with no grants, a single grant, several grants (whose
    /// stored order is canonical) and no owner at all.
    #[tokio::test]
    async fn every_acl_shape_round_trips_through_the_composite() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-acl-shapes");
        BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        let owner = owner();
        let other = OwnerId::new("f".repeat(64)).expect("a second canonical id");
        let canonical = Grant {
            grantee: Grantee::Canonical(owner.clone()),
            permission: Permission::FullControl,
        };
        let group = Grant {
            grantee: Grantee::Group(GroupUri(GROUP_ALL_USERS.to_owned())),
            permission: Permission::Read,
        };
        // The owner alone: the composite's grants are an empty array.
        assert!(
            BucketMeta::set_acl(&store, &data, Some(&owner), &AclGrants::new())
                .await
                .unwrap()
        );
        let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert_eq!(reloaded.owner.as_ref(), Some(&owner));
        assert!(reloaded.acl.grants.is_empty());
        assert_eq!(
            BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
            vec![Some(owner.clone())]
        );
        // One grant.
        let one = vec![canonical.clone()];
        assert!(
            BucketMeta::set_acl(&store, &data, Some(&other), &one)
                .await
                .unwrap()
        );
        let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert_eq!(reloaded.owner.as_ref(), Some(&other));
        assert_eq!(reloaded.acl.grants, one);
        // Several grants, stored in the canonical order whatever order they
        // arrive in.
        let several = vec![group.clone(), canonical.clone()];
        assert!(
            BucketMeta::set_acl(&store, &data, Some(&other), &several)
                .await
                .unwrap()
        );
        assert_eq!(
            BucketMeta::row(&store, &data)
                .await
                .unwrap()
                .unwrap()
                .acl
                .grants,
            vec![canonical, group]
        );
        // No owner and no grant: the composite's owner attribute is NULL.
        assert!(
            BucketMeta::set_acl(&store, &data, None, &AclGrants::new())
                .await
                .unwrap()
        );
        let reloaded = BucketMeta::row(&store, &data).await.unwrap().unwrap();
        assert!(reloaded.owner.is_none());
        assert!(reloaded.acl.grants.is_empty());
        assert_eq!(
            BucketMeta::owners(&store, from_ref(&data)).await.unwrap(),
            vec![None]
        );
        db.drop_db().await;
    }
}
