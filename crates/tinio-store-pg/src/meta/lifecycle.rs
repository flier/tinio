//! Bucket lifecycle rows: the configuration table beside `buckets` (LC19).
//!
//! The row dies with the bucket, in the same transaction. A merge validates
//! before it writes, and the caller commits only a stored result.

use std::time::SystemTime;

use sqlx::{query, query_file};

use crate::{
    _core::{bucket, lifecycle},
    Error, PgTx, begin,
    meta::{bucket::clear_config, bucket_row_present},
    store::PgStore,
    timestamp::to_timestamp,
};

/// Delete the bucket's lifecycle row inside the caller's transaction — LC19:
/// a rule never outlives the bucket it was written for, and the table has no
/// row for a bucket that stores none, so this is idempotent.
pub(in crate::meta) async fn clear_row(tx: &mut PgTx, bucket: &bucket::Name) -> Result<(), Error> {
    query!("DELETE FROM bucket_lifecycle WHERE bucket = $1", bucket)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

/// Read, upsert and validate inside `tx`. The caller commits only
/// [`lifecycle::Merged::Stored`]. `create_at` records a first-sight bucket
/// row after the merge validates (the lazy policy); without it a missing
/// bucket is [`lifecycle::Merged::Absent`].
pub(in crate::meta) async fn merge_in_tx(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    rules: &[lifecycle::Rule],
    create_at: Option<SystemTime>,
) -> Result<lifecycle::Merged, Error> {
    let present = bucket_row_present(tx, bucket).await?;
    if !present && create_at.is_none() {
        return Ok(lifecycle::Merged::Absent);
    }
    let row = query!(
        "SELECT config FROM bucket_lifecycle WHERE bucket = $1",
        bucket
    )
    .fetch_optional(&mut **tx)
    .await?;
    let current = row
        .and_then(|row| lifecycle::Config::from_wire(&row.config))
        .unwrap_or(lifecycle::Config { rules: Vec::new() });
    let config = match current.apply_rules(rules) {
        Ok(config) => config,
        Err(invalid) => return Ok(lifecycle::Merged::Refused(invalid)),
    };
    if let Some(now) = create_at.filter(|_| !present) {
        let inserted = query_file!(
            "sql/meta/bucket/record_if_absent.sql",
            bucket,
            to_timestamp(now)
        )
        .execute(&mut **tx)
        .await?;
        if inserted.rows_affected() > 0 {
            // OL4: the first-sight insert is a bucket-row write, so a stale
            // Object Lock configuration is cleared beside it.
            clear_config(tx, bucket).await?;
        }
    }
    upsert_row(tx, bucket, &config).await?;
    Ok(lifecycle::Merged::Stored)
}

/// Upsert the bucket's lifecycle row inside the caller's transaction.
pub(in crate::meta) async fn upsert_row(
    tx: &mut PgTx,
    bucket: &bucket::Name,
    config: &lifecycle::Config,
) -> Result<(), Error> {
    query!(
        "INSERT INTO bucket_lifecycle (bucket, config, updated_at) VALUES ($1, $2, now()) ON CONFLICT (bucket) DO UPDATE SET config = $2, updated_at = now()",
        bucket,
        config.to_wire()
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(in crate::meta) async fn get(
    store: &PgStore,
    bucket: &bucket::Name,
) -> Result<Option<lifecycle::Config>, Error> {
    let row = query!(
        "SELECT config FROM bucket_lifecycle WHERE bucket = $1",
        bucket
    )
    .fetch_optional(store.pool())
    .await?;
    Ok(row.and_then(|row| lifecycle::Config::from_wire(&row.config)))
}

pub(in crate::meta) async fn list(
    store: &PgStore,
) -> Result<Vec<(bucket::Name, lifecycle::Config)>, Error> {
    let rows = query!("SELECT bucket, config FROM bucket_lifecycle ORDER BY bucket COLLATE \"C\"")
        .fetch_all(store.pool())
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let config = lifecycle::Config::from_wire(&row.config)?;
            match bucket::name(row.bucket) {
                Ok(bucket) => Some((bucket, config)),
                Err(err) => {
                    tracing::warn!(
                        %err,
                        "discarding a lifecycle row under an invalid bucket name"
                    );
                    None
                }
            }
        })
        .collect())
}

pub(in crate::meta) async fn set(
    store: &PgStore,
    bucket: &bucket::Name,
    config: &lifecycle::Config,
) -> Result<bool, Error> {
    let mut tx = begin(store.pool()).await?;
    if !bucket_row_present(&mut tx, bucket).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    upsert_row(&mut tx, bucket, config).await?;
    tx.commit().await?;
    Ok(true)
}

/// Merge inside a transaction of its own, committing only a stored result.
async fn merge_with(
    store: &PgStore,
    bucket: &bucket::Name,
    rules: &[lifecycle::Rule],
    create_at: Option<SystemTime>,
) -> Result<lifecycle::Merged, Error> {
    let mut tx = begin(store.pool()).await?;
    let merged = merge_in_tx(&mut tx, bucket, rules, create_at).await?;
    if matches!(merged, lifecycle::Merged::Stored) {
        tx.commit().await?;
    } else {
        tx.rollback().await?;
    }
    Ok(merged)
}

pub(in crate::meta) async fn merge(
    store: &PgStore,
    bucket: &bucket::Name,
    rules: &[lifecycle::Rule],
) -> Result<lifecycle::Merged, Error> {
    merge_with(store, bucket, rules, None).await
}

pub(in crate::meta) async fn merge_or_create(
    store: &PgStore,
    bucket: &bucket::Name,
    rules: &[lifecycle::Rule],
    now: SystemTime,
) -> Result<lifecycle::Merged, Error> {
    merge_with(store, bucket, rules, Some(now)).await
}

pub(in crate::meta) async fn set_or_create(
    store: &PgStore,
    bucket: &bucket::Name,
    config: &lifecycle::Config,
    now: SystemTime,
) -> Result<(), Error> {
    let mut tx = begin(store.pool()).await?;
    // The lazy first-sight policy: `record_if_absent` inserts the bucket
    // row only when the directory-backed store has none, and does nothing
    // on an existing bucket. The configuration lands in the same
    // transaction, so no reader sees the row without it.
    let inserted = query_file!(
        "sql/meta/bucket/record_if_absent.sql",
        bucket,
        to_timestamp(now)
    )
    .execute(&mut *tx)
    .await?;
    if inserted.rows_affected() > 0 {
        // OL4: the first-sight insert is a bucket-row write, so a stale
        // Object Lock configuration is cleared beside it.
        clear_config(&mut tx, bucket).await?;
    }
    upsert_row(&mut tx, bucket, config).await?;
    tx.commit().await?;
    Ok(())
}

pub(in crate::meta) async fn clear(store: &PgStore, bucket: &bucket::Name) -> Result<bool, Error> {
    let mut tx = begin(store.pool()).await?;
    if !bucket_row_present(&mut tx, bucket).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    clear_row(&mut tx, bucket).await?;
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use tinio_util::testing::metadata::lifecycle_config;

    use crate::{
        _core::{
            bucket::Name,
            lifecycle,
            meta::{BucketMeta, bucket::Row as BucketRow},
        },
        PgStore,
        tests::fixtures::{name, scratch},
        timestamp::unix_seconds as at,
    };

    /// The lifecycle row round-trips and its setters answer bucket presence,
    /// never change: an identical re-write and a clear on a bucket that stores
    /// none both answer `true`, and only a missing bucket answers `false`.
    #[tokio::test]
    async fn the_lifecycle_row_round_trips_and_its_setters_answer_presence() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-lifecycle");
        let config = lifecycle_config("expire-logs");

        // No bucket row: nothing is written and both setters answer `false`.
        assert!(
            !BucketMeta::set_lifecycle(&store, &data, &config)
                .await
                .unwrap()
        );
        assert!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .is_none()
        );
        assert!(!BucketMeta::clear_lifecycle(&store, &data).await.unwrap());
        assert!(BucketMeta::lifecycles(&store).await.unwrap().is_empty());

        BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        assert!(
            BucketMeta::set_lifecycle(&store, &data, &config)
                .await
                .unwrap()
        );
        assert_eq!(
            BucketMeta::lifecycle(&store, &data).await.unwrap(),
            Some(config.clone())
        );
        assert_eq!(
            BucketMeta::lifecycles(&store).await.unwrap(),
            vec![(data.clone(), config.clone())]
        );
        assert!(
            BucketMeta::set_lifecycle(&store, &data, &config)
                .await
                .unwrap(),
            "an identical configuration is still presence"
        );
        assert!(BucketMeta::clear_lifecycle(&store, &data).await.unwrap());
        assert!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            BucketMeta::clear_lifecycle(&store, &data).await.unwrap(),
            "a bucket that stores no configuration is still present"
        );
        assert!(BucketMeta::lifecycles(&store).await.unwrap().is_empty());

        // The seed's merge keeps a rule it does not name, refuses a document
        // the request path would refuse, and writes nothing for a missing bucket.
        let keep = lifecycle_config("keep");
        let replace = lifecycle_config("replace");
        let mut stored = keep.clone();
        stored.rules.extend(replace.rules.clone());
        assert!(matches!(
            BucketMeta::merge_lifecycle(&store, &data, &stored.rules)
                .await
                .unwrap(),
            lifecycle::Merged::Stored
        ));
        let mut updated = replace.clone();
        updated.rules[0].expiration.as_mut().unwrap().days = Some(1);
        assert!(matches!(
            BucketMeta::merge_lifecycle(&store, &data, &updated.rules)
                .await
                .unwrap(),
            lifecycle::Merged::Stored
        ));
        let got = BucketMeta::lifecycle(&store, &data).await.unwrap().unwrap();
        assert_eq!(got.rules.len(), 2);
        assert_eq!(got.rules[1].expiration.as_ref().unwrap().days, Some(1));
        let mut broken = lifecycle_config("broken");
        broken.rules[0].expiration = None;
        assert!(matches!(
            BucketMeta::merge_lifecycle(&store, &data, &broken.rules)
                .await
                .unwrap(),
            lifecycle::Merged::Refused(_)
        ));
        assert_eq!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .unwrap()
                .rules
                .len(),
            2,
            "a refused merge leaves the stored rules"
        );
        let missing = name("plane-bucket-lifecycle-missing");
        assert!(matches!(
            BucketMeta::merge_lifecycle(&store, &missing, &keep.rules)
                .await
                .unwrap(),
            lifecycle::Merged::Absent
        ));
        db.drop_db().await;
    }

    /// LC19: `teardown_bucket` deletes the bucket row, and the schema's
    /// `ON DELETE CASCADE` takes the lifecycle row with it.
    #[tokio::test]
    async fn a_bucket_teardown_takes_the_lifecycle_row_with_it() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-teardown-lifecycle");
        BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        assert!(
            BucketMeta::set_lifecycle(&store, &data, &lifecycle_config("expire-logs"))
                .await
                .unwrap()
        );

        store.teardown_bucket(&data).await.unwrap();

        assert!(
            BucketMeta::row(&store, &data).await.unwrap().is_none(),
            "the bucket row is gone"
        );
        assert!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .is_none(),
            "the rule died with the bucket (LC19)"
        );
        assert!(BucketMeta::lifecycles(&store).await.unwrap().is_empty());
        db.drop_db().await;
    }

    /// Drop the `buckets` row alone: the FK cascade takes the lifecycle row
    /// with it. A runtime statement rather than a `query!` macro: a test's
    /// statement owes no `.sqlx` entry.
    async fn drop_bucket_row(store: &PgStore, bucket: &Name) {
        sqlx::query("DELETE FROM buckets WHERE name = $1")
            .bind(bucket.as_ref())
            .execute(store.as_driver())
            .await
            .unwrap();
    }

    /// LC19's pg arm is the schema's: `bucket_lifecycle.bucket` references
    /// `buckets(name) ON DELETE CASCADE`, so a re-created bucket starts clean.
    #[tokio::test]
    async fn the_schema_cascades_a_lifecycle_row_with_its_bucket() {
        let Some((db, store)) = scratch().await else {
            return;
        };
        let data = name("plane-bucket-lifecycle-create");
        BucketMeta::create(&store, &data, &BucketRow::at(at(1)))
            .await
            .unwrap();
        assert!(
            BucketMeta::set_lifecycle(&store, &data, &lifecycle_config("expire-logs"))
                .await
                .unwrap()
        );
        drop_bucket_row(&store, &data).await;
        assert!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .is_none(),
            "the FK cascade removes the row with the bucket"
        );
        assert!(
            BucketMeta::lifecycles(&store).await.unwrap().is_empty(),
            "…and the scan does not resurrect it"
        );

        BucketMeta::create(&store, &data, &BucketRow::at(at(2)))
            .await
            .unwrap();
        assert!(
            BucketMeta::lifecycle(&store, &data)
                .await
                .unwrap()
                .is_none(),
            "a re-created bucket starts with no rules"
        );
        db.drop_db().await;
    }
}
