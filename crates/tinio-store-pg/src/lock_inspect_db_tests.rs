//! The database-backed legs for the doctor's Object Lock scan (OL31).
//!
//! Each leg skips when `TINIO_PG_DSN` is unset, so the default workspace run
//! stays database-free (PG8). The legs seed the three tables with raw
//! statements: the corrupt shapes are what the store's own writers refuse to
//! produce, so the scan is the only reader that ever sees them.

use sqlx::query;

use crate::{
    _core::object_lock::{self, Lock},
    lock_inspect::{self, CorruptConfig, CorruptIntent, CorruptLock, OrphanLock},
    tests::fixtures::{bucket_row, key, name, scratch},
};

/// Insert one stored per-version lock row; every argument is written verbatim,
/// so a leg can seed a shape the writers refuse.
async fn lock_row(
    store: &crate::PgStore,
    bucket: &str,
    object_key: &str,
    version: &str,
    mode: Option<&str>,
    retain_until: Option<i64>,
    legal_hold: Option<&str>,
) {
    query(
        "INSERT INTO version_object_lock \
         (bucket, key, version_id, mode, retain_until, legal_hold) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(bucket)
    .bind(object_key)
    .bind(version)
    .bind(mode)
    .bind(retain_until)
    .bind(legal_hold)
    .execute(store.pool())
    .await
    .expect("the stored lock row");
}

/// Insert one stored upload intent; every argument is written verbatim.
async fn intent_row(
    store: &crate::PgStore,
    bucket: &str,
    upload: &str,
    mode: Option<&str>,
    retain_until: Option<i64>,
    legal_hold: Option<&str>,
) {
    query(
        "INSERT INTO upload_object_lock \
         (bucket, upload_id, mode, retain_until, legal_hold) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(bucket)
    .bind(upload)
    .bind(mode)
    .bind(retain_until)
    .bind(legal_hold)
    .execute(store.pool())
    .await
    .expect("the stored upload intent");
}

/// Insert one object version row and its index entry at sequence `seq`.
async fn version_row(store: &crate::PgStore, bucket: &str, object_key: &str, id: &str, seq: i64) {
    query(
        "INSERT INTO version_rows \
         (bucket, key, seq, id, blob, size, etag, created, became_noncurrent, \
          kind, tags, checksum, acl) \
         VALUES ($1, $2, $3, $4, '', 0, \
                 'd41d8cd98f00b204e9800998ecf8427e', now(), 0, 'object', \
                 '{}'::jsonb, '', row(NULL, ARRAY[]::acl_grant[])::acl)",
    )
    .bind(bucket)
    .bind(object_key)
    .bind(seq)
    .bind(id)
    .execute(store.pool())
    .await
    .expect("the stored version row");
    query("INSERT INTO version_index (bucket, key, id, seq) VALUES ($1, $2, $3, $4)")
        .bind(bucket)
        .bind(object_key)
        .bind(id)
        .bind(seq)
        .execute(store.pool())
        .await
        .expect("the stored version index entry");
}

/// Insert one current slot naming `version`.
async fn slot(store: &crate::PgStore, bucket: &str, object_key: &str, version: &str, seq: i64) {
    query(
        "INSERT INTO current_slots \
         (bucket, key, version_id, seq, is_delete_marker) \
         VALUES ($1, $2, $3, $4, FALSE)",
    )
    .bind(bucket)
    .bind(object_key)
    .bind(version)
    .bind(seq)
    .execute(store.pool())
    .await
    .expect("the stored current slot");
}

/// A database with no lock rows at all is clean: the scan's own baseline.
#[tokio::test]
async fn an_empty_database_scans_clean() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    assert!(scan.is_clean(), "{scan:?}");
    assert_eq!(scan, lock_inspect::Scan::default());
    db.drop_db().await;
}

/// A valid rule, a lock owned by a version row, a lock owned by the key's
/// current slot, a valid intent and a factless lock row all scan clean. The
/// factless row is the model's "not a lock" arm: it is neither corrupt nor an
/// orphan even though no version owns it.
#[tokio::test]
async fn valid_rows_scan_clean_and_a_factless_row_is_not_a_lock() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-clean");
    bucket_row(&store, &data).await;
    let indexed = key("indexed.bin");
    let slotted = key("slotted.bin");

    query("INSERT INTO bucket_object_lock (bucket, enabled, rule) VALUES ($1, $2, $3)")
        .bind(data.as_ref())
        .bind(true)
        .bind("GOVERNANCE,DAYS,30")
        .execute(store.pool())
        .await
        .expect("the valid rule");

    version_row(&store, data.as_ref(), indexed.as_ref(), "v-indexed", 1).await;
    lock_row(
        &store,
        data.as_ref(),
        indexed.as_ref(),
        "v-indexed",
        Some("COMPLIANCE"),
        Some(1_700_000_000_000_000_000),
        Some("ON"),
    )
    .await;

    slot(&store, data.as_ref(), slotted.as_ref(), "v-slotted", 0).await;
    lock_row(
        &store,
        data.as_ref(),
        slotted.as_ref(),
        "v-slotted",
        None,
        None,
        Some("ON"),
    )
    .await;

    intent_row(
        &store,
        data.as_ref(),
        "upload-clean",
        Some("GOVERNANCE"),
        Some(1_700_000_000_000_000_000),
        Some("OFF"),
    )
    .await;

    // No lock fact at all: the model reads "no lock", so the scan ignores the
    // row even though nothing owns it.
    lock_row(
        &store,
        data.as_ref(),
        "factless.bin",
        "v-factless",
        None,
        None,
        None,
    )
    .await;

    // The same shape with a hold `OFF` and no retention is also "no lock".
    lock_row(
        &store,
        data.as_ref(),
        "empty.bin",
        "v-empty",
        None,
        None,
        Some("OFF"),
    )
    .await;

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    assert!(scan.is_clean(), "{scan:?}");
    db.drop_db().await;
}

/// A bucket rule that does not decode is reported with the enabled bit that
/// survives the decoder (OL4): an operator must not read "corrupt" as "the
/// bucket stopped enforcing".
#[tokio::test]
async fn a_corrupt_bucket_rule_keeps_its_enabled_bit_in_the_report() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-corrupt-rule");
    bucket_row(&store, &data).await;
    query("INSERT INTO bucket_object_lock (bucket, enabled, rule) VALUES ($1, TRUE, $2)")
        .bind(data.as_ref())
        .bind("not a rule")
        .execute(store.pool())
        .await
        .expect("the corrupt rule");

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    assert_eq!(
        scan.corrupt_configs,
        vec![CorruptConfig {
            bucket: data.to_string(),
            enabled: true,
        }]
    );
    assert!(scan.corrupt_locks.is_empty());
    assert!(scan.corrupt_intents.is_empty());
    assert!(scan.orphan_locks.is_empty());
    db.drop_db().await;
}

/// A per-version row with a partial mode/date pair fails closed, and the scan
/// still resolves its ownership: an owned corrupt row is corrupt, not an
/// orphan.
#[tokio::test]
async fn a_corrupt_owned_lock_row_is_reported_and_not_an_orphan() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-corrupt-lock");
    bucket_row(&store, &data).await;
    let target = key("locked.bin");
    version_row(&store, data.as_ref(), target.as_ref(), "v-corrupt", 1).await;
    lock_row(
        &store,
        data.as_ref(),
        target.as_ref(),
        "v-corrupt",
        Some("GOVERNANCE"),
        None,
        Some("OFF"),
    )
    .await;

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    let expected = object_lock::Lock::from_columns(Some("GOVERNANCE"), None, Some("OFF"))
        .expect_err("the partial pair fails closed");
    assert_eq!(
        scan.corrupt_locks,
        vec![CorruptLock {
            bucket: data.to_string(),
            key: target.to_string(),
            version: "v-corrupt".to_owned(),
            detail: expected.detail,
        }]
    );
    assert!(
        scan.orphan_locks.is_empty(),
        "the version index owns the row: {:?}",
        scan.orphan_locks
    );
    db.drop_db().await;
}

/// A multipart intent whose mode/date pair is partial fails closed.
#[tokio::test]
async fn a_corrupt_upload_intent_is_reported() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-corrupt-intent");
    bucket_row(&store, &data).await;
    intent_row(
        &store,
        data.as_ref(),
        "upload-corrupt",
        Some("BAD"),
        None,
        Some("OFF"),
    )
    .await;

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    let expected = Lock::from_columns(Some("BAD"), None, Some("OFF"))
        .expect_err("the partial pair fails closed");
    assert_eq!(
        scan.corrupt_intents,
        vec![CorruptIntent {
            bucket: data.to_string(),
            upload: "upload-corrupt".to_owned(),
            detail: expected.detail,
        }]
    );
    assert!(scan.corrupt_configs.is_empty());
    assert!(scan.corrupt_locks.is_empty());
    assert!(scan.orphan_locks.is_empty());
    db.drop_db().await;
}

/// A well-formed lock row that resolves to no version row and no current slot
/// is an orphan (the drain's ownership rule), and it is not corrupt.
#[tokio::test]
async fn a_lock_row_that_resolves_to_nothing_is_an_orphan() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-orphan");
    bucket_row(&store, &data).await;
    let target = key("orphan.bin");
    lock_row(
        &store,
        data.as_ref(),
        target.as_ref(),
        "v-missing",
        Some("GOVERNANCE"),
        Some(1_700_000_000_000_000_000),
        Some("OFF"),
    )
    .await;

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    assert_eq!(
        scan.orphan_locks,
        vec![OrphanLock {
            bucket: data.to_string(),
            key: target.to_string(),
            version: "v-missing".to_owned(),
        }]
    );
    assert!(
        scan.corrupt_locks.is_empty(),
        "a well-formed row decodes: {:?}",
        scan.corrupt_locks
    );
    db.drop_db().await;
}

/// The slot arm of the ownership rule excludes a delete marker: a lock row
/// naming the marker's version is an orphan even though the slot records the
/// wire.
#[tokio::test]
async fn a_marker_slot_does_not_own_a_lock_row() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let data = name("inspect-marker");
    bucket_row(&store, &data).await;
    let target = key("marker.bin");
    query(
        "INSERT INTO current_slots \
         (bucket, key, version_id, seq, is_delete_marker) \
         VALUES ($1, $2, $3, 0, TRUE)",
    )
    .bind(data.as_ref())
    .bind(target.as_ref())
    .bind("v-marker")
    .execute(store.pool())
    .await
    .expect("the stored marker slot");
    lock_row(
        &store,
        data.as_ref(),
        target.as_ref(),
        "v-marker",
        Some("GOVERNANCE"),
        Some(1_700_000_000_000_000_000),
        Some("OFF"),
    )
    .await;

    let scan = lock_inspect::scan(store.pool())
        .await
        .expect("the scan answers");
    assert_eq!(scan.orphan_locks.len(), 1, "{scan:?}");
    assert_eq!(scan.orphan_locks[0].version, "v-marker");
    db.drop_db().await;
}
