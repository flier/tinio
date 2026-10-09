//! The persistence contracts' conformance suite, run from the crate's own
//! tests against real PostgreSQL.
//!
//! These legs skip when `TINIO_PG_DSN` is unset, so `cargo test --workspace`
//! stays database-free (PG8, PG15). Each leg opens its own scratch database
//! (see `crate::tests::db`), so the legs are safe under `cargo test`'s
//! parallelism.
// The tests address the crate and its dependencies by absolute path on purpose:
// a test that used a shortened path would not prove the public path an external
// caller needs.
#![allow(clippy::absolute_paths)]

use tinio_store_pg::{
    Access, Error, PgStore,
    migrations::{OWNERSHIP_LOCK_KEY, SCHEMA_VERSION},
    pool::Pool,
    store::SchemaState,
};
use tinio_util::testing::metadata::{
    assert_content_conformance, assert_guarded_write_conformance, assert_metadata_conformance,
    assert_version_conformance,
};

use crate::tests::{
    db::{TestDb, require_dsn},
    fixtures::scratch,
};

#[tokio::test]
async fn metadata_conforms() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_metadata_conformance(&store).await;
    db.drop_db().await;
}

#[tokio::test]
async fn version_conforms() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_version_conformance(&store).await;
    db.drop_db().await;
}

#[tokio::test]
async fn guarded_writes_conform() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_guarded_write_conformance(&store).await;
    db.drop_db().await;
}

#[tokio::test]
async fn content_conforms() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_content_conformance(&store).await;
    db.drop_db().await;
}

/// Review Focus 1: the order a listing answers is the key's UTF-8 byte
/// sequence, not the database's own locale. The adversarial pair is the suite's
/// (`data` vs `data-2` vs `data/`), plus the byte-order pair that a locale
/// collation would flip (`0`, `B`, `_`, `a`).
///
/// **Known limit (2026-10-09 review).** On this musl image the cluster's
/// `en_US.utf8` orders bytes exactly as `"C"` does (the pg design's finding
/// F9), so two databases under those two collations cannot disagree: this leg
/// guards that the pin is *present*, not that it is load-bearing. A
/// discriminating leg would need the second database created with
/// `LOCALE_PROVIDER icu`, whose `en-US-x-icu` order the spike measured to
/// differ; the design's collation bullet records the same limit.
#[tokio::test]
async fn the_order_does_not_move_with_the_database_locale() {
    if !require_dsn() {
        return;
    }
    const BYTE_ORDER: [&str; 4] = ["0", "B", "_", "a"];
    let default_locale = TestDb::create().await.expect("a scratch database");
    let builtin_c = TestDb::create_with("LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'")
        .await
        .expect("a scratch database with a non-locale default collation");
    for (n, db) in [&default_locale, &builtin_c].into_iter().enumerate() {
        let store = db.store().await.expect("the store connects and migrates");
        let bucket = tinio_core::bucket::name(format!("order-{n}")).unwrap();
        tinio_core::meta::BucketMeta::record(&store, &bucket, std::time::SystemTime::UNIX_EPOCH)
            .await
            .expect("the bucket row");
        let mut keys: Vec<tinio_core::object::Key> = Vec::new();
        for raw in ["data", "data-2", "data/", "data/leaf", "0", "B", "_", "a"] {
            let key = tinio_core::object::key(raw).unwrap();
            let row = tinio_core::meta::object::Row::from_values(
                &tinio_core::ETag::from_content(raw.as_bytes()).as_str(),
                raw.len() as u64,
                std::time::SystemTime::UNIX_EPOCH,
                0,
                tinio_core::object::Tags::empty(),
                "",
                tinio_core::acl::Acl::default_private(None),
            )
            .expect("a decodable row");
            tinio_core::meta::ObjectMeta::put(&store, &bucket, &key, &row)
                .await
                .expect("the object row");
            keys.push(key);
        }
        let walked: Vec<String> = tinio_core::meta::ObjectMeta::walk(&store, &bucket)
            .await
            .expect("the traversal")
            .into_iter()
            .map(|(key, _)| key.to_string())
            .collect();
        let mut expected = keys.iter().map(|key| key.to_string()).collect::<Vec<_>>();
        expected.sort();
        assert_eq!(walked, expected, "the traversal is byte-ordered");
        let byte_keys = walked
            .iter()
            .filter(|key| BYTE_ORDER.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(byte_keys, BYTE_ORDER, "COLLATE \"C\" orders the bytes");
        db.drop_db().await;
    }
}

/// The migration path: an empty database reaches the current version one step
/// at a time, records each step, and a second run is idempotent.
#[tokio::test]
async fn migrations_advance_an_empty_database_and_are_idempotent() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let inspected = PgStore::inspect(&db.dsn, 1, Access::ReadOnly)
        .await
        .expect("inspection");
    assert_eq!(inspected.schema().applied, 0);
    let writable = db.store().await.expect("the migration");
    assert_eq!(
        writable.schema(),
        SchemaState {
            applied: SCHEMA_VERSION,
            supported: SCHEMA_VERSION
        }
    );
    let applied = sqlx::query_file_scalar!("sql/tests/schema_version_rows.sql")
        .fetch_one(writable.as_driver())
        .await
        .expect("the migration table");
    assert_eq!(applied, SCHEMA_VERSION);
    // A second connection sees the landed version and runs nothing.
    let again = PgStore::connect(&db.dsn).await.expect("the second connect");
    assert!(again.schema().matches());
    again.migrate().await.expect("idempotent");
    let applied = sqlx::query_file_scalar!("sql/tests/schema_version_rows.sql")
        .fetch_one(again.as_driver())
        .await
        .expect("the migration table");
    assert_eq!(applied, SCHEMA_VERSION);
    db.drop_db().await;
}

/// The read-only role never migrates and never takes the ownership lock, while
/// a second writable instance is refused the lock the first holds.
#[tokio::test]
async fn a_read_only_instance_serves_beside_the_writer() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let writer = db.store().await.expect("the writer");
    let _locked = writer.hold_ownership().await.expect("the writer owns it");
    let pool = Pool::open(&db.dsn, 2).expect("the dsn parses");
    let reader = PgStore::connect_with_pool(pool, Access::ReadOnly)
        .await
        .expect("a read-only instance starts beside the writer");
    assert_eq!(reader.access(), Access::ReadOnly);
    // Both refusals are the store's own guard, taken before any SQL: a
    // read-only instance neither migrates nor takes the ownership lock, and
    // the message has to say so rather than being whatever the connection
    // answered.
    let err = reader
        .migrate()
        .await
        .expect_err("read-only never migrates");
    assert!(
        matches!(&err, Error::Database(message) if message.contains("read-only store")),
        "read-only migrate is the store's own refusal, got {err}"
    );
    let err = reader
        .hold_ownership()
        .await
        .expect_err("read-only never takes the ownership lock");
    assert!(
        matches!(&err, Error::Database(message) if message.contains("read-only store")),
        "read-only hold_ownership is the store's own refusal, got {err}"
    );
    let second = db
        .store()
        .await
        .expect("a second writable instance connects");
    let err = second
        .hold_ownership()
        .await
        .expect_err("the second writer is refused the lock");
    assert!(
        matches!(&err, Error::LockHeld { key, .. } if *key == OWNERSHIP_LOCK_KEY),
        "the second writer is refused the lock the first holds, got {err}"
    );
    db.drop_db().await;
}
