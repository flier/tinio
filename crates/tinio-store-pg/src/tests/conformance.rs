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
    assert_object_lock_conformance, assert_object_lock_upload_conformance,
    assert_version_conformance,
};

use crate::tests::{
    db::{TestDb, base_dsn, require_dsn},
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

/// The Object Lock contract's bucket, version, delete, batch and drain rules
/// over real PostgreSQL (OL4–OL8, OL21).
#[tokio::test]
async fn object_lock_conforms() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_object_lock_conformance(&store).await;
    db.drop_db().await;
}

/// The multipart lock-intent contract: the intent is written atomically with
/// the upload row, reads back, is replaced and clears (OL6, OL21).
#[tokio::test]
async fn object_lock_upload_conforms() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_object_lock_upload_conformance(&store).await;
    db.drop_db().await;
}

/// pg's Object Lock tables are part of the one initial migration, so a fresh
/// database always holds all three: the redb MAY_BE_ABSENT / read-only
/// pre-feature shape does not apply here. A database built before the feature
/// is not served at all — the schema checksum gate refuses it (OL4, OL26).
#[tokio::test]
async fn a_fresh_database_holds_the_three_object_lock_tables() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let present: Vec<Option<String>> =
        sqlx::query_scalar("SELECT to_regclass(t)::text FROM unnest(ARRAY[$1, $2, $3]) AS t")
            .bind("bucket_object_lock")
            .bind("version_object_lock")
            .bind("upload_object_lock")
            .fetch_all(store.as_driver())
            .await
            .expect("the table probes");
    assert_eq!(
        present,
        vec![
            Some("bucket_object_lock".to_owned()),
            Some("version_object_lock".to_owned()),
            Some("upload_object_lock".to_owned()),
        ],
        "the initial migration creates the three tables, all-or-nothing"
    );
    db.drop_db().await;
}

/// The row-lock pieces the Object Lock write path depends on (OL13).
///
/// **This pin is shallow and claims no ordering.** It checks each statement
/// that is expected to take a row lock names the row's table and carries
/// `FOR UPDATE`; it does not observe blocking, so it cannot prove the order
/// two statements run in. The ordering guarantee is the **Rust call
/// sequence**: `advance_slot_with` (bucket row via `assert_bucket_row`, then
/// the current slot, then the marker-bit read, then `lock_upsert`),
/// `expire_with`/`expire_many_report` (bucket row, slot, then per sequence the
/// version row and its lock row), `restore_slot` (bucket row, slot, version
/// row) and `drain_bucket` (bucket row, then `drain_lock_owners`). Those call
/// sites are the review surface; the behavioural legs
/// (`a_config_write_and_a_suspend_serialize_on_the_bucket_row`, the drain
/// refusal) are what exercise the contention.
#[test]
fn the_object_lock_statements_take_their_rows_for_update() {
    for (name, table, sql) in [
        (
            "slot",
            "current_slots",
            include_str!("../../sql/meta/version/slot_for_update.sql"),
        ),
        (
            "version row",
            "version_rows",
            include_str!("../../sql/meta/version/row_for_update.sql"),
        ),
        (
            "lock row",
            "version_object_lock",
            include_str!("../../sql/meta/version/lock_get_for_update.sql"),
        ),
        (
            "drain owners",
            "version_object_lock",
            include_str!("../../sql/meta/version/drain_lock_owners.sql"),
        ),
    ] {
        assert!(
            sql.contains(table),
            "the {name} statement names its table ({table})"
        );
        assert!(
            sql.contains("FOR UPDATE"),
            "the {name} statement takes its rows' lock"
        );
    }
}

/// Review Focus 1: the order a listing answers is the key's UTF-8 byte
/// sequence, not the database's own locale. The adversarial pair is the suite's
/// (`data` vs `data-2` vs `data/`), plus the byte-order pair that a locale
/// collation would flip (`0`, `B`, `_`, `a`).
///
/// **Three databases, one property.** The cluster's own `en_US.utf8` and a
/// `BUILTIN_LOCALE 'C.UTF-8'` database both order bytes exactly as `"C"` does on
/// this musl image (the pg design's finding F9), so neither can tell the pinned
/// `COLLATE "C"` from the database's default. The third, an ICU `en-US-x-icu`
/// database, orders them differently (see
/// [`the_icu_locale_would_move_the_order_without_the_pinned_collation`]), which
/// is what makes this a real guard rather than a decorative one: dropping
/// `COLLATE "C"` from a key column's `ORDER BY` turns the ICU database's walk
/// red.
#[tokio::test]
async fn the_order_does_not_move_with_the_database_locale() {
    if !require_dsn() {
        return;
    }
    let default_locale = TestDb::create().await.expect("a scratch database");
    let builtin_c = TestDb::create_builtin_locale()
        .await
        .expect("a scratch database with a non-locale default collation");
    let icu = TestDb::create_icu_locale()
        .await
        .expect("a scratch database with an ICU collation");
    for (n, db) in [&default_locale, &builtin_c, &icu].into_iter().enumerate() {
        let store = db.store().await.expect("the store connects and migrates");
        let walked = walked_collation_keys(&store, &format!("order-{n}")).await;
        let mut expected = COLLATION_KEYS
            .iter()
            .map(|key| (*key).to_owned())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(
            walked, expected,
            "the traversal is byte-ordered on database {n}"
        );
        let byte_keys = walked
            .iter()
            .filter(|key| BYTE_ORDER.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            byte_keys, BYTE_ORDER,
            "COLLATE \"C\" orders the bytes on database {n}"
        );
        db.drop_db().await;
    }
}

/// The discriminating collation leg (2026-10-13 review, finding 3).
///
/// The two databases in the leg above cannot disagree with `"C"` on this image,
/// so the pin could be dropped anywhere and nothing turned red. An ICU-locale
/// database does disagree: the spike measured its own order of the adversarial
/// pair as `_,0,a,B` against `"C"`'s `0,B,_,a`. This leg first proves the ICU
/// database's default order really is a *different* order — if it ever matched
/// `"C"`'s, the guard above would be decorative again and this assertion says so
/// loudly — and then walks the same keys, which the pinned `COLLATE "C"` still
/// answers in byte order.
///
/// The fixture image can create this database (`postgres:18-alpine` ships ICU;
/// re-measured 2026-10-13 against the compose fixture). If an image could not,
/// [`TestDb::create_icu_locale`] would fail naming the `CREATE DATABASE` error
/// rather than skip, and the alternative would be a `COLLATE` on the columns
/// from a provider the image does ship.
#[tokio::test]
async fn the_icu_locale_would_move_the_order_without_the_pinned_collation() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create_icu_locale()
        .await
        .expect("an ICU scratch database");
    let store = db.store().await.expect("the store connects and migrates");
    let own_order = database_default_order(&store).await;
    let byte_order = BYTE_ORDER
        .iter()
        .map(|key| (*key).to_owned())
        .collect::<Vec<_>>();
    assert_ne!(
        own_order, byte_order,
        "the ICU database orders the byte pair the same as \"C\", so this leg \
         discriminates nothing — pick a locale the image orders differently"
    );
    let walked = walked_collation_keys(&store, "icu-order").await;
    let byte_keys = walked
        .iter()
        .filter(|key| BYTE_ORDER.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        byte_keys, BYTE_ORDER,
        "COLLATE \"C\" still orders the bytes on an ICU-locale database"
    );
    db.drop_db().await;
}

/// The collation legs' adversarial keys: the suite's traversal triple
/// (`data`/`data-2`/`data/…`) plus the byte-order pair a locale collation would
/// flip.
const COLLATION_KEYS: [&str; 8] = ["data", "data-2", "data/", "data/leaf", "0", "B", "_", "a"];

/// The byte-order pair alone, in `"C"`'s order: a locale collation that is not
/// `"C"`'s orders these four differently (the ICU fixture measured `_,0,a,B`).
const BYTE_ORDER: [&str; 4] = ["0", "B", "_", "a"];

/// Seed `bucket_name` with [`COLLATION_KEYS`] and answer the walked key order —
/// the store's own [`tinio_core::meta::ObjectMeta::walk`], whose `ORDER BY`
/// carries the pinned `COLLATE "C"`.
async fn walked_collation_keys(store: &PgStore, bucket_name: &str) -> Vec<String> {
    let bucket = tinio_core::bucket::name(bucket_name.to_owned()).unwrap();
    tinio_core::meta::BucketMeta::record(store, &bucket, std::time::SystemTime::UNIX_EPOCH)
        .await
        .expect("the bucket row");
    for raw in COLLATION_KEYS {
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
        tinio_core::meta::ObjectMeta::put(store, &bucket, &key, &row)
            .await
            .expect("the object row");
    }
    tinio_core::meta::ObjectMeta::walk(store, &bucket)
        .await
        .expect("the traversal")
        .into_iter()
        .map(|(key, _)| key.to_string())
        .collect()
}

/// The database's **own** default-collation order of [`BYTE_ORDER`] — the order
/// the pinned `COLLATE "C"` must override.
///
/// A runtime query rather than a `query!` macro, so no `.sqlx` entry is owed
/// (the cache is not this round's to regenerate), and spelled inline rather
/// than as a `sql/tests/` file for the same reason the ICU `CREATE DATABASE`
/// shape is: this round's ownership leaves `crates/tinio-store-pg/sql/**` to
/// another lane. That file is the doctrine's home for it.
async fn database_default_order(store: &PgStore) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT c FROM (VALUES ('0'), ('B'), ('_'), ('a')) AS t(c) ORDER BY c::text",
    )
    .fetch_all(store.as_driver())
    .await
    .expect("the database's own order")
}

/// The migration path: an empty database reaches the current version one step
/// at a time, records each step, and a second run is idempotent.
///
/// The history is one file, so the forward path is the single step from an
/// empty database (`schema_version` absent, read as 0) to version 1, and the
/// bookkeeping holds exactly one row — one per embedded migration. The leg
/// states the property, not the count, so a second migration would only add
/// another step and another row.
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
    // One row per embedded migration, so a one-file history records the single
    // row for version 1 and `count(*)` equals the version.
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

/// The CI drift check's database prerequisite: **migrate the base database**
/// (`TINIO_PG_DSN`'s own, not a scratch one).
///
/// `cargo xtask sqlx check` describes every statement against the database the
/// dsn names, and an unmigrated database cannot answer that: the driver cannot
/// infer a column's type (`relation "…" does not exist`) and the check reports
/// that instead of the drift it exists for. The `@pg` cucumber pass happens to
/// migrate the base database as its first connect, so the check used to depend
/// on that pass having run; this leg is the explicit migration the CI job runs
/// in its own step, so the check reports drift whatever ran before it. It is
/// idempotent — a second connect sees the landed version and runs nothing — so
/// running it inside the ordinary suite as well is harmless.
#[tokio::test]
async fn the_base_database_is_migrated_for_the_sqlx_drift_check() {
    let Some(dsn) = base_dsn() else {
        return;
    };
    let store = PgStore::connect(&dsn)
        .await
        .expect("the base database migrates");
    assert!(
        store.schema().matches(),
        "the base database is at the binary's schema version"
    );
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
