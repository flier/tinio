//! The database-backed legs for the diagnostic probes.
//!
//! The crate is addressed by its public path: a shortened path would not prove
//! the path an external caller uses. Each leg skips when `TINIO_PG_DSN` is
//! unset, so the default workspace run stays database-free (PG8).

use tinio_store_pg::{PgStore, diagnostics, pool::Pool};

use crate::test_support::{TestDb, base_dsn};

/// **The off arm, made live.** The compose fixture serves plaintext — its dsn carries
/// `?sslmode=disable` — so the server reports `ssl = false` for the session and leaves the
/// version, cipher and key length `NULL`. The probe must read exactly that, because it is what
/// the "plaintext, and only because the dsn says so" row is built on.
///
/// The on arm has no server to produce it: the fixture offers no TLS, and standing up a
/// certificate-bearing PostgreSQL for one leg is out of this change's scope. The statement's
/// shape is pinned instead by the macro check and the `.sqlx` cache (a wrong column or type is a
/// build error), and the encrypted rendering is pinned by the `tinio-pg` doctor's pure legs.
#[tokio::test]
async fn the_fixture_session_is_plaintext() {
    let Some(dsn) = base_dsn() else {
        eprintln!("skipping: TINIO_PG_DSN is not set");
        return;
    };
    let pool = Pool::open(&dsn, 1).expect("the dsn parses");
    let tls = diagnostics::tls_state(&pool)
        .await
        .expect("the probe answers");
    assert!(!tls.enabled, "{tls:?}");
    assert_eq!(tls.version, None, "{tls:?}");
    assert_eq!(tls.cipher, None, "{tls:?}");
    assert_eq!(tls.bits, None, "{tls:?}");
    pool.close().await;
}

/// **Finding 8.** The privilege probe used to scope itself with
/// `current_schema()`, which answers for whichever schema the session resolves
/// first: a session opened with `options=-c search_path=…` naming anything else
/// read zero rows, `bool_or` answered NULL, and the coalesce printed "no DML
/// grants" while naming a schema the store's tables are not in. The probe is
/// scoped to the schema **holding the store's own relations** now, so the
/// search path cannot change its answer — and the answer names the schemas it
/// checked.
#[tokio::test]
async fn the_privilege_probe_is_scoped_by_the_stores_own_schema_not_the_search_path() {
    if base_dsn().is_none() {
        eprintln!("skipping: TINIO_PG_DSN is not set");
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // The store's relations live in `public`; a session whose search path names
    // another schema is exactly the mis-scoped case.
    PgStore::connect(&db.dsn).await.expect("the schema lands");
    let base = db
        .dsn
        .split_once('?')
        .map(|(base, _)| base)
        .unwrap_or(&db.dsn);
    let mis_scoped = Pool::open(
        &format!("{base}?options=-c%20search_path%3Dtenant&sslmode=disable"),
        1,
    )
    .expect("the dsn parses");

    let privileges = diagnostics::role_privileges(&mis_scoped)
        .await
        .expect("the probe answers");
    assert_eq!(
        privileges.schemas, "public",
        "the probe names the schema the store's relations are in, whatever the session resolves"
    );
    assert!(
        privileges.can_write,
        "the fixture's own role really can write, so a green row would be a false certification"
    );
    // The answer is the same as a session with the default search path: the scope
    // is the store's, not the session's.
    let default = Pool::open(&db.dsn, 1).expect("the dsn parses");
    let privileges = diagnostics::role_privileges(&default)
        .await
        .expect("the probe answers");
    assert_eq!(privileges.schemas, "public");
    assert!(privileges.can_write);

    // A database holding none of the store's relations has no schema to check,
    // and says so — the state the diagnosis must not read as "no grants".
    let fresh = TestDb::create()
        .await
        .expect("an unmigrated scratch database");
    let probe = Pool::open(&fresh.dsn, 1).expect("the dsn parses");
    let privileges = diagnostics::role_privileges(&probe)
        .await
        .expect("the probe answers");
    assert_eq!(
        privileges.schemas, "",
        "an unmigrated database reports that nothing was checked"
    );

    mis_scoped.close().await;
    default.close().await;
    probe.close().await;
    db.drop_db().await;
    fresh.drop_db().await;
}
