use sqlx::Connection;
use tinio_store_pg::{Access, Error, PgStore, migrations::SCHEMA_VERSION, pool::Pool};

use crate::tests::{
    db::{TestDb, require_dsn},
    fixtures::scratch,
};

#[tokio::test]
async fn connect_migrates_and_verifies_the_encoding() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    assert_eq!(store.schema().applied, SCHEMA_VERSION);
    assert!(store.schema().matches());
    // The pool really hands out a usable connection.
    let encoding = sqlx::query_file_scalar!("sql/store/server_encoding.sql")
        .fetch_one(store.as_driver())
        .await
        .expect("a pooled connection");
    assert_eq!(encoding, "UTF8");
    // Migrating again is idempotent.
    store.migrate().await.expect("a second migrate is a no-op");
    db.drop_db().await;
}

#[tokio::test]
async fn a_non_utf8_database_is_refused() {
    if !require_dsn() {
        return;
    }
    // `COLLATE "C"` orders the *server encoding's* bytes, so a LATIN1
    // server would order a listing differently from the contract's UTF-8
    // sequence: the connect refuses instead of serving it (PG4).
    let db = TestDb::create_latin1()
        .await
        .expect("a LATIN1 scratch database");
    let err = db.store().await.expect_err("a LATIN1 server is refused");
    assert!(
        matches!(&err, Error::Encoding { encoding } if encoding.eq_ignore_ascii_case("LATIN1")),
        "{err}"
    );
    assert!(err.to_string().contains("UTF-8"), "{err}");
    db.drop_db().await;
}

#[tokio::test]
async fn a_held_migration_lock_is_refused_after_the_bounded_wait() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // Another instance, mid-migration, holds the lock on its own dedicated
    // connection: the runner refuses within its bounded wait (PG11) rather
    // than blocking behind a migration that may never finish.
    let mut holder = sqlx::PgConnection::connect(&db.dsn)
        .await
        .expect("the holder connects");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(tinio_store_pg::migrations::MIGRATION_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("the holder takes the migration lock");

    let pool = Pool::open(&db.dsn, 1).expect("the pool");
    let err = super::migrate_with_lock_wait(&pool, std::time::Duration::from_millis(250))
        .await
        .expect_err("a held migration lock is refused");
    assert!(
        matches!(
            err,
            Error::MigrationLockBusy { key, .. }
                if key == tinio_store_pg::migrations::MIGRATION_LOCK_KEY
        ),
        "{err}"
    );
    assert!(err.to_string().contains("migrating"), "{err}");

    // Released, the same runner migrates: the refusal was the wait, not the
    // database.
    drop(holder);
    super::migrate_with_lock_wait(&pool, std::time::Duration::from_millis(250))
        .await
        .expect("the lock is free again");
    // The runner's own dedicated connection is the one refused once the
    // database goes away under the pool — the same host-and-database
    // refusal the ownership guard reports, never the dsn (PG5).
    db.drop_db().await;
    let err = super::migrate_with_lock_wait(&pool, std::time::Duration::from_millis(50))
        .await
        .expect_err("a refused dedicated connection stops the runner");
    assert!(
        matches!(&err, Error::Connect { database, .. } if database == &db.name),
        "{err}"
    );
}

#[tokio::test]
async fn a_store_debug_names_the_pool_identity_and_never_the_dsn() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let rendered = format!("{store:?}");
    assert!(
        rendered.contains(&format!("database: {:?}", db.name)),
        "{rendered}"
    );
    assert!(!rendered.contains(&db.dsn), "{rendered}");
    assert!(!rendered.contains("sslmode"), "{rendered}");
    db.drop_db().await;
}

#[tokio::test]
async fn a_read_only_store_never_migrates_and_refuses_a_mismatch() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // Read-only against a database at version 0: refused, and nothing applied.
    let err = db
        .read_only()
        .await
        .expect_err("read-only refuses an unmigrated database");
    assert!(
        matches!(err, Error::SchemaMismatch { found: 0, supported } if supported == SCHEMA_VERSION),
        "{err}"
    );
    let message = err.to_string();
    assert!(message.contains("never migrates"), "{message}");
    // The writable path migrates it, and read-only then serves beside it.
    let writable = db.store().await.expect("writable migrates");
    assert_eq!(writable.access(), Access::ReadWrite);
    let read_only = db
        .read_only()
        .await
        .expect("read-only accepts the landed schema");
    assert_eq!(read_only.access(), Access::ReadOnly);
    // What the read-only instance refuses once it is served — `migrate` and
    // `hold_ownership` — is `tests::conformance`'s leg; this one pins the
    // schema gate the two access modes answer differently.
    db.drop_db().await;
}

#[tokio::test]
async fn a_newer_database_is_refused_with_the_pg_remedy() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    // Plant a future version row: the binary must refuse, never rebuild.
    sqlx::query_file!(
        "sql/tests/plant_future_version.sql",
        (SCHEMA_VERSION + 1) as i32
    )
    .execute(store.as_driver())
    .await
    .expect("the plant");
    let err = PgStore::connect(&db.dsn)
        .await
        .expect_err("a newer schema is refused");
    assert!(
        matches!(err, Error::SchemaTooNew { found, supported } if found == SCHEMA_VERSION + 1 && supported == SCHEMA_VERSION),
        "{err}"
    );
    let message = err.to_string();
    assert!(
        message.contains("pg_dump"),
        "the remedy names a dump/restore: {message}"
    );
    assert!(message.contains("restore"), "{message}");
    assert!(message.contains("never drop and rebuild"), "{message}");
    db.drop_db().await;
}

#[tokio::test]
async fn only_one_writer_holds_the_ownership_lock() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let mut first = store
        .hold_ownership()
        .await
        .expect("the first writer owns it");
    assert!(first.still_held().await.expect("the probe"));
    let err = store
        .hold_ownership()
        .await
        .expect_err("a second writer is refused");
    let Error::LockHeld { holder, .. } = &err else {
        panic!("expected the lock refusal: {err}");
    };
    // PG12: the refusal names the owner. The holder's own connection
    // carries this crate's `application_name`, and the fixture's role sees
    // another session's columns.
    assert!(
        holder.contains("tinio-store-pg"),
        "the refusal names the holding session: {holder}"
    );
    let named = store
        .ownership_holder()
        .await
        .expect("the holder lookup")
        .expect("a session holds the lock");
    assert!(
        named.contains("tinio-store-pg"),
        "the store reports the same holder: {named}"
    );
    assert_eq!(first.key(), tinio_store_pg::migrations::OWNERSHIP_LOCK_KEY);
    drop(first);
    // Releasing the first lets the second take it.
    let _second = store
        .hold_ownership()
        .await
        .expect("the lock is free again");
    db.drop_db().await;
}

/// The guard and the migration runner each open a dedicated,
/// out-of-pool connection from the dsn the pool was built with. When that
/// *new* session is refused — here because the database goes away while the
/// store is running — the failure names the host and the database and
/// carries the redacted driver message (PG5), never the dsn.
#[tokio::test]
async fn a_refused_dedicated_connection_names_the_host_and_database_only() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    // The store is up and its pool is open; the database it names is gone.
    db.drop_db().await;

    let err = store
        .hold_ownership()
        .await
        .expect_err("a refused dedicated connection is an error");
    let Error::Connect { database, .. } = &err else {
        panic!("expected the connect refusal, got {err}");
    };
    assert_eq!(database, &db.name);
    let message = err.to_string();
    assert!(message.starts_with("postgres connect to "), "{message}");
    assert!(message.contains(&db.name), "{message}");
    assert!(
        !message.contains(&db.dsn),
        "the refusal never echoes the dsn: {message}"
    );
    db.drop_db().await;
}

/// The re-verify re-probes the lock while it holds; only a *lost* lock ends
/// the loop, by terminating the process, which an in-process test cannot
/// survive — this leg proves the held side of that fork, then aborts the task.
///
/// The re-probing is observable without a seam: the guard's dedicated
/// connection is the only other session in this database, so a statement of
/// one inside the window is the loop's own cadence.
#[tokio::test]
async fn the_recheck_loop_re_verifies_while_the_lock_is_held() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let guard = store.hold_ownership().await.expect("the writer owns it");
    let recheck = tokio::spawn(guard.verify_forever(std::time::Duration::from_millis(25)));
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let re_verified = sqlx::query_file_scalar!("sql/tests/recent_store_statements.sql")
        .fetch_one(store.as_driver())
        .await
        .expect("the activity probe");
    assert!(
        !recheck.is_finished(),
        "the held lock keeps the loop running"
    );
    assert!(
        re_verified > 0,
        "the loop re-probes on its cadence: no other session of this database \
         ran a statement inside the window"
    );
    recheck.abort();
    let _ = recheck.await;
    db.drop_db().await;
}

/// The live half of the holder lookup: a session that named no
/// `application_name` still has an address, and the lookup has to render
/// exactly that — the address-only arm `describe_holder`'s unit test covers by
/// hand, here over a row `pg_stat_activity` really produced.
///
/// The session is opened raw on purpose: `Dsn::parse` stamps this crate's
/// `application_name` on every session it builds, so only a connection made
/// outside it stands in for an unnamed holder.
///
/// What this still does not produce is the *unnameable* shape (both columns
/// `NULL`): measured on PostgreSQL 18, a session that never sent an
/// `application_name` reports the empty string, and the `NULL` pair is
/// PostgreSQL's answer to a role that may not see another session's details —
/// which the fixture's own superuser role cannot become. The statement's
/// `coalesce(..., '')`s are what fold both shapes into the one this leg pins.
#[tokio::test]
async fn an_unnamed_live_holder_is_described_by_its_address() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    let mut holder = sqlx::PgConnection::connect(&db.dsn)
        .await
        .expect("an unnamed session connects");
    let addr: Option<String> = sqlx::query_scalar!(
        "SELECT host(client_addr) FROM pg_stat_activity WHERE pid = pg_backend_pid()"
    )
    .fetch_one(&mut holder)
    .await
    .expect("the session's own client address");
    let addr = addr.expect("a TCP session carries an address");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(tinio_store_pg::migrations::OWNERSHIP_LOCK_KEY)
        .execute(&mut holder)
        .await
        .expect("the unnamed session takes the ownership lock");

    let named = store
        .ownership_holder()
        .await
        .expect("the lookup")
        .expect("the unnamed session holds the lock");
    assert!(
        named.starts_with("the session at ") && named.contains(&addr),
        "an unnamed holder is described by its address {addr}: {named}"
    );
    assert!(
        !named.contains("application_name"),
        "the unnamed session's row names no application to report: {named}"
    );
    drop(holder);
    db.drop_db().await;
}

#[tokio::test]
async fn the_ownership_lookup_is_scoped_to_this_database() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let store = db.store().await.expect("migrated");
    let _owner = store
        .hold_ownership()
        .await
        .expect("the first writer owns it");
    // Advisory locks are per-database: the same key held next door is a
    // different lock, so the neighbour must not be named as this database's
    // owner — and a database with no owner reports none.
    let neighbour_db = TestDb::create().await.expect("a second scratch database");
    let neighbour = neighbour_db.store().await.expect("the neighbour migrates");
    assert_eq!(
        neighbour.ownership_holder().await.expect("the lookup"),
        None,
        "another database's holder is not this database's"
    );
    assert!(
        store
            .ownership_holder()
            .await
            .expect("the lookup")
            .is_some(),
        "this database's owner is named"
    );
    db.drop_db().await;
    neighbour_db.drop_db().await;
}

#[tokio::test]
async fn the_ownership_lookup_ignores_the_two_int_lock_form() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    // `pg_advisory_lock(int4, int4)` records the same halves in
    // classid/objid and is a *different* lock (`objsubid` 2 against the
    // one-key form's 1), so it must not be read as this database's
    // ownership holder.
    let key = tinio_store_pg::migrations::OWNERSHIP_LOCK_KEY;
    let mut other = sqlx::PgConnection::connect(&db.dsn)
        .await
        .expect("the second session connects");
    sqlx::query("SELECT pg_advisory_lock($1, $2)")
        .bind(((key >> 32) & 0xffff_ffff) as i32)
        .bind((key & 0xffff_ffff) as i32)
        .execute(&mut other)
        .await
        .expect("the two-int lock is taken");
    assert_eq!(
        store.ownership_holder().await.expect("the lookup"),
        None,
        "the two-int form is a different lock"
    );
    drop(other);
    db.drop_db().await;
}

/// What a refused connect may say on the way out, and what it must never say:
/// the endpoint it could not reach, or the pool's own acquire timing out behind
/// a dead port — and in neither form the password the dsn carried (PG5).
#[tokio::test]
async fn a_connect_failure_never_prints_the_password() {
    if !require_dsn() {
        return;
    }
    let err = PgStore::connect("postgres://tinio:sup3rsecret@127.0.0.1:1/tinio")
        .await
        .expect_err("a dead port cannot connect");
    let message = err.to_string();
    match &err {
        Error::Connect { host, database, .. } => {
            assert_eq!((host.as_str(), database.as_str()), ("127.0.0.1", "tinio"));
        }
        // The port answers nothing at all on the machines this crate is
        // developed on, so the acquire is what runs out first.
        Error::Database(message) => assert!(message.contains("timed out"), "{message}"),
        other => panic!("a dead port is a connection failure, got {other}"),
    }
    assert!(!message.contains("sup3rsecret"), "{message}");
}

#[tokio::test]
async fn an_absent_database_names_the_host_and_database_only() {
    if !require_dsn() {
        return;
    }
    let base = crate::tests::db::base_dsn().expect("checked above");
    let dsn = crate::tests::db::swap_database(&base, "tinio_p3_absent_database")
        .expect("the dsn rewrites");
    let err = PgStore::connect(&dsn)
        .await
        .expect_err("an absent database cannot connect");
    let message = err.to_string();
    assert!(message.contains("tinio_p3_absent_database"), "{message}");
    assert!(!message.contains("tinio:tinio@"), "{message}");
}

#[tokio::test]
async fn inspect_reports_without_migrating() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let inspected = PgStore::inspect(&db.dsn, 1, Access::ReadOnly)
        .await
        .expect("the inspection path connects");
    assert_eq!(
        inspected.schema().applied,
        0,
        "an unmigrated database reads 0"
    );
    assert!(!inspected.schema().matches());
    assert_eq!(inspected.schema().pending(), SCHEMA_VERSION);
    assert_eq!(inspected.ssl_mode(), tinio_store_pg::pool::SslMode::Disable);
    // The inspection did not migrate it.
    let inspected = PgStore::inspect(&db.dsn, 1, Access::ReadOnly)
        .await
        .expect("still unmigrated");
    assert_eq!(inspected.schema().applied, 0);
    db.drop_db().await;
}

/// A read-only inspection is the one path that reports a database **ahead**
/// of the binary instead of refusing it: `doctor` has to be able to say what
/// it found, and the path never migrates, so nothing is at stake. The same
/// state on either writable path is `Error::SchemaTooNew`.
#[tokio::test]
async fn a_read_only_inspection_reports_a_newer_database_rather_than_refusing_it() {
    let Some((db, store)) = scratch().await else {
        return;
    };
    sqlx::query_file!(
        "sql/tests/plant_future_version.sql",
        (SCHEMA_VERSION + 1) as i32
    )
    .execute(store.as_driver())
    .await
    .expect("the plant");
    let inspected = PgStore::inspect(&db.dsn, 1, Access::ReadOnly)
        .await
        .expect("a read-only inspection only reports");
    assert_eq!(
        inspected.schema(),
        tinio_store_pg::SchemaState {
            applied: SCHEMA_VERSION + 1,
            supported: SCHEMA_VERSION
        }
    );
    assert!(!inspected.schema().matches());
    assert_eq!(
        inspected.schema().pending(),
        0,
        "a database ahead of the binary pends nothing rather than going negative"
    );
    db.drop_db().await;
}

#[tokio::test]
async fn a_writable_inspection_passes_the_same_schema_gate_as_connect() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // Behind the binary: the read-only inspection reports, the writable one
    // refuses — the inspecting path never migrates, so it cannot hand out a
    // writable store the schema gate has not passed.
    let err = PgStore::inspect(&db.dsn, 1, Access::ReadWrite)
        .await
        .expect_err("a writable inspection of an unmigrated database is refused");
    assert!(
        matches!(err, Error::SchemaBehind { found: 0, supported } if supported == SCHEMA_VERSION),
        "{err}"
    );

    let store = db.store().await.expect("the migration");
    PgStore::inspect(&db.dsn, 1, Access::ReadWrite)
        .await
        .expect("a database at the binary's version opens");
    // Newer than the binary: refused with the pg remedy, as `connect` does.
    sqlx::query_file!(
        "sql/tests/plant_future_version.sql",
        (SCHEMA_VERSION + 1) as i32
    )
    .execute(store.as_driver())
    .await
    .expect("the plant");
    let err = PgStore::inspect(&db.dsn, 1, Access::ReadWrite)
        .await
        .expect_err("a newer schema is refused");
    assert!(matches!(err, Error::SchemaTooNew { .. }), "{err}");
    db.drop_db().await;
}

#[tokio::test]
async fn the_pool_is_sized_and_reports_the_negotiated_mode() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let pool = Pool::open(&db.dsn, 3).expect("the dsn parses");
    assert_eq!(pool.max_connections(), 3);
    assert_eq!(pool.ssl_mode(), tinio_store_pg::pool::SslMode::Disable);
    assert_eq!(pool.database(), db.name);
    assert_eq!(
        pool.size(),
        (3, 0),
        "a lazily-opened pool holds no idle connection yet"
    );
    // What `connect` leaves behind: a working pool whose one used connection
    // is idle again, and a store that borrows it.
    let store = PgStore::connect_with_pool(pool, Access::ReadWrite)
        .await
        .expect("migrates");
    assert_eq!(store.database(), db.name);
    assert!(
        store.pool().num_idle() >= 1,
        "the connect's own session is idle in the pool"
    );
    assert!(!store.pool().is_closed());
    store.pool().close().await;
    assert!(store.pool().is_closed());
    db.drop_db().await;
}
#[tokio::test]
async fn a_require_dsn_never_downgrades_to_plaintext() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    // The scratch database lives on a plaintext server; the mode the dsn
    // asks for is require, so the connect must fail naming TLS rather than
    // fall back to plaintext.
    let dsn = db.dsn_with_mode("require");
    let err = PgStore::inspect(&dsn, 1, Access::ReadOnly)
        .await
        .expect_err("a plaintext server cannot answer a require dsn");
    let message = err.to_string();
    assert!(message.to_lowercase().contains("tls"), "{message}");
    // The refusal is the dsn asking for a mode the server cannot offer;
    // it is not a silent downgrade, so a plaintext dsn still works.
    let plaintext = db.dsn_with_mode("disable");
    PgStore::inspect(&plaintext, 1, Access::ReadOnly)
        .await
        .expect("the same server answers a plaintext dsn");
    db.drop_db().await;
}
