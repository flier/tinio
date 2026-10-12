//! The database-backed tests' harness: one scratch database per test.
//!
//! Every db-backed test is a distinct database, so the suite is safe under
//! `cargo test`'s parallelism (the migration table, the ownership lock and the
//! conformance buckets never collide). The base dsn comes from `TINIO_PG_DSN`;
//! a missing variable skips the test rather than failing the workspace gate,
//! and no credential is ever printed (PG5).
//!
//! The harness addresses the crate and the standard library by absolute path:
//! a shortened path would not prove the path an external caller uses.
#![allow(clippy::absolute_paths)]
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tinio_store_pg::{Access, Error, PgStore, pool::Pool};

/// The base dsn the database-backed tests connect with.
///
/// Trimmed, with an all-whitespace value read as unset: the variable's one reader is here, so
/// a trailing newline from a `.env` file or a shell export is normalized once rather than
/// reaching the driver as part of the dsn on some legs and not on others.
pub fn base_dsn() -> Option<String> {
    std::env::var("TINIO_PG_DSN")
        .ok()
        .map(|dsn| dsn.trim().to_owned())
        .filter(|dsn| !dsn.is_empty())
}

/// Whether the database-backed legs can run: `false`, with the skip notice,
/// when `TINIO_PG_DSN` is unset, so a leg returns instead of failing.
pub fn require_dsn() -> bool {
    if base_dsn().is_none() {
        eprintln!("skipping: TINIO_PG_DSN is not set");
        return false;
    }
    true
}

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// A scratch database, dropped when the test asks — and, if the test never asks
/// because it returned early, `?`ed out, or panicked, by its [`Drop`] backstop.
#[derive(Debug)]
pub struct TestDb {
    maintenance: String,
    pub name: String,
    pub dsn: String,
    /// Set once the database has been handed to a drop, so [`TestDb::drop_db`] and
    /// the backstop never both run.
    dropped: AtomicBool,
}

impl TestDb {
    /// Create a fresh scratch database derived from the base dsn, with the
    /// cluster's own locale.
    pub async fn create() -> Result<Self, String> {
        Self::create_with(CreateStatement::Plain).await
    }

    /// Create a fresh scratch database with one of the harness's `CREATE DATABASE`
    /// shapes (the plain one, the latin-1 fixture, or the builtin-locale one).
    pub async fn create_with(statement: CreateStatement) -> Result<Self, String> {
        let base = base_dsn().ok_or("TINIO_PG_DSN is not set")?;
        let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "tinio_p3_{}_{}_{}",
            std::process::id(),
            sequence,
            tinio_core::storage::now_nanos()
        );
        let maintenance = swap_database(&base, "postgres")?;
        let dsn = swap_database(&base, &name)?;
        create_database(&maintenance, &name, statement).await?;
        Ok(Self {
            maintenance,
            name,
            dsn,
            dropped: AtomicBool::new(false),
        })
    }

    /// Create a fresh scratch database whose `server_encoding` is not UTF-8
    /// (`LATIN1` with the `C` locales, so the encoding is the only difference):
    /// the leg that proves the store refuses such a server rather than serving
    /// a `COLLATE "C"` order of the wrong bytes (PG4).
    pub async fn create_latin1() -> Result<Self, String> {
        Self::create_with(CreateStatement::Latin1).await
    }

    /// Create a fresh scratch database whose default collation is the **builtin
    /// `C.UTF-8` locale** rather than the cluster's: the leg that proves the
    /// key order comes from the columns' pinned `COLLATE "C"` and not from the
    /// database's own default.
    ///
    /// `LOCALE_PROVIDER`/`BUILTIN_LOCALE` is an sqruff dialect gap (the two
    /// clauses are what its postgres dialect cannot parse), so
    /// `sql/tests/create_database_builtin_locale.sql` is the one statement in
    /// the crate sqruff does not check — and the SQL job stays green only
    /// because it lints without `--parsing-errors`. The corpus is otherwise
    /// unchecked nowhere, and the fixture is exercised by
    /// `the_order_does_not_move_with_the_database_locale`.
    pub async fn create_builtin_locale() -> Result<Self, String> {
        Self::create_with(CreateStatement::BuiltinLocale).await
    }

    /// Connect a writable store to this database (migrating it).
    ///
    /// The pool is deliberately smaller than `DEFAULT_POOL_SIZE` (the serving
    /// default, `tinio-config`'s): every leg opens its own store, and
    /// `cargo test` runs many legs at once, so the serving default (8 per
    /// store) exhausts the server's connection limit before the suite is half
    /// done. No leg needs more than one connection at a time.
    pub async fn store(&self) -> Result<PgStore, Error> {
        let pool = Pool::open(&self.dsn, 4)?;
        PgStore::connect_with_pool(pool, Access::ReadWrite).await
    }

    /// Connect a read-only store to this database.
    pub async fn read_only(&self) -> Result<PgStore, Error> {
        let pool = Pool::open(&self.dsn, 4)?;
        PgStore::connect_with_pool(pool, Access::ReadOnly).await
    }

    /// This database's dsn with an explicit `sslmode`, replacing whatever the
    /// base dsn asked for.
    pub fn dsn_with_mode(&self, mode: &str) -> String {
        let (base, _query) = match self.dsn.split_once('?') {
            Some((base, query)) => (base, Some(query)),
            None => (self.dsn.as_str(), None),
        };
        format!("{base}?sslmode={mode}")
    }

    /// Drop the scratch database. This is the deterministic drop a test takes on its
    /// success path; it marks the database so the [`Drop`] backstop does not repeat it.
    pub async fn drop_db(&self) {
        self.dropped.store(true, Ordering::Relaxed);
        let _ = drop_database(&self.maintenance, &self.name).await;
    }
}

/// The backstop for a test that never reaches [`TestDb::drop_db`]: an early `return`,
/// a `?`, or a panic left the scratch database behind, which is exactly how the
/// `tinio_p3_*` databases measured on the live fixture were leaked.
///
/// `Drop` cannot await, and handing the work to the current runtime does **not** work
/// for these legs: the default `#[tokio::test]` runtime is single-threaded and is
/// dropped as soon as the test body returns, so a task spawned from `Drop` would be
/// scheduled but never polled. The cleanup therefore runs on a thread with its own
/// runtime, which drops the database regardless of the test's runtime.
///
/// **The thread is joined, not detached.** A detached thread outlives the test but not
/// the process: a run whose last leg failed exits before its `DROP DATABASE` connects,
/// which is the leak this backstop exists to close (measured — the count returned to
/// `tinio` alone only once `Drop` joined). The join blocks the calling thread for one
/// `DROP DATABASE`, which a test-local `Drop` may do, and it discards every error — a
/// cleanup must never turn a test's own failure into a different one, and must never
/// panic in `Drop` (a panic while unwinding aborts the process).
impl Drop for TestDb {
    fn drop(&mut self) {
        if self.dropped.swap(true, Ordering::Relaxed) {
            return;
        }
        let maintenance = self.maintenance.clone();
        let name = self.name.clone();
        let cleanup = std::thread::Builder::new()
            .name("tinio-scratch-drop".to_owned())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                let _ = runtime.block_on(drop_database(&maintenance, &name));
            });
        if let Ok(cleanup) = cleanup {
            let _ = cleanup.join();
        }
    }
}

/// Truncate every table the schema owns except the migration bookkeeping — the
/// reset a shared database needs between scenarios (the `@pg` e2e pass).
///
/// The statement is built here, in the crate that owns the schema: the
/// bookkeeping table's name is [`MIGRATION_TABLE`](crate::migrations::MIGRATION_TABLE),
/// so a migration that renames it or adds a table cannot leave rows behind for
/// a caller that spelled the name itself. One `DO` block, so the whole reset is
/// one round trip, and `CASCADE`, so foreign keys never block it.
pub async fn truncate_all(conn: &mut tinio_store_pg::_sqlx::PgConnection) -> Result<(), Error> {
    use crate::migrations::MIGRATION_TABLE;

    let statement = format!(
        "DO $$\nDECLARE t text;\nBEGIN\n    FOR t IN SELECT tablename FROM pg_tables \
         WHERE schemaname = 'public' AND tablename <> '{MIGRATION_TABLE}'\n    LOOP\n        \
         EXECUTE format('TRUNCATE TABLE %I CASCADE', t);\n    END LOOP;\nEND $$"
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
        .execute(conn)
        .await
        .map_err(Error::from)?;
    Ok(())
}

/// Replace the database component of a URL dsn, keeping userinfo intact.
pub fn swap_database(dsn: &str, database: &str) -> Result<String, String> {
    let (scheme, rest) = dsn
        .split_once("://")
        .ok_or_else(|| format!("the dsn is not a URL: {dsn}"))?;
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    let query = rest[authority_end..]
        .split_once('?')
        .map(|(_, query)| format!("?{query}"))
        .unwrap_or_default();
    Ok(format!("{scheme}://{authority}/{database}{query}"))
}

/// Render one of the scratch-database statements by substituting its
/// placeholders.
///
/// The templates live in `sql/tests/` and are the only SQL these helpers run.
/// `CREATE DATABASE` and `DROP DATABASE` cannot carry a bound parameter, so the
/// name is spliced in -- and asserted to be ASCII alphanumerics, `_` and `-`
/// only, which is what `TestDb::create` generates and which makes the splice
/// safe for the quoting the templates use.
fn scratch_statement(template: &str, name: &str) -> String {
    assert!(
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "a scratch database name is generated, never arbitrary: {name:?}"
    );
    template.replace("{name}", name)
}

/// A scratch database's `CREATE DATABASE` shape: the plain one (the cluster's own
/// locale), the latin-1 fixture, and the builtin-locale fixture. The three are the
/// whole set the harness ever creates, so the statement each names is a literal
/// file rather than a clause spliced into one — the shape that made the two
/// fixtures parsable and the ignore list empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateStatement {
    /// The cluster's own locale: the statement every ordinary leg uses.
    Plain,
    /// `LATIN1` with the `C` locales (the non-UTF-8 server leg).
    Latin1,
    /// The builtin `C.UTF-8` locale (the collation leg).
    BuiltinLocale,
}

impl CreateStatement {
    /// The statement text this shape renders, with `{name}` still to be spliced.
    fn text(self) -> &'static str {
        match self {
            Self::Plain => include_str!("../../sql/tests/create_database.sql"),
            Self::Latin1 => include_str!("../../sql/tests/create_database_latin1.sql"),
            Self::BuiltinLocale => {
                include_str!("../../sql/tests/create_database_builtin_locale.sql")
            }
        }
    }
}

async fn create_database(
    maintenance: &str,
    name: &str,
    statement: CreateStatement,
) -> Result<(), String> {
    use tinio_store_pg::_sqlx::PgPool;
    let pool = PgPool::connect(maintenance)
        .await
        .map_err(|err| format!("connect: {err}"))?;
    // CREATE DATABASE cannot run inside a transaction, so it goes over the
    // simple query path rather than as a prepared statement.
    let statement = scratch_statement(statement.text(), name);
    sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
        .execute(&pool)
        .await
        .map_err(|err| format!("create database: {err}"))?;
    pool.close().await;
    Ok(())
}

async fn drop_database(maintenance: &str, name: &str) -> Result<(), String> {
    use tinio_store_pg::_sqlx::PgPool;
    let pool = PgPool::connect(maintenance)
        .await
        .map_err(|err| format!("connect: {err}"))?;
    let terminate = sqlx::AssertSqlSafe(scratch_statement(
        include_str!("../../sql/tests/terminate_connections.sql"),
        name,
    ));
    let _ = sqlx::raw_sql(terminate).execute(&pool).await;
    let drop_statement = sqlx::AssertSqlSafe(scratch_statement(
        include_str!("../../sql/tests/drop_database.sql"),
        name,
    ));
    let dropped = sqlx::raw_sql(drop_statement).execute(&pool).await;
    pool.close().await;
    dropped.map_err(|err| format!("drop database: {err}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{scratch_statement, swap_database};

    #[test]
    fn swapping_keeps_the_userinfo_and_the_query() {
        assert_eq!(
            swap_database(
                "postgres://tinio:pw@127.0.0.1:55432/tinio?sslmode=disable",
                "scratch"
            )
            .unwrap(),
            "postgres://tinio:pw@127.0.0.1:55432/scratch?sslmode=disable"
        );
        assert_eq!(
            swap_database("postgres://tinio:pw@127.0.0.1:55432/tinio", "postgres").unwrap(),
            "postgres://tinio:pw@127.0.0.1:55432/postgres"
        );
    }

    #[test]
    fn a_scratch_statement_splices_the_generated_nanosecond_name() {
        let name = "tinio_p3_18184_0_1759916892000000000";
        let rendered = scratch_statement(include_str!("../../sql/tests/create_database.sql"), name);
        assert!(rendered.contains(&format!("\"{name}\"")), "{rendered}");
        assert!(rendered.contains("TEMPLATE template0"), "{rendered}");
        assert!(!rendered.contains("{name}"), "{rendered}");
    }

    /// Every `CREATE DATABASE` shape the harness renders takes the name and
    /// nothing else: the two fixtures the collation and encoding legs create
    /// with spelled their clauses out when the ignore list was emptied, so the
    /// only placeholder left in any of the three files is `{name}`.
    #[test]
    fn each_create_statement_splices_only_the_name() {
        for (file, text) in [
            (
                "create_database.sql",
                include_str!("../../sql/tests/create_database.sql"),
            ),
            (
                "create_database_latin1.sql",
                include_str!("../../sql/tests/create_database_latin1.sql"),
            ),
            (
                "create_database_builtin_locale.sql",
                include_str!("../../sql/tests/create_database_builtin_locale.sql"),
            ),
        ] {
            let rendered = scratch_statement(text, "tinio_p3_1_2_3");
            assert!(
                rendered.contains("CREATE DATABASE \"tinio_p3_1_2_3\" TEMPLATE template0"),
                "{file}: {rendered}"
            );
            assert!(
                !rendered.contains('{'),
                "{file} keeps a placeholder: {rendered}"
            );
        }
    }

    /// The builtin-locale shape is the one statement sqruff's postgres dialect
    /// cannot parse, and the clauses are why: the file spells them out rather
    /// than splicing them, so the statement is its own fixture and the ignore
    /// list has no entry to hold it.
    #[test]
    fn the_builtin_locale_shape_carries_its_clauses() {
        let rendered = scratch_statement(
            include_str!("../../sql/tests/create_database_builtin_locale.sql"),
            "tinio_p3_1_2_3",
        );
        assert!(
            rendered.contains("LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'"),
            "{rendered}"
        );
        let latin1 = scratch_statement(
            include_str!("../../sql/tests/create_database_latin1.sql"),
            "tinio_p3_1_2_3",
        );
        assert!(
            latin1.contains("ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'"),
            "{latin1}"
        );
    }

    #[test]
    fn a_drop_statement_fills_in_the_name() {
        let rendered = scratch_statement(
            include_str!("../../sql/tests/drop_database.sql"),
            "tinio_p3_1_2_3",
        );
        assert!(rendered.contains("tinio_p3_1_2_3"), "{rendered}");
        assert!(!rendered.contains("{name}"), "{rendered}");
    }
}
