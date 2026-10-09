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

use std::sync::atomic::{AtomicU64, Ordering};

use tinio_store_pg::{Access, Error, PgStore, pool::Pool};

/// The base dsn the database-backed tests connect with.
pub fn base_dsn() -> Option<String> {
    std::env::var("TINIO_PG_DSN").ok()
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

/// A scratch database, dropped when the test asks.
pub struct TestDb {
    maintenance: String,
    pub name: String,
    pub dsn: String,
}

impl TestDb {
    /// Create a fresh scratch database derived from the base dsn, with the
    /// cluster's own locale.
    pub async fn create() -> Result<Self, String> {
        Self::create_with("").await
    }

    /// Create a fresh scratch database with an explicit `CREATE DATABASE` locale
    /// clause (for example `LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'`).
    /// The collation leg uses it to prove the pin does not depend on the
    /// database's own default.
    pub async fn create_with(locale: &str) -> Result<Self, String> {
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
        create_database(&maintenance, &name, locale).await?;
        Ok(Self {
            maintenance,
            name,
            dsn,
        })
    }

    /// Create a fresh scratch database whose `server_encoding` is not UTF-8
    /// (`LATIN1` with the `C` locales, so the encoding is the only difference):
    /// the leg that proves the store refuses such a server rather than serving
    /// a `COLLATE "C"` order of the wrong bytes (PG4).
    pub async fn create_latin1() -> Result<Self, String> {
        Self::create_with("ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C'").await
    }

    /// Connect a writable store to this database (migrating it).
    ///
    /// The pool is deliberately smaller than [`DEFAULT_POOL_SIZE`]: every leg
    /// opens its own store, and `cargo test` runs many legs at once, so the
    /// serving default (8 per store) exhausts the server's connection limit
    /// before the suite is half done. No leg needs more than one connection at
    /// a time.
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

    /// Drop the scratch database.
    pub async fn drop_db(&self) {
        let _ = drop_database(&self.maintenance, &self.name).await;
    }
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
fn scratch_statement(template: &str, name: &str, extra: &[(&str, &str)]) -> String {
    assert!(
        !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-'),
        "a scratch database name is generated, never arbitrary: {name:?}"
    );
    let mut rendered = template.replace("{name}", name);
    for (key, value) in extra {
        rendered = rendered.replace(&format!("{{{key}}}"), value);
    }
    rendered
}

async fn create_database(maintenance: &str, name: &str, locale: &str) -> Result<(), String> {
    use tinio_store_pg::_sqlx::PgPool;
    let pool = PgPool::connect(maintenance)
        .await
        .map_err(|err| format!("connect: {err}"))?;
    // CREATE DATABASE cannot run inside a transaction, so it goes over the
    // simple query path rather than as a prepared statement.
    let statement = if locale.is_empty() {
        scratch_statement(
            include_str!("../../sql/tests/create_database.sql"),
            name,
            &[],
        )
    } else {
        scratch_statement(
            include_str!("../../sql/tests/create_database_locale.sql"),
            name,
            &[("locale", locale)],
        )
    };
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
        &[],
    ));
    let _ = sqlx::raw_sql(terminate).execute(&pool).await;
    let drop_statement = sqlx::AssertSqlSafe(scratch_statement(
        include_str!("../../sql/tests/drop_database.sql"),
        name,
        &[],
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
        let rendered = scratch_statement(
            include_str!("../../sql/tests/create_database.sql"),
            name,
            &[],
        );
        assert!(rendered.contains(&format!("\"{name}\"")), "{rendered}");
        assert!(rendered.contains("TEMPLATE template0"), "{rendered}");
        assert!(!rendered.contains("{name}"), "{rendered}");
    }

    #[test]
    fn a_locale_statement_splices_the_locale_clause() {
        let rendered = scratch_statement(
            include_str!("../../sql/tests/create_database_locale.sql"),
            "tinio_p3_1_2_3",
            &[("locale", "LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'")],
        );
        assert!(
            rendered.contains("LOCALE_PROVIDER builtin BUILTIN_LOCALE 'C.UTF-8'"),
            "{rendered}"
        );
        assert!(!rendered.contains("{locale}"), "{rendered}");
    }

    #[test]
    fn a_drop_statement_fills_in_the_name() {
        let rendered = scratch_statement(
            include_str!("../../sql/tests/drop_database.sql"),
            "tinio_p3_1_2_3",
            &[],
        );
        assert!(rendered.contains("tinio_p3_1_2_3"), "{rendered}");
        assert!(!rendered.contains("{name}"), "{rendered}");
    }
}
