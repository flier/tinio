//! The read-only probes an operator's diagnosis asks a database for.
//!
//! `tinio-pg`'s doctor renders the rows and owns every remedy and severity; the statements live
//! here, in the crate that owns SQL, and are macro-checked so the database-free build pins them
//! through `.sqlx/` (`docs/style.md` §SQL). Nothing in this module writes, and nothing here takes
//! a lock: a diagnosis must not mutate its subject (PG11).

use sqlx::{query_file_as, query_file_scalar};

use crate::{Error, pool::Pool};

/// The role the inspecting session runs as, and whether it can write (PG6).
///
/// The privilege layer PG6 relies on under the read-only entry guard is exactly this: a role
/// without `INSERT`/`UPDATE`/`DELETE` on the store's tables. `can_write` is the answer for **any**
/// relation of **any** schema the store's relations live in — one relation a reader can write is
/// the guarantee broken.
///
/// # Examples
///
/// ```
/// use tinio_store_pg::diagnostics::RolePrivileges;
///
/// let privileges = RolePrivileges {
///     role: "tinio_reader".to_owned(),
///     schemas: "public".to_owned(),
///     can_write: false,
/// };
/// assert_eq!(privileges.role, "tinio_reader");
/// assert_eq!(privileges.schemas, "public");
/// assert!(!privileges.can_write);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolePrivileges {
    /// The role the session runs as.
    pub role: String,
    /// The schemas the probe scoped to — every schema holding the store's own
    /// bookkeeping table — comma-separated in name order. **Empty when the
    /// database holds none**, which is not the same answer as "no grants": there
    /// was nothing to check, and a diagnosis must say so rather than print a
    /// green row naming a schema with none of the store's tables (the
    /// 2026-10-13 review).
    pub schemas: String,
    /// Whether the role holds `INSERT`, `UPDATE` or `DELETE` on any relation of
    /// those schemas.
    pub can_write: bool,
}

/// The server encoding, as `current_setting('server_encoding')` reports it (PG4).
///
/// The same statement the store's own connect-time pin runs (`sql/store/server_encoding.sql`):
/// one text with one home, read by both call sites.
///
/// # Examples
///
/// Against a live server this prints the database's own `server_encoding` — `UTF8` for one a pg
/// deployment may serve, anything else for one PG4 refuses.
///
/// ```no_run
/// use tinio_store_pg::{diagnostics, pool::Pool};
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 1)?;
/// // A live server reports its own encoding, e.g. `UTF8`.
/// let encoding = diagnostics::server_encoding(&pool).await?;
/// println!("{encoding}");
/// # Ok(())
/// # }
/// ```
pub async fn server_encoding(pool: &Pool) -> Result<String, Error> {
    Ok(query_file_scalar!("sql/store/server_encoding.sql")
        .fetch_one(pool)
        .await?)
}

/// The inspecting role's own privileges (PG6), from
/// `sql/diagnostics/role_privileges.sql`.
///
/// The probe scopes to the **store's bookkeeping table**, not the session's `search_path`:
/// `current_schema()` answers for whichever schema the session resolves first, so a session
/// opened with `options=-c search_path=…` that does not name the store's schema read zero rows,
/// `bool_or` answered NULL, the `coalesce` printed `false`, and the diagnosis certified a role it
/// had not looked at (the 2026-10-13 review). Every schema holding a `schema_version` table is a
/// schema this store owns relations in, and [`RolePrivileges::schemas`] names them all, so a
/// reader can see what was checked.
///
/// The relation kinds are the ones a role could still write through: an ordinary table, a
/// partitioned table, a view (DML on an automatically updatable view writes through to its base
/// table) and a foreign table.
///
/// # Examples
///
/// Against a live server this prints the inspecting role, the schemas the probe scoped to and
/// whether that role can write — the three facts PG6's row is built from.
///
/// ```no_run
/// use tinio_store_pg::{diagnostics, pool::Pool};
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 1)?;
/// let privileges = diagnostics::role_privileges(&pool).await?;
/// println!("{} writes: {}", privileges.role, privileges.can_write);
/// # Ok(())
/// # }
/// ```
pub async fn role_privileges(pool: &Pool) -> Result<RolePrivileges, Error> {
    let row = query_file_as!(RolePrivileges, "sql/diagnostics/role_privileges.sql")
        .fetch_one(pool)
        .await?;
    Ok(row)
}

/// The current backend's TLS state, as the server itself reports it (PG10).
///
/// `pg_stat_ssl` joined to `pg_backend_pid()` answers for **this** connection — the one the pool
/// handed out — so the reading is a fact about the session, not a restatement of the dsn. The
/// server is the authority because the dsn only says what was asked for: a `disable` dsn against a
/// server that would have accepted TLS is a plaintext session nonetheless, and only the server can
/// say whether the certificate the store verified was the one it expected.
///
/// # Examples
///
/// ```
/// use tinio_store_pg::diagnostics::TlsState;
///
/// // A plaintext session: the server leaves `version`, `cipher` and `bits` NULL.
/// let plaintext = TlsState {
///     enabled: false,
///     version: None,
///     cipher: None,
///     bits: None,
/// };
/// assert!(!plaintext.enabled);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsState {
    /// Whether this session is encrypted at all.
    pub enabled: bool,
    /// The negotiated protocol version (e.g. `TLSv1.3`), `NULL` on a plaintext session.
    pub version: Option<String>,
    /// The negotiated cipher suite, `NULL` on a plaintext session.
    pub cipher: Option<String>,
    /// The cipher's key length in bits, `NULL` on a plaintext session.
    pub bits: Option<i32>,
}

/// The current backend's TLS state (PG10), from `sql/diagnostics/tls.sql`.
///
/// This is the server's own answer, not the dsn's: `enabled` is whether this session is encrypted
/// at all, and `version`/`cipher`/`bits` name what was negotiated when it is. PG10 shows the
/// *negotiated* mode, so a plaintext session is reported as plaintext however the policy resolved
/// — the policy says what was asked for, the server says what happened.
///
/// # Examples
///
/// Against a live server this prints whether the session is encrypted and, when it is, what was
/// negotiated — the fact PG10's row reports.
///
/// ```no_run
/// use tinio_store_pg::{diagnostics, pool::Pool};
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 1)?;
/// let tls = diagnostics::tls_state(&pool).await?;
/// println!("encrypted: {}", tls.enabled);
/// # Ok(())
/// # }
/// ```
pub async fn tls_state(pool: &Pool) -> Result<TlsState, Error> {
    let row = query_file_as!(TlsState, "sql/diagnostics/tls.sql")
        .fetch_one(pool)
        .await?;
    Ok(row)
}

#[cfg(test)]
#[path = "diagnostics_db_tests.rs"]
mod db_tests;
