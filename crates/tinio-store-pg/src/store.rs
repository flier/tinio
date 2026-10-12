//! The store: its pool, its access mode, its schema gate and its ownership
//! guard.
//!
//! PG11-PG13: a writable `connect` migrates the database. The ownership lock is
//! [`PgStore::hold_ownership`] on a dedicated connection, not part of connect;
//! a read-only instance verifies the schema version, never migrates, and takes
//! no lock. A database newer than
//! the binary is refused with the pg remedy (a newer binary, or dump/restore),
//! never a rebuild.

use std::{
    process,
    time::{Duration, Instant},
};

use derive_more::Debug;
use parse_display::Display;
use sqlx::{
    Connection, PgConnection, PgPool, postgres::types::Oid, query, query_file_as,
    query_file_scalar, query_scalar,
};
use tokio::time;

use crate::{
    _core::storage::now_nanos,
    error::Error,
    migrations::{
        MIGRATION_LOCK_KEY, MIGRATION_TABLE, OWNERSHIP_LOCK_KEY, SCHEMA_VERSION, VERSION_TABLE_SQL,
        pending,
    },
    pool::{DEFAULT_POOL_SIZE, Pool, SslMode},
};

/// How a store instance serves (PG12): one writable instance owns a database,
/// and a read-only instance answers reads beside it.
///
/// # Examples
///
/// ```rust
/// use tinio_store_pg::Access;
///
/// assert_eq!(Access::ReadWrite.to_string(), "read-write");
/// assert!(!Access::ReadOnly.is_writable());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(style = "kebab-case")]
pub enum Access {
    /// The whole contract surface, and the ownership lock.
    ReadWrite,
    /// Reads only, no lock and no migration.
    ReadOnly,
}

impl Access {
    /// Whether this mode may mutate the database.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::Access;
    ///
    /// assert!(Access::ReadWrite.is_writable());
    /// assert!(!Access::ReadOnly.is_writable());
    /// ```
    pub fn is_writable(self) -> bool {
        matches!(self, Self::ReadWrite)
    }
}

/// What the database records and what this binary knows (PG11).
///
/// # Examples
///
/// ```
/// use tinio_store_pg::SchemaState;
///
/// let behind = SchemaState {
///     applied: 0,
///     supported: 1,
/// };
/// assert!(!behind.matches());
/// assert_eq!(behind.pending(), 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaState {
    /// The version the database records (0 when nothing is applied).
    pub applied: i64,
    /// The newest version this binary migrates to.
    pub supported: i64,
}

impl SchemaState {
    /// The migrations still to run.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::SchemaState;
    ///
    /// assert_eq!(
    ///     SchemaState {
    ///         applied: 0,
    ///         supported: 3
    ///     }
    ///     .pending(),
    ///     3
    /// );
    /// // A database ahead of the binary pends nothing rather than going negative.
    /// assert_eq!(
    ///     SchemaState {
    ///         applied: 4,
    ///         supported: 3
    ///     }
    ///     .pending(),
    ///     0
    /// );
    /// ```
    pub fn pending(&self) -> i64 {
        (self.supported - self.applied).max(0)
    }

    /// The state a database at this binary's version reports.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::SchemaState;
    ///
    /// assert!(SchemaState::current().matches());
    /// assert_eq!(SchemaState::current().pending(), 0);
    /// ```
    pub fn current() -> Self {
        Self {
            applied: SCHEMA_VERSION,
            supported: SCHEMA_VERSION,
        }
    }

    /// Whether the database and the binary agree.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::SchemaState;
    ///
    /// assert!(
    ///     SchemaState {
    ///         applied: 1,
    ///         supported: 1
    ///     }
    ///     .matches()
    /// );
    /// assert!(
    ///     !SchemaState {
    ///         applied: 0,
    ///         supported: 1
    ///     }
    ///     .matches()
    /// );
    /// ```
    pub fn matches(&self) -> bool {
        self.applied == self.supported
    }
}

/// The store: a pool, its access mode and the schema version it verified.
///
/// `Debug` renders the pool's identity through [`Pool`]'s own `Debug` and
/// never the dsn (PG5).
///
/// # Examples
///
/// ```no_run
/// use tinio_store_pg::PgStore;
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
/// // What this type's rendering promises: the pool's identity, never the
/// // dsn's password.
/// let rendered = format!("{store:?}");
/// assert!(!rendered.contains("password"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct PgStore {
    pool: Pool,
    access: Access,
    schema: SchemaState,
}

impl PgStore {
    /// Open a **writable** store: parse the dsn, verify the server encoding is
    /// UTF-8, and migrate the database forward under the migration lock.
    /// The ownership lock is [`Self::hold_ownership`], which the caller takes
    /// and re-checks separately (PG9/PG11/PG12).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::PgStore;
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// // Against a live server this migrates. The ownership lock is a later
    /// // `hold_ownership`, so two connects do not yet exclude each other.
    /// assert!(store.schema().matches());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(dsn: &str) -> Result<Self, Error> {
        let pool = Pool::open(dsn, DEFAULT_POOL_SIZE)?;
        Self::connect_with_pool(pool, Access::ReadWrite).await
    }

    /// Open a store over an already-built pool.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{Access, PgStore, pool::Pool};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 8)?;
    /// // Read-only verifies the schema version and refuses a mismatch; it
    /// // never migrates and takes no ownership lock.
    /// let store = PgStore::connect_with_pool(pool, Access::ReadOnly).await?;
    /// println!("{:?}", store.schema());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_pool(pool: Pool, access: Access) -> Result<Self, Error> {
        schema_gate(&pool, access, SchemaGate::BringForward).await?;
        Ok(Self {
            pool,
            access,
            schema: SchemaState::current(),
        })
    }

    /// Open a store that never migrates: it reads the schema version and
    /// answers it. This is the pool-opening half of the path `doctor` uses
    /// (PG11) — a diagnostic must not change the database it inspects.
    ///
    /// The mode is the one the inspecting session is opened in — the pool is
    /// opened read-only for [`Access::ReadOnly`], so the session's own layer of
    /// the read-only mode (PG6) is present on the diagnostic path too, not just
    /// the reported flag — and the one [`PgStore::access`] reports, whatever
    /// the schema; it never migrates and never takes the ownership lock. A
    /// **writable** inspection is a writable store, so it passes the same
    /// schema gate [`PgStore::connect`] applies — and because this path never
    /// migrates, a database behind the binary is refused
    /// ([`Error::SchemaBehind`]) instead of migrated. A read-only inspection
    /// only reports, which is what `doctor` needs.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{Access, PgStore};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// // A read-only inspection reports whatever it finds, mismatches included,
    /// // and applies nothing (the schema gate above does not run).
    /// let inspected = PgStore::inspect(
    ///     "postgres://user:password@db.example:5432/tinio",
    ///     1,
    ///     Access::ReadOnly,
    /// )
    /// .await?;
    /// println!("applied {:?}", inspected.schema());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn inspect(dsn: &str, pool: u32, access: Access) -> Result<Self, Error> {
        // The session is opened in the asked-for mode: a read-only inspection
        // connects read-only, so a diagnostic run cannot write by accident
        // through the pool it inspects with (PG6).
        let pool = Pool::open_for(dsn, pool, access)?;
        Self::inspect_with_pool(pool, access).await
    }

    /// Inspect over an already-built pool, the non-migrating path [`Self::inspect`] takes over
    /// the pool it opens — the shape [`Self::connect_with_pool`] has. A caller that already
    /// holds the pool a diagnosis runs through hands it here rather than opening a second one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{Access, PgStore, pool::Pool};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 8)?;
    /// // The store inspects through the caller's own pool and never migrates it.
    /// let store = PgStore::inspect_with_pool(pool, Access::ReadOnly).await?;
    /// println!("applied {:?}", store.schema());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn inspect_with_pool(pool: Pool, access: Access) -> Result<Self, Error> {
        let applied = schema_gate(&pool, access, SchemaGate::ReportOnly).await?;
        Ok(Self {
            pool,
            access,
            schema: SchemaState {
                applied,
                supported: SCHEMA_VERSION,
            },
        })
    }

    /// Migrate the database forward under the migration lock, or refuse a
    /// database newer than this binary (PG11). `connect` already runs this on
    /// the read-write path; it is public so a caller that connected read-only
    /// or inspected can migrate deliberately.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::PgStore;
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// // Idempotent: a second run applies nothing and reports the same state,
    /// // because `connect` already migrated on the read-write path.
    /// let state = store.migrate().await?;
    /// assert!(state.matches());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn migrate(&self) -> Result<SchemaState, Error> {
        if !self.access.is_writable() {
            return Err(Error::Database("a read-only store never migrates".into()));
        }
        migrate(&self.pool).await?;
        Ok(SchemaState::current())
    }

    /// The database the store is connected to (never a secret).
    pub fn database(&self) -> &str {
        self.pool.database()
    }

    /// The TLS mode the dsn asked for.
    pub fn ssl_mode(&self) -> SslMode {
        self.pool.ssl_mode()
    }

    /// Borrow the store's pool. Crate-private, so a caller outside the four
    /// contracts cannot run statements through the store.
    pub(crate) fn pool(&self) -> &Pool {
        &self.pool
    }

    /// Borrow the driver's pool. Crate-private for the same reason as
    /// [`Self::pool`].
    pub(crate) fn as_driver(&self) -> &PgPool {
        &self.pool
    }

    /// The store's access mode.
    pub fn access(&self) -> Access {
        self.access
    }

    /// The schema version the store verified (PG11; doctor reports it).
    pub fn schema(&self) -> SchemaState {
        self.schema
    }

    /// Take the ownership lock on a dedicated out-of-pool connection (PG12).
    ///
    /// The guard holds the session-level advisory lock for the process's
    /// lifetime and re-verifies it on an interval. The lock is session-scoped,
    /// so a connection that dies has already released it: a failed recheck
    /// terminates the process, the same as a recheck that finds the lock gone
    /// (PG26: the driver exposes no keepalive knob, and the re-verify is also
    /// what keeps the connection's TCP path warm). A read-only store refuses
    /// to take the lock.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{PgStore, store::OWNERSHIP_RECHECK};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let guard = store.hold_ownership().await?;
    /// // The re-verify terminates the process if the lock is lost, so a serving
    /// // instance spawns it and keeps the guard for the process's lifetime.
    /// tokio::spawn(guard.verify_forever(OWNERSHIP_RECHECK));
    /// # Ok(())
    /// # }
    /// ```
    pub async fn hold_ownership(&self) -> Result<Ownership, Error> {
        if !self.access.is_writable() {
            return Err(Error::Database(
                "a read-only store never takes the ownership lock".into(),
            ));
        }
        let connection = connect_dedicated(&self.pool).await?;
        let mut ownership = Ownership {
            connection,
            key: OWNERSHIP_LOCK_KEY,
        };
        if !ownership.try_lock().await? {
            // The lock was held a moment ago, so a `None` here means the holder
            // released it between the attempt and this lookup.
            let holder = lock_holder(&self.pool, ownership.key)
                .await?
                .unwrap_or_else(|| "another connection".to_owned());
            return Err(Error::LockHeld {
                key: ownership.key,
                holder,
            });
        }
        Ok(ownership)
    }

    /// The session holding the ownership lock right now, in the words a second
    /// writable instance's refusal and `doctor` both report it (PG12); `None`
    /// when no session holds it, which is the answer `doctor` is asked for.
    ///
    /// PostgreSQL hides another session's identifying columns from a role that
    /// is neither a superuser nor a member of `pg_read_all_stats`; the answer
    /// says so rather than guessing.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::PgStore;
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// // The line `doctor` prints: the session holding the lock, or none.
    /// match store.ownership_holder().await? {
    ///     Some(holder) => println!("owned by {holder}"),
    ///     None => println!("this database has no writable instance"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ownership_holder(&self) -> Result<Option<String>, Error> {
        lock_holder(&self.pool, OWNERSHIP_LOCK_KEY).await
    }
}

/// A held ownership lock, released when the guard drops.
///
/// The lock is session-level, so the drop *is* the release: the driver closes
/// the dedicated connection, and PostgreSQL frees the advisory lock with it. No
/// `Drop` impl is needed for that, and none is written.
///
/// # Examples
///
/// ```no_run
/// use tinio_store_pg::PgStore;
///
/// # async fn example() -> Result<(), tinio_store_pg::Error> {
/// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
/// {
///     let guard = store.hold_ownership().await?;
///     assert_eq!(guard.key(), tinio_store_pg::migrations::OWNERSHIP_LOCK_KEY);
/// } // the guard drops here, and a second writable instance may take the lock
/// //
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Ownership {
    #[debug(skip)]
    connection: PgConnection,
    key: i64,
}

impl Ownership {
    /// The advisory lock's key.
    pub fn key(&self) -> i64 {
        self.key
    }

    /// Whether the lock is still held by this session.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::PgStore;
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let mut guard = store.hold_ownership().await?;
    /// // What the periodic re-verify asks: the lock is still this session's.
    /// assert!(guard.still_held().await?);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn still_held(&mut self) -> Result<bool, Error> {
        let (classid, objid) = key_parts(self.key);
        let held = query_file_scalar!("sql/store/ownership_recheck.sql", classid, objid)
            .fetch_one(&mut self.connection)
            .await?;
        Ok(held > 0)
    }

    /// Take the lock without waiting.
    async fn try_lock(&mut self) -> Result<bool, Error> {
        let locked = query_file_scalar!("sql/store/advisory_try_lock.sql", self.key)
            .fetch_one(&mut self.connection)
            .await?;
        Ok(locked)
    }

    /// Re-verify on the store's interval, terminating the process when the
    /// lock is gone or the recheck itself fails (PG12/PG26). A failed probe
    /// cannot prove the session lock is still held, and the advisory lock
    /// dies with the connection. The loop is the caller's to spawn.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use tinio_store_pg::{PgStore, store::OWNERSHIP_RECHECK};
    ///
    /// # async fn example() -> Result<(), tinio_store_pg::Error> {
    /// let store = PgStore::connect("postgres://user:password@db.example:5432/tinio").await?;
    /// let guard = store.hold_ownership().await?;
    /// // Never returns while the lock holds; a lost lock logs and exits with a
    /// // status the process supervisor can act on.
    /// guard.verify_forever(OWNERSHIP_RECHECK).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verify_forever(mut self, interval: Duration) -> Result<(), Error> {
        loop {
            time::sleep(interval).await;
            let probe = self.still_held().await;
            if !ownership_lost(&probe) {
                continue;
            }
            tracing::error!(
                key = self.key,
                error = probe
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .as_deref()
                    .unwrap_or("lock not held"),
                "the ownership lock was lost: terminating so a second writer cannot coexist"
            );
            process::exit(OWNERSHIP_LOST_EXIT);
        }
    }
}

/// A recheck that is not `Ok(true)` has lost the lock. The advisory lock is
/// session-scoped, so a dead connection has already released it; returning
/// the error would end the spawned task and leave the process writing.
fn ownership_lost<E>(probe: &Result<bool, E>) -> bool {
    !matches!(probe, Ok(true))
}

/// The exit status a lost ownership lock terminates with.
pub const OWNERSHIP_LOST_EXIT: i32 = 75;

/// One row of `sql/store/ownership_holder.sql`.
struct LockHolder {
    application_name: String,
    client_addr: String,
}

/// The session holding `key` in **this** database, or `None` when no session
/// does — as the refusal a second writable instance gets and
/// [`PgStore::ownership_holder`] both report it (PG12).
///
/// Advisory locks are per-database (the same key can be held in another
/// database of the same cluster without conflicting), so the statement scopes
/// the lookup to `current_database()`; without that, a neighbour's holder would
/// be named as this database's.
async fn lock_holder(pool: &Pool, key: i64) -> Result<Option<String>, Error> {
    let (classid, objid) = key_parts(key);
    let row = query_file_as!(LockHolder, "sql/store/ownership_holder.sql", classid, objid)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|row| describe_holder(&row)))
}

/// A 64-bit advisory lock's two 32-bit halves, as `pg_locks` records them: the
/// key's high 32 bits in `classid` and its low 32 bits in `objid` (both `oid`),
/// because `objid` alone cannot take the whole key. The two-int form of the
/// same halves is a *different* lock, and `objsubid` tells them apart —
/// measured on PostgreSQL 18: `1` for this one-key form, `2` for
/// `pg_try_advisory_lock(int4, int4)`, which the statements filter on.
fn key_parts(key: i64) -> (Oid, Oid) {
    let bits = key as u64;
    (Oid((bits >> 32) as u32), Oid(bits as u32))
}

/// Render the holding session from what `pg_stat_activity` showed of it.
fn describe_holder(holder: &LockHolder) -> String {
    match (
        holder.application_name.is_empty(),
        holder.client_addr.is_empty(),
    ) {
        (false, false) => format!(
            "the session at {} (application_name {})",
            holder.client_addr, holder.application_name
        ),
        (false, true) => format!("the session named {}", holder.application_name),
        (true, false) => format!("the session at {}", holder.client_addr),
        (true, true) => unidentifiable_holder(),
    }
}

/// The refusal PostgreSQL itself imposes: the holding session's row exists, but
/// the columns that name it are hidden from an ordinary role.
fn unidentifiable_holder() -> String {
    "a session this role cannot identify (its details need a superuser or pg_read_all_stats)"
        .to_owned()
}

/// Refuse a database whose applied schema is ahead of this binary, with the pg
/// remedy (PG11): a newer binary, or dump/restore — never a rebuild. The three
/// paths that read the version (connect, inspect, the migration runner) all
/// state it through this one rule.
fn refuse_a_newer_schema(applied: i64) -> Result<(), Error> {
    if applied > SCHEMA_VERSION {
        return Err(Error::SchemaTooNew {
            found: applied,
            supported: SCHEMA_VERSION,
        });
    }
    Ok(())
}

/// Verify the server encoding is UTF-8: COLLATE "C" orders the server
/// encoding's bytes, and the contracts order by the key's UTF-8 sequence
/// (PG4).
async fn verify_encoding(pool: &Pool) -> Result<(), Error> {
    let encoding: String = query_file_scalar!("sql/store/server_encoding.sql")
        .fetch_one(pool)
        .await?;
    if !encoding.eq_ignore_ascii_case("UTF8") {
        return Err(Error::Encoding { encoding });
    }
    Ok(())
}

/// The schema version the database records, or 0 when nothing is applied.
async fn read_schema_version(pool: &Pool) -> Result<i64, Error> {
    let present = query_scalar!(
        r#"SELECT to_regclass($1) IS NOT NULL AS "present!""#,
        MIGRATION_TABLE
    )
    .fetch_one(pool)
    .await?;
    if !present {
        return Ok(0);
    }
    let version = query_scalar!(r#"SELECT coalesce(max(id), 0) AS "version!" FROM schema_version"#)
        .fetch_one(pool)
        .await?;
    Ok(version as i64)
}

/// Which open path passes the schema gate — the one thing that decides what a
/// database which does not match this binary means.
#[derive(Debug, Clone, Copy)]
enum SchemaGate {
    /// [`PgStore::connect_with_pool`]: a writable open brings a database behind
    /// this binary forward, and a read-only one refuses anything but an exact
    /// match.
    BringForward,
    /// [`PgStore::inspect_with_pool`]: never migrates — a writable open refuses a
    /// database behind this binary instead ([`Error::SchemaBehind`]), and a
    /// read-only one reports whatever it finds (PG11).
    ReportOnly,
}

/// The encoding-and-schema gate both open paths pass: verify the encoding, read
/// the applied version, and — on the writable path — refuse a database newer
/// than this binary. What a database *behind* this binary means is the gate's
/// own argument, so the sequence has one text.
async fn schema_gate(pool: &Pool, access: Access, gate: SchemaGate) -> Result<i64, Error> {
    verify_encoding(pool).await?;
    let applied = read_schema_version(pool).await?;
    if access.is_writable() {
        refuse_a_newer_schema(applied)?;
        if applied < SCHEMA_VERSION {
            match gate {
                SchemaGate::BringForward => migrate(pool).await?,
                SchemaGate::ReportOnly => {
                    return Err(Error::SchemaBehind {
                        found: applied,
                        supported: SCHEMA_VERSION,
                    });
                }
            }
        }
    } else if matches!(gate, SchemaGate::BringForward) && applied != SCHEMA_VERSION {
        return Err(Error::SchemaMismatch {
            found: applied,
            supported: SCHEMA_VERSION,
        });
    }
    Ok(applied)
}

/// Apply every pending migration, each in its own transaction, under an
/// advisory lock held on a dedicated out-of-pool connection.
async fn migrate(pool: &Pool) -> Result<(), Error> {
    migrate_with_lock_wait(pool, MIGRATION_LOCK_WAIT).await
}

/// [`migrate`] with the migration lock's bounded wait made explicit, so the
/// database-backed test can pin the refusal with a wait it can afford.
/// Open the dedicated out-of-pool connection a lock lives on, with the dsn's
/// password stripped from the error.
///
/// The connection is opened **through the pool's own options**
/// ([`Pool::dedicated_options`]), not by re-parsing a mode-less dsn: a
/// dedicated connection therefore carries the pool's session, the read-only
/// startup option included (PG6), and cannot open with more privilege than the
/// pool that asked for it. Re-parsing the raw dsn here was the one way that
/// could happen.
async fn connect_dedicated(pool: &Pool) -> Result<PgConnection, Error> {
    PgConnection::connect_with(&pool.dedicated_options())
        .await
        .map_err(|err| {
            Error::connect(
                pool.host(),
                pool.database(),
                &err.to_string(),
                pool.redacted(),
            )
        })
}

async fn migrate_with_lock_wait(pool: &Pool, wait: Duration) -> Result<(), Error> {
    let mut connection = connect_dedicated(pool).await?;
    take_migration_lock(&mut connection, wait).await?;
    // The read rides the pool: the lock, not the read, is what has to be on the
    // dedicated connection.
    let applied = read_schema_version(pool).await?;
    refuse_a_newer_schema(applied)?;
    if applied == 0 {
        sqlx::query(VERSION_TABLE_SQL)
            .execute(&mut connection)
            .await?;
    }
    for migration in pending(applied) {
        tracing::info!(
            version = migration.version,
            name = %migration.description,
            "applying migration"
        );
        let mut tx = connection.begin().await?;
        sqlx::raw_sql(migration.sql.as_str())
            .execute(&mut *tx)
            .await?;
        query!(
            "INSERT INTO schema_version (id, applied_at, note) VALUES ($1, $2, $3)",
            migration.version as i32,
            now_nanos() as i64,
            migration.description.as_ref()
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }
    // Best effort: this connection is dropped next, which releases the lock
    // whether or not the statement lands.
    if let Err(err) = query!("SELECT pg_advisory_unlock($1)", MIGRATION_LOCK_KEY)
        .fetch_optional(&mut connection)
        .await
    {
        tracing::warn!(error = %err, "releasing the migration lock failed");
    }
    Ok(())
}

/// Take the migration lock, waiting at most `wait`: PG11's bounded wait, so a
/// second instance that finds the lock held reports it rather than blocking
/// forever behind a migration that may never finish.
async fn take_migration_lock(connection: &mut PgConnection, wait: Duration) -> Result<(), Error> {
    let deadline = Instant::now() + wait;
    loop {
        let locked = query_file_scalar!("sql/store/advisory_try_lock.sql", MIGRATION_LOCK_KEY)
            .fetch_one(&mut *connection)
            .await?;
        if locked {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::MigrationLockBusy {
                key: MIGRATION_LOCK_KEY,
                waited: wait,
            });
        }
        time::sleep(MIGRATION_LOCK_POLL).await;
    }
}

/// How long a caller waits between ownership re-checks.
pub const OWNERSHIP_RECHECK: Duration = Duration::from_secs(30);

/// How long the migration runner waits for the migration lock before it
/// refuses with [`Error::MigrationLockBusy`] (PG11: the wait is bounded).
pub const MIGRATION_LOCK_WAIT: Duration = Duration::from_secs(30);

/// How long the runner sleeps between attempts at the migration lock.
const MIGRATION_LOCK_POLL: Duration = Duration::from_millis(100);

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;

#[cfg(test)]
#[expect(
    clippy::absolute_paths,
    reason = "the module proves the crate's public path — its imports spell `tinio_store_pg::…` on purpose, and a shortened path would not prove the public one"
)]
#[path = "store_db_tests.rs"]
mod db_tests;
