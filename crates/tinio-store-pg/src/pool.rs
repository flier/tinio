//! The driver's pool, the dsn, and the TLS policy.
//!
//! PG9/PG10/PG26: the pool is `sqlx::PgPool`, sized by `[storage.pg] pool` with
//! an acquire timeout and `test_before_acquire` as the health check; TLS uses
//! the driver's rustls connector over the system trust store. Two mappings are
//! the store's, not the driver's defaults: an omitted `sslmode` is treated as
//! **require** (chain *and* hostname verified), and `require` itself is upgraded
//! to the driver's `verify-full`, because the driver's own `Require` stops at
//! the chain and never checks the hostname. Plaintext happens only with an
//! explicit `sslmode=disable`.
//!
//! The dsn carries the password; the parsed options hold it and nothing else
//! does. Every rendering this module produces is the redacted form (PG5).

use std::{ops::Deref, time::Duration};

use derive_more::Debug;
use futures::{future::BoxFuture, stream::BoxStream};
use parse_display::Display;
use sqlx::{
    Describe, Either, Error as SqlxError, Execute, Executor, PgPool, Postgres, SqlStr,
    postgres::{
        PgConnectOptions, PgPoolOptions, PgQueryResult, PgRow, PgSslMode, PgStatement, PgTypeInfo,
    },
};

use crate::error::{Error, redact_userinfo};

/// The default pool size (`[storage.pg] pool` overrides it).
pub const DEFAULT_POOL_SIZE: u32 = 8;

/// How long a caller waits for a pooled connection before the acquire fails.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);

/// The `application_name` every connection this crate opens reports, so
/// `pg_stat_activity` can name the session that holds a lock: a lock's refusal
/// and `doctor` both read it back (PG12).
const APPLICATION_NAME: &str = "tinio-store-pg";

/// The session setting a read-only pool opens every connection with (PG6,
/// PG26): the second of the three read-only layers, in place **before the first
/// statement** because it rides the startup packet's `options` rather than
/// being established by a statement of its own — the server refuses a write on
/// such a session even when the entry guard above was bypassed.
const READ_ONLY_SESSION_OPTION: (&str, &str) = ("default_transaction_read_only", "on");

/// The TLS mode the store serves under (PG10).
///
/// The mode set is the client's own: `disable`, `prefer` and `require`
/// (`verify-ca`/`verify-full` are not driver modes). `require` verifies the chain
/// **and** the hostname.
///
/// # Examples
///
/// ```rust
/// use tinio_store_pg::pool::SslMode;
///
/// assert_eq!(SslMode::Require.to_string(), "require");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(style = "kebab-case")]
pub enum SslMode {
    /// Plaintext, and only because the operator wrote `sslmode=disable`.
    Disable,
    /// TLS when the server offers it, plaintext otherwise.
    Prefer,
    /// TLS required, with the chain and the hostname verified.
    Require,
}

impl SslMode {
    /// The mode a parsed dsn asks for. An omitted mode is `require` (PG10);
    /// `allow` and `verify-ca`/`verify-full` fold onto the nearest mode in the
    /// client's real set rather than being silently downgraded.
    fn of(options: &PgConnectOptions, ssl_required: bool) -> Self {
        match options.get_ssl_mode() {
            PgSslMode::Disable => Self::Disable,
            PgSslMode::Prefer if ssl_required => Self::Prefer,
            PgSslMode::Allow
            | PgSslMode::Prefer
            | PgSslMode::Require
            | PgSslMode::VerifyCa
            | PgSslMode::VerifyFull => Self::Require,
        }
    }

    /// The driver mode this store maps the mode onto: `require` becomes the
    /// driver's `verify-full` so the chain and the hostname are both checked.
    fn driver(self) -> PgSslMode {
        match self {
            Self::Disable => PgSslMode::Disable,
            Self::Prefer => PgSslMode::Prefer,
            Self::Require => PgSslMode::VerifyFull,
        }
    }
}

/// A connection string and what it says. Only the redacted text is kept — the
/// raw, password-bearing one is consumed into the driver's options and never
/// retained — so every rendering this type produces stays the redacted form
/// (PG5, the module header).
#[derive(Clone, Debug)]
pub(crate) struct Dsn {
    /// The dsn with its userinfo password replaced.
    dsn: String,
    host: String,
    database: String,
    mode: SslMode,
    /// Whether the session is opened read-only (PG6): `default_transaction_read_only = on`
    /// rides the connection's startup options.
    read_only: bool,
    #[debug(skip)]
    options: PgConnectOptions,
}

impl Dsn {
    /// Parse a dsn. The password is written into the driver's options and
    /// nowhere else: every rendering this type produces is the redacted form.
    pub(crate) fn parse(dsn: &str) -> Result<Self, Error> {
        Self::parse_with(dsn, false)
    }

    /// Parse a dsn whose session is opened **read-only** (PG6, layer 2): the
    /// driver's options carry `default_transaction_read_only = on` as a startup
    /// option, so the setting is in place before the first statement rather
    /// than established by one (PG26).
    pub(crate) fn parse_read_only(dsn: &str) -> Result<Self, Error> {
        Self::parse_with(dsn, true)
    }

    fn parse_with(dsn: &str, read_only: bool) -> Result<Self, Error> {
        let options: PgConnectOptions = dsn
            .parse()
            .map_err(|err| Error::database(&format!("invalid dsn: {err}"), dsn))?;
        let host = options.get_host().to_owned();
        let database = options.get_database().unwrap_or_default().to_owned();
        let mode = SslMode::of(&options, names_an_ssl_mode(dsn));
        // A session this crate opens is nameable in `pg_stat_activity`, which
        // is what lets a second writable instance name the owner of the lock it
        // could not take (PG12). An `application_name` the dsn itself names is
        // left alone.
        let options = match options.get_application_name() {
            Some(name) if !name.is_empty() => options,
            _ => options.application_name(APPLICATION_NAME),
        };
        let options = options.ssl_mode(mode.driver());
        // Appended, never set: a dsn that already carries `options=` keeps its
        // own settings, and the read-only one rides beside them.
        let options = if read_only {
            options.options([READ_ONLY_SESSION_OPTION])
        } else {
            options
        };
        Ok(Self {
            dsn: redact_userinfo(dsn),
            host,
            database,
            mode,
            read_only,
            options,
        })
    }

    /// Whether this dsn's session is opened read-only.
    pub(crate) fn read_only(&self) -> bool {
        self.read_only
    }

    /// The dsn with its userinfo password replaced.
    pub(crate) fn redacted(&self) -> &str {
        &self.dsn
    }

    /// The host the dsn names (never a secret).
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    /// The database the dsn names (never a secret).
    pub(crate) fn database(&self) -> &str {
        &self.database
    }

    /// The TLS mode the dsn asks for.
    pub(crate) fn mode(&self) -> SslMode {
        self.mode
    }

    /// The driver's options, carrying the password.
    pub(crate) fn options(&self) -> PgConnectOptions {
        self.options.clone()
    }
}

/// Whether the dsn itself names an `sslmode`, as opposed to leaving the
/// driver's `prefer` default in place.
///
/// The word has to be a parameter *name*, not an occurrence inside another
/// value's text: only the start of the dsn or a separator ahead of it makes it
/// the operator speaking, so `?application_name=sslmode=require` is an omitted
/// mode — `require` — and never the driver's `prefer` (PG10).
fn names_an_ssl_mode(dsn: &str) -> bool {
    let lowered = dsn.to_ascii_lowercase();
    let token = "sslmode";
    let mut from = 0;
    while let Some(index) = lowered[from..].find(token) {
        let start = from + index;
        let opens_a_parameter = start == 0
            || matches!(
                lowered.as_bytes()[start - 1],
                b'?' | b'&' | b' ' | b'\t' | b'\n' | b'\r'
            );
        if opens_a_parameter && lowered[start + token.len()..].trim_start().starts_with('=') {
            return true;
        }
        from = start + token.len();
    }
    false
}

/// The store's connection pool and the facts `doctor` reports about it.
///
/// `Debug` renders the identity (`mode`, `host`, `database`, `read_only`) and
/// skips the driver's pool, the driver's options and the dsn, so a store's
/// rendering carries no secret (PG5).
///
/// # Examples
///
/// ```
/// use tinio_store_pg::pool::Pool;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), tinio_store_pg::Error> {
/// // The pool connects lazily, so this opens no connection; it does need the
/// // runtime it will serve on.
/// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 4)?;
/// // What the rendering promises: the identity, never the dsn's password.
/// let rendered = format!("{pool:?}");
/// assert!(rendered.contains("db.example"));
/// assert!(!rendered.contains("password"));
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Pool {
    #[debug(skip)]
    pool: PgPool,
    mode: SslMode,
    host: String,
    database: String,
    read_only: bool,
    /// The driver's options **every** connection this pool opens is built from — the pooled ones
    /// and a dedicated out-of-pool one alike — so the read-only startup option reaches the
    /// dedicated connection too and a pool cannot hand out a session with more privilege than it
    /// was opened with (PG6). Carries the password: never rendered.
    #[debug(skip)]
    options: PgConnectOptions,
    /// The dsn with its userinfo password replaced, for a connect failure's rendering (PG5).
    /// Skipped by `Debug` like the driver's pool and the raw dsn: the rendering is the identity.
    #[debug(skip)]
    redacted: String,
}

impl Pool {
    /// Build the pool for `dsn`, sized `max_connections`, with an acquire
    /// timeout and `test_before_acquire` as the health check (PG9).
    ///
    /// The pool connects lazily, so a wrong dsn answers at the first acquire
    /// rather than here; a connect failure names the host and the database and
    /// carries the redacted driver message.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::pool::{Pool, SslMode};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), tinio_store_pg::Error> {
    /// // A dsn that omits `sslmode` resolves to `require` — the store's policy
    /// // rather than the driver's `prefer` default (PG10).
    /// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 4)?;
    /// assert_eq!(pool.ssl_mode(), SslMode::Require);
    /// assert_eq!(pool.max_connections(), 4);
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(dsn: &str, max_connections: u32) -> Result<Self, Error> {
        Self::open_with(dsn, max_connections, DEFAULT_ACQUIRE_TIMEOUT)
    }

    /// Build a **read-only** pool for `dsn`: every connection it hands out is
    /// opened with `default_transaction_read_only = on` as a startup option
    /// (PG6, layer 2; the option rides the startup packet, so the server holds
    /// the setting before the first statement — PG26).
    ///
    /// This is the session half of read-only mode. The entry guard
    /// (`tinio-pg`'s `PgStorage`) is the half a caller sees; this one holds when
    /// a statement reaches the server by some path the guard did not cover, and
    /// the role's missing DML grants are the third.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::pool::Pool;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), tinio_store_pg::Error> {
    /// // Constructing opens nothing (the pool connects lazily), and
    /// // [`Pool::is_read_only`] reports the mode every connection will carry.
    /// let pool = Pool::open_read_only("postgres://user:password@db.example:5432/tinio", 4)?;
    /// assert!(pool.is_read_only());
    /// assert_eq!(pool.max_connections(), 4);
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_read_only(dsn: &str, max_connections: u32) -> Result<Self, Error> {
        Self::build(
            Dsn::parse_read_only(dsn)?,
            max_connections,
            DEFAULT_ACQUIRE_TIMEOUT,
        )
    }

    /// Build the pool for `dsn` in the mode `access` names — the choice every
    /// caller that holds an [`crate::Access`] would otherwise re-spell (PG6). A read-only
    /// access opens a pool whose sessions carry the read-only startup option, so a
    /// diagnosis cannot write through the pool it inspects with.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::{Access, pool::Pool};
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), tinio_store_pg::Error> {
    /// let dsn = "postgres://user:password@db.example:5432/tinio";
    /// // Constructing opens nothing (the pool connects lazily).
    /// assert!(Pool::open_for(dsn, 4, Access::ReadOnly)?.is_read_only());
    /// assert!(!Pool::open_for(dsn, 4, Access::ReadWrite)?.is_read_only());
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_for(dsn: &str, max_connections: u32, access: crate::Access) -> Result<Self, Error> {
        if access.is_writable() {
            Self::open(dsn, max_connections)
        } else {
            Self::open_read_only(dsn, max_connections)
        }
    }

    /// [`Pool::open`] with the acquire timeout made explicit.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use tinio_store_pg::pool::Pool;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), tinio_store_pg::Error> {
    /// let dsn = "postgres://user:password@db.example:5432/tinio?sslmode=disable";
    /// // The timeout is what a caller waits for a pooled connection before the
    /// // acquire fails; the pool itself connects lazily.
    /// let pool = Pool::open_with(dsn, 2, Duration::from_secs(1))?;
    /// assert_eq!(pool.max_connections(), 2);
    /// assert_eq!(pool.host(), "db.example");
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_with(
        dsn: &str,
        max_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, Error> {
        Self::build(Dsn::parse(dsn)?, max_connections, acquire_timeout)
    }

    /// The one constructor: a parsed dsn, a pool size and an acquire timeout
    /// become a lazy pool plus the identity `doctor` reports. `open`,
    /// `open_with` and `open_read_only` differ only in the dsn they hand it, so
    /// the access mode is decided in exactly one place — the parse (PG6).
    fn build(parsed: Dsn, max_connections: u32, acquire_timeout: Duration) -> Result<Self, Error> {
        let options = parsed.options();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .acquire_timeout(acquire_timeout)
            .test_before_acquire(true)
            .connect_lazy_with(options.clone());
        // PG10: the mode the connection is opened under is logged, and
        // [`Pool::ssl_mode`] reports the same value to `doctor`. The driver
        // exposes no post-handshake negotiation state, so this is the resolved
        // policy rather than a second reading of the wire.
        tracing::info!(
            mode = %parsed.mode(),
            host = %parsed.host(),
            database = %parsed.database(),
            read_only = parsed.read_only(),
            max_connections = max_connections.max(1),
            "postgres pool opened"
        );
        Ok(Self {
            pool,
            mode: parsed.mode(),
            host: parsed.host().to_owned(),
            database: parsed.database().to_owned(),
            read_only: parsed.read_only(),
            options,
            redacted: parsed.redacted().to_owned(),
        })
    }

    /// The TLS mode the dsn asked for (PG10; `doctor` reports it).
    pub fn ssl_mode(&self) -> SslMode {
        self.mode
    }

    /// Whether this pool's sessions are opened read-only (PG6).
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// The host the dsn names.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The database the dsn names.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The driver's options for a **dedicated out-of-pool** connection — the
    /// ownership guard, the migration runner (PG11/PG12).
    ///
    /// This is the pool's own session, the read-only startup option included
    /// (PG6): a dedicated connection is opened **through the pool's mode**, so it
    /// can never open with more privilege than the pool that asked for it. The
    /// mode-less raw dsn this crate once handed out has no accessor any more, so
    /// nothing can re-parse it and drop the mode by accident.
    pub(crate) fn dedicated_options(&self) -> PgConnectOptions {
        self.options.clone()
    }

    /// The dsn with its userinfo password replaced, for a connect failure's
    /// rendering (PG5). Never the raw dsn: it carries the password.
    pub(crate) fn redacted(&self) -> &str {
        &self.redacted
    }

    /// The pool's configured maximum.
    pub fn max_connections(&self) -> u32 {
        self.pool.options().get_max_connections()
    }

    /// How many connections are idle right now.
    pub fn num_idle(&self) -> usize {
        self.pool.num_idle()
    }

    /// Whether the pool can still hand out a connection.
    pub fn is_closed(&self) -> bool {
        self.pool.is_closed()
    }

    /// The pool's own size facts: the configured maximum and the idle count.
    pub fn size(&self) -> (u32, usize) {
        (self.max_connections(), self.num_idle())
    }

    /// Close every pooled connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_store_pg::pool::Pool;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), tinio_store_pg::Error> {
    /// let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 4)?;
    /// // Returns once every pooled connection has closed; a later acquire
    /// // fails rather than opening a new one.
    /// pool.close().await;
    /// assert!(pool.is_closed());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// The driver's pool, one deref away: `pool.begin()` and `begin(&pool)` both work,
/// and a driver method needs no accessor of its own.
impl Deref for Pool {
    type Target = PgPool;

    fn deref(&self) -> &PgPool {
        &self.pool
    }
}

/// The wrapper is itself an executor: the query macros and builders take the
/// pool as a generic `Executor` argument, and that bound is resolved on the
/// argument's own type, never through `Deref`. Every method hands the work to
/// the inner pool, which owns the connections and the acquire timeout.
impl<'c> Executor<'c> for &'c Pool {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<'e, Result<Either<PgQueryResult, PgRow>, SqlxError>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        self.pool.fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<PgRow>, SqlxError>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Postgres>,
    {
        self.pool.fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [PgTypeInfo],
    ) -> BoxFuture<'e, Result<PgStatement, SqlxError>>
    where
        'c: 'e,
    {
        self.pool.prepare_with(sql, parameters)
    }

    #[doc(hidden)]
    fn describe<'e>(self, sql: SqlStr) -> BoxFuture<'e, Result<Describe<Postgres>, SqlxError>>
    where
        'c: 'e,
    {
        self.pool.describe(sql)
    }
}

#[cfg(test)]
mod tests {
    // The tests address the crate and the standard library by absolute path on
    // purpose: a shortened path would not prove the public one.
    #![allow(clippy::absolute_paths)]

    use super::{DEFAULT_POOL_SIZE, Dsn, Pool, SslMode, names_an_ssl_mode};
    use crate::error::{Error, redact_userinfo};

    #[tokio::test]
    async fn an_omitted_sslmode_is_require() {
        let pool = Pool::open_with(
            "postgres://user:password@db.example:5432/tinio",
            DEFAULT_POOL_SIZE,
            std::time::Duration::from_millis(50),
        )
        .expect("the dsn parses");
        assert_eq!(pool.ssl_mode(), SslMode::Require);
        assert_eq!(pool.host(), "db.example");
        assert_eq!(pool.database(), "tinio");
        assert_eq!(pool.max_connections(), DEFAULT_POOL_SIZE);
        assert_eq!(pool.size().0, DEFAULT_POOL_SIZE);
        pool.close().await;
    }

    #[test]
    fn require_is_upgraded_to_verify_full() {
        let dsn = Dsn::parse("postgres://user:password@db.example:5432/tinio?sslmode=require")
            .expect("the dsn parses");
        assert_eq!(dsn.mode(), SslMode::Require);
        assert!(
            matches!(
                dsn.options().get_ssl_mode(),
                sqlx::postgres::PgSslMode::VerifyFull
            ),
            "chain and hostname are both verified"
        );
    }

    #[test]
    fn an_explicit_disable_stays_plaintext() {
        let dsn = Dsn::parse("postgres://user:password@db.example:5432/tinio?sslmode=disable")
            .expect("the dsn parses");
        assert_eq!(dsn.mode(), SslMode::Disable);
        assert!(matches!(
            dsn.options().get_ssl_mode(),
            sqlx::postgres::PgSslMode::Disable
        ));
    }

    #[test]
    fn prefer_is_opt_in_and_kept() {
        let dsn = Dsn::parse("postgres://user:password@db.example:5432/tinio?sslmode=prefer")
            .expect("the dsn parses");
        assert_eq!(dsn.mode(), SslMode::Prefer);
    }

    /// The modes this store does not model fold onto the nearest one it does,
    /// never onto a weaker one: `allow` (the driver's "plaintext first" mode) and
    /// both `verify-*` modes all land on `require`, and the connection is opened
    /// under the driver's chain-and-hostname check.
    #[test]
    fn the_unmodelled_ssl_modes_fold_onto_require() {
        for mode in ["allow", "verify-ca", "verify-full"] {
            let parsed = Dsn::parse(&format!(
                "postgres://user:password@db.example:5432/tinio?sslmode={mode}"
            ))
            .expect("the dsn parses");
            assert_eq!(
                parsed.mode(),
                SslMode::Require,
                "sslmode={mode} is not a mode this store serves below require"
            );
            assert!(
                matches!(
                    parsed.options().get_ssl_mode(),
                    sqlx::postgres::PgSslMode::VerifyFull
                ),
                "sslmode={mode} must not open a weaker connection than require"
            );
        }
    }

    /// Whether the *operator* named a mode, which is what keeps the driver's own
    /// `prefer` default from being read as an omission (PG10).
    #[test]
    fn the_ssl_mode_probe_reads_a_token_and_not_a_substring() {
        assert!(!names_an_ssl_mode(
            "postgres://user:password@db.example:5432/tinio"
        ));
        assert!(names_an_ssl_mode(
            "postgres://user:password@db.example:5432/tinio?sslmode=require"
        ));
        assert!(
            names_an_ssl_mode("postgres://user:password@db.example:5432/tinio?SSLMODE=require"),
            "the probe compares case-insensitively"
        );
        assert!(
            names_an_ssl_mode("host=db.example user=tinio sslmode = require"),
            "a keyword dsn may space the `=`"
        );
        assert!(
            !names_an_ssl_mode("postgres://user:password@db.example:5432/tinio?plansslmode"),
            "an occurrence without `=` is not the operator speaking"
        );
        assert!(
            !names_an_ssl_mode(
                "postgres://user:password@db.example:5432/tinio?application_name=sslmode"
            ),
            "an occurrence inside another value is not the operator speaking"
        );
        assert!(
            !names_an_ssl_mode(
                "postgres://user:password@db.example:5432/tinio?application_name=sslmode=require"
            ),
            "another value's text may carry `sslmode=` without the operator naming a mode"
        );
        // What the last two are for: a dsn that only *mentions* a mode is still
        // an omission, so it is served at `require` rather than falling back to
        // the driver's `prefer`, which would take plaintext (PG10).
        let mentioned = Dsn::parse(
            "postgres://user:password@db.example:5432/tinio?application_name=sslmode=require",
        )
        .expect("the dsn parses");
        assert_eq!(mentioned.mode(), SslMode::Require);
        assert!(
            matches!(
                mentioned.options().get_ssl_mode(),
                sqlx::postgres::PgSslMode::VerifyFull
            ),
            "a dsn that mentions a mode is still an omission, and an omission verifies"
        );
    }

    /// A dsn this crate cannot even parse is the one failure that reaches the
    /// caller before any connection; it names the reason, and it never echoes a
    /// password (PG5).
    #[tokio::test]
    async fn an_unparseable_dsn_is_refused_without_a_password() {
        let err = Pool::open(
            "not a dsn: postgres://tinio:sup3rsecret@db.internal/tinio",
            1,
        )
        .expect_err("an unparseable dsn is refused");
        assert!(matches!(&err, Error::Database(_)), "{err}");
        assert!(err.to_string().contains("invalid dsn"), "{err}");
        assert!(!err.to_string().contains("sup3rsecret"), "{err}");
    }

    /// A session this crate opens is nameable in `pg_stat_activity`, and a name
    /// the dsn itself carries is the operator's and is left alone.
    #[test]
    fn a_session_is_named_unless_the_dsn_names_it() {
        let dsn =
            Dsn::parse("postgres://user:password@db.example:5432/tinio").expect("the dsn parses");
        assert_eq!(
            dsn.options().get_application_name(),
            Some("tinio-store-pg"),
            "the crate's own sessions are nameable"
        );
        let named =
            Dsn::parse("postgres://user:password@db.example:5432/tinio?application_name=worker-7")
                .expect("the dsn parses");
        assert_eq!(named.options().get_application_name(), Some("worker-7"));
        let empty = Dsn::parse("postgres://user:password@db.example:5432/tinio?application_name=")
            .expect("the dsn parses");
        assert_eq!(
            empty.options().get_application_name(),
            Some("tinio-store-pg"),
            "an empty name is no name"
        );
    }

    /// PG10: the mode the connection is opened under is logged, and
    /// [`Pool::ssl_mode`] reports the same value to `doctor`. The driver exposes
    /// no post-handshake state, so the logged line *is* the resolved policy.
    #[tokio::test]
    async fn the_resolved_policy_is_logged_beside_the_identity_it_reports() {
        let capture = tinio_util::testing::capture();
        let recording = capture.record();
        let pool = Pool::open_with(
            "postgres://user:password@db.example:5432/tinio?sslmode=disable",
            4,
            std::time::Duration::from_millis(50),
        )
        .expect("the dsn parses");
        let logged = capture.text();
        drop(recording);

        tinio_util::testing::assert_recorded_at(
            &logged,
            "postgres pool opened",
            tracing::Level::INFO,
        );
        // The mode is the store's own rendering, so the line `doctor` and the
        // log share one vocabulary.
        assert!(logged.contains("mode=disable"), "{logged}");
        assert!(logged.contains("host=db.example"), "{logged}");
        assert!(logged.contains("database=tinio"), "{logged}");
        assert!(logged.contains("max_connections=4"), "{logged}");
        assert!(
            !logged.contains("password"),
            "the logged line never carries the dsn: {logged}"
        );
        assert_eq!(pool.ssl_mode(), SslMode::Disable);
    }

    #[tokio::test]
    async fn a_lazy_pool_closes_and_reports_it() {
        let pool = Pool::open("postgres://user:password@db.example:5432/tinio", 1)
            .expect("the dsn parses");
        assert!(!pool.is_closed());
        assert_eq!(pool.num_idle(), 0, "nothing has connected yet");
        assert_eq!(pool.size(), (1, 0));
        pool.close().await;
        assert!(pool.is_closed(), "a closed pool hands out no connection");
    }

    #[test]
    fn a_parsed_dsn_never_renders_the_password() {
        let dsn = Dsn::parse("postgres://tinio:sup3rsecret@db.internal:5432/tinio")
            .expect("the dsn parses");
        assert_eq!(
            dsn.redacted(),
            "postgres://tinio:***@db.internal:5432/tinio"
        );
        let rendered = format!("{dsn:?}");
        assert!(!rendered.contains("sup3rsecret"), "{rendered}");
        let pretty = format!("{dsn:#?}");
        assert!(
            pretty.contains(r#"dsn: "postgres://tinio:***@db.internal:5432/tinio""#),
            "{pretty}"
        );
        assert!(!pretty.contains("sup3rsecret"), "{pretty}");
    }

    #[tokio::test]
    async fn a_pool_debug_names_its_identity_and_no_password() {
        let pool = Pool::open(
            "postgres://tinio:sup3rsecret@db.internal:5432/tinio?sslmode=disable",
            1,
        )
        .expect("the dsn parses");
        let rendered = format!("{pool:?}");
        assert_eq!(
            rendered,
            r#"Pool { mode: Disable, host: "db.internal", database: "tinio", read_only: false, .. }"#
        );
        assert!(!rendered.contains("sup3rsecret"), "{rendered}");
    }

    /// The read-only pool (PG6, layer 2) carries `default_transaction_read_only
    /// = on` as a **startup option** — the driver's `options` parameter, which
    /// the server applies as the session opens, before any statement — and the
    /// writable pool does not. The rendering names the mode.
    #[test]
    fn a_read_only_pool_opens_its_session_read_only() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        let writable = Dsn::parse(dsn).expect("the dsn parses");
        assert!(!writable.read_only());
        assert!(
            !format!("{:?}", writable.options()).contains("default_transaction_read_only"),
            "a writable session carries no read-only option"
        );

        let read_only = Dsn::parse_read_only(dsn).expect("the dsn parses");
        assert!(read_only.read_only());
        let rendered = format!("{:?}", read_only.options());
        assert!(
            rendered.contains("default_transaction_read_only=on"),
            "the option is `-c default_transaction_read_only=on` in the startup \
             options: {rendered}"
        );
    }

    /// A dsn that names its own `options` keeps them: the read-only setting is
    /// appended, never substituted.
    #[test]
    fn the_read_only_option_rides_beside_a_dsn_own_options() {
        let parsed = Dsn::parse_read_only(
            "postgres://user:password@db.example:5432/tinio?options=-c%20search_path%3Dtenant",
        )
        .expect("the dsn parses");
        let rendered = format!("{:?}", parsed.options());
        assert!(
            rendered.contains("search_path=tenant"),
            "the dsn's own option survives: {rendered}"
        );
        assert!(
            rendered.contains("default_transaction_read_only=on"),
            "and the read-only one joins it: {rendered}"
        );
    }

    /// A **dedicated out-of-pool** connection — the ownership guard's, the
    /// migration runner's — opens through the pool's own options, so a
    /// read-only pool's dedicated connection carries the read-only startup
    /// option too. This is the read-back the fix for the mode-less reconnect
    /// rests on: before it, `connect_dedicated` re-parsed the raw dsn through
    /// the writable arm and the option was dropped, so the dedicated
    /// connection would have opened **writable** beside a read-only pool.
    #[tokio::test]
    async fn a_read_only_pools_dedicated_connection_is_read_only_too() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        let writable = Pool::open(dsn, 1).expect("the dsn parses");
        assert!(!writable.is_read_only());
        assert!(
            !format!("{:?}", writable.dedicated_options())
                .contains("default_transaction_read_only"),
            "a writable pool's dedicated connection carries no read-only option"
        );

        let read_only = Pool::open_read_only(dsn, 1).expect("the dsn parses");
        assert!(read_only.is_read_only());
        let rendered = format!("{:?}", read_only.dedicated_options());
        assert!(
            rendered.contains("default_transaction_read_only=on"),
            "the dedicated connection is opened through the pool's own read-only \
             session: {rendered}"
        );
    }

    /// [`Pool::is_read_only`] and the pool constructor agree: the writable
    /// constructors report `false`, `open_read_only` reports `true`.
    #[tokio::test]
    async fn the_pool_reports_the_mode_it_was_opened_in() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        assert!(!Pool::open(dsn, 1).expect("the dsn parses").is_read_only());
        assert!(
            Pool::open_read_only(dsn, 1)
                .expect("the dsn parses")
                .is_read_only()
        );
    }

    /// The wrapper is itself the executor the query macros take by value: the
    /// statement the trait hands it is prepared and described on the driver's
    /// own pool, which is what lets `query_file_scalar!(...).fetch_one(&pool)`
    /// resolve on `&Pool` rather than through `Deref`.
    #[tokio::test]
    async fn the_pool_is_an_executor_of_its_own() {
        let Some((db, _store)) = crate::tests::fixtures::scratch().await else {
            return;
        };
        let pool = Pool::open(&db.dsn, 1).expect("the dsn parses");
        let sql = sqlx::SqlStr::from_static("SELECT 1 AS one");
        let prepared = <&Pool as sqlx::Executor>::prepare_with(&pool, sql.clone(), &[])
            .await
            .expect("the wrapper prepares through the driver's pool");
        assert!(
            format!("{prepared:?}").contains("SELECT 1 AS one"),
            "{prepared:?}"
        );
        let described = <&Pool as sqlx::Executor>::describe(&pool, sql)
            .await
            .expect("the wrapper describes through the driver's pool");
        assert_eq!(
            described.columns().len(),
            1,
            "the description is the statement's own"
        );
        pool.close().await;
        db.drop_db().await;
    }

    #[test]
    fn masking_keeps_the_user_the_host_and_the_query() {
        assert_eq!(
            redact_userinfo("postgres://tinio:sup3rsecret@db.internal:5432/tinio?sslmode=require"),
            "postgres://tinio:***@db.internal:5432/tinio?sslmode=require"
        );
        assert_eq!(
            redact_userinfo("host=db user=tinio password=pw"),
            "host=db user=tinio password=***"
        );
    }
}
