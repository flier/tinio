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

/// A connection string and what it says. The raw text is password-bearing and
/// `Debug` skips it, so every rendering this type produces stays the redacted
/// form (PG5, the module header).
#[derive(Clone, Debug)]
pub(crate) struct Dsn {
    /// The dsn as given, for a reconnect on a dedicated connection.
    #[debug(skip)]
    raw: String,
    /// The dsn with its userinfo password replaced.
    dsn: String,
    host: String,
    database: String,
    mode: SslMode,
    #[debug(skip)]
    options: PgConnectOptions,
}

impl Dsn {
    /// Parse a dsn. The password is written into the driver's options and
    /// nowhere else: every rendering this type produces is the redacted form.
    pub(crate) fn parse(dsn: &str) -> Result<Self, Error> {
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
        Ok(Self {
            raw: dsn.to_owned(),
            dsn: redact_userinfo(dsn),
            host,
            database,
            mode,
            options,
        })
    }

    /// The dsn as given, for a reconnect on a dedicated connection.
    pub(crate) fn text(&self) -> &str {
        &self.raw
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
/// `Debug` renders the identity (`mode`, `host`, `database`) and skips the
/// driver's pool and the dsn, so a store's rendering carries no secret (PG5).
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
    #[debug(skip)]
    dsn: String,
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
        let parsed = Dsn::parse(dsn)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .acquire_timeout(acquire_timeout)
            .test_before_acquire(true)
            .connect_lazy_with(parsed.options());
        // PG10: the mode the connection is opened under is logged, and
        // [`Pool::ssl_mode`] reports the same value to `doctor`. The driver
        // exposes no post-handshake negotiation state, so this is the resolved
        // policy rather than a second reading of the wire.
        tracing::info!(
            mode = %parsed.mode(),
            host = %parsed.host(),
            database = %parsed.database(),
            max_connections = max_connections.max(1),
            "postgres pool opened"
        );
        Ok(Self {
            pool,
            mode: parsed.mode(),
            host: parsed.host().to_owned(),
            database: parsed.database().to_owned(),
            dsn: parsed.text().to_owned(),
        })
    }

    /// The TLS mode the dsn asked for (PG10; `doctor` reports it).
    pub fn ssl_mode(&self) -> SslMode {
        self.mode
    }

    /// The host the dsn names.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The database the dsn names.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The dsn as given: the reconnect a dedicated out-of-pool connection (the
    /// ownership guard, the migration runner) needs. Never logged.
    pub(crate) fn dsn_text(&self) -> &str {
        &self.dsn
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
            r#"Pool { mode: Disable, host: "db.internal", database: "tinio", .. }"#
        );
        assert!(!rendered.contains("sup3rsecret"), "{rendered}");
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
