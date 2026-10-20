//! The driver's pool, the dsn, and the TLS policy.
//!
//! PG9/PG10/PG26: the pool is `sqlx::PgPool`, sized by `[storage.pg] pool` with
//! an acquire timeout and `test_before_acquire` as the health check; TLS uses
//! the driver's rustls connector over the system trust store. The mode set is
//! the store's own and has exactly two members: `disable`, and `require` — the
//! driver's `verify-full`, so the chain **and** the hostname are checked. An
//! omitted `sslmode` is `require` (PG10), and plaintext happens only with an
//! explicit `sslmode=disable`.
//!
//! The driver's `prefer` is **refused**, not mapped (R1): sqlx sets
//! `accept_invalid_certs` for every mode below `verify-ca` and disables the
//! hostname check below `verify-full`, so a `prefer` session would install a
//! verifier that accepts any certificate and would authenticate nobody. Every
//! other spelling — `allow`, `verify-ca`, `verify-full`, an unknown value, or a
//! key the driver's own parse would ignore — is refused with the two accepted
//! modes named.
//!
//! The dsn's own text decides a session, and the read of it is held to the
//! driver's parse rather than trusted on its own: the query text is read with
//! the driver's key match (a case-sensitive `sslmode`/`ssl-mode`, the last
//! occurrence winning), the resolved mode is written onto the driver's options
//! explicitly, and a text the driver read differently is refused instead of
//! being served as an omission. A `PG*` environment is therefore never the
//! decider: `PGSSLMODE` is refused when it would weaken the mode the dsn
//! resolved, and `PGAPPNAME` cannot rename a session (the store's own
//! `application_name` is set unless the dsn names one), which is the name the
//! ownership refusal and `doctor` report (PG12).
//!
//! The dsn carries the password; the parsed options hold it and nothing else
//! does. Every rendering this module produces is the redacted form (PG5).

use std::{env, mem, ops::Deref, sync::Arc, time::Duration};

use derive_more::Debug;
use futures::{future::BoxFuture, stream::BoxStream};
use parse_display::Display;
use sqlx::{
    Describe, Either, Error as SqlxError, Execute, Executor, PgPool, Postgres, SqlStr,
    postgres::{
        PgConnectOptions, PgPoolOptions, PgQueryResult, PgRow, PgSslMode, PgStatement, PgTypeInfo,
    },
};

use crate::{
    Access,
    error::{Error, redact_userinfo},
    metrics::{PoolHandle, attach},
};

/// The default pool size (`[storage.pg] pool` overrides it).
pub const DEFAULT_POOL_SIZE: u32 = 8;

/// How long a caller waits for a pooled connection before the acquire fails.
pub const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(10);

/// The `application_name` every connection this crate opens reports, so
/// `pg_stat_activity` can name the session that holds a lock: a lock's refusal
/// and `doctor` both read it back (PG12).
pub(crate) const APPLICATION_NAME: &str = "tinio-store-pg";

/// The session setting a read-only pool opens every connection with (PG6,
/// PG26): the second of the three read-only layers, in place **before the first
/// statement** because it rides the startup packet's `options` rather than
/// being established by a statement of its own — the server refuses a write on
/// such a session even when the entry guard above was bypassed.
const READ_ONLY_SESSION_OPTION: (&str, &str) = ("default_transaction_read_only", "on");

/// A lookup for the libpq environment variables the driver seeds its options
/// from (`PGSSLMODE`, `PGAPPNAME`, `PGOPTIONS`, …).
///
/// The store's own decisions read that environment through this, so a test can
/// hand in the environment a deployment would have — mutating the process's own
/// is not something a test in this crate may do (`unsafe_code` is forbidden, and
/// `std::env::set_var` is unsafe in edition 2024).
pub(crate) type Environment<'e> = &'e dyn Fn(&str) -> Option<String>;

/// The process's own libpq environment: the one every parsed dsn reads unless a
/// test has handed in another.
pub(crate) fn process_environment(name: &str) -> Option<String> {
    env::var(name).ok()
}

/// The TLS mode the store serves under (PG10, R1).
///
/// Two modes and no third: `disable` is plaintext, and only because the operator
/// wrote it, while `require` verifies the chain **and** the hostname. The
/// driver's `prefer` is deliberately absent — sqlx installs a verifier that
/// accepts any certificate for it — and a dsn asking for it is refused rather
/// than mapped onto something it does not mean.
///
/// # Examples
///
/// ```rust
/// use tinio_store_pg::pool::SslMode;
///
/// assert_eq!(SslMode::Require.to_string(), "require");
/// assert_eq!(SslMode::Disable.to_string(), "disable");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Display)]
#[display(style = "kebab-case")]
pub enum SslMode {
    /// Plaintext, and only because the operator wrote `sslmode=disable`.
    Disable,
    /// TLS required, with the chain and the hostname verified.
    Require,
}

impl SslMode {
    /// The driver mode a session is opened under: `require` becomes the driver's
    /// `verify-full`, so the chain and the hostname are both checked.
    fn driver(self) -> PgSslMode {
        match self {
            Self::Disable => PgSslMode::Disable,
            Self::Require => PgSslMode::VerifyFull,
        }
    }

    /// The mode the **driver's own parse** of this store's spelling yields. The
    /// driver's `require` stops at the chain, so it is not what a session is
    /// opened with ([`Self::driver`] upgrades it); it is the form the store's
    /// reading of the dsn text is checked against, so a dsn whose parse
    /// disagrees with that reading is refused rather than served.
    fn parsed(self) -> PgSslMode {
        match self {
            Self::Disable => PgSslMode::Disable,
            Self::Require => PgSslMode::Require,
        }
    }
}

/// What a dsn's own query text says about one parameter.
///
/// The reading is the driver's: `sqlx` matches a URL's query keys
/// case-sensitively and the last occurrence of a key wins, so a parameter
/// spelled in another case is a token the driver never sees. That case is its
/// own answer — [`NamedParameter::Ignored`] — rather than an omission, because
/// serving it as one is how `?SSLMODE=require` became a plaintext-allowing
/// `prefer` (the 2026-10-13 review's root cause, R1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NamedParameter<'d> {
    /// The dsn names the parameter, with the last spelling's value.
    Named(&'d str),
    /// The dsn does not name it.
    Absent,
    /// The dsn names it in a case the driver's own parse ignores; the spelling
    /// is carried so the refusal can quote it.
    Ignored(&'d str),
}

/// Read one parameter out of a dsn's own query text, with the driver's rules
/// (`sqlx`'s URL parse): the pairs after the first `?` up to any `#`, split on
/// `&`, each split at its first `=`, matched by the exact key spelling.
///
/// The values are **not** percent-decoded here, which is deliberate: a key
/// spelled with a percent-escape is a key this reading cannot see, and the
/// driver's parse of the same text is required to agree with the reading
/// ([`SslMode`]'s cross-check), so such a dsn is refused rather than guessed at.
fn named_parameter<'d>(dsn: &'d str, name: &str) -> NamedParameter<'d> {
    let query = dsn
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or_default();
    let query = query.split('#').next().unwrap_or(query);
    let mut found = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == name {
            found = Some(value);
        } else if key.eq_ignore_ascii_case(name) {
            return NamedParameter::Ignored(key);
        }
    }
    match found {
        Some(value) => NamedParameter::Named(value),
        None => NamedParameter::Absent,
    }
}

/// Whether two driver modes are the same variant. The driver's `PgSslMode`
/// carries no `PartialEq`, so the comparison is by discriminant — the variant is
/// the whole of what a mode is.
fn same_mode(left: PgSslMode, right: PgSslMode) -> bool {
    mem::discriminant(&left) == mem::discriminant(&right)
}

/// The TLS mode a dsn's own text names, or the refusal that states the two this
/// store serves (PG10, R1).
///
/// This runs **before** the driver's parse, because an unknown value fails that
/// parse with a message naming no mode this store accepts, and an omitted mode
/// is `require` — the store's policy rather than the driver's `prefer` default.
fn named_ssl_mode(named: NamedParameter<'_>) -> Result<SslMode, Error> {
    match named {
        NamedParameter::Ignored(spelling) => Err(Error::SslMode {
            reason: format!(
                "the dsn spells `{spelling}` (`sslmode` in another case), which the driver's own \
                 parse ignores — it would be served as an omitted mode and never as the mode it \
                 says"
            ),
        }),
        NamedParameter::Absent => Ok(SslMode::Require),
        NamedParameter::Named(value) if value.eq_ignore_ascii_case("disable") => {
            Ok(SslMode::Disable)
        }
        NamedParameter::Named(value) if value.eq_ignore_ascii_case("require") => {
            Ok(SslMode::Require)
        }
        NamedParameter::Named(value) => Err(Error::SslMode {
            reason: format!(
                "the dsn asks for `{value}`, which is not a mode this store maps onto a session \
                 it can vouch for: the driver's `prefer` installs a verifier that accepts any \
                 certificate, and `allow` opens in plaintext first"
            ),
        }),
    }
}

/// Check that the driver's own parse of the dsn agrees with the reading above
/// (PG10, R1).
///
/// The two are one text read twice, so a disagreement means the mode the
/// operator wrote is not the mode the session would open under — a key this
/// reading cannot see (a percent-escaped spelling), or a later occurrence the
/// driver read differently. Refusing is the only answer that cannot end in a
/// session weaker than the dsn appears to ask for.
fn check_driver_parse(
    named: NamedParameter<'_>,
    mode: SslMode,
    parsed: &PgConnectOptions,
    seeded: PgSslMode,
) -> Result<(), Error> {
    let spelling = match named {
        NamedParameter::Named(value) => Some(value),
        NamedParameter::Absent => None,
        NamedParameter::Ignored(_) => return Ok(()),
    };
    // An omitted mode that the driver read as *something it would not have read
    // on its own* is a spelling this reading cannot see; a named mode the driver
    // read as another value is a text the two readings disagree about.
    let read_otherwise = spelling.map_or(seeded, |_| mode.parsed());
    if same_mode(parsed.get_ssl_mode(), read_otherwise) {
        return Ok(());
    }
    Err(Error::SslMode {
        reason: format!(
            "the dsn's own `sslmode` text and the driver's parse of it disagree (the driver read \
             {:?}, this store read {}): the mode a session opens under is not one both readings \
             can see, so spell it plainly as `sslmode=require` or `sslmode=disable`",
            parsed.get_ssl_mode(),
            spelling.unwrap_or("an omission")
        ),
    })
}

/// Refuse an environment that would weaken the mode the dsn resolved (R1,
/// PG10).
///
/// The store writes the resolved mode onto the driver's options, so the
/// environment cannot decide a session — but a `PGSSLMODE` that says the
/// session may fall back to plaintext is an operator's belief the dsn
/// contradicts, and `sqlx` seeds the options from it before the dsn is applied.
/// Naming it is the only thing that can tell them apart.
fn refuse_a_weakening_environment(env: Environment<'_>, mode: SslMode) -> Result<(), Error> {
    let Some(raw) = env("PGSSLMODE") else {
        return Ok(());
    };
    let seeded = raw.parse::<PgSslMode>().unwrap_or_default();
    let weakening = matches!(
        seeded,
        PgSslMode::Disable | PgSslMode::Allow | PgSslMode::Prefer
    );
    if mode == SslMode::Require && weakening {
        return Err(Error::SslMode {
            reason: format!(
                "the environment sets PGSSLMODE={raw}, which would serve this dsn's `require` as \
                 {seeded:?} (an omitted `sslmode` is `require`): the dsn's own text decides a \
                 session's TLS, so unset PGSSLMODE or name the mode in the dsn"
            ),
        });
    }
    if mode == SslMode::Disable && raw != "disable" {
        tracing::warn!(
            variable = "PGSSLMODE",
            value = %raw,
            "the environment sets PGSSLMODE, which the dsn's own `sslmode=disable` overrides"
        );
    }
    Ok(())
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
    /// The session mode (PG6): `Access::ReadOnly`'s connections carry
    /// `default_transaction_read_only = on` as a startup option.
    access: Access,
    #[debug(skip)]
    options: PgConnectOptions,
}

impl Dsn {
    /// Parse a dsn. The password is written into the driver's options and
    /// nowhere else: every rendering this type produces is the redacted form.
    pub(crate) fn parse(dsn: &str) -> Result<Self, Error> {
        Self::parse_with(dsn, Access::ReadWrite, &process_environment)
    }

    /// Parse a dsn whose session is opened **read-only** (PG6, layer 2): the
    /// driver's options carry `default_transaction_read_only = on` as a startup
    /// option, so the setting is in place before the first statement rather
    /// than established by one (PG26).
    pub(crate) fn parse_read_only(dsn: &str) -> Result<Self, Error> {
        Self::parse_with(dsn, Access::ReadOnly, &process_environment)
    }

    fn parse_with(dsn: &str, access: Access, env: Environment<'_>) -> Result<Self, Error> {
        // The single-line statement rule of docs/style.md §SQL reads the other
        // way here: this is the dsn's own text, read by hand, and the driver's
        // parse of the same text is what it is checked against. The mode is
        // resolved from the text **first**: an unknown value fails the driver's
        // own parse with a message naming no mode this store serves.
        let named_mode = named_parameter(dsn, "sslmode");
        let mode = named_ssl_mode(named_mode)?;
        let parsed: PgConnectOptions = dsn
            .parse()
            .map_err(|err| Error::database(&format!("invalid dsn: {err}"), dsn))?;
        let seeded = PgConnectOptions::new_without_pgpass().get_ssl_mode();
        check_driver_parse(named_mode, mode, &parsed, seeded)?;
        refuse_a_weakening_environment(env, mode)?;
        let host = parsed.get_host().to_owned();
        // libpq's own default, which the driver's startup packet leaves to the
        // server: a dsn that names no database connects to one named after the
        // user. Reporting the empty string made `Pool::database`, `doctor` and
        // every `Error::Connect { database }` name nothing while the driver
        // connected somewhere real.
        let database = parsed
            .get_database()
            .map(str::to_owned)
            .unwrap_or_else(|| parsed.get_username().to_owned());
        // A session this crate opens is nameable in `pg_stat_activity`, which
        // is what lets a second writable instance name the owner of the lock it
        // could not take (PG12). The environment is not consulted: a dsn that
        // names an `application_name` is the operator speaking and is kept —
        // the driver has already decoded it — and everything else gets this
        // crate's own name, so `PGAPPNAME` cannot rename the session a lock
        // refusal reports.
        let dsn_names_the_session = matches!(
            named_parameter(dsn, "application_name"),
            NamedParameter::Named(_)
        ) && parsed
            .get_application_name()
            .is_some_and(|name| !name.is_empty());
        let options = if dsn_names_the_session {
            parsed
        } else {
            parsed.application_name(APPLICATION_NAME)
        };
        let options = options.ssl_mode(mode.driver());
        // Appended, never set: a dsn that already carries `options=` keeps its
        // own settings, and the read-only one rides beside them — last, so a
        // `default_transaction_read_only=off` from the dsn or from `PGOPTIONS`
        // cannot outrank it.
        let options = if access.is_writable() {
            options
        } else {
            options.options([READ_ONLY_SESSION_OPTION])
        };
        Ok(Self {
            dsn: redact_userinfo(dsn),
            host,
            database,
            mode,
            access,
            options,
        })
    }

    /// The session mode this dsn opens under (PG6).
    pub(crate) fn access(&self) -> Access {
        self.access
    }

    /// The dsn with its userinfo password replaced.
    pub(crate) fn redacted(&self) -> &str {
        &self.dsn
    }

    /// The host the dsn names (never a secret).
    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    /// The database the session reaches (never a secret): the dsn's own, or the
    /// user's when it names none.
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

/// The store's connection pool and the facts `doctor` reports about it.
///
/// `Debug` renders the identity (`mode`, `host`, `database`, `access`) and
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
    /// The session mode every connection this pool opens is built for (PG6).
    /// Carried rather than derived, because a store handed this pool has to be
    /// able to tell whether the two agree ([`Pool::reopened_for`]).
    access: Access,
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
    /// The pool's handle in the connection gauge: held (never read) so the
    /// scrape-time collector sees a live pool and a dropped one leaves.
    #[debug(skip)]
    _metrics: Arc<PoolHandle>,
}

/// The lazy pool both constructors build: the size, the acquire timeout and the
/// health check are the store's (PG9), and the connections are built from
/// `options` — the same options a dedicated out-of-pool connection uses, so a
/// re-opened session carries exactly what a fresh one would.
fn lazy_pool(
    options: &PgConnectOptions,
    max_connections: u32,
    acquire_timeout: Duration,
) -> PgPool {
    PgPoolOptions::new()
        .max_connections(max_connections.max(1))
        .acquire_timeout(acquire_timeout)
        .test_before_acquire(true)
        .connect_lazy_with(options.clone())
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
    pub fn open_for(dsn: &str, max_connections: u32, access: Access) -> Result<Self, Error> {
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
        let pool = lazy_pool(&options, max_connections, acquire_timeout);
        // PG10: the mode the connection is opened under is logged, and
        // [`Pool::ssl_mode`] reports the same value to `doctor`. The driver
        // exposes no post-handshake negotiation state, so this is the resolved
        // policy rather than a second reading of the wire.
        tracing::info!(
            mode = %parsed.mode(),
            host = %parsed.host(),
            database = %parsed.database(),
            read_only = !parsed.access().is_writable(),
            max_connections = max_connections.max(1),
            "postgres pool opened"
        );
        let metrics = attach(pool.clone(), max_connections);
        Ok(Self {
            pool,
            mode: parsed.mode(),
            host: parsed.host().to_owned(),
            database: parsed.database().to_owned(),
            access: parsed.access(),
            options,
            redacted: parsed.redacted().to_owned(),
            _metrics: metrics,
        })
    }

    /// This pool, re-opened in the session `access` names (PG6, layer 2).
    ///
    /// A store's `connect_with_pool` cannot tell what session the pool it was
    /// handed opens, so it asks for the session it needs: a pool that already
    /// carries the mode is returned as it is, and a **read-only** store over a
    /// writable pool gets a pool whose connections are opened with
    /// `default_transaction_read_only = on` (the size, the acquire timeout and
    /// the options are this pool's own, with the read-only startup option
    /// appended last, so nothing the source pool carried is loosened).
    ///
    /// The other direction is [`Error::ReadOnlyPoolForAWriter`]: a read-only
    /// pool's options carry the startup option this crate appended and a pool
    /// holds no un-redacted dsn to rebuild them from, so a writable store over
    /// one is refused rather than repaired.
    pub(crate) fn reopened_for(&self, access: Access) -> Result<Self, Error> {
        if self.access == access {
            return Ok(self.clone());
        }
        if access.is_writable() {
            return Err(Error::ReadOnlyPoolForAWriter);
        }
        tracing::warn!(
            host = %self.host,
            database = %self.database,
            "the pool handed to a read-only store opens writable sessions: re-opened with \
             default_transaction_read_only = on (PG6, layer 2)"
        );
        let options = self.options.clone().options([READ_ONLY_SESSION_OPTION]);
        let pool = lazy_pool(
            &options,
            self.max_connections(),
            self.pool.options().get_acquire_timeout(),
        );
        let metrics = attach(pool.clone(), self.max_connections());
        Ok(Self {
            pool,
            mode: self.mode,
            host: self.host.clone(),
            database: self.database.clone(),
            access,
            options,
            redacted: self.redacted.clone(),
            _metrics: metrics,
        })
    }

    /// The TLS mode the dsn asked for (PG10; `doctor` reports it).
    pub fn ssl_mode(&self) -> SslMode {
        self.mode
    }

    /// The session mode every connection this pool opens carries (PG6).
    pub fn access(&self) -> Access {
        self.access
    }

    /// Whether this pool's sessions are opened read-only (PG6).
    pub fn is_read_only(&self) -> bool {
        !self.access.is_writable()
    }

    /// The host the dsn names.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The database the session reaches: the dsn's own, or the user's when the
    /// dsn names none (libpq's own default, which the driver leaves to the
    /// server's startup packet).
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

    use super::{
        DEFAULT_POOL_SIZE, Dsn, Environment, Pool, SslMode, named_parameter, process_environment,
    };
    use crate::{
        Access,
        error::{Error, redact_userinfo},
    };

    /// An environment with nothing in it: the dsn is then the only decider, which
    /// is what most of these legs are about.
    fn no_environment() -> impl Fn(&str) -> Option<String> {
        |_| None
    }

    /// One variable, as a deployment would export it.
    fn environment(variable: &'static str, value: &str) -> impl Fn(&str) -> Option<String> {
        let value = value.to_owned();
        move |name: &str| (name == variable).then(|| value.clone())
    }

    /// A parse with the libpq environment emptied, so a leg's answer is the
    /// dsn's own: a `PGSSLMODE` on the developer's host must not decide whether
    /// these legs pass.
    fn parse(dsn: &str) -> Result<Dsn, Error> {
        Dsn::parse_with(dsn, Access::ReadWrite, &no_environment())
    }

    /// The same, read-only (PG6, layer 2).
    fn parse_read_only(dsn: &str) -> Result<Dsn, Error> {
        Dsn::parse_with(dsn, Access::ReadOnly, &no_environment())
    }

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
        let dsn = parse("postgres://user:password@db.example:5432/tinio?sslmode=require")
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

    /// An omitted mode resolves the same way a spelled `require` does, and the
    /// session is opened under the driver's chain-and-hostname check — the
    /// promise PG10 makes and the one `prefer` could never keep (R1).
    #[test]
    fn an_omitted_mode_resolves_to_verify_full() {
        let dsn = parse("postgres://user:password@db.example:5432/tinio").expect("the dsn parses");
        assert_eq!(dsn.mode(), SslMode::Require);
        assert!(
            matches!(
                dsn.options().get_ssl_mode(),
                sqlx::postgres::PgSslMode::VerifyFull
            ),
            "an omission verifies the chain and the hostname"
        );
    }

    #[test]
    fn an_explicit_disable_stays_plaintext() {
        let dsn = parse("postgres://user:password@db.example:5432/tinio?sslmode=disable")
            .expect("the dsn parses");
        assert_eq!(dsn.mode(), SslMode::Disable);
        assert!(matches!(
            dsn.options().get_ssl_mode(),
            sqlx::postgres::PgSslMode::Disable
        ));
    }

    /// Every spelling outside the store's two is refused, and the refusal names
    /// both accepted modes and the one that gives up TLS (R1).
    #[test]
    fn every_other_mode_is_refused_with_the_two_that_are_served() {
        for value in ["prefer", "allow", "verify-ca", "verify-full", "bogus"] {
            let dsn = format!("postgres://user:password@db.example:5432/tinio?sslmode={value}");
            let err = parse(&dsn).expect_err("a mode this store does not serve is refused");
            let message = err.to_string();
            assert!(
                matches!(&err, Error::SslMode { reason } if reason.contains(value)),
                "the refusal quotes the spelling: {message}"
            );
            assert!(message.contains("`disable`"), "{message}");
            assert!(message.contains("`require`"), "{message}");
            assert!(
                message.contains("only an explicit `disable` gives up TLS"),
                "the remedy names the one plaintext mode: {message}"
            );
        }
    }

    /// `?SSLMODE=require` is **refused**, not read as an omission: sqlx matches
    /// query keys case-sensitively, so the driver would never see the mode the
    /// operator wrote — the review's root cause, where the old dsn-text probe
    /// lowercased the text and the dsn was then served at the driver's
    /// plaintext-allowing default (R1).
    #[test]
    fn a_key_the_drivers_parse_ignores_is_refused_rather_than_downgraded() {
        let err = parse("postgres://user:password@db.example:5432/tinio?SSLMODE=require")
            .expect_err("an uppercase key is refused");
        let message = err.to_string();
        assert!(message.contains("SSLMODE"), "{message}");
        assert!(message.contains("another case"), "{message}");
        // And the same for the lowercased-but-not-matching `ssl-mode` alias the
        // driver does accept, spelled in another case.
        assert!(parse("postgres://user:password@db.example:5432/tinio?SslMode=disable").is_err());
    }

    /// A dsn that only *mentions* a mode in another parameter's value is an
    /// omission (and verifies), which is what makes the text read a parameter
    /// read rather than a substring search.
    #[test]
    fn a_mention_inside_another_value_is_not_a_mode() {
        let mentioned = parse(
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
        assert_eq!(
            named_parameter(
                "postgres://db.example/tinio?application_name=sslmode=require",
                "sslmode"
            ),
            super::NamedParameter::Absent
        );
    }

    /// The last occurrence wins, as it does in the driver's own parse, and the
    /// key must be a whole parameter name.
    #[test]
    fn a_parameter_is_read_with_the_drivers_own_rules() {
        assert_eq!(
            named_parameter("postgres://db.example/tinio?sslmode=require", "sslmode"),
            super::NamedParameter::Named("require")
        );
        assert_eq!(
            named_parameter(
                "postgres://db.example/tinio?sslmode=require&sslmode=disable",
                "sslmode"
            ),
            super::NamedParameter::Named("disable")
        );
        assert_eq!(
            named_parameter("postgres://db.example/tinio?plansslmode=require", "sslmode"),
            super::NamedParameter::Absent
        );
        assert_eq!(
            named_parameter("postgres://db.example/tinio#sslmode=require", "sslmode"),
            super::NamedParameter::Absent,
            "a fragment is not a query"
        );
        // The store's reading and the driver's parse agree on both spellings of
        // a value's case, because the driver lowercases what it parses.
        let dsn = parse("postgres://user:password@db.example:5432/tinio?sslmode=REQUIRE")
            .expect("the value's case is the driver's to fold");
        assert_eq!(dsn.mode(), SslMode::Require);
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

    /// A session this crate opens is nameable in `pg_stat_activity`, a name the
    /// dsn itself carries is the operator's and is left alone, and an
    /// environment's `PGAPPNAME` never renames one: the name is what the
    /// ownership refusal and `doctor` report (PG12).
    #[test]
    fn a_session_is_named_by_the_dsn_or_by_this_crate_never_by_the_environment() {
        let dsn = parse("postgres://user:password@db.example:5432/tinio").expect("the dsn parses");
        assert_eq!(
            dsn.options().get_application_name(),
            Some("tinio-store-pg"),
            "the crate's own sessions are nameable"
        );
        let named =
            parse("postgres://user:password@db.example:5432/tinio?application_name=worker-7")
                .expect("the dsn parses");
        assert_eq!(named.options().get_application_name(), Some("worker-7"));
        let empty = parse("postgres://user:password@db.example:5432/tinio?application_name=")
            .expect("the dsn parses");
        assert_eq!(
            empty.options().get_application_name(),
            Some("tinio-store-pg"),
            "an empty name is no name"
        );
        // The environment's own name would have been seeded into the options by
        // the driver; the dsn's silence is what keeps it out.
        let env = environment("PGAPPNAME", "someone-elses-worker");
        let seeded = Dsn::parse_with(
            "postgres://user:password@db.example:5432/tinio",
            Access::ReadWrite,
            &env,
        )
        .expect("the dsn parses");
        assert_eq!(
            seeded.options().get_application_name(),
            Some("tinio-store-pg"),
            "PGAPPNAME cannot rename a store's session"
        );
        // A dsn that does name one still wins over the environment.
        let named_in_dsn = Dsn::parse_with(
            "postgres://user:password@db.example:5432/tinio?application_name=worker-7",
            Access::ReadWrite,
            &env,
        )
        .expect("the dsn parses");
        assert_eq!(
            named_in_dsn.options().get_application_name(),
            Some("worker-7")
        );
    }

    /// The other half of finding 2: an environment that would serve the dsn's
    /// `require` below it is refused by name, so `PGSSLMODE=disable` cannot turn
    /// an omitted mode into plaintext (PG10, R1).
    #[test]
    fn a_weakening_environment_is_refused_by_name() {
        for value in ["disable", "prefer", "allow", "bogus"] {
            let env = environment("PGSSLMODE", value);
            let err = Dsn::parse_with(
                "postgres://user:password@db.example:5432/tinio",
                Access::ReadWrite,
                &env,
            )
            .expect_err("an environment below the dsn's require is refused");
            let message = err.to_string();
            assert!(message.contains("PGSSLMODE"), "{message}");
            assert!(
                message.contains("unset PGSSLMODE"),
                "the refusal names the remedy: {message}"
            );
        }
        // A spell `require` dsn under the same environment is still refused:
        // the environment contradicts the dsn's own text.
        let env = environment("PGSSLMODE", "prefer");
        assert!(
            Dsn::parse_with(
                "postgres://user:password@db.example:5432/tinio?sslmode=require",
                Access::ReadWrite,
                &env,
            )
            .is_err()
        );
        // The modes that ask for as much as the store's `require` are not a
        // weakening: the store writes its own resolved mode onto the session.
        for value in ["require", "verify-ca", "verify-full"] {
            let env = environment("PGSSLMODE", value);
            let parsed = Dsn::parse_with(
                "postgres://user:password@db.example:5432/tinio",
                Access::ReadWrite,
                &env,
            )
            .expect("an environment that does not weaken the dsn is not a refusal");
            assert_eq!(parsed.mode(), SslMode::Require);
            assert!(
                matches!(
                    parsed.options().get_ssl_mode(),
                    sqlx::postgres::PgSslMode::VerifyFull
                ),
                "the dsn's remain resolves to the store's chain-and-hostname check"
            );
        }
        // An explicit plaintext dsn is the operator's own decision, so an
        // environment beside it is not a refusal.
        assert!(
            Dsn::parse_with(
                "postgres://user:password@db.example:5432/tinio?sslmode=disable",
                Access::ReadWrite,
                &environment("PGSSLMODE", "require"),
            )
            .is_ok()
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
        let dsn =
            parse("postgres://tinio:sup3rsecret@db.internal:5432/tinio").expect("the dsn parses");
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
            r#"Pool { mode: Disable, host: "db.internal", database: "tinio", access: ReadWrite, .. }"#
        );
        assert!(!rendered.contains("sup3rsecret"), "{rendered}");
    }

    /// A dsn that names no database connects to one named after the user — the
    /// server's own default for a startup packet with no `database` — and that
    /// is the name the pool, `doctor` and every connect failure report (PG5,
    /// R9's reporting half).
    #[tokio::test]
    async fn a_dsn_with_no_database_names_the_users_own() {
        let pool =
            Pool::open("postgres://tinio:sup3rsecret@db.example:5432", 1).expect("the dsn parses");
        assert_eq!(
            pool.database(),
            "tinio",
            "the effective database is the user's, so nothing renders empty"
        );
        assert!(format!("{pool:?}").contains("database: \"tinio\""));
    }

    /// The read-only pool (PG6, layer 2) carries `default_transaction_read_only
    /// = on` as a **startup option** — the driver's `options` parameter, which
    /// the server applies as the session opens, before any statement — and the
    /// writable pool does not. The rendering names the mode.
    #[test]
    fn a_read_only_pool_opens_its_session_read_only() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        let writable = parse(dsn).expect("the dsn parses");
        assert!(writable.access().is_writable());
        assert!(
            !format!("{:?}", writable.options()).contains("default_transaction_read_only"),
            "a writable session carries no read-only option"
        );

        let read_only = parse_read_only(dsn).expect("the dsn parses");
        assert!(!read_only.access().is_writable());
        let rendered = format!("{:?}", read_only.options());
        assert!(
            rendered.contains("default_transaction_read_only=on"),
            "the option is `-c default_transaction_read_only=on` in the startup \
             options: {rendered}"
        );
    }

    /// A dsn that names its own `options` keeps them: the read-only setting is
    /// appended, never substituted — and appended **last**, so a dsn's own
    /// `default_transaction_read_only=off` cannot outrank it.
    #[test]
    fn the_read_only_option_rides_beside_a_dsn_own_options() {
        let parsed = parse_read_only(
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
        let overridden = parse_read_only(
            "postgres://user:password@db.example:5432/tinio?options=-c%20default_transaction_read_only%3Doff",
        )
        .expect("the dsn parses");
        let rendered = format!("{:?}", overridden.options());
        assert!(
            rendered.rfind("default_transaction_read_only=on")
                > rendered.find("default_transaction_read_only=off"),
            "the store's read-only option is last, so it wins: {rendered}"
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

    /// PG6, layer 2: a store that was handed a pool it did not build can still
    /// tell what session that pool opens, and gets the one it asked for. A
    /// read-only request is **re-opened** read-only (the read-only startup
    /// option is appended to the pool's own options), and a writable request
    /// over a read-only pool is refused because that direction cannot be
    /// rebuilt.
    #[tokio::test]
    async fn a_store_re_opens_a_pool_whose_session_is_the_wrong_mode() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        let writable = Pool::open(dsn, 3).expect("the dsn parses");
        let read_only = writable
            .reopened_for(Access::ReadOnly)
            .expect("a read-only store re-opens a writable pool");
        assert!(read_only.is_read_only());
        assert_eq!(read_only.max_connections(), 3, "the size is the source's");
        assert_eq!(read_only.host(), writable.host());
        assert_eq!(read_only.database(), writable.database());
        assert_eq!(read_only.ssl_mode(), SslMode::Require);
        let rendered = format!("{:?}", read_only.dedicated_options());
        assert!(
            rendered.contains("default_transaction_read_only=on"),
            "every connection the re-opened pool opens is read-only: {rendered}"
        );
        // A pool that already carries the mode is returned as it is.
        let same = read_only
            .reopened_for(Access::ReadOnly)
            .expect("a matching pool is used as it is");
        assert!(same.is_read_only());
        // The direction that cannot be rebuilt: the read-only option is
        // appended to a pool's options and nothing holds the un-redacted dsn.
        let err = read_only
            .reopened_for(Access::ReadWrite)
            .expect_err("a writable store over a read-only pool is refused");
        assert!(
            matches!(err, Error::ReadOnlyPoolForAWriter),
            "the refusal is the store's own: {err}"
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

    /// The environment seam is the process's own unless a leg hands in another,
    /// and the process's is what every ordinary parse reads.
    #[test]
    fn the_default_environment_is_the_process_own() {
        let env: Environment<'_> = &process_environment;
        assert_eq!(env("TINIO_PG_NO_SUCH_VARIABLE"), None);
        assert_eq!(
            process_environment("TINIO_PG_NO_SUCH_VARIABLE"),
            None,
            "an unset variable is absent, not empty"
        );
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
