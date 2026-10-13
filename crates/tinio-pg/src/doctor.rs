//! The doctor's pg half: the checks a PostgreSQL instance is diagnosed by.
//!
//! `tinio doctor` prints these rows for a pg deployment, and the CLI's `commands::doctor` module
//! is the only caller — it owns the report, the severities' rendering, and the addressing rule
//! (ruling R8), while this module owns the questions and the store calls behind them.
//!
//! **A diagnosis never mutates its subject** (PG11): every store fact here is read through
//! [`PgStore::inspect_with_pool`], the non-migrating path over the diagnosis's own pool, and
//! never through [`PgStore::connect`], which migrates. The rows are:
//!
//! - connectivity **through the pool** (a `SELECT 1` on a pool this module opens), and the
//!   pool's own health line (PG9);
//! - the database's encoding, read directly so the row can name what it found (PG4);
//! - the negotiated TLS mode (PG10);
//! - the applied schema version and any pending migration, through `inspect_with_pool` (PG11);
//! - the read-only flag and the role's DML grants (PG6);
//! - the ownership lock's holder (PG12).
//!
//! Every row carries a remedy an operator can act on. The migration and encoding remedies name
//! a **dump/restore path** and explicitly never "delete and rebuild", because these tables hold
//! the only copy of the bytes (PG11).

use secrecy::ExposeSecret;

use crate::{
    _store_pg::{
        Error as StoreError, PgStore, SchemaState,
        diagnostics::{RolePrivileges, TlsState, role_privileges, server_encoding, tls_state},
        pool::{Pool, SslMode},
    },
    Access, Config,
};

/// The row name for the connectivity check.
pub const CONNECTION: &str = "postgres connection";
/// The row name for the pool-health check.
pub const POOL: &str = "postgres pool";
/// The row name for the server-encoding check (PG4).
pub const ENCODING: &str = "postgres encoding";
/// The row name for the negotiated TLS mode (PG10).
pub const TLS: &str = "postgres tls";
/// The row name for the applied/pending schema version (PG11).
pub const SCHEMA: &str = "postgres schema";
/// The row name for the read-only flag and the role's DML grants (PG6).
pub const ACCESS: &str = "postgres access";
/// The row name for the ownership lock's holder (PG12).
pub const OWNERSHIP: &str = "postgres ownership";

/// How serious one doctor row is — **the CLI report's own type**, not a second spelling of it.
///
/// These rows are folded into that report, so the three levels live in `tinio-core`
/// ([`tinio_core::severity`]) rather than being spelled once per crate and bridged by a
/// hand-written mapping whose arms are checked only where someone remembered to write them.
///
/// # Examples
///
/// ```
/// use tinio_pg::doctor::Severity;
///
/// assert_eq!(Severity::Error.to_string(), "error");
/// assert!(Severity::Error > Severity::Warn);
/// ```
pub use tinio_core::severity::Severity;

/// One pg doctor row: what was checked, how bad it is, and one line of detail.
///
/// # Examples
///
/// ```
/// use tinio_pg::doctor::{Check, Severity};
///
/// let row = Check::new("postgres pool", Severity::Ok, "open: 8 configured, 1 idle");
/// assert_eq!(row.severity, Severity::Ok);
/// assert!(row.detail.contains("idle"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// What was checked (one of this module's row-name constants).
    pub name: &'static str,
    /// How bad it is.
    pub severity: Severity,
    /// What was found, in one line.
    pub detail: String,
}

impl Check {
    /// A row.
    ///
    /// # Examples
    ///
    /// ```
    /// use tinio_pg::doctor::{Check, Severity};
    ///
    /// let row = Check::new(
    ///     "postgres schema",
    ///     Severity::Ok,
    ///     "schema version 1 is current",
    /// );
    /// assert_eq!(row.name, "postgres schema");
    /// ```
    pub fn new(name: &'static str, severity: Severity, detail: impl Into<String>) -> Self {
        Self {
            name,
            severity,
            detail: detail.into(),
        }
    }

    /// The "this leg did not run" row: a warn, never silent, with the reason.
    fn not_inspected(name: &'static str, reason: &str) -> Self {
        Self::new(name, Severity::Warn, format!("not inspected: {reason}"))
    }
}

/// The role facts the access row (PG6) is built from are the store's own [`RolePrivileges`]: the
/// statement's type stays the store's (it owns SQL) and the row prose stays here.
///
/// Diagnose `config`'s database in the mode `access` names, one [`Check`] per question.
///
/// The pool this opens is the diagnosis's own, and the store facts are read through that same
/// pool ([`PgStore::inspect_with_pool`]) — one pool's worth of connections for the length of
/// the call, **in the mode `access` names**, so a read-only run opens no writable session
/// anywhere in this path (PG6). Nothing is written on any path, and the store facts come from
/// the non-migrating inspection rather than from a migrating connect (PG11).
///
/// A dsn the pool cannot even parse answers one row and stops: without a pool there is nothing
/// left to ask. Every other leg reports its own answer or its own "not inspected" reason, so a
/// server that is down still produces the TLS row and a schema that cannot be read still
/// produces the connection and encoding rows.
///
/// # Examples
///
/// ```no_run
/// use tinio_pg::{Access, Config, doctor};
/// use tinio_util::secret::{Secret, SecretKey};
///
/// # async fn example() {
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// let rows = doctor::diagnose(&Config::new(dsn, 8), Access::ReadOnly).await;
/// // One row per check: connection, pool, encoding, tls, schema, access, ownership.
/// assert!(rows.iter().any(|row| row.name == doctor::SCHEMA));
/// # }
/// ```
pub async fn diagnose(config: &Config, access: Access) -> Vec<Check> {
    let dsn = config.dsn().expose_secret().as_ref();
    let size = config.pool();
    let mut rows = Vec::new();

    let pool = match Pool::open_for(dsn, size, access) {
        Ok(pool) => pool,
        // The dsn never renders its password (PG5), and neither does the pool's error.
        Err(err) => {
            rows.push(Check::new(
                CONNECTION,
                Severity::Error,
                format!(
                    "the dsn could not be used: {err}. The instance is addressed by \
                         TINIO_PG_DSN (or `--pg-dsn`), else by `[storage.pg] dsn`"
                ),
            ));
            return rows;
        }
    };

    // Connectivity **through the pool**: a connection the pool handed out. The pool is opened with
    // `test_before_acquire` (PG9), so a connection it hands over is one the driver has already
    // proved live — no statement of this module's own is needed to ask.
    let connected = pool.acquire().await.is_ok();
    rows.push(connection_check(
        connected,
        pool.host(),
        pool.database(),
        pool.max_connections(),
    ));
    rows.push(pool_check(
        pool.is_closed(),
        pool.max_connections(),
        pool.num_idle(),
    ));
    // PG10: the row reports what the server negotiated, not the dsn's intention — a `disable` dsn
    // is plaintext whatever the server would have accepted, so the authority is `pg_stat_ssl` for
    // the connection this pool hands out. The probe runs after the pool is open, and its own
    // failure falls back to the policy-only row, so a dsn the pool could not open still renders
    // what was asked for.
    let tls = tls_state(&pool).await;
    rows.push(tls_check(
        pool.ssl_mode(),
        tls.as_ref().map_err(|err| err.to_string()),
    ));

    match server_encoding(&pool).await {
        Ok(encoding) => rows.push(encoding_check(&encoding)),
        Err(err) => rows.push(Check::not_inspected(ENCODING, &err.to_string())),
    }

    match PgStore::inspect_with_pool(pool.clone(), access).await {
        Ok(store) => {
            rows.push(schema_check(Ok(store.schema())));
            rows.push(match store.ownership_holder().await {
                Ok(holder) => ownership_check(holder.as_deref()),
                Err(err) => Check::not_inspected(OWNERSHIP, &err.to_string()),
            });
        }
        Err(err) => {
            let ownership = Check::not_inspected(OWNERSHIP, &err.to_string());
            rows.push(schema_check(Err(err)));
            rows.push(ownership);
        }
    }

    rows.push(match role_privileges(&pool).await {
        Ok(privileges) => access_check(access, Some(&privileges)),
        Err(err) => access_check_without_role(access, &err.to_string()),
    });

    pool.close().await;
    rows
}

/// The remedy both migration rows share: the version this binary requires, and the dump/restore
/// path — never "delete and rebuild", because these tables hold the only copy of the bytes.
const PG_REMEDY: &str = "never drop and rebuild, because these tables hold the only copy of \
                          the bytes: upgrade the binary to one carrying the database's schema \
                          version, or dump the database with pg_dump and restore it into a \
                          database this binary created";

/// The connectivity row. `connected` is what the pool's `SELECT 1` answered.
fn connection_check(connected: bool, host: &str, database: &str, max: u32) -> Check {
    if connected {
        Check::new(
            CONNECTION,
            Severity::Ok,
            format!("connected to {host}/{database} through a pool of {max}"),
        )
    } else {
        Check::new(
            CONNECTION,
            Severity::Error,
            format!(
                "{host}/{database} did not answer through the pool: the instance is addressed by \
                 TINIO_PG_DSN (or `--pg-dsn`), else by `[storage.pg] dsn` — check the server is \
                 running, the dsn is right, and the role may connect"
            ),
        )
    }
}

/// The pool-health row: the configured size and the idle count, with a closed pool a concern.
fn pool_check(closed: bool, max: u32, idle: usize) -> Check {
    if closed {
        Check::new(
            POOL,
            Severity::Warn,
            format!(
                "closed: {max} connections were configured and the pool no longer hands any out"
            ),
        )
    } else {
        Check::new(
            POOL,
            Severity::Ok,
            format!("open: {max} connections configured, {idle} idle"),
        )
    }
}

/// The encoding row: PG4 refuses anything but UTF-8, so the row names what it found.
fn encoding_check(encoding: &str) -> Check {
    if encoding.eq_ignore_ascii_case("UTF8") {
        return Check::new(ENCODING, Severity::Ok, "server encoding is UTF-8");
    }
    Check::new(
        ENCODING,
        Severity::Error,
        format!(
            "the server encoding is {encoding}, not UTF-8: COLLATE \"C\" orders the server \
             encoding's bytes, not the contract's UTF-8 sequence (PG4). A database's encoding is \
             fixed at creation, so pg_dump the database and restore it into one created with \
             ENCODING 'UTF8' TEMPLATE template0 — never drop and rebuild, because these tables \
             hold the only copy of the bytes"
        ),
    )
}

/// The TLS row (PG10): what the server negotiated, beside the policy the dsn resolved.
///
/// The authority is the server (`pg_stat_ssl`, through [`tls_state`]), not the dsn: a `disable`
/// dsn is plaintext whatever the server would have accepted, and only the server can say what was
/// actually negotiated. The policy stays on the row because it is what the operator configured
/// *and* what explains the negotiation: `require` is a promise, `disable` the one way to give TLS
/// up.
///
/// `negotiated` is the probe's own answer — `Err` when the state could not be read at all (a
/// server that never answered), in which case the row falls back to the policy alone, so a down
/// server still renders what was asked for.
///
/// **A `require` policy over a plaintext negotiation is an `error`, not information.** The store
/// maps `require` — and an omitted `sslmode` — onto the driver's verify-full, which checks the
/// chain *and* the hostname and never downgrades, so the combination cannot come from a correct
/// dsn: it means the guarantee the policy makes did not hold, and it is exactly the finding this
/// row exists to surface.
fn tls_check(mode: SslMode, negotiated: Result<&TlsState, String>) -> Check {
    match negotiated {
        Err(reason) => tls_without_state(mode, &reason),
        Ok(state) if state.enabled => tls_encrypted_check(mode, state),
        Ok(_) => tls_plaintext_check(mode),
    }
}

/// The encrypted arm: the server's negotiation, beside the policy.
fn tls_encrypted_check(mode: SslMode, state: &TlsState) -> Check {
    let what = negotiated_tls(state);
    match mode {
        SslMode::Require => Check::new(
            TLS,
            Severity::Ok,
            format!(
                "negotiated {what}; policy sslmode require verifies the chain and the hostname"
            ),
        ),
        SslMode::Disable => Check::new(
            TLS,
            Severity::Ok,
            format!("negotiated {what}, although policy sslmode disable asked for plaintext"),
        ),
    }
}

/// The plaintext arm: the server says the session is in the clear, so the row is a finding for
/// any policy that did not ask for it (see [`tls_check`] on why `require` is the `error`).
fn tls_plaintext_check(mode: SslMode) -> Check {
    match mode {
        SslMode::Require => Check::new(
            TLS,
            Severity::Error,
            "policy sslmode require, but the server negotiated plaintext: a require dsn verifies \
             the chain and the hostname and never downgrades, so this is a guarantee that did not \
             hold — check the dsn and the server's TLS configuration",
        ),
        SslMode::Disable => Check::new(
            TLS,
            Severity::Ok,
            "plaintext, and only because the dsn says sslmode disable (an omitted sslmode would be \
             require)",
        ),
    }
}

/// The negotiated TLS rendered for the row: the version, the cipher and the key length the server
/// reported. A field the server left `NULL` is named rather than dropped, so the row never implies
/// more than it read.
fn negotiated_tls(state: &TlsState) -> String {
    let version = state.version.as_deref().unwrap_or("an unstated version");
    let cipher = state.cipher.as_deref().unwrap_or("an unstated cipher");
    match state.bits {
        Some(bits) => format!("{version} with {cipher} ({bits}-bit key)"),
        None => format!("{version} with {cipher}"),
    }
}

/// The policy-only row, for when the server's TLS state could not be read: the mode the dsn
/// resolved to, with the reason the fact is missing. The severity is the policy's own, because a
/// server that never answered is the connection row's error — not a second one here.
fn tls_without_state(mode: SslMode, reason: &str) -> Check {
    let (severity, policy) = match mode {
        SslMode::Require => (
            Severity::Ok,
            "policy sslmode require (the chain and the hostname are both verified)",
        ),
        SslMode::Disable => (
            Severity::Ok,
            "policy sslmode disable (plaintext, and only because the dsn says so)",
        ),
    };
    Check::new(
        TLS,
        severity,
        format!("{policy}; the server's negotiated state could not be read: {reason}"),
    )
}

/// The schema row (PG11): current, behind (pending migrations) or ahead (refused). The store's own
/// refusals ([`StoreError::SchemaBehind`] and [`StoreError::SchemaTooNew`]) carry both versions, so
/// the row renders the pg remedy without a second read; anything else is "not inspected" with the
/// store's message.
///
/// **A database ahead of the binary takes the pg remedy on every path**, including the version a
/// read-only inspection *reports* rather than refuses: "migrate it forward with a writable start"
/// is advice a writable start answers with the refusal itself, so the row that reports an
/// ahead-of-binary state states what actually remedies it — a binary carrying that schema, or a
/// dump/restore.
fn schema_check(state: Result<SchemaState, StoreError>) -> Check {
    match state {
        Ok(state) if state.matches() => Check::new(
            SCHEMA,
            Severity::Ok,
            format!("schema version {} is current", state.applied),
        ),
        Ok(state) if state.applied > state.supported => Check::new(
            SCHEMA,
            Severity::Error,
            format!(
                "the database records schema {}, newer than this binary's {}: {PG_REMEDY}",
                state.applied, state.supported
            ),
        ),
        Ok(state) => Check::new(
            SCHEMA,
            Severity::Warn,
            format!(
                "the database records schema {} and this binary carries {}: {} migration(s) are \
                 pending — migrate it forward with a writable start, and {PG_REMEDY}",
                state.applied,
                state.supported,
                state.pending()
            ),
        ),
        Err(StoreError::SchemaBehind {
            found: applied,
            supported,
        }) => Check::new(
            SCHEMA,
            Severity::Warn,
            format!(
                "the database records schema {applied} and this binary carries {supported}: \
                 {pending} migration(s) are pending, and this diagnosis never migrates its \
                 subject — start a writable instance to migrate it forward, and {PG_REMEDY}",
                pending = (supported - applied).max(0)
            ),
        ),
        Err(StoreError::SchemaTooNew {
            found: applied,
            supported,
        }) => Check::new(
            SCHEMA,
            Severity::Error,
            format!(
                "the database records schema {applied}, newer than this binary's {supported}: \
                 {PG_REMEDY}"
            ),
        ),
        Err(StoreError::SchemaChecksum {
            version,
            found,
            expected,
        }) => Check::new(
            SCHEMA,
            Severity::Error,
            format!(
                "the database's recorded checksum for schema version {version} ({found}) is not \
                 this binary's ({expected}): the schema it was built from is not the schema this \
                 binary serves, so nothing may be read through it — {PG_REMEDY}"
            ),
        ),
        Err(err) => Check::not_inspected(SCHEMA, &err.to_string()),
    }
}

/// The access row (PG6): the inspecting mode, and whether the role carries the DML grants that
/// are the privilege layer beneath the read-only entry guard. The role facts are the store's own
/// [`RolePrivileges`], taken as they come.
///
/// **A probe that checked no schema is not the green row.** An empty `schemas` means the database
/// holds none of the store's own relations — a wrong dsn, or a session whose `search_path` names
/// another schema — and "the role does not hold DML grants" would then certify a role nothing was
/// looked at (the 2026-10-13 review). The row says so instead, and names the schemas it did check
/// when it checked any.
fn access_check(access: Access, privileges: Option<&RolePrivileges>) -> Check {
    match privileges {
        None => access_check_without_role(access, "the role's privileges could not be read"),
        Some(privileges) => {
            let mode = if access.is_writable() {
                "read-write"
            } else {
                "read-only"
            };
            if privileges.schemas.is_empty() {
                return Check::new(
                    ACCESS,
                    Severity::Warn,
                    format!(
                        "the inspecting session is {mode}, and the role's DML grants were not \
                         checked: this database holds none of the store's own relations (no schema \
                         carries its `schema_version` table), so there was nothing to check — a \
                         wrong database, or a store that has never been migrated (PG6, PG11)"
                    ),
                );
            }
            if !access.is_writable() && privileges.can_write {
                return Check::new(
                    ACCESS,
                    Severity::Error,
                    format!(
                        "the inspecting session is read-only, but the role `{}` holds \
                         INSERT/UPDATE/DELETE grants in `{}`: a read-only deployment's role must \
                         carry none (PG6), so the entry guard is the only layer standing. REVOKE \
                         INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA \"{}\" FROM \"{}\", or \
                         connect read-only with a role that has only SELECT",
                        privileges.role, privileges.schemas, privileges.schemas, privileges.role
                    ),
                );
            }
            Check::new(
                ACCESS,
                Severity::Ok,
                format!(
                    "the inspecting session is {mode}; the role `{}` {} DML grants in `{}`",
                    privileges.role,
                    if privileges.can_write {
                        "holds"
                    } else {
                        "does not hold"
                    },
                    privileges.schemas
                ),
            )
        }
    }
}

/// The access row when the role probe itself failed: the flag is still reported, the privilege
/// layer as "not inspected".
fn access_check_without_role(access: Access, reason: &str) -> Check {
    let mode = if access.is_writable() {
        "read-write"
    } else {
        "read-only"
    };
    Check::new(
        ACCESS,
        Severity::Warn,
        format!(
            "the inspecting session is {mode}; the role's privileges are not inspected: {reason}"
        ),
    )
}

/// The ownership row (PG12): who holds the lock, or that nobody does.
fn ownership_check(holder: Option<&str>) -> Check {
    match holder {
        Some(holder) => Check::new(
            OWNERSHIP,
            Severity::Ok,
            format!("the ownership lock is held by {holder}"),
        ),
        None => Check::new(
            OWNERSHIP,
            Severity::Ok,
            "no writable instance holds the ownership lock; read-only instances take none (PG12)",
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::absolute_paths)]

    use super::*;

    #[test]
    fn a_connected_pool_names_the_instance_and_its_size() {
        let row = connection_check(true, "db.example", "tinio", 8);
        assert_eq!(row.severity, Severity::Ok);
        assert!(row.detail.contains("db.example/tinio"), "{}", row.detail);
        assert!(row.detail.contains("8"), "{}", row.detail);
    }

    #[test]
    fn a_failed_connection_names_both_remedies() {
        let row = connection_check(false, "db.example", "tinio", 8);
        assert_eq!(row.severity, Severity::Error);
        // R8: the channel that addresses a pg instance is named.
        assert!(row.detail.contains("TINIO_PG_DSN"), "{}", row.detail);
        assert!(row.detail.contains("storage.pg"), "{}", row.detail);
    }

    #[test]
    fn a_closed_pool_is_a_warning_and_an_open_one_is_not() {
        assert_eq!(pool_check(false, 8, 2).severity, Severity::Ok);
        assert_eq!(pool_check(true, 8, 0).severity, Severity::Warn);
        assert!(pool_check(false, 8, 2).detail.contains("2 idle"));
    }

    #[test]
    fn utf8_is_ok_and_anything_else_names_the_remedy() {
        assert_eq!(encoding_check("UTF8").severity, Severity::Ok);
        assert_eq!(encoding_check("utf8").severity, Severity::Ok);
        let row = encoding_check("LATIN1");
        assert_eq!(row.severity, Severity::Error);
        assert!(row.detail.contains("LATIN1"), "{}", row.detail);
        assert!(row.detail.contains("UTF8"), "{}", row.detail);
        // PG11/PG4: the remedy is a dump/restore, never a rebuild.
        assert!(row.detail.contains("pg_dump"), "{}", row.detail);
        assert!(!row.detail.contains("delete and rebuild"), "{}", row.detail);
    }

    /// A [`TlsState`] as the server reports it: `on` builds an encrypted session with its version,
    /// cipher and key length; anything else is the plaintext session the compose fixture serves.
    fn tls(on: bool) -> TlsState {
        if on {
            TlsState {
                enabled: true,
                version: Some("TLSv1.3".to_owned()),
                cipher: Some("TLS_AES_256_GCM_SHA384".to_owned()),
                bits: Some(256),
            }
        } else {
            TlsState {
                enabled: false,
                version: None,
                cipher: None,
                bits: None,
            }
        }
    }

    #[test]
    fn the_tls_row_reports_the_negotiated_mode_the_server_reports() {
        // Encrypted: the version, cipher and key length, with the policy beside them.
        let on = tls_check(SslMode::Require, Ok(&tls(true)));
        assert_eq!(on.severity, Severity::Ok);
        assert!(on.detail.contains("TLSv1.3"), "{}", on.detail);
        assert!(
            on.detail.contains("TLS_AES_256_GCM_SHA384"),
            "{}",
            on.detail
        );
        assert!(on.detail.contains("256"), "{}", on.detail);
        assert!(on.detail.contains("sslmode require"), "{}", on.detail);

        // Plaintext under `disable` — the arm the fixture's own dsn lands on — is ok.
        let off = tls_check(SslMode::Disable, Ok(&tls(false)));
        assert_eq!(off.severity, Severity::Ok);
        assert!(off.detail.contains("plaintext"), "{}", off.detail);

        // An encrypted session under `disable` is still ok: the dsn gave TLS up, and the
        // server having accepted it anyway is not a finding.
        let encrypted_by_chance = tls_check(SslMode::Disable, Ok(&tls(true)));
        assert_eq!(encrypted_by_chance.severity, Severity::Ok);
        assert!(
            encrypted_by_chance.detail.contains("TLSv1.3"),
            "{}",
            encrypted_by_chance.detail
        );
    }

    #[test]
    fn a_require_policy_over_plaintext_is_an_error() {
        // The combination this row exists to catch: a promise the negotiation broke. A `require`
        // (or omitted) dsn never downgrades, so plaintext under it is a guarantee that did not
        // hold, not information.
        let row = tls_check(SslMode::Require, Ok(&tls(false)));
        assert_eq!(row.severity, Severity::Error);
        assert!(row.detail.contains("require"), "{}", row.detail);
        assert!(row.detail.contains("plaintext"), "{}", row.detail);
    }

    #[test]
    fn an_unread_negotiation_still_renders_the_policy() {
        // A server that never answered: the policy alone, at its own severity, with the reason.
        let down = tls_check(SslMode::Require, Err("connection refused".to_owned()));
        assert_eq!(down.severity, Severity::Ok);
        assert!(down.detail.contains("sslmode require"), "{}", down.detail);
        assert!(
            down.detail.contains("connection refused"),
            "{}",
            down.detail
        );
        let plaintext = tls_check(SslMode::Disable, Err("connection refused".to_owned()));
        assert_eq!(plaintext.severity, Severity::Ok);
        assert!(
            plaintext.detail.contains("sslmode disable"),
            "{}",
            plaintext.detail
        );
    }

    #[test]
    fn a_current_schema_is_ok_and_a_pending_one_names_the_versions() {
        let current = schema_check(Ok(SchemaState {
            applied: 2,
            supported: 2,
        }));
        assert_eq!(current.severity, Severity::Ok);
        assert!(current.detail.contains('2'), "{}", current.detail);

        let behind = schema_check(Ok(SchemaState {
            applied: 1,
            supported: 3,
        }));
        assert_eq!(behind.severity, Severity::Warn);
        assert!(behind.detail.contains('1') && behind.detail.contains('3'));

        let refused = schema_check(Err(StoreError::SchemaBehind {
            found: 1,
            supported: 3,
        }));
        assert_eq!(refused.severity, Severity::Warn);
        assert!(refused.detail.contains('3'), "{}", refused.detail);
    }

    /// An ahead-of-binary database a read-only inspection **reports** (the store's non-migrating
    /// path answers it rather than refusing it, so `doctor` can say what it found) takes the pg
    /// remedy: "migrate it forward with a writable start" is advice the writable start answers
    /// with this very refusal.
    #[test]
    fn a_reported_ahead_of_binary_schema_takes_the_pg_remedy() {
        let row = schema_check(Ok(SchemaState {
            applied: 4,
            supported: 2,
        }));
        assert_eq!(row.severity, Severity::Error, "{}", row.detail);
        assert!(
            row.detail.contains("newer than this binary's 2"),
            "{}",
            row.detail
        );
        assert!(
            !row.detail
                .contains("migrate it forward with a writable start"),
            "the pending-migration advice is not an ahead database's remedy: {}",
            row.detail
        );
        assert!(row.detail.contains("pg_dump"), "{}", row.detail);
        assert!(
            row.detail.contains("never drop and rebuild"),
            "{}",
            row.detail
        );
    }

    /// R5's refusal, rendered: the recorded checksum is not the binary's, which is the one state
    /// that says the database was built from a different schema.
    #[test]
    fn a_checksum_divergence_names_both_digests_and_the_remedy() {
        let row = schema_check(Err(StoreError::SchemaChecksum {
            version: 1,
            found: "aa".repeat(48),
            expected: "bb".repeat(48),
        }));
        assert_eq!(row.severity, Severity::Error);
        assert!(row.detail.contains(&"aa".repeat(48)), "{}", row.detail);
        assert!(row.detail.contains(&"bb".repeat(48)), "{}", row.detail);
        assert!(
            row.detail.contains("schema version 1"),
            "the row names the version: {}",
            row.detail
        );
        assert!(row.detail.contains("pg_dump"), "{}", row.detail);
        assert!(
            row.detail.contains("never drop and rebuild"),
            "{}",
            row.detail
        );
    }

    #[test]
    fn a_newer_schema_is_refused_with_the_dump_and_restore_remedy() {
        let row = schema_check(Err(StoreError::SchemaTooNew {
            found: 4,
            supported: 2,
        }));
        assert_eq!(row.severity, Severity::Error);
        // PG11: the required version, and a dump/restore path — never a rebuild.
        assert!(
            row.detail.contains('4') && row.detail.contains('2'),
            "{}",
            row.detail
        );
        assert!(row.detail.contains("pg_dump"), "{}", row.detail);
        // **This is the half of PG11 the remedy could actually violate**, so the negatives name
        // the destructive instructions a rebuild-shaped remedy would carry. A negative on the
        // words "drop and rebuild" cannot fail here — the remedy states the exclusion itself
        // ("never drop and rebuild, because these tables hold the only copy of the bytes"), which
        // is what the sibling encoding test's `delete and rebuild` negative checks too.
        for forbidden in [
            "delete the database",
            "recreate the database",
            "rebuild the database",
            "start over",
            "drop the tables",
        ] {
            assert!(
                !row.detail.contains(forbidden),
                "the remedy proposes `{forbidden}`: {}",
                row.detail
            );
        }
        assert!(
            row.detail.contains("never drop and rebuild"),
            "the remedy states the exclusion, and does not merely omit it: {}",
            row.detail
        );
    }

    #[test]
    fn a_read_only_session_over_a_writable_role_is_the_broken_guarantee() {
        let writer = RolePrivileges {
            role: "tinio".into(),
            schemas: "public".into(),
            can_write: true,
        };
        let read_only = access_check(Access::ReadOnly, Some(&writer));
        assert_eq!(read_only.severity, Severity::Error);
        assert!(read_only.detail.contains("REVOKE"), "{}", read_only.detail);
        assert!(read_only.detail.contains("public"), "{}", read_only.detail);

        // The same role on a writable inspection is exactly what is expected.
        assert_eq!(
            access_check(Access::ReadWrite, Some(&writer)).severity,
            Severity::Ok
        );

        let reader = RolePrivileges {
            can_write: false,
            ..writer
        };
        let row = access_check(Access::ReadOnly, Some(&reader));
        assert_eq!(row.severity, Severity::Ok);
        assert!(row.detail.contains("does not hold"), "{}", row.detail);
        assert!(
            row.detail.contains("in `public`"),
            "the row names the schema it checked: {}",
            row.detail
        );
    }

    /// **The mis-scoped probe.** A probe that checked no schema answers "no grants" the same way a
    /// role that holds none does, and the green row would then certify a role nothing was looked
    /// at — the 2026-10-13 review's finding, where a session whose `search_path` named no schema
    /// with the store's tables printed "no DML grants" beside that schema's name.
    #[test]
    fn a_probe_that_checked_no_schema_is_not_the_green_row() {
        let unchecked = RolePrivileges {
            role: "tinio".into(),
            schemas: String::new(),
            can_write: false,
        };
        let row = access_check(Access::ReadOnly, Some(&unchecked));
        assert_eq!(row.severity, Severity::Warn);
        assert!(row.detail.contains("not checked"), "{}", row.detail);
        assert!(row.detail.contains("schema_version"), "{}", row.detail);
        assert!(
            !row.detail.contains("does not hold"),
            "the row must not certify what it did not look at: {}",
            row.detail
        );
        // Every schema the probe did check is named, so a reader can see the scope.
        let multi = RolePrivileges {
            role: "tinio".into(),
            schemas: "audit, public".into(),
            can_write: false,
        };
        let row = access_check(Access::ReadOnly, Some(&multi));
        assert!(row.detail.contains("audit, public"), "{}", row.detail);
    }

    #[test]
    fn an_unread_role_still_reports_the_flag_as_not_inspected() {
        let row = access_check_without_role(Access::ReadOnly, "permission denied");
        assert_eq!(row.severity, Severity::Warn);
        assert!(row.detail.contains("read-only"), "{}", row.detail);
        assert!(row.detail.contains("not inspected"), "{}", row.detail);
    }

    #[test]
    fn the_ownership_row_names_the_holder_or_its_absence() {
        let held = ownership_check(Some("the session at 127.0.0.1 (application_name tinio)"));
        assert_eq!(held.severity, Severity::Ok);
        assert!(held.detail.contains("127.0.0.1"), "{}", held.detail);

        let unheld = ownership_check(None);
        assert_eq!(unheld.severity, Severity::Ok);
        assert!(unheld.detail.contains("read-only instances take none"));
    }

    /// The live leg (PG8): `TINIO_PG_DSN` unset skips it, so the default workspace run stays
    /// database-free. It exercises the real wiring — the pool's `SELECT 1`, the encoding scalar,
    /// the role probe, and `PgStore::inspect_with_pool` for the schema, TLS and ownership rows.
    #[tokio::test]
    async fn a_live_database_is_diagnosed_row_by_row() {
        use crate::_util::secret::{Secret, SecretKey};

        let Some(dsn) = std::env::var("TINIO_PG_DSN")
            .ok()
            .filter(|dsn| !dsn.trim().is_empty())
        else {
            return;
        };
        let config = Config::new(Secret::from(SecretKey::from(dsn.trim().to_owned())), 4);
        // **No setup, and no assertion about the schema's severity.** The shared fixture's version
        // is whatever moved last — the table's own migrations are another slice's file, and it may
        // legitimately be behind, current or ahead of this binary at any moment — so this leg asks
        // the question the row exists to answer ("is it reported, and does it name a version?")
        // and leaves which arm it lands on to `schema_check`'s pure legs. An earlier revision
        // migrated the fixture here; that made the ok arm self-fulfilling *and* became unrunnable
        // the moment the fixture turned out newer than the binary.
        // Read-write, because that is the mode a writable deployment diagnoses in.
        let rows = diagnose(&config, Access::ReadWrite).await;
        let row = |name: &str| rows.iter().find(|row| row.name == name);
        let severity = |name: &str| row(name).map(|row| row.severity);

        for name in [CONNECTION, POOL, ENCODING, TLS, SCHEMA, ACCESS, OWNERSHIP] {
            assert!(row(name).is_some(), "the {name} row is missing: {rows:?}");
        }
        // The fixture server is on, its database is UTF-8, and the test dsn is plaintext only
        // because it says `sslmode=disable` — so these three are the ok arms.
        assert_eq!(severity(CONNECTION), Some(Severity::Ok), "{rows:?}");
        assert_eq!(severity(POOL), Some(Severity::Ok), "{rows:?}");
        assert_eq!(severity(ENCODING), Some(Severity::Ok), "{rows:?}");
        assert_eq!(severity(TLS), Some(Severity::Ok), "{rows:?}");
        assert_eq!(severity(ACCESS), Some(Severity::Ok), "{rows:?}");
        // The ownership row is read through `inspect`, so a schema the store refuses degrades it
        // to "not inspected" — a finding about the schema, not about the lock.
        assert!(
            matches!(severity(OWNERSHIP), Some(Severity::Ok | Severity::Warn)),
            "{rows:?}"
        );
        // Reported, whichever arm the fixture's version lands on — and it names the version it
        // found, which is what the row is for.
        assert!(
            matches!(
                severity(SCHEMA),
                Some(Severity::Ok | Severity::Warn | Severity::Error)
            ),
            "{rows:?}"
        );
        assert!(
            row(SCHEMA).is_some_and(|row| row.detail.contains("schema version")),
            "{rows:?}"
        );
        assert!(
            row(ENCODING).is_some_and(|row| row.detail.contains("UTF-8")),
            "{rows:?}"
        );
    }

    /// The mode reaches the diagnosis's own pool (PG6). Constructing a pool opens nothing (it
    /// connects lazily), so the leg needs no server: it is exactly the choice `diagnose` makes.
    #[tokio::test]
    async fn the_diagnosis_pool_carries_the_runs_mode() {
        let dsn = "postgres://user:password@db.example:5432/tinio";
        assert!(
            !Pool::open_for(dsn, 2, Access::ReadWrite)
                .expect("the dsn parses")
                .is_read_only()
        );
        assert!(
            Pool::open_for(dsn, 2, Access::ReadOnly)
                .expect("the dsn parses")
                .is_read_only(),
            "a read-only diagnosis opens no writable session"
        );
    }

    /// The catch-all arm: a store error that is neither of the two version faults is "not
    /// inspected" with the store's own message, never a fault. The two version faults' own rows
    /// are pinned by the tests above, which drive [`schema_check`] with the store's real errors.
    #[test]
    fn an_unreadable_schema_is_not_inspected_with_the_stores_message() {
        let row = schema_check(Err(StoreError::Database("boom".into())));
        assert_eq!(row.severity, Severity::Warn);
        assert!(row.detail.contains("not inspected"), "{}", row.detail);
        assert!(row.detail.contains("boom"), "{}", row.detail);
    }
}
