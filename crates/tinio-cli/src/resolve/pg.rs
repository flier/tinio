//! The pg invocation path: the synthetic root, the effective dsn and the `DIR` refusal.
//!
//! A pg run has no served root — the database is the store — so the resolution synthesizes one
//! instead (PG5; ruling R8): `~/.local/state/tinio/pg/<dsn identity>/`, or `TINIO_STATE_DIR`
//! verbatim when the operator set it. Everything downstream of the root (config discovery, the
//! state directory, the control channel, the state file, the logs) is the ordinary machinery,
//! which is what makes a pg run invocable at all.
//!
//! **The location is deliberately outside `~/.tinio`** (ruling R8), and that is the whole reason
//! it is not the design's original `~/.tinio/pg/<hash>`: [`locate`](crate::resolve::locate)'s
//! walk-up treats *any* ancestor holding a `.tinio` directory as a storage root, so a pg run that
//! created `<home>/.tinio/pg/<…>/` would leave `$HOME` itself resolving as a root — the next
//! bare `tinio server` from anywhere under the home would silently serve it. The XDG state
//! family under the home (`~/.local/state/tinio/`) is no `.tinio` directory, so creating it turns
//! nobody's home into a root.
//!
//! **The leaf names the dsn's identity, not its text** (ruling R8): host, port and database,
//! with the credentials and every query parameter dropped, so rotating a password (or adding
//! `?sslmode=…`) does not relocate the state directory and strand a running instance.
//!
//! **`[storage] type` is the selector and it is read from the configuration**, so a pg run's
//! configuration is the anchor: `--config` names it without needing a root (the one command
//! shape whose config read does not depend on the root), while a positional `DIR` or a
//! discovered `.tinio/` root is refused with the remedy (PG13) rather than ignored.

use std::path::{Path, PathBuf};

use secrecy::ExposeSecret;

use crate::{
    _core::object::RESERVED_SEGMENT,
    _util::{
        secret::{Secret, SecretKey},
        state,
    },
    Error,
    cli::Discovery,
    config::{Error as ConfigError, storage::pg},
};

/// The state-directory family the synthetic root lives under, relative to `$HOME`
/// (`~/.local/state/tinio/pg/`). Chosen because creating it must not mint a `.tinio` directory
/// under the home — the walk-up's root predicate — which is what ruling R8 changes the design's
/// `~/.tinio/pg/` for ([`synthetic_root`]'s doc states the failure it avoids).
const PG_STATE_DIR: [&str; 4] = [".local", "state", "tinio", "pg"];

/// The PostgreSQL default port, applied to a dsn that omits one so two spellings of one server
/// derive one leaf ([`dsn_identity`]).
const DEFAULT_PG_PORT: &str = "5432";

/// The synthetic root a pg run resolves its configuration and tool state under (PG5).
///
/// A pg run serves no directory — the database is the store — so the root the rest of the chain
/// needs (config discovery, the state directory, the control channel, the state file, the logs)
/// is synthesized from the dsn: `~/.local/state/tinio/pg/<dsn identity>/` ([`synthetic_root`]),
/// or the operator's `TINIO_STATE_DIR` verbatim when it is set. Two databases therefore get two
/// roots, and the second is derived from the connection string the run actually opens — which is
/// why this takes the **effective** dsn, the `TINIO_PG_DSN` answer included (PG5).
///
/// **The root is computed, not canonicalized and not created**: it need not exist yet (the
/// serving half creates it with the state directory), and an override is the operator's
/// spelling rather than a path to resolve.
///
/// # Errors
///
/// A home directory that cannot be resolved, with the remedy named: the derivation is the only
/// thing that needs one, and `TINIO_STATE_DIR` is the escape hatch (the same pair
/// `~/.tinio/roots/<hash>/` documents for a service account with no home). An override answers
/// before the lookup, so a host without a home serves pg at the location the operator named.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::pg::resolve_root;
/// use tinio_server::identity::auth::{Secret, SecretKey};
///
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// // An explicit location answers verbatim: the operator chose it.
/// let root = resolve_root(&dsn, Some(Path::new("/srv/tinio-pg"))).expect("an override");
/// assert_eq!(root, Path::new("/srv/tinio-pg"));
/// ```
pub fn resolve_root(dsn: &Secret, override_dir: Option<&Path>) -> Result<PathBuf, Error> {
    if let Some(dir) = override_dir {
        return Ok(dir.to_path_buf());
    }
    let home = dirs::home_dir().ok_or_else(|| {
        Error::Config(ConfigError::Missing(
            "home directory (the pg run's synthetic root is derived under it; set TINIO_STATE_DIR \
             to name the location instead)"
                .into(),
        ))
    })?;
    Ok(synthetic_root(dsn, &home))
}

/// [`resolve_root`]'s derivation with the home as an **explicit parameter**, never
/// `dirs::home_dir()` inline: a test must resolve a scratch home instead of the developer's, and
/// the caller that *wants* the environment's answer is [`resolve_root`] alone (the shape
/// `tinio_util::state::home_state_dir` uses for the read-only leaf).
///
/// **The layout is `{PG_STATE_DIR}/<leaf>`, and the leaf is the dsn's identity's digest** — the
/// first sixteen hex characters of the SHA-256 over `dsn_identity` — so the root is stable
/// across runs and platforms, and a credential rotation cannot move it.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::resolve::pg::synthetic_root;
/// use tinio_server::identity::auth::{Secret, SecretKey};
///
/// let dsn = Secret::from(SecretKey::from(
///     "postgres://user:hunter2@db.example:5432/tinio".to_owned(),
/// ));
/// let root = synthetic_root(&dsn, Path::new("/home/svc"));
/// assert_eq!(
///     root,
///     Path::new("/home/svc/.local/state/tinio/pg/85b95498583db52c")
/// );
/// ```
pub fn synthetic_root(dsn: &Secret, home: &Path) -> PathBuf {
    PG_STATE_DIR
        .iter()
        .fold(home.to_path_buf(), |dir, name| dir.join(name))
        .join(dsn_leaf(dsn))
}

/// The leaf name: [`state::short_digest`] over the dsn's **credential-free identity**
/// ([`dsn_identity`]).
fn dsn_leaf(dsn: &Secret) -> String {
    state::short_digest(dsn_identity(dsn.expose_secret()).as_bytes())
}

/// The identity a dsn's leaf is derived from: `scheme://host:port/database`, with the userinfo
/// (the credentials) and every query parameter dropped (ruling R8).
///
/// **Nothing that names a credential may reach this string.** The whole of the finding is that
/// the design hashed the dsn's *text*, so rotating the password — or adding `?sslmode=disable`,
/// or respelling the same connection as key/value pairs — relocated the state directory of a
/// running instance, which `status`, `stop` and `doctor` then could not find. The pair
/// `(host, port, database)` is what identifies a database, and an omitted port is the protocol
/// default, so `db.example/tinio` and `db.example:5432/tinio` are one instance.
///
/// The URL form (`postgres://user:password@host:5432/db?sslmode=disable`) is parsed here by hand
/// because this crate carries no URL parser, and the key/value form (`host=… port=… dbname=…`)
/// keeps its addressing keys alone. Both lowercase the host, which is not case-sensitive.
fn dsn_identity(dsn: &str) -> String {
    let dsn = dsn.trim();
    match dsn.split_once("://") {
        Some((scheme, rest)) => url_identity(scheme, rest),
        None => key_value_identity(dsn),
    }
}

/// [`dsn_identity`]'s URL arm: the authority's host and port, then the path's database.
fn url_identity(scheme: &str, rest: &str) -> String {
    // The query — where `sslmode`, `application_name` and the like live — and any fragment are
    // not part of the identity.
    let rest = rest.split(['?', '#']).next().unwrap_or(rest);
    // The authority ends at the first `/`; the userinfo, when there is one, ends at the **last**
    // `@` before that boundary, so a password carrying an unencoded `@` still cannot leak a byte
    // of itself into the identity.
    let (authority, database) = rest.split_once('/').unwrap_or((rest, ""));
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_userinfo, host_port)| host_port);
    format!(
        "{}://{}/{}",
        scheme.to_ascii_lowercase(),
        host_port_of(host_port),
        database
    )
}

/// `host:port` with the default port applied when the dsn omitted one, and the host lowercased
/// (a host name is not case-sensitive). An IPv6 literal keeps its brackets.
fn host_port_of(authority: &str) -> String {
    if let Some(rest) = authority.strip_prefix('[')
        && let Some((host, tail)) = rest.split_once(']')
    {
        let port = tail
            .strip_prefix(':')
            .filter(|port| !port.is_empty())
            .unwrap_or(DEFAULT_PG_PORT);
        return format!("[{}]:{port}", host.to_ascii_lowercase());
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => {
            format!("{}:{port}", host.to_ascii_lowercase())
        }
        _ => format!("{}:{DEFAULT_PG_PORT}", authority.to_ascii_lowercase()),
    }
}

/// [`dsn_identity`]'s key/value arm (`host=… port=… dbname=… user=… password=…`): the
/// addressing keys, rendered back into the **same canonical form the URL arm produces**, so one
/// server spelled either way derives one leaf.
///
/// A dsn naming none of the addressing keys answers with the empty rendition, and its leaf is
/// then the empty identity's: such a string is refused by the driver's own parse before the root
/// is ever used, so two of them sharing a leaf costs nothing.
fn key_value_identity(dsn: &str) -> String {
    let mut host = String::new();
    let mut port = String::new();
    let mut database = String::new();
    for token in dsn.split_ascii_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            continue;
        };
        match key.to_ascii_lowercase().as_str() {
            // `hostaddr` is the numeric spelling of the same field, so the first one written wins.
            "host" | "hostaddr" if host.is_empty() => host = value.to_ascii_lowercase(),
            "port" if port.is_empty() => port = value.to_owned(),
            "dbname" if database.is_empty() => database = value.to_owned(),
            _ => {}
        }
    }
    let port = if port.is_empty() {
        DEFAULT_PG_PORT
    } else {
        port.as_str()
    };
    format!("postgres://{host}:{port}/{database}")
}

/// The dsn a pg run opens: the parsed `TINIO_PG_DSN` answer when there is one, else
/// `[storage.pg] dsn` (PG5).
///
/// **The environment variable is an override and never a selector**: the pg backend is chosen by
/// `[storage] type` alone, so a host that merely has the variable exported keeps serving a
/// filesystem root until the key says `pg`. The value arrives here already parsed (the CLI's
/// hidden `--pg-dsn`, whose parser is the variable's channel), never read from the environment
/// beside it — the contract's one-owner rule for `TINIO_*`.
///
/// **Both sources are read through `named`, which is also where the value is normalized**, so
/// the same dsn spelled with surrounding whitespace is one dsn — and therefore one synthetic
/// root — whichever channel carried it.
///
/// # Errors
///
/// Neither source answering: a startup error naming **every** channel the operator has to go and
/// set — the key, the exported variable, and the `<state dir>/.env` file the run reads (a dsn
/// that lives only in that file is reachable when `TINIO_STATE_DIR` anchors the run, which is
/// what [`env_file_state_dir`] is for).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::resolve::pg::resolve_dsn;
/// use tinio_server::identity::auth::{Secret, SecretKey};
///
/// let from_env = Secret::from(SecretKey::from(
///     "postgres://user:password@db.example:5432/tinio".to_owned(),
/// ));
/// // The variable's answer beats the section's, and an empty section is no answer.
/// let dsn = resolve_dsn(None, Some(&from_env)).expect("the variable answered");
/// assert_eq!(dsn, from_env);
/// assert!(resolve_dsn(None, None).is_err());
/// ```
pub fn resolve_dsn(
    section: Option<&pg::Config>,
    from_env: Option<&Secret>,
) -> Result<Secret, Error> {
    let answer = named(from_env.cloned())
        .or_else(|| named(section.and_then(|pg| pg.dsn.clone())))
        .ok_or_else(|| {
            Error::Config(ConfigError::invalid_value(
                "storage.pg.dsn",
                "a pg run needs a dsn: set `[storage.pg] dsn` in the configuration file, export \
                 TINIO_PG_DSN, or put TINIO_PG_DSN in the state directory's `.env` file",
            ))
        })?;
    Ok(answer)
}

/// The dsn a source names: `None` when the source is absent **or empty**, because an empty dsn
/// names nothing. **This is the one reading of that rule**, shared by both of [`resolve_dsn`]'s
/// sources and by the readers that ask a `TINIO_PG_DSN` answer whether it named anything at all —
/// the [`Discovery`](crate::cli::Discovery) the resolution threads and the doctor's pg leg — so
/// `TINIO_PG_DSN=` and an unset variable are one answer everywhere. An exported-but-empty
/// variable therefore neither selects a pg run, nor moves a run's root to the synthetic one, nor
/// refuses a run that named no `DIR`.
///
/// **The value is normalized here too, and this is the one place that happens**: the surrounding
/// whitespace is dropped before the emptiness test, so a dsn an environment file's CRLF line
/// ending (or a quoted TOML value) left trailing is one dsn — one synthetic-root leaf — whether
/// it arrived through `TINIO_PG_DSN`/`--pg-dsn` or through `[storage.pg] dsn`, and a
/// whitespace-only value names nothing like an empty one does.
pub(crate) fn named(dsn: Option<Secret>) -> Option<Secret> {
    dsn.map(normalized)
        .filter(|dsn| !dsn.expose_secret().is_empty())
}

/// A dsn as the one spelling this resolution reads: the surrounding whitespace dropped, with the
/// material still inside a [`Secret`] (the credential never travels as a plain `String`).
fn normalized(dsn: Secret) -> Secret {
    Secret::from(SecretKey::from(dsn.expose_secret().trim().to_owned()))
}

/// The state directory whose `.env` a pg run's **root derivation** must see first, when the
/// discovery answers alone can name it.
///
/// A pg run's state directory is `<root>/.tinio`, and its root is `TINIO_STATE_DIR` verbatim
/// ([`resolve_root`]'s override arm) — the one shape in which that directory does not depend on
/// the dsn. That is what makes an environment file reachable *before* the root is derived: a
/// `TINIO_PG_DSN` the file supplies then reaches [`resolve_dsn`], so the root the resolution
/// derives and the dsn the store opens are one value. Without an override the file sits under
/// the very root the dsn derives, so a dsn only in that file cannot be read at all — and the
/// refusal names the override for exactly that reason.
///
/// **The run has to be the pg shape**: an addressed dsn (`TINIO_PG_DSN`/`--pg-dsn`, the R8
/// fallback a bare pg server reaches), or a named `--config` — the one document a pg run reads
/// without a root (PG13), and a flag only the server arm carries. A positional `DIR` rules the
/// shape out as well, since a pg run refuses one. The residual is a filesystem run that names
/// `--config`, sets `TINIO_STATE_DIR`, and has a `.env` at `<that dir>/.tinio/.env`: no
/// production path writes that file, and the run's own located `.env` is still the one the
/// authoritative parse reads last.
pub fn env_file_state_dir(discovery: &Discovery) -> Option<PathBuf> {
    let pg_shaped =
        discovery.dir.is_none() && (discovery.pg_dsn.is_some() || discovery.config.is_some());
    let override_dir = discovery.state_dir.as_deref()?;
    pg_shaped.then(|| override_dir.join(RESERVED_SEGMENT))
}

/// PG13's refusal for a **positional `DIR`** under `[storage] type = "pg"`.
///
/// `names_config` is the run's shape — whether the command that resolved this carries
/// `--config` — so the remedy can name a channel the command really has (`refuse`).
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::{Error, resolve::pg::refuse_positional_dir};
///
/// let refusal = refuse_positional_dir(Path::new("/srv/data"), true).to_string();
/// assert!(refusal.contains("name the configuration with"), "{refusal}");
/// assert!(matches!(
///     refuse_positional_dir(Path::new("/srv/data"), true),
///     Error::Usage(_)
/// ));
/// // The same refusal for a command without the flag names the channels it does have.
/// let refusal = refuse_positional_dir(Path::new("/srv/data"), false).to_string();
/// assert!(
///     !refusal.contains("name the configuration with"),
///     "{refusal}"
/// );
/// assert!(refusal.contains("TINIO_PG_DSN"), "{refusal}");
/// ```
pub fn refuse_positional_dir(dir: &Path, names_config: bool) -> Error {
    refuse(
        &format!("the positional DIR `{}`", dir.display()),
        names_config,
    )
}

/// PG13's refusal for the **discovered root** (the walk-up's `.tinio/` answer) under
/// `[storage] type = "pg"`.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use tinio_cli::{Error, resolve::pg::refuse_discovered_root};
///
/// let refusal = refuse_discovered_root(Path::new("/srv/data"), true).to_string();
/// assert!(refusal.contains("--config"), "{refusal}");
/// assert!(matches!(
///     refuse_discovered_root(Path::new("/srv/data"), true),
///     Error::Usage(_)
/// ));
/// ```
pub fn refuse_discovered_root(root: &Path, names_config: bool) -> Error {
    refuse(
        &format!("the discovered root `{}`", root.display()),
        names_config,
    )
}

/// The one sentence both PG13 refusals render: the subject, the rule and **a remedy the run's
/// own channels make actionable**.
///
/// **`--config PATH` is the server's flag alone.** `doctor`, `status`, `stop` and the `service`
/// verbs build a discovery with no `--config` (design: the client commands never name a
/// configuration), so telling their operator to use one names a channel the command cannot
/// carry — and the pg instance a document on disk configures then cannot be diagnosed or
/// stopped at all. The channels such a command does have are the environment's:
/// `TINIO_PG_DSN` for the database and `TINIO_STATE_DIR` for the root the instance was anchored
/// with, which is how a client reaches the same synthetic leaf the server resolved.
fn refuse(subject: &str, names_config: bool) -> Error {
    let remedy = if names_config {
        "name the configuration with `--config PATH` — the one document a pg run reads without a \
         root — and drop any positional DIR"
    } else {
        "this command names no `--config` (that flag is the server's alone), so name the database \
         with TINIO_PG_DSN — and the root with TINIO_STATE_DIR when the instance was anchored \
         with one — and drop any positional DIR"
    };
    Error::Usage(format!(
        "`[storage] type = \"pg\"` has no served root, so {subject} would be ignored and is \
         refused: {remedy}; a pg run's root is synthesized from its dsn \
         (`~/.local/state/tinio/pg/<dsn identity>/`, or TINIO_STATE_DIR when set)"
    ))
}

#[cfg(test)]
mod tests {
    use std::{ffi::OsStr, fs};

    use clap::parser::ValueSource;
    use secrecy::ExposeSecret;

    use super::*;
    use crate::{
        _util::{secret::SecretKey, state},
        cli::{Discovery, PG_DSN_VAR, Provenance},
        config::{Config, storage::Backend},
        resolve::{locate, locate_from},
    };

    /// A `Secret` from a fixture string (`Secret` itself takes `SecretKey`, not `String`).
    fn secret(raw: &str) -> Secret {
        Secret::from(SecretKey::from(raw.to_owned()))
    }

    /// A pg configuration file on disk: the selector, and the dsn the section carries.
    fn pg_config_file(dir: &Path, dsn: Option<&str>) -> PathBuf {
        let dsn = dsn.map_or(String::new(), |dsn| format!("dsn = \"{dsn}\"\n"));
        let path = dir.join("pg.toml");
        fs::write(
            &path,
            format!("version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\n{dsn}"),
        )
        .expect("write");
        path
    }

    /// The discovery answers a pg run's leg uses: the configuration, no `DIR`, nothing else.
    fn discovery_for(config: PathBuf) -> Discovery {
        Discovery {
            dir: None,
            config: Some(config),
            read_only: false,
            state_dir: None,
            pg_dsn: None,
        }
    }

    /// The discovery answers the walk-up's leg uses: no `DIR` and no `--config`, so the root can
    /// only be the ancestor walk's answer.
    fn walk_up_discovery() -> Discovery {
        Discovery {
            dir: None,
            config: None,
            read_only: false,
            state_dir: None,
            pg_dsn: None,
        }
    }

    const DSN: &str = "postgres://user:hunter2@db.example:5432/tinio";

    /// The leaf `DSN`'s **identity** derives: the first 16 hex characters of
    /// `sha256("postgres://db.example:5432/tinio")` — `credentials dropped`, the ruling R8
    /// input. Pinned as a literal so a change of hash input (the dsn's text back, or a length, or
    /// a different field) cannot pass while the shape assertions below still hold.
    const DSN_LEAF: &str = "85b95498583db52c";

    #[test]
    fn the_synthetic_root_is_the_documented_hash_of_the_dsns_identity() {
        // The derivation against a **scratch home**, so the assertion is about the hash and the
        // layout rather than about the developer's home (`synthetic_root`'s own reason).
        let home = tempfile::TempDir::new().expect("home");
        let root = synthetic_root(&secret(DSN), home.path());
        assert_eq!(
            root,
            home.path()
                .join(".local")
                .join("state")
                .join("tinio")
                .join("pg")
                .join(DSN_LEAF)
        );
        let leaf = root
            .file_name()
            .expect("a leaf")
            .to_string_lossy()
            .into_owned();
        assert_eq!(leaf.len(), 16, "{root:?}");
        assert!(leaf.chars().all(|c| c.is_ascii_hexdigit()), "{root:?}");
        // Deterministic: the same dsn derives the same root, a different one another.
        assert_eq!(root, synthetic_root(&secret(DSN), home.path()));
        let other = synthetic_root(&secret(&DSN.replace("tinio", "other")), home.path());
        assert_ne!(root, other);
    }

    /// **The ruling's own leg: `~/.tinio` is not created, so `$HOME` never becomes a root.**
    ///
    /// The layout is asserted by path, and then by filesystem: the serving half's `create_dir_all`
    /// over the derived root must leave the home without a `<home>/.tinio` — the directory
    /// `resolve::locate`'s walk-up reads as "this ancestor is a storage root". Before ruling R8
    /// the leaf was `<home>/.tinio/pg/<sha256(dsn)16>/`, so a pg run minted exactly that
    /// directory and every later bare `tinio server` from under the home served the home.
    #[test]
    fn creating_the_synthetic_root_does_not_mint_a_reserved_directory_under_the_home() {
        let home = tempfile::TempDir::new().expect("home");
        let root = synthetic_root(&secret(DSN), home.path());
        fs::create_dir_all(&root).expect("the serving half's create");
        assert!(
            !home.path().join(RESERVED_SEGMENT).exists(),
            "a pg run must not create `$HOME/.tinio`: {}",
            root.display()
        );
    }

    /// The walk-up's other half of the same ruling: a run started from a **subdirectory of the
    /// home**, after a pg run created its state there, still answers "no storage root" rather than
    /// resolving the home as one — which is what the old `~/.tinio/pg/...` layout turned the home
    /// into.
    #[test]
    fn a_run_under_the_home_after_a_pg_run_does_not_serve_the_home() {
        let home = tempfile::TempDir::new().expect("home");
        let nested = home.path().join("work/deep");
        fs::create_dir_all(&nested).expect("a working directory under the home");
        fs::create_dir_all(synthetic_root(&secret(DSN), home.path())).expect("the pg run's root");

        let err = locate_from(walk_up_discovery(), &Provenance::default(), &nested)
            .expect_err("no storage root above this working directory");
        assert!(matches!(err, Error::Usage(_)), "exit 2: {err}");
        assert!(err.to_string().contains("no storage root"), "{err}");
    }

    /// **Two dsns that differ only in the credential hash to one leaf** (ruling R8): the leaf is
    /// over the dsn's identity, so rotating a password — or respelling the same connection with a
    /// query, a default port, or the key/value form — leaves the state directory where it was and
    /// does not strand the instance `status`/`stop`/`doctor` are looking for.
    #[test]
    fn two_dsns_that_differ_only_in_the_credential_share_a_leaf() {
        let home = tempfile::TempDir::new().expect("home");
        let rotated = synthetic_root(
            &secret("postgres://user:rotated-password@db.example:5432/tinio"),
            home.path(),
        );
        let base = synthetic_root(&secret(DSN), home.path());
        assert_eq!(base, rotated, "a password rotation must not move the root");
        assert_eq!(
            base,
            synthetic_root(
                &secret("postgres://user:hunter2@db.example/tinio?sslmode=disable"),
                home.path()
            ),
            "an omitted port is the default, and a query is not part of the identity"
        );
        assert_eq!(
            base,
            synthetic_root(
                &secret("host=db.example port=5432 dbname=tinio user=u password=p"),
                home.path()
            ),
            "the key/value form of one connection is the same instance"
        );
        // …and the identity really is a function of host/port/database: each of the three moves
        // the leaf, whatever the credentials say.
        for other in [
            "postgres://user:hunter2@other.example:5432/tinio",
            "postgres://user:hunter2@db.example:5433/tinio",
            "postgres://user:hunter2@db.example:5432/other",
        ] {
            assert_ne!(base, synthetic_root(&secret(other), home.path()), "{other}");
        }
    }

    /// The identity itself, so a field the ruling names can be shown to be dropped or kept
    /// rather than inferred from a digest.
    #[test]
    fn the_identity_drops_the_credentials_and_keeps_the_addressing() {
        assert_eq!(
            dsn_identity("postgres://user:hunter2@db.example:5432/tinio?sslmode=require"),
            "postgres://db.example:5432/tinio"
        );
        // A password carrying an unencoded `@` still cannot leak into the identity: the userinfo
        // ends at the last boundary before the authority's path.
        assert_eq!(
            dsn_identity("postgres://user:pa@ss@db.example:5432/tinio"),
            "postgres://db.example:5432/tinio"
        );
        // The host lowercases (it is not case-sensitive) and the default port fills in.
        assert_eq!(
            dsn_identity("postgres://u:p@DB.Example/tinio"),
            "postgres://db.example:5432/tinio"
        );
        assert_eq!(
            dsn_identity("postgres://u:p@[::1]:5433/tinio"),
            "postgres://[::1]:5433/tinio"
        );
        assert_eq!(
            dsn_identity("postgresql://u:p@db.example:5432/tinio"),
            "postgresql://db.example:5432/tinio"
        );
    }

    #[test]
    fn the_derived_root_keeps_the_layout_under_the_real_home() {
        // `resolve_root`'s home branch (the environment's answer, not a scratch one): the shape
        // above, reached through the wrapper the resolution actually calls.
        let root = resolve_root(&secret(DSN), None).expect("the home resolves");
        // **`pg` under `tinio`, not under `.tinio`** — the whole of ruling R8's layout change,
        // and it is a parent's name that carries it.
        let names: Vec<&OsStr> = root
            .ancestors()
            .skip(1)
            .take(4)
            .filter_map(Path::file_name)
            .collect();
        assert_eq!(
            names,
            ["pg", "tinio", "state", ".local"].map(OsStr::new).to_vec(),
            "{root:?}"
        );
        assert_eq!(root.file_name(), Some(OsStr::new(DSN_LEAF)));
    }

    #[test]
    fn an_explicit_state_dir_is_the_root_verbatim() {
        // PG5: `TINIO_STATE_DIR` overrides the derivation. Taken verbatim: the operator chose
        // the location, and the directory need not exist for the resolution to name it.
        let dsn = secret(DSN);
        let chosen = PathBuf::from("/srv/tinio-pg-state");
        assert_eq!(
            resolve_root(&dsn, Some(&chosen)).expect("the override answers"),
            chosen
        );
    }

    #[test]
    fn the_environment_overrides_the_sections_dsn() {
        let section: pg::Config = {
            let config = Config::parse(&format!(
                "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"{DSN}\""
            ))
            .expect("config");
            config.storage.expect("section").pg.expect("pg")
        };
        // The env answer wins, verbatim.
        let over = secret("postgres://user:envpw@db.example:5432/other");
        let resolved = resolve_dsn(Some(&section), Some(&over)).expect("the override answers");
        assert_eq!(
            resolved.expose_secret().as_ref(),
            over.expose_secret().as_ref()
        );
        // Without it, the section's own dsn is the answer.
        let resolved = resolve_dsn(Some(&section), None).expect("the section answers");
        assert_eq!(resolved.expose_secret().as_ref(), DSN);
    }

    #[test]
    fn a_missing_dsn_is_refused_with_both_remedies() {
        // Neither source: the refusal names the key and the variable, because either one is
        // what the operator has to go and set.
        for section in [None, Some(pg::Config::default())] {
            let err = resolve_dsn(section.as_ref(), None).expect_err("no dsn");
            let rendered = err.to_string();
            assert!(rendered.contains("TINIO_PG_DSN"), "{rendered}");
            assert!(rendered.contains("dsn"), "{rendered}");
            assert!(
                matches!(err, Error::Config(ConfigError::InvalidValue { .. })),
                "{err}"
            );
        }
        // And an empty section cannot answer either.
        let empty = pg::Config {
            dsn: Some(secret("")),
            ..pg::Config::default()
        };
        assert!(resolve_dsn(Some(&empty), None).is_err());
    }

    #[test]
    fn the_pg_keys_parse_in_a_build_without_the_pg_backend() {
        // §6: the keys are not feature-gated. This crate's default feature set carries no pg
        // backend, so this leg is the feature-off leg, and the configuration parses anyway --
        // including the selector.
        let config = Config::parse(&format!(
            "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"{DSN}\"\npool = 3"
        ))
        .expect("the keys parse in every build");
        let storage = config.storage.expect("section");
        assert_eq!(storage.backend, Backend::Pg);
        assert_eq!(storage.pg.expect("pg").pool, 3);
    }

    #[test]
    fn a_positional_dir_is_refused_under_pg() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let mut discovery = discovery_for(config);
        discovery.dir = Some(dir.path().to_path_buf());
        let err = locate(discovery, &Provenance::default()).expect_err("a DIR is refused");
        assert!(matches!(err, Error::Usage(_)), "exit 2: {err}");
        let rendered = err.to_string();
        assert!(rendered.contains("--config"), "the remedy: {rendered}");
        assert!(rendered.contains("pg"), "{rendered}");
    }

    #[test]
    fn a_discovered_root_is_refused_under_pg() {
        // The walk-up's arm of PG13: a root is *found* (here by naming the starting directory —
        // no test may mutate the process's working directory, which would race every other test
        // in this binary) and the document under it selects pg, so `locate` refuses with the
        // remedy. The discovery carries no positional `DIR` and no `--config`: the root is the
        // walk-up's answer, which is the shape this arm exists for.
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("data");
        fs::create_dir_all(root.join(".tinio")).expect("root");
        fs::write(
            root.join(".tinio/.tinio.toml"),
            "version = 1\n[storage]\ntype = \"pg\"\n",
        )
        .expect("config");
        let err = locate_from(walk_up_discovery(), &Provenance::default(), &root)
            .expect_err("a discovered root is refused");
        assert!(matches!(err, Error::Usage(_)), "exit 2: {err}");
        let rendered = err.to_string();
        // **The remedy names a channel this command has.** No `--config` reached this run (the
        // discovery carries none), which is every client command's shape — `doctor`, `status`,
        // `stop` and `service` build exactly this discovery — so the sentence names the
        // environment's channels rather than telling the operator to use a flag this command
        // cannot type.
        assert!(
            !rendered.contains("name the configuration with"),
            "a `--config`-less command must not be told to use one: {rendered}"
        );
        assert!(rendered.contains("TINIO_PG_DSN"), "the remedy: {rendered}");
        assert!(
            rendered.contains("TINIO_STATE_DIR"),
            "the remedy: {rendered}"
        );
        assert!(rendered.contains("data"), "the subject: {rendered}");
    }

    /// **A read-only pg run's first start must not fail on the leaf it is about to create**
    /// (ruling R8's read-only half). With no `TINIO_STATE_DIR` the root is the dsn's synthetic
    /// leaf, which nothing has made yet — and `prepare_home_derived`, whose derivation
    /// canonicalizes the root, used to refuse the run before it created anything. The leg drives
    /// the call the serving half makes over the resolution such a run produces; the creation half
    /// (`prepare_synthetic_root`) is covered with a scratch root in `tinio_util::state`'s own
    /// tests, because this one resolves the developer's home on purpose (the environment branch a
    /// read-only run with no override takes), and a leg must not mint a state tree there.
    #[test]
    fn a_read_only_pg_run_starts_on_a_root_that_does_not_exist_yet() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let mut discovery = discovery_for(config);
        // Read-only, and no override: the synthetic root (under the real home) is derived and
        // nothing has created it.
        discovery.read_only = true;
        let located = locate(
            discovery,
            &Provenance {
                read_only: ValueSource::CommandLine,
                ..Provenance::default()
            },
        )
        .expect("a read-only pg run resolves");
        assert!(located.read_only);
        assert_eq!(
            located.root,
            resolve_root(&secret(DSN), None).expect("the derived root")
        );
        assert_eq!(located.state_dir, located.root.join(".tinio"));
        state::prepare_home_derived(&located.root, &located.state_dir)
            .expect("a root the first start has not created is no home-derived leaf");
    }

    /// **The same dsn spelled with surrounding whitespace is one dsn** — one synthetic-root leaf —
    /// whichever channel carried it. The env channel trimmed and the config-file channel did not,
    /// so `[storage.pg] dsn = "  postgres://…  "` derived a second leaf for the same database and
    /// handed the driver the file's whitespace.
    #[test]
    fn the_whitespace_a_channel_leaves_is_normalized_once() {
        let home = tempfile::TempDir::new().expect("home");
        let section: pg::Config = {
            let config = Config::parse(&format!(
                "version = 1\n[storage]\ntype = \"pg\"\n[storage.pg]\ndsn = \"  {DSN}  \"\n"
            ))
            .expect("config");
            config.storage.expect("section").pg.expect("pg")
        };
        let resolved = resolve_dsn(Some(&section), None).expect("the section answers");
        assert_eq!(
            resolved.expose_secret().as_ref(),
            DSN,
            "the file's whitespace must not reach the driver"
        );
        assert_eq!(
            synthetic_root(&resolved, home.path()),
            synthetic_root(&secret(DSN), home.path()),
            "one connection string, one leaf"
        );
        // The variable's channel answers through the same reading, and a whitespace-only value
        // names nothing.
        let from_env = secret(&format!(" {DSN}\n"));
        assert_eq!(
            resolve_dsn(None, Some(&from_env))
                .expect("the variable answers")
                .expose_secret()
                .as_ref(),
            DSN
        );
        assert!(named(Some(secret("   "))).is_none());
        assert!(resolve_dsn(None, Some(&secret("   "))).is_err());
    }

    /// [`env_file_state_dir`]'s two arms: the pg shape with an override names the directory an
    /// early `.env` load would read, and every other shape names none — so a filesystem run's own
    /// `<root>/.tinio/.env` stays the file the resolution reads after it named that directory.
    #[test]
    fn only_the_override_anchored_pg_shape_has_an_early_env_file() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let state = dir.path().join("state");

        // The pg shape: a named configuration (or an addressed dsn) and a state-dir override —
        // the directory whose `.env` the early load then reads.
        let mut discovery = discovery_for(config.clone());
        discovery.state_dir = Some(state.clone());
        assert_eq!(
            env_file_state_dir(&discovery),
            Some(state.join(".tinio")),
            "a pg run's state directory is `<root>/.tinio`"
        );

        // No override: the file sits under the very root the dsn derives, so nothing is early.
        assert_eq!(env_file_state_dir(&discovery_for(config.clone())), None);

        // A positional `DIR` rules the shape out: a pg run refuses one, and a filesystem run's
        // `.env` is the located directory's.
        let mut rooted = discovery_for(config.clone());
        rooted.dir = Some(dir.path().to_path_buf());
        rooted.state_dir = Some(state.clone());
        assert_eq!(env_file_state_dir(&rooted), None);

        // The walk-up shape with no dsn and no document is not pg at all.
        let mut bare = walk_up_discovery();
        bare.state_dir = Some(state.clone());
        assert_eq!(env_file_state_dir(&bare), None);
    }

    /// **The env-only pg shape resolves, and one dsn feeds both** (ruling R8's sibling finding).
    ///
    /// A `[storage] type = "pg"` document with **no** dsn plus a dsn the run supplies elsewhere
    /// (the environment, or the `<state dir>/.env` [`env_file_state_dir`] names) is the shape
    /// that used to die with "set `[storage.pg] dsn` … or export TINIO_PG_DSN" — naming a
    /// variable the operator *had* set — while a second pass folded the file's dsn in and let the
    /// store open a different database than the root was derived from. The leg drives the two
    /// halves that meet: the pg arm deriving the root from `Discovery::pg_dsn` (the value
    /// `command_line` refreshes from the authoritative parse), and `resolve_dsn` answering that
    /// same value for the store.
    #[test]
    fn the_env_only_shape_derives_the_root_from_the_dsn_the_store_opens() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), None);

        // **The discovery parse alone cannot resolve it**, and that is what makes the ordering
        // load-bearing: the file supplies the dsn, so the answers `command_line` derives *before*
        // the load (no dsn anywhere) are exactly the refusal the finding recorded — a run that
        // names a variable the operator did set.
        let refusal = locate(discovery_for(config.clone()), &Provenance::default())
            .expect_err("the discovery answers name no dsn");
        assert!(refusal.to_string().contains("TINIO_PG_DSN"), "{refusal}");
        assert!(refusal.to_string().contains(".env"), "{refusal}");

        // With the dsn the early `.env` load supplies — the value `command_line` folds onto the
        // discovery from the authoritative parse — the same document resolves.
        let supplied = secret(DSN);
        let mut discovery = discovery_for(config);
        discovery.pg_dsn = Some(supplied.clone());
        let located = locate(discovery, &Provenance::default()).expect("located");

        let section = located
            .config_doc
            .as_ref()
            .and_then(|(config, _)| config.pg_section());
        assert!(
            section.is_some_and(|pg| pg.dsn.is_none()),
            "the document the run reads names no dsn"
        );
        // The store's own resolution (`start::pg_backend_config`'s), over the section and the
        // parse that answered.
        let store = resolve_dsn(section, Some(&supplied)).expect("the supplied dsn");
        assert_eq!(store, supplied, "the file answered and nothing overrode it");
        assert_eq!(
            located.root,
            resolve_root(&store, None).expect("the derived root"),
            "the root came from the dsn the store opens"
        );
    }

    #[test]
    fn a_pg_run_resolves_the_synthetic_root_and_keeps_the_machinery() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let located = locate(discovery_for(config), &Provenance::default()).expect("located");
        // The root is synthesized from the dsn (no DIR, no walk-up: the current directory is
        // irrelevant to this leg).
        let expected = resolve_root(&secret(DSN), None).expect("root");
        assert_eq!(located.root, expected);
        // Everything downstream is the ordinary machinery: the state directory is the root's
        // own `.tinio` (PG's read-only mode has no served root to relocate away from).
        assert_eq!(located.state_dir, expected.join(".tinio"));
    }

    #[test]
    fn an_explicit_state_dir_moves_the_synthetic_root() {
        let dir = tempfile::TempDir::new().expect("temp");
        let config = pg_config_file(dir.path(), Some(DSN));
        let state = dir.path().join("state");
        let mut discovery = discovery_for(config);
        discovery.state_dir = Some(state.clone());
        let located = locate(discovery, &Provenance::default()).expect("located");
        assert_eq!(located.root, state);
        assert_eq!(located.state_dir, state.join(".tinio"));
    }

    #[test]
    fn an_fs_config_still_resolves_its_root_from_the_directory() {
        // The selector's default arm, through the same `locate`: a configuration that never
        // mentions pg keeps the positional root and the reserved state directory.
        let dir = tempfile::TempDir::new().expect("temp");
        let root = dir.path().join("data");
        fs::create_dir_all(root.join(".tinio")).expect("root");
        fs::write(root.join(".tinio/.tinio.toml"), "version = 1").expect("config");
        let mut discovery = discovery_for(root.join(".tinio/.tinio.toml"));
        discovery.dir = Some(root.clone());
        let located = locate(discovery, &Provenance::default()).expect("located");
        assert_eq!(located.root, root.canonicalize().expect("canonical"));
        assert_eq!(
            located.state_dir,
            root.canonicalize().expect("canonical").join(".tinio")
        );
    }

    #[test]
    fn the_dsn_variable_name_is_the_contracts() {
        assert_eq!(PG_DSN_VAR, "TINIO_PG_DSN");
    }
}
