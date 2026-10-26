//! The command line, as `contracts/cli.md` specifies it.
//!
//! **Every environment variable the tool reads is an argument declared here**, read through
//! clap's `env` feature, so a flag, its variable and its help text cannot drift apart. The two
//! **boolean switches** carry theirs with a custom `value_parser` rather than clap's `SetTrue`
//! action: `SetTrue` parses its values with `ValueParser::bool()`, whose only spellings are the
//! literals `true` and `false`, while `TINIO_READ_ONLY=1` and `TINIO_SANDBOX=1` are documented
//! (`contracts/config.md:110`) and are what the harnesses set — so the switches take
//! `ArgAction::Set` with `num_args(0..=1)` and a `default_missing_value`, which is what keeps the
//! bare `--read-only` working beside `TINIO_READ_ONLY=1`. `TINIO_STATE_DIR` and the credential
//! pair are arguments too, **`hide = true`** because the environment is their documented channel
//! and a secret typed on a command line is visible in the process table.
//!
//! **clap reads the environment at parse time, and that is what makes the parse two-pass**
//! (design decision 3): `<state dir>/.env` lives in a directory the mode decides, so it is
//! loaded *between* the two passes — [`parse`] runs both, [`Command::discovery`] is what the
//! first may be read for, and the second is the one whose values and `ValueSource`s FR-016's
//! chain resolves from.

use std::{ffi::OsString, path::PathBuf};

use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};
use smart_default::SmartDefault;

#[cfg(feature = "lifecycle")]
use crate::commands::lifecycle;
#[cfg(feature = "object-lock")]
use crate::commands::{legalhold, object_lock, retention};
#[cfg(feature = "api")]
use crate::commands::{status, stop};
use crate::{
    _util::secret::Secret,
    Error,
    commands::{EnvArgs, doctor, start, tier},
    resolve::pg,
    service,
};

/// An S3-compatible local storage server.
///
/// # Examples
///
/// ```rust
/// use clap::CommandFactory;
/// use tinio_cli::cli::Cli;
///
/// assert_eq!(Cli::command().get_name(), "tinio");
/// ```
#[derive(Debug, Parser)]
#[command(name = "tinio", version, about)]
pub struct Cli {
    /// The subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Where an env-backed flag's value came from, one field per such key.
///
/// Read off the `ArgMatches` the parse hands back (`ArgMatches::value_source`), because
/// `Cli::try_parse_from` returns the struct and drops the matches — which is the one thing
/// FR-016's chain needs to tell a flag from `TINIO_*`. `cli::provenance` is the only reader.
///
/// **One source per parse, so a value `.env` supplied is an `EnvVariable` answer like any
/// other.** dotenvy writes the file's values into the process environment (never over an
/// already-set variable), which is what puts `.env` below the process environment without a
/// precedence rule of its own — and what makes the two indistinguishable here: FR-016's chain
/// keeps its four levels, and the second of them is the one that answers for both.
///
/// clap's `ValueSource` implements no `Default`, so `SmartDefault` fills every key with
/// `DefaultValue` — the shape a caller with no parser in hand uses. `resolve::server`'s own
/// test is that caller; [`provenance`] is the one that reads a real parse.
///
/// # Examples
///
/// ```rust
/// use clap::parser::ValueSource;
/// use tinio_cli::cli::Provenance;
///
/// // The shape a caller with no parser in hand uses: every key at clap's own default.
/// assert!(matches!(
///     Provenance::default().port,
///     ValueSource::DefaultValue
/// ));
/// ```
#[derive(Debug, Clone, Copy, SmartDefault)]
pub struct Provenance {
    /// `--host` / `TINIO_HOST`.
    #[default(ValueSource::DefaultValue)]
    pub host: ValueSource,
    /// `--port` / `TINIO_PORT`.
    #[default(ValueSource::DefaultValue)]
    pub port: ValueSource,
    /// `--verbosity` / `TINIO_LOG_LEVEL`.
    #[default(ValueSource::DefaultValue)]
    pub verbosity: ValueSource,
    /// `--log-format` / `TINIO_LOG_FORMAT`.
    #[default(ValueSource::DefaultValue)]
    pub log_format: ValueSource,
    /// `--read-only` / `TINIO_READ_ONLY` — the mode's two channels above the config file
    /// (decision 4), and the reason `resolve::resolve_mode` can tell an explicit
    /// `TINIO_READ_ONLY=0` from nothing having answered.
    #[default(ValueSource::DefaultValue)]
    pub read_only: ValueSource,
    /// `--sandbox` / `TINIO_SANDBOX` — the confinement's two, which `sandbox::plan` reads to
    /// name the channel its refusal came from.
    #[default(ValueSource::DefaultValue)]
    pub sandbox: ValueSource,
}

/// `--host`'s variable name (config `[server] host`).
///
/// The CLI's own names live here, and the flags' `env` attributes spell the same strings:
/// `start::Args`'s test asserts every pair agrees, which is what keeps a declaration and its
/// constant from drifting. `TINIO_READ_ONLY`, `TINIO_SANDBOX` and `TINIO_STATE_DIR` are
/// `tinio_util::env`'s — the switches' parser and the state-dir parser name them — so they are
/// re-exported or checked from that one definition rather than declared a second time here.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::HOST_VAR;
///
/// assert_eq!(HOST_VAR, "TINIO_HOST");
/// ```
pub const HOST_VAR: &str = "TINIO_HOST";

/// `--port`'s variable name (config `[server] port`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::PORT_VAR;
///
/// assert_eq!(PORT_VAR, "TINIO_PORT");
/// ```
pub const PORT_VAR: &str = "TINIO_PORT";

/// `--verbosity`'s variable name (config `[log] verbosity`).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::LOG_LEVEL_VAR;
///
/// assert_eq!(LOG_LEVEL_VAR, "TINIO_LOG_LEVEL");
/// ```
pub const LOG_LEVEL_VAR: &str = "TINIO_LOG_LEVEL";

/// `--log-format`'s variable name (config `[log] server_log_format`). Not a server-log-*file*
/// variable: the contract's list has none, and `TINIO_ACCESS_LOG` names the access log.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::LOG_FORMAT_VAR;
///
/// assert_eq!(LOG_FORMAT_VAR, "TINIO_LOG_FORMAT");
/// ```
pub const LOG_FORMAT_VAR: &str = "TINIO_LOG_FORMAT";

/// `--read-only`'s and `--sandbox`'s variable names (config `[server] read_only` and
/// `[server] sandbox`).
///
/// Re-exported from [`tinio_util::env`], the one definition of the two names:
/// `--read-only`/`TINIO_READ_ONLY` is FR-023's mode and `--sandbox`/`TINIO_SANDBOX` is design
/// decision 10's confinement.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::{READ_ONLY_VAR, SANDBOX_VAR};
///
/// assert_eq!(READ_ONLY_VAR, "TINIO_READ_ONLY");
/// assert_eq!(SANDBOX_VAR, "TINIO_SANDBOX");
/// ```
pub use crate::_util::env::{READ_ONLY_VAR, SANDBOX_VAR};

/// The access-key variable's name (`contracts/config.md:110`), the `[auth]` pair's first half.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::ACCESS_KEY_VAR;
///
/// assert_eq!(ACCESS_KEY_VAR, "TINIO_ACCESS_KEY");
/// ```
pub const ACCESS_KEY_VAR: &str = "TINIO_ACCESS_KEY";

/// The secret-key variable's name, the pair's other half — the two are set together or not at
/// all. The `MINIO_*` names are deliberately not here: they are the data plane's own fallback
/// (T084), not a channel of this resolution.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::SECRET_KEY_VAR;
///
/// assert_eq!(SECRET_KEY_VAR, "TINIO_SECRET_KEY");
/// ```
pub const SECRET_KEY_VAR: &str = "TINIO_SECRET_KEY";

/// `--pg-dsn`'s variable name (config `[storage.pg] dsn`, PG5), the pg backend's connection
/// string.
///
/// **It overrides the config key and nothing else**: the backend is selected by
/// `[storage] type` alone, so an exported dsn can never turn a filesystem run into a pg one. A
/// secret's documented channel is the environment, which is why the flag carrying it is hidden
/// (the same rule `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` follow).
///
/// # Examples
///
/// ```rust
/// use tinio_cli::cli::PG_DSN_VAR;
///
/// assert_eq!(PG_DSN_VAR, "TINIO_PG_DSN");
/// ```
pub const PG_DSN_VAR: &str = "TINIO_PG_DSN";

/// The provenance of every env-backed key, in one place.
///
/// **The lookup happens on the subcommand's own matches**, descended into by the name the parse
/// reports: the env-backed flags live there, and asking the top-level matches for `port` is a
/// clap panic rather than an absent id. A caller that already holds the subcommand's matches — a
/// unit test driving the parse directly — passes them as they are.
///
/// `ValueSource` implements no `Default` (`clap_builder-4.6.7/src/parser/matches/value_source.rs:2`),
/// so an absent key — one the parser never saw at all, which is every key of an arm that does
/// not declare it — resolves to `DefaultValue`, the same answer a key that fell back on clap's
/// default gives, and what `pick`'s `_` arm already handles.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, parser::ValueSource};
/// use tinio_cli::cli::{Cli, provenance};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "server", "/srv/data", "--port", "9001"])
///     .expect("parse");
/// assert!(matches!(
///     provenance(&matches).port,
///     ValueSource::CommandLine
/// ));
/// ```
pub fn provenance(matches: &ArgMatches) -> Provenance {
    // The name is looked up before the descent because `subcommand_matches` panics for a
    // subcommand this command does not define — and the matches handed in are either the
    // top-level ones (which define it) or the arm's own (which do not).
    let matches = match matches.subcommand_name() {
        Some(name) => matches.subcommand_matches(name).unwrap_or(matches),
        None => matches,
    };
    // Two lookups, and the first is why this is not a bare `value_source`: that one *panics*
    // (in a debug build) for an id the arm does not declare, and an arm declares only its own
    // keys — a client command has no `--host`. `try_contains_id` answers `Err` for exactly that
    // case and the presence flag otherwise, and either way the key never answered, which is
    // what a missing entry means to `pick`.
    let source = |id: &str| {
        matches
            .try_contains_id(id)
            .ok()
            .filter(|present| *present)
            .and_then(|_present| matches.value_source(id))
            .unwrap_or(ValueSource::DefaultValue)
    };
    Provenance {
        host: source("host"),
        port: source("port"),
        verbosity: source("verbosity"),
        log_format: source("log_format"),
        read_only: source("read_only"),
        sandbox: source("sandbox"),
    }
}

/// The parse, in the one place both of them run it (design decision 3).
///
/// **The same argv is parsed twice per run**, because clap reads the environment at parse time
/// and `<state dir>/.env` is not in that environment until the state dir is known: the first
/// pass — the **discovery** one — answers `DIR`, `--config` and the two values that decide where
/// the file is ([`Command::discovery`]), the file is then loaded, and the second pass is the
/// authoritative one. Nothing here exits: clap's own `Error` maps to [`Error::Usage`], so a
/// usage error reaches the process as exit 2 through `Error::exit_code`.
///
/// # Examples
///
/// ```rust
/// use std::ffi::OsString;
///
/// use tinio_cli::cli::{Command, parse};
///
/// let argv: Vec<OsString> = ["tinio", "server", "/srv/data"]
///     .iter()
///     .map(OsString::from)
///     .collect();
/// let (cli, _matches) = parse(&argv).expect("parse");
/// assert!(matches!(cli.command, Command::Server(_)));
/// ```
pub fn parse(argv: &[OsString]) -> Result<(Cli, ArgMatches), Error> {
    let matches = Cli::command()
        .try_get_matches_from(argv)
        .map_err(|err| Error::Usage(err.to_string()))?;
    let cli = Cli::from_arg_matches(&matches).map_err(|err| Error::Usage(err.to_string()))?;
    Ok((cli, matches))
}

/// What the user asked for.
///
/// **`Status`, `Stop` and `Lifecycle` are compiled out with their own features** (the `status`
/// and `stop` sections say so; T055 asserts the same), so their variants sit behind
/// `#[cfg(feature = "api")]` and `#[cfg(feature = "lifecycle")]`, and without it a client
/// command is an unknown subcommand — clap's own usage error, exit 2. **`Lifecycle` is `api`'s
/// too**: the verbs speak the management plane's routes through `tinio-api`'s client, so a build
/// with no plane has nothing for them to talk to. **`ObjectLock`, `Retention` and `LegalHold`
/// are `object-lock`'s** (which requires `api`), because they drive the plane's two object-lock
/// routes and never a store.
///
/// **`Doctor` deliberately does not**: it diagnoses the always-present layers (root, config,
/// names, symlinks, free space, the `Cleanup` trait's private-state legs, the meta store), which
/// need no plane, so the command exists in both builds. **Which of its parts are gated, and why,
/// is the contract's doctor section's statement** — this doc keeps only the operational half —
/// and without the feature the subcommand list is `Server`, `Service`, `Doctor` and `Tier`.
///
/// **`Tier` does not either, for the same reason**: its three verbs open a root's own planes
/// offline and need no control channel; `tinio-tier` is a non-optional dependency of this crate,
/// so the group compiles in every graph. The `tier` cargo feature gates the *serving* wiring, not
/// these offline verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "server", "/srv/data"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// // `start` is the Minio-style alias the contract's `server`/`start` section names.
/// assert!(matches!(cli.command, Command::Server(_)));
/// ```
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Serve a storage root (Minio-style: the directory is positional).
    #[command(alias = "start")]
    Server(start::Args),
    /// Report whether a server is running for a root.
    #[cfg(feature = "api")]
    Status(status::Args),
    /// Ask a running server to stop.
    #[cfg(feature = "api")]
    Stop(stop::Args),
    /// Configure a bucket's lifecycle policy through a running server.
    #[cfg(feature = "lifecycle")]
    Lifecycle(lifecycle::Args),
    /// Enable a bucket's Object Lock through a running server.
    #[cfg(feature = "object-lock")]
    ObjectLock(object_lock::Args),
    /// Set, read or clear a version's retention through a running server.
    #[cfg(feature = "object-lock")]
    Retention(retention::Args),
    /// Set, read or clear a version's legal hold through a running server.
    #[cfg(feature = "object-lock")]
    #[command(name = "legalhold")]
    LegalHold(legalhold::Args),
    /// Diagnose a directory without a server.
    Doctor(doctor::Args),
    /// Inspect, repair or downgrade a tier root without a server.
    Tier(tier::Args),
    /// Manage the OS service that supervises a server.
    Service(service::Args),
}

/// What the **discovery parse** may be read for (design decision 3): the values that decide
/// where `<state dir>/.env` is, and therefore the only ones the first of the two passes can
/// answer.
///
/// The mode is among them because it is the state dir's input — the state dir relocates in
/// read-only mode — which is exactly why decision 4 keeps `.env` out of the mode: a file whose
/// own location is a function of the mode cannot also select it.
///
/// **`Clone` is for the one caller that resolves with these answers twice** (ruling R8's sibling
/// finding): a pg run whose dsn the `.env` supplies, where the root has to be derived from it and
/// the store has to open the same value, so `run::command_line` locates once, then
/// re-locates with the dsn the authoritative parse answered. Two uses of one parse's answers, not
/// a second read of anything.
///
/// # Examples
///
/// ```rust
/// use std::ffi::OsString;
///
/// use tinio_cli::cli::{Command, parse};
///
/// let argv: Vec<OsString> = ["tinio", "server", "/srv/data", "--read-only"]
///     .iter()
///     .map(OsString::from)
///     .collect();
/// let (cli, _matches) = parse(&argv).expect("parse");
/// assert!(matches!(cli.command, Command::Server(_)));
/// let discovery = cli.command.discovery();
/// assert_eq!(discovery.dir, Some("/srv/data".into()));
/// assert!(discovery.read_only);
/// ```
#[derive(Debug, Clone)]
pub struct Discovery {
    /// `DIR`.
    pub dir: Option<PathBuf>,
    /// `--config`, which only the `server` arm has: every other arm diagnoses or reports on the
    /// discovered file and never names one.
    pub config: Option<PathBuf>,
    /// The mode's two clap channels, `--read-only` and `TINIO_READ_ONLY`, as one answer —
    /// `Provenance::read_only`'s `ValueSource` is what says whether clap answered at all, which
    /// is how an explicit `TINIO_READ_ONLY=0` beats a config file saying `read_only = true`.
    pub read_only: bool,
    /// `TINIO_STATE_DIR`, the state-dir override — read here and never after the `.env` load:
    /// the file lives *in* the directory this value selects.
    pub state_dir: Option<PathBuf>,
    /// `TINIO_PG_DSN` (`--pg-dsn`), the pg backend's dsn (PG5). Read by the **discovery** parse
    /// because the synthetic root it decides is part of this parse's answers: a pg run's root is
    /// derived from the dsn's hash, which is what [`crate::resolve::locate`] needs before the
    /// config file — itself found under that root — can be read.
    ///
    /// **An empty value is not here**: the parse's `EnvArgs::resolved_pg_dsn` reads an empty
    /// variable as the unset one it means, so `None` is "no dsn addressed" rather than "a dsn
    /// whose material is empty".
    pub pg_dsn: Option<Secret>,
}

impl Command {
    /// The discovery inputs of the arm this is.
    ///
    /// **The `service` arm's are never read, and it is a rule rather than an omission**: that
    /// command reads no configuration file — the unit it installs is what will read one — so
    /// `.env` is not part of its chain and `lib.rs` answers it from the discovery parse without
    /// loading the file at all. What its arm answers is still the same three values a `server`
    /// would resolve for the same root, because the verbs resolve the root, the mode and the
    /// state dir exactly as the server does.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::ffi::OsString;
    ///
    /// use tinio_cli::cli::parse;
    ///
    /// // `doctor` rather than `status`: the client commands are `api`-gated, and this leg runs
    /// // in both graphs.
    /// let argv: Vec<OsString> = ["tinio", "doctor", "/srv/data", "--read-only"]
    ///     .iter()
    ///     .map(OsString::from)
    ///     .collect();
    /// let (cli, _matches) = parse(&argv).expect("parse");
    /// assert!(cli.command.discovery().read_only);
    /// assert_eq!(cli.command.discovery().dir, Some("/srv/data".into()));
    /// ```
    pub fn discovery(&self) -> Discovery {
        match self {
            Self::Server(args) => Discovery {
                dir: args.dir.clone(),
                config: args.config.clone(),
                read_only: args.read_only,
                state_dir: args.env.state_dir.clone(),
                pg_dsn: pg::named(args.env.pg_dsn.clone()),
            },
            #[cfg(feature = "api")]
            Self::Status(args) | Self::Stop(args) => {
                client_discovery(args.dir.clone(), args.read_only, &args.env)
            }
            #[cfg(feature = "lifecycle")]
            Self::Lifecycle(args) => {
                let dir = args.dir_args();
                client_discovery(dir.dir.clone(), dir.read_only, &dir.env)
            }
            #[cfg(feature = "object-lock")]
            Self::ObjectLock(args) => {
                let dir = args.dir_args();
                client_discovery(dir.dir.clone(), dir.read_only, &dir.env)
            }
            #[cfg(feature = "object-lock")]
            Self::Retention(args) => {
                let dir = args.dir_args();
                client_discovery(dir.dir.clone(), dir.read_only, &dir.env)
            }
            #[cfg(feature = "object-lock")]
            Self::LegalHold(args) => {
                let dir = args.dir_args();
                client_discovery(dir.dir.clone(), dir.read_only, &dir.env)
            }
            Self::Doctor(args) => {
                client_discovery(args.dir.dir.clone(), args.dir.read_only, &args.dir.env)
            }
            Self::Tier(args) => {
                let dir = args.dir_args();
                client_discovery(dir.dir.clone(), dir.read_only, &dir.env)
            }
            Self::Service(args) => client_discovery(args.dir.clone(), args.read_only, &args.env),
        }
    }
}

/// The `Discovery` the client commands share — the status, stop, doctor and service arms all
/// answer it: no `--config` (that flag is the server's alone), and the state dir and pg dsn the
/// environment named.
fn client_discovery(dir: Option<PathBuf>, read_only: bool, env: &EnvArgs) -> Discovery {
    Discovery {
        dir,
        config: None,
        read_only,
        state_dir: env.state_dir.clone(),
        pg_dsn: pg::named(env.pg_dsn.clone()),
    }
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;
    // `TINIO_STATE_DIR`'s name is `tinio_util::env`'s (`cli`'s constant doc states why), so the
    // closed-set walk below checks that name rather than a second constant here.
    use crate::_util::{env::STATE_DIR_VAR, testing::assert_send_sync};

    /// The set of variables the parser may read is the contract's, and it is closed: an argument
    /// reading anything else is a channel no document describes. Walked over **every** arm at
    /// every depth — `lifecycle`'s four verbs are subcommands of a subcommand, and their arguments
    /// carry the same channels — so a new arm cannot add one silently, one level down included.
    #[test]
    fn the_parser_reads_no_undocumented_variable() {
        let documented = [
            HOST_VAR,
            PORT_VAR,
            LOG_LEVEL_VAR,
            LOG_FORMAT_VAR,
            READ_ONLY_VAR,
            SANDBOX_VAR,
            STATE_DIR_VAR,
            ACCESS_KEY_VAR,
            SECRET_KEY_VAR,
            PG_DSN_VAR,
        ];
        // A worklist rather than one `get_subcommands()`: a walk that stopped at the first level
        // would promise coverage it no longer has.
        let root = Cli::command();
        let mut arms = vec![&root];
        let mut visited = Vec::new();
        while let Some(command) = arms.pop() {
            arms.extend(command.get_subcommands());
            visited.push(command.get_name());
            for arg in command.get_arguments() {
                let Some(name) = arg.get_env() else { continue };
                let name = name.to_string_lossy().into_owned();
                assert!(
                    documented.contains(&name.as_str()),
                    "`{} {}` reads an undocumented variable `{name}`",
                    command.get_name(),
                    arg.get_id()
                );
            }
        }
        // **The walk is not self-guarding**: a one-level version passed this suite while
        // `lifecycle`'s four verbs went uninspected, which is how that hole arose. So the depth
        // the walk promises is asserted, not assumed.
        #[cfg(feature = "lifecycle")]
        assert!(
            visited.contains(&"ls"),
            "the walk must reach a verb nested under `lifecycle`: {visited:?}"
        );
        // `TINIO_SCANNER` and `TINIO_ANONYMOUS` are the contract's and are not the CLI's: the
        // first is `tinio-fs`'s own switch, the second US3's T087.
        assert!(!documented.contains(&"TINIO_SCANNER"));
    }

    #[test]
    fn no_subcommand_is_a_usage_error() {
        // The parser's half of it: a bare invocation fails to parse here, which is what makes
        // `try_run` answer `Error::Usage` and the process exit 2 rather than clap's own `exit`.
        // The process-level half — exit code 2, and a message on stderr — is
        // `cli_lifecycle.feature`'s "A bare invocation is a usage error".
        assert!(Cli::command().try_get_matches_from(["tinio"]).is_err());
    }

    #[test]
    fn the_public_surface_is_send_and_sync() {
        assert_send_sync::<Cli>();
        assert_send_sync::<Command>();
        assert_send_sync::<Provenance>();
        assert_send_sync::<Discovery>();
    }
}
