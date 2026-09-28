//! Root, mode, state-dir and config resolution (FR-016, design §1).
//!
//! The order is §1's and it is an order rather than a list: the root is canonicalised before
//! anything is joined to it, the mode decides where the state dir is, the state dir decides
//! where `.env` and the config file live, and the config is loaded last — so an explicit
//! `--config` is checked before `load_or_create` can see it, and the only absent path that
//! reaches the create branch is discovery's own fallback.
//!
//! **The chain is `CLI flags > process environment > .env > config file`, and clap answers its
//! top three levels.** Every variable is an argument with an `env` attribute, and `.env` reaches
//! that parser because the load happens *between* its two passes (design decision 3): the
//! **discovery** parse answers what [`locate`] needs — `DIR`, `--config`, the mode's two
//! channels and `TINIO_STATE_DIR` — `<state dir>/.env` is then loaded into the process
//! environment (dotenvy never overrides an exported variable, which is what keeps the process
//! environment above the file), and the **authoritative** parse is the one whose values and
//! `ValueSource`s [`pick`] reads.
//!
//! Two consequences are worth stating where the chain is, because they read like surprises
//! otherwise: a value `.env` supplied arrives as an `EnvVariable` answer like any other, so the
//! second and third levels are no longer distinguishable in [`Provenance`] — the file is the
//! third level by *when* it was read, not by what the parser says about it; and the config file
//! is the only level [`pick`] still reads, because it is the only one clap has no answer for.

#[cfg(test)]
use std::{cell::Cell, ffi::OsString};
use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::parser::ValueSource;

use crate::{
    _config::{
        self, Config, auth, discover,
        log::{self, Format},
        server, storage,
    },
    _core::{mode::AccessMode, pipeline::InlineRunner},
    _fs::{Error as FsError, FsOptions, etag},
    _server::{identity, pipeline::Pipelines as ServerPipelines},
    Error,
    cli::{self, Provenance},
    commands::{DirArgs, EnvArgs, start},
};

mod locate;

use locate::chain;
pub(crate) use locate::current_dir;
pub use locate::{
    Located, apply_env_file, config_path, locate, locate_confined, pick, resolve_mode,
    resolve_root, resolve_state_dir, state_dir_candidates,
};

/// `[server] port`'s default — Minio's own (`contracts/cli.md:36`), and the port an
/// `--address` without one gets. Read from the schema, which is the value's one definer, so the
/// flag's default cannot drift from the config file's.
fn default_port() -> u16 {
    server::Config::default().port
}

/// The pipeline bundle this CLI builds: `tinio_server::pipeline::Pipelines` with its three
/// lanes' output types spelled — the IO lane carries `tinio_fs`'s ETag tasks, and the removal
/// and DB lanes the backend's unit results.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::resolve::Pipelines;
/// use tinio_config::pipeline::Config;
///
/// let pipelines = Pipelines::build(&Config::default()).expect("pipelines");
/// assert_eq!(pipelines.io().kind(), "io");
/// ```
pub type Pipelines = ServerPipelines<etag::Result, Result<(), FsError>, Result<(), FsError>>;

/// The resolved root, config, mode and state dir — every command's starting point.
///
/// `config_error` is the load outcome beside the parsed config: `None` for a config that
/// loaded, `Some(message)` when it did not — in which case `config` is `Config::default()` and
/// the message is what the operator is shown. It exists because "the configuration does not
/// parse" is one of `doctor`'s findings, and a `Resolution` that could only be built for a
/// valid config would put that check out of reach; [`server()`] propagates the failure instead,
/// because a server must not run on a config it could not read.
///
/// **`config_text` is the document that config was parsed from, and it is here because the
/// launcher forwards it** (design decision 18): a confined child may not be able to read the file
/// at all — its path can sit outside every exposure the confinement grants — so the text travels
/// as the launch payload instead of the path, and `sandbox::launch::prepare` used to get it by
/// reading the file a **third** time in one start (`resolve::mode_config` and this chain's own
/// load are the other two). The text is the read's own bytes rather than a serialization of the
/// parsed config, and a serving `Resolution` always carries one: the chain that reads or creates a
/// document is the one every launching command runs, and a failure to read it is an error rather
/// than an empty string. A [`for_doctor`] resolution carries whatever that command read, which no
/// launcher asks for.
///
/// **`state_dir_override` is the discovery parse's `TINIO_STATE_DIR`, carried rather than dropped**
/// — the *path* it named, which is also the whole of what a directory's *source* is (named by the
/// operator, or derived by the resolution). It lives here for one reason: `Located` is consumed by
/// the chain that builds this value, so a caller that wanted it had to take it off `Located`
/// **before** the move, and two callers did exactly that with the same comment. A value a
/// resolution answers is the resolution's to carry, and the one guard that needs the source
/// (macOS's read-only rule, `crate::sandbox`) derives it from this field beside the resolution
/// rather than the resolution carrying the same fact twice.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_cli::resolve::Resolution;
/// use tinio_config::Config;
///
/// let resolution = Resolution {
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_text: String::new(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     state_dir_override: None,
///     config_error: None,
/// };
/// assert_eq!(resolution.address().expect("address").port(), 9000);
/// ```
#[derive(Debug)]
pub struct Resolution {
    /// The served storage root, canonical.
    pub root: PathBuf,
    /// The effective configuration: the file, with the flags and `.env` folded in.
    pub config: Config,
    /// The text [`config`](Self::config) was parsed from — the launch payload's own bytes (design
    /// decision 18), read once by the chain that built this resolution.
    pub config_text: String,
    /// The file [`config`](Self::config) was read from.
    pub config_path: PathBuf,
    /// The resolved mode (decision 4).
    pub read_only: bool,
    /// The resolved state directory.
    pub state_dir: PathBuf,
    /// `TINIO_STATE_DIR` as the discovery parse read it: the directory's **source**, and the
    /// candidate pair's other element for the client commands
    /// ([`crate::resolve::state_dir_candidates`]).
    pub state_dir_override: Option<PathBuf>,
    /// The config load's failure, when it had one and the caller tolerates it.
    pub config_error: Option<String>,
}

impl Resolution {
    /// The resolved bind address, from `[server] host` and `[server] port` — the two fields,
    /// as the flags and `.env` folded them.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::PathBuf;
    ///
    /// use tinio_cli::resolve::Resolution;
    /// use tinio_config::Config;
    ///
    /// let resolution = Resolution {
    ///     root: PathBuf::from("/srv/data"),
    ///     config: Config::parse("version = 1\n[server]\nhost = \"0.0.0.0\"\nport = 8080\n")
    ///         .expect("config"),
    ///     config_text: String::new(),
    ///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
    ///     read_only: false,
    ///     state_dir: PathBuf::from("/srv/data/.tinio"),
    ///     state_dir_override: None,
    ///     config_error: None,
    /// };
    /// assert_eq!(
    ///     resolution.address().expect("address").to_string(),
    ///     "0.0.0.0:8080"
    /// );
    /// ```
    pub fn address(&self) -> Result<SocketAddr, Error> {
        bind_addr(
            "server.host",
            &self.config.server.host,
            self.config.server.port,
        )
    }
}

/// A bind address from a host and a port: `[server] host`/`port` and the `[api.http]` pair.
///
/// `key` is what a refusal names — the configuration key the host came from, so an unparseable
/// `[api.https] host` says `api.https.host` rather than `server.host`.
pub(crate) fn bind_addr(key: &str, host: &str, port: u16) -> Result<SocketAddr, Error> {
    let authority = authority(host, port);
    authority.parse().map_err(|_| {
        Error::Config(_config::Error::invalid_value(
            key,
            format!("`{authority}` is not a bind address"),
        ))
    })
}

/// A `host:port` authority — an IPv6 literal needs its brackets back before it is one. The one
/// spelling of that rule, for both sides of it: `bind_addr` parses it, `status`'s report
/// prints it.
pub fn authority(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// An `--address`-shaped authority (`HOST:PORT`, `:PORT` or `HOST`; the host defaults to
/// loopback and the port to `default_port`).
pub(crate) fn authority_addr(authority: &str, default_port: u16) -> Result<SocketAddr, Error> {
    let spelled = spell_authority(authority, default_port)?;
    spelled.parse().map_err(|_| {
        Error::Usage(format!(
            "`{authority}` is not a bind address: expected `HOST:PORT`, `:PORT`, `HOST` or `[IPv6]:PORT`"
        ))
    })
}

/// `HOST:PORT`, `:PORT`, `HOST`, `[IPv6]` or `[IPv6]:PORT`.
///
/// A single `split_once(':')` treats the first colon of `[::1]:9000` as the port separator.
/// Bracketed IPv6 is split on the closing bracket; a bare IPv6 (more than one colon) has no
/// port slot and takes `default_port`.
fn spell_authority(authority: &str, default_port: u16) -> Result<String, Error> {
    if let Some(rest) = authority.strip_prefix('[') {
        let Some((host, after)) = rest.split_once(']') else {
            return Err(Error::Usage(format!(
                "`{authority}` has no closing bracket: expected `[IPv6]` or `[IPv6]:PORT`"
            )));
        };
        let port = match after {
            "" => default_port,
            tail => {
                let Some(raw) = tail.strip_prefix(':') else {
                    return Err(Error::Usage(format!(
                        "`{authority}` is not a bind address: expected `[IPv6]` or `[IPv6]:PORT`"
                    )));
                };
                if raw.is_empty() {
                    default_port
                } else {
                    raw.parse().map_err(|_| {
                        Error::Usage(format!(
                            "`{authority}` has no usable port: expected `[IPv6]:PORT`"
                        ))
                    })?
                }
            }
        };
        return Ok(format!("[{host}]:{port}"));
    }
    if authority.matches(':').count() > 1 {
        return Ok(format!("[{authority}]:{default_port}"));
    }
    match authority.split_once(':') {
        Some((host, port)) => {
            let host = if host.is_empty() { "127.0.0.1" } else { host };
            let port = if port.is_empty() {
                default_port
            } else {
                port.parse().map_err(|_| {
                    Error::Usage(format!(
                        "`{authority}` has no usable port: expected HOST:PORT"
                    ))
                })?
            };
            Ok(format!("{host}:{port}"))
        }
        None => Ok(format!("{authority}:{default_port}")),
    }
}

/// The effective operational log file: `--log-file`, then `[log] server_log_file`, then the
/// format-derived default **in the state dir** — a daemon has no console, and Task 5's daemon
/// redirects to this answer rather than declaring a rule of its own.
///
/// A configured path is used verbatim (the config contract scopes the state-directory rule to
/// the *access* log), and an empty `[log] server_log_file` is the config's own "stderr", which
/// is what makes the third level reachable.
///
/// # Examples
///
/// ```rust
/// use std::path::PathBuf;
///
/// use tinio_cli::{
///     commands::start::Args,
///     resolve::{Resolution, log_file},
/// };
/// use tinio_config::Config;
///
/// let resolution = Resolution {
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_text: String::new(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     state_dir_override: None,
///     config_error: None,
/// };
/// let path = log_file(&resolution, &Args::default())?;
/// assert!(path.ends_with("server.log"));
/// # Ok::<(), tinio_cli::Error>(())
/// ```
pub fn log_file(resolution: &Resolution, args: &start::Args) -> Result<PathBuf, Error> {
    if let Some(path) = &args.log_file {
        return Ok(path.clone());
    }
    let configured = resolution
        .config
        .log
        .as_ref()
        .map(|log| log.server_log_file.as_str())
        .filter(|path| !path.is_empty());
    if let Some(path) = configured {
        return Ok(PathBuf::from(path));
    }
    // **Read, never re-derived.** The format level is the four-level chain's answer, and
    // `fold_flags` has already run that chain and written it into `config.log.server_log_format`
    // — every `Resolution` this is called with comes from `resolve::server`, which ends in
    // `fold_flags`. Running `pick` again here would be a second implementation of a rule that
    // already has one owner, and the two could drift on the next level or variable added.
    let format = resolution
        .config
        .log
        .as_ref()
        .map_or_else(Format::default, |log| log.server_log_format);
    Ok(resolution.state_dir.join(match format {
        Format::Json => "server.json",
        Format::Text => "server.log",
    }))
}

/// The server's chain (design §1 step f), on the discovery answers and the **authoritative**
/// parse: config path, load, and the flags' fold.
///
/// It fails on a config it could not read — a server must not serve on the default one — and
/// it is the same code [`for_doctor`] runs, so the two commands cannot resolve a root
/// differently: the difference is the chain's own argument, and `Chain::Serving` is the arm
/// that **creates** a missing configuration file (`contracts/cli.md:45`: it is generated "on
/// first start"), which no other command's chain does.
///
/// `args` and `provenance` are the **second** parse's: they are what a value `.env` supplied
/// arrives in, and folding the discovery parse's here would silently ignore the file.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::Args,
///     resolve::{locate, server},
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution = server(located, &args, &Provenance::default())?;
/// println!("serving {}", resolution.root.display());
/// # Ok(())
/// # }
/// ```
pub fn server(
    located: Located,
    args: &start::Args,
    provenance: &Provenance,
) -> Result<Resolution, Error> {
    server_with(located, args, provenance, None)
}

/// [`server()`] for a process a confinement launched: the configuration is the **payload's
/// text**, never a second read of the file it came from (design decision 18).
///
/// The launcher's resolution created a missing file, parsed the document and forwarded its
/// text; the file itself may sit outside every exposure, so a child that read the path again
/// would be denied by the confinement it is running under — and where the launcher generated
/// the file, a second `load_or_create` would mint a second credential pair.
///
/// The flags and the environment fold on top exactly as they did in the launcher: the child's
/// argv and environment are the launcher's own, so no level of the chain can disagree.
///
/// # Errors
///
/// A payload that does not parse, and every failure `fold_flags` reports.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::start::Args,
///     resolve::{locate, server_confined},
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     dir: Some("/srv/data".into()),
///     ..Args::default()
/// };
/// let resolution = server_confined(located, &args, &Provenance::default(), "version = 1\n")?;
/// assert_eq!(resolution.root, std::path::Path::new("/srv/data"));
/// # Ok(())
/// # }
/// ```
pub fn server_confined(
    located: Located,
    args: &start::Args,
    provenance: &Provenance,
    config_text: &str,
) -> Result<Resolution, Error> {
    server_with(located, args, provenance, Some(config_text))
}

/// [`server`] and [`server_confined`]'s one body: the chain, then the flag fold.
fn server_with(
    located: Located,
    args: &start::Args,
    provenance: &Provenance,
    adopted: Option<&str>,
) -> Result<Resolution, Error> {
    let mut resolution = resume(located, Chain::Serving, adopted)?;
    fold_flags(args, provenance, &mut resolution)?;
    Ok(resolution)
}

/// The same chain for the commands that never launch a server — plan C's `doctor` and, through
/// `commands::client_resolution`, `status` and `stop` — so decision 19's client-side
/// resolution is one implementation and not three.
///
/// Three differences, all in the arguments to the shared order: [`Located::config`] is `None`
/// (`--config` is the server's, and the discovery pass never fills it for these arms — `doctor`
/// diagnoses the discovered configuration), the chain **reads** the configuration instead of
/// creating one (`Chain::Reading`: a command asked to report on a root, or to diagnose it, must
/// leave that root exactly as it found it — design decisions 14 and 26), and it **records** a
/// config failure in [`Resolution::config_error`] instead of propagating it, because "the
/// configuration does not parse" is one of the findings `doctor` exists for.
///
/// What it does with `args` is the `[auth]` fold and nothing else: a client resolves no mode,
/// root or state dir of its own — those are [`Located`]'s, which the caller resolved once for
/// both parses ([`locate`]) — but a `.env`-supplied credential pair is part of the resolution
/// every command shares.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::DirArgs,
///     resolve::{for_doctor, locate},
/// };
///
/// # fn main() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = DirArgs {
///     dir: Some("/srv/data".into()),
///     ..DirArgs::default()
/// };
/// let resolution = for_doctor(located, &args)?;
/// assert!(resolution.config_path.ends_with(".tinio.toml"));
/// # Ok(())
/// # }
/// ```
pub fn for_doctor(located: Located, args: &DirArgs) -> Result<Resolution, Error> {
    let mut resolution = resume(located, Chain::Reading, None)?;
    fold_credentials(&args.env, &mut resolution.config)?;
    Ok(resolution)
}

/// Which of the two chains a caller runs — the one a **launching** command runs, or the one
/// every other command runs.
///
/// The two differ in exactly two ways, and they travel as one value rather than two parameters
/// because no caller mixes them: the serving chain **creates** a missing configuration file
/// (`contracts/cli.md:45`: it is generated "on first start") and refuses to run on one it could
/// not read, while a non-launching command reads what is there — falling back to the defaults
/// when nothing is — and records the failure in [`Resolution::config_error`] instead of failing.
///
/// **The second difference is the load-bearing one for this value existing at all**: a command
/// that launches nothing must never write a configuration file. It was asked to report on a root
/// or to diagnose it, and `discover::load_or_create` writes the file *and* a generated credential
/// pair inside it (design decisions 14 and 26; plan C's assertion is that `doctor` never
/// auto-creates anything).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    /// `server`: create the configuration on first start, and propagate a load failure.
    Serving,
    /// Every other command (`status`, `stop`, plan C's `doctor`): load or default, never write,
    /// and record a load failure.
    Reading,
}

/// The chain's tail, every command's: config path → load → the mode pinned into the config.
///
/// It starts from [`Located`], so the three steps that decided where `.env` lives — root, mode,
/// state dir — have already happened and cannot happen again here: a second resolution of any of
/// them would be a second answer to a question the first parse already answered. What is left is
/// the part that needs the state dir, and the two arms differ in one thing only ([`Chain`]).
///
/// A file that is absent, or present and unparseable, contributes nothing here; the failure
/// itself is the load's to report ([`Chain::Serving`] as an error, [`Chain::Reading`] as
/// `config_error`), and only the serving arm may write the file it did not find.
///
/// **`adopted` replaces the load outright when it is `Some`**: that is the confined child's
/// run, whose document is the text its launcher forwarded (design decision 18) — no path is
/// read and nothing is created, which is the whole reason the text travels.
///
/// **The two facts that used to be taken off [`Located`] before it was consumed are carried on**
/// (the discovery parse's `TINIO_STATE_DIR`, and the config text the document was read from):
/// both are attached to the [`Resolution`] below, so a caller reads them off the value it holds
/// rather than off the one this function moves.
fn resume(located: Located, chain: Chain, adopted: Option<&str>) -> Result<Resolution, Error> {
    let Located {
        root,
        read_only,
        state_dir,
        config: explicit,
        state_dir_override,
    } = located;
    // **The path is derived, not validated, when the document is already in hand** (design
    // decision 18): an adopted child's `--config` may name a file outside every exposure, and the
    // launcher — which could read it, and did — has already made the "it must be there" decision.
    // The path is still derived, so `Resolution::config_path` names the file the document came
    // from. Everywhere else the contract stands unchanged: `config_path` refuses an explicit path
    // that is not a file (usage error, exit 2), which is what a mistyped `--config` must hear.
    let path = match adopted {
        Some(_) => {
            discover::resolve_path(&root, &state_dir, explicit.as_deref()).map_err(Error::from)?
        }
        None => config_path(&root, &state_dir, explicit.as_deref())?,
    };
    // **The document's text is kept beside the configuration it parses to**, because the launch
    // payload carries the text and never the path (decision 18): `sandbox::launch::prepare` read
    // the file a third time for it — this chain's own read is the second — and the text it found
    // there could differ from the one this resolution is built on.
    let (loaded, text) = match (adopted, chain) {
        // The payload's text **is** the document: no path is read and nothing is created.
        (Some(text), _) => (Config::parse(text), text.to_owned()),
        // The serving chain **creates** the file it did not find (`contracts/cli.md:45`: it is
        // generated "on first start"), and the text of a document that call created is the text
        // that call wrote — `to_toml`'s own bytes, from the value it serialized them from. The
        // create hands those bytes back rather than this arm serializing the same `Config` again.
        //
        // **The loaded arm is reachable**: the guard above is this arm's own `is_file` test, and
        // another process can create the file between it and the call.
        (None, Chain::Serving) if !path.is_file() => {
            let (config, outcome) = discover::load_or_create(&root, &path, read_only)?;
            let text = match outcome {
                discover::Outcome::Created(text) => text,
                discover::Outcome::Loaded => config.to_toml()?,
            };
            (Ok(config), text)
        }
        // A **non-launching** command reads what is there and leaves the root exactly as it found
        // it (decisions 14 and 26): an absent file contributes nothing, and nothing is written.
        // The empty text is that absence spelled out, and no launcher ever reads it — the chain
        // that launches is [`Chain::Serving`], whose absent-file arm above creates the document.
        (None, Chain::Reading) if !path.is_file() => (Ok(Config::default()), String::new()),
        // Every other run reads the document **once** and keeps both answers.
        (None, _) => match document(&path) {
            Ok((config, text)) => (Ok(config), text),
            Err(err) => (Err(err), String::new()),
        },
    };
    let (mut config, config_error) = match loaded {
        Ok(config) => (config, None),
        Err(err) if chain == Chain::Reading => (Config::default(), Some(err.to_string())),
        Err(err) => return Err(err.into()),
    };

    // The mode is a config key as well as a resolution result, and the two are the same value:
    // `[server] read_only` is level 4 of the chain, so the effective mode is what the key means.
    config.server.read_only = read_only;

    Ok(Resolution {
        root,
        config,
        config_text: text,
        config_path: path,
        read_only,
        state_dir,
        state_dir_override,
        config_error,
    })
}

/// The `[auth]` overlay's fold: `TINIO_ACCESS_KEY`/`TINIO_SECRET_KEY` (and `.env` with them,
/// which is the same channel one level lower) replace the file's pair whenever they are set.
///
/// **Half a pair is a configuration failure, never a run that keeps the file's secret key and
/// silently drops the operator's access key** — the refusal names the half that is missing,
/// because that is the one the operator has to go and set.
///
/// The pair is not a config key, so it has no clap answer the fold could read a `ValueSource`
/// from: it is an environment-only argument pair ([`EnvArgs`]), and presence is the whole of
/// what a fold needs — with both halves present the answer is the variables', with neither it is
/// the file's, and with one it is the refusal above.
fn fold_credentials(env: &EnvArgs, config: &mut Config) -> Result<(), Error> {
    match (&env.access_key, &env.secret_key) {
        (None, None) => Ok(()),
        (Some(access_key), Some(secret_key)) => {
            config.auth = Some(auth::Config {
                access_key: access_key.clone(),
                secret_key: auth::SecretKey::from(secret_key.clone()).into(),
            });
            Ok(())
        }
        (access, _secret) => Err(_config::Error::invalid_value(
            if access.is_none() {
                cli::ACCESS_KEY_VAR
            } else {
                cli::SECRET_KEY_VAR
            },
            format!(
                "both {} and {} must be set together, or neither: on its own, the pair's other \
                 key silently falls back to the config file",
                cli::ACCESS_KEY_VAR,
                cli::SECRET_KEY_VAR
            ),
        )
        .into()),
    }
}

/// The document `path` holds: **the text**, and the configuration parsed from it — one read for
/// both answers.
///
/// [`Config::load`] answers only the second of them, and the text has to survive the call: it is
/// what the launch payload carries (design decision 18), because the file may sit outside every
/// exposure the launcher's confinement grants. `sandbox::launch::prepare` re-read the path for it
/// until this function existed, which was the **third** read of one start — `resolve::mode_config`
/// is the first, this is the second — and a second answer to what the document says: a file edited
/// between the resolution and the launch would have served a configuration the resolution never
/// saw. What this reads is the file as it is, and the parse is the one `Config::load` runs, so the
/// two answers cannot drift.
///
/// **A refusal still names the file.** `Config::parse` labels the document it is handed `(inline)`
/// — the payload's case, where no file is behind the text — and this one came off a disk, so the
/// parse's own message is re-spelled with the path it came from rather than with a spelling that
/// would leave the operator hunting for which file is broken.
fn document(path: &Path) -> Result<(Config, String), _config::Error> {
    let text = fs::read_to_string(path).map_err(|source| _config::Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    match Config::parse(&text) {
        Ok(config) => Ok((config, text)),
        Err(_config::Error::Parse { message, .. }) => Err(_config::Error::Parse {
            path: path.to_path_buf(),
            message,
        }),
        Err(err) => Err(err),
    }
}

/// The config the **mode** is resolved from, before the state dir is known.
///
/// `<root>/.tinio/{.tinio.toml,config.toml}` is the pair that is readable whatever the mode
/// turns out to be — in read-write mode the state dir *is* `<root>/.tinio`, and in read-only
/// mode the root-side file is the one FR-023 keeps readable — so a hand-written config that
/// says `read_only = true` still selects the relocated state dir on the next run.
///
/// **A confined child reads no path here when `--config` named one** (design decision 18): the
/// payload's text *is* that file's content — the launcher read it and resolved this same mode
/// from it — so the text is the file level and the path is never asked for. Inside the
/// confinement that path may sit outside every exposure, which is why [`config_path`]'s
/// existence check does not apply to an adopted run; asking for it again is what refused a
/// confined `--config`-outside-the-root run before it could serve. Without `--config` the
/// root-side pair above is inside the served root and therefore readable, so that shape is
/// unchanged.
fn mode_config(
    root: &Path,
    root_state_dir: &Path,
    explicit: Option<&Path>,
    adopted: Option<&str>,
) -> Result<Option<Config>, Error> {
    if let Some(text) = adopted
        && explicit.is_some()
    {
        return Ok(Config::parse(text).ok());
    }
    let path = config_path(root, root_state_dir, explicit)?;
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Config::load(&path).ok())
}

/// Folds the flags clap cannot hand a config into the resolved config (design §4): a flag and
/// its config key are honoured by the same code path, so they cannot disagree.
///
/// `args` and `provenance` are the **authoritative** parse's, so every level above the file is
/// already in them — the flag, `TINIO_*`, and `.env`, which reached clap as an environment
/// answer like any other. That is why the `.env` argument [`pick`] still takes is `None` at
/// every call site here: reading the process environment back would be a second implementation
/// of a precedence the parser owns, and the two would drift the first time a variable moved.
///
/// `--address` is the Minio-style spelling of `--host` + `--port` and wins over both when it is
/// given; it carries no `env` attribute (it is a spelling, not a key), so a typed `--address`
/// reports itself as a `CommandLine` answer for the two keys it spells.
fn fold_flags(
    args: &start::Args,
    provenance: &Provenance,
    resolution: &mut Resolution,
) -> Result<(), Error> {
    let address = match &args.address {
        Some(raw) => Some(authority_addr(raw, default_port())?),
        None => None,
    };
    let (host_source, port_source) = if address.is_some() {
        (ValueSource::CommandLine, ValueSource::CommandLine)
    } else {
        (provenance.host, provenance.port)
    };

    let server = &mut resolution.config.server;
    server.host = chain(
        host_source,
        args.host
            .clone()
            .or_else(|| address.map(|addr| addr.ip().to_string())),
        server.host.clone(),
    );
    server.port = chain(
        port_source,
        args.port.or_else(|| address.map(|addr| addr.port())),
        server.port,
    );

    // The effective logging configuration, materialised: the flags, `.env` and the file are one
    // value by the time a caller reads it, so the subscriber Task 3 installs has one place to
    // look. An absent `[log]` section is no level at all rather than the section's defaults —
    // `pick`'s absence rule — so a flag beats it and the enum's own default is the last resort.
    let file_verbosity = resolution.config.log.as_ref().map(|log| log.verbosity);
    let verbosity = pick(provenance.verbosity, args.verbosity, file_verbosity).unwrap_or_default();
    let file_format = resolution
        .config
        .log
        .as_ref()
        .map(|log| log.server_log_format);
    let format = pick(provenance.log_format, args.log_format, file_format).unwrap_or_default();
    let log = resolution
        .config
        .log
        .get_or_insert_with(log::Config::default);
    log.verbosity = verbosity;
    log.server_log_format = format;

    if args.no_follow_symlinks {
        resolution
            .config
            .storage
            .get_or_insert_with(storage::Config::default)
            .fs
            .follow_symlinks = false;
    }
    // The `[auth]` pair's fold is the same one every command runs (`for_doctor` calls it too),
    // and it belongs here rather than in `resume` because the values it reads are the
    // authoritative parse's — `.env`'s included.
    fold_credentials(&args.env, &mut resolution.config)
}

/// The one `Resolution` → `FsOptions` mapping in this CLI, so a resolved root becomes an
/// `FsStorage` by the same code whichever command asks.
///
/// `pipelines` is `Some` for the serving half — the configured `[pipeline.*]` values, built by
/// the caller that owns their lifetime — and `None` for an offline command, which builds no
/// pipelines and whose configured values therefore apply as they stand (the inline runner is
/// `tinio-core`'s reference implementation, the shape its own offline contexts use).
///
/// The state dir is always explicit: it is the resolved one, which is `<root>/.tinio` in
/// read-write mode and the relocated directory in read-only mode (decision 6).
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use tinio_cli::resolve::{Resolution, fs_options};
/// use tinio_config::Config;
///
/// let resolution = Resolution {
///     root: PathBuf::from("/srv/data"),
///     config: Config::default(),
///     config_text: String::new(),
///     config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
///     read_only: false,
///     state_dir: PathBuf::from("/srv/data/.tinio"),
///     state_dir_override: None,
///     config_error: None,
/// };
/// let options = fs_options(&resolution, None);
/// assert_eq!(
///     options.state_dir.as_deref(),
///     Some(resolution.state_dir.as_path())
/// );
/// ```
pub fn fs_options(resolution: &Resolution, pipelines: Option<&Pipelines>) -> FsOptions {
    let fs = resolution.config.storage.clone().unwrap_or_default().fs;
    // The three lanes default to fresh inline runners and are overwritten by the pipelines when
    // the caller has them (the serving half always does). The literal is built first because the
    // fields are `Arc<dyn Runner<..>>`: the unsized coercion needs the target type, which only
    // the struct field supplies — a `match` over the `Option` does not compile.
    let mut options = FsOptions {
        follow_symlinks: fs.follow_symlinks,
        state_dir: Some(resolution.state_dir.clone()),
        access: AccessMode::from(resolution.read_only),
        compact_threshold_percent: fs.compact_threshold_percent,
        meta_batch_size: fs.meta_batch_size,
        meta_batch_bytes: fs.meta_batch_bytes,
        owner_uids: identity::owner_uids_for(&resolution.config),
        io_pipeline: Arc::new(InlineRunner::default()),
        remove_pipeline: Arc::new(InlineRunner::default()),
        db_pipeline: Arc::new(InlineRunner::default()),
    };
    if let Some(pipelines) = pipelines {
        options.io_pipeline = pipelines.io();
        options.remove_pipeline = pipelines.remove();
        options.db_pipeline = pipelines.db();
    }
    options
}

#[cfg(test)]
mod tests;
