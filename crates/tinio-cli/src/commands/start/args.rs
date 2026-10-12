//! `tinio server` / `tinio start` flags.

#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;

use clap::{ArgAction, Args as ClapArgs};

use crate::{
    _config::log::{Format, Verbosity},
    commands::{EnvArgs, parse_switch},
};

/// `tinio server` / `tinio start`: every flag `contracts/cli.md:28-31` lists, plus `--config`
/// (decision 5) and `--sandbox` (decision 10).
///
/// **`--anonymous` is deliberately not here.** The flag, its `TINIO_ANONYMOUS` wiring and
/// anonymous semantics are US3's T087 and nothing inert ships: a flag that parses and does
/// nothing is worse than a flag that is not there yet. The same rule keeps `TINIO_API_UNIX` /
/// `TINIO_API_HTTP` / `TINIO_API_HTTPS` and `TINIO_ACCESS_LOG` unwired — a contract-side
/// decision this phase records rather than assumes.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "start", "/srv/data", "--port", "0"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Server(args) = cli.command else {
///     panic!("the alias parses to the server arm");
/// };
/// assert_eq!(args.port, Some(0));
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct Args {
    /// The storage root; absent: walk up from the current directory to the nearest ancestor
    /// containing `.tinio/`.
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Minio-style alias for `--host` + `--port` (`[HOST:]PORT`; the host defaults to
    /// `127.0.0.1`).
    #[arg(long, value_name = "HOST:PORT")]
    pub address: Option<String>,
    /// Bind host (config `[server] host`).
    #[arg(long, env = "TINIO_HOST", value_name = "HOST")]
    pub host: Option<String>,
    /// Bind port (config `[server] port`; `0` = an OS-assigned ephemeral port).
    #[arg(long, env = "TINIO_PORT", value_name = "N")]
    pub port: Option<u16>,
    /// Read-only mode: reject every S3 write, never write the storage root, and relocate the
    /// state to the home state dir (config `[server] read_only`, env `TINIO_READ_ONLY`).
    #[arg(
        long,
        env = "TINIO_READ_ONLY",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true,
        action = ArgAction::Set,
        default_value_t = false,
    )]
    pub read_only: bool,
    /// Confine the server process (env `TINIO_SANDBOX`, config `[server] sandbox`).
    ///
    /// **Which declaration exists is the feature's** (decision 10, and the refusal is the point
    /// rather than the flag): with the feature the operator's channel is `--sandbox`; without
    /// it the flag is **not in the surface at all** — an unknown flag is clap's own usage error
    /// — while `TINIO_SANDBOX` cannot be compiled away and would otherwise be silently ignored,
    /// so the variable keeps its argument under a hidden name and the run still fails closed
    /// with the refusal that names the build.
    #[cfg(feature = "sandbox")]
    #[arg(
        long,
        env = "TINIO_SANDBOX",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true,
        action = ArgAction::Set,
        default_value_t = false,
    )]
    pub sandbox: bool,
    /// The feature-off spelling of the switch above: hidden, and never documented — the
    /// variable is its only channel there.
    #[cfg(not(feature = "sandbox"))]
    #[arg(
        long = "sandbox-env",
        env = "TINIO_SANDBOX",
        value_parser = parse_switch,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true,
        action = ArgAction::Set,
        default_value_t = false,
        hide = true,
    )]
    pub sandbox: bool,
    /// Detach and run in the background, waiting for the server to be ready.
    ///
    /// **`api`-only.** The parent's proof of readiness is the `state` file plus an answer on the
    /// control channel it names, and both belong to the management plane — so a build without
    /// the feature has nothing to prove a daemon ready with, and the flag is not in its surface
    /// rather than accepted-and-refused. Unlike `--sandbox` there is no variable or config key
    /// for it, so compiling it away cannot silently ignore a channel.
    #[cfg(feature = "api")]
    #[arg(long)]
    pub daemon: bool,
    /// Log level: `error`, `warn`, `info` or `debug` (config `[log] verbosity`).
    #[arg(long, env = "TINIO_LOG_LEVEL", value_parser = parse_verbosity, value_name = "LEVEL")]
    pub verbosity: Option<Verbosity>,
    /// Operational log file for a **detached** run (`--daemon` redirects its stderr there; config
    /// `[log] server_log_file`; absent, the format-derived `server.log`/`server.json` in the state
    /// dir). A foreground run logs to stderr and this flag has no sink there.
    #[arg(long, value_name = "PATH")]
    pub log_file: Option<PathBuf>,
    /// Operational log format: `text` or `json` (config `[log] server_log_format`).
    #[arg(long, env = "TINIO_LOG_FORMAT", value_parser = parse_format, value_name = "FORMAT")]
    pub log_format: Option<Format>,
    /// Configuration file; absent: discovery under the root, then under the state dir.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,
    /// Reject symlinks in the storage root (config `[storage.fs] follow_symlinks`).
    #[arg(long)]
    pub no_follow_symlinks: bool,
    /// The environment-only channels: the state-dir override and the credential pair.
    #[command(flatten)]
    pub env: EnvArgs,
    /// The `[api]` transport flags (`api` builds only).
    #[cfg(feature = "api")]
    #[command(flatten)]
    pub api: ApiFlags,
}

/// The management-transport flags `contracts/cli.md:30-31` lists.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from([
///         "tinio",
///         "server",
///         "/srv/data",
///         "--api",
///         "http://127.0.0.1:9001",
///     ])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Server(args) = cli.command else {
///     panic!("the server arm");
/// };
/// assert_eq!(args.api.urls, ["http://127.0.0.1:9001"]);
/// ```
#[cfg(feature = "api")]
#[derive(Debug, Default, ClapArgs)]
pub struct ApiFlags {
    /// A management transport, repeatable; the URL scheme selects it (`unix://PATH`,
    /// `pipe://NAME`, `http://HOST:PORT`, `https://HOST:PORT`); a scheme-less `HOST:PORT` is
    /// HTTP. Any `--api` flag disables every transport the flags do not name.
    #[arg(long = "api", value_name = "URL")]
    pub urls: Vec<String>,
    /// Disable the local management channel.
    #[arg(long)]
    pub no_api_unix: bool,
    /// PEM certificate for an `https://` management transport.
    #[arg(long, value_name = "PATH")]
    pub api_cert: Option<PathBuf>,
    /// PEM private key for an `https://` management transport.
    #[arg(long, value_name = "PATH")]
    pub api_key: Option<PathBuf>,
}

impl Args {
    /// The confinement switch's answer: `--sandbox` or `TINIO_SANDBOX` where the flag is in the
    /// surface, and the variable alone in a build without the feature.
    ///
    /// One accessor rather than a `#[cfg]` at every call site, so `sandbox::plan` reads the same
    /// expression in both builds (design decision 10) — and its answer is the *value* only:
    /// which of the two channels produced it is `Provenance::sandbox`'s, and that is what a
    /// refusal names.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::path::PathBuf;
    ///
    /// use tinio_cli::commands::start::Args;
    ///
    /// let args = Args {
    ///     dir: Some(PathBuf::from("/srv/data")),
    ///     ..Args::default()
    /// };
    /// // Nothing asked for the confinement here.
    /// assert!(!args.sandbox_flag());
    /// ```
    pub fn sandbox_flag(&self) -> bool {
        self.sandbox
    }

    /// An `Args` naming `root` and nothing else — the shape `resolve.rs`'s unit tests and
    /// plan C's `test_support` need, so a leg drives the real resolution rather than a
    /// hand-built `Resolution`.
    #[cfg(test)]
    pub fn for_test(root: &Path) -> Self {
        Self {
            dir: Some(root.to_path_buf()),
            ..Self::default()
        }
    }
}

/// `--verbosity`'s parser: the config's own four spellings, refused with the flag's name.
fn parse_verbosity(raw: &str) -> Result<Verbosity, String> {
    raw.parse()
        .map_err(|_| format!("`{raw}` is not a log level: `error`, `warn`, `info` or `debug`"))
}

/// `--log-format`'s parser: `text` or `json`, refused with the flag's name.
fn parse_format(raw: &str) -> Result<Format, String> {
    raw.parse()
        .map_err(|_| format!("`{raw}` is not a log format: `text` or `json`"))
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "api")]
    use std::path::Path;
    use std::{ffi::OsString, iter::once, path::PathBuf};

    use clap::{CommandFactory, FromArgMatches, parser::ValueSource};

    use crate::{
        _config::{log::Format, state::STATE_DIR_VAR},
        _util::testing::assert_send_sync,
        Error,
        cli::{
            ACCESS_KEY_VAR, Cli, Command, HOST_VAR, LOG_FORMAT_VAR, LOG_LEVEL_VAR, PG_DSN_VAR,
            PORT_VAR, READ_ONLY_VAR, SANDBOX_VAR, SECRET_KEY_VAR, parse, provenance,
        },
    };

    /// The `server` subcommand's arguments, as clap built them.
    fn server_arguments() -> clap::Command {
        Cli::command()
            .find_subcommand("server")
            .expect("the server subcommand")
            .clone()
    }

    /// The one argument `--<flag>` names.
    fn argument(flag: &str) -> clap::Arg {
        server_arguments()
            .get_arguments()
            .find(|arg| arg.get_long() == Some(flag))
            .unwrap_or_else(|| panic!("`--{flag}` is in the surface"))
            .clone()
    }

    /// The confinement switch's long name: the feature's, because the feature-off declaration
    /// keeps the variable under a hidden name (`Args::sandbox`'s two arms are the rule).
    fn sandbox_flag() -> &'static str {
        if cfg!(feature = "sandbox") {
            "sandbox"
        } else {
            "sandbox-env"
        }
    }

    #[test]
    fn every_env_backed_flag_carries_the_name_this_module_declares() {
        // One owner per variable, tested rather than asserted in prose: the `env` attribute's
        // literal and the constant the resolution (or a refusal) names are the same string.
        for (flag, var) in [
            ("host", HOST_VAR),
            ("port", PORT_VAR),
            ("verbosity", LOG_LEVEL_VAR),
            ("log-format", LOG_FORMAT_VAR),
            ("read-only", READ_ONLY_VAR),
            (sandbox_flag(), SANDBOX_VAR),
            ("state-dir", STATE_DIR_VAR),
            ("access-key", ACCESS_KEY_VAR),
            ("secret-key", SECRET_KEY_VAR),
            ("pg-dsn", PG_DSN_VAR),
        ] {
            assert_eq!(
                argument(flag).get_env().map(|name| name.to_string_lossy()),
                Some(var.into()),
                "`--{flag}`'s env attribute is `{var}`",
            );
        }
    }

    #[test]
    fn the_log_file_flag_has_no_variable_of_its_own() {
        // `contracts/config.md:110`'s list has no server-log-file variable, and
        // `TINIO_ACCESS_LOG` names the *access* log, which `--log-file` is not. The same rule
        // keeps `--address` and `--config` flag-only.
        //
        // `--daemon` is in this list only in an `api` build: the flag is `api`-only (its own
        // doc says why), so a `--no-default-features` run has no argument to ask about.
        #[cfg(feature = "api")]
        let flags = ["log-file", "config", "address", "daemon"];
        #[cfg(not(feature = "api"))]
        let flags = ["log-file", "config", "address"];
        for flag in flags {
            assert_eq!(argument(flag).get_env(), None, "`--{flag}` has no variable");
        }
    }

    #[test]
    fn a_switch_is_typed_bare_valued_or_not_at_all() {
        // The three command-line spellings the attributes exist for: the bare flag (the
        // `default_missing_value`), an explicit value through the tolerant parser, and nothing
        // — which is what leaves the config file's key to answer.
        let read_only = |argv: [&str; 4]| {
            let matches = Cli::command().try_get_matches_from(argv).expect("parse");
            let cli = Cli::from_arg_matches(&matches).expect("from matches");
            let Command::Server(args) = cli.command else {
                panic!("the server arm");
            };
            (args.read_only, provenance(&matches).read_only)
        };
        let (flag, source) = read_only(["tinio", "server", "/srv/data", "--read-only"]);
        assert!(flag, "the bare flag means `true`");
        assert_eq!(source, ValueSource::CommandLine);

        let (flag, source) = read_only(["tinio", "server", "/srv/data", "--read-only=false"]);
        assert!(
            !flag,
            "an explicit `0`/`false` is an answer, not an absence"
        );
        assert_eq!(source, ValueSource::CommandLine);

        // A different flag beside it changes nothing about `read_only`'s source. `--daemon` is
        // the one this crate has that is `api`-only (its own doc), so the `--no-default-features`
        // graph gets the same assertion through a flag every build carries.
        #[cfg(feature = "api")]
        let argv = ["tinio", "server", "/srv/data", "--daemon"];
        #[cfg(not(feature = "api"))]
        let argv = ["tinio", "server", "/srv/data", "--no-follow-symlinks"];
        let (flag, source) = read_only(argv);
        assert!(!flag);
        assert_eq!(
            source,
            ValueSource::DefaultValue,
            "nothing answered, so the config file is what the chain reads"
        );
    }

    /// `api` only: without the feature neither the flags nor the empty-`[api]` rule exist
    /// (`Command`'s doc: the whole channel is compiled out, so the build cannot promise a
    /// transport it does not have).
    #[cfg(feature = "api")]
    #[test]
    fn the_api_flags_are_repeatable_and_the_flags_are_shared() {
        let matches = Cli::command()
            .try_get_matches_from([
                "tinio",
                "server",
                "/srv/data",
                "--api",
                "http://127.0.0.1:9001",
                "--api",
                "https://127.0.0.1:9443",
                "--api-cert",
                "cert.pem",
            ])
            .expect("parse");
        let cli = Cli::from_arg_matches(&matches).expect("from matches");
        let Command::Server(args) = cli.command else {
            panic!("the server arm");
        };
        assert_eq!(args.api.urls.len(), 2);
        assert_eq!(args.api.api_cert.as_deref(), Some(Path::new("cert.pem")));
    }

    #[test]
    fn a_typed_log_level_is_the_configs_own_enum() {
        let matches = Cli::command()
            .try_get_matches_from(["tinio", "server", "/srv/data", "--log-format", "json"])
            .expect("parse");
        let cli = Cli::from_arg_matches(&matches).expect("from matches");
        let Command::Server(args) = cli.command else {
            panic!("the server arm");
        };
        assert_eq!(args.log_format, Some(Format::Json));

        let err = Cli::command()
            .try_get_matches_from(["tinio", "server", "/srv/data", "--log-format", "loud"])
            .expect_err("not a format");
        assert!(err.to_string().contains("text"), "{err}");
    }

    #[test]
    fn a_switch_takes_a_bare_form_or_an_equals_form_and_nothing_between() {
        // `require_equals` is what stops `tinio server --read-only /srv/data` from reading the
        // path as the switch's value — `num_args(0..=1)` alone would swallow it — so this leg
        // pins the three spellings an operator can type. A bare `--read-only` that eats the
        // positional is a silent wrong-root bug, which is why the refusal is asserted rather
        // than left to the attribute.
        fn argv(args: &[&str]) -> Vec<OsString> {
            once("tinio")
                .chain(args.iter().copied())
                .map(OsString::from)
                .collect()
        }

        // Bare, with the root after it: the switch is true and the positional is the root.
        let (cli, _) = parse(&argv(&["server", "--read-only", "/srv/data"])).expect("bare");
        let Command::Server(args) = cli.command else {
            panic!("the server arm");
        };
        assert!(args.read_only, "a bare switch means true");
        assert_eq!(
            args.dir,
            Some(PathBuf::from("/srv/data")),
            "the root, not the value"
        );

        // `=`, both spellings of the value the parser accepts.
        let (cli, _) = parse(&argv(&["server", "--read-only=false", "/srv/data"])).expect("equals");
        let Command::Server(args) = cli.command else {
            panic!("the server arm");
        };
        assert!(!args.read_only, "`=false` is the off spelling");

        let (cli, _) = parse(&argv(&["server", "--read-only=1", "/srv/data"])).expect("equals");
        let Command::Server(args) = cli.command else {
            panic!("the server arm");
        };
        assert!(
            args.read_only,
            "`=1` is the tolerant spelling the variable also accepts"
        );

        // Space-separated: refused, which is the property that keeps the path uneatable.
        let err = parse(&argv(&["server", "--read-only", "false", "/srv/data"]))
            .expect_err("a space-separated value is not the switch's");
        assert!(matches!(err, Error::Usage(_)), "{err}");
    }

    #[test]
    fn the_public_surface_is_send_and_sync() {
        assert_send_sync::<super::Args>();
    }
}
