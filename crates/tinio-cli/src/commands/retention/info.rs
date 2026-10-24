//! `tinio retention info` — read a version's retention, or the bucket's configuration when the
//! target names no key.

use std::{mem, process::ExitCode};

use super::{DirArgs, OutputArgs, RetentionMessage, format_instant};
use crate::{
    _api::object_lock::NO_CONFIGURATION,
    _core::object_lock as model,
    Error,
    commands::{
        object_lock::object_address,
        plane::{
            ConfigMessage, Connection, Outcome, Plane, Target, decoded, error_message, not_enabled,
            refusal, require_status, serialized, validity_label,
        },
    },
    resolve::Located,
};

/// The arguments `info` takes: the target, an optional version, and the root.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::retention::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from([
///         "tinio",
///         "retention",
///         "info",
///         "photos/report.txt",
///         "--version-id",
///         "abc",
///     ])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// let Verb::Info(info) = args.verb else {
///     panic!("the info verb");
/// };
/// assert_eq!(info.target, "photos/report.txt");
/// assert_eq!(info.version_id.as_deref(), Some("abc"));
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The object (`BUCKET/KEY`), or the bucket whose configuration to print.
    #[arg(value_name = "BUCKET[/KEY]")]
    pub target: String,
    /// Address one version; absent, the current version is addressed.
    #[arg(long, value_name = "ID")]
    pub version_id: Option<String>,
    /// Emit the subcommand's `mc` shape as JSON.
    #[command(flatten)]
    pub output: OutputArgs,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Prints the retention the plane serves for `args.target`.
///
/// **`info <BUCKET>` without a key reads the bucket configuration** (`contracts/cli.md` §`tinio
/// retention`); with a key it reads that version's facts. A version carrying no retention — an
/// expired one, or a legal hold alone — answers the `status:"error"` shape under `--json` and an
/// operational error otherwise.
///
/// **A bucket that is not enabled is a successful read of "disabled"**, not a refusal: the plane's
/// `404 {"error":"no object lock configuration"}` renders the same `ConfigMessage::disabled` shape
/// `clear --default` uses (`enabled:false`) and exits 0, because "this bucket has no Object Lock
/// configuration" is the question `info <bucket>` asks. Every other refusal stays an error.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a `--version-id` with no key (exit 2), no running
/// server (exit 1), and the plane's refusal in its own words (exit 1) — `object lock is not
/// enabled` (the keyed read), `no such version`, `no such bucket`.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command, Discovery, Provenance},
///     commands::retention::{self, Verb},
///     resolve::locate,
/// };
///
/// # async fn info() -> Result<(), tinio_cli::Error> {
/// let root = tempfile::TempDir::new().expect("a scratch root");
/// let discovery = Discovery {
///     dir: Some(root.path().to_path_buf()),
///     config: None,
///     read_only: false,
///     state_dir: Some(root.path().join("state")),
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "retention", "info", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// assert!(matches!(args.verb, Verb::Info(_)));
/// let error = retention::run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(info()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server.
pub async fn run(mut args: Args, located: Located) -> Result<ExitCode, Error> {
    let target = plan(&args)?;
    let dir = mem::take(&mut args.dir);
    let plane = Connection::connect(dir, located).await?;
    send(&target, &args, &plane).await.map(Outcome::finish)
}

/// The pure half: parse the target and refuse a meaningless `--version-id` **before** the server
/// is looked up, so a misuse is exit 2 even on a stopped root.
pub(crate) fn plan(args: &Args) -> Result<Target, Error> {
    let target = Target::parse(&args.target)?;
    if target.key.is_none() && args.version_id.is_some() {
        return Err(Error::Usage(format!(
            "retention info {} takes no --version-id without a key",
            args.target
        )));
    }
    Ok(target)
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute<P: Plane>(args: &Args, plane: &P) -> Result<Outcome, Error> {
    let target = plan(args)?;
    send(&target, args, plane).await
}

async fn send<P: Plane>(target: &Target, args: &Args, plane: &P) -> Result<Outcome, Error> {
    let version = args.version_id.as_deref().unwrap_or_default();
    match &target.key {
        None => {
            let answer = plane.bucket_get(&target.bucket).await?;
            if answer.status == 404 && error_message(&answer).as_deref() == Some(NO_CONFIGURATION) {
                return not_enabled(args.output.json, "info", &target.bucket);
            }
            require_status(&args.target, &answer, 200)?;
            let config: model::Config = decoded(&args.target, &answer)?;
            if args.output.json {
                Outcome::json(&ConfigMessage::from_body("info", &config))
            } else {
                let line = match &config.rule {
                    Some(rule) => format!(
                        "object lock is enabled for {}: default retention {} {}",
                        target.bucket,
                        rule.default_retention.mode,
                        validity_label(rule.default_retention.period)
                    ),
                    None => format!(
                        "object lock is enabled for {}: no default retention",
                        target.bucket
                    ),
                };
                Ok(Outcome::done(line))
            }
        }
        Some(key) => {
            let address = object_address(key, args.version_id.as_deref());
            let answer = plane
                .object_get(&target.bucket, &serialized(&args.target, &address)?)
                .await?;
            let view: model::Lock = if answer.status == 200 {
                decoded(&args.target, &answer)?
            } else {
                let message =
                    error_message(&answer).unwrap_or_else(|| answer.body.trim().to_string());
                if args.output.json {
                    return Outcome::json_failed(&RetentionMessage::failure(
                        &message, target, version,
                    ));
                }
                return Err(refusal(&args.target, &answer));
            };
            match view.retention {
                Some(retention) => {
                    let until = format_instant(retention.retain_until)?;
                    if args.output.json {
                        Outcome::json(&RetentionMessage::success(
                            retention.mode,
                            &until,
                            target,
                            version,
                        ))
                    } else {
                        Ok(Outcome::done(format!(
                            "retention for {}: {} until {}",
                            target.urlpath(),
                            retention.mode,
                            until
                        )))
                    }
                }
                None => {
                    if args.output.json {
                        Outcome::json_failed(&RetentionMessage::failure(
                            "no retention",
                            target,
                            version,
                        ))
                    } else {
                        Err(Error::Operational(format!(
                            "object lock {}: no retention",
                            target.urlpath()
                        )))
                    }
                }
            }
        }
    }
}
