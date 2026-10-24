//! `tinio legalhold` — set, read and clear a version's legal hold (`contracts/cli.md`
//! §`tinio legalhold`).
//!
//! **The verbs carry no status positional**: `set` writes `ON` and `clear` writes `OFF`, as `mc`
//! hard-codes them. There is no `--bypass`: a legal hold cannot be bypassed (OL11). The key and
//! version address travel in the plane's JSON body (OL29).

pub mod clear;
pub mod info;
pub mod set;

use std::{mem, process::ExitCode};

use clap::{Args as ClapArgs, Subcommand};
use serde::Serialize;

pub use super::DirArgs;
use super::{
    object_lock::object_update,
    plane::{
        Connection, ErrorMessage, Outcome, OutputArgs, Plane, Target, check_answer, serialized,
    },
};
use crate::{_core::object_lock as model, Error, resolve::Located};

/// The `tinio legalhold` subcommand: the contract's three verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "legalhold", "info", "photos/report.txt"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::LegalHold(_)));
/// ```
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The verb and its own arguments.
    #[command(subcommand)]
    pub verb: Verb,
}

/// The contract's three verbs, each with the arguments its own row lists.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::legalhold::Verb,
/// };
///
/// fn verb(argv: &[&str]) -> Verb {
///     let matches = Cli::command().try_get_matches_from(argv).expect("parse");
///     let cli = Cli::from_arg_matches(&matches).expect("from matches");
///     let Command::LegalHold(args) = cli.command else {
///         panic!("the legalhold arm");
///     };
///     args.verb
/// }
///
/// assert!(matches!(
///     verb(&["tinio", "legalhold", "set", "photos/report.txt"]),
///     Verb::Set(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "legalhold", "info", "photos/report.txt"]),
///     Verb::Info(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "legalhold", "clear", "photos/report.txt"]),
///     Verb::Clear(_)
/// ));
/// ```
#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Set the legal hold ON.
    Set(HoldArgs),
    /// Print the legal hold.
    Info(info::Args),
    /// Clear the legal hold to OFF.
    Clear(HoldArgs),
}

/// The arguments `legalhold set` and `legalhold clear` take: the target, an optional version,
/// `--json`, and the root. One definition, because the two verbs differ only in the hold they
/// write.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::legalhold::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "legalhold", "set", "photos/report.txt"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::LegalHold(args) = cli.command else {
///     panic!("the legalhold arm");
/// };
/// let Verb::Set(set) = args.verb else {
///     panic!("the set verb");
/// };
/// assert_eq!(set.target, "photos/report.txt");
/// assert!(set.version_id.is_none());
/// ```
#[derive(Debug, clap::Args)]
pub struct HoldArgs {
    /// The object whose legal hold to set or clear.
    #[arg(value_name = "BUCKET/KEY")]
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

impl Args {
    /// The root, the mode and the environment channels every verb flattens.
    pub(crate) fn dir_args(&self) -> &DirArgs {
        match &self.verb {
            Verb::Set(args) => &args.dir,
            Verb::Info(args) => &args.dir,
            Verb::Clear(args) => &args.dir,
        }
    }
}

/// Runs the verb the command line named.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), the refusal a root with nothing running answers
/// (exit 1), and the plane's refusal in its own words (exit 1).
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command, Discovery, Provenance},
///     commands::legalhold::{self, Verb},
///     resolve::locate,
/// };
///
/// # async fn show() -> Result<(), tinio_cli::Error> {
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
///     .try_get_matches_from(["tinio", "legalhold", "info", "photos/report.txt"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::LegalHold(args) = cli.command else {
///     panic!("the legalhold arm");
/// };
/// assert!(matches!(args.verb, Verb::Info(_)));
/// let error = legalhold::run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(show()).expect("the refusal");
/// ```
///
/// The example runs against an empty scratch root: the call reaches the no-server refusal and
/// asserts exit 1, so it needs no live server.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    match args.verb {
        Verb::Set(args) => set::run(args, located).await,
        Verb::Info(args) => info::run(args, located).await,
        Verb::Clear(args) => clear::run(args, located).await,
    }
}

/// Runs `set` or `clear`: one implementation, with the hold and the verb name the two differ by.
pub(crate) async fn run_hold(
    mut args: HoldArgs,
    located: Located,
    hold: model::LegalHold,
    verb: &'static str,
) -> Result<ExitCode, Error> {
    let target = plan(&args.target, verb)?;
    let dir = mem::take(&mut args.dir);
    let plane = Connection::connect(dir, located).await?;
    send(&target, &args, &plane, hold, verb)
        .await
        .map(Outcome::finish)
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute_hold<P: Plane>(
    args: &HoldArgs,
    plane: &P,
    hold: model::LegalHold,
    verb: &str,
) -> Result<Outcome, Error> {
    let target = plan(&args.target, verb)?;
    send(&target, args, plane, hold, verb).await
}

/// The legal-hold `--json` shape (`mc`'s `{legalhold, urlpath, key, versionID}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LegalHoldMessage {
    pub(crate) legalhold: String,
    pub(crate) urlpath: String,
    pub(crate) key: String,
    #[serde(rename = "versionID")]
    pub(crate) version_id: String,
}

impl LegalHoldMessage {
    /// The shape of the hold on one addressed version.
    pub(crate) fn new(hold: model::LegalHold, target: &Target, version_id: &str) -> Self {
        Self {
            legalhold: hold_label(hold),
            urlpath: target.urlpath(),
            key: target.key.clone().unwrap_or_default(),
            version_id: version_id.to_owned(),
        }
    }
}

/// The legal hold's `ON`/`OFF` spelling.
pub(crate) fn hold_label(hold: model::LegalHold) -> String {
    hold.to_string()
}

/// The pure half every legal-hold verb shares: a target with a key, checked **before** the
/// server is looked up, so a bucket-only target is exit 2 even on a stopped root.
pub(crate) fn plan(subject: &str, verb: &str) -> Result<Target, Error> {
    let target = Target::parse(subject)?;
    target.require_key(verb, subject)?;
    Ok(target)
}

/// Write one legal-hold state and answer the verb's output — the shared body of `set` and
/// `clear`, so the two cannot send different request shapes. `target` is the plan's own, so the
/// key cannot be absent here.
async fn send<P: Plane>(
    target: &Target,
    args: &HoldArgs,
    plane: &P,
    hold: model::LegalHold,
    verb: &str,
) -> Result<Outcome, Error> {
    let key = target.require_key(verb, &args.target)?;
    let update = object_update(key, args.version_id.as_deref(), None, Some(hold), false);
    let answer = plane
        .object_put(&target.bucket, &serialized(&args.target, &update)?)
        .await?;
    if let Some(outcome) = check_answer(&args.target, &answer, 204, args.output.json, |error| {
        ErrorMessage { error }
    })? {
        return Ok(outcome);
    }
    if args.output.json {
        Outcome::json(&LegalHoldMessage::new(
            hold,
            target,
            args.version_id.as_deref().unwrap_or_default(),
        ))
    } else {
        Ok(Outcome::done(format!(
            "legal hold for {}: {}",
            target.urlpath(),
            hold_label(hold)
        )))
    }
}
