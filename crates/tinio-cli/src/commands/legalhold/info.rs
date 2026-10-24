//! `tinio legalhold info` — read a version's legal hold.

use std::{mem, process::ExitCode};

use super::{DirArgs, LegalHoldMessage, OutputArgs, hold_label};
use crate::{
    _api::object_lock::NO_RETENTION,
    _core::object_lock as model,
    Error,
    commands::{
        object_lock::object_address,
        plane::{Connection, Outcome, Plane, Target, decoded, error_message, refusal, serialized},
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
///     commands::legalhold::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "legalhold", "info", "photos/report.txt", "--json"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::LegalHold(args) = cli.command else {
///     panic!("the legalhold arm");
/// };
/// let Verb::Info(info) = args.verb else {
///     panic!("the info verb");
/// };
/// assert_eq!(info.target, "photos/report.txt");
/// assert!(info.output.json);
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The object whose legal hold to print.
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

/// Prints the hold the plane serves for `args.target`.
///
/// A version carrying neither a retention nor a hold answers the plane's `404 {"error": "no
/// retention"}`, which for a legal-hold read is `OFF`: the version exists and holds no hold. The
/// other `404`s — `object lock is not enabled`, `no such version` — stay refusals, because they
/// name a subject this verb cannot report on.
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a target with no key (exit 2), no running server
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

/// The pure half: a target with a key, checked before the server is looked up.
pub(crate) fn plan(args: &Args) -> Result<Target, Error> {
    super::plan(&args.target, "legalhold info")
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute<P: Plane>(args: &Args, plane: &P) -> Result<Outcome, Error> {
    let target = plan(args)?;
    send(&target, args, plane).await
}

async fn send<P: Plane>(target: &Target, args: &Args, plane: &P) -> Result<Outcome, Error> {
    let key = target.require_key("legalhold info", &args.target)?;
    let version = args.version_id.as_deref().unwrap_or_default();
    let address = object_address(key, args.version_id.as_deref());
    let answer = plane
        .object_get(&target.bucket, &serialized(&args.target, &address)?)
        .await?;
    let hold = if answer.status == 200 {
        let view: model::Lock = decoded(&args.target, &answer)?;
        view.legal_hold
    } else if answer.status == 404 && error_message(&answer).as_deref() == Some(NO_RETENTION) {
        // The version exists and carries neither fact: the hold is OFF.
        model::LegalHold::Off
    } else {
        return Err(refusal(&args.target, &answer));
    };
    if args.output.json {
        Outcome::json(&LegalHoldMessage::new(hold, target, version))
    } else {
        Ok(Outcome::done(format!(
            "legal hold for {}: {}",
            target.urlpath(),
            hold_label(hold)
        )))
    }
}
