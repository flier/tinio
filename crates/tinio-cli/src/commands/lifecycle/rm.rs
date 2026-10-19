//! `tinio lifecycle rm` — delete the bucket's whole configuration.

use std::process::ExitCode;

use http::Method;

use super::{DirArgs, Plane, require_status};
use crate::{Error, resolve::Located};

/// The arguments `rm` takes: a bucket, and the root every verb resolves.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::lifecycle::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "lifecycle", "rm", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Lifecycle(args) = cli.command else {
///     panic!("the lifecycle arm");
/// };
/// let Verb::Rm(rm) = args.verb else {
///     panic!("the rm verb");
/// };
/// assert_eq!(rm.bucket, "photos");
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The bucket whose configuration to delete.
    #[arg(value_name = "BUCKET")]
    pub bucket: String,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Deletes the configuration the plane stores for `args.bucket`.
///
/// The plane's `204` is idempotent — removing one that was never stored is the same answer, since
/// what the caller needs is that nothing is stored now — so this verb prints nothing on success.
///
/// # Errors
///
/// No running server for the root (exit 1, `stop`'s sentence) and the plane's refusal in its own
/// words (exit 1): `no such bucket`, `access denied` on a read-only run, and
/// `lifecycle is not served here` when the run carries no capability (LC15, LC23).
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::lifecycle::rm::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn remove() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     bucket: "photos".into(),
///     dir: Default::default(),
/// };
/// let code = run(args, located).await?;
/// // 0 once nothing is stored; a refusal is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// The example is `no_run` because it needs a running server; a live call removes the bucket's
/// configuration and exits 0.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let plane = Plane::connect(args.dir, located, &args.bucket).await?;
    let answer = plane.call(Method::DELETE, None).await?;
    require_status(&args.bucket, &answer, 204)?;
    Ok(ExitCode::SUCCESS)
}
