//! `tinio lifecycle ls` — the bucket's stored configuration, as the plane keeps it.

use std::process::ExitCode;

use http::Method;

use super::{DirArgs, Plane, require_status};
use crate::{Error, resolve::Located};

/// The arguments `ls` takes: a bucket, and the root every verb resolves.
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
///     .try_get_matches_from(["tinio", "lifecycle", "ls", "photos"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Lifecycle(args) = cli.command else {
///     panic!("the lifecycle arm");
/// };
/// let Verb::Ls(ls) = args.verb else {
///     panic!("the ls verb");
/// };
/// assert_eq!(ls.bucket, "photos");
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The bucket whose configuration to print.
    #[arg(value_name = "BUCKET")]
    pub bucket: String,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them — the server this verb asks is the one that serves this root.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Prints what the plane serves for `args.bucket`.
///
/// **The body is printed verbatim**, not re-serialized: the model tolerates a member a later
/// binary adds (LC17), and a round trip through this build's `Config` would drop it.
///
/// # Errors
///
/// No running server for the root (exit 1, `stop`'s sentence) and the plane's refusal in its own
/// words (exit 1): `no lifecycle configuration` when the bucket stores none, `no such bucket`, and
/// `lifecycle is not served here` when the run carries no capability (LC15, LC23).
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::lifecycle::ls::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn print() -> Result<(), tinio_cli::Error> {
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
/// // 0 once the plane's body has printed; a refusal is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// The example is `no_run` because it needs a running server; a live call prints the stored
/// configuration and exits 0.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let plane = Plane::connect(args.dir, located, &args.bucket).await?;
    let answer = plane.call(Method::GET, None).await?;
    require_status(&args.bucket, &answer, 200)?;
    println!("{}", answer.body);
    Ok(ExitCode::SUCCESS)
}
