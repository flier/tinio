//! `tinio object-lock enable` — enable Object Lock on a bucket through the plane's enable path.

use std::{mem, process::ExitCode};

use super::DirArgs;
use crate::{
    _core::object_lock as model,
    Error,
    commands::{
        plane::{
            Connection, ErrorMessage, Outcome, OutputArgs, Plane, Target, decoded, refusal,
            refusal_message, refuse_object_scope,
        },
        retention::write_config,
    },
    resolve::Located,
};

/// The arguments `enable` takes: a bucket, `--json`, and the root every verb resolves.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::object_lock::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "object-lock", "enable", "photos", "--json"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::ObjectLock(args) = cli.command else {
///     panic!("the object-lock arm");
/// };
/// let Verb::Enable(enable) = args.verb else {
///     panic!("the enable verb");
/// };
/// assert_eq!(enable.bucket, "photos");
/// assert!(enable.output.json);
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The bucket to enable.
    #[arg(value_name = "BUCKET")]
    pub bucket: String,
    /// Emit the bucket-configuration shape as JSON.
    #[command(flatten)]
    pub output: OutputArgs,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Enables `args.bucket` through the plane's `PUT /buckets/{bucket}/object-lock`.
///
/// **The enable path is the plane's** (OL24): the admin enables the bucket's versioning first
/// when it is unversioned and then stores `{enabled: true, rule}`, so this command issues one
/// bucket `PUT` and never a versioning call of its own — the CLI has no store handle. The rule is
/// the bucket's stored one when it has a configuration, so a repeat enable does not clear a default
/// rule the plane's replace-all `PUT` would otherwise drop; a bucket with no configuration sends
/// `rule: null`.
///
/// # Errors
///
/// No running server for the root (exit 1, `stop`'s sentence) and the plane's refusal in its own
/// words (exit 1): `no such bucket`, `access denied` on a read-only run, `invalid bucket state`
/// for a suspended bucket, and `object lock is not served here` when the run carries no
/// capability.
///
/// # Examples
///
/// ```rust
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::object_lock::enable::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn enable() -> Result<(), tinio_cli::Error> {
/// let root = tempfile::TempDir::new().expect("a scratch root");
/// let discovery = Discovery {
///     dir: Some(root.path().to_path_buf()),
///     config: None,
///     read_only: false,
///     state_dir: Some(root.path().join("state")),
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     bucket: "photos".into(),
///     output: Default::default(),
///     dir: Default::default(),
/// };
/// let error = run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(enable()).expect("the refusal");
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

/// The pure half: parse the target and refuse an object scope **before** the server is looked up,
/// so a misuse is exit 2 even on a stopped root.
pub(crate) fn plan(args: &Args) -> Result<Target, Error> {
    let target = Target::parse(&args.bucket)?;
    refuse_object_scope(&target, &args.bucket, "object-lock enable")?;
    Ok(target)
}

/// The request/answer half, over any [`Plane`]: the tests drive it with a recording fake.
#[cfg(test)]
pub(crate) async fn execute<P: Plane>(args: &Args, plane: &P) -> Result<Outcome, Error> {
    let target = plan(args)?;
    send(&target, args, plane).await
}

async fn send<P: Plane>(target: &Target, args: &Args, plane: &P) -> Result<Outcome, Error> {
    // OL24/R30: the plane's PUT is replace-all, so enabling an already
    // configured bucket must not drop its default rule. Read the stored
    // configuration first and carry the rule it holds; a bucket with no
    // configuration (404) is the bare enable.
    let existing = plane.bucket_get(&target.bucket).await?;
    let rule = match existing.status {
        200 => decoded::<model::Config>(&args.bucket, &existing)?.rule,
        // The plane's "no configuration" (and, for a missing bucket, the PUT's
        // own refusal below) both take the bare enable path.
        404 => None,
        _ if args.output.json => {
            return Outcome::json_failed(&ErrorMessage {
                error: refusal_message(&existing),
            });
        }
        _ => return Err(refusal(&args.bucket, &existing)),
    };
    let config = model::Config {
        enabled: true,
        rule,
    };
    write_config(
        &args.bucket,
        &target.bucket,
        &config,
        "enable",
        args.output.json,
        format!("object lock enabled for {}", target.bucket),
        plane,
    )
    .await
}
