//! `tinio retention clear` — clear a version's retention or the bucket's default rule.

use std::{mem, process::ExitCode};

use super::{DirArgs, OutputArgs, RetentionMessage, clear_update};
use crate::{
    _api::object_lock::NO_CONFIGURATION,
    _core::object_lock as model,
    Error,
    commands::plane::{
        ConfigMessage, Connection, ErrorMessage, Outcome, Plane, Target, check_answer, decoded,
        error_message, not_enabled, refuse_object_scope, serialized,
    },
    resolve::Located,
};

/// The arguments `clear` takes: the target, an optional version, `--default`, and the root.
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
///     .try_get_matches_from(["tinio", "retention", "clear", "photos", "--default"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// let Verb::Clear(clear) = args.verb else {
///     panic!("the clear verb");
/// };
/// assert_eq!(clear.target, "photos");
/// assert!(clear.default);
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The object (`BUCKET/KEY`) or, with `--default`, the bucket.
    #[arg(value_name = "BUCKET[/KEY]")]
    pub target: String,
    /// Address one version; absent, the current version is addressed.
    #[arg(long, value_name = "ID")]
    pub version_id: Option<String>,
    /// Clear only the bucket's default-retention rule; the bucket stays enabled.
    #[arg(long)]
    pub default: bool,
    /// Emit the subcommand's `mc` shape as JSON.
    #[command(flatten)]
    pub output: OutputArgs,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Clears the retention the flags name.
///
/// **`clear --default` never enables a bucket** (OL24): it reads the stored configuration first,
/// writes only when one exists, and on a bucket that is not enabled reports `not enabled` and
/// writes nothing. The keyed form carries the implicit governance bypass the plane token
/// authorizes (`mc` always passes it on clear).
///
/// # Errors
///
/// A root that cannot be resolved (exit 2), a `--default` with a key or a version (exit 2), no
/// running server (exit 1), and the plane's refusal in its own words (exit 1) — `no such bucket`,
/// `access denied` on a read-only run, or a protection the retention refuses.
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
/// # async fn clear() -> Result<(), tinio_cli::Error> {
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
///     .try_get_matches_from(["tinio", "retention", "clear", "photos", "--default"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Retention(args) = cli.command else {
///     panic!("the retention arm");
/// };
/// assert!(matches!(args.verb, Verb::Clear(_)));
/// let error = retention::run(args, located)
///     .await
///     .expect_err("a root with nothing running is the documented refusal");
/// assert_eq!(error.exit_code(), 1, "{error}");
/// # Ok(())
/// # }
/// # tokio::runtime::Runtime::new().expect("runtime").block_on(clear()).expect("the refusal");
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

/// The pure half: parse the target and enforce the contract's scopes **before** the server is
/// looked up, so a misuse is exit 2 even on a stopped root.
pub(crate) fn plan(args: &Args) -> Result<Target, Error> {
    let target = Target::parse(&args.target)?;
    if args.default {
        refuse_object_scope(&target, &args.target, "retention clear --default")?;
        if args.version_id.is_some() {
            return Err(Error::Usage(
                "retention clear --default takes no --version-id".to_owned(),
            ));
        }
    } else {
        target.require_key("retention clear", &args.target)?;
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
    if !args.default {
        let key = target.require_key("retention clear", &args.target)?;
        let update = clear_update(key, args.version_id.as_deref());
        let answer = plane
            .object_put(&target.bucket, &serialized(&args.target, &update)?)
            .await?;
        let version = args.version_id.as_deref().unwrap_or_default();
        if let Some(outcome) =
            check_answer(&args.target, &answer, 204, args.output.json, |error| {
                RetentionMessage::failure(&error, target, version)
            })?
        {
            return Ok(outcome);
        }
        if args.output.json {
            Outcome::json(&RetentionMessage {
                mode: String::new(),
                until: String::new(),
                urlpath: target.urlpath(),
                version_id: version.to_owned(),
                status: "success".to_owned(),
                error: String::new(),
            })
        } else {
            Ok(Outcome::done(format!(
                "retention cleared for {}",
                target.urlpath()
            )))
        }
    } else {
        let answer = plane.bucket_get(&target.bucket).await?;
        if answer.status == 404 && error_message(&answer).as_deref() == Some(NO_CONFIGURATION) {
            // The contract's no-op: a bucket that is not enabled has no rule to clear, and this
            // verb must not enable it on the way past (OL24).
            return not_enabled(args.output.json, "clear", &target.bucket);
        }
        if let Some(outcome) =
            check_answer(&args.target, &answer, 200, args.output.json, |error| {
                ErrorMessage { error }
            })?
        {
            return Ok(outcome);
        }
        let stored: model::Config = decoded(&args.target, &answer)?;
        let cleared = model::Config {
            enabled: stored.enabled,
            rule: None,
        };
        let answer = plane
            .bucket_put(&target.bucket, &serialized(&args.target, &cleared)?)
            .await?;
        if let Some(outcome) =
            check_answer(&args.target, &answer, 204, args.output.json, |error| {
                ErrorMessage { error }
            })?
        {
            return Ok(outcome);
        }
        if args.output.json {
            Outcome::json(&ConfigMessage::from_body("clear", &cleared))
        } else {
            Ok(Outcome::done(format!(
                "default retention cleared for {}",
                target.bucket
            )))
        }
    }
}
