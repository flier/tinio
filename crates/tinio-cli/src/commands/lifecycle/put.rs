//! `tinio lifecycle put` — replace the bucket's whole configuration from a file.

use std::{path::PathBuf, process::ExitCode};

use http::Method;
use tokio::fs;

use super::{DirArgs, Plane, require_status};
use crate::{_core::lifecycle::Config, Error, resolve::Located};

/// The arguments `put` takes: a bucket, the file, and the root every verb resolves.
///
/// # Examples
///
/// ```rust
/// use std::path::Path;
///
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::lifecycle::Verb,
/// };
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "lifecycle", "put", "photos", "config.json"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Lifecycle(args) = cli.command else {
///     panic!("the lifecycle arm");
/// };
/// let Verb::Put(put) = args.verb else {
///     panic!("the put verb");
/// };
/// assert_eq!(put.bucket, "photos");
/// assert_eq!(put.file, Path::new("config.json"));
/// ```
#[derive(Debug, clap::Args)]
pub struct Args {
    /// The bucket whose configuration to replace.
    #[arg(value_name = "BUCKET")]
    pub bucket: String,
    /// The file carrying the whole configuration, in the plane's JSON — the stored model.
    #[arg(value_name = "FILE")]
    pub file: PathBuf,
    /// The root, the mode and the environment channels, resolved exactly as `status` resolves
    /// them.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Replaces the configuration the plane stores for `args.bucket` with the one in `args.file`.
///
/// **The file is read and parsed before the server is asked**, because a file that is not the
/// model is this command's own input to refuse: `serde_json` names the line and column, which is
/// the reason the parse happens here at all — the plane's answer to a syntax error is one
/// sentence for the whole body. **What is sent is the file's text**, not a re-serialization: a
/// member this build's model does not carry is still the operator's document, and the plane
/// ignores it as it ignores one on any read (LC17). No rule is validated here — the plane is the
/// request path (LC8).
///
/// # Errors
///
/// A file that cannot be read or does not parse (exit 2: it is an argument, like a mistyped
/// `--config`), no running server for the root (exit 1), and the plane's refusal in its own words
/// (exit 1) — a semantic violation as `400 {"rule","reason"}`, which prints the offending rule's
/// ID and the model's reason.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::lifecycle::put::{Args, run},
///     resolve::locate,
/// };
///
/// # async fn replace() -> Result<(), tinio_cli::Error> {
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
///     file: "config.json".into(),
///     dir: Default::default(),
/// };
/// let code = run(args, located).await?;
/// // 0 once the plane stored the file's configuration; a refusal is an error and never reaches here.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
///
/// The example is `no_run` because it needs a running server and a configuration file; a live call
/// replaces the bucket's configuration with the file's text and exits 0.
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    let text = fs::read_to_string(&args.file)
        .await
        .map_err(|err| Error::Usage(format!("{}: {err}", args.file.display())))?;
    if let Err(err) = serde_json::from_str::<Config>(&text) {
        return Err(Error::Usage(format!("{}: {err}", args.file.display())));
    }
    let plane = Plane::connect(args.dir, located, &args.bucket).await?;
    let answer = plane.call(Method::PUT, Some(&text)).await?;
    require_status(&args.bucket, &answer, 204)?;
    Ok(ExitCode::SUCCESS)
}
