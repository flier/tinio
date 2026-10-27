//! `tinio tier` — the offline commands over a tier root (T12, T17, T18, T28).
//!
//! **They never run beside a server.** Each command opens the root itself — the
//! placement index at `<root>/.tinio/tier.redb`, the warm filesystem plane and
//! the cold database — so the operator stops the server first. The read-only
//! command (`verify`) opens all three planes read-only and takes no ownership
//! lock; the two writers take the database's ownership lock for their run, which
//! is why a second writer is refused rather than interleaved.
//!
//! **What each command may write is its own contract**: `verify` writes nothing,
//! `repair` writes placements and — only under `--delete-orphans` — removes cold
//! copies, and `prepare-downgrade` promotes every cold object and then removes
//! the index and the marker. Nothing else is ever removed.
//!
//! **A span carries counts and flags, never an object's identity.** No span
//! field holds a key, a bucket, a tag set, an ACL or a [`ColdInfo`], because a
//! trace is exported to systems that are not the storage root's audience. A
//! finding does name the object — the operator needs it — but the spans do not.
//!
//! [`ColdInfo`]: crate::_tier::ColdInfo

pub mod downgrade;
pub mod repair;
pub mod verify;

use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};

pub use super::DirArgs;
use crate::{
    _core::{
        AccessMode, ETag, bucket,
        content::{ContentKey, ObjectContent, hash_body},
        meta::BucketMeta as _,
        object,
        storage::{self, BucketOps, ListBucketsParams},
    },
    _fs::FsOptions,
    _tier::{_pg, ColdStore, TierStorage, is_tier_root, pg},
    Error,
    config::storage::{Config as StorageConfig, Tier as TierSection, pg::DEFAULT_POOL_SIZE},
    report::Report,
    resolve::{self, Located, Resolution},
};

/// The page size every scan in this tree reads.
pub(crate) const PAGE: usize = 256;

/// The one paging loop both callers run: `fetch` answers the page after the cursor
/// it is given and whether a later page exists, every entry goes to `each`, and
/// the next cursor is the last entry's key as `key_of` spells it.
///
/// The two callers differ only in how their source says "a later page exists" —
/// a page at least [`PAGE`] long ([`each_page`]), or the page's own `truncated`
/// flag (the bucket scan) — so that answer is what each adapts instead of a
/// second loop.
///
/// # Errors
///
/// `fetch`'s page failure, or `each`'s own.
async fn walk_pages<T, K, Fetch, KeyOf, Each>(
    mut fetch: Fetch,
    key_of: KeyOf,
    mut each: Each,
) -> Result<(), Error>
where
    Fetch: AsyncFnMut(Option<&K>) -> Result<(Vec<T>, bool), Error>,
    KeyOf: Fn(&T) -> K,
    Each: AsyncFnMut(T) -> Result<(), Error>,
{
    let mut after: Option<K> = None;
    loop {
        let (page, more) = fetch(after.as_ref()).await?;
        for entry in page {
            after = Some(key_of(&entry));
            each(entry).await?;
        }
        if !more {
            break;
        }
    }
    Ok(())
}

/// Walk one key-ordered scan in pages, handing every entry to `each`.
///
/// **One paging contract for every plane walk**: `fetch` answers the page after
/// the cursor it is given, a page shorter than [`PAGE`] ends the walk, and the
/// next cursor is the last entry's key as `key_of` spells it. The cursor type is
/// the caller's — a `String` for the row and placement scans, an
/// [`object::Key`] for the cold plane's — so verify, repair and
/// prepare-downgrade cannot disagree about where a bucket's scan stops.
///
/// # Errors
///
/// `fetch`'s page failure, or `each`'s own.
pub(crate) async fn each_page<T, K, Fetch, KeyOf, Each>(
    mut fetch: Fetch,
    key_of: KeyOf,
    each: Each,
) -> Result<(), Error>
where
    Fetch: AsyncFnMut(Option<&K>) -> Result<Vec<T>, Error>,
    KeyOf: Fn(&T) -> K,
    Each: AsyncFnMut(T) -> Result<(), Error>,
{
    walk_pages(
        async |after| {
            let page = fetch(after).await?;
            let more = page.len() >= PAGE;
            Ok((page, more))
        },
        key_of,
        each,
    )
    .await
}

/// Adopt the cold plane's failure as the storage error a tier command carries.
///
/// The store's own error becomes [`storage::Error`] exactly as `Error::Storage`'s
/// `#[from]` would; the one home keeps every verb's cold failure on the same
/// variant with the same text.
pub(crate) fn cold_error(err: impl Into<storage::Error>) -> Error {
    Error::Storage(err.into())
}

/// The `tinio tier` subcommand: one of the three offline verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "tier", "verify", "/srv/data", "--deep"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::Tier(_)));
/// ```
#[derive(Debug, ClapArgs)]
pub struct Args {
    /// The verb and its own arguments.
    #[command(subcommand)]
    pub verb: Verb,
}

/// The three offline verbs.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::{
///     cli::{Cli, Command},
///     commands::tier::Verb,
/// };
///
/// fn verb(argv: &[&str]) -> Verb {
///     let matches = Cli::command().try_get_matches_from(argv).expect("parse");
///     let cli = Cli::from_arg_matches(&matches).expect("from matches");
///     let Command::Tier(args) = cli.command else {
///         panic!("the tier arm");
///     };
///     args.verb
/// }
///
/// assert!(matches!(
///     verb(&["tinio", "tier", "verify", "/srv/data"]),
///     Verb::Verify(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "tier", "repair", "/srv/data"]),
///     Verb::Repair(_)
/// ));
/// assert!(matches!(
///     verb(&["tinio", "tier", "prepare-downgrade", "/srv/data"]),
///     Verb::PrepareDowngrade(_)
/// ));
/// ```
#[derive(Debug, Subcommand)]
pub enum Verb {
    /// Report placement and content problems without changing anything.
    Verify(verify::Args),
    /// Rebuild placements from the cold database and the filesystem rows.
    Repair(repair::Args),
    /// Promote every cold object and take the root out of tier mode.
    #[command(name = "prepare-downgrade")]
    PrepareDowngrade(downgrade::Args),
}

impl Args {
    /// The root, the mode and the environment channels every verb flattens: the
    /// discovery parse reads them for all three through this one accessor, so no
    /// verb can resolve one root and then open another.
    pub(crate) fn dir_args(&self) -> &DirArgs {
        match &self.verb {
            Verb::Verify(args) => &args.dir,
            Verb::Repair(args) => &args.dir,
            Verb::PrepareDowngrade(args) => &args.dir,
        }
    }
}

/// Runs the verb the command line named.
///
/// # Errors
///
/// A root, a configuration or a dsn that cannot be resolved (exit 2 or 1), the
/// tier layer's own refusal (a lost index without `--adopt`, a root-ID
/// mismatch, a second writer's ownership refusal), and the planes' failures.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::tier::{Args, Verb, run, verify},
///     resolve::locate,
/// };
///
/// # async fn verify_a_root() -> Result<(), tinio_cli::Error> {
/// let discovery = Discovery {
///     dir: Some("/srv/data".into()),
///     config: None,
///     read_only: false,
///     state_dir: None,
///     pg_dsn: None,
/// };
/// let located = locate(discovery, &Provenance::default())?;
/// let args = Args {
///     verb: Verb::Verify(verify::Args::default()),
/// };
/// let code = run(args, located).await?;
/// // 0 when nothing was found; 1 when a finding was reported.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    match args.verb {
        Verb::Verify(args) => verify::run(args, located).await,
        Verb::Repair(args) => repair::run(args, located).await,
        Verb::PrepareDowngrade(args) => downgrade::run(args, located).await,
    }
}

/// Print a verb's report and answer the exit code it earns.
///
/// **The three verbs' one output path**: each `execute` answers `(Report, u8)` and says nothing
/// about stdout or the process's code; this helper is the single place that renders the report and
/// turns the code into an [`ExitCode`], so verify, repair and prepare-downgrade cannot print or
/// exit differently.
///
/// # Errors
///
/// The verb's own failure, carried through untouched.
pub(crate) fn report_and_exit(outcome: Result<(Report, u8), Error>) -> Result<ExitCode, Error> {
    let (report, code) = outcome?;
    println!("{}", report.render_text());
    Ok(ExitCode::from(code))
}

/// The run's [`FsOptions`] with the tier's own access policy.
///
/// **A read-only plane carries no state-dir override**: the read-only metadata
/// source is always `<root>/.tinio` — the filesystem contract ignores a
/// configured state dir in that mode, and refuses an in-root one — so the
/// options carry none and the warm plane reads the root's own directory.
/// [`require_root_state_dir`] has already refused an explicit `TINIO_STATE_DIR`
/// naming another path, and a read-only run's home-derived leaf is its *tool*
/// state, which the storage never holds.
pub(crate) fn fs_options(resolution: &Resolution, access: AccessMode) -> FsOptions {
    let mut options = resolve::fs_options(resolution, None);
    options.access = access;
    if access.is_read_only() {
        options.state_dir = None;
    }
    options
}

/// Refuse a run whose explicit `TINIO_STATE_DIR` names a state directory other
/// than the root's own `<root>/.tinio`.
///
/// **A tier root's state directory is fixed**: the marker, the index and the
/// `[storage.tier]`/`[storage.pg]` configuration all live in `<root>/.tinio`,
/// and the layer reads that directory directly in either mode. A read-only run
/// that names no override resolves its *tool* state at the home-derived leaf —
/// its socket, `state` file and logs, which the storage never holds — so that
/// resolution is accepted and the tier still opens over the root's own
/// directory. An override that names a different path is refused before any
/// plane opens, because it would point the run's configuration somewhere the
/// tier records are not. The message names the variable because it is the
/// channel an operator moves the state dir with.
///
/// # Errors
///
/// A usage error naming the variable, the expected directory and the path the
/// variable named.
pub(crate) fn require_root_state_dir(resolution: &Resolution) -> Result<(), Error> {
    let own = resolution.root.join(object::RESERVED_SEGMENT);
    let Some(named) = resolution.state_dir_override.as_deref() else {
        return Ok(());
    };
    if named == own.as_path() {
        return Ok(());
    }
    Err(Error::Usage(format!(
        "tier roots keep their state at `{}`; unset TINIO_STATE_DIR or point it at that path \
         (this run's TINIO_STATE_DIR names `{}`)",
        own.display(),
        named.display()
    )))
}

/// The root a command resolved and the tier layer opened over it.
pub(crate) struct Opened {
    /// The resolution the root, the mode and the runtime keys came from.
    pub(crate) resolution: Resolution,
    /// The three planes the command inspects or repairs.
    pub(crate) tier: TierStorage<ColdStore>,
}

/// Resolve `dir`'s root and open the tier over it in `access`.
///
/// **One construction for the three commands**, so the root, the runtime keys
/// and the database a command opens cannot come from different documents: the
/// resolution is the client chain ([`resolve::for_doctor`] — load or default,
/// never write), the runtime keys are the root's `[storage.tier]`, and the dsn
/// is `TINIO_PG_DSN` over `[storage.pg] dsn` — the section and variable the
/// server itself opens.
///
/// `adopt` is `repair --adopt`: the bind's root-ID rule is the layer's own
/// (T18), so the flag is passed through rather than re-decided here.
///
/// # Errors
///
/// A pg run's synthetic root (the commands need a served root), a missing dsn,
/// a read-only run asked to write, and the layer's own open refusals.
pub(crate) async fn open(
    dir: DirArgs,
    located: Located,
    access: AccessMode,
    adopt: bool,
) -> Result<Opened, Error> {
    let from_env = dir.env.pg_dsn.clone();
    let resolution = resolve::for_doctor(located, &dir)?;
    if resolution.is_pg() {
        return Err(Error::Usage(
            "a tier root is a served directory, not a pg run's synthetic root: name the served \
             root with DIR instead"
                .into(),
        ));
    }
    require_root_state_dir(&resolution)?;
    if access == AccessMode::ReadWrite && resolution.read_only {
        return Err(Error::Usage(
            "this verb writes the tier index; it cannot run in read-only mode — drop \
             `--read-only`/`TINIO_READ_ONLY`"
                .into(),
        ));
    }
    // **A writable verb never converts a plain root.** `TierStorage::open_pg` records the
    // marker and creates the index *before* it binds, so a root with neither record would
    // silently become tier mode's — after which every non-tier start and `doctor` refuses
    // it, and the operator would have minted a tier root by asking to repair one. The
    // genuine recovery for a root whose **both** records were lost is `--adopt`, which is
    // the operator's own statement and reaches the layer's root-ID rule.
    if access == AccessMode::ReadWrite && !adopt && !is_tier_root(&resolution.root)? {
        return Err(Error::Usage(format!(
            "{} is not a tier root: neither the marker nor the index is present, and a writable \
             tier verb must not convert a plain root into tier mode; if both records were lost, \
             `tinio tier repair --adopt` is the recovery",
            resolution.root.display()
        )));
    }
    let section = resolution.config.pg_section();
    let dsn = resolve::pg::resolve_dsn(section, from_env.as_ref())?;
    let pool = section.map_or(DEFAULT_POOL_SIZE, |pg| pg.pool);
    let pg_config = _pg::Config::new(dsn, pool);
    let runtime = match resolution
        .config
        .storage
        .as_ref()
        .and_then(StorageConfig::tier_runtime)
    {
        Some(config) => config,
        None => TierSection::default().runtime(),
    };
    let options = fs_options(&resolution, access);
    let tier = TierStorage::open_pg(
        resolution.root.clone(),
        options,
        runtime,
        &pg_config,
        access,
        adopt,
    )
    .await?;
    Ok(Opened { resolution, tier })
}

/// Every bucket the root holds content in, in name order; the reserved
/// namespace is never one (T18).
///
/// **Two sources, because either plane can outlive the other.** The warm
/// plane's listing is the fs bucket records, paged exactly as the collection
/// pass pages it; the cold plane's recorded bucket rows are the buckets that
/// were demoted into (or quarantined) even if the warm bucket was later
/// removed. A bucket enumerated from the warm list alone would hide cold
/// content whose warm counterpart is gone, so `verify` would miss it, `repair`
/// would not reach it and `prepare-downgrade` would strand it. The union is
/// the one answer every verb uses; the cold keys themselves are walked per
/// bucket by each verb.
///
/// # Errors
///
/// The bucket listing's failure, or the cold plane's bucket-row read.
pub(crate) async fn buckets(tier: &TierStorage<ColdStore>) -> Result<Vec<bucket::Name>, Error> {
    use std::collections::BTreeSet;

    let mut names = BTreeSet::new();
    walk_pages(
        async |after| {
            let page = tier
                .list_buckets(
                    ListBucketsParams {
                        prefix: String::new(),
                        start_after: after.cloned(),
                        max_buckets: PAGE,
                    },
                    None,
                    None,
                )
                .await?;
            Ok((page.buckets, page.truncated))
        },
        |entry| entry.name.to_string(),
        async |entry| {
            if !pg::is_reserved(&entry.name) {
                names.insert(entry.name.to_string());
            }
            Ok(())
        },
    )
    .await?;
    // The cold plane's own bucket rows: a namespace whose warm bucket is gone
    // still holds content and must stay visible to every verb.
    for (name, _) in tier.cold().load_all().await.map_err(cold_error)? {
        let name = bucket::name(name).map_err(Error::Storage)?;
        if !pg::is_reserved(&name) {
            names.insert(name.to_string());
        }
    }
    names
        .into_iter()
        .map(|name| bucket::name(name).map_err(Error::Storage))
        .collect()
}

/// Whether the object at `key` has a file on the warm plane.
///
/// The row alone does not answer it: a cold object is exactly a row with no
/// file (T2). The probe is the filesystem plane's own head, so it reads the row
/// and the file through the same mapping every other read uses; a missing key
/// and a missing bucket directory are the two answers read as "no file".
///
/// # Errors
///
/// Every other failure — an unreadable root, a store failure.
pub(crate) async fn has_file(
    tier: &TierStorage<ColdStore>,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<bool, Error> {
    use crate::_core::storage::ObjectOps as _;

    match tier.fs().head_object(bucket, key).await {
        Ok(_) => Ok(true),
        Err(err) => match storage::Error::from(err) {
            // A missing key and a missing bucket directory are both "the key has no file":
            // the row is the object's existence, and the file is what may be absent (T2's
            // cold object). Any other failure is real and stops the leg.
            storage::Error::NoSuchKey(_) | storage::Error::NoSuchBucket(_) => Ok(false),
            other => Err(other.into()),
        },
    }
}

/// The content digest of a cold object's bytes in the database.
///
/// # Errors
///
/// The cold plane's open or a body read's failure.
pub(crate) async fn cold_digest(
    tier: &TierStorage<ColdStore>,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<[u8; 16], Error> {
    let content_key = ContentKey::Object(key.clone());
    let body = tier
        .cold()
        .open(bucket, &content_key, None)
        .await
        .map_err(cold_error)?;
    Ok(hash_body(body).await?)
}

/// The content digest of a warm object's file.
///
/// # Errors
///
/// The warm plane's read or a body read's failure.
pub(crate) async fn warm_digest(
    tier: &TierStorage<ColdStore>,
    bucket: &bucket::Name,
    key: &object::Key,
) -> Result<[u8; 16], Error> {
    use crate::_core::storage::ObjectOps as _;

    let got = tier.fs().get_object(bucket, key, None).await?;
    Ok(hash_body(got.body).await?)
}

/// The expected single-part digest of `etag`, or `None` for a composed
/// multipart ETag.
///
/// A composed ETag is the MD5 over the part digests, which the retained part
/// rows do not keep — so there is nothing to recompose without the bytes' own
/// part hashes, and a `--deep` run reports such an object as unverified rather
/// than guessing (T12's accepted limit).
pub(crate) fn single_digest(etag: &str) -> Option<[u8; 16]> {
    match etag.parse::<ETag>() {
        Ok(ETag::Single(digest)) => Some(digest),
        _ => None,
    }
}

/// The fixtures and helpers the three commands' legs share; each leg uses a subset.
#[cfg(test)]
#[expect(
    dead_code,
    reason = "test support: shared fixtures; each leg uses a subset"
)]
pub(crate) mod tests {
    use std::{
        fs,
        future::Future,
        path::{Path, PathBuf},
        pin::Pin,
        thread,
        time::{Duration, SystemTime},
    };

    use tinio_core::{
        AccessMode,
        acl::Acl,
        bucket,
        content::{ContentKey, ObjectContent, stage_body},
        object,
        storage::{BucketOps, ObjectOps},
    };
    use tinio_fs::testing;
    use tinio_store_pg::test_support::TestDb;
    use tinio_tier::{ColdStore, Config as RuntimeConfig, FlushMode, MoveOutcome, TierStorage, pg};
    use tinio_util::{
        secret::{Secret, SecretKey},
        testing::body,
    };
    use tokio::runtime::Builder;

    use super::{PAGE, each_page};
    use crate::{
        _tier::_pg::Config as PgConfig,
        cli::{Discovery, Provenance},
        report::{Report, Severity},
        resolve::{self, Located},
    };

    /// Every finding that is not `ok`, as the report renders it.
    pub(crate) fn problems(report: &Report) -> Vec<String> {
        report
            .findings()
            .iter()
            .filter(|finding| finding.severity != Severity::Ok)
            .map(|finding| format!("{}: {}", finding.check, finding.detail))
            .collect()
    }

    /// The bucket every leg writes to.
    pub(crate) const DATA: &str = "data";

    /// A small runtime configuration: synchronous writes and a long grace.
    pub(crate) fn runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            capacity_bytes: 1024 * 1024,
            max_object_bytes: 64 * 1024,
            flush_mode: FlushMode::Sync,
            batch_bytes: 8 * 1024,
            batch_interval: Duration::from_secs(1),
            heat_window: Duration::from_secs(60),
            promote_reads: 2,
            stats_flush: Duration::from_secs(10),
            high_water_percent: 90,
            low_water_percent: 80,
            demote_idle: Duration::from_secs(3600),
            dwell: Duration::from_secs(3600),
            scan_interval: Duration::from_secs(60),
            move_concurrency: 1,
            index_compact_percent: 30,
            gc_grace: Duration::from_secs(300),
        }
    }

    /// The same runtime configuration with the batch buffer armed, so a write
    /// can be made to wait for a flush (the shape a quarantine is seeded from).
    pub(crate) fn batch_runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            flush_mode: FlushMode::Batch,
            ..runtime_config()
        }
    }

    /// The scratch database's cold-plane configuration.
    pub(crate) fn pg_config(db: &TestDb) -> PgConfig {
        PgConfig::new(Secret::from(SecretKey::from(db.dsn.clone())), 4)
    }

    /// A root with a tier-mode configuration naming the scratch database.
    pub(crate) fn seed_root(root: &Path, dsn: &str) {
        let dir = root.join(".tinio");
        fs::create_dir_all(&dir).expect("the reserved directory");
        fs::write(
            dir.join(".tinio.toml"),
            format!("version = 1\n[storage]\ntype = \"tier\"\n[storage.pg]\ndsn = \"{dsn}\"\n"),
        )
        .expect("the tier configuration");
    }

    /// A fixture future: boxed so one helper can drive every leg's setup.
    ///
    /// Deliberately **not** `BoxFuture`: the future is created *inside* the thread
    /// that polls it, so it never crosses a thread boundary and the `Send` bound the
    /// alias carries would only narrow which fixture results are allowed.
    pub(crate) type Fixture<T> = Pin<Box<dyn Future<Output = T>>>;

    /// Run `f` on a runtime of its own and join it.
    ///
    /// **Every writable tier open needs its own**: the cold plane's ownership
    /// lock lives on the connection the store and its re-verify task share, so
    /// a second writable attempt in one runtime is refused — the same rule
    /// `tinio-tier`'s own restart legs follow.
    pub(crate) fn on_runtime<T, F>(f: F) -> T
    where
        F: FnOnce() -> Fixture<T> + Send + 'static,
        T: Send + 'static,
    {
        thread::spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a test runtime");
            let fixture = f();
            runtime.block_on(fixture)
        })
        .join()
        .expect("the leg's runtime ends")
    }

    /// The fixture's tier, opened writable on a runtime of its own.
    pub(crate) fn with_tier<T, F>(root: &Path, dsn: &str, f: F) -> T
    where
        F: FnOnce(TierStorage<ColdStore>) -> Fixture<T> + Send + 'static,
        T: Send + 'static,
    {
        let root = root.to_path_buf();
        let pg = PgConfig::new(Secret::from(SecretKey::from(dsn.to_owned())), 4);
        on_runtime(move || {
            Box::pin(async move {
                let tier = TierStorage::open_pg(
                    root,
                    testing::fs_options(),
                    runtime_config(),
                    &pg,
                    AccessMode::ReadWrite,
                    false,
                )
                .await
                .expect("the fixture tier opens");
                f(tier).await
            })
        })
    }

    /// The fixture's tier, opened writable on a runtime of its own with
    /// `config` — the batch mode a quarantine leg needs.
    pub(crate) fn with_tier_config<T, F>(root: &Path, dsn: &str, config: RuntimeConfig, f: F) -> T
    where
        F: FnOnce(TierStorage<ColdStore>) -> Fixture<T> + Send + 'static,
        T: Send + 'static,
    {
        let root = root.to_path_buf();
        let pg = PgConfig::new(Secret::from(SecretKey::from(dsn.to_owned())), 4);
        on_runtime(move || {
            Box::pin(async move {
                let tier = TierStorage::open_pg(
                    root,
                    testing::fs_options(),
                    config,
                    &pg,
                    AccessMode::ReadWrite,
                    false,
                )
                .await
                .expect("the fixture tier opens");
                f(tier).await
            })
        })
    }

    /// Plant one write a failed batch flush quarantines: the bucket is created,
    /// a plain write is buffered, a directory blocks its publish, and the drain
    /// quarantines the pair under the root's `.tinio/quarantine/`.
    pub(crate) fn seed_quarantine(root: &Path, dsn: &str, name: &str, bytes: &'static [u8]) {
        let file_root = root.to_path_buf();
        let name = name.to_owned();
        with_tier_config(root, dsn, batch_runtime_config(), move |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, &name, bytes).await;
                assert!(
                    !tier.buffer().is_empty(),
                    "the fixture's write waits in the batch buffer"
                );
                // A directory where the object's file belongs fails every publish
                // attempt of this key, so the drain quarantines it.
                fs::create_dir_all(object_file(&file_root, &name)).expect("the blocker exists");
                tier.shutdown().await.expect("the drain reaches the end");
                assert_eq!(
                    tier.quarantined().expect("the quarantine is listed").len(),
                    1,
                    "the fixture left one quarantined write"
                );
            })
        });
    }

    /// The `Located` a command resolves `root` to, exactly as the process does.
    pub(crate) fn located(root: &Path) -> Located {
        resolve::locate(
            Discovery {
                dir: Some(root.to_path_buf()),
                config: None,
                read_only: false,
                state_dir: None,
                pg_dsn: None,
            },
            &Provenance::default(),
        )
        .expect("the temp root resolves")
    }

    /// `root` plus the command's resolution, on a path outside it.
    pub(crate) fn root_and_location(root: &Path) -> (PathBuf, Located) {
        (root.to_path_buf(), located(root))
    }

    /// The fixture's bucket name.
    pub(crate) fn data() -> bucket::Name {
        bucket::name(DATA).expect("a valid bucket name")
    }

    /// The fixture's object key.
    pub(crate) fn key(name: &str) -> object::Key {
        object::key(name).expect("a valid object key")
    }

    /// The object file a plain key maps to under a root.
    pub(crate) fn object_file(root: &Path, key: &str) -> PathBuf {
        root.join(DATA).join(key)
    }

    /// Create the bucket and write one object, answering the tier it wrote
    /// through to the caller's future.
    pub(crate) async fn create_bucket(tier: &TierStorage<ColdStore>) {
        tier.create_bucket(&data(), None, &Acl::default_private(None))
            .await
            .expect("the fixture bucket is created");
    }

    /// Write one object through the tier's own write path.
    pub(crate) async fn put(tier: &TierStorage<ColdStore>, name: &str, bytes: &'static [u8]) {
        tier.put_object(&data(), &key(name), body(bytes))
            .await
            .expect("the fixture object is written");
    }

    /// Demote one object and answer whether it moved.
    pub(crate) async fn demote(tier: &TierStorage<ColdStore>, name: &str) -> bool {
        matches!(
            tier.demote(&data(), &key(name), SystemTime::now())
                .await
                .expect("the fixture demote runs"),
            MoveOutcome::Moved { .. }
        )
    }

    /// Overwrite a cold object's bytes in the database, leaving its placement
    /// and its recorded metadata alone — the corruption a deep run must catch.
    pub(crate) async fn overwrite_cold(
        tier: &TierStorage<ColdStore>,
        name: &str,
        bytes: &'static [u8],
    ) {
        let content_key = ContentKey::Object(key(name));
        let mut staged = tier
            .cold()
            .stage(&data(), &content_key)
            .await
            .expect("the cold stage opens");
        stage_body(&mut staged, body(bytes))
            .await
            .expect("the replacement bytes land");
        tier.cold()
            .publish(&data(), staged, &content_key)
            .await
            .expect("the replacement is published");
    }

    /// The cold content of one object, `None` when the database holds none.
    pub(crate) fn cold_stat(dsn: &str, name: &str) -> Option<u64> {
        let dsn = dsn.to_owned();
        let name = name.to_owned();
        on_runtime(move || {
            Box::pin(async move {
                let pool_config = PgConfig::new(Secret::from(SecretKey::from(dsn)), 2);
                let cold = pg::connect(&pool_config, AccessMode::ReadOnly)
                    .await
                    .expect("the read-only cold plane opens");
                cold.stat(&data(), &key(&name))
                    .await
                    .expect("the cold stat runs")
                    .map(|entry| entry.len)
            })
        })
    }

    /// Whether the database records a tier root identity.
    ///
    /// The identity lives as content under the public reserved namespace
    /// (`tinio_tier::pg::ROOT_NAMESPACE`), so an object-key scan of that namespace answers
    /// the question without a private API: an empty page is "no root was ever bound".
    ///
    /// The connection is writable because a scratch database has not been migrated yet and
    /// the read-only store's schema gate refuses one: the writable open is the inspection's
    /// own connection (it migrates and takes the ownership lock for the helper's runtime,
    /// then gives both back), and it records no root identity of its own.
    pub(crate) fn pg_root_recorded(dsn: &str) -> bool {
        let dsn = dsn.to_owned();
        on_runtime(move || {
            Box::pin(async move {
                let pool_config = PgConfig::new(Secret::from(SecretKey::from(dsn)), 2);
                let cold = pg::connect(&pool_config, AccessMode::ReadWrite)
                    .await
                    .expect("the cold plane opens for the probe");
                let reserved = bucket::name(pg::ROOT_NAMESPACE)
                    .expect("the reserved namespace is a bucket name");
                !cold
                    .scan(&reserved, "", None, PAGE)
                    .await
                    .expect("the reserved namespace scans")
                    .is_empty()
            })
        })
    }

    /// The keys one bucket's placement table holds, in key order.
    pub(crate) async fn placements(root: &Path) -> Vec<(String, tinio_tier::Placement)> {
        let index = tinio_tier::Index::open_read_only(tinio_tier::index_path(root))
            .expect("the rebuilt index opens read-only");
        let mut out = Vec::new();
        each_page(
            async |after| {
                Ok(index
                    .scan_placement(&data(), "", after.map(String::as_str), PAGE)
                    .expect("the placement page runs"))
            },
            |entry| entry.0.as_ref().to_owned(),
            async |entry| {
                out.push((entry.0.as_ref().to_owned(), entry.1));
                Ok(())
            },
        )
        .await
        .expect("the placement page runs");
        out
    }

    /// The placement one key records, if any.
    pub(crate) async fn placement_of(root: &Path, name: &str) -> Option<tinio_tier::Placement> {
        placements(root)
            .await
            .into_iter()
            .find(|(key, _)| key == name)
            .map(|(_, placement)| placement)
    }

    /// The root identity the rebuilt index records.
    pub(crate) fn index_root_id(root: &Path) -> Option<[u8; 16]> {
        tinio_tier::Index::open_read_only(tinio_tier::index_path(root))
            .expect("the rebuilt index opens read-only")
            .root_id()
            .expect("the root id reads")
    }

    /// The root identity the marker records.
    pub(crate) fn marker_root_id(root: &Path) -> Option<[u8; 16]> {
        let bytes = fs::read(tinio_tier::marker_path(root)).ok()?;
        <[u8; 16]>::try_from(bytes.as_slice()).ok()
    }

    /// I6: an offline verb refuses an explicit `TINIO_STATE_DIR` naming a state dir other than the
    /// root's own `<root>/.tinio`, before opening any plane. The tier records live in the root's
    /// own directory, and the run's configuration must be read from the same tree.
    #[tokio::test]
    async fn a_relocated_state_dir_refuses_the_offline_open() {
        use clap::parser::ValueSource;

        let root = tempfile::TempDir::new().expect("a temporary root");
        let relocated = tempfile::TempDir::new().expect("a relocated state dir");
        let located = resolve::locate(
            Discovery {
                dir: Some(root.path().to_path_buf()),
                config: None,
                read_only: true,
                state_dir: Some(relocated.path().to_path_buf()),
                pg_dsn: None,
            },
            // The read-only level is the second parse's: a `DefaultValue` provenance falls
            // through to the config file, so the mode has to arrive as an explicit channel.
            &Provenance {
                read_only: ValueSource::CommandLine,
                ..Provenance::default()
            },
        )
        .expect("the root resolves");
        assert_ne!(
            located.state_dir,
            located.root.join(".tinio"),
            "the fixture's state dir really is relocated"
        );

        let refused = match super::open(
            super::DirArgs::default(),
            located,
            AccessMode::ReadOnly,
            false,
        )
        .await
        {
            Ok(_) => panic!("the relocated state dir is refused"),
            Err(err) => err,
        };
        assert!(matches!(refused, crate::Error::Usage(_)), "{refused}");
        let rendered = refused.to_string();
        assert!(
            rendered.contains("TINIO_STATE_DIR"),
            "the refusal names the variable: {rendered}"
        );
        // The refusal renders the resolved root, which is canonical; the TempDir path is not.
        let expected = root.path().canonicalize().expect("the root canonicalizes");
        assert!(
            rendered.contains(&expected.join(".tinio").display().to_string()),
            "the refusal names the expected directory: {rendered}"
        );
    }

    /// I6: a read-only resolution that names **no** override is accepted — its state dir is the
    /// home-derived leaf, which is the run's *tool* state; the tier opens over `<root>/.tinio`
    /// anyway. The open therefore reaches its normal path (here the dsn it needs).
    #[tokio::test]
    async fn a_read_only_resolution_without_an_override_reaches_the_offline_open() {
        let root = tempfile::TempDir::new().expect("a temporary root");
        let tool_state = tempfile::TempDir::new().expect("a home-derived leaf");
        let located = Located {
            root: root.path().to_path_buf(),
            read_only: true,
            state_dir: tool_state.path().to_path_buf(),
            state_dir_override: None,
            pg_root: None,
            config: None,
            config_doc: None,
        };

        let refused = match super::open(
            super::DirArgs::default(),
            located,
            AccessMode::ReadOnly,
            false,
        )
        .await
        {
            Ok(_) => panic!("the fixture names no dsn, so the open must stop there"),
            Err(err) => err,
        };
        assert!(
            !matches!(refused, crate::Error::Usage(_)),
            "the state-dir gate accepted the run: {refused}"
        );
        assert!(
            refused.to_string().contains("dsn"),
            "the open reached the cold plane's dsn: {refused}"
        );
    }

    /// I6: a writable resolution still needs the root's own state dir, and passes the state-dir
    /// gate; the next refusal is the plan-root one (`open`'s writable rule).
    #[tokio::test]
    async fn a_writable_resolution_keeps_the_roots_state_dir() {
        let root = tempfile::TempDir::new().expect("a temporary root");
        let located = Located {
            root: root.path().to_path_buf(),
            read_only: false,
            state_dir: root.path().join(".tinio"),
            state_dir_override: None,
            pg_root: None,
            config: None,
            config_doc: None,
        };

        let refused = match super::open(
            super::DirArgs::default(),
            located,
            AccessMode::ReadWrite,
            false,
        )
        .await
        {
            Ok(_) => panic!("a plain root is not converted by a writable verb"),
            Err(err) => err,
        };
        let rendered = refused.to_string();
        assert!(
            rendered.contains("not a tier root"),
            "the state-dir gate accepted the run and the plan-root rule answered: {rendered}"
        );
    }
}
