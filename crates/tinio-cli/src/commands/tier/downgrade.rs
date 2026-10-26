//! `tinio tier prepare-downgrade` — take a root out of tier mode (T28).
//!
//! The command promotes **every** cold object, removes the index and then the
//! marker, and verifies the root is filesystem-only: every former cold object
//! has its file back, body and recorded ETag included. The order matters — the
//! index goes before the marker, so a crash between the two leaves a root the
//! seam still refuses (T17) rather than one a non-tier build would serve with
//! cold placements still in it.
//!
//! A placement the promotion cannot settle refuses the whole downgrade: the
//! index and the marker stay, and the report names what has to be resolved
//! first. An older binary does not read the marker, so the operator must run
//! this before a rollback (T28).
//!
//! **This verb has no `--adopt`.** A lost index is bound by
//! `tinio tier repair --adopt` first, and the downgrade is retried after it.

use std::{fs, io::ErrorKind, path::Path, process::ExitCode};

use clap::Args as ClapArgs;

use super::{DirArgs, PAGE, buckets, cold_error, each_page, has_file, open};
use crate::{
    _core::{AccessMode, bucket, content::ObjectContent, object, storage::ObjectOps as _},
    _fs::{FsOptions, FsStorage},
    _tier::{
        ColdStore, Error as TierError, MoveOutcome, State, Tier, TierStorage, index_path,
        is_tier_root, marker_path,
    },
    Error,
    report::{Finding, Report, Severity},
    resolve::Located,
};

/// The row a promoted object earns.
const PROMOTED: &str = "promoted";
/// The row the filesystem-only answer earns.
const FS_ONLY: &str = "filesystem only";
/// The row a placement the promotion cannot settle earns.
const UNSETTLED: &str = "placement";
/// The row the run's summary earns.
const SUMMARY: &str = "tier root";

/// `tinio tier prepare-downgrade`: the root alone.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "tier", "prepare-downgrade", "/srv/data"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// assert!(matches!(cli.command, Command::Tier(_)));
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct Args {
    /// The root the tier serves; every cold object under it is promoted.
    #[command(flatten)]
    pub dir: DirArgs,
}

/// Runs the verb: the report on stdout, the exit code its findings earn.
///
/// # Errors
///
/// The root, the configuration, the dsn or the layer's open refusal, and the
/// planes' failures on the way out.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::tier::{downgrade, downgrade::Args},
///     resolve::locate,
/// };
///
/// # async fn downgrade_a_root() -> Result<(), tinio_cli::Error> {
/// let located = locate(
///     Discovery {
///         dir: Some("/srv/data".into()),
///         config: None,
///         read_only: false,
///         state_dir: None,
///         pg_dsn: None,
///     },
///     &Provenance::default(),
/// )?;
/// let code = downgrade::run(Args::default(), located).await?;
/// // 0 when the root is filesystem-only; 1 when a placement was left unsettled.
/// # let _ = code;
/// # Ok(())
/// # }
/// ```
pub async fn run(args: Args, located: Located) -> Result<ExitCode, Error> {
    super::report_and_exit(execute(args, located).await)
}

/// The command's body without the printing: the report and the exit code it
/// earns, which is what the legs assert.
#[tracing::instrument(
    target = "tinio::tier",
    name = "tier.prepare_downgrade",
    level = "info",
    skip_all
)]
pub(crate) async fn execute(args: Args, located: Located) -> Result<(Report, u8), Error> {
    let super::Opened { resolution, tier } = open(args.dir, located, AccessMode::ReadWrite, false)
        .await
        .map_err(|err| match err {
            // The layer's bind refusal names `--adopt`, a flag this verb does not carry. Its
            // real recovery is the repair verb's bind, then this command again.
            Error::Tier(TierError::AdoptRequired) => Error::Operational(
                "the tier index is missing and the cold database already holds data; \
                 `prepare-downgrade` has no `--adopt`: run `tinio tier repair --adopt` to bind \
                 the index to the database, then retry `tinio tier prepare-downgrade`"
                    .into(),
            ),
            other => other,
        })?;
    let (cold, mut report) = cold_objects(&tier).await?;

    // A placement the promotion cannot settle refuses the downgrade whole: the
    // index and the marker stay, so the root is still tier mode's and nothing a
    // non-tier build serves loses its metadata.
    if report.severity() == Severity::Error {
        return Ok(stopped(report));
    }

    let mut promoted = 0usize;
    for object in &cold {
        let moved = tier.promote(&object.bucket, &object.key).await;
        match moved {
            Ok(MoveOutcome::Moved { .. }) => {
                promoted += 1;
                report.push(Finding::check(
                    PROMOTED,
                    Severity::Ok,
                    format!(
                        "{}/{}: promoted to the filesystem plane",
                        object.bucket, object.key
                    ),
                ));
            }
            // The promotion aborted without moving bytes. Which half it was is the row's
            // answer: a surviving row is the completed warm overwrite that won (T5), and
            // the cold copy is the stale half of the crash; an absent row means the
            // placement outlived the object, which no promotion can settle and which must
            // refuse the downgrade rather than report the run clean.
            Ok(MoveOutcome::Aborted) => {
                if tier
                    .tier_rows()
                    .info(&object.bucket, &object.key)
                    .await?
                    .is_some()
                {
                    report.push(Finding::check(
                        PROMOTED,
                        Severity::Ok,
                        format!(
                            "{}/{}: already warm; the cold copy was dropped",
                            object.bucket, object.key
                        ),
                    ));
                } else {
                    report.push(Finding::check(
                        UNSETTLED,
                        Severity::Error,
                        format!(
                            "{}/{}: the filesystem row disappeared before the promotion, so the \
                             cold copy had no object to restore; reconcile the root before the \
                             downgrade",
                            object.bucket, object.key
                        ),
                    ));
                }
            }
            Ok(MoveOutcome::Skipped) => report.push(Finding::check(
                UNSETTLED,
                Severity::Error,
                format!(
                    "{}/{}: the promotion was skipped (the key was busy); the placement is still \
                     cold",
                    object.bucket, object.key
                ),
            )),
            Ok(MoveOutcome::Unchanged) => report.push(Finding::check(
                UNSETTLED,
                Severity::Error,
                format!(
                    "{}/{}: the promotion moved nothing; the placement is still cold",
                    object.bucket, object.key
                ),
            )),
            Err(err) => report.push(Finding::check(
                UNSETTLED,
                Severity::Error,
                format!(
                    "{}/{}: the promotion failed: {err}",
                    object.bucket, object.key
                ),
            )),
        }
    }
    if report.severity() == Severity::Error {
        return Ok(stopped(report));
    }

    let options = super::fs_options(&resolution, AccessMode::ReadOnly);
    // The index file is still open: drop the layer before removing its files
    // (a Windows delete of an open file is a sharing violation).
    drop(tier);
    remove_tier_files(&resolution.root)?;
    let restored = verify_fs_only(&resolution.root, options, &cold).await?;
    report.extend(restored.findings().iter().cloned());
    if report.severity() == Severity::Ok {
        report.push(summary(promoted));
    } else {
        report.push(Finding::check(
            SUMMARY,
            Severity::Error,
            "the promotions ran and the tier records are gone, but the filesystem-only check \
             failed",
        ));
    }
    let code = report.exit_code();
    Ok((report, code))
}

/// Stop the downgrade with the tier's records intact: the index and the
/// marker stay, so the root is still tier mode's and nothing a non-tier build
/// serves loses its metadata.
fn stopped(mut report: Report) -> (Report, u8) {
    report.push(Finding::check(
        SUMMARY,
        Severity::Error,
        "the downgrade stopped: the index and the marker are still in place",
    ));
    let code = report.exit_code();
    (report, code)
}

/// One cold object the downgrade has to promote, and the ETag the file must
/// still carry afterwards.
pub(crate) struct Cold {
    /// The object's bucket.
    pub(crate) bucket: bucket::Name,
    /// The object's key.
    pub(crate) key: object::Key,
    /// The recorded ETag wire the restored file's content must still hash to.
    pub(crate) etag: String,
}

/// The cold objects of `tier`, or the findings that refuse the downgrade.
///
/// Two walks answer it, because T28's "promotes every cold object" cannot be read off
/// the index alone: the placements name the cold objects the index knows, and the
/// filesystem rows name every object — a row with no file **is** cold (T2), whether or
/// not the placement was recorded. A cold row the index does not place cold is a
/// refusal, never a silent skip.
pub(crate) async fn cold_objects(
    tier: &TierStorage<ColdStore>,
) -> Result<(Vec<Cold>, Report), Error> {
    let mut cold = Vec::new();
    let mut report = Report::default();
    for bucket in buckets(tier).await? {
        let rows = tier.tier_rows();
        each_page(
            async |after| {
                Ok(tier
                    .index()
                    .scan_placement(&bucket, "", after.map(String::as_str), PAGE)?)
            },
            |entry| entry.0.as_ref().to_owned(),
            async |entry| {
                let (key, placement) = entry;
                match (placement.tier, placement.state) {
                    (Tier::Pg, State::Live) => {
                        let Some(info) = placement.cold else {
                            report.push(Finding::check(
                                UNSETTLED,
                                Severity::Error,
                                format!(
                                    "{bucket}/{key}: a cold placement carries no cold metadata"
                                ),
                            ));
                            return Ok(());
                        };
                        // **No promotion without a row.** A placement is a claim about an
                        // object, and the row is the object; promoting into a missing row is
                        // the promotion sequence's "deleted out from under the placement"
                        // path, which retires the placement and drops the cold copy. The
                        // same rule repair applies to a database key with no row, applied
                        // before anything is touched: the content cannot name an object, and
                        // removing the index and the marker would orphan its only copy.
                        if rows.info(&bucket, &key).await?.is_none() {
                            report.push(Finding::check(
                                UNSETTLED,
                                Severity::Error,
                                format!(
                                    "{bucket}/{key}: the index places the object cold but the \
                                     filesystem plane has no row for it; the cold content cannot \
                                     be promoted to an object, and no promotion restores a missing \
                                     row — reconcile the root before the downgrade"
                                ),
                            ));
                            return Ok(());
                        }
                        cold.push(Cold {
                            bucket: bucket.clone(),
                            key,
                            etag: info.etag,
                        });
                    }
                    (_, State::Live) => {}
                    (_, state) => report.push(Finding::check(
                        UNSETTLED,
                        Severity::Error,
                        format!(
                            "{bucket}/{key}: a {} placement is still in flight; run the collection \
                             pass (and `tinio tier verify` for its report) before the downgrade",
                            state.as_str()
                        ),
                    )),
                }
                Ok(())
            },
        )
        .await?;

        // **T2: a row with no file *is* a cold object**, whatever the index says. The
        // placement walk above finds the ones it places cold; a cold row the index does
        // not place cold is outside the promotion set, and removing the index and the
        // marker would leave the only copy unreachable — so it refuses the downgrade.
        each_page(
            async |after| {
                Ok(rows
                    .scan(&bucket, "", after.map(String::as_str), PAGE)
                    .await?)
            },
            |row| row.key.as_ref().to_owned(),
            async |row| {
                if has_file(tier, &bucket, &row.key).await? {
                    return Ok(());
                }
                if tier
                    .index()
                    .placement(&bucket, &row.key)?
                    .is_some_and(|placement| placement.tier == Tier::Pg)
                {
                    // Already in the cold set the placement walk answered.
                    return Ok(());
                }
                report.push(Finding::check(
                    UNSETTLED,
                    Severity::Error,
                    format!(
                        "{bucket}/{}: the row has no file, so the object is cold, but the index \
                         does not place it cold; run `tinio tier repair` before the downgrade",
                        row.key
                    ),
                ));
                Ok(())
            },
        )
        .await?;

        // **The cold plane's own keys are the third source.** A key with neither a filesystem
        // row nor a placement is invisible to both walks above, and removing the index and the
        // marker would strand its only copy with nothing naming an object to promote it into
        // (T2). The scan is repair's own — a key-ordered, paged walk of the cold plane — so the
        // two verbs cannot disagree about what the plane holds.
        let mut stranded: Vec<(&bucket::Name, object::Key)> = Vec::new();
        each_page(
            async |after| {
                tier.cold()
                    .scan(&bucket, "", after, PAGE)
                    .await
                    .map_err(cold_error)
            },
            |entry| entry.key.clone(),
            async |entry| {
                if rows.info(&bucket, &entry.key).await?.is_some() {
                    return Ok(());
                }
                // A `pg`/live placement with no row is the placement walk's own refusal; it is
                // named above and counted once.
                if tier
                    .index()
                    .placement(&bucket, &entry.key)?
                    .is_some_and(|placement| {
                        placement.tier == Tier::Pg && placement.state == State::Live
                    })
                {
                    return Ok(());
                }
                stranded.push((&bucket, entry.key));
                Ok(())
            },
        )
        .await?;
        if !stranded.is_empty() {
            const NAMED: usize = 8;
            let keys = stranded
                .iter()
                .take(NAMED)
                .map(|(bucket, key)| format!("{bucket}/{key}"))
                .collect::<Vec<_>>()
                .join(", ");
            let more = stranded.len().saturating_sub(NAMED);
            report.push(Finding::check(
                UNSETTLED,
                Severity::Error,
                format!(
                    "{} cold-plane key(s) have neither a filesystem row nor a placement, so the \
                     downgrade would strand them: {keys}{}; reconcile the root with `tinio tier \
                     repair` before the downgrade",
                    stranded.len(),
                    if more > 0 {
                        format!(" and {more} more")
                    } else {
                        String::new()
                    },
                ),
            ));
        }
    }
    Ok((cold, report))
}

/// Remove the index, then the marker — the reverse of the order T17 creates
/// them in, so a crash between the two still leaves a root the seam refuses.
///
/// A missing file is the state the caller asked for and is not an error.
pub(crate) fn remove_tier_files(root: &Path) -> Result<(), Error> {
    for path in [index_path(root), marker_path(root)] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => return Err(Error::Io(err)),
        }
    }
    Ok(())
}

/// Every former cold object's file is back, and the root is filesystem-only.
///
/// This is T28's "verifies the root is fs-only" half, read through a plain
/// filesystem plane rather than the tier's: the objects must be readable with
/// the tier files gone. `options` is the run's own resolved `FsOptions` with the
/// read-only policy already imposed (`super::fs_options`) — nothing here writes.
pub(crate) async fn verify_fs_only(
    root: &Path,
    options: FsOptions,
    cold: &[Cold],
) -> Result<Report, Error> {
    let mut report = Report::default();
    if is_tier_root(root)? {
        report.push(Finding::check(
            FS_ONLY,
            Severity::Error,
            format!(
                "{} still records tier mode; the index or the marker survived",
                root.display()
            ),
        ));
        return Ok(report);
    }
    let fs = FsStorage::new(root, options)?;
    for object in cold {
        match fs.get_object(&object.bucket, &object.key, None).await {
            Ok(got) if got.info.etag.to_string() == object.etag => {}
            Ok(got) => report.push(Finding::check(
                FS_ONLY,
                Severity::Error,
                format!(
                    "{}/{}: the restored file records ETag {} but the cold metadata recorded {}",
                    object.bucket, object.key, got.info.etag, object.etag
                ),
            )),
            Err(err) => report.push(Finding::check(
                FS_ONLY,
                Severity::Error,
                format!(
                    "{}/{}: the restored file could not be read: {err}",
                    object.bucket, object.key
                ),
            )),
        }
    }
    if report.severity() == Severity::Ok {
        report.push(Finding::check(
            FS_ONLY,
            Severity::Ok,
            format!(
                "{} former cold object(s) read back from the filesystem; no tier record remains",
                cold.len()
            ),
        ));
    }
    Ok(report)
}

/// The summary row a finished downgrade carries.
pub(crate) fn summary(promoted: usize) -> Finding {
    Finding::check(
        SUMMARY,
        Severity::Ok,
        format!("{promoted} cold object(s) promoted; the root is filesystem-only"),
    )
}

#[cfg(test)]
mod tests {
    use tinio_core::{
        content::{ContentKey, ObjectContent, stage_body},
        storage::{BucketOps, ObjectOps},
    };
    use tinio_store_pg::test_support::{TestDb, require_dsn};
    use tinio_util::testing::body;

    use super::*;
    use crate::commands::tier::tests::{
        cold_stat, create_bucket, data, demote, key, located, object_file, on_runtime, problems,
        put, seed_root, with_tier,
    };

    /// T28: every cold object is promoted, the index and the marker go, and the
    /// root serves its object bodies from the filesystem alone.
    #[tokio::test]
    async fn prepare_downgrade_leaves_fs_only() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "a.bin", b"alpha").await;
                put(&tier, "b.bin", b"bravo").await;
                assert!(demote(&tier, "a.bin").await, "a.bin is cold");
                assert!(demote(&tier, "b.bin").await, "b.bin is cold");
            })
        });
        assert_eq!(cold_stat(&dsn, "a.bin"), Some(5), "the fixture is cold");

        let located = located(&root_path);
        let args = Args::default();
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the downgrade runs");

        assert_eq!(code, 0, "a clean downgrade: {:?}", problems(&report));
        assert!(
            report
                .findings()
                .iter()
                .filter(|finding| finding.check == PROMOTED)
                .count()
                >= 2,
            "both cold objects are promoted: {:?}",
            report.findings()
        );
        assert!(
            report
                .findings()
                .iter()
                .any(|finding| finding.check == FS_ONLY && finding.severity == Severity::Ok),
            "{:?}",
            report.findings()
        );

        assert!(
            !tinio_tier::is_tier_root(&root_path).expect("the seam answers"),
            "no tier record survives"
        );
        assert!(!index_path(&root_path).exists(), "the index is removed");
        assert!(!marker_path(&root_path).exists(), "the marker is removed");
        assert_eq!(
            fs::read(object_file(&root_path, "a.bin")).expect("a.bin is back"),
            b"alpha"
        );
        assert_eq!(
            fs::read(object_file(&root_path, "b.bin")).expect("b.bin is back"),
            b"bravo"
        );
        assert_eq!(cold_stat(&dsn, "a.bin"), None, "the cold copy is gone");
        assert_eq!(cold_stat(&dsn, "b.bin"), None, "the cold copy is gone");
    }

    /// T2/T28: a row with no file **is** a cold object even when the index lost its
    /// placement. Removing the index and the marker would orphan that object's only copy,
    /// so the downgrade must refuse and leave both records in place.
    #[tokio::test]
    async fn prepare_downgrade_refuses_an_unplaced_cold_row() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        let file_root = root_path.clone();
        with_tier(&root_path, &dsn, move |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "orphan.bin", b"hello").await;
                // No demote and no placement: the row alone says the object is cold.
                fs::remove_file(object_file(&file_root, "orphan.bin"))
                    .expect("the object's file is removed");
            })
        });

        let located = located(&root_path);
        let args = Args::default();
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the downgrade answers with a report");

        assert_eq!(
            code,
            1,
            "an unplaced cold row refuses the downgrade: {:?}",
            problems(&report)
        );
        assert!(
            report
                .findings()
                .iter()
                .any(|finding| finding.check == UNSETTLED && finding.detail.contains("orphan.bin")),
            "{:?}",
            report.findings()
        );
        assert!(
            is_tier_root(&root_path).expect("the seam answers"),
            "the tier records survive the refusal"
        );
        assert!(index_path(&root_path).exists(), "the index is untouched");
        assert!(marker_path(&root_path).exists(), "the marker is untouched");
        assert!(
            !object_file(&root_path, "orphan.bin").exists(),
            "nothing was promoted"
        );
    }

    /// T2/T28: a cold-plane key with neither a filesystem row nor a placement is invisible to
    /// both of the scan's walks. The downgrade must refuse it before anything is promoted or
    /// removed: removing the index and the marker would strand the only copy with no object to
    /// restore it into.
    #[tokio::test]
    async fn prepare_downgrade_refuses_a_cold_key_with_no_row_or_placement() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                tier.ensure_cold_bucket(&data())
                    .await
                    .expect("the cold namespace exists");
                // Content the cold plane holds under a key no row and no placement names.
                let content_key = ContentKey::Object(key("planted.bin"));
                let mut staged = tier
                    .cold()
                    .stage(&data(), &content_key)
                    .await
                    .expect("the planted stage opens");
                stage_body(&mut staged, body(b"planted"))
                    .await
                    .expect("the planted bytes land");
                tier.cold()
                    .publish(&data(), staged, &content_key)
                    .await
                    .expect("the planted content is published");
            })
        });

        let located = located(&root_path);
        let args = Args::default();
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the downgrade answers with a report");

        assert_eq!(
            code,
            1,
            "a stranded cold key refuses the downgrade: {:?}",
            problems(&report)
        );
        assert!(
            report.findings().iter().any(|finding| {
                finding.check == UNSETTLED
                    && finding.severity == Severity::Error
                    && finding.detail.contains("1 cold-plane key")
                    && finding.detail.contains("planted.bin")
            }),
            "the refusal names the count and the key: {:?}",
            report.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "planted.bin"),
            Some(7),
            "the refusal leaves the only copy intact"
        );
        assert!(
            is_tier_root(&root_path).expect("the seam answers"),
            "the tier records survive the refusal"
        );
        assert!(index_path(&root_path).exists(), "the index is untouched");
        assert!(marker_path(&root_path).exists(), "the marker is untouched");
        assert!(
            !object_file(&root_path, "planted.bin").exists(),
            "nothing was promoted"
        );
    }

    /// T2/T28: a bucket whose warm counterpart was removed but whose cold content
    /// survives is invisible to the fs bucket listing. The enumeration unions the
    /// cold plane's bucket rows, so the cold-only key reaches the walk and refuses
    /// the downgrade before anything is promoted.
    #[tokio::test]
    async fn prepare_downgrade_refuses_a_cold_only_bucket() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "only.bin", b"hello").await;
                assert!(demote(&tier, "only.bin").await, "only.bin is cold");
                // The row and then the warm bucket go; the placement is cleared, so
                // only the cold plane's bucket row and content name the namespace.
                tier.fs()
                    .delete_object(&data(), &key("only.bin"))
                    .await
                    .expect("the row is deleted");
                tier.index()
                    .clear(&data(), &key("only.bin"))
                    .expect("the placement is cleared");
                tier.fs()
                    .delete_bucket(&data())
                    .await
                    .expect("the warm bucket is removed");
            })
        });

        let located = located(&root_path);
        let args = Args::default();
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the downgrade answers with a report");

        assert_eq!(
            code,
            1,
            "a cold-only bucket refuses the downgrade: {:?}",
            problems(&report)
        );
        assert!(
            report.findings().iter().any(|finding| {
                finding.check == UNSETTLED
                    && finding.severity == Severity::Error
                    && finding.detail.contains("only.bin")
                    && finding.detail.contains("cold-plane key")
            }),
            "the cold-only key is walked and names the count: {:?}",
            report.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "only.bin"),
            Some(5),
            "the refusal leaves the only copy intact"
        );
        assert!(
            is_tier_root(&root_path).expect("the seam answers"),
            "the tier records survive the refusal"
        );
        assert!(
            !object_file(&root_path, "only.bin").exists(),
            "nothing was promoted"
        );
    }

    /// T2/T28: a `pg/live` placement whose row is gone is not a promotion candidate. The
    /// promotion sequence reads a missing row as a deleted object, retires the placement and
    /// drops the cold copy — the only copy left — so the scan must refuse before anything is
    /// touched and leave the placement, the cold copy, the index and the marker in place.
    #[tokio::test]
    async fn prepare_downgrade_refuses_a_cold_placement_with_no_row() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "gone.bin", b"hello").await;
                assert!(demote(&tier, "gone.bin").await, "gone.bin is cold");
                // Delete the row (and the leftover file) through the warm plane alone: the
                // placement and the cold copy survive, which is the shape the scan refuses.
                tier.fs()
                    .delete_object(&data(), &key("gone.bin"))
                    .await
                    .expect("the row is deleted out from under the placement");
            })
        });

        let located = located(&root_path);
        let args = Args::default();
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the downgrade answers with a report");

        assert_eq!(
            code,
            1,
            "a cold placement with no row refuses the downgrade: {:?}",
            problems(&report)
        );
        assert!(
            report
                .findings()
                .iter()
                .any(|finding| finding.check == UNSETTLED
                    && finding.detail.contains("gone.bin")
                    && finding.detail.contains("no row")),
            "{:?}",
            report.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "gone.bin"),
            Some(5),
            "the refusal leaves the cold copy intact"
        );
        assert!(
            is_tier_root(&root_path).expect("the seam answers"),
            "the tier records survive the refusal"
        );
        assert!(index_path(&root_path).exists(), "the index is untouched");
        assert!(marker_path(&root_path).exists(), "the marker is untouched");
        assert!(
            !object_file(&root_path, "gone.bin").exists(),
            "nothing was promoted"
        );
    }
}
