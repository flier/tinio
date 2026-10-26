//! `tinio tier repair` — rebuild the placement index from the cold plane and the
//! filesystem rows (T2, T18).
//!
//! **The rebuild is a match, never a guess.** Every key the database holds is
//! matched with the row the filesystem plane records for it: a row with no file
//! is a cold object, a row with a file is warm and the database's copy for it is
//! a reported orphan, and a key with no row is unadoptable — no row is ever
//! synthesized from content alone (T2).
//!
//! **`--adopt` is the bind, not a bypass.** It is the operator's statement that
//! a fresh index belongs to the database it found; the layer still refuses a
//! root-ID mismatch, and a marker that disagrees with the index is never
//! reconciled silently (T18). `--delete-orphans` is the run's one destructive
//! flag: without it an orphan is reported and left exactly where it is, and the
//! quarantined writes a failed batch flush left under `.tinio/quarantine/` are
//! reported and left too. With it the orphan copies are removed and the
//! quarantine is cleared, and the report names how many pairs went.

use std::process::ExitCode;

use clap::Args as ClapArgs;

use super::{DirArgs, PAGE, buckets, cold_error, each_page, has_file, open};
#[cfg(test)]
use crate::_util::testing::metrics::test_lock;
use crate::{
    _core::{
        AccessMode,
        content::{ContentKey, ObjectContent},
    },
    _tier::{ColdInfo, ColdStore, Placement, State, Tier, TierStorage},
    _util::metrics::record_tier_orphans,
    Error,
    report::{Finding, Report, Severity},
    resolve::Located,
};

/// The row a rebuilt placement earns.
const REBUILT: &str = "placement rebuilt";
/// The row an unreferenced cold copy earns.
const ORPHAN: &str = "orphan";
/// The row a database key with no filesystem row earns.
const UNADOPTABLE: &str = "unadoptable";
/// The row the quarantined writes earn.
const QUARANTINE: &str = "quarantine";
/// The row the run's summary earns.
const SUMMARY: &str = "tier root";

/// `tinio tier repair`: the root and the run's two switches.
///
/// # Examples
///
/// ```rust
/// use clap::{CommandFactory, FromArgMatches};
/// use tinio_cli::cli::{Cli, Command};
///
/// let matches = Cli::command()
///     .try_get_matches_from(["tinio", "tier", "repair", "/srv/data", "--adopt"])
///     .expect("parse");
/// let cli = Cli::from_arg_matches(&matches).expect("from matches");
/// let Command::Tier(args) = cli.command else {
///     panic!("the tier arm");
/// };
/// let tinio_cli::commands::tier::Verb::Repair(repair) = args.verb else {
///     panic!("the repair verb");
/// };
/// assert!(repair.adopt);
/// assert!(!repair.delete_orphans);
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct Args {
    /// The root the tier serves.
    #[command(flatten)]
    pub dir: DirArgs,
    /// Bind this root to a cold database that already holds data. The layer
    /// still refuses a database whose recorded root identity differs from this
    /// root's (T18).
    #[arg(long)]
    pub adopt: bool,
    /// Remove the cold copies a warm object makes unreferenced. The only
    /// destructive flag this command has: without it every orphan is reported
    /// and left in place.
    #[arg(long)]
    pub delete_orphans: bool,
}

/// Runs the verb: the report on stdout, the exit code its findings earn.
///
/// # Errors
///
/// The root, the configuration, the dsn or the layer's open refusal — a lost
/// index without `--adopt`, a root-ID mismatch, a second writer.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::tier::{repair, repair::Args},
///     resolve::locate,
/// };
///
/// # async fn repair_a_root() -> Result<(), tinio_cli::Error> {
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
/// let code = repair::run(Args::default(), located).await?;
/// // 0 when the rebuild was clean; 1 when a finding was reported.
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
    name = "tier.repair",
    level = "info",
    skip_all,
    fields(adopt = args.adopt, delete_orphans = args.delete_orphans)
)]
pub(crate) async fn execute(args: Args, located: Located) -> Result<(Report, u8), Error> {
    let opened = open(args.dir, located, AccessMode::ReadWrite, args.adopt).await?;
    let report = rebuild(&opened.tier, args.delete_orphans).await?;
    let code = report.exit_code();
    Ok((report, code))
}

/// The rebuild: every key the database holds, matched with the filesystem
/// plane's row for it.
///
/// **No row is synthesized.** A key with no row is unadoptable and left in
/// place, because the cold content alone cannot name an object's metadata
/// (T2). A row with a file is warm and the database's copy is an orphan; a row
/// with no file is cold, and its placement's cold metadata is the row's own.
///
/// **The orphan's deletion is the flag's alone** (T12): without
/// `--delete-orphans` the copy is a finding and stays; with it the copy is
/// removed and the row is informational, so a completed cleanup exits 0.
#[tracing::instrument(
    target = "tinio::tier",
    name = "tier.repair.rebuild",
    level = "debug",
    skip_all,
    fields(delete_orphans = delete_orphans)
)]
pub(crate) async fn rebuild(
    tier: &TierStorage<ColdStore>,
    delete_orphans: bool,
) -> Result<Report, Error> {
    let mut report = Report::default();
    let rows = tier.fs().tier_rows();
    let mut rebuilt = 0usize;
    let mut orphans = 0usize;
    let mut unadoptable = 0usize;
    let mut removed = 0usize;

    for bucket in buckets(tier).await? {
        each_page(
            async |after| {
                tier.cold()
                    .scan(&bucket, "", after, PAGE)
                    .await
                    .map_err(cold_error)
            },
            |entry| entry.key.clone(),
            async |entry| {
                let key = entry.key;
                let Some(row) = rows.info(&bucket, &key).await? else {
                    // No row: the content cannot name an object, so no placement is invented.
                    unadoptable += 1;
                    report.push(Finding::check(
                        UNADOPTABLE,
                        Severity::Error,
                        format!(
                            "{bucket}/{key}: the database holds content the filesystem plane has \
                             no row for; no placement can be rebuilt from content alone (left in \
                             place)"
                        ),
                    ));
                    return Ok(());
                };
                let warm = has_file(tier, &bucket, &key).await?;
                let placement = if warm {
                    Placement {
                        tier: Tier::Fs,
                        state: State::Live,
                        cold: None,
                        mtime: row.last_modified,
                    }
                } else {
                    Placement {
                        tier: Tier::Pg,
                        state: State::Live,
                        cold: Some(ColdInfo::from_info(&row)),
                        mtime: row.last_modified,
                    }
                };
                tier.index().set_placement(&bucket, &key, &placement)?;
                rebuilt += 1;
                report.push(Finding::check(
                    REBUILT,
                    Severity::Ok,
                    format!(
                        "{bucket}/{key}: {} placement",
                        if warm { "warm" } else { "cold" }
                    ),
                ));
                if !warm {
                    return Ok(());
                }
                orphans += 1;
                if delete_orphans {
                    tier.cold()
                        .remove(&bucket, &ContentKey::Object(key.clone()))
                        .await
                        .map_err(cold_error)?;
                    removed += 1;
                    report.push(Finding::check(
                        ORPHAN,
                        Severity::Ok,
                        format!(
                            "{bucket}/{key}: the database's copy is an orphan (the object is \
                             warm); removed under `--delete-orphans`"
                        ),
                    ));
                } else {
                    report.push(Finding::check(
                        ORPHAN,
                        Severity::Warn,
                        format!(
                            "{bucket}/{key}: the database's copy is an orphan (the object is \
                             warm); rerun with `--delete-orphans` to remove it"
                        ),
                    ));
                }
                Ok(())
            },
        )
        .await?;
    }

    // **The quarantine is the run's other disposable residue.** A failed batch
    // flush's pair sits outside both planes, so no placement rebuild reaches it;
    // only the destructive flag clears it, and a report-only run lists it and
    // leaves every file where it is.
    let quarantined = tier.quarantined()?;
    let cleared = if delete_orphans {
        tier.clear_quarantine()?
    } else {
        0
    };
    if delete_orphans {
        report.push(Finding::check(
            QUARANTINE,
            Severity::Ok,
            format!("{cleared} quarantined write(s) removed"),
        ));
    } else if !quarantined.is_empty() {
        report.push(Finding::check(
            QUARANTINE,
            Severity::Warn,
            format!(
                "{} quarantined write(s) left in place; rerun with `--delete-orphans` to remove \
                 them",
                quarantined.len()
            ),
        ));
    }

    // The gauge is the absolute count this pass observed, so a served
    // `/metrics` reflects the last repair — including a clean run's zero.
    #[cfg(test)]
    let _gate = test_lock::writer();
    record_tier_orphans(orphans as u64);

    let detail = if removed > 0 {
        format!(
            "{rebuilt} placement(s) rebuilt from the database's keys; {orphans} orphan copy(ies), \
             {unadoptable} unadoptable, {removed} orphan copy(ies) removed"
        )
    } else {
        format!(
            "{rebuilt} placement(s) rebuilt from the database's keys; {orphans} orphan copy(ies), \
             {unadoptable} unadoptable"
        )
    };
    report.push(Finding::check(SUMMARY, Severity::Ok, detail));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use tinio_store_pg::test_support::{TestDb, require_dsn};
    use tinio_tier::{index_path, marker_path};
    use tinio_util::testing::metrics::{gauge, test_lock};

    use super::*;
    use crate::{
        _core::{
            ETag,
            storage::{BucketOps, ObjectOps},
        },
        commands::tier::tests::{
            cold_stat, create_bucket, data, demote, index_root_id, key, located, marker_root_id,
            object_file, on_runtime, pg_root_recorded, placement_of, problems, put, seed_root,
            with_tier,
        },
    };

    /// A root identity no database ever recorded.
    const FOREIGN: [u8; 16] = *b"fedcba9876543210";

    /// The run's answer for `root`.
    async fn repair_on_runtime(
        root: &Path,
        adopt: bool,
        delete_orphans: bool,
    ) -> Result<(Report, u8), Error> {
        let located = located(root);
        let args = Args {
            adopt,
            delete_orphans,
            ..Args::default()
        };
        on_runtime(move || Box::pin(async move { execute(args, located).await }))
    }

    /// A settled cold object: demoted, with the leftover file garbage
    /// collection removes removed, so the row records an object with no file.
    fn settle_cold(root: &Path, dsn: &str, name: &str) {
        let root = root.to_path_buf();
        let file_root = root.clone();
        let name = name.to_owned();
        with_tier(&root, dsn, move |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, &name, b"hello").await;
                assert!(demote(&tier, &name).await, "the fixture object is cold");
                fs::remove_file(object_file(&file_root, &name))
                    .expect("the leftover file is removed");
            })
        });
    }

    /// T2: the rebuild matches the database's keys with the filesystem rows — a
    /// row with no file is cold, a row with a file is warm — and records no row
    /// of its own.
    #[tokio::test]
    async fn repair_rebuilds_from_rows() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        settle_cold(root.path(), &db.dsn, "cold.bin");
        let root_path = root.path().to_path_buf();
        with_tier(&root_path, &db.dsn, |tier| {
            Box::pin(async move {
                put(&tier, "warm.bin", b"warm!").await;
            })
        });

        // The index is lost; the marker survives, and `--adopt` binds the fresh
        // index to the database the marker names.
        fs::remove_file(index_path(&root_path)).expect("the index is lost");

        let (report, code) = repair_on_runtime(&root_path, true, false)
            .await
            .expect("the rebuild runs");
        assert_eq!(code, 0, "a clean rebuild: {:?}", problems(&report));
        assert!(
            report
                .findings()
                .iter()
                .any(|finding| finding.check == REBUILT),
            "{:?}",
            report.findings()
        );

        let cold = placement_of(&root_path, "cold.bin")
            .await
            .expect("the cold placement is rebuilt");
        assert_eq!(cold.tier, Tier::Pg);
        assert_eq!(cold.state, State::Live);
        assert_eq!(
            cold.cold.as_ref().map(|info| info.etag.as_str()),
            Some(ETag::from_content(b"hello").to_string().as_str()),
            "the cold metadata is the row's own"
        );

        // A key the database holds nothing for needs no placement: the rebuild records
        // what the cold plane proves and invents nothing (T2's "no synthetic rows").
        assert_eq!(
            placement_of(&root_path, "warm.bin").await,
            None,
            "a warm object with no cold content is left placement-free"
        );
        assert!(
            object_file(&root_path, "warm.bin").is_file(),
            "the warm object is untouched"
        );

        assert_eq!(
            index_root_id(&root_path),
            marker_root_id(&root_path),
            "the adopted index records the marker's root identity"
        );
    }

    /// T2/T18: a bucket whose warm counterpart was removed but whose cold content
    /// survives is invisible to the fs bucket listing. The enumeration unions the
    /// cold plane's bucket rows, so the rebuild reaches the cold-only key and
    /// reports it unadoptable (no row can be synthesized from content alone).
    #[tokio::test]
    async fn repair_reports_a_cold_only_bucket() {
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

        let (report, code) = repair_on_runtime(&root_path, false, false)
            .await
            .expect("the rebuild runs");

        assert_eq!(
            code,
            1,
            "a cold-only key is a finding: {:?}",
            problems(&report)
        );
        assert!(
            report.findings().iter().any(|finding| {
                finding.check == UNADOPTABLE
                    && finding.severity == Severity::Error
                    && finding.detail.contains("only.bin")
            }),
            "the cold-only key is reached and reported: {:?}",
            report.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "only.bin"),
            Some(5),
            "the report leaves the only copy in place"
        );
    }

    /// T18: a lost index refuses until `--adopt`, `--adopt` refuses a root-ID
    /// mismatch, and it binds when the identities agree.
    #[tokio::test]
    async fn repair_adopt_requires_the_root_id() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        settle_cold(root.path(), &db.dsn, "cold.bin");
        let root_path = root.path().to_path_buf();
        let recorded =
            marker_root_id(&root_path).expect("the construction recorded the root identity");

        // A fresh index over the non-empty database is refused without the
        // operator's statement — and the refusal names the flag. The marker survives,
        // so the root is still a tier root and the layer's own bind is what refuses
        // (the both-records-lost case is `a_writable_verb_refuses_a_plain_root`).
        fs::remove_file(index_path(&root_path)).expect("the index is lost");
        let refused = repair_on_runtime(&root_path, false, false)
            .await
            .expect_err("a fresh index over a non-empty database is refused");
        assert!(
            refused.to_string().contains("--adopt"),
            "the refusal names the flag: {refused}"
        );

        // A pair recording another root's identity is refused even with
        // `--adopt`: the rule is the match, not the flag.
        fs::remove_file(index_path(&root_path)).expect("the index is lost");
        fs::write(marker_path(&root_path), FOREIGN).expect("a foreign marker");
        tinio_tier::Index::open(index_path(&root_path))
            .expect("the foreign index opens")
            .set_root_id(FOREIGN)
            .expect("the foreign identity is recorded");
        let mismatched = repair_on_runtime(&root_path, true, false)
            .await
            .expect_err("a root-ID mismatch is refused");
        assert!(
            mismatched.to_string().contains("root-ID mismatch"),
            "the refusal names the mismatch: {mismatched}"
        );

        // The same run with the root's own identity binds and rebuilds.
        fs::remove_file(index_path(&root_path)).expect("the foreign index is removed");
        fs::write(marker_path(&root_path), recorded).expect("the root's own marker");
        let (report, code) = repair_on_runtime(&root_path, true, false)
            .await
            .expect("the adopt binds");
        assert_eq!(code, 0, "{:?}", problems(&report));
        assert_eq!(
            index_root_id(&root_path),
            Some(recorded),
            "the index records the database's root identity"
        );
        assert!(
            placement_of(&root_path, "cold.bin").await.is_some(),
            "the placements are rebuilt after the bind"
        );
    }

    /// T12/T18: an orphan is reported either way, and only `--delete-orphans`
    /// removes it — the one destructive action, opt-in per run.
    #[tokio::test]
    async fn repair_delete_orphans_is_opt_in() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        // A demote leaves the warm file behind for garbage collection, so the
        // row has a file and the database has a copy: the orphan shape.
        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "warm.bin", b"hello").await;
                assert!(
                    demote(&tier, "warm.bin").await,
                    "the fixture object is cold"
                );
            })
        });
        assert!(
            object_file(&root_path, "warm.bin").is_file(),
            "the demote leaves the file behind"
        );

        // Without the flag: reported, and the copy stays exactly where it is.
        let (reported, code) = repair_on_runtime(&root_path, false, false)
            .await
            .expect("the rebuild runs");
        assert_eq!(
            code,
            1,
            "the orphan is a finding: {:?}",
            problems(&reported)
        );
        assert!(
            reported
                .findings()
                .iter()
                .any(|finding| finding.check == ORPHAN && finding.detail.contains("warm.bin")),
            "{:?}",
            reported.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "warm.bin"),
            Some(5),
            "the orphan survives without the flag"
        );
        assert_eq!(
            placement_of(&root_path, "warm.bin")
                .await
                .map(|placement| placement.tier),
            Some(Tier::Fs),
            "the object is warm, so the rebuilt placement says so"
        );

        // With the flag: the copy goes, and the run's report says so at `ok` — the
        // finding the plain run left is what the flag resolved.
        let (deleted, code) = repair_on_runtime(&root_path, false, true)
            .await
            .expect("the destructive run runs");
        assert_eq!(
            code,
            0,
            "the resolved orphan is no finding: {:?}",
            problems(&deleted)
        );
        assert!(
            deleted
                .findings()
                .iter()
                .any(|finding| finding.check == ORPHAN && finding.detail.contains("warm.bin")),
            "{:?}",
            deleted.findings()
        );
        assert_eq!(
            cold_stat(&dsn, "warm.bin"),
            None,
            "`--delete-orphans` is the action that removes it"
        );
        assert!(
            object_file(&root_path, "warm.bin").is_file(),
            "the warm object itself is untouched"
        );
    }

    /// The rebuild books the absolute orphan count it observed on the gauge,
    /// so a served `/metrics` reflects the last repair run.
    #[tokio::test]
    async fn repair_books_the_orphan_count_it_observed() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        // A demote leaves the warm file behind, so the row has a file and the
        // database has a copy: the orphan shape the rebuild reports.
        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "orphan.bin", b"hello").await;
                assert!(
                    demote(&tier, "orphan.bin").await,
                    "the fixture object is cold"
                );
            })
        });

        let located = located(&root_path);
        let (report, code, orphans) = on_runtime(move || {
            Box::pin(async move {
                let _window = test_lock::window();
                let (report, code) = execute(Args::default(), located)
                    .await
                    .expect("the rebuild runs");
                (report, code, gauge("tinio_tier_orphans", &[]))
            })
        });
        assert_eq!(code, 1, "the orphan is a finding: {:?}", problems(&report));
        assert_eq!(
            orphans, 1.0,
            "the gauge reports the absolute count the run observed"
        );
    }

    /// T17: a writable verb must not convert a plain root. `TierStorage::open_pg`
    /// records the marker and creates the index **before** it binds, so without the guard
    /// a repair of an ordinary directory would mint a tier root — after which every
    /// non-tier start and `doctor` refuses it — and the empty database would gain a root
    /// identity as a side effect.
    #[tokio::test]
    async fn a_writable_verb_refuses_a_plain_root() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();

        assert!(
            !pg_root_recorded(&db.dsn),
            "the fixture database records no root identity"
        );

        let refused = repair_on_runtime(&root_path, false, false)
            .await
            .expect_err("a plain root is not converted");
        assert!(
            matches!(refused, Error::Usage(_)),
            "a usage refusal: {refused}"
        );
        assert!(
            refused.to_string().contains("not a tier root"),
            "the refusal names the reason: {refused}"
        );

        assert!(!index_path(&root_path).exists(), "no index is created");
        assert!(!marker_path(&root_path).exists(), "no marker is recorded");
        assert!(
            !pg_root_recorded(&db.dsn),
            "no root identity reaches the database"
        );
    }
}
