//! `tinio tier verify` — the report-only reconciliation of a tier root (T12).
//!
//! The pass reads the index, the warm rows and the cold plane and reports what
//! disagrees; it writes nothing. A clean root answers one informational row and
//! exit 0, and any finding is exit 1. A settled disagreement is an error — a
//! missing or short cold copy, a row that is not the cold metadata's mirror, a
//! settled placement with no row, a cold row with no file and no placement, a
//! content digest that does not match (under `--deep`). A transient state the
//! collection pass settles is a warning: a `deleting` tombstone, a `copying`
//! still inside its grace, a warm object's unreferenced copy. The one `copying`
//! the pass cannot decide from the planes' own records stays an error. A write
//! a failed batch flush quarantined is a warning too: its bytes sit outside both
//! planes, so the report names them and the one remedy that applies.

use std::{collections::HashSet, process::ExitCode, time::SystemTime};

use clap::Args as ClapArgs;

use super::{
    DirArgs, PAGE, buckets, cold_digest, cold_error, each_page, has_file, open, single_digest,
    warm_digest,
};
#[cfg(test)]
use crate::_util::testing::metrics::test_lock;
use crate::{
    _core::{AccessMode, bucket, content::ObjectContent, object},
    _tier::{ColdInfo, ColdStore, Placement, State, Tier, TierStorage},
    _util::{metrics::record_tier_orphans, time::civil_from_days},
    Error,
    report::{Finding, Report, Severity},
    resolve::Located,
};

/// The row a clean summary carries.
const SUMMARY: &str = "tier root";
/// The row cold content's presence and length earn.
const COLD_COPY: &str = "cold copy";
/// The row a filesystem row that is not the cold metadata's mirror earns.
const COLD_METADATA: &str = "cold metadata";
/// The row the warm file's presence earns.
const WARM_CONTENT: &str = "warm content";
/// The row a deep content digest earns.
const CONTENT_DIGEST: &str = "content digest";
/// The row a placement's lifecycle or its missing row earns.
const PLACEMENT: &str = "placement";
/// The row cold content no placement references earns.
const ORPHAN: &str = "orphan";
/// The row a write a failed batch flush quarantined earns.
const QUARANTINE: &str = "quarantine";

/// `tinio tier verify`: the root plus the deep switch.
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
/// let Command::Tier(args) = cli.command else {
///     panic!("the tier arm");
/// };
/// let tinio_cli::commands::tier::Verb::Verify(verify) = args.verb else {
///     panic!("the verify verb");
/// };
/// assert!(verify.deep);
/// ```
#[derive(Debug, Default, ClapArgs)]
pub struct Args {
    /// The root the tier serves.
    #[command(flatten)]
    pub dir: DirArgs,
    /// Hash the content: the database's bytes for a cold object, the file's for
    /// a warm one. A composed multipart ETag stays unverified — the retained
    /// part rows keep no part digests to recompose it from.
    #[arg(long)]
    pub deep: bool,
}

/// Runs the verb: the report on stdout, the exit code its findings earn.
///
/// # Errors
///
/// The root, the configuration, the dsn or the tier layer's open refusal.
///
/// # Examples
///
/// ```no_run
/// use tinio_cli::{
///     cli::{Discovery, Provenance},
///     commands::tier::{verify, verify::Args},
///     resolve::locate,
/// };
///
/// # async fn verify_a_root() -> Result<(), tinio_cli::Error> {
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
/// let code = verify::run(Args::default(), located).await?;
/// // 0 when nothing was found; 1 when a finding was reported.
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
    name = "tier.verify",
    level = "info",
    skip_all,
    fields(deep = args.deep)
)]
pub(crate) async fn execute(args: Args, located: Located) -> Result<(Report, u8), Error> {
    let opened = open(args.dir, located, AccessMode::ReadOnly, false).await?;
    let report = inspect(&opened.tier, args.deep, SystemTime::now()).await?;
    let code = report.exit_code();
    Ok((report, code))
}

/// The reconciliation over an opened tier.
///
/// The pass walks three sources and reconciles them: the filesystem rows (one
/// object each), the placements (each one's lifecycle and, where a row exists,
/// its match), and the cold plane's keys (each one's placement, or its
/// orphanhood). A settled disagreement is an error; a transient state the
/// collection pass finishes is a warning, and its own report — the aged
/// `copying` placements it cannot decide — keeps those at an error.
#[tracing::instrument(
    target = "tinio::tier",
    name = "tier.verify.scan",
    level = "debug",
    skip_all,
    fields(deep = deep)
)]
pub(crate) async fn inspect(
    tier: &TierStorage<ColdStore>,
    deep: bool,
    now: SystemTime,
) -> Result<Report, Error> {
    let unresolved: HashSet<(String, String)> = tier
        .verify(now)
        .await?
        .unresolved_copying
        .into_iter()
        .map(|(bucket, key)| (bucket.as_ref().to_owned(), key.as_ref().to_owned()))
        .collect();
    let mut check = Check {
        tier,
        deep,
        unresolved,
        report: Report::default(),
        cold: 0,
        warm: 0,
        orphans: 0,
    };
    for bucket in buckets(tier).await? {
        check.bucket(&bucket).await?;
    }
    // **Every quarantined write is visible to the operator here**, because a
    // failed batch flush's bytes sit outside both planes and no other verb reads
    // them. The advice follows what the planes actually hold: only a cold copy
    // with a matching row is something `tinio tier repair` can settle.
    check.quarantine().await?;
    // The family is a gauge: it reports the orphan count this run found, so a
    // clean root reports zero rather than leaving a stale value behind.
    record_orphans(check.orphans);
    let detail = if deep {
        format!(
            "{} cold and {} warm object(s) checked; content hashed",
            check.cold, check.warm
        )
    } else {
        format!(
            "{} cold and {} warm object(s) checked",
            check.cold, check.warm
        )
    };
    check
        .report
        .push(Finding::check(SUMMARY, Severity::Ok, detail));
    Ok(check.report)
}

/// One bucket's reconciliation, and the findings it produced.
struct Check<'a> {
    /// The tier whose planes are read.
    tier: &'a TierStorage<ColdStore>,
    /// Whether content is hashed.
    deep: bool,
    /// The `(bucket, key)` pairs the collection pass cannot settle (T12).
    unresolved: HashSet<(String, String)>,
    /// The findings, in the order they were found.
    report: Report,
    /// The cold objects seen, for the summary.
    cold: usize,
    /// The warm objects seen, for the summary.
    warm: usize,
    /// The orphan findings pushed, which the tier orphan gauge reports.
    orphans: usize,
}

impl Check<'_> {
    /// One bucket: its rows, then the placements no row explains, then the cold
    /// copies no placement references.
    async fn bucket(&mut self, bucket: &bucket::Name) -> Result<(), Error> {
        let tier = self.tier;
        let rows = tier.tier_rows();
        let mut seen: HashSet<String> = HashSet::new();
        each_page(
            async |after| {
                Ok(rows
                    .scan(bucket, "", after.map(String::as_str), PAGE)
                    .await?)
            },
            |info| info.key.as_ref().to_owned(),
            async |info| {
                seen.insert(info.key.as_ref().to_owned());
                self.object(bucket, &info).await
            },
        )
        .await?;

        each_page(
            async |after| {
                Ok(tier
                    .index()
                    .scan_placement(bucket, "", after.map(String::as_str), PAGE)?)
            },
            |entry| entry.0.as_ref().to_owned(),
            async |entry| {
                let (key, placement) = entry;
                if seen.contains(key.as_ref()) {
                    return Ok(());
                }
                // **A settled placement with no row is the inconsistency; a transient one is the
                // collection pass's.** `finish_tombstone` finishes a `Deleting` over a gone row,
                // and `resolve_copying` discards a `Copying` whose row is gone — both settle
                // after the grace, so neither is an error. The one `Copying` the pass cannot
                // decide (no cold snapshot, grace elapsed) stays an error.
                let (severity, detail) = match placement.state {
                    State::Live => (
                        Severity::Error,
                        format!(
                            "{bucket}/{key}: the index records a {} placement but the filesystem \
                             plane has no row for the object",
                            tier_name(placement.tier)
                        ),
                    ),
                    State::Deleting => (
                        Severity::Warn,
                        format!(
                            "{bucket}/{key}: a {} delete tombstone awaits garbage collection; \
                             the object has no row",
                            tier_name(placement.tier)
                        ),
                    ),
                    State::Copying => {
                        let undecided = self
                            .unresolved
                            .contains(&(bucket.as_ref().to_owned(), key.as_ref().to_owned()));
                        let (severity, note) = if undecided {
                            (
                                Severity::Error,
                                "; the collection pass cannot decide this one from the planes' own \
                                 records",
                            )
                        } else {
                            (
                                Severity::Warn,
                                "; the collection pass settles it after the grace",
                            )
                        };
                        (
                            severity,
                            format!(
                                "{bucket}/{key}: a {} copy's row is gone{note}",
                                tier_name(placement.tier)
                            ),
                        )
                    }
                };
                self.report
                    .push(Finding::check(PLACEMENT, severity, detail));
                Ok(())
            },
        )
        .await?;

        each_page(
            async |after| {
                tier.cold()
                    .scan(bucket, "", after, PAGE)
                    .await
                    .map_err(cold_error)
            },
            |entry| entry.key.clone(),
            async |entry| self.orphan(bucket, &entry.key, &seen).await,
        )
        .await?;
        Ok(())
    }

    /// One row's object, by the placement that governs it.
    async fn object(&mut self, bucket: &bucket::Name, info: &object::Info) -> Result<(), Error> {
        let placement = self.tier.index().placement(bucket, &info.key)?;
        match placement {
            Some(placement) if placement.state != State::Live => {
                let key = info.key.as_ref().to_owned();
                let undecided = self
                    .unresolved
                    .contains(&(bucket.as_ref().to_owned(), key.clone()));
                let severity = if undecided {
                    Severity::Error
                } else {
                    Severity::Warn
                };
                let note = if undecided {
                    "; the collection pass cannot decide this one from the planes' own records"
                } else if placement.state == State::Deleting {
                    // `finish_tombstone` deliberately does nothing while the row survives: the
                    // tombstone is the delete's own rollback to settle, not the collection
                    // pass's, so the report must not promise that the pass settles it.
                    "; the row survives, so the collection pass leaves it to the delete's own \
                     rollback — repeat the delete to settle it"
                } else {
                    "; the collection pass settles it after the grace"
                };
                self.report.push(Finding::check(
                    PLACEMENT,
                    severity,
                    format!(
                        "{bucket}/{key}: a {} placement{note}",
                        placement.state.as_str()
                    ),
                ));
            }
            Some(placement) if placement.tier == Tier::Pg => {
                self.cold_object(bucket, info, &placement).await?;
            }
            _ => self.warm_object(bucket, info).await?,
        }
        Ok(())
    }

    /// A cold object: the row mirror, the database's copy, and — deep — its bytes.
    async fn cold_object(
        &mut self,
        bucket: &bucket::Name,
        info: &object::Info,
        placement: &Placement,
    ) -> Result<(), Error> {
        self.cold += 1;
        let key = &info.key;
        let Some(cold) = placement.cold.as_ref() else {
            self.report.push(Finding::check(
                COLD_METADATA,
                Severity::Error,
                format!("{bucket}/{key}: the placement is cold but carries no cold metadata"),
            ));
            return Ok(());
        };

        // T2: `ColdInfo` is the authority and the row is its mirror.
        let mirror = ColdInfo::from_info(info);
        let mut differ = Vec::new();
        if mirror.etag != cold.etag {
            differ.push("etag");
        }
        if mirror.size != cold.size {
            differ.push("size");
        }
        if mirror.last_modified != cold.last_modified {
            differ.push("last modified");
        }
        if mirror.tags != cold.tags {
            differ.push("tags");
        }
        if mirror.checksum != cold.checksum {
            differ.push("checksum");
        }
        if mirror.owner != cold.owner {
            differ.push("owner");
        }
        if mirror.acl != cold.acl {
            differ.push("acl");
        }
        if mirror.version_id != cold.version_id {
            differ.push("version identity");
        }
        if mirror.bucket_records_versions != cold.bucket_records_versions {
            differ.push("versioning flag");
        }
        if !differ.is_empty() {
            self.report.push(Finding::check(
                COLD_METADATA,
                Severity::Error,
                format!(
                    "{bucket}/{key}: the filesystem row is not the cold metadata's mirror \
                     ({})",
                    differ.join(", ")
                ),
            ));
        }

        // The copy the placement promises: present, and the recorded length.
        let present = match self
            .tier
            .cold()
            .stat(bucket, key)
            .await
            .map_err(cold_error)?
        {
            None => {
                self.report.push(Finding::check(
                    COLD_COPY,
                    Severity::Error,
                    format!(
                        "{bucket}/{key}: the index places the object cold but the database holds \
                         no content"
                    ),
                ));
                false
            }
            Some(entry) if entry.len != cold.size => {
                self.report.push(Finding::check(
                    COLD_COPY,
                    Severity::Error,
                    format!(
                        "{bucket}/{key}: the database holds {} bytes but the cold metadata records \
                         {}",
                        entry.len, cold.size
                    ),
                ));
                true
            }
            Some(_) => true,
        };

        // **An absent copy is already the one finding; a digest of nothing would
        // be a second error for the same key.** The deep pass hashes only a copy
        // the placement can actually hand it.
        if self.deep && present {
            self.deep_cold(bucket, key, &cold.etag).await?;
        }
        Ok(())
    }

    /// A cold object's bytes, hashed from the database.
    async fn deep_cold(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &str,
    ) -> Result<(), Error> {
        let tier = self.tier;
        self.deep_digest(
            bucket,
            key,
            etag,
            "the database's content",
            "ETag",
            async || cold_digest(tier, bucket, key).await,
        )
        .await
    }

    /// Compare one copy's bytes against the ETag a record carries.
    ///
    /// **The deep pass has one comparison**: a composed multipart ETag is
    /// unverified, and a single one either matches the copy's digest or earns
    /// the digest finding — while an unreadable copy earns the hash finding.
    /// `subject` names where the bytes came from and `authority` the record the
    /// ETag came from, so each caller's sentence stays its own.
    ///
    /// `digest` is only called once the ETag is known to be a single-part one,
    /// so a composed ETag is never hashed.
    async fn deep_digest<F>(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        etag: &str,
        subject: &str,
        authority: &str,
        digest: F,
    ) -> Result<(), Error>
    where
        F: AsyncFnOnce() -> Result<[u8; 16], Error>,
    {
        let Some(expected) = single_digest(etag) else {
            self.report.push(unverified_composed(bucket, key));
            return Ok(());
        };
        match digest().await {
            Ok(actual) if actual == expected => {}
            Ok(actual) => self.report.push(Finding::check(
                CONTENT_DIGEST,
                Severity::Error,
                format!(
                    "{bucket}/{key}: {subject} hashes to {} but the {authority} records {}",
                    hex::encode(actual),
                    hex::encode(expected)
                ),
            )),
            Err(err) => self.report.push(Finding::check(
                CONTENT_DIGEST,
                Severity::Error,
                format!("{bucket}/{key}: {subject} could not be hashed: {err}"),
            )),
        }
        Ok(())
    }

    /// A warm object: the file, and — deep — its bytes.
    async fn warm_object(
        &mut self,
        bucket: &bucket::Name,
        info: &object::Info,
    ) -> Result<(), Error> {
        self.warm += 1;
        let key = &info.key;
        match has_file(self.tier, bucket, key).await {
            Ok(true) => {}
            Ok(false) => {
                // T2: a row with no file *is* a cold object. One the index does not place
                // cold has lost (or never got) its placement, and nothing can serve it.
                self.report.push(Finding::check(
                    WARM_CONTENT,
                    Severity::Error,
                    format!(
                        "{bucket}/{key}: the row has no file, so the object is cold, but the index \
                         does not place it cold; run `tinio tier repair` to rebuild the placement"
                    ),
                ));
                return Ok(());
            }
            Err(err) => {
                self.report.push(Finding::check(
                    WARM_CONTENT,
                    Severity::Error,
                    format!("{bucket}/{key}: the file could not be inspected: {err}"),
                ));
                return Ok(());
            }
        }
        if !self.deep {
            return Ok(());
        }
        let etag = info.etag.to_string();
        let tier = self.tier;
        self.deep_digest(
            bucket,
            key,
            &etag,
            "the file's content",
            "row",
            async || warm_digest(tier, bucket, key).await,
        )
        .await
    }

    /// Cold content no placement references (T12's reported orphan).
    async fn orphan(
        &mut self,
        bucket: &bucket::Name,
        key: &object::Key,
        seen: &HashSet<String>,
    ) -> Result<(), Error> {
        if self
            .tier
            .index()
            .placement(bucket, key)?
            .is_some_and(|placement| placement.tier == Tier::Pg)
        {
            return Ok(());
        }
        // **The orphan's advice follows what repair actually does.** A warm object's copy is the
        // one repair removes (`--delete-orphans`); a cold row whose placement was lost is one it
        // rebuilds, and the copy there is the object's only content — advising the delete flag
        // would destroy it. A key with no row at all is unadoptable either way (T2).
        let warm = seen.contains(key.as_ref()) && has_file(self.tier, bucket, key).await?;
        let (severity, note) = match (seen.contains(key.as_ref()), warm) {
            (false, _) => (
                Severity::Error,
                "; the filesystem plane has no row for it, so no placement can be rebuilt",
            ),
            (true, true) => (
                Severity::Warn,
                "; the object is warm — `tinio tier repair --delete-orphans` removes the copy",
            ),
            (true, false) => (
                Severity::Error,
                "; the row has no file, so the object is cold with a lost placement; \
                 `tinio tier repair` rebuilds it — the content is the only copy",
            ),
        };
        self.orphans += 1;
        self.report.push(Finding::check(
            ORPHAN,
            severity,
            format!("{bucket}/{key}: the database holds content no placement references{note}"),
        ));
        Ok(())
    }

    /// Every write a failed batch flush quarantined, as a warning with the one
    /// truthful remedy.
    ///
    /// **A quarantined write sits outside both planes.** Its bytes and the
    /// metadata that recovers them are a pair under the root's
    /// `.tinio/quarantine/`, so no placement can be rebuilt from the index and
    /// no read can reach it. The advice therefore follows what the planes hold:
    /// a cold copy *and* its filesystem row is something `tinio tier repair`
    /// rebuilds, while a key with no cold content — or with content but no row —
    /// cannot be settled by repair at all, and the report says so and names the
    /// quarantined bytes.
    async fn quarantine(&mut self) -> Result<(), Error> {
        let root = self.tier.fs().root();
        let directory = root.join(object::RESERVED_SEGMENT).join("quarantine");
        for entry in self.tier.quarantined()? {
            let cold = self
                .tier
                .cold()
                .stat(&entry.bucket, &entry.key)
                .await
                .map_err(cold_error)?;
            let row = self
                .tier
                .tier_rows()
                .info(&entry.bucket, &entry.key)
                .await?;
            let advice = match (cold.is_some(), row.is_some()) {
                (true, true) => {
                    "the cold plane holds a copy and the filesystem plane has the row, \
                                 so `tinio tier repair` rebuilds the placement"
                }
                (true, false) => {
                    "the cold plane holds content but the filesystem plane has no \
                                  row, so `tinio tier repair` reports it unadoptable and cannot \
                                  settle it"
                }
                (false, _) => {
                    "the cold plane holds no content for it, so `tinio tier repair` \
                               cannot settle it"
                }
            };
            self.report.push(Finding::check(
                QUARANTINE,
                Severity::Warn,
                format!(
                    "{}/{}: {} byte(s) quarantined at {} by a failed batch flush; {advice}; the \
                     quarantined bytes under `{}` are the failed write's only record — restore or \
                     discard them by hand, or clear them with `tinio tier repair --delete-orphans`",
                    entry.bucket,
                    entry.key,
                    entry.len,
                    instant(entry.recorded),
                    directory.display(),
                ),
            ));
        }
        Ok(())
    }
}

/// An instant as an RFC3339 UTC string — the report's one time spelling.
///
/// **Written with `std` alone** because this command must compile in a build
/// without the `object-lock` feature (whose `time` dependency is what the
/// retention verbs format through). The civil-date conversion is
/// [`tinio_util::time::civil_from_days`], the workspace's one copy of Howard
/// Hinnant's algorithm (shared with the test signer's SigV4 timestamp).
fn instant(time: SystemTime) -> String {
    let seconds = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0);
    let days = i64::try_from(seconds / 86_400).unwrap_or(i64::MAX);
    let rest = seconds % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rest / 3_600,
        (rest / 60) % 60,
        rest % 60
    )
}

/// Record the orphan count the report carries.
///
/// Fire-and-forget: the metric write cannot change the report or its exit
/// code. The count is per run, so a clean root reports zero. The `verify`
/// verb is a one-shot process, so the value is observable only to an
/// in-process caller (the command exits before any other process could scrape
/// it).
fn record_orphans(count: usize) {
    #[cfg(test)]
    let _gate = test_lock::writer();
    record_tier_orphans(count as u64);
}

/// A placement's plane, as a finding names it.
fn tier_name(tier: Tier) -> &'static str {
    match tier {
        Tier::Fs => "warm",
        Tier::Pg => "cold",
    }
}

/// The row a composed multipart ETag earns under `--deep`.
///
/// The composed ETag is the MD5 over the part digests, and the retained part rows keep
/// sizes and checksums but no part digests — so there is nothing to recompose it from,
/// and the run says so instead of guessing (T12's accepted limit). One helper, so the
/// cold and warm halves cannot word the same limit two ways.
fn unverified_composed(bucket: &bucket::Name, key: &object::Key) -> Finding {
    Finding::check(
        CONTENT_DIGEST,
        Severity::Ok,
        format!(
            "{bucket}/{key}: not verified: a composed multipart ETag (the retained part rows keep \
             no part digests to recompose it from)"
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use tinio_core::content::{ContentKey, stage_body};
    use tinio_store_pg::test_support::{TestDb, require_dsn};
    use tinio_util::testing::{
        body,
        metrics::{gauge, test_lock},
    };

    use super::*;
    use crate::commands::tier::tests::{
        create_bucket, data, demote, key, located, object_file, on_runtime, overwrite_cold,
        problems, put, seed_quarantine, seed_root, with_tier,
    };

    /// The findings under `check`, as the report holds them.
    fn under<'a>(report: &'a Report, check: &'a str) -> Vec<&'a Finding> {
        report
            .findings()
            .iter()
            .filter(|finding| finding.check == check)
            .collect()
    }

    /// Every finding whose detail names `key` — whichever check produced it.
    fn naming<'a>(report: &'a Report, key: &str) -> Vec<&'a Finding> {
        report
            .findings()
            .iter()
            .filter(|finding| finding.detail.contains(key))
            .collect()
    }

    /// A placement that says cold while the database holds nothing is the one
    /// finding a run must never miss (T12): the object is unreadable and
    /// nothing else reports it.
    ///
    /// **The deep run is the one that exercises the guard.** Without the
    /// `cold().stat` skip, `--deep` would add a `CONTENT_DIGEST` error for the
    /// copy it could not hash; a plain run would never reach that path, so the
    /// exactly-one assertion is made against the deep report.
    #[tokio::test]
    async fn verify_reports_a_missing_cold_copy() {
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
                put(&tier, "cold.bin", b"hello").await;
                assert!(
                    demote(&tier, "cold.bin").await,
                    "the fixture object is cold"
                );
                tier.cold()
                    .remove(&data(), &ContentKey::Object(key("cold.bin")))
                    .await
                    .expect("the cold copy is removed under the placement");
            })
        });

        let report = deep_report(&root_path).await;
        assert_eq!(
            report.exit_code(),
            1,
            "a finding is an exit 1: {:?}",
            report.findings()
        );
        assert!(
            under(&report, COLD_COPY).iter().any(|finding| {
                finding.severity == Severity::Error && finding.detail.contains("cold.bin")
            }),
            "{:?}",
            problems(&report)
        );
        // **One key, one finding.** An absent copy is the whole defect; a deep
        // run must not add a second "content could not be hashed" error for it.
        let named = naming(&report, "cold.bin");
        assert_eq!(
            named.len(),
            1,
            "the missing copy is reported once: {:?}",
            problems(&report)
        );
        assert_eq!(named[0].check, COLD_COPY);
    }

    /// A write a failed batch flush quarantined is a warning naming the bucket,
    /// the key, the length and when it was quarantined, and its advice is
    /// truthful: with no cold content, `tinio tier repair` cannot settle it, and
    /// the quarantined bytes are the copy an operator has to handle.
    #[tokio::test]
    async fn verify_reports_a_quarantined_write_with_its_remedy() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        seed_quarantine(&root_path, &db.dsn, "lost.bin", b"lost bytes");

        let (report, code) = verify_on_runtime(&root_path).await;
        assert_eq!(code, 1, "a quarantined write is a finding");
        let quarantined = under(&report, QUARANTINE);
        assert_eq!(
            quarantined.len(),
            1,
            "one pair, one finding: {:?}",
            problems(&report)
        );
        let finding = quarantined[0];
        assert_eq!(finding.severity, Severity::Warn);
        assert!(
            finding.detail.contains("data/lost.bin"),
            "the finding names the bucket and the key: {}",
            finding.detail
        );
        assert!(
            finding.detail.contains("10 byte(s)"),
            "the finding names the quarantined length: {}",
            finding.detail
        );
        assert!(
            finding.detail.contains("quarantined at 20"),
            "the finding names the time it was quarantined: {}",
            finding.detail
        );
        assert!(
            finding.detail.contains("cannot settle it"),
            "with no cold copy, repair cannot settle it: {}",
            finding.detail
        );
        assert!(
            finding.detail.contains("quarantine"),
            "the finding points at the quarantined bytes: {}",
            finding.detail
        );
    }

    /// A composed multipart ETag cannot be recomposed from the retained part rows,
    /// so a deep run reports it as unverified information — never as corruption.
    #[tokio::test]
    async fn verify_deep_reports_a_composed_etag_as_unverified() {
        if !require_dsn() {
            return;
        }
        let db = TestDb::create().await.expect("a scratch database");
        let root = tempfile::TempDir::new().expect("a temporary root");
        seed_root(root.path(), &db.dsn);
        let root_path = root.path().to_path_buf();
        let dsn = db.dsn.clone();

        // The ETag a two-part upload records: the part digests' MD5, then the
        // part count. The row and the cold metadata carry the same value, so the
        // mirror check is clean and the deep pass is what sees the composed form.
        let composed = "0123456789abcdef0123456789abcdef-2";
        with_tier(&root_path, &dsn, move |tier| {
            Box::pin(async move {
                create_bucket(&tier).await;
                put(&tier, "multi.bin", b"hello").await;
                assert!(demote(&tier, "multi.bin").await, "multi.bin is cold");
                let mut placement = tier
                    .index()
                    .placement(&data(), &key("multi.bin"))
                    .expect("the placement reads")
                    .expect("the placement exists");
                placement
                    .cold
                    .as_mut()
                    .expect("the cold metadata is present")
                    .etag = composed.to_owned();
                tier.index()
                    .set_placement(&data(), &key("multi.bin"), &placement)
                    .expect("the composed ETag is recorded");
                let mut info = tier
                    .tier_rows()
                    .info(&data(), &key("multi.bin"))
                    .await
                    .expect("the row reads")
                    .expect("the row exists");
                info.etag = composed.parse().expect("a composed ETag parses");
                tier.tier_rows()
                    .set_row(&data(), &info, info.last_modified)
                    .await
                    .expect("the row's ETag is rewritten");
            })
        });

        let report = deep_report(&root_path).await;
        assert!(
            under(&report, CONTENT_DIGEST).iter().any(|finding| {
                finding.severity == Severity::Ok
                    && finding.detail.contains("multi.bin")
                    && finding.detail.contains("not verified")
            }),
            "a composed ETag is unverified, not corrupt: {:?}",
            problems(&report)
        );
        assert!(
            naming(&report, "multi.bin")
                .iter()
                .all(|finding| finding.severity != Severity::Error),
            "no error is raised for a composed ETag: {:?}",
            problems(&report)
        );
    }

    /// A cold key with neither a filesystem row nor a placement is reported once:
    /// the orphan finding is the whole story, and the report does not repeat it.
    #[tokio::test]
    async fn verify_reports_an_unadoptable_cold_key_once() {
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
                // Cold content with no row and no placement: no verb can adopt it,
                // so it must be reported exactly once (T2).
                let content_key = ContentKey::Object(key("stranded.bin"));
                let mut staged = tier
                    .cold()
                    .stage(&data(), &content_key)
                    .await
                    .expect("the stranded stage opens");
                stage_body(&mut staged, body(b"stranded"))
                    .await
                    .expect("the stranded bytes land");
                tier.cold()
                    .publish(&data(), staged, &content_key)
                    .await
                    .expect("the stranded content is published");
            })
        });

        let (report, code) = verify_on_runtime(&root_path).await;
        let named = naming(&report, "stranded.bin");
        assert_eq!(
            named.len(),
            1,
            "an unadoptable cold key is reported once: {:?}",
            problems(&report)
        );
        assert_eq!(named[0].check, ORPHAN);
        assert_eq!(named[0].severity, Severity::Error);
        assert_eq!(code, 1, "an orphan is a finding");
    }

    /// A deep run hashes the database's bytes for a cold object — never the
    /// leftover file the demote leaves for garbage collection — and the file's
    /// bytes for a warm object.
    #[tokio::test]
    async fn verify_deep_hashes_pg_not_the_fs_file() {
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
                put(&tier, "cold.bin", b"hello").await;
                put(&tier, "warm.bin", b"warm!").await;
                assert!(
                    demote(&tier, "cold.bin").await,
                    "the fixture object is cold"
                );
            })
        });

        // The cold object's file is a leftover garbage collection removes; a
        // deep run must not verify it as the object's content.
        fs::write(object_file(&root_path, "cold.bin"), b"stale").expect("the leftover rewritten");
        let clean = deep_report(&root_path).await;
        assert!(
            problems(&clean).is_empty(),
            "a cold object's leftover file is not its content: {:?}",
            problems(&clean)
        );

        // The database's bytes are what a cold object is verified against.
        with_tier(&root_path, &dsn, |tier| {
            Box::pin(async move {
                overwrite_cold(&tier, "cold.bin", b"world").await;
            })
        });
        let caught = deep_report(&root_path).await;
        assert!(
            under(&caught, CONTENT_DIGEST).iter().any(|finding| {
                finding.severity == Severity::Error && finding.detail.contains("cold.bin")
            }),
            "the database's bytes disagree with the ETag: {:?}",
            problems(&caught)
        );

        // The warm half: a warm object's file is its content.
        fs::write(object_file(&root_path, "warm.bin"), b"other").expect("the warm file rewritten");
        let warm = deep_report(&root_path).await;
        assert!(
            under(&warm, CONTENT_DIGEST).iter().any(|finding| {
                finding.severity == Severity::Error && finding.detail.contains("warm.bin")
            }),
            "a warm object's file is hashed: {:?}",
            problems(&warm)
        );
    }

    /// T12: a `deleting` tombstone whose row is already gone is the collection pass's to
    /// finish, not an error: `finish_tombstone` completes it after the grace. The report says
    /// so at `warn`, and no error is raised for the placement.
    #[tokio::test]
    async fn verify_reports_a_tombstone_with_no_row_as_transient() {
        use tinio_core::storage::ObjectOps as _;

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
                // The row and file go; the placement stays as a delete's tombstone.
                tier.fs()
                    .delete_object(&data(), &key("gone.bin"))
                    .await
                    .expect("the row is deleted under the tombstone");
                tier.index()
                    .set_placement(
                        &data(),
                        &key("gone.bin"),
                        &Placement {
                            tier: Tier::Pg,
                            state: State::Deleting,
                            cold: None,
                            mtime: SystemTime::now(),
                        },
                    )
                    .expect("the tombstone is recorded");
            })
        });

        let (report, _code) = verify_on_runtime(&root_path).await;
        assert!(
            under(&report, PLACEMENT).iter().any(|finding| {
                finding.severity == Severity::Warn
                    && finding.detail.contains("gone.bin")
                    && finding.detail.contains("garbage collection")
            }),
            "a tombstone without a row is transient: {:?}",
            problems(&report)
        );
    }

    /// T12: cold content whose row has no file and whose placement was lost is the object's only
    /// copy. The orphan advice must send the operator to `tinio tier repair`, never to
    /// `--delete-orphans`, and must not call a no-file row "warm".
    #[tokio::test]
    async fn verify_advises_repair_for_a_cold_row_with_a_lost_placement() {
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
                put(&tier, "lost.bin", b"hello").await;
                assert!(demote(&tier, "lost.bin").await, "lost.bin is cold");
                fs::remove_file(object_file(&file_root, "lost.bin"))
                    .expect("the leftover file is removed");
                tier.index()
                    .clear(&data(), &key("lost.bin"))
                    .expect("the placement is lost");
            })
        });

        let (report, code) = verify_on_runtime(&root_path).await;
        let orphans = under(&report, ORPHAN);
        assert!(
            orphans.iter().any(|finding| {
                finding.severity == Severity::Error
                    && finding.detail.contains("lost.bin")
                    && finding.detail.contains("the content is the only copy")
            }),
            "the copy is the only content, so repair rebuilds the placement: {:?}",
            problems(&report)
        );
        assert!(
            orphans
                .iter()
                .all(|finding| !finding.detail.contains("the object is warm")),
            "a row with no file is not warm: {:?}",
            problems(&report)
        );
        assert_eq!(code, 1, "an unplaced cold row is a finding");
    }

    /// The plain report of `root`, as the command computes it.
    async fn verify_on_runtime(root: &Path) -> (Report, u8) {
        let located = located(root);
        let args = Args::default();
        on_runtime(move || Box::pin(async move { execute(args, located).await }))
            .expect("the verification runs")
    }

    /// The deep report of `root`, as the command computes it.
    async fn deep_report(root: &Path) -> Report {
        let located = located(root);
        let args = Args {
            deep: true,
            ..Args::default()
        };
        let (report, code) =
            on_runtime(move || Box::pin(async move { execute(args, located).await }))
                .expect("the verification runs");
        assert_eq!(
            code,
            report.exit_code(),
            "the command returns the report's own code"
        );
        report
    }

    /// The orphan gauge is the count the report carries: one cold copy no
    /// placement references is one finding and one reported orphan.
    #[tokio::test]
    async fn verify_records_the_orphan_count_it_reports() {
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
                // Cold content no row and no placement reference: exactly
                // the orphan the report names.
                let content_key = ContentKey::Object(key("orphan.bin"));
                let mut staged = tier
                    .cold()
                    .stage(&data(), &content_key)
                    .await
                    .expect("the orphan's stage opens");
                stage_body(&mut staged, body(b"orphan"))
                    .await
                    .expect("the orphan's bytes land");
                tier.cold()
                    .publish(&data(), staged, &content_key)
                    .await
                    .expect("the orphan is published");
            })
        });

        let (report, code, orphans) = report_with_orphans(&root_path).await;
        let reported = under(&report, ORPHAN).len();
        assert_eq!(
            reported,
            1,
            "the fixture is one orphan: {:?}",
            problems(&report)
        );
        assert_eq!(code, 1, "an orphan is a finding");
        assert_eq!(
            orphans, reported as f64,
            "the gauge reports the count the run found"
        );
    }

    /// The plain report and the orphan gauge, read inside the test window so a
    /// parallel leg's recording cannot move it.
    async fn report_with_orphans(root: &Path) -> (Report, u8, f64) {
        let located = located(root);
        on_runtime(move || {
            Box::pin(async move {
                let _window = test_lock::window();
                let (report, code) = execute(Args::default(), located)
                    .await
                    .expect("the verification runs");
                (report, code, gauge("tinio_tier_orphans", &[]))
            })
        })
    }
}
