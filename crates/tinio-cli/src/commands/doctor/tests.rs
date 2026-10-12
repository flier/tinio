#![allow(clippy::absolute_paths)]

// Item-scoped like the file's other cfg'd imports: the restore statement below is a bare
// `#[cfg(unix)]` statement outside the block that sets the mode, so an import inside that
// block would not cover it.
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
#[cfg(feature = "pg")]
use std::path::PathBuf;

// Reached directly rather than through the module's `pub use`: the row is `pub(crate)` and
// re-exporting it would put a foreign `_sandbox::Error` in this crate's public API.
use super::checks::{HOST_CONFINEMENT, HOST_CONFINEMENT_OK, host_confinement_finding};
use super::*;
#[cfg(feature = "pg")]
use crate::{
    _config::Config,
    _util::secret::{Secret, SecretKey},
    cli::{Discovery, Provenance},
    resolve::{locate_from, pg},
};
use crate::{
    _sandbox,
    _util::testing::assert_send_sync,
    commands::DirArgs,
    report::{Finding, Severity},
};

impl Args {
    /// An `Args` naming `root` and nothing else.
    pub(crate) fn for_test(root: &Path) -> Self {
        Self {
            dir: DirArgs {
                dir: Some(root.to_path_buf()),
                ..DirArgs::default()
            },
            ..Self::default()
        }
    }
}

#[test]
fn the_public_surface_is_send_and_sync() {
    assert_send_sync::<Args>();
}

/// The findings that are not `ok`, **excluding `free space` and `host confinement`** — the unit
/// legs' answer to "was this root reported clean?".
///
/// `free space` reads the **host's** volume, so a machine below doctor's own floor
/// (`max(1 GiB, 5%)`) answers `warn` on a root with nothing wrong with it, and a leg asserting the
/// whole report `Ok` would be a function of the CI disk's free space. The floor's two arms are
/// pinned with explicit numbers instead — `the_floor_is_the_larger_of_one_gib_and_five_percent`
/// drives `below_floor` itself — so nothing is lost by leaving the host out here.
///
/// `host confinement` is the second row that reads the **host** rather than the root: it asks
/// whether this machine can confine a process, so a Low-integrity session, a service account or
/// an AppContainer-hostile runner answers `warn` on a root that has nothing wrong with it, and a
/// leg asserting the whole report `Ok` would be a function of the machine. Its two arms are
/// pinned by `a_host_that_cannot_confine_is_reported_as_a_warning` and
/// `a_host_that_can_confine_says_so`, and the assembly's wiring by
/// `the_report_wires_the_host_confinement_row_to_the_predicate`, so like the floor's, nothing is
/// lost by leaving the host out here.
fn problems(report: &Report) -> Vec<String> {
    report
        .findings()
        .iter()
        .filter(|finding| {
            finding.severity != Severity::Ok
                && finding.check != "free space"
                && finding.check != HOST_CONFINEMENT
        })
        .map(|finding| format!("{}: {}", finding.check, finding.detail))
        .collect()
}

#[tokio::test]
async fn a_clean_root_reports_ok() {
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let problems = problems(&report);
    assert!(
        problems.is_empty(),
        "{problems:?} in {:?}",
        report.findings()
    );
}

#[tokio::test]
async fn a_symlink_while_following_is_disabled_is_reported() {
    #[cfg(unix)]
    {
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config(dir.path());
        crate::test_support::seed_bucket_with_symlink(dir.path());
        let report =
            crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert_eq!(report.severity(), Severity::Warn);
        assert!(
            report
                .findings()
                .iter()
                .any(|f| f.check.contains("symlink")),
            "{:?}",
            report.findings()
        );
    }
}

/// U2: the mode reaches the fixture the way production sends it. The row is always reported;
/// what changes is that read-only mode answers the **skipped** arm rather than running the
/// probe, which is what this asserts (a run whose mode was lost would answer "a probe file was
/// created and removed" instead). The read-write half is unix-only: the chmod is what makes
/// that probe fail.
#[tokio::test]
async fn a_read_only_root_skips_the_writability_check() {
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    #[cfg(unix)]
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
        .expect("read-only");

    let report = crate::test_support::run_checks(dir.path(), true, Proposals::Informational).await;
    let row = report
        .findings()
        .iter()
        .find(|finding| finding.check == "storage root writable")
        .expect("the writability row is reported in every mode");
    assert_eq!(row.severity, Severity::Ok, "{:?}", report.findings());
    assert!(
        row.detail.starts_with("skipped"),
        "read-only mode skips the probe: {}",
        row.detail
    );

    #[cfg(unix)]
    {
        // Still 0o500: the same root in read-write mode does report the error, so the
        // read-only arm is not passing because the probe can never fail.
        let read_write =
            crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        assert!(
            read_write
                .findings()
                .iter()
                .any(|finding| finding.check == "storage root writable"
                    && finding.severity == Severity::Error),
            "a read-write run over an unwritable root reports the error: {:?}",
            read_write.findings()
        );
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
            .expect("restore");
    }
}

/// The readability half of the root row: a directory this process may not enumerate is an
/// **error naming that**, never "is not a directory". `is_dir()` answers `true` for it — the
/// stat succeeds — so the row has to open the directory to see the difference, which is the
/// whole reason it does.
///
/// Unix-only: the chmod is what denies the read, and the mode bits carry that meaning on unix
/// alone (on Windows an ACL would, and std cannot set one).
#[cfg(unix)]
#[tokio::test]
async fn an_unreadable_root_is_an_error_and_not_a_missing_directory() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");

    let readable = crate::test_support::run_checks(&root, false, Proposals::Informational).await;
    let row = readable
        .findings()
        .iter()
        .find(|finding| finding.check == "storage root")
        .expect("the row is reported");
    assert_eq!(row.severity, Severity::Ok, "{:?}", readable.findings());

    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o000)).expect("unreadable");
    let unreadable = crate::test_support::run_checks(&root, false, Proposals::Informational).await;
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).expect("restore");

    // A privileged process (root, or a container's user namespace) reads a 0o000 directory
    // anyway, and then there is no arm to assert: the guard says so rather than failing a test
    // whose precondition the host does not meet.
    if !unreadable
        .findings()
        .iter()
        .any(|finding| finding.check == "storage root" && finding.severity == Severity::Error)
    {
        return;
    }
    let row = unreadable
        .findings()
        .iter()
        .find(|finding| finding.check == "storage root")
        .expect("the row is reported");
    assert!(
        row.detail.contains("cannot be read"),
        "the unreadable arm, not the missing-directory one: {}",
        row.detail
    );
}

#[tokio::test]
async fn an_unparseable_config_is_an_error_finding() {
    let dir = tempfile::TempDir::new().expect("temp");
    std::fs::create_dir_all(dir.path().join(".tinio")).expect("reserved dir");
    std::fs::write(
        dir.path().join(".tinio/.tinio.toml"),
        "version = 1\n[server]\nport = \"nine\"\n",
    )
    .expect("seed");
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    assert_eq!(
        report.severity(),
        Severity::Error,
        "{:?}",
        report.findings()
    );
    assert!(
        report.findings().iter().any(|f| f.check.contains("config")),
        "{:?}",
        report.findings()
    );
}

#[tokio::test]
async fn an_invalid_name_several_levels_down_is_reported() {
    // The rule is about **any depth** (the doctor section's any-depth name rule, T073): a `.tinio` segment
    // below the bucket level is exactly the case it exists for, and a top-level-only walk
    // would silently pass it.
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let deep = dir.path().join("bucket/a/b/.tinio");
    std::fs::create_dir_all(&deep).expect("deep reserved segment");
    std::fs::write(deep.join("object.txt"), b"data").expect("object");
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    assert_eq!(report.severity(), Severity::Warn, "{:?}", report.findings());
    assert!(
        report
            .findings()
            .iter()
            .any(|f| f.check.contains("name") && f.detail.contains(".tinio")),
        "{:?}",
        report.findings()
    );
}

/// The rule itself, with no filesystem in the way: the audit hands `object::key` the **key**,
/// never the `bucket/key` path, and the rules that changes are the start-anchored ones. `C:foo`
/// is refused as a key; the path form `bucket/C:foo` is not refused, because its second byte is
/// `y` rather than `:` — which is how a root could hold a file no request can address while this
/// leg reported it clean. A segment that is merely reserved stays caught at any depth, so the
/// split must not lose that either.
#[test]
fn the_key_rule_refuses_what_the_path_form_hides() {
    let refuses = |path: &str| super::audit::name_problem(Path::new(path), path);
    // What the API itself says about the key, so the assertion is not on the audit's wording:
    // `object::key` reports one generic sentence for every rule it applies.
    let api_says = |key: &str| {
        crate::_core::object::key(key.to_string())
            .err()
            .map(|err| err.to_string())
    };

    assert!(
        api_says("C:foo").is_some(),
        "the premise: the key is refused"
    );
    assert_eq!(
        refuses("bucket/C:foo"),
        api_says("C:foo"),
        "the audit's verdict on `bucket/C:foo` is the API's verdict on the key `C:foo`"
    );
    assert_eq!(
        refuses("bucket/a/b/C:foo"),
        api_says("a/b/C:foo"),
        "the key is everything after the bucket, at any depth"
    );
    assert!(
        refuses("bucket/a/.tinio").is_some_and(|why| why.contains("reserved")),
        "a reserved segment below the bucket stays caught"
    );
    assert!(
        refuses("bucket/legal/object.txt").is_none(),
        "and a legal key is still legal"
    );
}

/// The same rule end to end, through the walk and the report: the case above as a real entry.
///
/// Unix-only for the fixture's sake: Win32 cannot create a file whose name holds a colon, which
/// is also why the hole only ever bit unix roots.
///
/// Unix-only for the fixture's sake: Win32 cannot create a file whose name holds a colon, which
/// is also why the hole only ever bit unix roots.
#[cfg(unix)]
#[tokio::test]
async fn a_drive_letter_key_is_reported_under_its_bucket() {
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let bucket = dir.path().join("bucket");
    std::fs::create_dir_all(&bucket).expect("bucket dir");
    std::fs::write(bucket.join("C:foo"), b"data").expect("the drive-letter key");

    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let row = report
        .findings()
        .iter()
        .find(|finding| finding.check == "on-disk names")
        .expect("the names row is reported");
    assert_eq!(row.severity, Severity::Warn, "{}", row.detail);
    assert!(
        row.detail.contains("C:foo"),
        "the entry is refused by the key's rule, so the row names it: {}",
        row.detail
    );
}

#[tokio::test]
async fn a_missing_config_is_a_warning_and_never_created() {
    // `doctor` is offline diagnostics: the file's absence is a warn (`a first start would
    // auto-create it`), and the run must leave the root exactly as it found it. Nothing
    // creates `.tinio` here on either count: the config leg goes through `Config::load`,
    // never `load_or_create`, and the store legs answer an absent `meta.redb` without
    // opening anything (the Global Constraints' store rule).
    let dir = tempfile::TempDir::new().expect("temp");
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    assert_eq!(report.severity(), Severity::Warn, "{:?}", report.findings());
    assert!(
        report.findings().iter().any(|f| f.check.contains("config")),
        "{:?}",
        report.findings()
    );
    assert!(
        !dir.path().join(".tinio").exists(),
        "doctor never auto-creates anything"
    );
}

/// The no-`[auth]` row's text, pinned. The case is a configuration that **exists** without an
/// `[auth]` section, which the credential remediation (2026-10-02) made the built-in loopback
/// pair's and not the generated-credentials case: `discover::load_or_create` generates only when
/// the file is *absent* (`a_missing_config_is_a_warning_and_never_created` above), so a start here
/// serves the built-in `minioadmin` pair on loopback and refuses a non-loopback bind. The retired
/// sentence ("a start would generate session credentials") was also a warn on this same row, so a
/// severity-or-name-only assertion would have passed it; the detail is asserted whole for that
/// reason.
#[tokio::test]
async fn a_config_without_auth_names_the_loopback_fallback() {
    let dir = tempfile::TempDir::new().expect("temp");
    // The fixture family, not a second spelling of the reserved directory and the file name:
    // this is the one fixture that has no `[auth]` pair, which is the row's whole subject.
    crate::test_support::seed_config_without_auth(dir.path());
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let row = report
        .findings()
        .iter()
        .find(|finding| finding.check == "configuration")
        .expect("the configuration row is reported");
    assert_eq!(row.severity, Severity::Warn, "{:?}", report.findings());
    assert_eq!(
        row.detail,
        "no [auth] pair: a start would serve the built-in minioadmin development pair on \
         loopback, and would refuse a non-loopback bind instead"
    );
}

#[test]
fn the_floor_is_the_larger_of_one_gib_and_five_percent() {
    // The floor is arithmetic over the probed totals, so it is pinned with explicit
    // numbers instead of a filesystem: `free_space` computes
    // `max(DEFAULT_FLOOR_BYTES, total / 20)` and `below_floor` is that rule on its own,
    // which is the whole point of splitting it out — two arms, both driven here.
    const GIB: u64 = 1024 * 1024 * 1024;
    // A small filesystem: the fixed 1 GiB arm decides, never the percentage.
    assert!(below_floor(GIB - 1, 4 * GIB), "just under 1 GiB free");
    assert!(
        !below_floor(GIB, 4 * GIB),
        "1 GiB free is at the floor, not below it"
    );
    // A large filesystem: 100 GiB total puts 5% (5 GiB) above the fixed arm, so this is
    // the percentage arm and only this arm.
    assert!(
        below_floor(5 * GIB - 1, 100 * GIB),
        "just under 5% of 100 GiB"
    );
    assert!(
        !below_floor(5 * GIB, 100 * GIB),
        "5% is at the floor, not below it"
    );
    assert!(!below_floor(50 * GIB, 100 * GIB), "half the volume free");
    assert!(below_floor(0, 0), "an empty filesystem is below any floor");
}

#[test]
fn free_space_reports_the_amount_and_its_percentage() {
    // The probe itself, on whatever filesystem the test runs from: this asserts the
    // shape of the answer — both numbers are in the detail — and never a byte count.
    let dir = tempfile::TempDir::new().expect("temp");
    let finding = free_space(&resolution(dir.path(), "version = 1\n"));
    assert_eq!(finding.check, "free space");
    assert!(
        finding.detail.contains('%'),
        "the amount and its percentage: {}",
        finding.detail
    );
    // The **severity** is deliberately not asserted: it is the floor's answer about *this host's*
    // volume, and a machine below 5% free answers `warn` for reasons that have nothing to do with
    // the probe under test. The rule that decides it is pinned with explicit numbers — both arms,
    // at and below the floor — in `the_floor_is_the_larger_of_one_gib_and_five_percent`.
    assert!(
        matches!(finding.severity, Severity::Ok | Severity::Warn),
        "the floor's two arms are the only answers this row has: {}",
        finding.detail
    );
}

#[test]
fn a_host_that_cannot_confine_is_reported_as_a_warning() {
    // The landed shape of the refusal (Task 3/4): `HostCannotConfine` carries cause, remedy and
    // the raw HRESULT, and it is **not** constructible from a `String` — so the fixture builds
    // the value the predicate really hands over and lets `Display` render the one sentence.
    let finding = host_confinement_finding(Err(_sandbox::Error::HostCannotConfine {
        cause: "this token cannot create an AppContainer profile (hr=0x80070005)".to_string(),
        remedy: "run in an ordinary user session",
        hr: 0x8007_0005u32 as i32,
    }));
    assert_eq!(finding.check, HOST_CONFINEMENT);
    assert_eq!(finding.severity, Severity::Warn);
    assert!(
        finding.detail.contains("hr=0x80070005"),
        "{}",
        finding.detail
    );
}

#[test]
fn a_host_that_can_confine_says_so() {
    // The name's other half: the row must **say** the platform can confine, not merely be
    // `ok`. The expectation is the platform's own sentence (`HOST_CONFINEMENT_OK`), so the
    // assertion is honest on a target whose truth is not Windows's; swapping in the refusal
    // sentence turns it red.
    let finding = host_confinement_finding(Ok(()));
    assert_eq!(finding.check, HOST_CONFINEMENT);
    assert_eq!(finding.severity, Severity::Ok);
    assert_eq!(
        finding.detail, HOST_CONFINEMENT_OK,
        "the row says the platform's own truth, not the refusal's sentence"
    );
}

/// M-3: the arm tests above drive `host_confinement_finding` directly, so a constant severity at
/// the push in `checks()` would leave them green. This leg runs the **real assembly**
/// (`crate::test_support::run_checks`) and reads the row it emits: exactly one
/// `host confinement` row, at the severity the predicate answers in this same test. The
/// expectation is computed from `_sandbox::can_confine()` rather than hardcoded, so the leg is
/// honest on a host that cannot confine too.
#[tokio::test]
async fn the_report_wires_the_host_confinement_row_to_the_predicate() {
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let rows: Vec<&Finding> = report
        .findings()
        .iter()
        .filter(|finding| finding.check == HOST_CONFINEMENT)
        .collect();
    assert_eq!(rows.len(), 1, "one row: {:?}", report.findings());
    assert_eq!(
        rows[0].severity,
        host_confinement_finding(_sandbox::can_confine()).severity,
        "the assembly emits the predicate's answer, not a constant: {:?}",
        report.findings()
    );
}

#[tokio::test]
async fn a_missing_meta_store_is_not_a_finding_and_is_never_created() {
    // A root that has never opened a store has nothing to check: absent is `ok`, not a
    // warn that would nag every fresh root — **and it is not opened either**, which is the
    // half that cannot be seen in the report. `tinio_fs::database::open` would create the
    // file, so the leg probes first; this assertion is the only thing that fails if that
    // probe is ever replaced by an open.
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let meta: Vec<&Finding> = report
        .findings()
        .iter()
        .filter(|f| f.check.contains("meta.redb"))
        .collect();
    assert!(
        !meta.is_empty(),
        "the leg reports even when there is nothing to check: {:?}",
        report.findings()
    );
    assert!(
        meta.iter().all(|f| f.severity == Severity::Ok),
        "{:?}",
        report.findings()
    );
    assert!(
        !dir.path().join(".tinio/meta.redb").exists(),
        "a diagnostics run over a store-less root creates nothing: the probe is a `symlink_metadata`, not an open"
    );
}

#[tokio::test]
async fn a_clean_existing_store_still_reports_private_state_ok() {
    // A store that opens and whose cleanup stream is empty must not go silent: the plan's
    // empty-action rule is an `ok` row ("nothing to clear"), the same check name the absent
    // and held arms use.
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    drop(
        crate::_fs::FsStorage::new(dir.path(), crate::_fs::testing::fs_options())
            .expect("seed a clean store"),
    );
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let private: Vec<&Finding> = report
        .findings()
        .iter()
        .filter(|f| f.check == "private state")
        .collect();
    assert_eq!(
        private.len(),
        1,
        "the private-state leg must speak once: {:?}",
        report.findings()
    );
    assert_eq!(private[0].severity, Severity::Ok);
    assert!(
        private[0].detail.contains("nothing to clear"),
        "{}",
        private[0].detail
    );
}

#[tokio::test]
async fn a_corrupt_meta_store_is_an_error() {
    // A file that is not a redb database: `check_integrity` cannot even open it, so the
    // leg takes the **error** arm (`Corrupted` is the arm for a database that opens and
    // fails its check; this one never opens) — and the run still produces a report.
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    std::fs::create_dir_all(dir.path().join(".tinio")).expect("reserved dir");
    std::fs::write(dir.path().join(".tinio/meta.redb"), b"not a redb file").expect("seed");
    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    assert_eq!(
        report.severity(),
        Severity::Error,
        "{:?}",
        report.findings()
    );
    assert!(
        report
            .findings()
            .iter()
            .any(|f| f.check.contains("meta.redb") && f.severity == Severity::Error),
        "{:?}",
        report.findings()
    );
}

#[tokio::test]
async fn an_unopenable_store_is_an_error_finding_not_an_ok_note() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(root.join(".tinio")).expect("reserved dir");
    std::fs::write(root.join(".tinio/meta.redb"), b"not a redb file").expect("corrupt store");
    let resolution = resolution(&root, "version = 1\n");
    let state = StateDir {
        dir: root.join(".tinio"),
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    };

    let outcome = super::repair(&Run {
        resolution: &resolution,
        state: &state,
    })
    .await;

    assert!(
        outcome.failures.iter().any(|finding| {
            finding.severity == Severity::Error && finding.detail.contains("could not be opened")
        }),
        "the open failure is an error finding: {:?}",
        outcome.failures
    );
    assert!(
        !outcome.summary.contains("could not be opened"),
        "not an ok-summary note: {}",
        outcome.summary
    );
}

#[tokio::test]
async fn a_held_store_during_fix_is_a_warn_not_an_ok_note() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let _held = crate::_fs::FsStorage::new(&root, crate::_fs::testing::fs_options())
        .expect("take the store");
    let resolution = resolution(&root, "version = 1\n");
    let state = StateDir {
        dir: root.join(".tinio"),
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    };

    let outcome = super::repair(&Run {
        resolution: &resolution,
        state: &state,
    })
    .await;

    assert!(
        outcome.failures.iter().any(|finding| {
            finding.check == "meta.redb"
                && finding.severity == Severity::Warn
                && finding.detail.starts_with("not inspected:")
        }),
        "a held store is the diagnostics warn: {:?}",
        outcome.failures
    );
    assert!(
        !outcome.summary.contains("could not be opened"),
        "not an ok-summary note: {}",
        outcome.summary
    );
}

/// T154: a read-only `--fix` opens — and therefore creates — no store at the chosen state dir.
///
/// FR-023 amended puts a read-only run's metadata source at the **root's own**
/// `<root>/.tinio/meta.redb`, opened read-only by the storage, and the relocated tool-state dir
/// holds tool state only (the socket, the `state` file, the logs). The `Pass::Fix` open this leg
/// used to take created `meta.redb` under the relocated dir and handed the handle to
/// `FsStorage::new_from_db`, which drops it unread a line later — so the file outlived the very
/// run that promised not to write it. The state dir is left exactly as the run found it.
#[tokio::test]
async fn a_read_only_fix_opens_no_store_at_the_state_dir() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    // The relocated state dir a read-only run resolves: outside the served root, which is the
    // mode's rule, and empty — the shape the assertions below are about.
    let state_dir = dir.path().join("home-state/roots/0123456789abcdef");
    std::fs::create_dir_all(&state_dir).expect("the relocated state dir");
    let mut resolution = resolution(&root, "version = 1\n");
    resolution.read_only = true;
    resolution.state_dir = state_dir.clone();
    resolution.state_dir_override = Some(state_dir.clone());
    let state = StateDir {
        dir: state_dir.clone(),
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    };

    let outcome = super::repair(&Run {
        resolution: &resolution,
        state: &state,
    })
    .await;

    assert!(
        !state_dir.join("meta.redb").exists(),
        "a read-only repair never opens the tool-state store, so it never creates it"
    );
    assert!(
        std::fs::read_dir(&state_dir)
            .expect("the state dir is readable")
            .next()
            .is_none(),
        "and nothing else either: the run left the state dir as it found it"
    );
    assert!(
        outcome.summary.contains("read-only"),
        "the skipped stages are named, not silent: {}",
        outcome.summary
    );
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
}

/// T154's other half: the stages a read-only repair **does** own still run — the store's stages
/// are skipped, not the repair. A crashed read-only server's `state` file is still reclaimed,
/// and the absence of a store is not a failure row.
#[cfg(feature = "api")]
#[tokio::test]
async fn a_read_only_fix_still_reclaims_the_plane_leftovers() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let state_dir = dir.path().join("home-state/roots/0123456789abcdef");
    std::fs::create_dir_all(&state_dir).expect("the relocated state dir");
    // A crashed run's document, written through the plane's own writer so the fixture cannot
    // drift from the file a server publishes. Nothing answers the channel it records, which is
    // what makes the leftovers reclaimable. The name is the derivation the server's own fallback
    // uses (`socket_path`), not a second spelling.
    let control = crate::_api::state::socket_path(&state_dir, &root).expect("the control name");
    crate::_api::State::with_token(
        control.to_string_lossy().into_owned(),
        Some(9000),
        &root,
        "00".repeat(32),
    )
    .expect("the fixture's state document")
    .write(&state_dir)
    .expect("the stale state file");
    let mut resolution = resolution(&root, "version = 1\n");
    resolution.read_only = true;
    resolution.state_dir = state_dir.clone();
    resolution.state_dir_override = Some(state_dir.clone());
    let state = StateDir {
        dir: state_dir.clone(),
        resolved: true,
        liveness: Liveness::Stopped,
        control_name: Some(control.to_string_lossy().into_owned()),
        channel_answers: false,
    };

    let outcome = super::repair(&Run {
        resolution: &resolution,
        state: &state,
    })
    .await;

    assert!(
        !state_dir.join("state").exists(),
        "the plane's reclaim still runs: {:?}",
        outcome.failures
    );
    assert!(
        !state_dir.join("meta.redb").exists(),
        "and the store is still never created"
    );
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
}

#[tokio::test]
async fn a_store_held_by_another_process_is_a_warn_not_an_error() {
    // The test holds the store open first (that `FsStorage::new` also creates the file is
    // the point: the leg's probe finds it and tries to use it), so neither
    // `check_integrity` nor the diagnostics pass's single open can take it: the redb lock
    // answers `Database already open`, `tinio_fs::database::Error::is_already_open` is the
    // predicate that recognises it, and that is "not inspected" — a warn carrying the
    // reason, never an error it cannot substantiate.
    let dir = tempfile::TempDir::new().expect("temp");
    crate::test_support::seed_config(dir.path());
    let _held = crate::_fs::FsStorage::new(dir.path(), crate::_fs::testing::fs_options())
        .expect("take the store");

    let report = crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
    let meta = report
        .findings()
        .iter()
        .find(|f| f.check.contains("meta.redb"))
        .expect("the leg reports");
    assert_eq!(meta.severity, Severity::Warn, "{:?}", report.findings());
    assert!(meta.detail.starts_with("not inspected:"), "{}", meta.detail);
}

/// B15: the `Repaired` arm has no store a test can seed — `check_integrity` answers it only when
/// redb repairs a database in place, which no public API constructs — but `Integrity` is a
/// public enum with a public unit variant, so the row's mapping is driven directly instead.
#[test]
fn the_repaired_integrity_arm_is_a_warning() {
    let finding = super::checks::integrity_finding(crate::_fs::database::Integrity::Repaired);
    assert_eq!(finding.check, "meta.redb");
    assert_eq!(finding.severity, Severity::Warn);
    assert_eq!(
        finding.detail,
        "the integrity check failed and was repaired automatically"
    );
}

/// The offline options are plan B's one mapping with the chosen state dir overridden — the
/// configured `[storage.fs]` values included, not a local paraphrase that used the schema
/// defaults (and the wrong follow policy: the schema's `follow_symlinks` default is `false`,
/// so "no `[storage]` section means follow" silently followed links the server rejects).
/// The `Resolution` the options tests build: `root`'s own `.tinio` as the state dir, and the
/// configuration parsed from `config`.
fn resolution(root: &Path, config: &str) -> Resolution {
    let state_dir = root.join(".tinio");
    Resolution {
        pg_root: None,
        root: root.to_path_buf(),
        config: crate::_config::Config::parse(config).expect("config"),
        config_text: config.to_owned(),
        config_path: state_dir.join(".tinio.toml"),
        read_only: false,
        state_dir,
        state_dir_override: None,
        config_error: None,
    }
}

/// The chosen state dir those tests pin the options to: the alternate candidate, never the
/// resolution's own, which is the override the whole rule is about.
fn chosen_state(root: &Path) -> StateDir {
    StateDir {
        dir: root.join(".tinio/chosen"),
        resolved: false,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    }
}

#[test]
fn the_storage_options_carry_the_configured_storage_values() {
    let dir = tempfile::TempDir::new().expect("temp");
    let configured = resolution(
        &dir.path().join("root"),
        "version = 1\n\
         [storage.fs]\n\
         follow_symlinks = true\n\
         compact_threshold_percent = 55\n\
         meta_batch_size = 300\n\
         meta_batch_bytes = 4096\n",
    );
    let chosen = chosen_state(&dir.path().join("root"));

    let options = super::checks::fs_options(&Run {
        resolution: &configured,
        state: &chosen,
    });
    assert!(
        options.follow_symlinks,
        "the configured follow policy, not the schema default: {}",
        options.follow_symlinks
    );
    assert_eq!(options.compact_threshold_percent, 55);
    assert_eq!(options.meta_batch_size, 300);
    assert_eq!(options.meta_batch_bytes, 4096);
    assert_eq!(
        options.state_dir.as_deref(),
        Some(chosen.dir.as_path()),
        "the chosen directory overrides the resolution's own"
    );

    // T4-2: the guard must discriminate. The paraphrase this replaced answered `true` for a
    // root with **no** `[storage]` section, so a fixture that sets the key to `true` alone
    // would pass under the old code; the schema's secure default is `false`.
    let bare = resolution(&dir.path().join("bare"), "version = 1\n");
    let bare_options = super::checks::fs_options(&Run {
        resolution: &bare,
        state: &chosen,
    });
    assert!(
        !bare_options.follow_symlinks,
        "no `[storage]` section is the schema's secure default (`false`), not `true`"
    );
}

/// T4-2's other half: the configured owner->uid map is carried, where the paraphrase built an
/// empty one. Windows configs cannot carry `local_uid` (the parse refuses it), so the
/// discriminating half is unix-only; the empty map is the correct answer on Windows.
#[cfg(unix)]
#[test]
fn the_storage_options_carry_the_configured_owner_map() {
    let dir = tempfile::TempDir::new().expect("temp");
    let configured = resolution(
        &dir.path().join("root"),
        "version = 1\n\
         [owner]\n\
         local_uid = 1000\n\
         [[users]]\n\
         access_key = \"AKID\"\n\
         secret_key = \"sk\"\n\
         local_uid = 1001\n",
    );
    let chosen = chosen_state(&dir.path().join("root"));
    let options = super::checks::fs_options(&Run {
        resolution: &configured,
        state: &chosen,
    });
    assert_eq!(
        options.owner_uids.len(),
        2,
        "the configured mapping, not the empty map the paraphrase built"
    );
}

/// I-2: the symlink row reads the **configured** follow policy. The contract's check is "link
/// entries present while `follow_symlinks` is disabled" (the doctor section's symlink-check bullet), so with
/// following enabled the row is `ok` and says the links are served; the old row warned
/// unconditionally and its detail was false there.
#[test]
fn the_symlink_row_reads_the_configured_follow_policy() {
    let samples = vec!["bucket/link.txt".to_string()];
    let served = super::audit::symlink_finding(samples.clone(), 1, true);
    assert_eq!(served.severity, Severity::Ok, "{}", served.detail);
    assert!(
        served.detail.contains("following is enabled"),
        "{}",
        served.detail
    );
    let refused = super::audit::symlink_finding(samples, 1, false);
    assert_eq!(refused.severity, Severity::Warn);
    assert_eq!(
        super::audit::symlink_finding(Vec::new(), 0, false).severity,
        Severity::Ok
    );
}

/// I-2 at the filesystem level: a bucket holding a real link entry with following enabled
/// through the root's own configuration. `#[cfg(unix)]` because a Windows test cannot create
/// a symlink without elevation; the unit leg above covers the arm on every platform.
#[tokio::test]
async fn a_symlink_while_following_is_enabled_is_ok() {
    #[cfg(unix)]
    {
        let dir = tempfile::TempDir::new().expect("temp");
        crate::test_support::seed_config_with(dir.path(), true);
        crate::test_support::seed_bucket_with_symlink(dir.path());
        let report =
            crate::test_support::run_checks(dir.path(), false, Proposals::Informational).await;
        let row = report
            .findings()
            .iter()
            .find(|finding| finding.check == "symlinks")
            .expect("the symlink row");
        assert_eq!(row.severity, Severity::Ok, "{:?}", report.findings());
        assert!(
            row.detail.contains("following is enabled"),
            "{}",
            row.detail
        );
    }
}

/// I-1: a walk that read everything is silent about inspection; one that could not read a
/// directory turns that into a `warn` "not inspected" row rather than a clean name row. A
/// missing root drives the unreadable-directory count on every platform — a directory this
/// host cannot open is not something a Windows test can arrange.
#[test]
fn a_walk_that_could_not_read_is_reported_as_not_inspected() {
    let missing = std::path::Path::new("tinio-doctor-no-such-root");
    let walk = super::audit::walk_audit(missing);
    assert_eq!(walk.unreadable_dirs, 1, "the unreadable root is counted");
    let finding = super::audit::inspection_finding(&walk, missing).expect("a row");
    assert_eq!(finding.severity, Severity::Warn);
    assert!(
        finding.detail.starts_with("not inspected:"),
        "{}",
        finding.detail
    );

    let dir = tempfile::TempDir::new().expect("temp");
    let clean = super::audit::walk_audit(dir.path());
    assert_eq!(clean.unreadable_dirs, 0);
    assert!(
        super::audit::inspection_finding(&clean, dir.path()).is_none(),
        "a walk that read everything is not a finding"
    );
}

/// M-6: the name row's early return is keyed on the count, not on the sample, so a capped
/// collector can never render "0 invalid name(s)".
#[test]
fn the_name_row_reads_the_total_not_the_sample() {
    let finding = super::audit::name_finding(Vec::new(), 3);
    assert_eq!(finding.severity, Severity::Warn);
    assert_eq!(finding.detail, "3 invalid name(s)");
    assert_eq!(
        super::audit::name_finding(Vec::new(), 0).severity,
        Severity::Ok
    );
}

/// M-1: the name text is normalized for Windows' separator only. A backslash is a legal
/// filename byte on unix, so rewriting it there would judge a different name than the disk
/// holds.
#[test]
fn the_name_text_normalizes_the_windows_separator_only() {
    let path = std::path::Path::new("bucket").join("a\\b.txt");
    #[cfg(windows)]
    assert_eq!(super::audit::name_text(&path), "bucket/a/b.txt");
    #[cfg(not(windows))]
    assert_eq!(super::audit::name_text(&path), "bucket/a\\b.txt");
}
/// B10 (T2-8): the capped collector's tail and the empty-sample shape both render — the
/// "and N more" suffix beside a full sample, and a bare count with no dangling colon when the
/// sample is empty (the M-6 shape, which `symlink_finding` now shares with `name_finding`).
#[test]
fn the_symlink_row_renders_its_tail_and_an_empty_sample() {
    let samples: Vec<String> = (0..super::audit::SAMPLES)
        .map(|index| format!("bucket/link-{index}.txt"))
        .collect();
    let total = super::audit::SAMPLES + 7;
    let full = super::audit::symlink_finding(samples, total, false);
    assert_eq!(full.severity, Severity::Warn);
    assert!(
        full.detail.contains(&format!("{total} link entr(ies)")),
        "{}",
        full.detail
    );
    assert!(full.detail.contains("and 7 more"), "{}", full.detail);
    assert!(
        full.detail.contains("`follow_symlinks` is disabled"),
        "{}",
        full.detail
    );

    let empty = super::audit::symlink_finding(Vec::new(), 4, false);
    assert_eq!(empty.severity, Severity::Warn);
    assert_eq!(
        empty.detail,
        "4 link entr(ies) present while `follow_symlinks` is disabled"
    );
}

/// A `Resolution` for a hand-written configuration, as `doctor`'s pg leg reads one: the fields
/// the leg touches are the config and the mode; the paths are fictional because nothing on the
/// filesystem is read for a pg diagnosis.
#[cfg(feature = "pg")]
fn pg_resolution(config_text: &str, read_only: bool) -> crate::resolve::Resolution {
    crate::resolve::Resolution {
        pg_root: None,
        root: PathBuf::from("/srv/pg"),
        config: Config::parse(config_text).expect("the fixture configuration parses"),
        config_text: config_text.to_owned(),
        config_path: PathBuf::from("/srv/pg/.tinio/.tinio.toml"),
        read_only,
        state_dir: PathBuf::from("/srv/pg/.tinio"),
        state_dir_override: None,
        config_error: None,
    }
}

/// A `Secret` from a fixture string — the shape `EnvArgs::pg_dsn` holds.
#[cfg(feature = "pg")]
fn pg_secret(raw: &str) -> Secret {
    Secret::from(SecretKey::from(raw.to_owned()))
}

/// **R8's negative half**: a filesystem run carries no pg key and no addressed dsn, so the pg
/// leg adds no rows at all — the report is exactly the filesystem report it has always been.
#[cfg(feature = "pg")]
#[tokio::test]
async fn the_pg_leg_stays_silent_for_a_filesystem_run() {
    let resolution = pg_resolution("version = 1\n[server]\nport = 9000\n", false);
    assert!(
        super::checks::pg_findings(&resolution, None)
            .await
            .is_empty()
    );
}

/// A configuration that selects pg but names no dsn is the one shape reported rather than
/// skipped: the run says pg and nothing can be diagnosed, which is the operator's one fix.
#[cfg(feature = "pg")]
#[tokio::test]
async fn a_pg_configuration_without_a_dsn_is_reported_with_the_remedy() {
    let resolution = pg_resolution("version = 1\n[storage]\ntype = \"pg\"\n", false);
    let rows = super::checks::pg_findings(&resolution, None).await;
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].severity, Severity::Error, "{rows:?}");
    assert!(
        rows[0].detail.contains("TINIO_PG_DSN"),
        "the remedy names the addressing channel: {}",
        rows[0].detail
    );
}

/// **R8's positive half**: with `TINIO_PG_DSN` addressed, a dsn nothing answers for is a
/// connection row at `error` whose remedy names `TINIO_PG_DSN` (and `[storage.pg] dsn`) — the
/// channels that decide which instance the run is diagnosing. The port is closed on any host,
/// so the refusal is immediate and the leg needs no server.
#[cfg(feature = "pg")]
#[tokio::test]
async fn a_refused_pg_instance_names_the_channel_that_addresses_it() {
    let resolution = pg_resolution("version = 1\n[storage]\ntype = \"pg\"\n", false);
    let dsn = pg_secret("postgres://tinio:tinio@127.0.0.1:1/tinio?sslmode=disable");
    let rows = super::checks::pg_findings(&resolution, Some(&dsn)).await;

    let connection = rows
        .iter()
        .find(|row| row.check == "postgres connection")
        .expect("the connection row is reported");
    assert_eq!(connection.severity, Severity::Error, "{rows:?}");
    assert!(
        connection.detail.contains("TINIO_PG_DSN"),
        "{}",
        connection.detail
    );
    assert!(
        connection.detail.contains("storage.pg"),
        "{}",
        connection.detail
    );
    // The other legs still report: a server that cannot be reached does not silence the TLS row
    // (which is the dsn's own resolution) or the encoding row's "not inspected" reason.
    assert!(
        rows.iter().any(|row| row.check == "postgres tls"),
        "{rows:?}"
    );
    // PG5: no row may carry the dsn's text (which holds the password).
    let raw = "postgres://tinio:tinio@127.0.0.1:1/tinio?sslmode=disable";
    assert!(
        rows.iter().all(|row| !row.detail.contains(raw)),
        "a row rendered the dsn: {rows:?}"
    );
}

/// The live leg, skipping without `TINIO_PG_DSN` (PG8): the dsn the environment carries becomes
/// the diagnosis's subject, and the addressable channels (TINIO_PG_DSN) are what R8 settled on.
#[cfg(feature = "pg")]
#[tokio::test]
async fn the_pg_leg_diagnoses_the_addressed_instance() {
    let Some(dsn) = std::env::var("TINIO_PG_DSN")
        .ok()
        .filter(|dsn| !dsn.trim().is_empty())
    else {
        return;
    };
    let resolution = pg_resolution("version = 1\n[storage]\ntype = \"pg\"\n", true);
    let rows = super::checks::pg_findings(&resolution, Some(&pg_secret(&dsn))).await;
    let row = |name: &str| rows.iter().find(|row| row.check == name);

    for name in [
        "postgres connection",
        "postgres pool",
        "postgres encoding",
        "postgres tls",
        "postgres schema",
        "postgres access",
        "postgres ownership",
    ] {
        assert!(row(name).is_some(), "the {name} row is missing: {rows:?}");
    }
    // The fixture server is up and its database is UTF-8.
    assert_eq!(
        row("postgres connection").map(|row| row.severity),
        Some(Severity::Ok),
        "{rows:?}"
    );
    assert_eq!(
        row("postgres encoding").map(|row| row.severity),
        Some(Severity::Ok),
        "{rows:?}"
    );
    assert!(
        rows.iter().all(|row| !row.detail.contains(&dsn)),
        "a row rendered the dsn: {rows:?}"
    );
}

/// **R8's remaining half**: a pg run has no `DIR` — its root is the dsn-derived synthetic root
/// (`contracts/config.md`'s `[storage.pg]` bullet) — so `TINIO_PG_DSN=… tinio doctor` with no
/// `DIR` and no `.tinio` above must resolve that root rather than answer the usage error. The
/// walk-up starts at a scratch directory (the `locate_from` seam), so the leg never depends on
/// the process's working directory.
#[cfg(feature = "pg")]
#[test]
fn a_pg_dsn_with_no_dir_resolves_the_synthetic_root() {
    let scratch = tempfile::TempDir::new().expect("temp");
    let dsn = pg_secret("postgres://tinio:tinio@127.0.0.1:55432/tinio?sslmode=disable");
    let located = locate_from(
        Discovery {
            dir: None,
            config: None,
            read_only: false,
            state_dir: None,
            pg_dsn: Some(dsn.clone()),
        },
        &Provenance::default(),
        scratch.path(),
    )
    .expect("the dsn derives the root");

    let root = pg::resolve_root(&dsn, None).expect("the synthetic root");
    assert_eq!(located.root, root);
    assert_eq!(located.state_dir, root.join(".tinio"));
    // The override names the **root**, which is the value a pg run's override always is (the
    // operator's `TINIO_STATE_DIR` reaches `pg::resolve_root` as the root): the client pair reads
    // it in place of the home-derived second candidate, whose derivation would canonicalize a
    // leaf the serving half has not created yet — and §6 says a pg run never relocates. It is
    // also what lets a consumer re-derive this root and compare, which is how the serving half
    // refuses such a run and the check list skips its served-root legs.
    assert_eq!(
        located.state_dir_override.as_deref(),
        Some(located.root.as_path())
    );
}

/// The other arm, unchanged: with no dsn and no `DIR` the usage error still fires, so the
/// fallback is the dsn's and not "any run with no root".
#[cfg(feature = "pg")]
#[test]
fn no_dsn_and_no_dir_still_answers_the_root_usage_error() {
    let scratch = tempfile::TempDir::new().expect("temp");
    let err = locate_from(
        Discovery {
            dir: None,
            config: None,
            read_only: false,
            state_dir: None,
            pg_dsn: None,
        },
        &Provenance::default(),
        scratch.path(),
    )
    .expect_err("no root and no dsn");
    assert!(matches!(err, Error::Usage(_)), "{err}");
    assert!(err.to_string().contains("no storage root"), "{err}");
}

/// **A pg run's report is green on a healthy database** (ruling R8). The served-root legs answer
/// `ok` and say why instead of failing on a directory the serving half has not created — without
/// the skip this leg reports `error storage root: … could not be read` and exits 1 on an instance
/// that is perfectly well, which is a doctor no script can trust.
///
/// The root is a scratch leaf that is **never created**, which is exactly the state a pg
/// deployment is in before its first serving start: the row set is asserted too, so a script that
/// reads check names sees the same ones on either backend.
#[cfg(feature = "pg")]
#[tokio::test]
async fn a_pg_run_skips_the_served_root_legs_and_is_green() {
    let scratch = tempfile::TempDir::new().expect("temp");
    let root = scratch.path().join("pg-root");
    let state_dir = root.join(".tinio");
    let resolution = Resolution {
        // The record `locate`'s dsn-only arm writes: this run took it, so its root is synthetic.
        pg_root: Some(root.clone()),
        root: root.clone(),
        config: Config::default(),
        config_text: String::new(),
        config_path: state_dir.join(".tinio.toml"),
        read_only: false,
        state_dir: state_dir.clone(),
        // The shape `locate`'s dsn-only fallback produces: the override names the derived root.
        state_dir_override: Some(root.clone()),
        config_error: None,
    };
    let state = StateDir {
        dir: state_dir,
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    };

    let report = checks(
        &Run {
            resolution: &resolution,
            state: &state,
        },
        Proposals::Informational,
    )
    .await;

    assert!(problems(&report).is_empty(), "{:?}", report.findings());
    for name in [
        "storage root",
        "storage root writable",
        "free space",
        "meta.redb",
    ] {
        let row = report
            .findings()
            .iter()
            .find(|finding| finding.check == name)
            .expect("the row is kept for a pg run");
        assert!(row.detail.starts_with("skipped"), "{row:?}");
    }
    assert!(
        report
            .findings()
            .iter()
            .any(|finding| finding.check == "configuration"
                && finding.detail.contains("TINIO_PG_DSN")),
        "the configuration row names the addressing channel: {:?}",
        report.findings()
    );
    assert!(!root.exists(), "the diagnosis created its root: {root:?}");
}

/// **F1's shape**: a served root that is also the operator's `TINIO_STATE_DIR`, with a pg dsn
/// exported, is a **filesystem** run. The check list decides on `Located::pg_root` — the arm's own
/// record, carried in here as `None` — and never on the environment, so the served-root legs run
/// as they always did. (Before the fix the decision re-derived the synthetic root from
/// `state_dir_override`, which *is* `TINIO_STATE_DIR` on a filesystem run: the leg below answered
/// `skipped` and the report described a database that does not exist.)
#[cfg(feature = "pg")]
#[tokio::test]
async fn a_root_that_is_also_the_state_dir_override_still_runs_the_root_legs() {
    let scratch = tempfile::TempDir::new().expect("temp");
    let root = scratch.path().to_path_buf();
    let resolution = Resolution {
        pg_root: None,
        root: root.clone(),
        config: Config::default(),
        config_text: String::new(),
        config_path: root.join(".tinio").join(".tinio.toml"),
        read_only: false,
        state_dir: root.join(".tinio"),
        // `TINIO_STATE_DIR` == the served root, the shape that used to read as pg.
        state_dir_override: Some(root.clone()),
        config_error: None,
    };
    let state = StateDir {
        dir: root.join(".tinio"),
        resolved: true,
        liveness: Liveness::NoStateFile,
        control_name: None,
        channel_answers: false,
    };

    let report = checks(
        &Run {
            resolution: &resolution,
            state: &state,
        },
        Proposals::Informational,
    )
    .await;

    let row = report
        .findings()
        .iter()
        .find(|finding| finding.check == "storage root")
        .expect("the row is reported");
    assert_eq!(row.severity, Severity::Ok, "{:?}", report.findings());
    assert!(
        !row.detail.starts_with("skipped"),
        "a filesystem run runs its root legs: {row:?}"
    );
}
