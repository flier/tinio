//! The offline tier verbs at their public boundary: `tinio tier verify`,
//! `repair` and `prepare-downgrade` over a real root and a scratch PostgreSQL
//! database.
//!
//! **Why this target is not `tinio-tier/tests/repair.rs`.** The three verbs are
//! `tinio-cli` code: their entry points are `pub(crate) fn execute` and their
//! public surface is `commands::tier::{verify, repair, downgrade}::run`, which
//! takes the CLI's own `Located` resolution. Task 23's module tests already
//! pin each finding and flag at the `execute` boundary; this target proves the
//! same commands through the public `run` path — the argument type, the root
//! resolution, the three-plane open and the on-disk outcome — which a
//! `tinio-tier` test cannot reach without moving CLI code. The deviation is
//! recorded in the task report.
//!
//! Each verb opens the root itself and takes the database's ownership lock, so
//! every invocation runs on a runtime of its own that ends before the next
//! one opens the same root.
//!
//! The `pg` feature carries `tinio-pg`, the cold-plane configuration this
//! target names; the default workspace build (no `pg`) compiles the target
//! empty.

#![cfg(feature = "pg")]
// This target's file-level helpers are not `#[test]` functions, and clippy's
// `allow-*-in-tests` reach `#[test]` functions only (docs/style.md §Panics).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: an integration-test target's file-level helpers (docs/style.md §Panics)"
)]

use std::{
    fs,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    process::ExitCode,
    thread,
    time::{Duration, SystemTime},
};

use tinio_cli::{
    Error as CliError,
    cli::{Discovery, Provenance},
    commands::tier::{downgrade, repair, verify},
    resolve::{self, Located},
};
use tinio_core::{
    AccessMode, ETag,
    acl::Acl,
    bucket,
    content::{ContentKey, ObjectContent},
    object,
    storage::{BucketOps, ObjectOps},
};
use tinio_fs::{FsStorage, testing};
use tinio_pg::Config as PgConfig;
use tinio_store_pg::test_support::{TestDb, require_dsn};
use tinio_tier::{
    ColdStore, Config as RuntimeConfig, FlushMode, Index, MoveOutcome, Placement, State, Tier,
    TierStorage, index_path, is_tier_root, marker_path, pg,
};
use tinio_util::{
    secret::{Secret, SecretKey},
    testing::{body, read_body},
};
use tokio::runtime::Builder;

/// The bucket every leg writes to.
const DATA: &str = "data";

/// The scratch database's cold-plane configuration.
fn pg_config(dsn: &str) -> PgConfig {
    PgConfig::new(Secret::from(SecretKey::from(dsn.to_owned())), 4)
}

/// The runtime configuration every leg opens the tier with.
fn runtime_config() -> RuntimeConfig {
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

/// A fixture future built inside the runtime thread that polls it, so it never
/// crosses a thread boundary.
type Fixture<T> = Pin<Box<dyn Future<Output = T>>>;

/// Run `f` on a runtime of its own and join it: the tier a leg opens is gone,
/// ownership lock included, before the next verb opens the same database.
fn on_runtime<T, F>(f: F) -> T
where
    F: FnOnce() -> Fixture<T> + Send + 'static,
    T: Send + 'static,
{
    thread::spawn(move || {
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a test runtime");
        runtime.block_on(f())
    })
    .join()
    .expect("the leg's runtime ends")
}

/// The runtime configuration with the batch buffer armed, so a write waits for
/// a flush and can be made to fail it.
fn batch_runtime_config() -> RuntimeConfig {
    RuntimeConfig {
        flush_mode: FlushMode::Batch,
        ..runtime_config()
    }
}

/// Open the tier over `root` writable with `config` and hand it to `f`.
fn with_tier_config<T, F>(root: &Path, dsn: &str, config: RuntimeConfig, f: F) -> T
where
    F: FnOnce(TierStorage<ColdStore>) -> Fixture<T> + Send + 'static,
    T: Send + 'static,
{
    let root = root.to_path_buf();
    let pg = pg_config(dsn);
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

/// Plant one write a failed batch flush quarantines: a buffered write whose
/// publish a directory blocks, drained so the pair lands under the root's
/// `.tinio/quarantine/`.
fn seed_quarantine(root: &Path, dsn: &str, name: &str, bytes: &'static [u8]) {
    let file_root = root.to_path_buf();
    let name = name.to_owned();
    with_tier_config(root, dsn, batch_runtime_config(), move |tier| {
        Box::pin(async move {
            if tier.head_bucket(&data()).await.is_err() {
                tier.create_bucket(&data(), None, &Acl::default_private(None))
                    .await
                    .expect("the fixture bucket is created");
            }
            tier.put_object(&data(), &key(&name), body(bytes))
                .await
                .expect("the fixture write is acknowledged");
            assert!(
                !tier.buffer().is_empty(),
                "the fixture's write waits in the batch buffer"
            );
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

/// How many quarantine pairs the root holds (one record and its bytes each).
fn quarantine_entries(root: &Path) -> usize {
    match fs::read_dir(root.join(".tinio").join("quarantine")) {
        Ok(entries) => entries
            .filter(|entry| {
                entry
                    .as_ref()
                    .ok()
                    .is_some_and(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
            })
            .count(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => 0,
        Err(err) => panic!("the quarantine directory is readable: {err}"),
    }
}

/// Write the root's tier-mode configuration naming the scratch database.
fn seed_root(root: &Path, dsn: &str) {
    let dir = root.join(".tinio");
    fs::create_dir_all(&dir).expect("the reserved directory");
    fs::write(
        dir.join(".tinio.toml"),
        format!("version = 1\n[storage]\ntype = \"tier\"\n[storage.pg]\ndsn = \"{dsn}\"\n"),
    )
    .expect("the tier configuration");
}

/// The `Located` a verb resolves `root` through, exactly as the process does.
fn located(root: &Path) -> Located {
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
    .expect("the temporary root resolves")
}

/// The fixture's bucket name.
fn data() -> bucket::Name {
    bucket::name(DATA).expect("a valid bucket name")
}

/// The fixture's object key.
fn key(name: &str) -> object::Key {
    object::key(name).expect("a valid object key")
}

/// The warm file a plain key maps to.
fn object_file(root: &Path, name: &str) -> PathBuf {
    root.join(DATA).join(name)
}

/// Open the tier over `root` writable and hand it to `f`.
fn with_tier<T, F>(root: &Path, dsn: &str, f: F) -> T
where
    F: FnOnce(TierStorage<ColdStore>) -> Fixture<T> + Send + 'static,
    T: Send + 'static,
{
    let root = root.to_path_buf();
    let pg = pg_config(dsn);
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

/// Write one object and settle it cold.
async fn put_cold_async(tier: &TierStorage<ColdStore>, name: &str, bytes: &'static [u8]) {
    if tier.head_bucket(&data()).await.is_err() {
        tier.create_bucket(&data(), None, &Acl::default_private(None))
            .await
            .expect("the fixture bucket is created");
    }
    tier.put_object(&data(), &key(name), body(bytes))
        .await
        .expect("the fixture object is written");
    let moved = tier
        .demote(&data(), &key(name), SystemTime::now())
        .await
        .expect("the fixture demote runs");
    assert!(
        matches!(moved, MoveOutcome::Moved { .. }),
        "the fixture object reaches the cold plane"
    );
}

/// Seed a root with one settled cold object whose warm file is removed.
fn seed_cold(root: &Path, dsn: &str, name: &str, bytes: &'static [u8]) {
    let root = root.to_path_buf();
    let file_root = root.clone();
    let name = name.to_owned();
    with_tier(&root, dsn, move |tier| {
        Box::pin(async move {
            put_cold_async(&tier, &name, bytes).await;
            fs::remove_file(object_file(&file_root, &name))
                .expect("the leftover warm file is removed");
        })
    });
}

/// The placement one key records in the index at `root`, if any.
fn placement_of(root: &Path, name: &str) -> Option<Placement> {
    let index = Index::open_read_only(index_path(root)).expect("the index opens read-only");
    index
        .placement(&data(), &key(name))
        .expect("the placement reads")
}

/// Whether the cold plane holds content for `name`.
fn cold_holds(dsn: &str, name: &str) -> bool {
    let pg = pg_config(dsn);
    let name = name.to_owned();
    on_runtime(move || {
        Box::pin(async move {
            let cold = pg::connect(&pg, AccessMode::ReadOnly)
                .await
                .expect("the read-only cold plane opens");
            cold.stat(&data(), &key(&name))
                .await
                .expect("the cold stat runs")
                .is_some()
        })
    })
}

/// Run `verify` over `root`, answering its result without the printed report.
fn run_verify(root: &Path) -> Result<(), CliError> {
    let located = located(root);
    let args = verify::Args::default();
    on_runtime(move || Box::pin(async move { verify::run(args, located).await.map(|_| ()) }))
}

/// Run `verify` over `root`, answering its exit code.
fn run_verify_code(root: &Path) -> Result<ExitCode, CliError> {
    let located = located(root);
    let args = verify::Args::default();
    on_runtime(move || Box::pin(async move { verify::run(args, located).await }))
}

/// Run `repair` over `root` with the two switches, answering its exit code.
fn run_repair_code(root: &Path, adopt: bool, delete_orphans: bool) -> Result<ExitCode, CliError> {
    let located = located(root);
    let args = repair::Args {
        adopt,
        delete_orphans,
        ..repair::Args::default()
    };
    on_runtime(move || Box::pin(async move { repair::run(args, located).await }))
}

/// Run `repair` over `root` with the two switches.
fn run_repair(root: &Path, adopt: bool, delete_orphans: bool) -> Result<(), CliError> {
    let located = located(root);
    let args = repair::Args {
        adopt,
        delete_orphans,
        ..repair::Args::default()
    };
    on_runtime(move || Box::pin(async move { repair::run(args, located).await.map(|_| ()) }))
}

/// Run `prepare-downgrade` over `root`.
fn run_downgrade(root: &Path) -> Result<(), CliError> {
    let located = located(root);
    let args = downgrade::Args::default();
    on_runtime(move || Box::pin(async move { downgrade::run(args, located).await.map(|_| ()) }))
}

/// `verify` is report-only: a clean root and a corrupt one both answer `Ok`,
/// and neither pass changes the placement or the cold copy.
#[tokio::test]
async fn verify_never_writes() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    seed_cold(root.path(), &db.dsn, "cold.bin", b"hello");
    let root_path = root.path().to_path_buf();

    run_verify(&root_path).expect("a clean root verifies");
    assert_eq!(
        placement_of(&root_path, "cold.bin").map(|placement| placement.tier),
        Some(Tier::Pg),
        "verify writes nothing"
    );
    assert!(
        cold_holds(&db.dsn, "cold.bin"),
        "the cold copy is untouched"
    );

    // The corruption a plain run must report: the placement says cold while
    // the database holds no bytes for it.
    with_tier(&root_path, &db.dsn, |tier| {
        Box::pin(async move {
            tier.cold()
                .remove(&data(), &ContentKey::Object(key("cold.bin")))
                .await
                .expect("the cold copy is removed");
        })
    });
    run_verify(&root_path).expect("a finding still answers Ok");
    assert_eq!(
        placement_of(&root_path, "cold.bin").map(|placement| placement.state),
        Some(State::Live),
        "the report-only pass leaves the placement"
    );
}

/// The quarantine a failed batch flush leaves is visible to `verify` (its
/// warning earns exit 1) and a report-only run leaves every pair where it is.
#[tokio::test]
async fn verify_reports_a_quarantined_write() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    seed_quarantine(root.path(), &db.dsn, "lost.bin", b"lost bytes");
    let root_path = root.path().to_path_buf();
    assert_eq!(
        quarantine_entries(&root_path),
        1,
        "the fixture left one pair"
    );

    let code = run_verify_code(&root_path).expect("the report is produced");
    assert_eq!(code, ExitCode::from(1), "a warning is a finding");
    assert_eq!(
        quarantine_entries(&root_path),
        1,
        "verify is report-only and leaves the pair"
    );
}

/// `repair --delete-orphans` clears the quarantine and the report says how many
/// pairs went; without the flag the pair is reported and left untouched.
#[tokio::test]
async fn repair_delete_orphans_clears_the_quarantine() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    seed_quarantine(root.path(), &db.dsn, "lost.bin", b"lost bytes");
    let root_path = root.path().to_path_buf();

    let code = run_repair_code(&root_path, false, false).expect("the plain rebuild runs");
    assert_eq!(
        code,
        ExitCode::from(1),
        "a quarantined pair is a warning without the flag"
    );
    assert_eq!(
        quarantine_entries(&root_path),
        1,
        "the report-only run removes nothing"
    );

    let code = run_repair_code(&root_path, false, true).expect("the destructive run runs");
    assert_eq!(
        code,
        ExitCode::SUCCESS,
        "the resolved quarantine is no finding"
    );
    assert_eq!(
        quarantine_entries(&root_path),
        0,
        "`--delete-orphans` is the action that clears the quarantine"
    );
}

/// `repair --adopt` binds a fresh index to the database and rebuilds the
/// placements from the filesystem rows.
#[tokio::test]
async fn repair_adopt_rebuilds_the_index() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    seed_cold(root.path(), &db.dsn, "cold.bin", b"hello");
    let root_path = root.path().to_path_buf();

    fs::remove_file(index_path(&root_path)).expect("the index is lost");
    assert!(
        !index_path(&root_path).exists(),
        "the adopted run starts without an index"
    );

    run_repair(&root_path, true, false).expect("--adopt binds and rebuilds");

    let placement = placement_of(&root_path, "cold.bin").expect("the cold placement is rebuilt");
    assert_eq!(placement.tier, Tier::Pg);
    assert_eq!(placement.state, State::Live);
    assert_eq!(
        placement.cold.as_ref().map(|cold| cold.etag.clone()),
        Some(ETag::from_content(b"hello").to_string()),
        "the cold metadata is the row's own"
    );
    assert!(
        marker_path(&root_path).exists(),
        "the marker survives the rebuild"
    );
}

/// `repair --delete-orphans` is opt-in: the plain run reports the warm
/// object's unreferenced cold copy and leaves it, and the flag removes it.
#[tokio::test]
async fn repair_delete_orphans_is_opt_in() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    let root_path = root.path().to_path_buf();
    // The demote leaves the warm file behind, so the row has a file and the
    // database has a copy: the orphan shape.
    seed_cold_keeping_file(&root_path, &db.dsn, "warm.bin", b"hello");
    assert!(
        object_file(&root_path, "warm.bin").is_file(),
        "the demote leaves the file behind"
    );

    run_repair(&root_path, false, false).expect("the plain rebuild runs");
    assert!(
        cold_holds(&db.dsn, "warm.bin"),
        "the orphan survives without the flag"
    );
    assert_eq!(
        placement_of(&root_path, "warm.bin").map(|placement| placement.tier),
        Some(Tier::Fs),
        "the object is warm, so the rebuilt placement says so"
    );

    run_repair(&root_path, false, true).expect("the destructive run runs");
    assert!(
        !cold_holds(&db.dsn, "warm.bin"),
        "`--delete-orphans` is the action that removes it"
    );
    assert!(
        object_file(&root_path, "warm.bin").is_file(),
        "the warm object itself is untouched"
    );
}

/// Seed a root with one demoted object and its leftover warm file kept.
fn seed_cold_keeping_file(root: &Path, dsn: &str, name: &str, bytes: &'static [u8]) {
    let root = root.to_path_buf();
    let name = name.to_owned();
    with_tier(&root, dsn, move |tier| {
        Box::pin(async move {
            put_cold_async(&tier, &name, bytes).await;
        })
    });
}

/// `prepare-downgrade` promotes every cold object, removes the index and the
/// marker, and leaves a root the filesystem plane serves alone.
#[tokio::test]
async fn prepare_downgrade_leaves_fs_only() {
    if !require_dsn() {
        return;
    }
    let db = TestDb::create().await.expect("a scratch database");
    let root = tempfile::TempDir::new().expect("a temporary root");
    seed_root(root.path(), &db.dsn);
    seed_cold(root.path(), &db.dsn, "a.bin", b"alpha");
    seed_cold(root.path(), &db.dsn, "b.bin", b"bravo");
    let root_path = root.path().to_path_buf();
    assert!(
        is_tier_root(&root_path).expect("the seam answers"),
        "the root starts in tier mode"
    );

    run_downgrade(&root_path).expect("the downgrade runs");

    assert!(
        !is_tier_root(&root_path).expect("the seam answers"),
        "no tier record survives"
    );
    assert!(!index_path(&root_path).exists(), "the index is removed");
    assert!(!marker_path(&root_path).exists(), "the marker is removed");
    assert!(
        !cold_holds(&db.dsn, "a.bin") && !cold_holds(&db.dsn, "b.bin"),
        "the cold copies are gone"
    );
    assert_eq!(
        fs::read(object_file(&root_path, "a.bin")).expect("a.bin is back"),
        b"alpha"
    );
    assert_eq!(
        fs::read(object_file(&root_path, "b.bin")).expect("b.bin is back"),
        b"bravo"
    );

    // The filesystem plane serves the root alone: the downgraded root is a
    // plain fs root, with both bodies and no cold placement.
    let storage = FsStorage::new(&root_path, testing::fs_options()).expect("the fs root opens");
    let got = storage
        .get_object(&data(), &key("a.bin"), None)
        .await
        .expect("the downgraded root serves a.bin");
    let bytes = read_body(got.body).await.expect("the body reads");
    assert_eq!(bytes, b"alpha");
}
