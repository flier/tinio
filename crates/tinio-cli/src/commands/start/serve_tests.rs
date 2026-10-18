#[cfg(feature = "versioning")]
use std::future::pending;
use std::{path::PathBuf, time::Duration};

use tokio::{
    sync::watch,
    time::{sleep, timeout},
};

use super::{
    Resolution, fs_cleanup, multipart_grace_for, scanner_options, scanner_options_for, stopping,
    sweep, sweep_options_for,
};
#[cfg(feature = "versioning")]
use super::{bounded_versioning_warning, versioning_disabled_warning};
#[cfg(feature = "versioning")]
use crate::_core::{
    acl::Acl,
    bucket,
    storage::{BucketOps, VersionOps},
    version::State,
};
#[cfg(feature = "versioning")]
use crate::_server::Capabilities;
use crate::{
    _core::mode::AccessMode,
    _fs::{FsStorage, testing},
    config::{Config, s3, scanner},
};

/// **The drain bound is armed by a stop, never by the clock**, and this leg is the defect in
/// the form a unit test can hold: with the channel still `false`, [`stopping`] must not
/// resolve. `serve` used to arm `DRAIN_BOUND` at exactly this point, so an idle server walked
/// the drain path ~12 s after readiness, removed `state` and exited.
#[tokio::test]
async fn an_unstopped_server_never_reaches_its_drain() {
    let (_sender, mut stop) = watch::channel(false);
    assert!(
        timeout(Duration::from_millis(50), stopping(&mut stop))
            .await
            .is_err(),
        "nothing asked for a stop, so the drain must not begin"
    );
}

/// **The two orders a stop can arrive in are equivalent, and the second is why the current
/// value is read before `changed()` is awaited**: a receiver that already saw the flip would
/// otherwise wait for the *next* change, which never comes.
#[tokio::test]
async fn a_stop_starts_the_drain_in_either_order() {
    // The flip lands during the wait.
    let (sender, mut stop) = watch::channel(false);
    let flip = tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        let _ = sender.send(true);
    });
    assert!(
        timeout(Duration::from_secs(5), stopping(&mut stop))
            .await
            .is_ok(),
        "a stop that arrived during the wait must start the drain"
    );
    flip.await.expect("the flip task");

    // The flip landed before the wait.
    let (sender, mut stop) = watch::channel(false);
    sender.send(true).expect("flip before the wait");
    assert!(
        timeout(Duration::from_millis(50), stopping(&mut stop))
            .await
            .is_ok(),
        "a stop that arrived before the wait must start the drain"
    );
}

/// The toggle-off startup warning: with versioning compiled in, a root that holds version
/// data is named by count when `[s3] versioning` is off — and the warning is silent when the
/// toggle is on or the root holds no version data (spec 2026-10-03).
#[cfg(feature = "versioning")]
#[tokio::test]
async fn the_versioning_disabled_warning_names_the_buckets_that_hold_version_data() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(dir.path(), testing::fs_options()).unwrap();
    let off = Capabilities {
        versioning: false,
        ..Default::default()
    };
    // A root with no version data is silent, whatever the toggle says.
    assert!(versioning_disabled_warning(&storage, &off).await.is_none());
    // One versioned bucket: the message's verb agrees with its count.
    let alpha = bucket::name("alpha").unwrap();
    storage
        .create_bucket(&alpha, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&alpha, State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        versioning_disabled_warning(&storage, &off).await.as_deref(),
        Some(
            "[s3] versioning is off but 1 bucket holds version data; current versions keep serving and every version operation is refused"
        ),
    );
    // Two: the plural spelling, unchanged.
    let beta = bucket::name("beta").unwrap();
    storage
        .create_bucket(&beta, None, &Acl::default_private(None))
        .await
        .unwrap();
    storage
        .set_bucket_versioning(&beta, State::Enabled)
        .await
        .unwrap();
    assert_eq!(
        versioning_disabled_warning(&storage, &off).await.as_deref(),
        Some(
            "[s3] versioning is off but 2 buckets hold version data; current versions keep serving and every version operation is refused"
        ),
    );
    // The toggle on is silent on the very same root.
    let on = Capabilities {
        versioning: true,
        ..Default::default()
    };
    assert!(versioning_disabled_warning(&storage, &on).await.is_none());
}

/// The bounded warning's timed-out arm — the arm whose absence kept the shutdown join one
/// wedged scan away from hanging the process. A scan that never answers must still produce a
/// warning that names the condition, and it must not pretend a count. [`pending`] stands in
/// for the wedged read so the arm is driven deterministically; a real [`FsStorage`]'s scan has
/// no seam a test can hold open, which is why the bound is the wrapper's parameter.
#[cfg(feature = "versioning")]
#[tokio::test]
async fn a_timed_out_versioning_warning_names_the_condition_without_a_count() {
    assert_eq!(
        bounded_versioning_warning(Duration::from_millis(10), pending::<Option<String>>())
            .await
            .as_deref(),
        Some(
            "[s3] versioning is off and this root may hold version data, but the count timed out; current versions keep serving and every version operation is refused"
        ),
    );
    // A scan that answers inside the bound passes through untouched: the bound is a
    // degradation path, never a rewrite of what the scan said.
    assert_eq!(
        bounded_versioning_warning(Duration::from_secs(5), async {
            Some("the scan's own answer".to_owned())
        })
        .await
        .as_deref(),
        Some("the scan's own answer"),
    );
}

/// A resolved-config fixture: the `[s3]` section the legs under it exercise is handed in
/// **directly**, and every other section is the defaults.
///
/// **The trade this makes, stated rather than hidden.** Building the section as a struct
/// rather than parsing a TOML fixture means these legs pin the **conversions** — `temp_ttl_hours`
/// and the days/minutes keys to the `Duration`s the sweeper reads — and not the key names or
/// their units. That half lives in the root parser's own tests (`config/mod.rs`), which parse the
/// very keys: `versioning_defaults_on_and_the_retention_knobs_default_to_keep_everything`
/// (temp/multipart/`noncurrent_keep`/`noncurrent_expire_days`/`block_grace_minutes` at their
/// values) and `a_zero_sweep_ttl_is_refused`/`a_zero_block_grace_is_refused` (the floors). So
/// no key name or unit is left unpinned by the tree, only by this file.
fn resolution(s3: Option<s3::Config>) -> Resolution {
    Resolution {
        pg_root: None,
        root: PathBuf::from("/srv/data"),
        config: Config {
            s3,
            ..Config::default()
        },
        identity: Default::default(),
        config_text: String::new(),
        config_path: PathBuf::from("/srv/data/.tinio/.tinio.toml"),
        read_only: false,
        state_dir: PathBuf::from("/srv/data/.tinio"),
        state_dir_override: None,
        config_error: None,
    }
}

/// R2: the `[s3]` TTL and retention keys reach the sweep pass — the section is handed in
/// directly (see [`resolution`]), so the hours/days/minutes-to-seconds **conversions** are what
/// is pinned here, and the same resolved section the neighbouring `capabilities_for` reads is
/// what feeds it.
#[test]
fn the_sweep_options_carry_the_resolved_ttl_and_retention_keys() {
    let options = sweep_options_for(&resolution(Some(s3::Config {
        temp_ttl_hours: 6,
        multipart_expire_days: 2,
        noncurrent_keep: 3,
        noncurrent_expire_days: 30,
        block_grace_minutes: 45,
        ..Default::default()
    })));
    assert_eq!(
        options.temp_ttl,
        Duration::from_secs(6 * 3600),
        "the config's hours are the sweeper's seconds"
    );
    assert_eq!(
        options.multipart_ttl,
        Duration::from_secs(2 * 24 * 3600),
        "the config's days are the sweeper's seconds"
    );
    assert_eq!(options.noncurrent_keep, 3);
    assert_eq!(options.noncurrent_expire_days, 30);
    assert_eq!(
        options.block_grace,
        Duration::from_secs(45 * 60),
        "the config's minutes are the sweeper's seconds"
    );

    // Without an `[s3]` section every default applies, and the versioning keys' defaults are
    // the safe reading: keep every noncurrent version, never expire by age, ten minutes'
    // grace — beside the sweeper's 24 h temp and 7 d multipart TTLs.
    assert_eq!(
        sweep_options_for(&resolution(None)),
        sweep::Options::default()
    );
}

/// `[scanner]` presence is the enabled gate (FR-024): an absent section leaves the scanner off
/// with the documented `10s`/`15s`/`24h` pacing, a present one turns it on with those same
/// defaults when it omits a key, and its three keys are the scanner's own when written. The
/// access mode travels through unchanged — the read-only early return is not this mapping's.
#[test]
fn scanner_options_map_the_section_presence_and_pacing() {
    let write = AccessMode::ReadWrite;
    let read_only = AccessMode::ReadOnly;

    let absent = scanner_options(None, write);
    assert!(!absent.enabled);
    assert_eq!(absent.delay, Duration::from_secs(10));
    assert_eq!(absent.max_wait, Duration::from_secs(15));
    assert_eq!(absent.cycle, Duration::from_secs(24 * 3600));
    assert_eq!(absent.access, write);

    let present = scanner_options(Some(&scanner::Config::default()), read_only);
    assert!(present.enabled);
    assert_eq!(present.delay, Duration::from_secs(10));
    assert_eq!(present.max_wait, Duration::from_secs(15));
    assert_eq!(present.cycle, Duration::from_secs(24 * 3600));
    assert_eq!(present.access, read_only);

    let custom = scanner::Config {
        delay: 2.5,
        max_wait: Duration::from_secs(60),
        cycle: Duration::from_secs(2 * 3600),
    };
    let options = scanner_options(Some(&custom), write);
    assert!(options.enabled);
    assert_eq!(options.delay, Duration::from_millis(2_500));
    assert_eq!(options.max_wait, Duration::from_secs(60));
    assert_eq!(options.cycle, Duration::from_secs(2 * 3600));
    assert_eq!(options.access, write);

    let non_finite = scanner::Config {
        delay: f64::INFINITY,
        ..scanner::Config::default()
    };
    assert_eq!(
        scanner_options(Some(&non_finite), write).delay,
        Duration::from_secs(10)
    );
}

/// The resolved configuration's own section is what [`scanner_options_for`] maps — the keys as
/// the parser reads them, so the config field to the options is pinned, not only the mapping
/// function.
#[test]
fn scanner_options_for_reads_the_resolved_scanner_section() {
    let config =
        Config::parse("version = 1\n[scanner]\ndelay = 2.5\nmax_wait = \"1m\"\ncycle = \"2h\"")
            .unwrap();
    let configured = Resolution {
        config,
        ..resolution(None)
    };
    let options = scanner_options_for(&configured, AccessMode::ReadWrite);
    assert!(options.enabled);
    assert_eq!(options.delay, Duration::from_millis(2_500));
    assert_eq!(options.max_wait, Duration::from_secs(60));
    assert_eq!(options.cycle, Duration::from_secs(2 * 3600));

    let absent = scanner_options_for(&resolution(None), AccessMode::ReadWrite);
    assert!(!absent.enabled);
}

/// The startup repair hands the cleanup the resolved `[s3] multipart_expire_days` as its
/// orphan-upload grace: the very value the sweeper's `multipart_ttl` carries, so the repair
/// can no longer judge orphans by the hardcoded 7-day default while the sweeper honours the
/// configured clock.
///
/// **The built cleanup is asserted, not only the value.** `repair` runs whatever
/// [`fs_cleanup`] returns; a test over [`multipart_grace_for`] alone pins the converted
/// value but not whether it is chained onto that cleanup at all, which is the half a
/// reviewer could only read-verify.
#[test]
fn the_startup_repair_grace_is_the_resolved_multipart_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let storage = FsStorage::new(dir.path(), testing::fs_options()).unwrap();

    let configured = resolution(Some(s3::Config {
        multipart_expire_days: 2,
        ..Default::default()
    }));
    assert_eq!(
        multipart_grace_for(&configured),
        Duration::from_secs(2 * 24 * 3600),
        "the config's days are the cleanup's seconds"
    );
    assert_eq!(
        fs_cleanup(&storage, &configured).multipart_grace(),
        Duration::from_secs(2 * 24 * 3600),
        "the cleanup the repair runs carries the configured grace"
    );

    let absent = resolution(None);
    assert_eq!(
        multipart_grace_for(&absent),
        Duration::from_secs(7 * 24 * 3600),
        "without an [s3] section the sweeper's own default applies"
    );
    assert_eq!(
        fs_cleanup(&storage, &absent).multipart_grace(),
        Duration::from_secs(7 * 24 * 3600),
        "and the cleanup the repair runs falls back to that same default"
    );
}

// ---- the pg backend serves: the phase's headline, a round trip through the database ----

#[cfg(all(feature = "pg", feature = "api"))]
use std::{
    env, fs,
    net::SocketAddr,
    process::{self, ExitCode},
};

#[cfg(all(feature = "pg", feature = "api"))]
use tokio::runtime::Builder;

#[cfg(all(feature = "pg", feature = "api"))]
use super::{Args, Ready, Served, pg_pass, serve};
#[cfg(all(feature = "pg", feature = "api"))]
use crate::{
    _api::state::State as ApiState,
    _pg::{
        _store_pg::_sqlx::{Executor as _, PgPool, query, query_scalar},
        Access as PgAccess, Config as PgConfig, PgStorage,
    },
    _server::identity::auth,
    _store_pg::test_support::TestDb,
    _util::secret::{Secret, SecretKey},
    config::{
        server,
        storage::{self, Backend, pg},
    },
    resolve::Pipelines,
    signal,
};

/// **The phase's headline: a `type = "pg"` configuration *serves*, and a request round-trips
/// through the database.** This drives the real [`serve`] path with a [`Served::Pg`] backend —
/// the exact value `run` now produces for a pg configuration, where the interim refusal used to
/// sit — against a **scratch database** (never the shared `tinio` one: PG12's ownership lock is
/// process-lifetime, and the shared fixture belongs to another slice), PUTs a bucket and an
/// object, GETs the object back and asserts the bytes, then stops the server and asserts a
/// clean exit. Skipped (not failed) without `TINIO_PG_DSN`, so `cargo test` stays
/// database-free (PG8).
///
/// **What this proves that the `@pg` pass cannot**: the pass serves pg *in process* on its own
/// `DataPlane`; this leg proves the **CLI's** serving path constructs the pg backend inside its
/// own runtime, takes the ownership guard, binds its management plane, repairs and serves — the
/// refusal this slice removed is gone, and the thing it refused now answers a request.
#[cfg(all(feature = "pg", feature = "api"))]
#[test]
fn a_pg_configuration_serves_and_round_trips_a_request() {
    let Some(_) = env::var("TINIO_PG_DSN").ok().filter(|dsn| !dsn.is_empty()) else {
        eprintln!("skipping: TINIO_PG_DSN is unset (PG8)");
        return;
    };

    // **A synchronised `#[test]`, not `#[tokio::test]`, for the ownership guard's sake.** a
    // writable pg backend spawns PG12's guard, which lives on the serving runtime and
    // *terminates the process* (`OWNERSHIP_LOST_EXIT`) the moment it finds the lock lost. So
    // the runtime is built by hand and **dropped before the scratch database is**: dropping
    // the runtime aborts the guard task, and only then is it safe to drop the database the
    // guard's connection held a lock on.
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the test runtime");
    let db = runtime.block_on(run_pg_serve_round_trip());
    drop(runtime);
    drop_test_db(db);
}

/// The round-trip body: a scratch database, the real [`serve`] path with a [`Served::Pg`]
/// backend, one signed request journey, and a clean stop. Returns the scratch database so the
/// caller can drop it **after the runtime (and the ownership guard) is gone**.
#[cfg(all(feature = "pg", feature = "api"))]
async fn run_pg_serve_round_trip() -> TestDb {
    const ACCESS: &str = "cli-serve-test";
    const SECRET: &str = "cli-serve-test-secret";

    let db = TestDb::create().await.expect("a scratch database");
    let dsn = db.dsn.clone();
    let scratch = tempfile::TempDir::new().expect("a scratch dir");
    let root = scratch.path().join("pg");
    let state_dir = root.join(".tinio");
    fs::create_dir_all(&state_dir).expect("the state directory");

    // A pg configuration, a loopback ephemeral bind and one credential pair — the shape the
    // CLI resolves for `[storage] type = "pg"`. Built as a struct, never parsed from TOML: the
    // key names and units are the config root's own tests' to pin, and this leg is about the
    // serving path, not the schema.
    let secret = Secret::from(SecretKey::from(dsn));
    let config = Config {
        server: server::Config {
            host: "127.0.0.1".to_owned(),
            port: 0,
            ..Default::default()
        },
        auth: Some(auth::Config {
            access_key: ACCESS.to_owned(),
            secret_key: Secret::from(SecretKey::from(SECRET.to_owned())),
        }),
        storage: Some(storage::Config {
            backend: Backend::Pg,
            pg: Some(pg::Config {
                dsn: Some(secret.clone()),
                pool: 4,
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let resolution = Resolution {
        pg_root: None,
        root: root.clone(),
        config,
        identity: Default::default(),
        config_text: String::new(),
        config_path: state_dir.join(".tinio.toml"),
        read_only: false,
        state_dir: state_dir.clone(),
        state_dir_override: None,
        config_error: None,
    };
    let pipeline_section = resolution.config.pipeline.clone().unwrap_or_default();
    let pipelines = Pipelines::build(&pipeline_section).expect("the pipelines build");

    // `signal::Handles` is a plain pair of a `watch` channel (its doc says so), so the test
    // builds one instead of installing process-global signal handlers.
    let (stopper, shutdown) = watch::channel(false);
    let handles = signal::Handles {
        shutdown,
        stopper: stopper.clone(),
    };
    let ready = Ready {
        resolution,
        args: Args::default(),
        adopted: None,
        handles,
        backend: Served::Pg {
            config: PgConfig::new(secret, 4),
            access: PgAccess::ReadWrite,
        },
        pipelines,
        on_ready: None,
    };

    let serving = tokio::spawn(serve(ready));

    // Readiness is the published `state` file — the same discovery a client uses — and it
    // names the bound port. Polled with a bound so a wedged start is a failure, not a hang.
    let port = timeout(Duration::from_secs(90), async {
        loop {
            if let Ok(Some(state)) = ApiState::read(&state_dir) {
                break state.port.expect("the state file names the bound port");
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the pg server became ready inside the bound");

    let addr: SocketAddr = ([127, 0, 0, 1], port).into();
    let host = format!("127.0.0.1:{port}");
    let bucket = format!("/cli-serve-{}", process::id());
    let payload = b"a request that round-trips the pg database\n";

    let (status, _) = send(addr, &host, "PUT", &bucket, b"", ACCESS, SECRET).await;
    assert_eq!(status, 200, "the bucket is created through the pg backend");

    let path = format!("{bucket}/hello.txt");
    let (status, _) = send(addr, &host, "PUT", &path, payload, ACCESS, SECRET).await;
    assert_eq!(status, 200, "the object is written through the pg backend");

    let (status, body) = send(addr, &host, "GET", &path, b"", ACCESS, SECRET).await;
    assert_eq!(status, 200, "the object is read back");
    assert_eq!(body, payload, "the bytes round-tripped the database");

    // A clean stop: the drain path removes `state` and the run answers exit 0.
    stopper.send(true).expect("ask the server to stop");
    let code = timeout(Duration::from_secs(30), serving)
        .await
        .expect("the server stopped inside the bound")
        .expect("the serving task joins")
        .expect("a clean exit");
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(
        matches!(ApiState::read(&state_dir), Ok(None)),
        "the stop removed the state file"
    );

    db
}

/// Drop the scratch database on a runtime of its own. Called **after** the serving runtime
/// (and so the ownership guard) is gone: dropping the database while the guard still holds
/// its connection makes the guard's next probe end the test process.
#[cfg(all(feature = "pg", feature = "api"))]
fn drop_test_db(db: TestDb) {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the drop runtime");
    runtime.block_on(db.drop_db());
}

/// **The pg background pass runs the whole serving sweep** (PG7): this is the leg that proves a
/// served pg instance reclaims on a cadence, not only at startup, and that it runs **what the
/// filesystem sweeper runs** — so `[s3] noncurrent_keep` / `noncurrent_expire_days` are not
/// inert on a served pg instance. After one pass: a planted orphan chunk is gone (the residue
/// scan), the older of two noncurrent versions is gone while the newest is spared (the
/// retention), and a block no row references is gone (the block sweep). The read-only arm
/// builds no pass at all (a long delay is not slept).
///
/// Skipped without `TINIO_PG_DSN` (PG8). A synchronised `#[test]`, for the round-trip leg's
/// reason: the pass needs a runtime, the storage takes PG12's ownership guard (which lives on
/// that runtime and terminates the process if it finds the lock lost), and the scratch database
/// is dropped only after the runtime is gone.
#[cfg(all(feature = "pg", feature = "api"))]
#[test]
fn the_pg_background_pass_runs_the_residue_retention_and_block_stages() {
    let Some(_) = env::var("TINIO_PG_DSN").ok().filter(|dsn| !dsn.is_empty()) else {
        eprintln!("skipping: TINIO_PG_DSN is unset (PG8)");
        return;
    };
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("the test runtime");
    let db = runtime.block_on(run_pg_pass_leg());
    drop(runtime);
    drop_test_db(db);
}

/// The pass leg's body: plant all three shapes the pass must handle, drive a pass with an
/// injected tick, watch them go, then prove the read-only arm sleeps nothing. Returns the
/// scratch database so the caller can drop it after the runtime is gone.
#[cfg(all(feature = "pg", feature = "api"))]
async fn run_pg_pass_leg() -> TestDb {
    let db = TestDb::create().await.expect("a scratch database");
    let dsn = db.dsn.clone();
    let secret = Secret::from(SecretKey::from(dsn.clone()));

    // A writable store: it migrates the scratch database and takes the ownership guard.
    let storage = PgStorage::connect(&PgConfig::new(secret.clone(), 4), PgAccess::ReadWrite)
        .await
        .expect("construct the writable pg backend");

    // **The three shapes the serving pass must handle** — the residue the fs scanner reclaims,
    // and the retention and block sweep the fs *sweeper* runs: an orphan chunk (a `content` row
    // no head names, stamped past the grace), two noncurrent version rows at one key, and a
    // block no row references.
    let pool = PgPool::connect(&dsn).await.expect("the planting pool");
    for stmt in [
        "INSERT INTO buckets (name, created, tags, acl, cors) \
         VALUES ('cli-pass', now(), '{}'::jsonb, \
         ROW(NULL::text, ARRAY[]::acl_grant[])::acl, '')",
        "INSERT INTO content (bucket, kind, ckey, seq, chunk_no, chunk, published) \
         VALUES ('cli-pass', 'object', 'o:orphan.txt', 7, 0, '\\x00'::bytea, \
         now() - interval '2 hours')",
        "INSERT INTO blocks (hash, seq, chunk, len, published) \
         VALUES ('0000000000000000000000000000000000000000000000000000000000000abc', 0, \
         '\\x00'::bytea, 1, now() - interval '1 hour')",
    ] {
        pool.execute(stmt).await.expect("plant a residue row");
    }
    // Two history versions at one key, both long past any retention clock; a `keep` budget of
    // one expires the older. `became_noncurrent` is the nanos stamp the retention reads.
    for (seq, id) in [(1_i64, "planted-v1"), (2_i64, "planted-v2")] {
        query(
            "INSERT INTO version_rows (bucket, key, seq, id, blob, size, etag, created, \
             became_noncurrent, kind, tags, checksum, acl) \
             VALUES ('cli-pass', 'v.txt', $1, $2, '', 0, 'planted', now(), 1, 'object', \
             '{}'::jsonb, '', ROW(NULL::text, ARRAY[]::acl_grant[])::acl)",
        )
        .bind(seq)
        .bind(id)
        .execute(&pool)
        .await
        .expect("a planted version row");
    }

    // One pass, on an injected tick: the delay is milliseconds rather than PG_PASS_DELAY, so
    // the leg tests the pass rather than the constant. The cycle is long, so a second pass
    // never races the assertions. The retention options are the leg's own — `keep = 1` so the
    // retention stage actually runs (`0` is the "keep every version" reading and returns
    // early), `expire = 0` so only the keep budget decides.
    let (stopper, shutdown) = watch::channel(false);
    let pass = tokio::spawn(pg_pass(
        storage.clone(),
        shutdown,
        AccessMode::ReadWrite,
        sweep::Options {
            noncurrent_keep: 1,
            noncurrent_expire_days: 0,
            block_grace: Duration::from_secs(600),
            ..Default::default()
        },
        Duration::from_millis(20),
        Duration::from_secs(3600),
    ));
    timeout(Duration::from_secs(30), async {
        loop {
            if rows(
                &pool,
                "SELECT count(*) FROM content WHERE bucket = 'cli-pass' \
                 AND ckey = 'o:orphan.txt'",
            )
            .await
                == 0
            {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the pass reclaimed the orphan inside the bound");
    stopper.send(true).expect("stop the pass");
    timeout(Duration::from_secs(5), pass)
        .await
        .expect("the pass stopped inside the bound")
        .expect("the pass task joins");

    // The residue scan ran…
    assert_eq!(
        rows(
            &pool,
            "SELECT count(*) FROM content WHERE bucket = 'cli-pass' \
             AND ckey = 'o:orphan.txt'",
        )
        .await,
        0,
        "the orphan chunk is reclaimed"
    );
    // …the **retention** ran (F4's subject: a served pg instance expires noncurrent versions
    // by `[s3] noncurrent_keep` exactly as a served fs instance does)…
    assert_eq!(
        rows(
            &pool,
            "SELECT count(*) FROM version_rows WHERE bucket = 'cli-pass' AND seq = 1",
        )
        .await,
        0,
        "the keep budget expires the older noncurrent version"
    );
    assert_eq!(
        rows(
            &pool,
            "SELECT count(*) FROM version_rows WHERE bucket = 'cli-pass' AND seq = 2",
        )
        .await,
        1,
        "and spares the newest history row"
    );
    // …and the **block sweep** ran.
    assert_eq!(
        rows(&pool, "SELECT count(*) FROM blocks").await,
        0,
        "the unreferenced block is reclaimed, not merely reported"
    );

    // **The read-only arm builds no pass**: with the delay set to an hour, the pass returns at
    // once rather than sleeping it — the mode promises to write nothing, and a pass's whole
    // purpose is to remove rows.
    let read_only = PgStorage::connect(&PgConfig::new(secret, 4), PgAccess::ReadOnly)
        .await
        .expect("construct the read-only pg backend");
    let (_ro_stopper, ro_shutdown) = watch::channel(false);
    timeout(
        Duration::from_secs(2),
        pg_pass(
            read_only,
            ro_shutdown,
            AccessMode::ReadOnly,
            sweep::Options::default(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        ),
    )
    .await
    .expect("a read-only pass returns at once rather than sleeping its delay");

    db
}

/// One scalar count, for the pass leg's assertions.
#[cfg(all(feature = "pg", feature = "api"))]
async fn rows(pool: &PgPool, sql: &'static str) -> i64 {
    query_scalar::<_, i64>(sql)
        .fetch_one(pool)
        .await
        .expect("count the rows")
}

/// One signed request against the served data plane, returning the status and the body.
#[cfg(all(feature = "pg", feature = "api"))]
async fn send(
    addr: SocketAddr,
    host: &str,
    method: &str,
    path: &str,
    body: &[u8],
    access: &str,
    secret: &str,
) -> (u16, Vec<u8>) {
    use tinio_util::testing::{http::exchange, sigv4::Signer};
    // The one SigV4 signer, shared with `tinio-e2e`'s acl steps (`tinio_util::testing::sigv4`):
    // path-style, `s3`, `us-east-1`, signed with `x-amz-content-sha256` and `x-amz-date`, which
    // is exactly what `s3s` verifies. **Test-only**: the CLI's served path never signs anything.
    let signed = Signer::new(access, secret).sign(method, path, &[], body, host);
    let headers: Vec<(&str, &str)> = signed
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    // The framing is the shared one; this server is in-process, so nothing can be lost in an
    // accept queue and the retry budget is none.
    let response = exchange(host, addr, method, path, &headers, body, 0).await;
    (response.status, response.body)
}
