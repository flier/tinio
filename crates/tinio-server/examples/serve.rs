//! Minimal serve binary for the interop harness (task T032).
//!
//! Serves one storage root over the S3 data plane: `serve <root>
//! [--port N] [--address HOST:PORT]`. The actual bound address is printed
//! to stdout (the harness parses it — needed for `--port 0`). The scanner
//! is on by default (config defaults) and toggled by `TINIO_SCANNER`
//! (`0`/`1`); the sweep runs with default TTLs. Superseded by the full CLI
//! in US2.
//!
//! Auth: s3s rejects signed requests when no auth provider is configured,
//! and real S3 clients always sign — so the harness accepts the fixed
//! MinIO-convention pair `minioadmin` / `minioadmin` (unsigned requests are
//! rejected). Config-based auth lands in US3 (T082/T083).

use std::{
    env, error::Error, fs, net::SocketAddr, path::PathBuf, process, sync::Arc, time::Duration,
};

use futures::StreamExt;
use sweep::{Options, Sweeper};
use tinio_config::{
    Config,
    log::{self, AccessFormat, Format, Verbosity},
    pipeline,
};
use tinio_core::{
    cleanup::{Cleanup, CleanupOptions, RepairKind},
    pipeline::Runner,
    storage::{
        DEFAULT_COMPACT_THRESHOLD_PERCENT, DEFAULT_META_BATCH_BYTES, DEFAULT_META_BATCH_SIZE,
    },
};
use tinio_fs::{
    FsCleanup, FsOptions, FsStorage, Scanner, ScannerOptions, database::WriteLockSnapshot, sweep,
};
use tinio_server::{
    Capabilities, DataPlane, log as server_log,
    metrics::{self, WriteLockStats},
    pipeline::Pipelines,
};
use tokio::{net::TcpListener, sync::watch};
use tracing::subscriber;

fn usage() -> ! {
    eprintln!("usage: serve <root> [--port N] [--address HOST:PORT] [--config <config.toml>]");
    process::exit(2);
}

/// The write-lock snapshot of the fs storage into the metric layer's
/// plain-data form — the wiring-point conversion that keeps
/// `tinio-server`'s metrics decoupled from backend snapshot types.
fn write_lock_stats(snapshot: WriteLockSnapshot) -> WriteLockStats {
    WriteLockStats {
        wait_buckets: snapshot.wait_buckets,
        total_buckets: snapshot.total_buckets,
        count: snapshot.count,
        wait_sum_us: snapshot.wait_sum_us,
        wait_max_us: snapshot.wait_max_us,
        total_sum_us: snapshot.total_sum_us,
        total_max_us: snapshot.total_max_us,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = env::args().skip(1);
    let root = match args.next() {
        Some(root) => root,
        None => usage(),
    };
    let mut address: Option<SocketAddr> = None;
    let mut config_path: Option<PathBuf> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                config_path = Some(match args.next() {
                    Some(path) => path.into(),
                    None => usage(),
                });
            }
            "--port" => {
                let port: u16 = match args.next() {
                    Some(p) => match p.parse() {
                        Ok(port) => port,
                        Err(_) => usage(),
                    },
                    None => usage(),
                };
                address = Some(SocketAddr::from(([127, 0, 0, 1], port)));
            }
            "--address" => {
                let raw = match args.next() {
                    Some(raw) => raw,
                    None => usage(),
                };
                address = Some(match raw.parse() {
                    Ok(addr) => addr,
                    Err(_) => usage(),
                });
            }
            _ => usage(),
        }
    }
    let address = match address {
        Some(addr) => addr,
        None => SocketAddr::from(([127, 0, 0, 1], 9000)),
    };

    // The harness passes a fresh scratch path — create the root if missing
    // (FsStorage itself requires an existing directory).
    fs::create_dir_all(&root)?;
    // The real dual-pipeline runtime (pipeline-spec.md §3.3): `--config
    // <file>` consumes the parsed `[pipeline]` section (F07 — the values
    // an operator sets are no longer silently dropped); without it the
    // section defaults apply (an absent `[pipeline]` resolves to the
    // defaults, Q8).
    let config = match &config_path {
        Some(path) => Some(Config::load(path)?),
        None => None,
    };
    let pipeline_config = match &config {
        Some(config) => config.pipeline.clone().unwrap_or_default(),
        None => Config::default(),
    };
    let pipelines = Pipelines::build(&pipeline_config)?;
    let mut storage = FsStorage::new(
        root,
        FsOptions {
            follow_symlinks: false,
            state_dir: None,
            compact_threshold_percent: DEFAULT_COMPACT_THRESHOLD_PERCENT,
            meta_batch_size: DEFAULT_META_BATCH_SIZE,
            meta_batch_bytes: DEFAULT_META_BATCH_BYTES,
            io_pipeline: pipelines.io(),
            remove_pipeline: pipelines.remove(),
            db_pipeline: pipelines.db(),
        },
    )?;
    // `[s3] max_concurrent_uploads` caps in-progress multipart uploads
    // (default 1000) — an authenticated client cannot accumulate an
    // unbounded number of uploads.
    if let Some(s3) = config.as_ref().and_then(|c| c.s3.as_ref()) {
        storage.set_max_concurrent_uploads(s3.max_concurrent_uploads);
    }
    // Operational logs to stderr (info), access log to `<root>/.tinio/access.log`
    // (T052). The `[log]` config wiring lands with the US2 CLI.
    fs::create_dir_all(storage.state_dir())?;
    let subscriber = server_log::build_subscriber(
        Verbosity::Info,
        Format::Text,
        &AccessFormat::Combined,
        &storage.state_dir().join("access.log"),
        None,
    )?;
    subscriber::set_global_default(subscriber)?;

    // D-B: synchronous Startup repair — the fast, deterministic items
    // (tmp, staging residue, multipart orphans, stale bucket records)
    // run before readiness; the delete-tombstone stage routes through
    // the storage's removal lane (the `remove_pipeline` wired above).
    // Best-effort: a failed stage is warned and readiness proceeds — the
    // scanner covers the residue. The stale-bucket prune runs here,
    // pre-serving, so it stays lock-free (no request can race it yet).
    let cleanup = FsCleanup::new(&storage, CleanupOptions::default());
    match cleanup.repair(RepairKind::Startup).await {
        Ok(mut actions) => {
            while let Some(action) = actions.next().await {
                if let Err(err) = action {
                    tracing::warn!(
                        error = %err,
                        "startup repair step failed; the scanner covers the residue"
                    );
                }
            }
        }
        Err(err) => tracing::warn!(
            error = %err,
            "startup repair failed; the scanner covers the residue"
        ),
    }

    let listener = TcpListener::bind(address).await?;
    println!("listening on {}", listener.local_addr()?);

    // Background scanner (FR-024; `TINIO_SCANNER=0` disables).
    let scanner_options = ScannerOptions {
        enabled: true,
        delay: Duration::from_secs(10),
        max_wait: Duration::from_secs(15),
        cycle: Duration::from_secs(24 * 3600),
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let scanner = Scanner::new(storage.clone(), scanner_options);
    let scanner_task = tokio::spawn(async move {
        scanner.run(shutdown_rx).await;
    });

    // Async sweep (temp 24 h, multipart 7 d).
    let (sweep_tx, sweep_rx) = watch::channel(false);
    let sweeper = Sweeper::new(storage.clone(), Options::default());
    let sweeper_task = tokio::spawn(async move {
        sweeper.run(sweep_rx).await;
    });

    // The `/metrics` scrape endpoint (F10): the plane's hook refreshes
    // the pipeline gauges and the write-lock histograms on every scrape.
    let metrics_storage = storage.clone();
    let metrics_io = pipelines.io();
    let metrics_db = pipelines.db();
    let plane =
        DataPlane::new_with_auth(storage, Capabilities::default(), "minioadmin", "minioadmin")
            .with_metrics(Arc::new(move || {
                metrics::refresh(
                    metrics_io.stats(),
                    metrics_db.stats(),
                    write_lock_stats(metrics_storage.write_lock_stats()),
                );
            }));
    plane.serve(listener, shutdown_tx.subscribe()).await?;
    // Stop the scanner and the sweeper BEFORE the pipelines: the watch
    // signals take effect at the next pass boundary, so awaiting the
    // handles guarantees no pass is mid-flight when the pipelines shut
    // down — a running pass would otherwise keep enqueueing after
    // shutdown and hit Err(ShutDown) on enqueue (a spurious "scanner
    // pass failed" warn at stop, data-path review 2026-08-29 finding 7).
    let _ = shutdown_tx.send(true);
    let _ = sweep_tx.send(true);
    let _ = scanner_task.await;
    let _ = sweeper_task.await;
    // R5 (pipeline-spec.md §3.5): the server has stopped accepting new
    // requests and the background tasks are done — shut the pipelines
    // down in order, IO first and the DB write pipeline last, so
    // in-flight list batches can drain.
    pipelines.shutdown();
    // Await the workers' exit — the in-flight tasks shutdown already
    // guaranteed have visibly completed (item 6c — observability, not
    // new shutdown semantics).
    pipelines.drain().await;
    Ok(())
}
