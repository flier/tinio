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

use std::{net::SocketAddr, time::Duration};

use tokio::sync::watch;

use tinio_config::{AccessLogFormat, LogFormat, Verbosity};
use tinio_fs::{FsOptions, FsStorage, Scanner, ScannerOptions, SweepOptions, Sweeper};
use tinio_server::{Capabilities, DataPlane, log};

fn usage() -> ! {
    eprintln!("usage: serve <root> [--port N] [--address HOST:PORT]");
    std::process::exit(2);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let root = match args.next() {
        Some(root) => root,
        None => usage(),
    };
    let mut address: Option<SocketAddr> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
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
    std::fs::create_dir_all(&root)?;
    let storage = FsStorage::new(root, FsOptions::default())?;
    // Operational logs to stderr (info), access log to `<root>/.tinio/access.log`
    // (T052). The `[log]` config wiring lands with the US2 CLI.
    std::fs::create_dir_all(storage.state_dir())?;
    let subscriber = log::build_subscriber(
        Verbosity::Info,
        LogFormat::Text,
        &AccessLogFormat::Combined,
        &storage.state_dir().join("access.log"),
        None,
    )?;
    tracing::subscriber::set_global_default(subscriber)?;

    let listener = tokio::net::TcpListener::bind(address).await?;
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
    tokio::spawn(async move {
        scanner.run(shutdown_rx).await;
    });

    // Async sweep (temp 24 h, multipart 7 d).
    let (sweep_tx, sweep_rx) = watch::channel(false);
    let sweeper = Sweeper::new(storage.clone(), SweepOptions::default());
    tokio::spawn(async move {
        sweeper.run(sweep_rx).await;
    });

    let plane =
        DataPlane::new_with_auth(storage, Capabilities::default(), "minioadmin", "minioadmin");
    plane.serve(listener, shutdown_tx.subscribe()).await?;
    let _ = (shutdown_tx, sweep_tx);
    Ok(())
}
