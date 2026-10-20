//! The CLI's spans, asserted from a dedicated binary.
//!
//! **A separate target rather than a module of the lib test binary**, for the reason
//! tinio-sandbox's span test carries: the recorder is the process default, so a capture opened
//! beside another test would read its spans. This binary has one test and nothing else races the
//! instrumented callsites.
//!
//! **The legs are the refusals, deliberately.** start::run is driven into its synthetic-root
//! refusal, stop into "no running server" and cli::run into its usage exit: each is the same T0
//! span a successful run opens, it closes on the refusal arm rather than only the happy one, and
//! no leg binds a port, installs a signal handler or writes a root. service::run is the debug
//! leg, refused at its root resolution before it reaches a supervisor.

#![cfg(feature = "api")]

use std::{path::PathBuf, process::ExitCode};

use tinio_cli::{
    cli::Provenance,
    commands::{EnvArgs, start, stop},
    resolve::Located,
    service,
};
use tinio_util::testing::spans::{self, one_named};
use tokio::runtime::Builder;
use tracing::Level;

/// One span of the given name, which the legs below open exactly once.
fn one(name: &str) -> spans::RecordedSpan {
    let recorded = spans::spans();
    one_named(&recorded, name).clone()
}

#[test]
fn the_cli_lifecycle_opens_its_spans() {
    spans::install();

    // **start.run** - the synthetic pg root with no configuration is refused before anything is
    // created, which is the cheapest path through the pre-runtime half's T0 span.
    let synthetic = PathBuf::from("/synthetic/tinio-pg-root");
    let located = Located {
        root: synthetic.clone(),
        read_only: false,
        state_dir: synthetic.join(".tinio"),
        config: None,
        pg_root: Some(synthetic),
        state_dir_override: None,
        config_doc: None,
    };
    let refused = start::run(
        start::Args::default(),
        located,
        None,
        &Provenance::default(),
    );
    assert!(refused.is_err(), "the synthetic root is refused");

    // **stop** - a root no server answers for is the command's refusal arm; the span is the same
    // one a successful stop opens.
    let dir = tempfile::TempDir::new().expect("temp dir");
    let located = Located {
        root: dir.path().to_path_buf(),
        read_only: true,
        state_dir: dir.path().join(".tinio"),
        config: None,
        pg_root: None,
        state_dir_override: None,
        config_doc: None,
    };
    let stop = stop::run(
        stop::Args {
            dir: Some(dir.path().to_path_buf()),
            read_only: true,
            env: EnvArgs::default(),
        },
        located,
    );
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let stopped = runtime.block_on(stop);
    assert!(stopped.is_err(), "no server is running for the root");

    // **service.run** - the debug leg's dispatch, refused at its root resolution before it can
    // reach a supervisor.
    let service = service::run(service::Args {
        verb: service::Verb::Query,
        dir: Some(PathBuf::from("/nonexistent/tinio-service-root")),
        name: None,
        user: false,
        read_only: true,
        env: EnvArgs::default(),
    });
    assert!(service.is_err(), "an unusable root is refused");

    // **cli.run** - the test harness's own argv is not a tinio command line, so the parse refuses
    // and the entry point's span closes on exit 2. The span is opened before the parse, which is
    // the whole point of it being the process's outermost unit.
    assert_eq!(tinio_cli::run(), ExitCode::from(2));

    // One assertion per span: name, target and level, never a recorded field's text.
    for (name, level) in [
        ("cli.run", Level::INFO),
        ("start.run", Level::INFO),
        ("stop", Level::INFO),
        ("service.run", Level::DEBUG),
    ] {
        let span = one(name);
        assert_eq!(span.target, "tinio::cli", "{name}");
        assert_eq!(span.level, level, "{name}");
        assert!(
            span.fields.is_empty(),
            "{name} records no fields: this task's spans are skip_all, so no path, token or \
             configuration text has a route onto one"
        );
    }
}
