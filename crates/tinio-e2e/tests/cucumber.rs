//! The single cucumber test binary. All scenarios live in
//! `tests/features/`; step definitions live in `tests/steps/`.
//!
//! Default tag filter: scenarios that need external client binaries
//! (`@interop`/`@boto3`/`@mc`) are excluded unless the user passes an
//! explicit `--tags` on the CLI or sets `TINIO_E2E_EXTERNAL=1` — the
//! same "opt-in" semantics the old `#[ignore]` integration tests had.
//!
//! `TINIO_E2E_REPORT=<path>` additionally writes a Cucumber-JSON report
//! to the given file (CI uses this for the PR test report).
//!
//! `fail_on_skipped()` turns undefined (and any other skipped) steps into
//! failures — a feature/step that drifts out of sync with its step
//! definitions must fail the run, never pass silently as "skipped".

#[doc(hidden)]
pub extern crate tinio_core as _core;
#[doc(hidden)]
pub extern crate tinio_fs as _fs;
#[doc(hidden)]
pub extern crate tinio_mem as _mem;
#[doc(hidden)]
pub extern crate tinio_server as _server;
#[doc(hidden)]
pub extern crate tinio_util as _util;

mod steps;

use cucumber::{
    WriterExt as _,
    writer::{Basic as BasicWriter, Coloring, Json as JsonWriter, Verbosity},
};
use steps::{World, configure};
use tokio::runtime::Builder;

fn main() {
    // Must run before the tokio runtime starts: CUCUMBER_FILTER_TAGS is
    // read by cucumber's CLI parser when no --tags is given. SAFETY: no
    // threads exist yet; the runtime is built below.
    if std::env::var_os("TINIO_E2E_EXTERNAL").is_none() && !std::env::args().any(|a| a == "--tags")
    {
        unsafe {
            std::env::set_var(
                "CUCUMBER_FILTER_TAGS",
                "not @interop and not @boto3 and not @mc",
            );
        }
    }

    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    // The report file opens BEFORE the runtime starts — no blocking fs
    // inside the async block (style.md). The Json writer takes a std
    // `Write`, so the file must be a std one.
    let report_file = std::env::var("TINIO_E2E_REPORT")
        .ok()
        .map(|path| std::fs::File::create(&path).expect("create report file"));
    rt.block_on(async {
        if let Some(file) = report_file {
            // Pretty output to stdout + Cucumber-JSON to the file, mirroring
            // cucumber-rs book "output/multiple.md". (The writer type differs
            // between the branches, so the runners are built separately.)
            configure()
                .init_tracing()
                .with_writer(
                    BasicWriter::new(std::io::stdout(), Coloring::Auto, Verbosity::Default)
                        .summarized()
                        .tee::<World, _>(JsonWriter::new(file).discard_stats_writes()),
                )
                .fail_on_skipped()
                .run_and_exit("tests/features")
                .await;
        } else {
            configure()
                .init_tracing()
                .fail_on_skipped()
                .run_and_exit("tests/features")
                .await;
        }
    });
}
