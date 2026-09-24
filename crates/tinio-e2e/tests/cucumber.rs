//! The single cucumber test binary. All scenarios live in
//! `tests/features/`; step definitions live in `tests/steps/`.
//!
//! Default tag filter: scenarios that cannot pass in this build are excluded
//! unless the user passes an explicit `--tags` on the CLI. The four exclusion
//! axes are independent: `@parquet` drops out unless the binary was compiled
//! `--features parquet` (the parquet input reader lives behind
//! `tinio-server/select-parquet`, off by default); the platform-bound
//! scenarios (`@unix`/`@windows`) drop out on the platform whose step
//! definitions do not exist, because `fail_on_skipped()` would turn their
//! undefined steps into a failure; the external client tags
//! (`@interop`/`@boto3`/`@mc`) drop out unless `TINIO_E2E_EXTERNAL=1` — the
//! same "opt-in" semantics the old `#[ignore]` integration tests had; and the
//! host-bound confinement scenarios (`@confinement`) run on macOS, where the
//! stub backend owes one deterministic refusal, and drop out elsewhere unless
//! `TINIO_E2E_CONFINEMENT=1` — a target whose backend confines pays the
//! launcher's ACL grant, and no budget here covers what `docs/tests.md`'s
//! "Grant cost" measures (see that section's owner rule; the platform
//! scripts are the real gate).
//!
//! `TINIO_E2E_REPORT=<path>` additionally writes a Cucumber-JSON report
//! to the given file (CI uses this for the PR test report).
//!
//! `fail_on_skipped()` turns undefined (and any other skipped) steps into
//! failures — a feature/step that drifts out of sync with its step
//! definitions must fail the run, never pass silently as "skipped".

use std::{env, fs::File, io};
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
    //
    // Three independent axes: the cargo feature decides `@parquet` (without the
    // reader compiled in, those scenarios can only 501), the target platform
    // decides `@unix`/`@windows` (each platform-bound scenario's steps exist
    // only where it can run — the systemd notify socket is a unix datagram)
    // and `@confinement` (macOS's stub owes a refusal; a launcher target owes
    // an ACE grant whose measured cost tens of seconds of variance), and the
    // env opt-ins decide the external-client tags (they need third-party
    // binaries) and the confinement scenarios off macOS.
    if !env::args().any(|a| a == "--tags") {
        let mut excluded = Vec::new();
        if !cfg!(feature = "parquet") {
            excluded.push("@parquet");
        }
        excluded.push(if cfg!(windows) { "@unix" } else { "@windows" });
        if env::var_os("TINIO_E2E_EXTERNAL").is_none() {
            excluded.extend(["@interop", "@boto3", "@mc"]);
        }
        // `@confinement` is host-bound: macOS answers with the stub's refusal,
        // while Linux/Windows run the confinement and pay the launcher's ACL
        // grant — measured here at 4.6 s one run and past 120 s the next, so a
        // leg cannot budget it and the capable host opts in.
        if !cfg!(target_os = "macos") && env::var_os("TINIO_E2E_CONFINEMENT").is_none() {
            excluded.push("@confinement");
        }
        if !excluded.is_empty() {
            let filter = excluded
                .iter()
                .map(|tag| format!("not {tag}"))
                .collect::<Vec<_>>()
                .join(" and ");
            unsafe {
                env::set_var("CUCUMBER_FILTER_TAGS", filter);
            }
        }
    }

    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    // The report file opens BEFORE the runtime starts — no blocking fs
    // inside the async block (style.md). The Json writer takes a std
    // `Write`, so the file must be a std one.
    let report_file = env::var("TINIO_E2E_REPORT")
        .ok()
        .map(|path| File::create(&path).expect("create report file"));
    rt.block_on(async {
        if let Some(file) = report_file {
            // Pretty output to stdout + Cucumber-JSON to the file, mirroring
            // cucumber-rs book "output/multiple.md". (The writer type differs
            // between the branches, so the runners are built separately.)
            configure()
                .init_tracing()
                .with_writer(
                    BasicWriter::new(io::stdout(), Coloring::Auto, Verbosity::Default)
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
