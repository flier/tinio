//! The single cucumber test binary. All scenarios live in
//! `tests/features/`; step definitions live in `tests/steps/`.
//!
//! Default tag filter: scenarios that cannot pass in this build are excluded
//! unless the user passes an explicit `--tags` on the CLI. The four exclusion
//! axes are independent (`steps/tag_filter.rs` owns the composition): `@parquet`
//! drops out unless the binary was compiled `--features parquet` (the parquet
//! input reader lives behind `tinio-server/select-parquet`, off by default);
//! a platform-bound scenario (`@linux`/`@macos`/`@windows`/`@unix`) drops out on
//! every target its tags do not match — the step definitions exist only where
//! it may run, and `fail_on_skipped()` would turn their absence into a failure —
//! so a scenario carrying several platform tags runs where at least one
//! matches; the external client tags
//! (`@interop`/`@boto3`/`@mc`) drop out unless `TINIO_E2E_EXTERNAL=1` — the
//! same "opt-in" semantics the old `#[ignore]` integration tests had; and the
//! host-bound confinement scenarios (`@confinement`) run by default on macOS,
//! whose launcher binds and spawns the confined child rather than paying a
//! grant, and drop out elsewhere — `TINIO_E2E_CONFINEMENT` overrides that
//! per-host default in both directions (`1`/`true` keeps the family on every
//! target, `0`/`false` drops it on every target, macOS included — CI's macOS
//! parallel pass is that state's reader), and any other value stops the run
//! rather than picking a side (`steps/tag_filter.rs` owns the grammar): a
//! Windows target pays the launcher's ACL grant, and no budget here covers what
//! `docs/tests.md`'s "Grant cost" measures (see that section's owner rule; the
//! platform scripts are the real gate).
//!
//! `TINIO_E2E_REPORT=<path>` additionally writes a Cucumber-JSON report
//! to the given file (CI uses this for the PR test report).
//!
//! `fail_on_skipped()` turns undefined (and any other skipped) steps into
//! failures — a feature/step that drifts out of sync with its step
//! definitions must fail the run, never pass silently as "skipped".

// **A `harness = false` root (docs/style.md §Panics), and a stated exemption because clippy's
// test keys do not reach the whole target it roots** (design 2026-10-09 D7): the target is not
// built as a test, so nothing in it — this file's runner or `steps/`, which inherits this level
// — is a test context as far as `allow-*-in-tests` is concerned. `tinio-e2e` is outside the
// cleanup (design non-goals); this one allow is what keeps its rows from firing on a suite the
// plan does not convert.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test support: a `harness = false` target's helpers (docs/style.md §Panics)"
)]

use std::{env, fs::File, io, process, thread};
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
use steps::{World, configure, tag_filter};
use tokio::runtime::Builder;

/// The stack the runner's own thread is given: 32 MiB.
///
/// **The Windows main thread's stack is what aborted the `@interop` multipart leg.** A Rust
/// binary inherits the PE's stack reserve, and this test binary carries **1 048 576 bytes**
/// (`SizeOfStackReserve`; `SizeOfStackCommit` 4 KiB), read from
/// `target\debug\deps\cucumber-70617418b044b36b.exe`'s optional header after the run died with
/// `STATUS_STACK_OVERFLOW` (`0xC00000FD`) on `thread 'main'`; a Linux main thread gets 8 MiB,
/// which is why the same scenarios are green there. The whole run is polled from that one thread
/// (`rt.block_on` in [`run`]), so every synchronous helper a step calls rides its stack — the
/// harness's largest single frame is `files_equal`'s two 64 KiB buffers
/// (`tests/steps/clients.rs`), and **nothing in the leg holds the 10 MiB itself**: the scratch
/// file is a heap `Vec` from `deterministic_bytes`.
///
/// 32 MiB is 32× the reserve that failed and the number `crates/tinio-select/src/parquet.rs`
/// already gives its own enlarged-stack test thread. **Only the reserve is virtual**: the commit
/// stays at the OS default and grows on demand, so this costs address space, not memory.
///
/// Measured 2026-09-28: the `@interop and @aws` selection — the 10 MiB multipart leg included —
/// passes with this stack, where the 1 MiB default aborted it.
const RUNNER_STACK: usize = 32 * 1024 * 1024;

fn main() {
    // Must run before the tokio runtime starts: CUCUMBER_FILTER_TAGS is
    // read by cucumber's CLI parser when no --tags is given. SAFETY: no
    // threads exist yet; the runtime is built below.
    //
    // Three independent axes: the cargo feature decides `@parquet` (without the
    // reader compiled in, those scenarios can only 501), the target platform
    // decides the platform tags (each platform-bound scenario's steps exist
    // only where its tags match — the systemd notify socket is a unix datagram,
    // and a launcher scenario tagged `@macos @windows` runs on both) and
    // `@confinement` (macOS runs the confinement by default: its launcher binds
    // and spawns, with no grant to budget; a Windows target owes an ACE grant
    // whose measured cost tens of seconds of variance), and the env opt-ins
    // decide the external-client tags (they need third-party binaries) and,
    // through `TINIO_E2E_CONFINEMENT`'s explicit states, the confinement
    // scenarios on every target.
    if !env::args().any(|a| a == "--tags") {
        // The confinement switch is three-state — unset (the target's own
        // default), `1`/`true` (force in) and `0`/`false` (force out) — and a
        // value that is none of those stops the run here, before any scenario
        // starts, rather than silently defaulting (`tag_filter` owns the
        // grammar, the same one `TINIO_SANDBOX` reads).
        let confinement = match env::var("TINIO_E2E_CONFINEMENT") {
            Ok(value) => tag_filter::parse_confinement(Some(&value)),
            Err(env::VarError::NotPresent) => Ok(tag_filter::Confinement::Platform),
            Err(env::VarError::NotUnicode(value)) => Err(format!("not valid UTF-8: {value:?}")),
        }
        .unwrap_or_else(|refusal| panic!("TINIO_E2E_CONFINEMENT: {refusal}"));
        let filter = tag_filter::default_filter(
            env::consts::OS,
            cfg!(feature = "parquet"),
            env::var_os("TINIO_E2E_EXTERNAL").is_some(),
            confinement,
        );
        if !filter.is_empty() {
            unsafe {
                env::set_var("CUCUMBER_FILTER_TAGS", filter);
            }
        }
    }

    // The report file opens BEFORE the runtime starts — no blocking fs
    // inside the async block (style.md). The Json writer takes a std
    // `Write`, so the file must be a std one.
    let report_file = env::var("TINIO_E2E_REPORT")
        .ok()
        .map(|path| File::create(&path).expect("create report file"));

    // The runtime, and with it every scenario the runner polls, belongs to a thread of its own —
    // `main`'s stack is the platform's, and on Windows that is not enough (see [`RUNNER_STACK`]).
    // Nothing thread-bound crosses the boundary: the report file is moved in, and the runtime,
    // the world, the writer and the summary are all built inside.
    let runner = thread::Builder::new()
        .name("cucumber-runner".to_owned())
        .stack_size(RUNNER_STACK)
        .spawn(move || run(report_file))
        .expect("spawn the cucumber runner thread");
    if runner.join().is_err() {
        // A failed run reports itself by panicking on the runner thread, and the default hook
        // has already printed that summary there. Carry the code a panicking `main` would have
        // left (101) rather than panicking here and printing the same summary a second time.
        process::exit(101);
    }
}

/// Build the runtime and run the suite. **Not `main`**, so the runner gets [`RUNNER_STACK`] and
/// not the stack the platform gives the process's first thread.
fn run(report_file: Option<File>) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
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
