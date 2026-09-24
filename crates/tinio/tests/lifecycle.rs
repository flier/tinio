//! The CLI's Windows console-handler leg — the one process-level CLI leg that is not a
//! cucumber scenario.
//!
//! **The rest of the process surface moved.** Every leg that spawns the `tinio` binary and
//! reads what it did — the readiness event, `TINIO_PORT` and the `.env` file's two keys, the
//! second instance on one root, `--daemon`'s parent probe (both its ready and its timeout
//! path), the systemd notifications, the `server` → `status` → `stop` round trip, the five
//! state-dir and mode legs `status`/`stop` are held to, and the two sandbox refusals — is a
//! scenario in `crates/tinio-e2e/tests/features/cli_lifecycle.feature` now, driven by the
//! harness in `crates/tinio-e2e/tests/steps/cli.rs`. That is where a leg belongs when the
//! operator's words about the CLI are the test: the scenarios read as commands and answers, and
//! the process legs share one scratch-root harness instead of each spawning its own. The
//! leg-by-leg mapping, and the reason each leg is where it is, is `docs/tests.md`'s
//! "CLI (tinio)" section.
//!
//! **Why this one stayed.** A console control event cannot be raised from a step definition:
//! `GenerateConsoleCtrlEvent` is an `unsafe extern` call and the child has to be spawned
//! `CREATE_NEW_PROCESS_GROUP` to be addressable by it at all — the second of the workspace's two
//! console-related `#[allow(unsafe_code)]` sites, `tinio-cli`'s `signal.rs` handler being the
//! other (the workspace has five in all: those two, `tinio-winsec`'s file DACLs, and the raw
//! Windows modules `tinio-api`'s `dacl.rs` and `tinio-cli`'s `daemon.rs` own) — and a
//! `#[cfg(windows)]` arm inside the cucumber suite's step table.
//! What it proves is the handler's own half: that a console control event reaches the shutdown
//! channel `tinio-cli`'s `signal.rs` installs, and that the run drains rather than dying where
//! it stands.
//!
//! This file lives in the `tinio` package — the one that owns the `tinio` bin — because
//! `env!("CARGO_BIN_EXE_tinio")` is defined for a package's own binaries and for nothing else.
//! `crates/tinio-cli` keeps no copy of it: its tests are unit-level and spawn nothing.

#[cfg(windows)]
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
};

/// The `tinio` binary under test.
///
/// `CARGO_BIN_EXE_*` is defined for bins of the package under test, which is why this file lives
/// in `crates/tinio` — the package that owns the `tinio` bin — and not in `crates/tinio-cli`,
/// where the macro would not resolve at all.
#[cfg(windows)]
fn tinio() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_tinio"))
}

/// The serving child, killed and reaped on every path out of the leg — including the panic
/// path, so a failing assertion never leaves a server holding the root's state database.
#[cfg(windows)]
struct Serving(Child);

#[cfg(windows)]
impl Drop for Serving {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The Windows console handler is the crate's only Windows signal path, and this leg is what
/// proves it **reaches the shutdown channel** rather than only compiling: the `#[cfg(unix)]`
/// unit in `tinio-cli`'s `signal.rs` asserts the registration on unix,
/// `docs/tests.md`'s `windows-gnu` clippy leg proves the arm compiles, and nothing else starts
/// a real server and then signals it.
///
/// **The signal has to be a console control event**, because that is what Windows delivers to a
/// process. `CTRL_C_EVENT` cannot be targeted at a specific process group at all (`GenerateConsoleCtrlEvent`
/// documents that it reaches the whole console when the group is set), so signalling the child
/// that way would signal this test runner too — which is why the child is created
/// `CREATE_NEW_PROCESS_GROUP` and `CTRL_BREAK_EVENT` is what is sent. `CTRL_CLOSE_EVENT` cannot
/// be generated at all (it arrives when the console window closes), so this leg drives the arm
/// the handler body *shares* with it — one `send` on the one channel — which
/// `tinio-cli`'s `install_console_handler` doc records as one path.
///
/// **Both of the child's streams go to files rather than pipes**, the shape the moved daemon and
/// `.env` legs used and for the same family of reasons: this leg has to signal a *live* child and
/// then wait for it, so `Command::output()` is not usable here at all, and a pipe this leg holds
/// is a handle any descendant of the server could keep open — a file read of the readiness line
/// has no such coupling.
///
/// **The drain is the assertion and the exit code deliberately is not.** A first console
/// control event is answered `TRUE` and the process drains on its own; but a *second* one is
/// answered `FALSE` on purpose, handing the process to the default handler's immediate
/// termination, whose exit code is Windows's rather than this process's — so the property
/// `contracts/cli.md:43` fixes, and the only one this leg may lean on, is which side of the
/// drain the process ended on. Its evidence is the artifact a grace period leaves behind: the
/// plane removes `state` on its way out, and nothing else does.
///
/// **What that artifact can and cannot carry.** `state` exists only in the `api` graph, so in a
/// `--no-default-features` build the assertion below holds trivially and what the leg proves
/// there is the reachability half instead: the child becomes ready, is addressable by a console
/// control event through its own process group, and terminates on one. The drain is the default
/// build's evidence — the graph the product ships.
#[cfg(windows)]
#[allow(unsafe_code)] // the one call below is `GenerateConsoleCtrlEvent`; see the assertion
#[test]
fn the_console_handler_reaches_the_shutdown_path() {
    use std::{
        os::windows::process::CommandExt,
        thread,
        time::{Duration, Instant},
    };

    use windows_sys::Win32::System::{
        Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent},
        Threading::CREATE_NEW_PROCESS_GROUP,
    };

    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    fs::create_dir_all(&root).expect("root");
    let stdout = dir.path().join("child.stdout");
    let stderr = dir.path().join("child.stderr");

    // `CREATE_NEW_PROCESS_GROUP` is what makes the child reachable by a console control event:
    // `GenerateConsoleCtrlEvent` targets a process **group**, and without the flag the child
    // shares this test runner's group — signalling it would signal the runner.
    let child = Command::new(tinio())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .creation_flags(CREATE_NEW_PROCESS_GROUP)
        .stdin(Stdio::null())
        .stdout(Stdio::from(fs::File::create(&stdout).expect("stdout")))
        .stderr(Stdio::from(fs::File::create(&stderr).expect("stderr")))
        .spawn()
        .expect("spawn");
    let mut serving = Serving(child);

    // Readiness, out of the file the server is writing: bounded, so a server that never gets
    // there fails this leg with its own stderr instead of hanging it, and one that dies first
    // fails immediately rather than burning the whole bound. **The readiness is an event frame
    // rather than a printed line** (decision 19): the serving path's whole stdout is the frame,
    // which is what the launcher platforms and this leg both decode.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !fs::read_to_string(&stdout).is_ok_and(|text| {
        matches!(
            tinio_sandbox::Event::decode(text.trim_end()),
            Some(tinio_sandbox::Event::Listening { .. })
        )
    }) {
        if serving.0.try_wait().expect("status").is_some() {
            panic!(
                "the server exited before it was ready; stderr:\n{}",
                fs::read_to_string(&stderr).unwrap_or_default()
            );
        }
        assert!(
            Instant::now() < deadline,
            "the server was not ready within 30 s; stderr:\n{}",
            fs::read_to_string(&stderr).unwrap_or_default()
        );
        thread::sleep(Duration::from_millis(20));
    }

    assert_ne!(
        unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, serving.0.id()) },
        0,
        "the console control event reached the server's own process group"
    );

    let status = serving.0.wait().expect("wait");
    assert!(
        !root.join(".tinio/state").exists(),
        "a console control event is a graceful shutdown: the plane removed its `state` on the way out \
         (child status {status})"
    );
}
