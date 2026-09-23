//! The `serve` example binary in read-only mode (FR-023).
//!
//! The coverage hole this closes: every other read-only test drives the
//! data plane in process (`tests/read_only.rs`), and the only thing that
//! spawns the example binary is the CI-only `@interop` cucumber harness
//! (it needs aws-cli/rclone, so no local run reaches it) — `serve.rs`'s
//! `main`, the mode resolution and the state-dir choice included, is
//! executed by no local test. These legs spawn the real binary and observe
//! it from outside: its stdout, its stderr, its exit status, and the S3
//! answers on the wire.
//!
//! Signing is mandatory. The example always configures the fixed
//! `minioadmin` pair (`plane_for`), so an unsigned request is refused with
//! `Signature is required` before it reaches the layer under test — an
//! unsigned probe would answer the same thing whether or not the resolved
//! mode was wired into the plane. Every request below carries a header-auth
//! SigV4 signature: the compact signer `data.rs`'s `acl` test module wrote
//! for exactly this situation, which lives in [`common::signed`] because the
//! Windows confinement leg needs the same header for the same reason.
//!
//! The mode discriminator needs no fixture: a signed `PUT` to a bucket that
//! does not exist answers `404 NoSuchBucket` read-write (the reference
//! answer `read_only.rs`'s leg 4 pins) and `403 AccessDenied` read-only. A
//! signed `GET` of a file planted in the served root shows reads still
//! serve.
//!
//! Each spawned child is killed and reaped before its leg ends, on the
//! assertion-failure path too: [`Serve`]'s `Drop` does it, so the redb lock
//! is released before a sibling server starts on the same root (the
//! ordering `crates/tinio-e2e/tests/steps/clients.rs`'s `terminate`
//! documents).

mod common;

#[path = "support/serve_spawn.rs"]
mod spawn;

use std::{fs, path::Path};

use common::{plant, signed};
use spawn::{Mode, Scratch, Serve, names_path, observed_mode, state_var};
use tinio_config::sources::READ_ONLY_VAR;
use tinio_sandbox::Event;

/// Case 1: `--read-only` with `TINIO_STATE_DIR` serves read-only, keeps the
/// state out of the served root, and writes exactly one frame to stdout.
#[tokio::test]
async fn the_read_only_flag_relocates_the_state_and_refuses_writes() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    plant(&root, "data", "a.txt", "hello");

    let mut server = Serve::spawn(&root, &["--port", "0", "--read-only"], &[state_var(&state)]);
    let addr = server.ready().await;

    // The flag reached the plane: the write path is refused where the
    // read-write plane answers NoSuchBucket from the bucket check itself.
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`--read-only` must refuse the signed write with AccessDenied"
    );
    // Reads still serve, with the planted body: a refusal decorator that
    // broke reads would still pass the discriminator above.
    let object = signed(addr, "GET", "/data/a.txt", b"").await;
    assert_eq!(
        object.status,
        200,
        "the planted object must serve on a read-only plane: {}",
        object.text()
    );
    assert_eq!(
        object.text(),
        "hello",
        "the planted object's body must arrive intact"
    );
    // The state really moved out of the root, into the dir the diagnostic
    // names on stderr (stdout is the harness's readiness frame alone).
    let diagnostic = format!("read-only mode: state dir {}", state.display());
    let stderr = server.stderr_containing(&diagnostic).await;
    assert!(
        stderr.contains(&diagnostic),
        "stderr must carry `{diagnostic}`; got:\n{stderr}"
    );
    assert!(
        state.join("meta.redb").is_file(),
        "the state database must land in {} (TINIO_STATE_DIR)",
        state.display()
    );
    assert!(
        !root.join(".tinio").exists(),
        "the read-only server wrote a `.tinio` into the served root it promises to leave alone"
    );

    server.stop();
    // Complete now that the child is gone: stdout carried the readiness
    // frame and nothing else — two field lines and the blank line that
    // closes them, which the line count pins and the decode reads.
    let stdout = server.stdout_lines();
    assert_eq!(
        Event::decode(&stdout.join("\n")),
        Some(Event::Listening { addr }),
        "stdout must carry the readiness frame; got {stdout:?}"
    );
    assert_eq!(
        stdout.len(),
        3,
        "stdout must carry the readiness frame alone (an event line, a data line, a blank \
         line); got {stdout:?}"
    );
}

/// Case 2: a config file's `[server] read_only = true` puts the plane in
/// read-only mode with no flag and no environment variable.
#[tokio::test]
async fn a_config_file_turns_read_only_on() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    let config = scratch.config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[state_var(&state)],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`[server] read_only = true` must put the plane in read-only mode without a flag or \
         TINIO_READ_ONLY"
    );
    server.stop();
}

/// Case 3: `TINIO_READ_ONLY=1` puts the plane in read-only mode with no
/// flag and no config file.
#[tokio::test]
async fn the_read_only_env_var_turns_read_only_on() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();

    let mut server = Serve::spawn(
        &root,
        &["--port", "0"],
        &[state_var(&state), (READ_ONLY_VAR, "1")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "TINIO_READ_ONLY=1 must put the plane in read-only mode without a flag or a config file"
    );
    server.stop();
}

/// Case 4a: the environment beats the config file — `read_only = true` in
/// the file with `TINIO_READ_ONLY=0` serves READ-WRITE.
#[tokio::test]
async fn the_env_var_beats_the_config_file() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();
    let config = scratch.config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[state_var(&state), (READ_ONLY_VAR, "0")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadWrite,
        "TINIO_READ_ONLY=0 must override the config file's `read_only = true`"
    );
    // The stronger half of "read-write": a write really lands. The
    // discriminator's 404 could come from a plane that refuses writes some
    // other way; the create and upload below are the plain proof it does not.
    let created = signed(addr, "PUT", "/fresh", b"").await;
    assert_eq!(
        created.status,
        200,
        "CreateBucket must succeed on the read-write plane: {}",
        created.text()
    );
    let uploaded = signed(addr, "PUT", "/fresh/a.txt", b"body").await;
    assert_eq!(
        uploaded.status,
        200,
        "a signed upload must succeed on the read-write plane: {}",
        uploaded.text()
    );
    assert_eq!(
        fs::read_to_string(root.join("fresh").join("a.txt")).expect("the uploaded object"),
        "body",
        "the uploaded object must land in the served root"
    );
    // And the state stayed in the root: read-write mode never consults
    // TINIO_STATE_DIR (the read-only escape hatch), so the exported value is
    // ignored rather than obeyed.
    assert!(
        root.join(".tinio").is_dir(),
        "read-write mode must keep its state in <root>/.tinio, not in TINIO_STATE_DIR"
    );
    server.stop();
}

/// Case 4b: the flag beats the environment — `TINIO_READ_ONLY=0` with
/// `--read-only` serves READ-ONLY.
#[tokio::test]
async fn the_read_only_flag_beats_the_env_var() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = scratch.state();

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--read-only"],
        &[state_var(&state), (READ_ONLY_VAR, "0")],
    );
    let addr = server.ready().await;
    assert_eq!(
        observed_mode(addr).await,
        Mode::ReadOnly,
        "`--read-only` must win over TINIO_READ_ONLY=0"
    );
    server.stop();
}

/// Case 5: `--read-only` never creates the root it was told to serve — a
/// missing root fails the process instead. Both routes the example
/// documents are driven: with no `TINIO_STATE_DIR` the derived
/// `<home>/.tinio/roots/<hash>/` resolution fails (it canonicalizes the
/// root before it touches the home, which is why that route cannot leave
/// anything in the real home either), and with `TINIO_STATE_DIR` set the
/// backend's own canonicalization fails.
#[tokio::test]
async fn a_missing_root_is_not_created_in_read_only_mode() {
    let scratch = Scratch::new();
    let missing = scratch.missing_root();
    let state = scratch.state();

    // Route 1: the derived state dir. Its failure names the root it could
    // not canonicalize.
    let stderr = missing_root_fails("the derived state dir", &missing, &[]).await;
    assert!(
        names_path(&stderr, &missing),
        "the derived-layout failure must name the missing root {}; stderr:\n{stderr}",
        missing.display()
    );

    // Route 2: an explicit `TINIO_STATE_DIR`. The resolution succeeds — its
    // diagnostic names the scratch dir — and the backend's canonicalization
    // of the root fails instead, as a bare `Io` error that carries no path,
    // so the diagnostic is the evidence that this route got that far.
    let stderr = missing_root_fails("TINIO_STATE_DIR", &missing, &[state_var(&state)]).await;
    let diagnostic = format!("read-only mode: state dir {}", state.display());
    assert!(
        stderr.contains(&diagnostic),
        "the explicit state dir is resolved before the root is opened, so stderr must carry \
         `{diagnostic}`; got:\n{stderr}"
    );
}

/// Spawn `--read-only` over a root that does not exist and assert what every
/// route shares — a non-zero exit, and the root still absent afterwards —
/// returning the child's stderr for the route's own evidence.
async fn missing_root_fails(label: &str, missing: &Path, env: &[(&str, &str)]) -> String {
    let mut server = Serve::spawn(missing, &["--read-only"], env);
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert!(
        !status.success(),
        "{label}: a missing root must fail startup, not be served ({status}); stderr:\n{stderr}"
    );
    assert!(
        !missing.exists(),
        "{label}: read-only mode created the root it was told to serve"
    );
    stderr
}

/// Case 6: an unknown argument exits with status 2, the refused-command-line
/// path.
#[tokio::test]
async fn an_unknown_argument_exits_with_the_usage_status() {
    let scratch = Scratch::new();
    let root = scratch.root();

    let mut server = Serve::spawn(&root, &["--not-a-flag"], &[]);
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert_eq!(
        status.code(),
        Some(2),
        "an unknown argument must exit 2 (usage); stderr:\n{stderr}"
    );
    // clap's rendering, and the casing is its: a `Usage:` line that still
    // names the binary is the guidance this leg is about, and the flag that
    // could have answered it (`--help`) is disabled.
    assert!(
        stderr.contains("Usage: serve"),
        "the usage text must name the binary; stderr:\n{stderr}"
    );
    // And the same refusal is an event on stdout: a consumer that waits for a
    // frame reads the cause where an empty stream is the timeout it reports
    // (design decision 19). This is the one leg that reads a *refusal* frame
    // off stdout without a server ever binding.
    let event = server
        .stdout_event()
        .expect("the refusal frame, since the process exited");
    assert!(
        matches!(&event, Event::Error { message } if message.contains("--not-a-flag")),
        "an unknown argument must send an `error` frame naming it, not {event:?}"
    );
    // **The frame and nothing else**, counted rather than grepped: the parse
    // library in the dependency graph is the one writer the source grep for
    // `println!`/`stdout()` cannot see, so stdout's exclusivity is asserted
    // here, on the stream itself, for the path that now runs clap.
    let lines = server.stdout_lines();
    assert_eq!(
        lines.len(),
        3,
        "stdout must carry one frame — an `event:` line, a `data:` line and the blank line \
         that closes it — and nothing else; got {lines:?}"
    );
}
