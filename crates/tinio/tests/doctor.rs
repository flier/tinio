//! T057: the doctor through the binary.
//!
//! The legs only: the roots they diagnose, the spawn shape and the store seeds live in
//! [`common`], which this target declares below.

// The legs spell `std::fs`/`std::process` at the call site rather than importing each one; this is
// a test crate and the absolute paths are the point (they name the module, not a local alias), the
// same shape `common/` and `commands::doctor::tests` use.
#![allow(clippy::absolute_paths)]

mod common;

use common::*;
use tinio_util::testing::json_keys;
// The two teardowns below are the `api` graph's: no plane means no live server to
// stop, so this name has no caller in a `--no-default-features` build and is gated
// with them rather than imported unused.
#[cfg(feature = "api")]
use tinio_util::testing::kill_and_reap;

#[test]
fn a_root_with_stale_state_is_diagnosed_and_left_alone() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = run_doctor(dir.path(), &root, &[]);
    // The stale `state` file is the only defect this fixture holds, and the plane is the layer
    // that reports it — so the exit code is asserted per build, while what is diagnostic is
    // asserted in both (which is what keeps this test ungated). Underneath it, the fixture's
    // leftover temp file is a cleanup *proposal*, and a plain run renders those as information
    // (`Proposals`), which is the other half of the same distinction.
    #[cfg(feature = "api")]
    assert_eq!(
        out.status.code(),
        Some(1),
        "the stale state file is a defect: 1"
    );
    #[cfg(not(feature = "api"))]
    assert!(
        problems(&out).is_empty(),
        "proposals only where the plane is absent, the host's disk aside: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("private state"), "{text}");
    // The liveness answer this fixture carries: a `state` file with nothing behind it is
    // `Stopped`, and the row is the plane's own (`api`-only), so the assertion is too.
    #[cfg(feature = "api")]
    assert!(text.contains("a crashed server left a stale"), "{text}");
    // B2: the T144 "not repaired" row is `--fix`'s alone — a plain run never claims a repair.
    assert!(!text.contains("home root-state dirs"), "{text}");
    assert!(
        root.join(".tinio/tmp/leftover").exists(),
        "a plain run never touches anything"
    );
    assert!(
        root.join(".tinio/meta.redb").exists(),
        "and the store it found is still the store it left"
    );
}

#[test]
#[cfg(feature = "api")]
fn dry_run_lists_the_repairs_where_a_plain_run_reports_them_as_information() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());

    let dry = run_doctor(dir.path(), &root, &["--dry-run"]);
    assert_eq!(
        dry.status.code(),
        Some(1),
        "the proposals are findings here: 1"
    );
    let dry_text = String::from_utf8_lossy(&dry.stdout);
    assert!(
        dry_text.contains("would clear leftover temp file"),
        "{dry_text}"
    );
    assert!(
        root.join(".tinio/tmp/leftover").exists(),
        "dry-run never touches anything"
    );

    // The same fixture without the flag — except for the state file, which is a defect this
    // test is not about and which the state/socket leg reports either way. What is left is a
    // root whose only issue is a cleanup proposal, and that is exactly the pair `--dry-run`
    // exists to separate: the identical row, informational here and a finding above.
    std::fs::remove_file(root.join(".tinio/state")).expect("drop the stale state file");
    let plain = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&plain).is_empty(),
        "the same proposals as information, the host's disk aside: {}",
        String::from_utf8_lossy(&plain.stdout)
    );
    let plain_text = String::from_utf8_lossy(&plain.stdout);
    assert!(
        plain_text.contains("would clear leftover temp file"),
        "{plain_text}"
    );
    assert!(
        root.join(".tinio/tmp/leftover").exists(),
        "and a plain run touches nothing either"
    );
}

#[test]
#[cfg(feature = "api")]
fn dry_run_and_fix_together_are_a_usage_error() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = run_doctor(dir.path(), &root, &["--dry-run", "--fix"]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "the pair is a usage error, not a silent win for one flag"
    );
    assert!(!out.stderr.is_empty(), "and it names the flags that clash");
}

#[test]
#[cfg(feature = "api")]
fn fix_repairs_the_private_state_and_never_the_data() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let object = root.join("bucket/object.txt");
    std::fs::create_dir_all(object.parent().expect("parent")).expect("bucket");
    std::fs::write(&object, b"user data").expect("object");

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    // Everything the repair *could* clear it cleared — the leftover temp file, the stale state
    // file — and the re-check finds nothing else wrong; `crashed_root`'s configuration is
    // complete, so the credential warn is not in play either. `1` is the floor every `--fix`
    // report sits on: the `home root-state dirs` row is a `warn` because this build does not
    // carry the T144 stage (the contract's exit-code rule states the floor per mode).
    assert_eq!(
        out.status.code(),
        Some(1),
        "1: the T144 row is the only warn left — {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !root.join(".tinio/state").exists(),
        "the stale state file is reclaimed"
    );
    // B2: the report names the stage this build cannot attempt, and its owner.
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("home root-state dirs"), "{text}");
    assert!(text.contains("T144"), "{text}");
    assert_eq!(
        std::fs::read(&object).expect("object"),
        b"user data",
        "user data is untouched"
    );
}

/// A stage that failed is a **finding**, never an early return: `--fix --json` still emits a whole
/// document, the stages behind the failure still run, and the summary row still says what
/// happened. Before this, a failure anywhere in the repair returned `Err` from `run`, so stdout
/// carried nothing at all and the plane's reclaim was skipped with it.
///
/// The failure is made real rather than mocked: the fixture's leftover temp file is held open
/// with **no sharing at all** — Windows refuses to unlink a file opened that way, where a plain
/// `File::open` shares delete and would let the stage succeed.
#[test]
#[cfg(all(feature = "api", windows))]
fn fix_reports_a_failed_stage_and_still_emits_json() {
    use std::os::windows::fs::OpenOptionsExt;

    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let leftover = root.join(".tinio/tmp/leftover");
    let _held = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(&leftover)
        .expect("hold the stale temp file with no sharing");

    let out = run_doctor(dir.path(), &root, &["--fix", "--json"]);
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout)
        .expect("--fix --json is a document even when a stage failed");
    let findings = parsed["findings"].as_array().expect("findings");
    assert!(
        findings.iter().any(|f| f["severity"] == "error"),
        "the stage that failed is an error finding: {parsed}"
    );
    assert!(
        findings.iter().any(|f| f["check"] == "repairs applied"),
        "and the summary row is still there: {parsed}"
    );
    // The plane's reclaim ran anyway: the repair's other half is not hostage to the store's.
    assert!(
        !root.join(".tinio/state").exists(),
        "the stale state file is reclaimed even though the cleanup stage failed"
    );
    assert!(
        leftover.exists(),
        "and the file it could not remove is still there"
    );
}

#[test]
#[cfg(feature = "api")]
fn fix_refuses_while_a_server_is_running() {
    // A live control channel is an error for `--fix` (the doctor section's `--fix` rule) and the
    // **finding in the report**, never a bare `Err`: stdout carries it, the exit code is the
    // report's, and `--fix --json` on the same fixture is still a document a consumer can parse.
    // The server is started the way plan B's lifecycle test starts one.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let mut child = spawn(dir.path())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let mut line = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(child.stdout.take().expect("stdout")),
        &mut line,
    )
    .expect("ready");

    // The fourth liveness answer: a live server is a *diagnosis*, not a defect — the state row is
    // `ok` and only `--fix` refuses. The run's own exit code is 1 for an unrelated reason this
    // fixture cannot avoid: the server holds the store open, so the meta legs answer the "not
    // inspected" warn (`is_already_open`), exactly the shape the unit test
    // `a_store_held_by_another_process_is_a_warn_not_an_error` pins.
    let plain = run_doctor(dir.path(), &root, &[]);
    let plain_text = String::from_utf8_lossy(&plain.stdout);
    assert!(
        plain_text.contains("state file: a server is running"),
        "a live server renders the state row ok: {plain_text}"
    );

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the refusal is an error finding, so 1"
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("control channel"),
        "the check names what refused: {text}"
    );
    assert!(
        text.contains("running"),
        "the detail names the reason: {text}"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("control channel"),
        "the refusal is a finding, not a bare error: {stderr}"
    );

    let json = run_doctor(dir.path(), &root, &["--fix", "--json"]);
    assert_eq!(
        json.status.code(),
        Some(1),
        "a refused run still produces a report"
    );
    let parsed: serde_json::Value =
        serde_json::from_slice(&json.stdout).expect("a refused --fix --json is still a document");
    assert_eq!(
        parsed["severity"], "error",
        "the refusal is the report's worst finding"
    );

    let _ = spawn(dir.path()).arg("stop").arg(&root).output();
    kill_and_reap(&mut child);
}

/// A `state` file this build cannot read is **not** proof that nobody is behind it: one of
/// `Unreadable`'s causes is a version this build does not read, which is exactly what a *newer*
/// server writes. When the channel that document records still answers, `--fix` refuses with the
/// same error as a same-version live server, **before** any store repair — the derived name misses
/// a recorded override, and dialling the derived name was what made a live server's channel read
/// as silence.
///
/// Not unix-gated, and that is the point: the dial is `client::probe` at the recorded name, so a
/// Windows named pipe gets the same answer as a unix socket from the same call. The socket-*file*
/// probe it replaced had no Windows arm at all, which left that platform clearing the file
/// unconditionally.
#[test]
#[cfg(feature = "api")]
fn fix_leaves_a_foreign_version_state_file_alone_while_its_channel_answers() {
    let dir = tempfile::TempDir::new().expect("temp");
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).expect("root");
    let mut child = spawn(dir.path())
        .arg("server")
        .arg(&root)
        .args(["--port", "0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn");
    let mut line = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(child.stdout.take().expect("stdout")),
        &mut line,
    )
    .expect("ready");

    // The same document, one version ahead: everything needed to *dial* it is still in there —
    // the recorded `control_name` above all — and only the version is foreign.
    let path = root.join(".tinio/state");
    let mut doc: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).expect("state")).expect("json");
    doc["version"] = serde_json::json!(tinio_api::state::STATE_VERSION + 1);
    std::fs::write(&path, doc.to_string()).expect("foreign-version state");

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("stop it before --fix"),
        "an answering channel is the same refusal as a live server, before any repair: {text}"
    );
    assert!(
        !text.contains("repairs applied"),
        "the store is not repaired while that channel answers: {text}"
    );
    assert!(
        path.exists(),
        "a channel that answered keeps its `state` file: {text}"
    );

    let _ = spawn(dir.path()).arg("stop").arg(&root).output();
    kill_and_reap(&mut child);
}

#[test]
fn the_json_report_is_the_schema_shape() {
    // The normative half of the shape is `contracts/doctor-report.schema.json`; this is the
    // implementation half, and a field added on one side only must fail the other. The fixture's
    // root **has** a store, so this is the M-5 key set on a populated report — the same assertion
    // the store-less legs and the unit tests make.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let out = run_doctor(dir.path(), &root, &["--json"]);
    let parsed: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(json_keys(&parsed), ["findings", "severity"]);
    assert!(
        matches!(parsed["severity"].as_str(), Some("ok" | "warn" | "error")),
        "{parsed}"
    );
    let findings = parsed["findings"].as_array().expect("findings");
    assert!(findings.len() > 1, "{parsed}");
    for finding in findings {
        assert_eq!(
            json_keys(finding),
            ["check", "detail", "severity"],
            "{finding}"
        );
        assert!(
            matches!(finding["severity"].as_str(), Some("ok" | "warn" | "error")),
            "{finding}"
        );
        assert!(finding["check"].is_string(), "{finding}");
        assert!(finding["detail"].is_string(), "{finding}");
    }
}

#[test]
fn the_token_in_the_state_file_never_reaches_a_rendering() {
    // The rule Task 2 states and this leg keeps true under later edits: a finding names *what* it
    // found and never the bytes it read. The fixture's `state` file carries a distinctive token;
    // neither rendering of the report — nor anything on stderr — may quote it.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = crashed_root(dir.path());
    let state = std::fs::read_to_string(root.join(".tinio/state")).expect("state");
    assert!(
        state.contains(TOKEN),
        "the fixture carries the token: {state}"
    );

    let plain = run_doctor(dir.path(), &root, &[]);
    let json = run_doctor(dir.path(), &root, &["--json"]);
    for out in [&plain, &json] {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stdout.contains(TOKEN), "stdout: {stdout}");
        assert!(!stderr.contains(TOKEN), "stderr: {stderr}");
    }
}

#[test]
fn a_store_less_root_is_ok_and_gains_no_store() {
    // The store rule's own leg: a root that has never opened a store has nothing to check, so the
    // meta and private-state rows are `ok` — and the run opens nothing, which is the half that
    // cannot be seen in the report. `database::open` would *create* the file, so the diagnostics
    // pass probes first; this assertion is what fails if that probe is ever replaced by an open.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    let out = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&out).is_empty(),
        "a root with nothing wrong, the host's disk aside: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("there is no store; nothing to clear"),
        "{text}"
    );
    assert!(text.contains("not present; nothing to check"), "{text}");
    assert!(
        !root.join(".tinio/meta.redb").exists(),
        "a plain diagnostics pass never creates the store"
    );
}

#[test]
#[cfg(feature = "api")]
fn dry_run_on_a_store_less_root_gains_no_store() {
    // The other half of "in either mode": `--dry-run` is the same diagnostics pass, so it must
    // not create the store either (`--fix` deliberately does — it owns the store — which is why
    // this leg is the pair a plain run and a dry run form, and never `--fix`).
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    let out = run_doctor(dir.path(), &root, &["--dry-run"]);
    assert!(
        problems(&out).is_empty(),
        "a proposal-free root, the host's disk aside: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("there is no store; nothing to clear"),
        "{text}"
    );
    assert!(
        !root.join(".tinio/meta.redb").exists(),
        "a dry run never creates the store"
    );
}

#[test]
#[cfg(feature = "api")]
fn an_unreadable_state_file_is_a_warning_and_fix_clears_it() {
    // The fourth liveness answer: the `state` file is there but cannot be decoded, so the
    // live/stale question is not answered — the row says "not inspected: <reason>" and a warn it
    // is. `--fix` clears the file rather than aborting on it.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    std::fs::write(root.join(".tinio/state"), b"not a document").expect("state");

    let plain = run_doctor(dir.path(), &root, &[]);
    assert_eq!(plain.status.code(), Some(1), "an unreadable state warns: 1");
    let text = String::from_utf8_lossy(&plain.stdout);
    assert!(text.contains("state file: not inspected:"), "{text}");

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the repair clears the file, so the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert!(
        !root.join(".tinio/state").exists(),
        "an undecodable state file is reclaimed, not left behind"
    );
    #[cfg(unix)]
    {
        // U1: the socket row must speak from a **re-probe** after the repair. Before the refresh it
        // rendered the pre-repair snapshot's `Unreadable` verdict — a `warn` "not inspected" for a
        // `state` file the repair had already removed — which made a repaired run exit 1.
        let fixed_text = String::from_utf8_lossy(&fixed.stdout);
        assert!(
            !fixed_text.contains("stale control socket: not inspected"),
            "the socket row re-probes the repaired directory: {fixed_text}"
        );
    }
}

#[test]
#[cfg(all(feature = "api", unix))]
fn a_read_only_client_creates_no_home_state_tree() {
    // V5/R28: a read-only client that only probes derives `<home>/.tinio/roots/<hash>/` and must
    // not create it — the creating form used to mint it merely because the run asked where it would
    // be. Unix-only: `dirs` reads `HOME` there, while a Windows run would be sent at the
    // developer's real profile.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    // The same spawn, minus the state-dir override: the run derives its own alternate candidate,
    // which is what this leg is about. `HOME`/`USERPROFILE` alone steer it on unix; on Windows
    // `dirs` resolves the home through the shell API, so the leg is unix-gated.
    let out = spawn(dir.path())
        .env_remove("TINIO_STATE_DIR")
        .arg("doctor")
        .arg(&root)
        .arg("--read-only")
        .output()
        .expect("doctor");
    assert!(
        problems(&out).is_empty(),
        "a configuration-complete root, the host's disk aside: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        !dir.path().join(".tinio").exists(),
        "a read-only client must not mint the home state tree: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

#[test]
#[cfg(all(feature = "api", unix))]
fn no_state_file_still_probes_the_derived_socket_path() {
    // `NoStateFile` is an answer, not silence: a crashed instance can leave the socket behind
    // with no `state` file at all, so the derived name is probed and the row names it. The
    // derivation is plan A's `socket_path`, called here rather than re-spelled.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    let derived = socket_path(&root.join(".tinio"), &root).expect("socket path");

    let out = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&out).is_empty(),
        "nothing is wrong here, the host's disk aside: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("no `state` file in"), "{text}");
    assert!(
        text.contains("no control socket at"),
        "the derived path is probed: {text}"
    );
    assert!(text.contains(&*derived.to_string_lossy()), "{text}");
    assert!(!derived.exists(), "and nothing is created");
}

#[test]
#[cfg(all(feature = "api", unix))]
fn a_stale_unix_socket_beside_a_stale_state_file_is_reclaimed_by_fix() {
    // The socket a crashed unix run leaves: bound once so the file exists, then closed so nothing
    // answers it. The test never probes it itself — the report's own `reclaim_stale(path, true)`
    // is the probe under test, and the "a stale control socket at <path>" row is its answer.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    let socket = socket_path(&root.join(".tinio"), &root).expect("socket path");
    write_state_file(&root, &socket.to_string_lossy());
    drop(UnixListener::bind(&socket).expect("stale socket"));

    let plain = run_doctor(dir.path(), &root, &[]);
    assert_eq!(
        plain.status.code(),
        Some(1),
        "a stale socket is a defect: 1"
    );
    let text = String::from_utf8_lossy(&plain.stdout);
    assert!(text.contains("a stale control socket at"), "{text}");
    assert!(text.contains(&*socket.to_string_lossy()), "{text}");
    assert!(
        socket.exists(),
        "a plain run probes and leaves the file alone"
    );

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert!(
        !socket.exists(),
        "--fix reclaims the socket through `reclaim_stale(path, false)`"
    );
    assert!(
        !root.join(".tinio/state").exists(),
        "and the stale state file with it"
    );
}

#[test]
#[cfg(all(feature = "api", unix))]
fn fix_reclaims_the_recorded_socket_path_not_the_derived_one() {
    // The path rule, and the leg a `--fix` that re-derived would fail while passing the one
    // above: the `state` file's `control_name` is at a path that is *not* `socket_path`'s
    // output — the shape `[api.unix] path` produces, the documented remedy for a deep root — and
    // `--fix` must remove *that* file. The derived path is never created.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    let recorded = root.join(".tinio/recorded.sock");
    let derived = socket_path(&root.join(".tinio"), &root).expect("socket path");
    assert_ne!(recorded, derived);
    write_state_file(&root, &recorded.to_string_lossy());
    drop(UnixListener::bind(&recorded).expect("stale socket"));
    assert!(!derived.exists(), "the derived path is never created");

    let plain = run_doctor(dir.path(), &root, &[]);
    assert_eq!(plain.status.code(), Some(1));
    let text = String::from_utf8_lossy(&plain.stdout);
    assert!(text.contains("a stale control socket at"), "{text}");
    assert!(text.contains(&*recorded.to_string_lossy()), "{text}");

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert!(
        !recorded.exists(),
        "--fix reclaimed the path the `state` file recorded"
    );
    assert!(
        !derived.exists(),
        "and never re-derived one that was never the socket"
    );
}

#[tokio::test]
async fn a_marked_store_is_reported_as_fragmented() {
    // The fragmentation leg: the marker forces the rule `needs_compact` owns (a small temp-dir
    // store never crosses its 64 MiB floor on the ratio alone), and the row is a finding even in a
    // plain run — the `Proposals` mapping is the cleanup leg's, not this one's.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    mark_compact_needed(&root.join(".tinio")).await;

    let out = run_doctor(dir.path(), &root, &[]);
    assert_eq!(out.status.code(), Some(1), "fragmentation is a warn: 1");
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("meta.redb fragmentation"), "{text}");
    assert!(text.contains("compaction would reclaim space"), "{text}");
    assert!(text.contains("integrity check passed"), "{text}");
}

#[tokio::test]
#[cfg(feature = "api")]
async fn fix_compacts_a_marked_store() {
    // `--fix`'s own meta-store leg: `compact_if_needed` runs inside `open_store`'s
    // `Pass::Fix`, so the re-check sees a clean store and the marker is cleared.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    mark_compact_needed(&root.join(".tinio")).await;

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let open = tinio_fs::database::open(&root.join(".tinio")).expect("open the store");
    assert!(
        !open.compact_needed,
        "--fix compacted and cleared the marker"
    );
}

#[test]
fn a_corrupt_meta_store_is_an_error_and_the_report_still_renders() {
    // `meta.redb` holding bytes that are not a database: `check_integrity` cannot open it, so
    // the meta leg is an `error` — and the run still produces a whole report naming the store
    // rather than aborting on the first failure.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    std::fs::write(root.join(".tinio/meta.redb"), b"not a redb file").expect("corrupt store");

    let out = run_doctor(dir.path(), &root, &[]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "an unopenable store is an error: 1"
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("meta.redb"), "{text}");
    assert!(text.contains("severity: error"), "{text}");
    assert!(
        text.contains("storage root"),
        "the whole report is there: {text}"
    );
}

#[test]
#[cfg(feature = "api")]
fn fix_on_a_store_it_cannot_open_still_reports() {
    // A store `--fix` cannot open is a repair-stage failure: an `error` finding, and the ok
    // "repairs applied" row does not carry it. The report is still the product.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    std::fs::write(root.join(".tinio/meta.redb"), b"not a redb file").expect("corrupt store");

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(out.status.code(), Some(1), "the error survives the repair");
    let text = String::from_utf8_lossy(&out.stdout);
    let open_failure = text
        .lines()
        .find(|line| line.contains("could not be opened"));
    assert!(
        open_failure.is_some_and(|line| line.starts_with("error")),
        "the open failure is an error finding: {text}"
    );
    assert!(
        text.lines()
            .find(|line| line.contains("repairs applied"))
            .is_some_and(|line| !line.contains("could not be opened")),
        "the failure is not buried in the ok summary: {text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "api")]
async fn orphaned_meta_entries_are_reported_and_reclaimed() {
    // A row whose object file is gone: the `Full` repair's meta-orphan leg. The integrity and
    // fragmentation rows are asserted beside it, because they are the same store's other two
    // answers and a report missing one of them would read as a clean store.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    seed_user_data(&root, "data");
    seed_orphaned_meta(&root, "data", "orphan.txt").await;

    let plain = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&plain).is_empty(),
        "a proposal is information without --dry-run, the host's disk aside: {}",
        String::from_utf8_lossy(&plain.stdout)
    );
    assert!(
        String::from_utf8_lossy(&plain.stdout).contains("orphaned meta entry"),
        "the row is rendered either way"
    );

    let dry = run_doctor(dir.path(), &root, &["--dry-run"]);
    assert_eq!(dry.status.code(), Some(1), "a finding under --dry-run: 1");
    let dry_text = String::from_utf8_lossy(&dry.stdout);
    assert!(
        dry_text.contains("would reclaim orphaned meta entry for orphan.txt in data"),
        "{dry_text}"
    );
    assert!(dry_text.contains("integrity check passed"), "{dry_text}");
    assert!(dry_text.contains("meta.redb fragmentation"), "{dry_text}");

    let json = run_doctor(dir.path(), &root, &["--dry-run", "--json"]);
    assert_eq!(json.status.code(), Some(1));
    let parsed: serde_json::Value = serde_json::from_slice(&json.stdout).expect("json");
    assert_eq!(parsed["severity"], "warn");
    assert!(
        parsed["findings"]
            .as_array()
            .expect("findings")
            .iter()
            .any(|finding| finding["detail"]
                .as_str()
                .unwrap_or_default()
                .contains("orphaned meta entry")),
        "{parsed}"
    );
    assert!(
        !root.join("data/orphan.txt").exists(),
        "a dry run touched nothing"
    );

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert_eq!(
        std::fs::read(root.join("data/object.txt")).expect("object"),
        b"user data",
        "user data is untouched"
    );
    let store = tinio_fs::meta::store(&root.join(".tinio")).expect("meta store");
    assert!(
        store
            .get(
                &tinio::bucket::name("data").expect("bucket"),
                &tinio::object::key("orphan.txt").expect("key"),
            )
            .await
            .expect("get")
            .is_none(),
        "the orphan row is reclaimed"
    );
}

#[test]
#[cfg(feature = "api")]
fn abandoned_multipart_uploads_are_reported_and_removed() {
    // A multipart subtree whose bucket directory no longer exists (failure-handling.md §3): the
    // `Startup`/`Full` repair's bucket-orphaned stage, which has no idle grace (an upload
    // directory without a `UPLOADS` record does, and a fresh fixture would survive it).
    let dir = tempfile::TempDir::new().expect("temp");
    let root = stored_root(dir.path());
    seed_user_data(&root, "live");
    let subtree = root.join(".tinio/multipart/gone/u1");
    std::fs::create_dir_all(&subtree).expect("multipart subtree");
    std::fs::write(subtree.join("part-1"), b"x").expect("part");

    let plain = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&plain).is_empty(),
        "information without the flag, the host's disk aside: {}",
        String::from_utf8_lossy(&plain.stdout)
    );
    assert!(
        String::from_utf8_lossy(&plain.stdout).contains("multipart subtree of missing bucket gone")
    );

    let dry = run_doctor(dir.path(), &root, &["--dry-run"]);
    assert_eq!(dry.status.code(), Some(1), "a finding under --dry-run: 1");
    let dry_text = String::from_utf8_lossy(&dry.stdout);
    assert!(
        dry_text.contains("would remove multipart subtree of missing bucket gone"),
        "{dry_text}"
    );
    assert!(subtree.exists(), "a dry run never removes anything");

    let json = run_doctor(dir.path(), &root, &["--dry-run", "--json"]);
    let parsed: serde_json::Value = serde_json::from_slice(&json.stdout).expect("json");
    assert_eq!(parsed["severity"], "warn");

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert!(
        !root.join(".tinio/multipart/gone").exists(),
        "the abandoned subtree is removed"
    );
    assert_eq!(
        std::fs::read(root.join("live/object.txt")).expect("object"),
        b"user data",
        "user data is untouched"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "api")]
async fn orphaned_bucket_records_are_reported_and_pruned() {
    // A `BUCKETS` row whose bucket directory is gone: the stale-record stage. The record is
    // seeded through `tinio_fs::bucket::store` — a redb row is not a file a test can write — and
    // the read-back is the store's own, not a second guess at the row.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    seed_user_data(&root, "live");
    seed_stale_bucket_record(&root, "gone").await;

    let plain = run_doctor(dir.path(), &root, &[]);
    assert!(
        problems(&plain).is_empty(),
        "information without the flag, the host's disk aside: {}",
        String::from_utf8_lossy(&plain.stdout)
    );
    assert!(String::from_utf8_lossy(&plain.stdout).contains("stale bucket record"));

    let dry = run_doctor(dir.path(), &root, &["--dry-run"]);
    assert_eq!(dry.status.code(), Some(1), "a finding under --dry-run: 1");
    let dry_text = String::from_utf8_lossy(&dry.stdout);
    assert!(
        dry_text.contains("would prune stale bucket record for gone"),
        "{dry_text}"
    );

    let json = run_doctor(dir.path(), &root, &["--dry-run", "--json"]);
    let parsed: serde_json::Value = serde_json::from_slice(&json.stdout).expect("json");
    assert_eq!(parsed["severity"], "warn");

    let fixed = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        fixed.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&fixed.stdout)
    );
    assert_eq!(
        std::fs::read(root.join("live/object.txt")).expect("object"),
        b"user data",
        "user data is untouched"
    );
    let store = tinio_fs::bucket::store(&root.join(".tinio")).expect("bucket store");
    let rows = store.load_all().await.expect("rows");
    assert!(
        !rows.iter().any(|(name, _)| name == "gone"),
        "the stale record is pruned: {rows:?}"
    );
}
#[test]
#[cfg(feature = "api")]
fn a_read_only_run_diagnoses_the_relocated_state_dir() {
    // B1: `--read-only` moves the state dir, and the run must diagnose **that** directory and
    // name it. `TINIO_STATE_DIR` stands in for the home-derived relocation — it is the documented
    // explicit form of it, and the only form a Windows process can be pointed at (`dirs` resolves
    // the home through the shell API there, so `HOME`/`USERPROFILE` do not move it).
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    let relocated = dir.path().join("home-state");
    std::fs::create_dir_all(&relocated).expect("relocated state dir");
    write_state_in(&relocated, &root, &control_name(&root));

    let out = run_doctor(dir.path(), &root, &["--read-only"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a stale state file in the relocated dir is a defect: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains(&*relocated.to_string_lossy()),
        "the report names the relocated directory: {text}"
    );
    assert!(text.contains("a crashed server left a stale"), "{text}");
    assert!(
        !root.join(".tinio/state").exists(),
        "nothing was written into the served root's reserved dir"
    );
}

#[test]
#[cfg(feature = "api")]
fn fix_on_a_store_less_root_creates_the_store_and_leaves_the_data() {
    // B13 (M-3): the one mode no leg drove — `--fix` **owns** the store, so it creates the one a
    // diagnostics pass deliberately leaves absent, and still never touches user data.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    seed_user_data(&root, "live");
    assert!(
        !root.join(".tinio/meta.redb").exists(),
        "the fixture starts store-less"
    );

    let out = run_doctor(dir.path(), &root, &["--fix"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        root.join(".tinio/meta.redb").exists(),
        "`--fix` owns the store and creates it"
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("meta.redb"),
        "the meta rows are present: {text}"
    );
    assert_eq!(
        std::fs::read(root.join("live/object.txt")).expect("object"),
        b"user data",
        "user data is untouched"
    );
}

#[test]
#[cfg(feature = "api")]
fn fix_read_only_repairs_the_relocated_state_dir_and_not_the_root() {
    // B13 (M-4): `--fix --read-only` repairs in the **relocated** state dir and writes nothing
    // in the served root — not even the store the repair path opens.
    let dir = tempfile::TempDir::new().expect("temp");
    let root = configured_root(dir.path());
    let relocated = dir.path().join("home-state");
    std::fs::create_dir_all(&relocated).expect("relocated state dir");
    write_state_in(&relocated, &root, &control_name(&root));

    let out = run_doctor(dir.path(), &root, &["--fix", "--read-only"]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "the T144 row is the only warn left: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        !relocated.join("state").exists(),
        "the relocated stale state file is reclaimed"
    );
    assert!(
        relocated.join("meta.redb").exists(),
        "the store the repair owns is the relocated one"
    );
    assert!(
        !root.join(".tinio/meta.redb").exists(),
        "read-only never writes the served root, the store included"
    );
}
