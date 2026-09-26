//! Sandbox channels on the `tinio server` path: the flag a feature-off build
//! cannot honour, and the channels a target whose backend is not wired cannot
//! (design decision 5).
//!
//! **What every leg here is actually about is the invariant, not the refusal**:
//! a requested confinement is never silently ignored. Where nothing can honour
//! the channel the run refuses and names it; where this target's backend is
//! wired the request is honoured and the run serves. Read-only mode lives in
//! `serve_read_only.rs`. Both files spawn through [`spawn`].

mod common;

#[path = "support/serve_spawn.rs"]
mod spawn;

use std::time::{Duration, Instant};

use common::wait_timeout;
#[cfg(all(windows, feature = "sandbox"))]
use spawn::state_var;
// The state-directory override is the environment channel only one test can reach, and that test is
// `cfg(all(windows, feature = "sandbox"))` — the in-root state directory is refused by the shared
// containment rule before any server starts, and Windows is the platform where one directory has
// two spellings (the case rule the test is about). It is imported under that same gate rather than
// in the list above, because the macOS and Linux legs of this suite never call it and the
// workspace's `-D warnings` gate turns the unused name into a failure — which the macOS clippy step
// added for the sandbox phase is what caught.
use spawn::{Scratch, Serve};
use tinio_config::sources::SANDBOX_VAR;
use tinio_sandbox::Event;
use tokio::time::sleep;

/// Case 7: a build **without** the `sandbox` feature refuses `--sandbox`
/// rather than ignoring the flag.
///
/// **This arm is reachable as of the final review pass, and that is the whole
/// reason this leg exists.** The example used to be declared
/// `required-features = ["sandbox"]`, so no build could produce a `serve`
/// binary without the feature — which left the refusal in the example's `main`
/// (`#[cfg(not(feature = "sandbox"))]`) unreachable in every configuration that
/// could be built, and untested. The attribute is gone because a
/// `--no-default-features` build must still be able to serve the harness, and
/// what the feature gates is the `--sandbox` arm's body rather than the
/// program's existence (design decisions 4 and 19) — so a build without it is
/// a build that can receive the request this test makes.
///
/// It runs only where the feature is off, which is CI's `no-default` leg: that
/// leg builds the example with `--no-default-features` and then runs this
/// suite, so the binary under test is the one the assertion is about. A
/// default-features run compiles this out, because there the flag is honoured.
#[cfg(not(feature = "sandbox"))]
#[tokio::test]
async fn a_build_without_the_sandbox_feature_refuses_the_flag() {
    let scratch = Scratch::new();
    let root = scratch.root();

    let mut server = Serve::spawn(&root, &["--sandbox"], &[]);
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert_eq!(
        status.code(),
        Some(2),
        "a build without the feature must refuse `--sandbox` with the usage status rather than \
         run unconfined; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("without the `sandbox` feature"),
        "stderr must name the feature the build is missing; got:\n{stderr}"
    );
    // And the same refusal is an event on stdout: a consumer that waits for a
    // frame reads the cause where an empty stream is the timeout it reports
    // (design decision 19) — the property that keeps a refusal legible without
    // the operator having to know which build they were handed.
    let event = server
        .stdout_event()
        .expect("the refusal frame, since the process exited");
    assert!(
        matches!(&event, Event::Error { message } if message.contains("`sandbox` feature")),
        "the refusal must send an `error` frame naming the feature, not {event:?}"
    );
    let lines = server.stdout_lines();
    assert_eq!(
        lines.len(),
        3,
        "stdout must carry the refusal frame and nothing else — an `event:` line, a `data:` \
         line and the blank line that closes it; got {lines:?}"
    );
}

/// What a sandbox channel's run actually did — the two shapes the invariant allows.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// Nothing on this build can honour the channel, so the run refused rather than serving
    /// unconfined.
    Refused,
    /// This target's backend is wired, so the channel was honoured and the process is serving.
    Honoured,
}

/// **The invariant every sandbox channel is held to, on every build: a requested confinement is
/// never silently ignored.**
///
/// Where no backend can honour the channel the run refuses, names the channel on stderr and
/// writes **one** `Error` frame on stdout; where this target's backend is wired the request is
/// honoured and the run serves. Nothing on this side of the pipe can certify "confined", so the
/// honoured arm asserts the readiness event and stops there — the platform legs
/// (`sandbox_e2e*`) are what prove the confinement itself.
async fn refused_or_honoured(server: &mut Serve, channel: &str) -> Answer {
    let timeout = wait_timeout();
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(event) = server.stdout_event() {
            match event {
                Event::Error { message } => {
                    assert!(
                        message.contains(channel),
                        "a refusal names the channel that asked for it (`{channel}`): {message}"
                    );
                    let status = server.exited().await;
                    let stderr = server.stderr_text();
                    assert!(
                        !status.success(),
                        "a channel this build cannot honour must refuse to start rather than serve \
                         unconfined ({status}); stderr:\n{stderr}"
                    );
                    assert!(
                        stderr.contains(channel),
                        "the refusal must name the channel that was set (`{channel}`); stderr:\n{stderr}"
                    );
                    assert!(
                        stderr.contains("--sandbox"),
                        "the refusal must name the channel that works (`--sandbox`); stderr:\n{stderr}"
                    );
                    let lines = server.stdout_lines();
                    assert_eq!(
                        lines.len(),
                        3,
                        "stdout must carry the refusal frame and nothing else — an `event:` line, \
                         a `data:` line and the blank line that closes it; got {lines:?}"
                    );
                    return Answer::Refused;
                }
                Event::Listening { .. } => {
                    server.stop();
                    return Answer::Honoured;
                }
            }
        }
        if Instant::now() >= deadline {
            server.stop();
            panic!(
                "the run neither refused nor reported `listening` within {} s; stderr:\n{}",
                timeout.as_secs(),
                server.stderr_text()
            );
        }
        sleep(Duration::from_millis(2)).await;
    }
}

/// Case 8: `TINIO_SANDBOX=1` with no `--sandbox` is never silently ignored.
///
/// **This is the leg the design's §11 row exists for.** Decision 1 names three channels; an
/// operator who sets `TINIO_SANDBOX=1` believes the server is confined, and one that started
/// anyway would have failed them in precisely the way §11 is written to prevent — silently, and
/// only in the direction that matters. Where this build cannot honour the channel it refuses, and
/// the refusal lands before the run creates anything it serves; where this target's backend is
/// wired the variable **is** honoured, and the run serves confined — the confinement itself being
/// the platform legs' subject.
#[tokio::test]
async fn the_sandbox_env_var_is_refused_or_honoured() {
    let scratch = Scratch::new();
    let root = scratch.root();

    let mut server = Serve::spawn(&root, &["--port", "0"], &[(SANDBOX_VAR, "1")]);
    if refused_or_honoured(&mut server, SANDBOX_VAR).await == Answer::Refused {
        assert!(
            !root.join(".tinio").exists(),
            "a refusal lands before the run serves, and this one creates the state it serves: \
             {} exists",
            root.join(".tinio").display()
        );
    }
}

/// Case 9: `[server] sandbox = true` with no `--sandbox` is never silently ignored, naming the
/// key — **the file channel, which never reaches the process as an environment variable and so
/// cannot be caught by looking at the environment alone**.
///
/// The key is schema-known, which is what makes it a named channel rather than an unknown-key
/// error: the probe reads it, and the refusal speaks about it. On a target whose backend is wired
/// the key is honoured instead, and the run serves.
#[tokio::test]
async fn the_sandbox_config_key_is_refused_or_honoured() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let config = scratch.sandbox_config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[],
    );
    refused_or_honoured(&mut server, "[server] sandbox").await;
}

/// Case 10: both channels switched **off** serve, silently.
///
/// The refusal is about a channel that asks for a confinement this phase cannot
/// deliver, not about the variable's presence: `TINIO_SANDBOX=0` and
/// `[server] sandbox = false` are the operator saying they want no sandbox, and
/// a run that refused them would be a bug of the same family as the one cases
/// 8 and 9 close. Both spellings are driven, because they are resolved by
/// different readers (the environment overlay, and the probe's key).
#[tokio::test]
async fn a_sandbox_channel_switched_off_is_not_a_refusal() {
    let scratch = Scratch::new();

    let root = scratch.root();
    let mut server = Serve::spawn(&root, &["--port", "0"], &[(SANDBOX_VAR, "0")]);
    server.ready().await;
    server.stop();

    // The same root again: the first server is stopped and reaped above, so the
    // state database's lock is released before this one starts.
    let config = scratch.sandbox_config_file(false);
    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[],
    );
    server.ready().await;
    server.stop();
}

/// Case 11: the in-root state directory is refused whatever its case, because on
/// the volume it runs on the two spellings are one directory.
///
/// Design §9 refuses a read-only root that holds the state directory, because
/// the launcher would have to grant that directory a **read-write** entry
/// inside a root the operator asked to expose read-only. On NTFS
/// `<scratch>\root` and `<scratch>\ROOT` resolve to the same directory while a
/// literal component comparison calls them unrelated — so the state directory
/// spelled in the other case slipped past both guards and got exactly that entry.
/// `tinio_util::path::contains_under_any_spelling` is the shared rule every call
/// site now uses — the config layer's guard and the launcher's among them; this
/// leg is what fails while they do not.
///
/// **Two layers refuse this pair and the config layer answers first, which is why
/// the sentence pinned below is its own.** `discover::load_or_create` asks
/// `contains_under_any_spelling` before it auto-creates the config, and this leg's first start
/// has no config yet, so the refusal that lands is `server.read_only`'s
/// (`read-only mode never writes under the served root`); the launcher's
/// `refuse_in_root_state_dir` — whose sentence names the state directory it would
/// have granted read-write — is the second of the two and is never reached. The
/// earlier refusal is the right outcome rather than an accident: the pair is
/// unservable for the reason both layers state, and the case fold that makes
/// `ROOT` and `root` one directory is what puts the pair in front of the earlier
/// one at all.
///
/// **The two paths really are one directory**: the root exists as `<scratch>\
/// root`, `TINIO_STATE_DIR` names `<scratch>\ROOT\state`, and the volume
/// resolves both to the same tree — so the refusal is about a genuinely in-root
/// state directory, not about a spelling. A run that let it through would go on
/// to build the exposure set with a read-write entry inside the read-only root
/// (which case 11's unit-test twin in the example pins directly).
///
/// Windows-only because the fold is, and feature-gated because a build without
/// `sandbox` refuses `--sandbox` at the flag, before any configuration is read —
/// so the state-directory refusal is unreachable there and this leg would fail
/// for the wrong reason.
#[cfg(all(windows, feature = "sandbox"))]
#[tokio::test]
async fn an_in_root_state_dir_is_refused_whatever_its_case() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let state = root
        .parent()
        .expect("the root sits in the scratch dir")
        .join("ROOT")
        .join("state");

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--read-only", "--sandbox"],
        &[state_var(&state)],
    );
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert!(
        !status.success(),
        "a read-only root that holds the state directory cannot be served, and a differently-cased \
         spelling does not make it servable ({status}); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("read-only mode never writes under the served root"),
        "the refusal must be the config layer's in-root refusal rather than anything else it could \
         have failed on (the launcher's guard is the second layer and this run never reaches it); \
         stderr:\n{stderr}"
    );
    // **And it is a frame, not only a stderr line.** The config layer runs inside
    // `start::run`, whose result is `main`'s return value — so the frame comes from the
    // boundary rather than from the refusal: `run`'s serving arm sends every refusal
    // that `start::run` returns through `events::emit_error` before the `Err` travels
    // on, which is decision 19's "a startup failure is an event rather than an absence"
    // applied to the sequence as a whole. The lines are pinned rather than the event
    // alone: one frame, and nothing else on stdout.
    let event = server
        .stdout_event()
        .expect("the refusal frame, since the process exited");
    assert!(
        matches!(&event, Event::Error { message } if message.contains("read-only mode never writes under the served root")),
        "the in-root refusal must send an `error` frame, not {event:?}"
    );
    assert_eq!(
        server.stdout_lines().len(),
        3,
        "stdout must carry the refusal frame and nothing else — an `event:` line, a `data:` line \
         and the blank line that closes it; got {:?}",
        server.stdout_lines()
    );
}
