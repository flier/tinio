//! Sandbox channels on the `serve` example: the flag this build cannot honour,
//! and the two channels no phase implements (decision 5).
//!
//! Read-only mode lives in `serve_read_only.rs`. Both files spawn through
//! [`spawn`].

mod common;

#[path = "support/serve_spawn.rs"]
mod spawn;

use std::process::ExitStatus;

use spawn::{Scratch, Serve, state_var};
use tinio_config::sources::SANDBOX_VAR;
use tinio_sandbox::Event;

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

/// The half both sandbox-channel refusals share: a non-zero exit, the channel
/// named on stderr, and **one** `Error` frame on stdout naming it too.
///
/// The frame is the half a consumer reads (decision 19), and the line count is
/// what pins that the refusal is a frame and not a frame plus something else —
/// the same check every other refusal leg here makes.
async fn refused_channel(server: &mut Serve, channel: &str) -> ExitStatus {
    let status = server.exited().await;
    let stderr = server.stderr_text();
    assert!(
        !status.success(),
        "a run whose `{channel}` channel is not honoured must refuse to start rather than serve \
         unconfined ({status}); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains(channel),
        "the refusal must name the channel that was set (`{channel}`); stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("--sandbox"),
        "the refusal must name the channel that works today (`--sandbox`); stderr:\n{stderr}"
    );
    let event = server
        .stdout_event()
        .expect("the refusal frame, since the process exited");
    assert!(
        matches!(&event, Event::Error { message } if message.contains(channel)),
        "the refusal must send an `error` frame naming `{channel}`, not {event:?}"
    );
    let lines = server.stdout_lines();
    assert_eq!(
        lines.len(),
        3,
        "stdout must carry the refusal frame and nothing else — an `event:` line, a `data:` \
         line and the blank line that closes it; got {lines:?}"
    );
    status
}

/// Case 8: `TINIO_SANDBOX=1` with no `--sandbox` is refused, naming the channel.
///
/// **This is the leg the design's §11 row exists for.** Decision 1 names three
/// channels and no phase implements two of them; an operator who sets
/// `TINIO_SANDBOX=1` believes the server is confined, and one that started
/// anyway would have failed them in precisely the way §11 is written to
/// prevent — silently, and only in the direction that matters.
///
/// **The root does not exist, and that is deliberate**: the refusal fires
/// before the run creates anything, so a missing root that is still missing
/// afterwards is the evidence that nothing was served — and a build without the
/// refusal would create it and bind a port here rather than fail.
#[tokio::test]
async fn the_sandbox_env_var_is_refused_rather_than_ignored() {
    let scratch = Scratch::new();
    let missing = scratch.missing_root();

    let mut server = Serve::spawn(&missing, &["--port", "0"], &[(SANDBOX_VAR, "1")]);
    refused_channel(&mut server, SANDBOX_VAR).await;
    assert!(
        !missing.exists(),
        "the refusal must land before the run serves, and this one creates the root it serves: \
         {} exists",
        missing.display()
    );
}

/// Case 9: `[server] sandbox = true` with no `--sandbox` is refused, naming the
/// key — **the file channel, which never reaches the process as an environment
/// variable and so cannot be caught by looking at the environment alone**.
///
/// The key is schema-known, which is what makes it a named channel rather than
/// an unknown-key error: the probe reads it, the refusal speaks about it, and
/// the operator is told which channel is honoured today.
#[tokio::test]
async fn the_sandbox_config_key_is_refused_rather_than_ignored() {
    let scratch = Scratch::new();
    let root = scratch.root();
    let config = scratch.sandbox_config_file(true);

    let mut server = Serve::spawn(
        &root,
        &["--port", "0", "--config", config.to_str().unwrap()],
        &[],
    );
    refused_channel(&mut server, "[server] sandbox").await;
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

/// Case 11: the state-directory guard is case-folded, because on the volume it
/// runs on the two spellings are one directory.
///
/// Design §9 refuses a read-only root that holds the state directory, because
/// the launcher would have to grant that directory a **read-write** entry
/// inside a root the operator asked to expose read-only. On NTFS
/// `<scratch>\root` and `<scratch>\ROOT` resolve to the same directory while a
/// literal component comparison calls them unrelated — so the state directory
/// spelled in the other case slipped past the guard and got exactly that entry.
/// `tinio_util::path::path_contains` is the shared rule both call sites now
/// use; this leg is what fails while they do not.
///
/// **The two paths really are one directory**: the root exists as `<scratch>\
/// root`, `TINIO_STATE_DIR` names `<scratch>\ROOT\state`, and the volume
/// resolves both to the same tree — so the refusal is about a genuinely in-root
/// state directory, not about a spelling. A run that let it through would go on
/// to build the exposure set with a read-write entry inside the read-only root
/// (which case 11's unit-test twin in the example pins directly).
///
/// Windows-only because the fold is, and feature-gated because the refusal is
/// the launcher's — on a build without `sandbox` the flag is refused before it.
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
        stderr.contains("holds the state directory"),
        "the refusal must be the in-root state directory guard rather than anything else it could \
         have failed on; stderr:\n{stderr}"
    );
    // **And it is a frame, not only a stderr line.** This guard refuses inside the
    // launcher's arm, whose result is `main`'s return value — so it skipped the
    // `emit_on_error` wrapping every other startup step goes through, and a
    // consumer decoding the stream waited out a readiness timeout for a run that
    // had already refused. Decision 19's rule is that a startup failure is an
    // event rather than an absence, and the lines are pinned rather than the
    // event alone: one frame, and nothing else on stdout.
    let event = server
        .stdout_event()
        .expect("the refusal frame, since the process exited");
    assert!(
        matches!(&event, Event::Error { message } if message.contains("holds the state directory")),
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
