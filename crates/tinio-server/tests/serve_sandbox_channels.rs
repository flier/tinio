//! Sandbox channels on the `tinio server` path: the flag a feature-off build
//! cannot honour, and the channels a target whose backend is not wired cannot
//! (design decision 5).
//!
//! **What every leg here is actually about is the invariant, not the refusal**:
//! a requested confinement is never silently ignored. Where this target's backend
//! is wired the request is honoured and the run serves; where nothing can honour
//! the channel the run refuses and does not serve. **The refusal's own shape stays here** —
//! exactly one `Error` frame on stdout, exactly one cause named on stderr, and no
//! service from a refused run ([`refused_or_honoured`], `design §12` bullet 4) — while
//! the dispatch itself (which channel asked for the confinement, that the refusal names
//! it) is `crates/tinio-e2e/tests/features/confinement.feature`'s:
//! its step layer (`steps/cli.rs`'s `refused` / `refused_by_build` / `applied`)
//! drives the same invariant from outside the process. Read-only mode lives in
//! `serve_read_only.rs`. Both files spawn through [`spawn`].

mod common;

#[path = "support/serve_spawn.rs"]
mod spawn;

use std::time::{Duration, Instant};

use common::wait_timeout;
use spawn::{Scratch, Serve};
use tinio_config::sources::SANDBOX_VAR;
use tinio_sandbox::Event;
use tokio::time::sleep;

/// Case 7: a build **without** the `sandbox` feature refuses `--sandbox`
/// rather than ignoring the flag.
///
/// **The binary under test is the facade's `tinio`**, which the CI
/// `no-default` leg builds with `--no-default-features` as a prerequisite of
/// this suite (`run-test-leg`'s own step — the artifact under test must be the
/// one the leg's feature set produces), and in that build **the flag is not in
/// the parser at all**: its declaration is the feature's (decision 10,
/// `Args`'s `sandbox` field), so clap answers it in clap's own words with the
/// usage status rather than the crate refusing a request a parser accepted.
/// `TINIO_SANDBOX` cannot be compiled away, so it keeps a hidden argument and
/// the crate refuses it itself — [`refused_or_honoured`] is where that
/// refusal's shape is pinned.
///
/// **The refusal is a frame on stdout in neither build, and the order is the
/// reason**: a parse error is answered before any command is dispatched, and
/// the serving path is the only writer of a frame (decision 19). What this
/// channel owes is the usage status and a run that never served, and both are
/// what the leg asserts. A default-features run compiles the leg out, because
/// there the flag is in the parser and honoured.
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
    // The flag was received and answered — clap's refusal names the argument it
    // was given, which is what keeps this leg's exit status a refusal of *this*
    // request rather than of anything else on the command line.
    assert!(
        stderr.contains("unexpected argument") && stderr.contains("--sandbox"),
        "the flag is not in that parser, so clap is what refuses it — in clap's words, naming \
         the argument; got:\n{stderr}"
    );
    assert!(
        server.stdout_lines().is_empty(),
        "stdout is the serving path's protocol and no command has been dispatched, so a refusal \
         answered by the parser writes no frame; got {:?}",
        server.stdout_lines()
    );
    assert!(
        !root.join(".tinio").exists(),
        "a refusal lands before the run creates anything it serves: {} exists",
        root.join(".tinio").display()
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

/// **The wiring of every sandbox channel's run, on every build: a requested confinement is
/// never silently ignored — and, where the run refuses, what a refusal looks like on the wire.**
///
/// Where this target's backend is wired the request is honoured and the run serves; where nothing
/// can honour the channel the run refuses instead of serving unconfined, and what stays in Rust is
/// the refusal's **own shape**: exactly one frame on stdout, exactly one cause named on stderr, and
/// no service from a refused run (`design §12` bullet 4). **The dispatch — which channel asked for the
/// confinement, that the refusal names it, and that the refused run never served — is the step
/// layer's** (`confinement.feature`'s G1–G7 through `steps/cli.rs`'s `refused` /
/// `refused_by_build` / `applied`), which is where the plan's §5 split sent it; a decoded step
/// would hide the bytes pinned here. Nothing on this side of the pipe can certify "confined", so
/// the honoured arm asserts the readiness event and stops there — the platform legs
/// (`sandbox_e2e*`) are what prove the confinement itself.
async fn refused_or_honoured(server: &mut Serve) -> Answer {
    let timeout = wait_timeout();
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(event) = server.stdout_event() {
            match event {
                Event::Error { .. } => {
                    // **The wait before the read is the pipes' need rather than an assertion's**:
                    // a refusal's stderr is only complete once the process has exited, and the
                    // exactly-one-cause rule below reads it. What the status *is* belongs to the
                    // step layer's `refused`; what stays here is that the refusal did not serve.
                    let status = server.exited().await;
                    let stderr = server.stderr_text();
                    assert!(
                        !status.success(),
                        "a channel this build cannot honour must refuse to start rather than serve \
                         unconfined ({status}); stderr:\n{stderr}"
                    );
                    // The pair's own rule (`refuse_unhonourable`'s doc): the build's message
                    // names the build and never the platform, the platform's names the platform
                    // and never the build. `--sandbox` is not asserted as "the channel that
                    // works" in either of them: a build whose feature is off has no such flag in
                    // its parser at all (Case 7), so no refusal this crate writes can name it.
                    assert!(
                        stderr.contains("this build") != stderr.contains("this platform"),
                        "the refusal names exactly one cause — the build's missing feature or the \
                         platform's stub backend, never both; stderr:\n{stderr}"
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
    if refused_or_honoured(&mut server).await == Answer::Refused {
        // **The store, not the directory it sits in.** `resolve::server` runs before the sandbox
        // plan and mints `<root>/.tinio/.tinio.toml` on its way to the resolution — the document
        // every first run creates, refused or not — which is the one creation `start::run`'s own
        // note measures a refusal as unable to precede. What a refusal does precede is the
        // serving path, and the store is its first write under the root; so the absence asserted
        // here is the one that says "nothing was served".
        assert!(
            !root.join(".tinio/meta.redb").exists(),
            "a refusal lands before the run serves, and the store is what a serving run opens: \
             {} exists",
            root.join(".tinio/meta.redb").display()
        );
    }
}

/// Case 9: `[server] sandbox = true` with no `--sandbox` is never silently ignored —
/// **the file channel, which never reaches the process as an environment variable and so
/// cannot be caught by looking at the environment alone**.
///
/// The key is schema-known, which is what makes it a named channel rather than an unknown-key
/// error: the probe reads it and the run answers it rather than erroring on it. On a target whose
/// backend is wired the key is honoured and the run serves; where nothing can honour it the
/// refusal's shape is [`refused_or_honoured`]'s, and the channel's own naming is the step layer's
/// (`confinement.feature`'s G1/G3 through `steps/cli.rs`'s `refused`).
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
    refused_or_honoured(&mut server).await;
}
