//! The one sandbox refusal that must stay in Rust: **a build without the
//! `sandbox` feature refuses `--sandbox` rather than ignoring it** (FR-036,
//! design decision 10/§11).
//!
//! **Why not a scenario.** The `@confinement` family's step layer drives one
//! binary — the default-features `tinio` the e2e leg builds — so the feature-off
//! refusal is the one arm it cannot reach; the plan keeps this leg for exactly
//! that reason (`docs/superpowers/plans/2026-09-29-sandbox-bdd-e2e.md` §5, the
//! `serve_sandbox_channels.rs:55` row).
//!
//! **The build under test is the CI `no-default` leg's.** `run-test-leg` builds
//! `tinio --no-default-features` as a prerequisite ("the artifact under test
//! must be the one the leg's feature set produces", `.github/actions/run-test-leg/action.yml`),
//! and this target carries no `required-features` in `Cargo.toml`: the same leg
//! compiles it in, while a default-features run compiles the whole file out.
//!
//! The spawn harness is [`spawn`]'s, through `common`'s framing helpers.

#![cfg(not(feature = "sandbox"))]

mod common;

#[path = "support/serve_spawn.rs"]
mod spawn;

use spawn::{Scratch, Serve};

/// FR-036: a build **without** the `sandbox` feature refuses `--sandbox`
/// rather than run unconfined.
///
/// **The flag is not in that parser at all**: its declaration is the feature's
/// (decision 10, `Args`'s `sandbox` field), so clap answers it in clap's own
/// words with the usage status rather than the crate refusing a request a parser
/// accepted.
///
/// **The refusal is a frame on stdout in neither build, and the order is the
/// reason**: a parse error is answered before any command is dispatched, and
/// the serving path is the only writer of a frame (decision 19). What this
/// channel owes is the usage status and a run that never served, and both are
/// what the leg asserts.
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
