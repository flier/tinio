//! Test-only helpers, shared by this crate's own unit tests, the jail suite and
//! `tinio-server`'s Linux e2e legs.
//!
//! **Three of them are this platform's own and live here rather than in
//! `tinio-util::testing`**: [`requested_model`] and [`host_can_confine`] speak
//! this crate's [`Mechanism`], and [`unshare_probe`] runs util-linux's
//! `unshare(1)` against this platform's `/proc` — none of the three can cross to
//! a shared crate without taking this crate's vocabulary and this platform's
//! tooling with it. They are here because `tests/jail.rs` and
//! `tinio-server/tests/sandbox_e2e.rs` must agree on them word for word and had
//! a copy each; `docs/tests.md` names the convention — shared test support lives
//! on the crate that owns the subject.
//!
//! **The other two are `tinio_util::testing`'s, re-exported here** for the reason
//! that module states: `record_events`, the `assert_step` read off it and the
//! `with_ansi(false)` that assertion rests on are spelled once there, because a
//! crate whose root is `#![cfg(target_os = "…")]` cannot take them from another
//! such crate. This crate's unit-test modules name them through `crate::testing`.
//!
//! **The module is compiled for this crate's unit tests and behind the
//! non-default `testing` feature**, which is what its self dev-dependency and
//! `tinio-server`'s turn on: a `#[cfg(test)]` module is invisible to `tests/`, so
//! the harness has to be lib code to cross that boundary at all.

use std::{
    env,
    process::{Command, Stdio},
};

// The two shared devices, under the names this crate's unit tests already
// import — the `cfg(test)` is the only build they are needed in, and it is the
// one build that has `tinio-util` on the lib's own edge list.
#[cfg(test)]
pub(crate) use tinio_util::testing::{assert_step, record_events};

use crate::Mechanism;

/// The model the WSL2 script asked for, read the way the child reads it: the
/// same variable through the same one reader.
pub fn requested_model() -> Mechanism {
    Mechanism::from_env(env::var("TINIO_SANDBOX_MODEL").ok().as_deref())
        .expect("TINIO_SANDBOX_MODEL must name a model")
}

/// Whether this host can confine under `model` — asked **before** the child
/// runs, and asked as a capability rather than as a substring of a failure.
///
/// **This is the whole difference between a green suite and a quiet one.** The
/// earlier shape skipped whenever the child's log contained `confine-failed:`,
/// which made every confinement bug indistinguishable from a host that cannot
/// confine: a mount table that never built, a device node created as a
/// directory (Task 5's `apply_mount`) or a `prctl` that was refused all printed
/// `skipping:` and exited 0. The question has a positive answer, it is
/// answerable here, and asking it first is what lets the child's failure be a
/// failure.
///
/// `Ok(())` when the kernel granted every namespace the model needs. `Err` with
/// the `unshare(1)` invocations that answered no, spelled as an operator would
/// retype them — carried out rather than logged, because the caller is where
/// the answer decides between failing and skipping, and its message has to name
/// the command it tried.
///
/// **`unshare(1)` rather than the backend's own `userns_available()`**: that
/// probe forks, and its safety argument is that nothing else exists in the
/// process yet (design §10) — a libtest worker thread is not that place. The
/// command is the one the WSL2 script gates on, so the script and the test
/// agree by construction rather than by review. It is also a *lower* bound on
/// this binary's capability, and knowingly so: `mechanism.rs` tells operators to
/// give the binary an AppArmor profile, which confines a profiled child where
/// plain `unshare(1)` still fails — so a host can answer this probe with "no"
/// and still confine the child. That is the residual fuzziness a skip has to be
/// deliberate to cross, and it is why the opt-in exists.
pub fn host_can_confine(model: Mechanism) -> Result<(), String> {
    // Each variant is asked for the namespaces it actually needs: `--user
    // --map-root-user` for the user-namespace model, and a bare `--mount` for
    // the root model, whose single capability is `CAP_SYS_ADMIN`. Asking the
    // kernel rather than reading `geteuid()` is the point — a container can run
    // as uid 0 and still refuse a mount namespace, which is a host where this
    // model would fail at the first mount rather than at the probe.
    let user: &[&str] = &["--user", "--map-root-user"];
    let mount: &[&str] = &["--mount"];
    let probes: &[&[&str]] = match model {
        Mechanism::UserNamespace => &[user],
        Mechanism::Root => &[mount],
        Mechanism::Auto => &[user, mount],
    };
    if probes.iter().any(|flags| unshare_probe(flags)) {
        return Ok(());
    }
    Err(probes
        .iter()
        .map(|flags| format!("unshare {} true", flags.join(" ")))
        .collect::<Vec<_>>()
        .join(" and "))
}

/// `unshare(1)` with `flags`, as a process of its own.
///
/// A missing `unshare(1)` panics rather than skipping: the precondition is then
/// unknowable, and a suite that cannot tell "this host cannot confine" from "I
/// could not ask" is the shape this function exists to delete.
pub fn unshare_probe(flags: &[&str]) -> bool {
    Command::new("unshare")
        .args(flags)
        .arg("true")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or_else(|err| panic!("the precondition needs util-linux's `unshare(1)`: {err}"))
}
